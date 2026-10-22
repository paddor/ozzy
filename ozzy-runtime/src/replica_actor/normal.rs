//! One bounded live window, independent of lagging peers' retained disk history.

use crate::replica_actor::HistoryReason;
use crate::replica_actor::require_normal;
mod append;
mod flow;
mod propose;
mod receive;
mod recent;
mod replay;

pub(super) use flow::{Flow, remote_transmitters};

use std::collections::VecDeque;

use ozzy_replication::{Commit, NormalReplica, ReplicaSnapshot, ReplicationError};

use super::ingress::{NextProposal, ProposalWait, Proposals, Reply, Submission};
use super::{
    ActorConfig, ActorError, AppendBuffer, Control, DriverError, Duration, JournalGeneration,
    Message, NodeId, PendingIo, Prefix, ProposalOutcome, ReplicaActor, Scope, SendClass,
};

#[derive(Debug)]
pub(super) struct Live {
    pub(super) retry: bool,
    pub(super) published: bool,
    pub(super) scope: Scope,
    pub(super) generation: JournalGeneration,
    pub(super) predecessor: Prefix,
    pub(super) end: Prefix,
    pub(super) buffer: Option<AppendBuffer>,
    pub(super) reply: Reply,
    pub(super) packets: [Option<Message>; 3],
}

#[derive(Debug)]
pub(super) struct Work {
    scope: Scope,
    pub(super) live: VecDeque<Live>,
    pub(super) proposals: Proposals,
    // Core-admitted proposal waiting for physical journal capacity.
    pub(super) ready: Option<(
        ozzy_replication::WriteTicket,
        crate::replica_journal::ValidatedAppend,
    )>,
    // A proposal is waiting for live-window room; counts each wait once.
    window_full: bool,
    // Written work not yet captured by a submitted barrier. An older captured
    // barrier may still be executing independently of this next batch.
    pub(super) needs_sync: bool,
    sync_batch_operations: usize,
    sync_batch_body_bytes: usize,
    // When the last disk-quorum write completed.
    last_persisted: Option<Duration>,
    // Body bytes of each queued write, in the order of `pending_persistence`.
    pub(super) persisting_bytes: VecDeque<usize>,
    cursors: [Option<Prefix>; 3],
    // Last COMMIT sent to each backup. A repeat waits for the core heartbeat.
    pub(super) announced: [Option<Commit>; 3],
    deferred_commit: Option<(NodeId, Commit)>,
    replay_peer: usize,
    replay_plan: Option<replay::Plan>,
    replay_blocked: [Option<replay::Blocked>; 3],
    pub(super) receive: receive::Receive,
    recent: recent::Recent,
    pub(super) flow: Flow,
}

impl Work {
    pub(super) fn new(
        config: &ActorConfig,
        channel: ozzy_replication::flow::Channel,
        receive_buffer: AppendBuffer,
        flow: Flow,
    ) -> Result<Self, ActorError> {
        let capacity = config.proposal_capacity;
        Ok(Self {
            scope: channel.scope,
            live: VecDeque::with_capacity(capacity),
            proposals: Proposals::default(),
            ready: None,
            window_full: false,
            needs_sync: false,
            sync_batch_operations: 0,
            sync_batch_body_bytes: 0,
            last_persisted: None,
            persisting_bytes: VecDeque::with_capacity(capacity),
            cursors: [None; 3],
            announced: [None; 3],
            deferred_commit: None,
            replay_peer: 0,
            replay_plan: None,
            replay_blocked: [None; 3],
            receive: receive::Receive::new(receive_buffer, channel, config.pipeline)?,
            recent: recent::Recent::new(config.replay_cache),
            flow,
        })
    }

    /// Drop every cursor, staged suffix, and cached packet of the old scope.
    /// Returns the arenas of a deferred or split received suffix for
    /// recycling.
    fn change_scope(
        &mut self,
        scope: Scope,
        now: Duration,
    ) -> Result<[Option<AppendBuffer>; 2], ActorError> {
        self.proposals.reject_deferred();
        self.scope = scope;
        self.cursors = [None; 3];
        self.announced = [None; 3];
        self.deferred_commit = None;
        let retry = self.receive.reset();
        self.recent.clear();
        self.replay_plan = None;
        self.replay_blocked = [None; 3];
        self.flow.change_scope(scope, now)?;
        Ok(retry)
    }

    /// Reply to live entries whose writes returned their arenas: committed
    /// once applied in the same scope and generation, unknown once fenced.
    fn finish_live(&mut self, snapshot: Option<&ReplicaSnapshot>) {
        // Count/body bounded by the admitted live window, not total retained history.
        let same = |live: &Live| {
            snapshot
                .is_some_and(|s| s.scope == live.scope && s.journal.generation == live.generation)
        };
        // An admitted write still owns its arena; an applied-pending entry waits.
        while let Some(mut live) = self.live.pop_front_if(|front| {
            front.buffer.is_some()
                && !(same(front) && snapshot.is_some_and(|s| s.applied.op < front.end.op))
        }) {
            let same = same(&live);
            let Some(buffer) = live.buffer.take() else {
                continue;
            };
            let outcome = if same {
                if !live.retry {
                    self.recent.retain(recent::Entry {
                        scope: live.scope,
                        generation: live.generation,
                        predecessor: live.predecessor,
                        end: live.end,
                        operations: buffer.len(),
                        retained_bytes: buffer.retained_bytes(),
                        control: buffer.operations().any(|operation| {
                            !matches!(
                                operation.kind,
                                ozzy_journal::operation::OperationKind::Append
                                    | ozzy_journal::operation::OperationKind::ResolvedAppend
                            )
                        }),
                        packets: live.packets,
                    });
                }
                ProposalOutcome::Committed {
                    scope: live.scope,
                    through: live.end,
                }
            } else {
                ProposalOutcome::Unknown
            };
            live.reply.finish(buffer.into(), outcome);
        }
    }

    pub(super) fn receive_report(&self) -> ozzy_replication::flow::Report {
        self.receive.ledger.report()
    }

    pub(super) fn receive_has_work(&self) -> bool {
        self.receive.in_flight() || self.receive.queued_operations() != 0
    }

    pub(super) fn receive_capacity(&self) -> ozzy_replication::PipelineLimits {
        self.receive.ledger.available()
    }

    pub(super) fn receive_epoch(&self) -> ozzy_replication::flow::ReceiveEpoch {
        self.receive.ledger.report().channel.epoch
    }
}

impl ReplicaActor {
    pub(super) fn normal_round(&mut self, now: Duration) -> Result<(), ActorError> {
        if self.work.scope != self.driver.scope() {
            for buffer in self
                .work
                .change_scope(self.driver.scope(), now)?
                .into_iter()
                .flatten()
            {
                self.recycle(buffer)?;
            }
        }
        let snapshot = self.driver.normal().map(NormalReplica::snapshot);
        self.work.finish_live(snapshot.as_ref());
        let Some(snapshot) = snapshot else {
            return Ok(());
        };
        self.flow_round(now)?;
        if let Some((from, commit)) = self.work.deferred_commit
            && commit.scope == snapshot.scope
            && commit.committed.op <= snapshot.accepted.op
        {
            self.work.deferred_commit = None;
            self.receive_control(from, Control::Commit(commit), now)?;
        }
        if self.configuration.primary(snapshot.scope.view) != self.local {
            return Ok(());
        }
        let committed = require_normal(&self.driver)?.snapshot().committed;
        for voter in 0..3 {
            if self.configuration.voters()[voter] != self.local {
                self.announce(
                    voter,
                    Commit {
                        scope: snapshot.scope,
                        committed,
                    },
                    false,
                )?;
            }
        }
        self.send_live()?;
        Ok(())
    }

    /// Send one backup a COMMIT unless it repeats the last one queued there.
    /// The core heartbeat passes `repeat` and covers a lost packet.
    pub(super) fn announce(
        &mut self,
        voter: usize,
        commit: Commit,
        repeat: bool,
    ) -> Result<(), ActorError> {
        let to = self.configuration.voters()[voter];
        if !repeat && self.work.announced[voter] == Some(commit) {
            return Ok(());
        }
        // A missing session or a full queue drops it; the next call retries.
        let before = self.outbox.queued(to, SendClass::Control);
        self.control(to, Control::Commit(commit))?;
        if self.outbox.queued(to, SendClass::Control) != before {
            self.work.announced[voter] = Some(commit);
        }
        Ok(())
    }

    pub(super) fn receive_control(
        &mut self,
        from: NodeId,
        control: Control,
        now: Duration,
    ) -> Result<(), ActorError> {
        match self.driver.receive(from, control, now) {
            Ok(Some(start)) => self.start = Some((from, start)),
            Ok(None) => {}
            Err(DriverError::Replication(ReplicationError::HistoryGap)) => {
                let Control::Commit(commit) = control else {
                    return Err(ActorError::history(HistoryReason::ReceiveWindow));
                };
                if self.work.deferred_commit.is_none_or(|(_, old)| {
                    old.scope != commit.scope || old.committed.op < commit.committed.op
                }) {
                    self.work.deferred_commit = Some((from, commit));
                }
                return Ok(()); // Retry once the missing contiguous payload is available.
            }
            Err(error) => return Err(error.into()),
        }
        if let Some(prefix) = control.acknowledged()
            && self
                .driver
                .normal()
                .is_some_and(|normal| normal.snapshot().scope == control.scope())
            && self.work.scope == control.scope()
        {
            let index = self
                .configuration
                .voters()
                .iter()
                .position(|voter| *voter == from)
                .ok_or_else(|| {
                    DriverError::from(ozzy_replication::ReplicationError::UnknownVoter)
                })?;
            // Below the core's applied floor this is only a replay hint. Disk
            // or recent-cache lookup verifies its digest before a specific COMMIT.
            if self.work.cursors[index].is_none_or(|cursor| cursor.op < prefix.op) {
                self.work.cursors[index] = Some(prefix);
            }
        }
        Ok(())
    }

    pub(super) fn schedule_sync(&mut self) -> Result<(), ActorError> {
        assert!(self.pending_sync.is_none());
        let ticket = self.driver.begin_sync()?;
        if self.driver.normal().is_some() {
            self.pending_sync = Some(super::PendingSync::Barrier(
                self.journal.begin_pipelined_sync(ticket)?,
            ));
        } else {
            // Fenced drainage admits no later work; it needs no overlap lane.
            assert!(self.pending.is_none());
            self.pending = Some(PendingIo::Sync(self.journal.sync(ticket)?));
        }
        self.work.needs_sync = false;
        self.work.sync_batch_operations = 0;
        self.work.sync_batch_body_bytes = 0;
        Ok(())
    }

    /// `hold` keeps work that extends the accepted suffix waiting for a due
    /// retention turn: new proposals, ingress, and a follower's received
    /// data. Work that settles the suffix still runs.
    pub(super) fn schedule_normal(&mut self, now: Duration, hold: bool) -> Result<(), ActorError> {
        let snapshot = require_normal(&self.driver)?.snapshot();
        let primary = self.configuration.primary(snapshot.scope.view) == self.local;
        debug_assert!(self.work.ready.is_none(), "ready admission rides its turn");
        if snapshot.applied != snapshot.committed {
            return self.schedule_apply(primary, now);
        }
        if self.sync_due(now) {
            if self.pending_sync.is_none() {
                self.schedule_sync()?;
                // The captured group can sync while another group fills. Keep
                // queued ingress actionable; a full next group pauses below.
                self.ingress.resume();
            }
            return Ok(());
        }
        if self.schedule_donor(false)? {
            return Ok(());
        }
        if !primary && !hold && self.schedule_received()? {
            return Ok(());
        }
        if primary && self.schedule_flow_verification()? {
            return Ok(());
        }
        if primary && self.schedule_replay(now)? {
            return Ok(());
        }
        if !hold {
            if !primary {
                self.work.proposals.fill(&mut self.ingress);
                // A waiting proposal defers due maintenance. No journal
                // command completes once it leaves, so ask for that round now.
                if self.work.proposals.reject_all() != 0 {
                    self.more_work = true;
                }
            } else if let Some(propose) = self.take_proposal(&snapshot)? {
                self.submit_turn(crate::replica_journal::Turn {
                    propose: Some(propose),
                    ..Default::default()
                })?;
                return Ok(());
            }
        }
        // Pending `DURABLE` evidence publishes only through `sync_due`.
        if !self.work.needs_sync {
            self.schedule_donor(true)?;
        }
        if !hold && self.work.proposals.is_idle() {
            self.ingress.resume();
        }
        Ok(())
    }

    /// Apply the committed suffix. A follower's staged suffix validates in
    /// the same turn, before the apply.
    fn schedule_apply(&mut self, primary: bool, now: Duration) -> Result<(), ActorError> {
        let ticket = self.driver.begin_validation()?;
        // Confirmed identity time must be applied before bounded expiry can
        // make room for a following canonical batch on a full follower.
        let validate = if !primary && !self.sync_due(now) && !self.journal.deduplication_enabled() {
            self.take_received()?
        } else {
            None
        };
        if validate.is_some() {
            return self.submit_turn(crate::replica_journal::Turn {
                validate,
                apply: Some(ticket),
                ..Default::default()
            });
        }
        self.pending = Some(PendingIo::Apply(self.journal.apply_committed(ticket)?));
        Ok(())
    }

    fn sync_due(&self, now: Duration) -> bool {
        let target = self.config.sync_batch_target;
        let full = self.work.sync_batch_operations >= target.operations
            || self.work.sync_batch_body_bytes >= target.body_bytes;
        // `DURABLE` is recovery evidence, never a vote. It waits for a quiet
        // moment, a filled batch, or due maintenance.
        let idle = self.pending_persistence.is_empty();
        self.work.needs_sync
            && (full
                || self.evidence_deadline().is_some_and(|at| now >= at)
                || (idle && self.maintenance.due(now)))
    }

    /// When disk-quorum `DURABLE` evidence comes due with no write in flight.
    pub(super) fn evidence_deadline(&self) -> Option<Duration> {
        (self.work.needs_sync && self.pending_persistence.is_empty()).then(|| {
            self.work.last_persisted.map_or(Duration::ZERO, |at| {
                at.saturating_add(self.config.evidence_quiet)
            })
        })
    }

    fn submit_turn(&mut self, turn: crate::replica_journal::Turn) -> Result<(), ActorError> {
        assert!(self.pending.is_none());
        self.pending = Some(PendingIo::Turn(
            self.journal
                .turn(Box::new(turn))
                .map_err(|rejected| rejected.reason)?,
        ));
        Ok(())
    }
}
