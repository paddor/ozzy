//! One bounded live window, independent of lagging peers' retained disk history.

use crate::replica_actor::HistoryReason;
mod append;
mod flow;
mod receive;
mod recent;
mod replay;

pub(super) use flow::Flow;

use std::collections::VecDeque;

use ozzy_replication::{Commit, NormalReplica, ReplicationError};

use super::ingress::{Reply, Submission};
use super::{
    ActorError, AppendBuffer, Control, DriverError, Duration, JournalGeneration, Message, NodeId,
    PendingIo, Prefix, ProposalOutcome, ReplicaActor, Scope, SendClass,
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
    pub(super) waiting: Option<Submission>,
    validating: Option<Reply>,
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
    // Bound rejected/stale validations too: they do not fill accepted capacity.
    sync_batch_attempts: usize,
    sync_batch_operations: usize,
    sync_batch_body_bytes: usize,
    // Starts at admission, not completion: slow writes/rolls consume this age.
    // Retained across a view fence until the old admitted I/O is synchronized.
    sync_batch_started: Option<Duration>,
    // Body bytes of each queued write, in the order of `pending_persistence`.
    pub(super) persisting_bytes: VecDeque<usize>,
    cursors: [Option<Prefix>; 3],
    announced: Option<Prefix>,
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
        capacity: usize,
        channel: ozzy_replication::flow::Channel,
        receive_buffer: AppendBuffer,
        pipeline: ozzy_replication::PipelineLimits,
        replay_cache: ozzy_replication::PipelineLimits,
        flow: Flow,
    ) -> Result<Self, ActorError> {
        Ok(Self {
            scope: channel.scope,
            live: VecDeque::with_capacity(capacity),
            waiting: None,
            validating: None,
            ready: None,
            window_full: false,
            needs_sync: false,
            sync_batch_attempts: 0,
            sync_batch_operations: 0,
            sync_batch_body_bytes: 0,
            sync_batch_started: None,
            persisting_bytes: VecDeque::with_capacity(capacity),
            cursors: [None; 3],
            announced: None,
            deferred_commit: None,
            replay_peer: 0,
            replay_plan: None,
            replay_blocked: [None; 3],
            receive: receive::Receive::new(receive_buffer, channel, pipeline)?,
            recent: recent::Recent::new(replay_cache),
            flow,
        })
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
            self.work.scope = self.driver.scope();
            self.work.cursors = [None; 3];
            self.work.announced = None;
            self.work.sync_batch_attempts = 0;
            self.work.deferred_commit = None;
            self.work.receive.reset();
            self.work.recent.clear();
            self.work.replay_plan = None;
            self.work.replay_blocked = [None; 3];
            self.work.flow.change_scope(self.driver.scope(), now)?;
        }
        let snapshot = self.driver.normal().map(NormalReplica::snapshot);
        // Count/body bounded by the admitted live window, not total retained history.
        while let Some(front) = self.work.live.front() {
            if front.buffer.is_none() {
                break;
            } // An admitted write still owns the arena.
            let same = snapshot.is_some_and(|s| {
                s.scope == front.scope && s.journal.generation == front.generation
            });
            if same && snapshot.is_some_and(|s| s.applied.op < front.end.op) {
                break;
            }
            let live = self.work.live.pop_front().expect("checked front");
            let buffer = live.buffer.expect("completed write");
            let outcome = if same {
                if !live.retry {
                    self.work.recent.retain(recent::Entry {
                        scope: live.scope,
                        generation: live.generation,
                        predecessor: live.predecessor,
                        end: live.end,
                        operations: buffer.len(),
                        retained_bytes: buffer.retained_bytes(),
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
        let committed = self
            .driver
            .normal()
            .expect("normal primary")
            .snapshot()
            .committed;
        if self.work.announced != Some(committed) {
            for to in *self.configuration.voters() {
                if to != self.local {
                    self.control(
                        to,
                        Control::Commit(Commit {
                            scope: snapshot.scope,
                            committed,
                        }),
                    )?;
                }
            }
            self.work.announced = Some(committed);
        }
        self.send_live()?;
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
                .expect("authenticated voter");
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
        self.work.sync_batch_attempts = 0;
        self.work.sync_batch_operations = 0;
        self.work.sync_batch_body_bytes = 0;
        self.work.sync_batch_started = None;
        Ok(())
    }

    pub(super) fn schedule_normal(&mut self, now: Duration) -> Result<(), ActorError> {
        let snapshot = self.driver.normal().expect("normal scheduling").snapshot();
        let primary = self.configuration.primary(snapshot.scope.view) == self.local;
        debug_assert!(self.work.ready.is_none(), "ready admission rides its turn");
        if snapshot.applied != snapshot.committed {
            let ticket = self.driver.begin_validation()?;
            // A staged suffix validates in the same turn, before the apply.
            let validate = if !primary && !self.sync_due(primary, now) {
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
            return Ok(());
        }
        if self.sync_due(primary, now) {
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
        if !primary && self.schedule_received()? {
            return Ok(());
        }
        if primary && self.schedule_flow_verification()? {
            return Ok(());
        }
        if primary && self.schedule_replay(now)? {
            return Ok(());
        }
        if self.work.waiting.is_none() {
            self.work.waiting = self.ingress.pop();
        }
        if self.work.waiting.is_some() {
            if !primary {
                let submission = self.work.waiting.take().expect("waiting request");
                submission
                    .reply
                    .finish(submission.buffer, ProposalOutcome::NotAdmitted);
            } else if let Some(propose) = self.take_proposal(&snapshot)? {
                self.submit_turn(crate::replica_journal::Turn {
                    propose: Some(propose),
                    ..Default::default()
                })?;
                return Ok(());
            }
        }
        if self.work.needs_sync {
            if self.pending_sync.is_none() {
                self.schedule_sync()?;
            }
        } else {
            self.schedule_donor(true)?;
        }
        if self.work.waiting.is_none() {
            self.ingress.resume();
        }
        Ok(())
    }

    fn sync_due(&self, primary: bool, now: Duration) -> bool {
        self.work.needs_sync
            && ((primary
                && self.work.sync_batch_attempts >= self.config.sync_batch_target.operations)
                || self.work.sync_batch_operations >= self.config.sync_batch_target.operations
                || self.work.sync_batch_body_bytes >= self.config.sync_batch_target.body_bytes
                || self.work.sync_batch_started.is_some_and(|started| {
                    now.saturating_sub(started) >= self.config.sync_batch_max_age
                }))
    }

    /// Take the waiting or next queued proposal when the live window has room,
    /// with a validation ticket for the image the journal will then hold.
    fn take_proposal(
        &mut self,
        snapshot: &ozzy_replication::ReplicaSnapshot,
    ) -> Result<
        Option<(
            ozzy_replication::driver::ValidationTicket,
            crate::replica_journal::ProposalBuffer,
        )>,
        ActorError,
    > {
        if self.work.waiting.is_none() {
            self.work.waiting = self.ingress.pop();
        }
        let Some(submission) = &self.work.waiting else {
            return Ok(None);
        };
        if submission.reply.fenced() {
            let submission = self.work.waiting.take().expect("waiting request");
            submission
                .reply
                .finish(submission.buffer, ProposalOutcome::NotAdmitted);
            return Ok(None);
        }
        if !self
            .journal
            .validation_has_capacity(&submission.buffer.0, snapshot.accepted.op)
        {
            return Ok(None);
        }
        if submission.buffer.len()
            > self.config.pipeline.max_operations - snapshot.pending_operations
            || submission.buffer.body_bytes()
                > self.config.pipeline.max_body_bytes - snapshot.pending_body_bytes
        {
            if !std::mem::replace(&mut self.work.window_full, true) {
                crate::profiling::event(crate::profiling::Event::LiveWindowFull);
            }
            return Ok(None);
        }
        self.work.window_full = false;
        let mut submission = self.work.waiting.take().expect("waiting request");
        let ticket = self.driver.begin_validation()?;
        submission.reply.scheduled();
        if !self.work.needs_sync {
            self.work.sync_batch_attempts = 0;
        }
        self.work.sync_batch_attempts += 1;
        assert!(self.work.validating.replace(submission.reply).is_none());
        Ok(Some((ticket, submission.buffer)))
    }

    /// Install the accepted batch directly. The next validation observes its
    /// installed image in the next bounded actor turn.
    pub(super) fn send_ready_turn(&mut self, _now: Duration) -> Result<(), ActorError> {
        let Some(admit) = self.work.ready.take() else {
            return Ok(());
        };
        let apply = if self.driver.normal().is_some_and(|normal| {
            let snapshot = normal.snapshot();
            snapshot.applied != snapshot.committed
        }) {
            Some(self.driver.begin_validation()?)
        } else {
            None
        };
        let turn = Box::new(crate::replica_journal::Turn {
            admit: Some(admit),
            apply,
            ..Default::default()
        });
        assert!(self.pending.is_none());
        match self.journal.turn(turn) {
            Ok(completion) => self.pending = Some(PendingIo::Turn(completion)),
            Err(rejected) if rejected.reason == crate::replica_journal::SubmitError::Full => {
                // Retained backing can fill the physical backlog before the
                // core's operation/body window fills. Keep the same validated
                // batch in its existing slot until physical progress frees it.
                self.work.ready = rejected.value.admit;
            }
            Err(rejected) => return Err(rejected.reason.into()),
        }
        Ok(())
    }

    /// Record live-window occupancy after a core admission.
    pub(super) fn observe_window(&self) {
        if crate::profiling::enabled()
            && let Some(normal) = self.driver.normal()
        {
            let snapshot = normal.snapshot();
            crate::profiling::peak(
                crate::profiling::Event::LiveWindowPeakOperations,
                snapshot.pending_operations as u64,
            );
            crate::profiling::peak(
                crate::profiling::Event::LiveWindowPeakBytes,
                snapshot.pending_body_bytes as u64,
            );
        }
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
