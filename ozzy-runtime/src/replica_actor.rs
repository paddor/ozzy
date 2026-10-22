//! Fixed-voter replication and full-WAL recovery over ordinary OMQ PEER.
//!
//! One task owns protocol state, bounded outboxes, and journal completions. Disk
//! operations never block its receive/timer loop. Native client ingress is opt-in
//! on this PEER endpoint, with
//! credit-bounded offset subscriptions. Client-side routing lives in `replicated`. Local proposals
//! and remote appends share the journal/quorum path.
//!
//! Voter sessions must already be established with independently authenticated voters.
//! For trusted inproc/loopback development, callers may configure explicit static
//! bindings. Routing IDs and the supplied session IDs do not authenticate anyone.

mod config;
mod construct;
mod deadline;
mod diagnostics;
mod donors;
pub use diagnostics::{HistoryFailure, HistoryReason};
mod election;
mod groups;
pub use groups::{GroupCoordinator, GroupCoordinatorConfig, TopicTopology};
mod history;
mod ids;
mod maintenance;
pub use ids::{ActorIds, ReceiptHandles};
mod ingress;
mod io;
mod local;
pub use local::{LocalActor, LocalActorConfig};
mod native;
mod normal;
mod periodic;
pub use native::{
    NativeAccess, NativeIntake, NativeIntakeConfig, NativeIntakeError, NativeReceive,
};
mod reader_cursor;
mod response;
mod retention_turns;
mod shared_readers;
pub use shared_readers::SharedReaderConfig;
mod recovery;
mod scheduled;
mod startup;
mod status;
pub use scheduled::{ScheduleError, ScheduledReplica};
mod partitions;
pub use partitions::{
    PartitionActor, PartitionActors, PartitionError, PartitionStatus, SessionReplacement,
};
mod routes;
pub use routes::{RoutePublicationError, RoutePublisher};

pub use config::{ActorConfig, MAX_TRANSFER_OPERATIONS, SyncBatchTarget};
pub use recovery::{RecoveryActor, RecoveryTiming, ScheduledRecovery};
use status::SharedStatus;
pub use status::{ActorStatus, MaintenanceStatus};

#[cfg(feature = "simulation")]
pub mod simulation;

pub use ingress::{
    PendingProposal, ProposalOutcome, ProposalReply, ProposalStopped, ProposalSubmitError,
    ProposalSubmitter, UnsubmittedProposal,
};

use crate::transport::Frames;
use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{LinkSessionId, NodeId};
use ozzy_replication::driver::{Action, DriverError, ReplicaDriver, Timing};
use ozzy_replication::wire::{self, Control, PeerBinding, ReplicaMessage, WireLimits};
use ozzy_replication::{
    Configuration, JournalGeneration, LogSource, NormalReplica, PipelineLimits, Prefix,
    PromiseTicket, ReplicaSnapshot, Scope, StartView,
};

use crate::replica_journal::{
    AppendBuffer, JournalError, ProposalBuffer, ReplicaJournal, SubmitError,
};
use crate::replica_transport::{EnqueueError, OutboxError, ReplicaOutbox, SendClass};
use history::{Lookup, Transfer};
use io::{PendingIo, PendingSync};

/// One mutable protocol owner with journal futures polled on its shard.
#[derive(Debug)]
pub struct ReplicaActor {
    maintenance: maintenance::Turns,
    startup_probe: Option<startup::Probe>,
    recovery_required: bool,
    donors: Option<Box<donors::Donors>>,
    configuration: Configuration,
    local: NodeId,
    config: ActorConfig,
    /// Work found outside a journal observation: received input, a dropped
    /// link, or a published flow snapshot. The next observation reports it.
    more_work: bool,
    driver: ReplicaDriver,
    ids: ActorIds,
    journal: ReplicaJournal,
    outbox: ReplicaOutbox,
    bindings: [Option<PeerBinding>; 3],
    wire_limits: WireLimits,
    metadata: Vec<u8>,
    payload: Vec<u8>,
    buffer: Option<AppendBuffer>,
    history_allocator: Option<crate::memory::Allocator>,
    history_receive_allocator: Option<crate::memory::Allocator>,
    // Second staging arena, leased on demand when the pool allows, so a
    // follower stages the next suffix while one waits for its admission turn.
    spare: Option<AppendBuffer>,
    pending: Option<PendingIo>,
    // At most one received-suffix validation queued behind `pending`; set only
    // while `pending` is set. Needs a third journal command slot.
    pending_replay: Option<io::PendingReplay>,
    pending_sync: Option<PendingSync>,
    pending_persistence:
        VecDeque<crate::replica_journal::JournalCompletion<ozzy_replication::WriteTicket>>,
    /// When the journal last completed a command or the storage lane a job,
    /// or the journal was seen idle; from the first round.
    storage_progress_at: Option<Duration>,
    /// The storage lane's completed-job count at the last round.
    storage_jobs: u64,
    promise: Option<PromiseTicket>,
    pinned: Option<LogSource>,
    wanted_pin: Option<(Scope, LogSource)>,
    start: Option<(NodeId, StartView)>,
    lookup: Lookup,
    transfer: Option<Transfer>,
    staged: Option<Prefix>,
    journal_scope: Scope,
    activated: Option<(Scope, JournalGeneration)>,
    ack_at: Duration,
    // Last vote queued for the primary, and when.
    acked: Option<(Control, Duration)>,
    status: SharedStatus,
    ingress: ingress::Ingress,
    submitters: Vec<ProposalSubmitter>,
    work: normal::Work,
}

impl ReplicaActor {
    /// Latest observation. Intermediate states between visits are not kept.
    pub fn status(&self) -> ActorStatus {
        self.status.get()
    }

    /// Take one startup-allocated submission lane. Call before moving into `run`.
    pub fn take_submitter(&mut self) -> Option<ProposalSubmitter> {
        self.submitters.pop()
    }

    /// Lease one transfer-sized proposal arena during setup; reuse completions.
    pub fn lease_proposal_buffer(&self) -> Result<ProposalBuffer, SubmitError> {
        self.journal
            .lease_append_buffer_with_limits(self.config.transfer)
            .map(ProposalBuffer)
    }

    /// Reserve a proposal arena smaller than the complete live replication window.
    ///
    /// Both bounds must be nonzero and no larger than this actor's transfer limit.
    /// Each lease still consumes one shared worker-buffer permit until dropped.
    /// Clear/reuse preserves its smaller allocation and limits through rejection,
    /// validation, writes, and quorum replies. History/receive arenas are unchanged.
    /// Use during startup for a known request bound, not once per append.
    pub fn lease_proposal_buffer_with_limits(
        &self,
        limits: PipelineLimits,
    ) -> Result<ProposalBuffer, ActorError> {
        if limits.max_operations == 0
            || limits.max_operations > self.config.transfer.max_operations
            || limits.max_body_bytes == 0
            || limits.max_body_bytes > self.config.transfer.max_body_bytes
        {
            return Err(ActorError::Limits);
        }
        self.journal
            .lease_append_buffer_with_limits(limits)
            .map(ProposalBuffer)
            .map_err(ActorError::from)
    }

    async fn shutdown(&mut self) -> Result<(), ActorError> {
        self.publish_status(false);
        self.ingress.close();
        self.journal.shutdown().await.map_err(ActorError::Journal)
    }

    fn advance(&mut self, now: Duration) -> Result<(), ActorError> {
        // Work marked during this turn stays for the next observation.
        self.more_work = false;
        self.ingress.begin_round();
        if self.startup_round(now)? {
            self.publish_status(true);
            return Ok(());
        }
        // The view waits on this copy's own promise or installation I/O;
        // timing out while it runs would only discard the work.
        if matches!(
            self.pending,
            Some(
                PendingIo::Promise(_)
                    | PendingIo::Begin(_)
                    | PendingIo::Chunk(_)
                    | PendingIo::Finish(_)
            )
        ) {
            self.driver.history_progress(now)?;
        }
        // Storage that has work and completes none of it holds the role as
        // a stalled write does, though no write was admitted.
        let busy = self.pending.is_some()
            || self.pending_replay.is_some()
            || self.pending_sync.is_some()
            || !self.pending_persistence.is_empty();
        // A command of many jobs on a slow device is not a stall while its
        // jobs complete.
        let jobs = self.journal.completed_jobs();
        if !busy || jobs != self.storage_jobs {
            self.storage_progress_at = Some(now);
            self.storage_jobs = jobs;
        }
        let since = *self.storage_progress_at.get_or_insert(now);
        self.driver.observe_storage(busy.then_some(since), now)?;
        // At most four fixed controls (eight destinations), one history chunk,
        // and one disk submission per iteration. Each chunk has byte/count bounds.
        for _ in 0..4 {
            let Some(action) = self.driver.poll(now)? else {
                break;
            };
            self.action(action)?;
        }
        self.normal_round(now)?;
        self.donor_round()?;
        self.schedule(now)?;
        self.retry_transfer(now)?;
        self.ack(now)?;
        self.publish_status(true);
        Ok(())
    }

    fn publish_status(&self, running: bool) {
        let next = ActorStatus {
            maintenance: self.maintenance.status,
            scope: self.driver.scope(),
            normal: self.driver.normal().map(NormalReplica::snapshot),
            disk_pending: self.pending.is_some()
                || self.pending_replay.is_some()
                || self.pending_sync.is_some()
                || !self.pending_persistence.is_empty(),
            application_ready: running && self.application_ready(),
            running,
        };
        self.status.set(next);
    }

    fn application_ready(&self) -> bool {
        self.driver.normal().is_some_and(|normal| {
            let snapshot = normal.snapshot();
            snapshot.ready_for_appends
                && self.activated == Some((snapshot.scope, snapshot.journal.generation))
        })
    }

    fn action(&mut self, action: Action) -> Result<(), ActorError> {
        match action {
            Action::PersistPromise(ticket) => {
                assert!(self.promise.is_none());
                self.wanted_pin = Some((
                    ticket.scope(),
                    LogSource {
                        voter: self.local,
                        generation: ticket.generation(),
                        accepted: ticket.log().accepted,
                    },
                ));
                self.promise = Some(ticket);
            }
            Action::Broadcast(Control::Commit(commit)) => {
                for voter in 0..3 {
                    if self.configuration.voters()[voter] != self.local {
                        self.announce(voter, commit, true)?;
                    }
                }
            }
            Action::Broadcast(control) => {
                for to in *self.configuration.voters() {
                    if to != self.local {
                        self.control(to, control)?;
                    }
                }
            }
            Action::Send { to, message } => self.control(to, message)?,
        }
        Ok(())
    }

    fn control(&mut self, to: NodeId, control: Control) -> Result<(), ActorError> {
        let Some(session) = self.session(to) else {
            return Ok(()); // The driver retains/retries its protocol evidence.
        };
        let source = match control {
            Control::DoViewChange(report) => Some(LogSource {
                voter: self.local,
                generation: report.generation,
                accepted: report.log.accepted,
            }),
            Control::StartView(start) => Some(LogSource {
                voter: self.local,
                generation: start.generation,
                accepted: start.accepted,
            }),
            _ => None,
        };
        if let Some(source) = source.filter(|_| !self.application_ready()) {
            self.wanted_pin = Some((control.scope(), source));
            if self.pinned != Some(source) {
                return Ok(());
            }
        }
        let encoded = wire::encode_control(self.local, session, control, &mut self.metadata)?;
        let message = control_message(encoded, &self.metadata);
        self.enqueue(to, SendClass::Control, message)
    }

    fn enqueue(
        &mut self,
        to: NodeId,
        class: SendClass,
        message: Message,
    ) -> Result<(), ActorError> {
        offer(&mut self.outbox, to, class, message)
    }

    fn session(&self, to: NodeId) -> Option<LinkSessionId> {
        let index = self
            .configuration
            .voters()
            .iter()
            .position(|peer| *peer == to)?;
        self.bindings[index].map(|_| self.config.sessions[index])
    }

    fn receive(&mut self, message: &Message, now: Duration) -> Result<(), ActorError> {
        if let Some([peer, group, state]) = message.frames() {
            let Ok(peer) = <[u8; 16]>::try_from(peer).map(NodeId::from_bytes) else {
                return Ok(());
            };
            let Some(voter) = self
                .configuration
                .voters()
                .iter()
                .position(|p| *p == peer && *p != self.local)
            else {
                return Ok(());
            };
            if self.bindings[voter].is_none() || group != self.driver.scope().group_id.as_bytes() {
                return Ok(());
            }
            let Ok(compact) = wire::CompactState::decode(state) else {
                return Ok(());
            };
            return self.receive_compact(voter, compact, now);
        }
        let Some((index, binding, frames)) = routed(self.configuration, &self.bindings, message)
        else {
            return Ok(());
        };
        let from = self.configuration.voters()[index];
        let Ok(decoded) = wire::decode(&frames, binding, self.wire_limits) else {
            return Ok(());
        };
        match decoded {
            ReplicaMessage::Recovery(wire::RecoveryMessage::State(state))
                if self.startup_probe.is_some() =>
            {
                self.startup_response(index, &state);
            }
            ReplicaMessage::HistoryRetired(notice) => self.receive_retired(from, notice)?,
            ReplicaMessage::Checkpoint(wire::CheckpointMessage::Request(request)) => {
                self.queue_recovery_checkpoint(from, request);
            }
            ReplicaMessage::Flow(flow) => self.receive_flow(index, flow, now)?,
            ReplicaMessage::Control(control) => {
                self.receive_control(from, control, now)?;
                if matches!(control, Control::StartView(_)) {
                    self.ack_at = now;
                }
            }
            ReplicaMessage::FetchOps(request) => self.serve(from, request)?,
            ReplicaMessage::Ops(batch) => self.receive_ops(&batch, now)?,
            ReplicaMessage::Recovery(wire::RecoveryMessage::Request(request)) => {
                self.receive_recovery(index, request, now)?;
            }
            ReplicaMessage::Recovery(wire::RecoveryMessage::State(_))
            | ReplicaMessage::Checkpoint(wire::CheckpointMessage::Chunk { .. })
            | ReplicaMessage::Prepare(_) => {}
        }
        Ok(())
    }

    fn receive_data(&mut self, message: &Message, now: Duration) -> Result<bool, ActorError> {
        let Some((voter, binding, frames)) = routed(self.configuration, &self.bindings, message)
        else {
            return Ok(true);
        };
        let scope = self.driver.scope();
        let Some((batch, publication)) = self.decode_data(&scope, frames, binding) else {
            return Ok(true);
        };
        // A missing predecessor cannot be supplied by waiting on this frame.
        // Drop the gap and let the next correlated probe request the prefix.
        if batch.predecessor().op > self.work.receive_report().received.op {
            if publication {
                crate::profiling::event(crate::profiling::Event::ReplicaPublicationGap);
                self.work.flow.observe_receipt(
                    self.work.receive_report().received.op,
                    Some(batch.end().op),
                    true,
                    now,
                );
            }
            return Ok(true);
        }
        let end = batch.end().op;
        let consumed = self.receive_prepare(self.configuration.voters()[voter], batch, now)?;
        if publication && !consumed {
            self.work.flow.pending_publication = Some(end);
        }
        self.work.flow.observe_receipt(
            self.work.receive_report().received.op,
            None,
            publication,
            now,
        );
        // PUB receipt can advance beyond PEER reservations. Keep its full digest
        // report so the leader verifies exact history instead of trusting a counter.
        if publication {
            self.work.flow.clear_compact_receipt();
        }
        Ok(consumed)
    }

    /// Decode follower data: a group publication from the primary or a PEER
    /// flow PREPARE. `true` marks a publication; `None` drops the packet.
    fn decode_data<'a>(
        &self,
        scope: &'a Scope,
        frames: [&'a [u8]; 3],
        binding: PeerBinding,
    ) -> Option<(wire::PrepareBatch<'a>, bool)> {
        let packet = ozzy_proto::decode_packet(&frames, self.wire_limits.envelope).ok()?;
        if packet.envelope.opcode != ozzy_proto::Opcode::PreparePub {
            let Ok(ReplicaMessage::Flow(wire::FlowMessage::Prepare { batch, .. })) =
                wire::decode(&frames, binding, self.wire_limits)
            else {
                return None;
            };
            return Some((batch, false));
        }
        let published = [
            scope.group_id.as_bytes().as_slice(),
            frames[0],
            frames[1],
            frames[2],
        ];
        let batch =
            wire::decode_publication(&published, binding, self.local, *scope, self.wire_limits)
                .ok()?;
        crate::profiling::event(crate::profiling::Event::ReplicaPublicationReceived);
        Some((batch, true))
    }

    fn ack(&mut self, now: Duration) -> Result<(), ActorError> {
        if now < self.ack_at {
            return Ok(());
        }
        let retransmit = self.config.timing.retransmit;
        self.ack_at = now.saturating_add(retransmit);
        let primary = self.configuration.primary(self.driver.scope().view);
        if primary == self.local {
            return Ok(());
        }
        let Some(normal) = self.driver.normal() else {
            return Ok(());
        };
        let Ok(ack) = normal.acknowledgment() else {
            return Ok(());
        };
        let control = Control::PrepareOk { ack };
        // An event asks for the current vote. An unchanged one waits for the
        // retransmit timer, which also covers a lost packet.
        if let Some((vote, at)) = self.acked
            && vote == control
            && now < at.saturating_add(retransmit)
        {
            self.ack_at = at.saturating_add(retransmit);
            return Ok(());
        }
        // A missing session or a full queue drops it; the next call retries.
        let before = self.outbox.queued(primary, SendClass::Control);
        self.control(primary, control)?;
        if self.outbox.queued(primary, SendClass::Control) != before {
            self.acked = Some((control, now));
        }
        Ok(())
    }
}

/// The normal replica. Normal scheduling, flow and replay run only in that role.
fn require_normal(driver: &ReplicaDriver) -> Result<&ozzy_replication::NormalReplica, ActorError> {
    driver
        .normal()
        .ok_or_else(|| DriverError::from(ozzy_replication::ReplicationError::NotNormal).into())
}

/// Fail-closed actor error. No variant grants authority to weaken commit policy.
#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    /// Explicit single-broker state rejected a transition.
    #[error(transparent)]
    Local(#[from] ozzy_replication::local::Error),
    /// Nonvoting recovery rejected authority or history evidence.
    #[error(transparent)]
    Recovery(#[from] ozzy_replication::recovery::RecoveryError),
    /// Volatile receipt/repair policy failed; never interpreted as a disk vote.
    #[error(transparent)]
    Flow(#[from] ozzy_replication::flow::TransmitError),
    /// Storage-validated startup belongs to another worker incarnation.
    #[error("replica startup does not belong to this journal worker")]
    StartupMismatch,
    /// Inconsistent or unrepresentable startup bounds.
    #[error("invalid replica actor limits")]
    Limits,
    /// A bounded actor invariant could not be fulfilled by verified history.
    #[error(transparent)]
    History(Box<HistoryFailure>),
    /// Production protocol core rejected an authority or history transition.
    #[error(transparent)]
    Driver(#[from] DriverError),
    /// A real journal action failed or disappeared.
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// Worker admission closed or unexpectedly exhausted its reserved action budget.
    #[error(transparent)]
    Submit(#[from] SubmitError),
    /// An internally generated packet failed wire validation.
    #[error(transparent)]
    Wire(#[from] wire::WireError),
    /// Configured outbox or OMQ transmission failure.
    #[error(transparent)]
    Outbox(#[from] OutboxError),
    /// Invalid internally generated message, not ordinary queue backpressure.
    #[error("invalid replica outgoing message: {0:?}")]
    Enqueue(EnqueueError),
    /// Local socket failure; never counted as evidence about a remote operation.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
}

/// The voter slot a routed packet came from, its binding, and the packet.
/// `None` for an unknown or unbound route.
fn routed<'a>(
    configuration: Configuration,
    bindings: &[Option<PeerBinding>; 3],
    message: &'a Message,
) -> Option<(usize, PeerBinding, [&'a [u8]; 3])> {
    let (route, frames) = message.routed_packet()?;
    let index = configuration
        .voters()
        .iter()
        .position(|voter| voter.as_bytes() == route)?;
    Some((index, bindings[index]?, frames))
}

/// A control packet without payload, its metadata copied out of the actor's
/// scratch buffer.
fn control_message(encoded: wire::ControlEncoding, metadata: &[u8]) -> Message {
    Message::multipart([
        Bytes::copy_from_slice(&encoded.header),
        Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        Bytes::new(),
    ])
}

/// Queue one outgoing message. A full queue drops it: the protocol retains
/// and retries its evidence.
fn offer(
    outbox: &mut ReplicaOutbox,
    to: NodeId,
    class: SendClass,
    message: Message,
) -> Result<(), ActorError> {
    match outbox.try_enqueue(to, class, message) {
        Ok(()) | Err((EnqueueError::Full, _)) => Ok(()),
        Err((error, _)) => Err(ActorError::Enqueue(error)),
    }
}
