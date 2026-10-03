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

mod donors;
mod election;
mod history;
mod ids;
mod maintenance;
pub use ids::ActorIds;
mod ingress;
mod io;
mod local;
pub use local::{LocalActor, LocalActorConfig};
mod native;
mod normal;
pub use native::{
    NativeAccess, NativeIntake, NativeIntakeConfig, NativeIntakeError, NativeReceive,
};
mod reader_cursor;
mod response;
mod shared_readers;
pub use shared_readers::SharedReaderConfig;
mod recovery;
mod scheduled;
pub use scheduled::{ScheduleError, ScheduledReplica};
mod partitions;
pub use partitions::{PartitionActor, PartitionActors, PartitionError, PartitionStatus};
mod routes;
pub use routes::{RoutePublicationError, RoutePublisher};

pub use recovery::{RecoveryActor, RecoveryTiming, ScheduledRecovery};

#[cfg(feature = "simulation")]
pub mod simulation;

pub use ingress::{
    PendingProposal, ProposalOutcome, ProposalReply, ProposalStopped, ProposalSubmitError,
    ProposalSubmitter, UnsubmittedProposal,
};

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
use tokio::sync::watch;

use crate::replica_journal::{
    AppendBuffer, InstallationConfig, JournalError, JournalStartup, ProposalBuffer, ReplicaJournal,
    SubmitError,
};
use crate::replica_transport::{EnqueueError, OutboxError, QueueLimits, ReplicaOutbox, SendClass};
use history::{Lookup, Transfer};
use io::{PendingIo, PendingSync};

/// Maximum operations in one replica packet, independent of live-window capacity.
/// Keeps per-packet descriptor scratch bounded while journal groups can be larger.
pub const MAX_TRANSFER_OPERATIONS: usize = 64;

/// Flush triggers that leave room for later writes in the live pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncBatchTarget {
    /// Flush after this many complete canonical operations or validation attempts.
    pub operations: usize,
    /// Flush after this many complete canonical body bytes.
    pub body_bytes: usize,
}

impl SyncBatchTarget {
    /// Target half the live window, rounded up to at least one operation/byte.
    /// Windows larger than one can leave room for writes during an older barrier.
    /// This is a sizing starting point, not a measured optimum for every device.
    pub fn half_window(pipeline: PipelineLimits) -> Self {
        Self {
            operations: pipeline.max_operations.div_ceil(2).max(1),
            body_bytes: pipeline.max_body_bytes.div_ceil(2).max(1),
        }
    }
}

/// Initial sessions and hard work bounds for one replica actor.
#[derive(Debug, Clone, Copy)]
pub struct ActorConfig {
    /// Session with each voter in configuration order. The local entry is unused.
    /// Each remote entry must match that peer's binding for this connection.
    /// Zero means no established link. It permits startup, never wire admission.
    /// Shared scheduling replaces links through `ScheduledReplica::replace_session`.
    pub sessions: [LinkSessionId; 3],
    /// Protocol timeouts; storage startup happens before the actor clock starts.
    pub timing: Timing,
    /// Small receipt probe pacing, independent of durable/election deadlines.
    pub flow_probe: ozzy_replication::flow::ProbeTiming,
    /// Live core operation/body limits, including staged and validating receives.
    /// Background persistence may span multiple bounded journal commands.
    pub pipeline: PipelineLimits,
    /// Independently bounded applied packets retained for follower catch-up.
    /// Must cover at least one live window. Eviction never waits for followers.
    pub replay_cache: PipelineLimits,
    /// Maximum operations/body bytes in one PREPARE or history-transfer packet.
    /// Both bounds must be nonzero and no larger than the live pipeline. Producer
    /// proposals must fit one packet; backups can coalesce several packets into
    /// a larger journal write and sync group under the live bounds.
    /// Use a common transfer profile across voters; packet limits are not negotiated.
    /// Every retained canonical operation must fit without splitting its body.
    pub transfer: PipelineLimits,
    /// Group-flush triggers, independent of total live pipeline capacity.
    /// Both must be nonzero and no larger than the pipeline. One indivisible
    /// admitted write may cross a trigger, but never the hard pipeline limits.
    /// Backups coalesce staged work up to these targets while an older barrier
    /// runs. Its completion also releases partial groups without a new timer.
    pub sync_batch_target: SyncBatchTarget,
    /// Maximum collection age from the first admitted write in a sync batch.
    /// Must be nonzero. Checked between journal actions on every voter; an
    /// expired batch syncs before accepting more queued work. An empty queue
    /// syncs immediately. This is not a linger delay or an I/O timeout: an
    /// executing write/barrier cannot be interrupted and may exceed this age.
    pub sync_batch_max_age: Duration,
    /// Startup-allocated producer lanes; no runtime sender registration.
    pub proposal_lanes: usize,
    /// Global outstanding proposal count, including queued and quorum-waiting work.
    pub proposal_capacity: usize,
    /// Independently reserved per-peer control queue.
    pub control: QueueLimits,
    /// Independently reserved per-peer bounded history queue.
    pub data: QueueLimits,
    /// Local selected-history staging policy.
    pub installation: InstallationConfig,
}

impl ActorConfig {
    fn wire_limits(self) -> Result<WireLimits, ActorError> {
        let limits =
            WireLimits::for_transfer(self.transfer.max_operations, self.transfer.max_body_bytes)
                .ok_or(ActorError::Limits)?;
        let message_bytes = limits.message_bytes().ok_or(ActorError::Limits)?;
        if self.pipeline.max_operations == 0
            || self.replay_cache.max_operations < self.pipeline.max_operations
            || self.replay_cache.max_operations > 65536
            || self.replay_cache.max_body_bytes < self.pipeline.max_body_bytes
            || u32::try_from(self.replay_cache.max_body_bytes).is_err()
            || self.replay_cache.max_body_bytes > isize::MAX as usize
            || self.transfer.max_operations == 0
            || self.transfer.max_operations > MAX_TRANSFER_OPERATIONS
            || self.transfer.max_operations > self.pipeline.max_operations
            || self.transfer.max_body_bytes == 0
            || self.transfer.max_body_bytes > self.pipeline.max_body_bytes
            || self.sync_batch_target.operations == 0
            || self.sync_batch_target.operations > self.pipeline.max_operations
            || self.sync_batch_target.body_bytes == 0
            || self.sync_batch_target.body_bytes > self.pipeline.max_body_bytes
            || self.sync_batch_max_age.is_zero()
            || self.pipeline.max_body_bytes == 0
            || u32::try_from(self.pipeline.max_body_bytes).is_err()
            || self.pipeline.max_body_bytes > isize::MAX as usize
            || self.data.message_bytes < message_bytes
            || self.control.message_bytes < 300
            || self.proposal_capacity == 0
            || self.proposal_capacity > crate::replica_journal::MAX_APPEND_OPERATIONS
            || self.proposal_lanes == 0
            || self.proposal_lanes > self.proposal_capacity
        {
            return Err(ActorError::Limits);
        }
        Ok(limits)
    }
}

/// Completed device maintenance work. Observability only; never storage authority.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceStatus {
    /// Successfully completed validation commands.
    pub validation_steps: u64,
    /// Captured segment lists fully checked.
    pub validation_cycles: u64,
    /// Segment bytes read by completed validation commands.
    pub validation_read_bytes: u64,
    /// Successfully completed metadata cleanup commands.
    pub metadata_steps: u64,
    /// Successfully completed orphan cleanup commands.
    pub orphan_steps: u64,
    /// File bytes removed by completed cleanup commands.
    pub reclaimed_bytes: u64,
}

impl MaintenanceStatus {
    fn observe_reclaimed(&mut self, bytes: u64) {
        self.reclaimed_bytes = self.reclaimed_bytes.saturating_add(bytes);
    }

    fn observe_validation(&mut self, step: ozzy_journal_segment::StorageValidationStep) {
        self.validation_steps = self.validation_steps.saturating_add(1);
        self.validation_cycles = self
            .validation_cycles
            .saturating_add(u64::from(step.remaining_segments == 0));
        self.validation_read_bytes = self
            .validation_read_bytes
            .saturating_add(step.checked_bytes);
    }
}

/// Observability only. A snapshot is never permission to write or emit an ACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorStatus {
    /// Cumulative completed maintenance work since actor creation.
    pub maintenance: MaintenanceStatus,
    /// Latest proposed view, including uninstalled election state.
    pub scope: Scope,
    /// Current normal-core evidence, absent during view change or installation.
    pub normal: Option<ReplicaSnapshot>,
    /// Whether a real journal action is outstanding.
    pub disk_pending: bool,
    /// Core readiness plus successful activation of this exact worker generation.
    pub application_ready: bool,
    /// True while the actor loop is running. A closed watch is also terminal.
    pub running: bool,
}

/// One mutable protocol owner with journal futures polled on its shard.
#[derive(Debug)]
pub struct ReplicaActor {
    maintenance_status: MaintenanceStatus,
    foreground_turn_due: bool,
    orphan_cleanup: Option<(Duration, Duration, ozzy_journal_segment::MaintenanceBudget)>,
    metadata_cleanup: Option<(Duration, Duration, ozzy_journal_segment::MaintenanceBudget)>,
    storage_validation: Option<(
        Duration,
        Duration,
        ozzy_journal_segment::StorageValidationBudget,
    )>,
    donors: Option<Box<donors::Donors>>,
    configuration: Configuration,
    local: NodeId,
    config: ActorConfig,
    ready_work: std::sync::Arc<crate::signal::DataSignal>,
    incoming: VecDeque<Message>,
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
    status: watch::Sender<ActorStatus>,
    ingress: ingress::Ingress,
    submitters: Vec<ProposalSubmitter>,
    work: normal::Work,
}

impl ReplicaActor {
    /// Consume storage-validated startup and reserve actor buffers before serving.
    /// The journal must have two available append-buffer leases at these bounds:
    /// one for journal/transfer work and one for contiguous backup receive staging.
    /// At least two command slots must be available for write/sync overlap.
    pub fn new(
        journal: ReplicaJournal,
        startup: JournalStartup,
        config: ActorConfig,
    ) -> Result<Self, ActorError> {
        Self::new_with_ids(journal, startup, config, ActorIds::random())
    }

    /// Construct with an explicit protocol ID source. The simulator must give
    /// each actor incarnation its own namespace, including after restart.
    pub fn new_with_ids(
        journal: ReplicaJournal,
        startup: JournalStartup,
        config: ActorConfig,
        ids: ActorIds,
    ) -> Result<Self, ActorError> {
        Self::construct(journal, startup, &config, ids)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "explicit bounded actor state initialization"
    )]
    fn construct(
        journal: ReplicaJournal,
        startup: JournalStartup,
        config: &ActorConfig,
        mut ids: ActorIds,
    ) -> Result<Self, ActorError> {
        let configuration = startup.configuration();
        let local = startup.local();
        let journal_scope = startup
            .recovered()
            .map_or(configuration.scope(), |state| state.scope);
        let source = LogSource {
            voter: local,
            generation: startup.generation(),
            accepted: startup
                .recovered()
                .map_or(Prefix::GENESIS, |state| state.log.accepted),
        };
        let activated = startup
            .recovered()
            .is_none()
            .then_some((journal_scope, startup.generation()));
        let wire_limits = config.wire_limits()?;
        if journal.command_capacity() < 2 {
            return Err(ActorError::Limits);
        }
        let backlog = journal.backlog();
        if config.pipeline.max_operations > backlog.max_operations
            || config.pipeline.max_body_bytes > backlog.max_body_bytes
        {
            return Err(ActorError::Limits);
        }
        let buffer = journal.lease_append_buffer()?;
        let history_allocator = buffer.allocator();
        if buffer.owner_generation() != startup.generation() {
            return Err(ActorError::StartupMismatch);
        }
        let required = if journal.replicated() {
            config.transfer
        } else {
            config.pipeline
        };
        if required.max_operations > buffer.limits().max_operations
            || required.max_body_bytes > buffer.limits().max_body_bytes
        {
            return Err(ActorError::Limits);
        }
        let driver = startup.into_driver(Duration::ZERO, config.timing, config.pipeline)?;
        let (ingress, submitters) = ingress::Ingress::new(
            config.proposal_lanes,
            config.proposal_capacity,
            buffer.owner_generation(),
            config.transfer,
        );
        let receive_buffer = journal.lease_append_buffer()?;
        let flow = normal::Flow::new(
            configuration,
            local,
            driver.scope(),
            activated,
            config,
            &mut ids,
        )?;
        let work = normal::Work::new(
            config.proposal_capacity,
            ids.channel(driver.scope())?,
            receive_buffer,
            config.pipeline,
            config.replay_cache,
            flow,
        )?;
        let outbox = ReplicaOutbox::new(configuration, local, config.control, config.data)?;
        let mut bindings = [None; 3];
        for (index, &peer) in configuration.voters().iter().enumerate() {
            if peer != local && config.sessions[index].as_bytes() != &[0; 16] {
                bindings[index] = Some(
                    PeerBinding::new(configuration, peer, config.sessions[index])?
                        .with_receive_epoch(work.receive_epoch()),
                );
            }
        }
        let (status, _) = watch::channel(ActorStatus {
            maintenance: MaintenanceStatus::default(),
            scope: driver.scope(),
            normal: driver.normal().map(NormalReplica::snapshot),
            disk_pending: false,
            application_ready: false,
            running: false,
        });
        let wanted_pin = Some((driver.scope(), source));
        Ok(Self {
            maintenance_status: MaintenanceStatus::default(),
            storage_validation: None,
            metadata_cleanup: None,
            orphan_cleanup: None,
            foreground_turn_due: false,
            donors: None,
            ready_work: std::sync::Arc::new(crate::signal::DataSignal::default()),
            incoming: VecDeque::with_capacity(1024),
            configuration,
            local,
            config: *config,
            driver,
            ids,
            journal,
            outbox,
            bindings,
            wire_limits,
            metadata: vec![0; wire_limits.envelope.max_metadata_bytes],
            payload: vec![0; config.transfer.max_body_bytes],
            buffer: Some(buffer),
            history_allocator,
            history_receive_allocator: None,
            spare: None,
            pending: None,
            pending_replay: None,
            pending_sync: None,
            pending_persistence: VecDeque::with_capacity(config.pipeline.max_operations),
            promise: None,
            pinned: None,
            wanted_pin,
            start: None,
            lookup: Lookup::default(),
            transfer: None,
            staged: None,
            journal_scope,
            activated,
            ack_at: Duration::ZERO,
            status,
            ingress,
            submitters,
            work,
        })
    }

    /// Subscribe to bounded, coalesced state observations. Intermediate states may coalesce.
    pub fn subscribe(&self) -> watch::Receiver<ActorStatus> {
        self.status.subscribe()
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
        // Canceled waits preserve readiness. Consume it before this turn so
        // work marked during bounded draining stays ready for the next turn.
        self.ready_work.drain(|| ());
        self.drain_incoming(now)?;
        self.ingress.begin_round();
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
        if !self.incoming.is_empty() {
            self.ready_work.mark();
        }
        self.publish_status(true);
        Ok(())
    }

    fn drain_incoming(&mut self, now: Duration) -> Result<(), ActorError> {
        let started = std::time::Instant::now();
        let mut bytes = 0;
        while let Some(message) = self.incoming.pop_front() {
            bytes += message.byte_len();
            self.receive(&message, now)?;
            if bytes >= 2 * 1024 * 1024 || started.elapsed() >= Duration::from_millis(1) {
                break;
            }
        }
        Ok(())
    }

    fn publish_status(&self, running: bool) {
        let next = ActorStatus {
            maintenance: self.maintenance_status,
            scope: self.driver.scope(),
            normal: self.driver.normal().map(NormalReplica::snapshot),
            disk_pending: self.pending.is_some()
                || self.pending_replay.is_some()
                || self.pending_sync.is_some()
                || !self.pending_persistence.is_empty(),
            application_ready: running && self.application_ready(),
            running,
        };
        self.status.send_if_modified(|old| {
            if *old == next {
                false
            } else {
                *old = next;
                true
            }
        });
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
        if let Some(source) = source {
            self.wanted_pin = Some((control.scope(), source));
            if self.pinned != Some(source) {
                return Ok(());
            }
        }
        let encoded = wire::encode_control(self.local, session, control, &mut self.metadata)?;
        let message = Message::multipart([
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
            Bytes::new(),
        ]);
        self.enqueue(to, SendClass::Control, message)
    }

    fn enqueue(
        &mut self,
        to: NodeId,
        class: SendClass,
        message: Message,
    ) -> Result<(), ActorError> {
        match self.outbox.try_enqueue(to, class, message) {
            Ok(()) | Err((EnqueueError::Full, _)) => Ok(()), // Protocol retains/retries evidence.
            Err((error, _)) => Err(ActorError::Enqueue(error)),
        }
    }

    fn session(&self, to: NodeId) -> Option<LinkSessionId> {
        let index = self
            .configuration
            .voters()
            .iter()
            .position(|peer| *peer == to)
            .expect("configured destination");
        self.bindings[index].map(|_| self.config.sessions[index])
    }

    fn receive(&mut self, message: &Message, now: Duration) -> Result<(), ActorError> {
        if message.len() == 3 {
            let Some(peer) = message
                .part_slice(0)
                .and_then(|p| <[u8; 16]>::try_from(p).ok())
                .map(NodeId::from_bytes)
            else {
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
            if self.bindings[voter].is_none()
                || message.part_slice(1) != Some(self.driver.scope().group_id.as_bytes().as_slice())
            {
                return Ok(());
            }
            let Ok(compact) = wire::CompactState::decode(message.part_slice(2).unwrap_or_default())
            else {
                return Ok(());
            };
            return self.receive_compact(voter, compact, now);
        }
        if message.len() != 4 {
            return Ok(());
        }
        let route = message.part_bytes(0).expect("checked part count");
        let Some(index) = self
            .configuration
            .voters()
            .iter()
            .position(|peer| peer.as_bytes().as_slice() == route.as_ref())
        else {
            return Ok(());
        };
        let Some(binding) = self.bindings[index] else {
            return Ok(());
        };
        let from = self.configuration.voters()[index];
        let frames: [Bytes; 3] =
            std::array::from_fn(|index| message.part_bytes(index + 1).expect("checked part count"));
        let frames = frames.each_ref().map(AsRef::as_ref);
        let Ok(decoded) = wire::decode(&frames, binding, self.wire_limits) else {
            return Ok(());
        };
        match decoded {
            ReplicaMessage::Flow(flow) => self.receive_flow(index, flow, now)?,
            ReplicaMessage::Control(control) => {
                self.receive_control(from, control, now)?;
                if matches!(control, Control::StartView(_)) {
                    self.ack_at = now;
                }
            }
            ReplicaMessage::FetchOps(request) => self.serve(from, request)?,
            ReplicaMessage::Ops(batch) => self.receive_ops(&batch)?,
            ReplicaMessage::Recovery(wire::RecoveryMessage::Request(request)) => {
                self.receive_recovery(index, request)?;
            }
            ReplicaMessage::Recovery(wire::RecoveryMessage::State(_))
            | ReplicaMessage::Prepare(_) => {}
        }
        Ok(())
    }

    fn receive_data(&mut self, message: &Message, now: Duration) -> Result<bool, ActorError> {
        if message.len() != 4 {
            return Ok(true);
        }
        let Some(voter) = self
            .configuration
            .voters()
            .iter()
            .position(|peer| message.part_slice(0) == Some(peer.as_bytes().as_slice()))
        else {
            return Ok(true);
        };
        let Some(binding) = self.bindings[voter] else {
            return Ok(true);
        };
        let frames: [&[u8]; 3] =
            std::array::from_fn(|i| message.part_slice(i + 1).expect("four frames"));
        let Ok(packet) = ozzy_proto::decode_packet(&frames, self.wire_limits.envelope) else {
            return Ok(true);
        };
        let scope = self.driver.scope();
        let (batch, publication) = if packet.envelope.opcode == ozzy_proto::Opcode::PreparePub {
            let published = [
                scope.group_id.as_bytes().as_slice(),
                frames[0],
                frames[1],
                frames[2],
            ];
            let Ok(batch) =
                wire::decode_publication(&published, binding, self.local, scope, self.wire_limits)
            else {
                return Ok(true);
            };
            crate::profiling::event(crate::profiling::Event::ReplicaPublicationReceived);
            (batch, true)
        } else {
            let Ok(ReplicaMessage::Flow(wire::FlowMessage::Prepare { batch, .. })) =
                wire::decode(&frames, binding, self.wire_limits)
            else {
                return Ok(true);
            };
            (batch, false)
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

    fn ack(&mut self, now: Duration) -> Result<(), ActorError> {
        if now < self.ack_at {
            return Ok(());
        }
        self.ack_at = now + self.config.timing.retransmit;
        let primary = self.configuration.primary(self.driver.scope().view);
        if primary == self.local {
            return Ok(());
        }
        let Some(normal) = self.driver.normal() else {
            return Ok(());
        };
        let control = match self.configuration.policy() {
            ozzy_replication::QuorumPolicy::Durable => {
                let Ok(ack) = normal.acknowledgment() else {
                    return Ok(());
                };
                Control::PrepareOk { ack }
            }
            ozzy_replication::QuorumPolicy::Replicated => {
                let Ok(ack) = normal.retained_acknowledgment() else {
                    return Ok(());
                };
                Control::PrepareRetained { ack }
            }
        };
        self.control(primary, control)
    }
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
    #[error("replica history does not satisfy selected authority")]
    History,
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
