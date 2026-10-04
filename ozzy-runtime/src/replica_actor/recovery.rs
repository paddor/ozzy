//! Nonvoting full-WAL recovery on a shared application shard.

mod scheduled;
#[cfg(feature = "simulation")]
pub(super) mod simulation;
pub use scheduled::ScheduledRecovery;

use super::{
    ActorConfig, ActorError, ActorIds, ActorStatus, AppendBuffer, Bytes, Configuration, Duration,
    EnqueueError, Message, NodeId, PeerBinding, Prefix, ReplicaActor, ReplicaMessage,
    ReplicaOutbox, Scope, SendClass, WireLimits, watch, wire,
};
use crate::replica_journal::{
    JournalCompletion, OwnedRecoveryGenerations, PublishedRecovery, ReceivedChunk, RecoveryPlan,
    RecoveryStartup, RecoveryStorage, ShardRecoveringJournal,
};
use ozzy_replication::recovery::{Recovery, RecoveryError, RecoveryTicket};
use ozzy_replication::wire::{Control, FetchOps, RecoveryMessage, RecoveryRequest};

/// Recovery request pacing, independent of normal PREPARE/receipt timers.
#[derive(Debug, Clone, Copy)]
pub struct RecoveryTiming {
    /// Initial retry interval for one unanswered metadata or chunk request.
    pub initial: Duration,
    /// Exponential retry ceiling. Completed chunks reset their own backoff.
    pub maximum: Duration,
    /// No validated progress for this long abandons the attempt after admitted I/O settles.
    pub stalled: Duration,
}

impl Default for RecoveryTiming {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            maximum: Duration::from_secs(1),
            stalled: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Retry {
    at: Duration,
    delay: Duration,
}
impl Retry {
    fn new(timing: RecoveryTiming) -> Self {
        Self {
            at: Duration::ZERO,
            delay: timing.initial,
        }
    }
    fn sent(&mut self, now: Duration, timing: RecoveryTiming) {
        self.at = now.saturating_add(self.delay);
        self.delay = self.delay.saturating_mul(2).min(timing.maximum);
    }
}

#[derive(Debug)]
enum Pending {
    Checkpoint(JournalCompletion<crate::replica_journal::CheckpointProgress>),
    Begin(JournalCompletion<RecoveryPlan>),
    Chunk(JournalCompletion<ReceivedChunk>),
    Finish(JournalCompletion<PublishedRecovery>),
    Abort(JournalCompletion<RecoveryTicket>),
}

#[derive(Debug)]
enum Completed {
    Checkpoint(crate::replica_journal::CheckpointProgress),
    Begin(RecoveryPlan),
    Chunk(Box<ReceivedChunk>),
    Finish(Box<PublishedRecovery>),
    Abort,
}
impl Pending {
    async fn wait(&mut self) -> Result<Completed, ActorError> {
        Ok(match self {
            Self::Checkpoint(future) => Completed::Checkpoint(future.await?),
            Self::Begin(future) => Completed::Begin(future.await?),
            Self::Chunk(future) => Completed::Chunk(Box::new(future.await?)),
            Self::Finish(future) => Completed::Finish(Box::new(future.await?)),
            Self::Abort(future) => {
                future.await?;
                Completed::Abort
            }
        })
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "one bounded publication per cold recovery attempt"
)]
#[derive(Debug)]
enum Outcome {
    Published(PublishedRecovery),
    Restart,
}

enum Transition<J: RecoveryStorage> {
    Retry(Box<RecoveryActor<J>>),
    Normal(Box<ReplicaActor>),
}

/// Explicit replacement-voter role. Owns no normal core or producer admission.
///
/// Peers must opt into `ReplicaActor::enable_recovery` and use independently
/// authenticated bindings. The caller explicitly creates/opens the replacement;
/// this actor never formats a missing path or overwrites an existing voter.
#[derive(Debug)]
pub struct RecoveryActor<J: RecoveryStorage = ShardRecoveringJournal> {
    configuration: Configuration,
    local: NodeId,
    config: ActorConfig,
    timing: RecoveryTiming,
    journal: Option<J>,
    ids: ActorIds,
    recovery: Recovery,
    scope: Scope,
    observed: Option<(NodeId, Scope)>,
    requests: [RecoveryRequest; 3],
    replies: [Option<u64>; 3],
    retries: [Retry; 3],
    ticket: Option<RecoveryTicket>,
    staged: Option<Prefix>,
    plan: RecoveryPlan,
    fetch: Option<(FetchOps, Retry)>,
    checkpoint_fetch: Option<(wire::CheckpointRequest, Retry)>,
    checkpoint_through: u64,
    checkpoint_ready: bool,
    pending: Option<Pending>,
    abandoning: bool,
    full_retry: bool,
    progress_at: Duration,
    buffer: Option<AppendBuffer>,
    metadata: Vec<u8>,
    bindings: [Option<PeerBinding>; 3],
    wire_limits: WireLimits,
    outbox: ReplicaOutbox,
    status: watch::Sender<ActorStatus>,
}

impl<J: RecoveryStorage> RecoveryActor<J> {
    /// Reserve bounded transfer resources and generate a fresh recovery nonce.
    /// The actor's transfer bounds must fit the worker's leased arena.
    pub fn new(
        journal: J,
        startup: RecoveryStartup,
        config: ActorConfig,
        timing: RecoveryTiming,
    ) -> Result<Self, ActorError> {
        Self::new_with_ids(journal, startup, config, timing, ActorIds::random())
    }

    /// Use explicit IDs for recovery attempts, requests, and later journal
    /// generations. Retry and normal handoff retain this same source.
    pub fn new_with_ids(
        journal: J,
        startup: RecoveryStartup,
        config: ActorConfig,
        timing: RecoveryTiming,
        mut ids: ActorIds,
    ) -> Result<Self, ActorError> {
        if timing.initial.is_zero()
            || timing.maximum < timing.initial
            || timing.stalled <= timing.maximum
        {
            return Err(ActorError::Limits);
        }
        let configuration = startup.configuration();
        let local = startup.local();
        let scope = configuration.scope();
        let buffer = journal.lease_append_buffer()?;
        if buffer.owner_generation() != startup.generation() {
            return Err(ActorError::StartupMismatch);
        }
        if config.transfer.max_operations > buffer.limits().max_operations
            || config.transfer.max_body_bytes > buffer.limits().max_body_bytes
        {
            return Err(ActorError::Limits);
        }
        let wire_limits = config.wire_limits()?;
        let nonce = ids.request()?;
        let recovery = startup.into_recovery(nonce, config.transfer)?;
        let outbox = ReplicaOutbox::new(configuration, local, config.control, config.data)?;
        let mut bindings = [None; 3];
        for (index, &peer) in configuration.voters().iter().enumerate() {
            if peer != local && config.sessions[index].as_bytes() != &[0; 16] {
                bindings[index] = Some(PeerBinding::new(
                    configuration,
                    peer,
                    config.sessions[index],
                )?);
            }
        }
        let (status, _) = watch::channel(ActorStatus {
            maintenance: super::MaintenanceStatus::default(),
            scope,
            normal: None,
            disk_pending: false,
            application_ready: false,
            running: false,
        });
        let requests =
            [ids.request()?, ids.request()?, ids.request()?].map(|request_id| RecoveryRequest {
                scope,
                request_id,
                nonce,
            });
        Ok(Self {
            configuration,
            local,
            config,
            timing,
            journal: Some(journal),
            ids,
            recovery,
            scope,
            observed: None,
            requests,
            replies: [None; 3],
            retries: [Retry::new(timing); 3],
            ticket: None,
            staged: None,
            plan: RecoveryPlan::Full,
            fetch: None,
            checkpoint_fetch: None,
            checkpoint_through: 0,
            checkpoint_ready: false,
            pending: None,
            abandoning: false,
            full_retry: false,
            progress_at: Duration::ZERO,
            buffer: Some(buffer),
            metadata: vec![0; wire_limits.envelope.max_metadata_bytes],
            bindings,
            wire_limits,
            outbox,
            status,
        })
    }

    /// Observe recovery and the succeeding normal actor through the same watch channel.
    /// No observation grants authority; normal/ready remain absent throughout transfer.
    pub fn subscribe(&self) -> watch::Receiver<ActorStatus> {
        self.status.subscribe()
    }

    // The OMQ loop and controlled event driver use exactly the same lifecycle.
    // Owning self keeps cancellation from leaving a resumable half-handoff.
    async fn transition(mut self, outcome: Outcome) -> Result<Transition<J>, ActorError> {
        let journal = self.journal.take().expect("owned recovery journal");
        self.buffer = None;
        match outcome {
            Outcome::Published(publication) => {
                let generation = self.ids.generation()?;
                let (journal, startup) = journal
                    .adopt(&mut self.recovery, publication, self.abandoning, generation)
                    .await?;
                let mut actor =
                    ReplicaActor::new_with_ids(journal, startup, self.config, self.ids)?;
                actor.enable_recovery();
                actor.status = self.status;
                Ok(Transition::Normal(Box::new(actor)))
            }
            Outcome::Restart => {
                let generations = OwnedRecoveryGenerations {
                    attempt: self.ids.generation()?,
                    temporary: self.ids.generation()?,
                };
                let (journal, startup) = journal.restart(self.full_retry, generations).await?;
                let mut next =
                    Self::new_with_ids(journal, startup, self.config, self.timing, self.ids)?;
                if let Some((from, scope)) = self.observed {
                    next.observe(from, scope)?;
                }
                next.status = self.status;
                Ok(Transition::Retry(Box::new(next)))
            }
        }
    }

    async fn shutdown(mut self) -> Result<(), ActorError> {
        self.publish(false);
        self.buffer = None;
        self.journal
            .take()
            .expect("owned recovery journal")
            .shutdown()
            .await?;
        Ok(())
    }

    fn publish(&self, running: bool) {
        let next = ActorStatus {
            maintenance: super::MaintenanceStatus::default(),
            scope: self.scope,
            normal: None,
            disk_pending: self.pending.is_some(),
            application_ready: false,
            running,
        };
        self.status.send_if_modified(|old| {
            let mut next = next;
            // Recovery requests may start with a lower view hint. Routing must
            // keep its last observed fence while this copy remains nonvoting.
            if old.scope.view > next.scope.view {
                next.scope = old.scope;
            }
            if *old == next {
                false
            } else {
                *old = next;
                true
            }
        });
    }

    fn observe(&mut self, from: NodeId, scope: Scope) -> Result<(), ActorError> {
        self.recovery.observe_view(from, scope)?;
        if scope.view > self.scope.view {
            self.scope = scope;
            self.observed = Some((from, scope));
            for (request, retry) in self.requests.iter_mut().zip(&mut self.retries) {
                request.scope = scope;
                *retry = Retry::new(self.timing);
            }
            if self
                .ticket
                .is_some_and(|ticket| ticket.scope().view < scope.view)
            {
                self.abandoning = true;
                self.fetch = None;
            }
        }
        Ok(())
    }

    fn advance(&mut self, now: Duration) -> Result<Option<Outcome>, ActorError> {
        if now.saturating_sub(self.progress_at) >= self.timing.stalled {
            self.abandoning = true;
            self.fetch = None;
            // A damaged sealed range can belong to an abandoned, uncommitted
            // branch that the current donor cannot serve. Retry the existing
            // full recovery protocol with fresh authority, never loop forever
            // insisting on that old range.
            self.full_retry |= matches!(self.plan, RecoveryPlan::Repair(_));
        }
        if self.pending.is_some() {
            return Ok(None);
        }
        let journal = self.journal.as_mut().expect("owned recovery journal");
        if self.abandoning {
            if let Some(ticket) = self.ticket.filter(|_| self.staged.is_some()) {
                self.pending = Some(Pending::Abort(journal.abort_recovery(ticket)?));
                return Ok(None);
            }
            return Ok(Some(Outcome::Restart));
        }
        if self.ticket.is_none() {
            match self.recovery.begin_transfer() {
                Ok(ticket) => {
                    self.ticket = Some(ticket);
                    self.pending = Some(Pending::Begin(
                        journal.begin_recovery(ticket, self.config.installation)?,
                    ));
                    return Ok(None);
                }
                Err(RecoveryError::QuorumMissing | RecoveryError::PrimaryMissing) => {}
                Err(error) => return Err(error.into()),
            }
            self.request_quorum(now)?;
        } else if let (Some(ticket), Some(staged)) = (self.ticket, self.staged) {
            if let Some(anchor) = ticket.checkpoint().filter(|_| !self.checkpoint_ready) {
                if self.checkpoint_fetch.is_none() {
                    self.checkpoint_fetch = Some((
                        wire::CheckpointRequest {
                            scope: ticket.scope(),
                            request_id: self.ids.request()?,
                            nonce: ticket.nonce(),
                            source: ticket.source(),
                            offset: self.checkpoint_through,
                            max_bytes: self
                                .config
                                .transfer
                                .max_body_bytes
                                .min(anchor.chunk_bytes as usize)
                                as u32,
                        },
                        Retry::new(self.timing),
                    ));
                }
                self.request_checkpoint(now)?;
                return Ok(None);
            }
            if matches!(self.plan, RecoveryPlan::Repair(None))
                || (self.plan == RecoveryPlan::Full && staged == ticket.source().accepted)
            {
                self.pending = Some(Pending::Finish(journal.finish_recovery(ticket)?));
            } else {
                if self.fetch.is_none() {
                    self.fetch = Some((
                        FetchOps {
                            scope: ticket.scope(),
                            request_id: self.ids.request()?,
                            source: ticket.source(),
                            predecessor: staged,
                            max_operations: match self.plan {
                                RecoveryPlan::Repair(Some(range)) => {
                                    self.config.transfer.max_operations.min(
                                        (range.through.op_number - range.after.op_number) as usize,
                                    ) as u32
                                }
                                _ => self.config.transfer.max_operations as u32,
                            },
                            max_body_bytes: self.config.transfer.max_body_bytes as u32,
                        },
                        Retry::new(self.timing),
                    ));
                }
                self.request_chunk(now)?;
            }
        }
        Ok(None)
    }

    fn request_quorum(&mut self, now: Duration) -> Result<(), ActorError> {
        for index in 0..3 {
            let to = self.configuration.voters()[index];
            if to != self.local
                && self.bindings[index].is_some()
                && self.replies[index] != Some(self.scope.view)
                && now >= self.retries[index].at
            {
                let encoded = wire::encode_recovery(
                    self.local,
                    self.config.sessions[index],
                    self.requests[index],
                    &mut self.metadata,
                    self.wire_limits,
                )?;
                let message = Message::multipart([
                    Bytes::copy_from_slice(&encoded.header),
                    Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
                    Bytes::new(),
                ]);
                self.enqueue(to, SendClass::Control, message)?;
                self.retries[index].sent(now, self.timing);
            }
        }
        Ok(())
    }

    fn request_chunk(&mut self, now: Duration) -> Result<(), ActorError> {
        let (request, retry) = self.fetch.as_mut().expect("pending chunk request");
        if now < retry.at {
            return Ok(());
        }
        let request = *request;
        retry.sent(now, self.timing);
        let index = self
            .configuration
            .voters()
            .iter()
            .position(|voter| *voter == request.source.voter)
            .expect("configured donor");
        if self.bindings[index].is_none() {
            return Ok(());
        }
        let encoded = wire::encode_fetch(
            self.local,
            self.config.sessions[index],
            request,
            &mut self.metadata,
            self.wire_limits,
        )?;
        let message = Message::multipart([
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
            Bytes::new(),
        ]);
        self.enqueue(request.source.voter, SendClass::Data, message)
    }

    fn enqueue(
        &mut self,
        to: NodeId,
        class: SendClass,
        message: Message,
    ) -> Result<(), ActorError> {
        match self.outbox.try_enqueue(to, class, message) {
            Ok(()) | Err((EnqueueError::Full, _)) => Ok(()),
            Err((error, _)) => Err(ActorError::Enqueue(error)),
        }
    }

    fn receive(&mut self, message: &Message, now: Duration) -> Result<(), ActorError> {
        if message.len() != 4 {
            return Ok(());
        }
        let route = message.part_bytes(0).expect("checked part count");
        let Some(index) = self
            .configuration
            .voters()
            .iter()
            .position(|voter| voter.as_bytes().as_slice() == route.as_ref())
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
            ReplicaMessage::Recovery(RecoveryMessage::State(state)) => {
                if state.validate_response(self.requests[index]).is_err() {
                    return Ok(());
                }
                self.recovery.receive(from, state.response)?;
                self.observe(from, state.response.scope)?;
                if self.replies[index].is_none_or(|view| state.response.scope.view > view) {
                    self.progress_at = now;
                    self.replies[index] = Some(state.response.scope.view);
                }
            }
            ReplicaMessage::Control(control) => {
                let scope = control.scope();
                let primary = self.configuration.primary(scope.view);
                let authoritative = match control {
                    // Volatile suspicion cannot supersede a recovery attempt.
                    Control::ExitView(_) => false,
                    Control::Commit(_) | Control::StartView(_) => from == primary,
                    Control::PrepareOk { .. }
                    | Control::PrepareRetained { .. }
                    | Control::DoViewChange(_) => self.local == primary,
                    Control::StartViewChange(_) => true,
                };
                if authoritative {
                    self.observe(from, scope)?;
                }
            }
            ReplicaMessage::Checkpoint(wire::CheckpointMessage::Chunk { request, .. }) => {
                self.receive_checkpoint(
                    request,
                    message.part_bytes(3).ok_or(ActorError::History)?,
                )?;
            }
            ReplicaMessage::Ops(batch) => {
                let Some((request, _)) = self.fetch else {
                    return Ok(());
                };
                if self.abandoning
                    || self.pending.is_some()
                    || batch.validate_response(request).is_err()
                {
                    return Ok(());
                }
                let ticket = self.ticket.ok_or(ActorError::History)?;
                let bytes = batch.operations().map(|op| op.canonical().body.len()).sum();
                if !self
                    .buffer
                    .as_mut()
                    .ok_or(ActorError::History)?
                    .reserve_incoming(bytes)?
                {
                    return Ok(()); // Preserve fetch correlation and retry without partial staging.
                }
                let mut buffer = self.buffer.take().ok_or(ActorError::History)?;
                for operation in batch.operations() {
                    buffer.push(operation.canonical())?;
                }
                self.fetch = None;
                self.pending = Some(Pending::Chunk(
                    self.journal
                        .as_mut()
                        .expect("owned journal")
                        .receive_chunk(ticket, buffer)
                        .map_err(|rejected| rejected.reason)?,
                ));
            }
            _ => {} // Never answer normal votes, flow credits, or recovery requests.
        }
        Ok(())
    }

    fn receive_checkpoint(
        &mut self,
        request: wire::CheckpointRequest,
        payload: Bytes,
    ) -> Result<(), ActorError> {
        if self.abandoning
            || self.pending.is_some()
            || self
                .checkpoint_fetch
                .is_none_or(|(outstanding, _)| outstanding != request)
        {
            return Ok(());
        }
        let ticket = self.ticket.ok_or(ActorError::History)?;
        let completion = match self
            .journal
            .as_mut()
            .expect("owned journal")
            .receive_checkpoint(ticket, request.offset, payload)
        {
            Ok(completion) => completion,
            Err(crate::replica_journal::SubmitError::Full) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        self.pending = Some(Pending::Checkpoint(completion));
        self.checkpoint_fetch = None;
        Ok(())
    }

    fn complete(
        &mut self,
        completed: Completed,
        now: Duration,
    ) -> Result<Option<Outcome>, ActorError> {
        match completed {
            Completed::Checkpoint(progress) => {
                let ticket = self.ticket.ok_or(ActorError::History)?;
                self.checkpoint_through = progress.through;
                if !self.abandoning
                    && let Some(revision) = progress.revision
                {
                    self.recovery.complete_checkpoint(
                        ticket,
                        ticket.checkpoint().ok_or(ActorError::History)?,
                        revision,
                    )?;
                    self.checkpoint_ready = true;
                }
                self.progress_at = now;
            }
            Completed::Begin(plan) => {
                self.plan = plan;
                self.staged = Some(match plan {
                    RecoveryPlan::Repair(Some(range)) => Prefix {
                        op: ozzy_replication::OpNumber(range.after.op_number),
                        digest: range.after.digest,
                    },
                    _ => self
                        .ticket
                        .and_then(RecoveryTicket::checkpoint)
                        .map_or(Prefix::GENESIS, |anchor| anchor.predecessor),
                });
            }
            Completed::Chunk(chunk) => {
                let chunk = *chunk;
                if !self.abandoning && self.plan == RecoveryPlan::Full {
                    self.recovery
                        .validate_chunk(chunk.ticket(), chunk.prepared())?;
                }
                self.plan = chunk.plan();
                if self.plan == RecoveryPlan::RetryFull {
                    self.abandoning = true;
                    self.full_retry = true;
                }
                self.staged = Some(match self.plan {
                    RecoveryPlan::Repair(Some(range)) => Prefix {
                        op: ozzy_replication::OpNumber(range.after.op_number),
                        digest: range.after.digest,
                    },
                    _ => chunk.end(),
                });
                let mut buffer = chunk.into_buffer();
                buffer.clear();
                self.buffer = Some(buffer);
                self.progress_at = now;
            }
            Completed::Finish(publication) => return Ok(Some(Outcome::Published(*publication))),
            Completed::Abort => return Ok(Some(Outcome::Restart)),
        }
        Ok(None)
    }
}

impl<J: RecoveryStorage> RecoveryActor<J> {
    fn request_checkpoint(&mut self, now: Duration) -> Result<(), ActorError> {
        let (request, retry) = self
            .checkpoint_fetch
            .as_mut()
            .expect("pending checkpoint range");
        if now < retry.at {
            return Ok(());
        }
        let request = *request;
        retry.sent(now, self.timing);
        let index = self
            .configuration
            .voters()
            .iter()
            .position(|&voter| voter == request.source.voter)
            .expect("configured donor");
        if self.bindings[index].is_none() {
            return Ok(());
        }
        let encoded = wire::encode_checkpoint(
            self.local,
            self.config.sessions[index],
            wire::CheckpointMessage::Request(request),
            &mut self.metadata,
            self.wire_limits,
        )?;
        self.enqueue(
            request.source.voter,
            SendClass::Control,
            Message::multipart([
                Bytes::copy_from_slice(&encoded.header),
                Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
                Bytes::new(),
            ]),
        )?;
        Ok(())
    }
}
