//! Nonvoting full-WAL recovery on a shared application shard.

use crate::replica_actor::HistoryReason;
mod deadline;
mod receive;
mod requests;
mod scheduled;
#[cfg(feature = "simulation")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "simulation test support"
)]
pub(super) mod simulation;
pub use scheduled::ScheduledRecovery;

use super::{
    ActorConfig, ActorError, ActorIds, ActorStatus, AppendBuffer, Configuration, Duration, NodeId,
    PeerBinding, Prefix, ReplicaActor, ReplicaOutbox, Scope, SharedStatus, WireLimits, wire,
};
use crate::replica_journal::{
    JournalCompletion, OwnedRecoveryGenerations, PublishedRecovery, ReceivedChunk, RecoveryPlan,
    RecoveryStartup, RecoveryStorage, ShardRecoveringJournal,
};
use ozzy_replication::recovery::{Recovery, RecoveryError, RecoveryTicket};
use ozzy_replication::wire::{FetchOps, RecoveryRequest};
use std::cell::Cell;

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

impl RecoveryTiming {
    /// Retries back off from a nonzero start and give up only after their
    /// ceiling.
    fn validate(self) -> Result<Self, ActorError> {
        if self.initial.is_zero() || self.maximum < self.initial || self.stalled <= self.maximum {
            return Err(ActorError::Limits);
        }
        Ok(self)
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
    journal: J,
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
    status: SharedStatus,
}

impl<J: RecoveryStorage> RecoveryActor<J> {
    /// Reserve bounded transfer resources and generate a fresh recovery nonce.
    /// The actor's transfer bounds must fit the worker's leased arena.
    /// Receipt aliases come from a private source, as in `ReplicaActor::new`.
    pub fn new(
        journal: J,
        startup: RecoveryStartup,
        config: ActorConfig,
        timing: RecoveryTiming,
    ) -> Result<Self, ActorError> {
        Self::new_with_ids(
            journal,
            startup,
            config,
            timing,
            ActorIds::random(super::ReceiptHandles::default()),
        )
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
        let timing = timing.validate()?;
        let configuration = startup.configuration();
        let local = startup.local();
        let scope = configuration.scope();
        let buffer = journal.lease_append_buffer()?;
        let buffer =
            super::construct::checked_arena(buffer, startup.generation(), config.transfer)?;
        let wire_limits = config.wire_limits()?;
        let nonce = ids.request()?;
        let recovery = startup.into_recovery(nonce, config.transfer)?;
        let outbox = ReplicaOutbox::new(configuration, local, config.control, config.data)?;
        let bindings = super::construct::peer_bindings(configuration, local, &config, None)?;
        let status = SharedStatus::new(Cell::new(ActorStatus::initial(scope)));
        let requests =
            [ids.request()?, ids.request()?, ids.request()?].map(|request_id| RecoveryRequest {
                scope,
                request_id,
                nonce,
                nonvoting: true,
            });
        Ok(Self {
            configuration,
            local,
            config,
            timing,
            journal,
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

    /// Latest observation. No observation grants authority; normal and ready
    /// stay absent throughout transfer.
    pub fn status(&self) -> ActorStatus {
        self.status.get()
    }

    // The OMQ loop and controlled event driver use exactly the same lifecycle.
    // Owning self keeps cancellation from leaving a resumable half-handoff.
    async fn transition(mut self, outcome: Outcome) -> Result<Transition<J>, ActorError> {
        let journal = self.journal;
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
        self.journal.shutdown().await?;
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
        let mut next = next;
        // Recovery requests may start with a lower view hint. Routing must
        // keep its last observed fence while this copy remains nonvoting.
        let old = self.status.get();
        if old.scope.view > next.scope.view {
            next.scope = old.scope;
        }
        self.status.set(next);
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
        if self.abandoning {
            if let Some(ticket) = self.ticket.filter(|_| self.staged.is_some()) {
                self.pending = Some(Pending::Abort(self.journal.abort_recovery(ticket)?));
                return Ok(None);
            }
            return Ok(Some(Outcome::Restart));
        }
        if self.ticket.is_none() {
            match self.recovery.begin_transfer() {
                Ok(ticket) => {
                    self.ticket = Some(ticket);
                    self.pending = Some(Pending::Begin(
                        self.journal
                            .begin_recovery(ticket, self.config.installation)?,
                    ));
                    return Ok(None);
                }
                Err(RecoveryError::QuorumMissing | RecoveryError::PrimaryMissing) => {}
                Err(error) => return Err(error.into()),
            }
            self.request_quorum(now)?;
        } else if let (Some(ticket), Some(staged)) = (self.ticket, self.staged) {
            self.transfer(&ticket, staged, now)?;
        }
        Ok(None)
    }

    fn complete(
        &mut self,
        completed: Completed,
        now: Duration,
    ) -> Result<Option<Outcome>, ActorError> {
        match completed {
            Completed::Checkpoint(progress) => {
                let ticket = self
                    .ticket
                    .ok_or_else(|| ActorError::history(HistoryReason::Recovery))?;
                self.checkpoint_through = progress.through;
                if !self.abandoning
                    && let Some(revision) = progress.revision
                {
                    self.recovery.complete_checkpoint(
                        ticket,
                        ticket
                            .checkpoint()
                            .ok_or_else(|| ActorError::history(HistoryReason::Recovery))?,
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
