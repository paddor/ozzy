//! Single-broker scheduling over the shared shard-owned journal executor.
//! Transport and shared broker dispatch remain outside partition authority.

mod progress;
use super::{
    ActorError, ProposalOutcome, ProposalSubmitter,
    ingress::{Ingress, Reply, Submission},
    io::PendingSync,
};
use crate::replica_journal::{
    AdmittedAppend, AppendBuffer, JournalCompletion, OwnedJournal, ProposalBuffer,
    ProposalValidation, ReplicaJournal, ShardJournalConfig, SubmitError, ValidatedAppend,
};
use ozzy_replication::{
    Prefix, WriteTicket,
    driver::ValidationTicket,
    local::{Driver, Snapshot},
};
use std::{
    collections::VecDeque,
    task::{Context, Poll},
    time::Instant,
};

/// Per-actor scheduling bounds. Device workers and admission remain shared.
#[derive(Debug, Clone, Copy)]
pub struct LocalActorConfig {
    /// Periodic bounded retention turns; absent for unlimited topic policies.
    pub retention_interval: Option<std::time::Duration>,
    /// Bounded commands and physical write progress on this shard.
    pub journal: ShardJournalConfig,
    /// Startup-registered submission lanes sharing one request budget.
    pub proposal_lanes: usize,
    /// Outstanding proposals through reply publication. Arenas stay charged
    /// separately until their final owner releases them.
    pub proposal_capacity: usize,
    /// Ready actor steps before yielding to other partition actors.
    pub turn_steps: usize,
}

impl Default for LocalActorConfig {
    fn default() -> Self {
        Self {
            retention_interval: None,
            journal: ShardJournalConfig::default(),
            proposal_lanes: 1,
            proposal_capacity: 64,
            turn_steps: 16,
        }
    }
}

#[derive(Debug)]
enum Pending {
    Ready(Box<(WriteTicket, ValidatedAppend)>),
    Propose(JournalCompletion<ProposalValidation>),
    Admit(JournalCompletion<AdmittedAppend>),
    Apply(JournalCompletion<ValidationTicket>),
    Retention(JournalCompletion<crate::replica_journal::RetentionTurn>),
}

#[derive(Debug)]
struct Live {
    through: Prefix,
    reply: Reply,
    buffer: Option<AppendBuffer>,
}

/// One local-durable partition actor. It polls storage futures on its application
/// shard and creates no thread or task. Callers poll actors independently so one
/// partition's pending I/O never blocks another. Intake is count/byte bounded.
#[derive(Debug)]
pub struct LocalActor {
    journal: ReplicaJournal,
    now: std::time::Duration,
    retention_at: Option<std::time::Duration>,
    foreground_turn_due: bool,
    driver: Driver,
    config: LocalActorConfig,
    ingress: Ingress,
    submitters: Vec<ProposalSubmitter>,
    waiting: Option<Submission>,
    validating: Option<Reply>,
    live: VecDeque<Live>,
    pending: Option<Pending>,
    persistence: VecDeque<(JournalCompletion<WriteTicket>, Option<Instant>)>,
    sync: Option<PendingSync>,
    propose_started_at: Option<Instant>,
    admit_started_at: Option<Instant>,
    apply_started_at: Option<Instant>,
    sync_started_at: Option<Instant>,
    sync_install_started_at: Option<Instant>,
    closing: bool,
    faulted: bool,
}

impl LocalActor {
    pub(super) fn read_access(&mut self) -> (Option<ValidationTicket>, &mut ReplicaJournal) {
        let ticket = (!self.closing && !self.faulted)
            .then(|| self.driver.begin_validation().ok())
            .flatten();
        (ticket, &mut self.journal)
    }

    /// Immutable partition group. This observation grants no write authority.
    pub fn group(&self) -> ozzy_proto::GroupId {
        self.driver.configuration().scope().group_id
    }

    /// Current local routing observation. This establishes no confirmation.
    pub fn authority_hint(&self) -> ozzy_proto::nack::AuthorityHint {
        let configuration = self.driver.configuration();
        let scope = configuration.scope();
        ozzy_proto::nack::AuthorityHint {
            authority: ozzy_proto::append::Authority {
                group_id: scope.group_id,
                config_epoch: scope.configuration_epoch,
                view: scope.view,
            },
            primary: configuration.broker(),
        }
    }

    pub(super) fn route_state(
        &self,
        partition: ozzy_proto::PartitionIncarnation,
    ) -> ozzy_proto::directory::RouteState {
        let hint = self.authority_hint();
        ozzy_proto::directory::RouteState {
            group: hint.authority.group_id,
            config_epoch: hint.authority.config_epoch,
            partition,
            members: [hint.primary].into(),
            view: 0,
            leader: (!self.closing && !self.faulted).then_some(hint.primary),
        }
    }

    pub(super) fn observe_route(&self, route: &mut ozzy_proto::directory::RouteState) -> bool {
        let hint = self.authority_hint();
        if route.group != hint.authority.group_id
            || route.config_epoch != hint.authority.config_epoch
            || route.members.as_ref() != [hint.primary]
        {
            return false;
        }
        route.view = 0;
        route.leader = (!self.closing && !self.faulted).then_some(hint.primary);
        true
    }

    /// Take matching local storage and driver authority after format/recovery.
    /// The timestamp source is injected; simulations need no wall-clock input.
    pub fn new(
        owner: OwnedJournal,
        driver: Driver,
        config: LocalActorConfig,
        timestamp: impl Fn() -> u64 + 'static,
    ) -> Result<Self, ActorError> {
        owner.check_local_driver(&driver)?;
        if config
            .retention_interval
            .is_some_and(|interval| interval.is_zero())
            || config.proposal_lanes == 0
            || config.proposal_lanes > 65536
            || config.proposal_capacity == 0
            || config.proposal_capacity > 65536
            || config.turn_steps == 0
            || config.turn_steps > 1024
            || config.journal.commands < 3
        {
            return Err(ActorError::Limits);
        }
        let journal = owner.into_shard_journal(config.journal, timestamp)?;
        let limits = driver.limits();
        let backlog = journal.backlog();
        if limits.max_operations > backlog.max_operations
            || limits.max_body_bytes > backlog.max_body_bytes
        {
            return Err(ActorError::Limits);
        }
        let (ingress, submitters) = Ingress::new(
            config.proposal_lanes,
            config.proposal_capacity,
            driver.snapshot().journal.generation,
            limits,
        );
        Ok(Self {
            journal,
            now: std::time::Duration::ZERO,
            retention_at: config.retention_interval,
            foreground_turn_due: false,
            driver,
            config,
            ingress,
            submitters,
            waiting: None,
            validating: None,
            live: VecDeque::with_capacity(config.proposal_capacity),
            pending: None,
            persistence: VecDeque::with_capacity(limits.max_operations),
            sync: None,
            propose_started_at: None,
            admit_started_at: None,
            apply_started_at: None,
            sync_started_at: None,
            sync_install_started_at: None,
            closing: false,
            faulted: false,
        })
    }

    /// Take one startup-registered lane before running the actor.
    pub fn take_submitter(&mut self) -> Option<ProposalSubmitter> {
        self.submitters.pop()
    }

    /// Lease a bounded arena for reuse through proposal completions.
    pub fn lease_proposal_buffer(&self) -> Result<ProposalBuffer, SubmitError> {
        self.journal.lease_proposal_buffer()
    }

    /// Lease a smaller startup arena against this partition's existing pool.
    /// Allocation limits remain fixed through reuse and proposal completion.
    pub fn lease_proposal_buffer_with_limits(
        &self,
        limits: ozzy_replication::PipelineLimits,
    ) -> Result<ProposalBuffer, ActorError> {
        let maximum = self.driver.limits();
        if limits.max_operations == 0
            || limits.max_operations > maximum.max_operations
            || limits.max_body_bytes == 0
            || limits.max_body_bytes > maximum.max_body_bytes
        {
            return Err(ActorError::Limits);
        }
        self.journal
            .lease_append_buffer_with_limits(limits)
            .map(ProposalBuffer)
            .map_err(ActorError::from)
    }

    /// Inspect acceptance, local durability, application, and live capacity.
    pub fn snapshot(&self) -> Snapshot {
        self.driver.snapshot()
    }

    /// Poll a bounded round. Healthy actors remain pending, with wakeups from
    /// ingress and file completions. Shutdown returns only after admitted work
    /// is durable and applied. A terminal error closes admission and authority.
    pub fn poll_progress(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ActorError>> {
        if self.faulted {
            return Poll::Ready(Err(crate::replica_journal::JournalError::Faulted.into()));
        }
        for _ in 0..self.config.turn_steps {
            match self.progress(cx) {
                Err(error) => {
                    self.faulted = true;
                    self.ingress.close();
                    let _ = self.driver.fail(self.driver.snapshot().journal.generation);
                    return Poll::Ready(Err(error));
                }
                Ok(false) => {
                    return if self.closing && self.drained() {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Pending
                    };
                }
                Ok(true) => {}
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    /// Drive alongside other partition futures on the same current-thread runtime.
    pub async fn stopped(&mut self) -> Result<(), ActorError> {
        std::future::poll_fn(|cx| self.poll_progress(cx)).await
    }

    /// Close unadmitted requests, drain admitted writes/barriers, then close the
    /// journal through its backend. Dropping this future cannot cancel file jobs.
    pub async fn shutdown(mut self) -> Result<(), ActorError> {
        self.closing = true;
        self.ingress.close();
        self.submitters.clear();
        let progress = if self.faulted {
            Ok(())
        } else {
            std::future::poll_fn(|cx| self.poll_progress(cx)).await
        };
        let closed = self.journal.shutdown().await;
        progress?;
        closed?;
        Ok(())
    }

    fn drained(&self) -> bool {
        let state = self.driver.snapshot();
        self.pending.is_none()
            && self.persistence.is_empty()
            && self.sync.is_none()
            && self.live.is_empty()
            && state.accepted == state.applied
    }

    fn schedule(&mut self) -> Result<bool, ActorError> {
        if self.journal.available_command_slots() == 0 {
            return Ok(false);
        }
        if matches!(self.pending, Some(Pending::Ready(_))) {
            let Some(Pending::Ready(ready)) = self.pending.take() else {
                unreachable!("ready admission");
            };
            let (ticket, validated) = *ready;
            return self.admit_ready(ticket, validated);
        }
        let state = self.driver.snapshot();
        let mut changed = false;
        if self.sync.is_none() && state.journal.written > state.journal.durable {
            self.sync = Some(PendingSync::Barrier(
                self.journal
                    .begin_pipelined_sync(self.driver.begin_sync()?)?,
            ));
            self.sync_started_at = crate::profiling::start();
            changed = true;
        }
        if self.pending.is_some() || self.journal.available_command_slots() == 0 {
            return Ok(changed);
        }
        if state.applied != state.committed {
            self.pending = Some(Pending::Apply(
                self.journal
                    .apply_committed(self.driver.begin_validation()?)?,
            ));
            self.apply_started_at = crate::profiling::start();
            return Ok(true);
        }
        if self.closing {
            if let Some(waiting) = self.waiting.take().or_else(|| self.ingress.pop()) {
                waiting
                    .reply
                    .finish(waiting.buffer, ProposalOutcome::NotAdmitted);
                return Ok(true);
            }
            return Ok(changed);
        }
        let foreground_turn = std::mem::take(&mut self.foreground_turn_due);
        if !foreground_turn
            && self.retention_at.is_some_and(|at| self.now >= at)
            && self.waiting.is_none()
        {
            if !self.persistence.is_empty()
                || self.sync.is_some()
                || !self.live.is_empty()
                || state.accepted != state.applied
            {
                return Ok(changed);
            }
            self.pending = Some(Pending::Retention(self.journal.retention_turn(
                self.driver.begin_validation()?,
                ozzy_proto::OperationId::from_bytes(*ozzy_proto::RequestId::new().as_bytes()),
                true,
            )?));
            self.retention_at = self
                .config
                .retention_interval
                .map(|interval| self.now.saturating_add(interval));
            return Ok(true);
        }
        if self.waiting.is_none() {
            self.waiting = self.ingress.pop();
        }
        let Some(waiting) = &self.waiting else {
            return Ok(changed);
        };
        if waiting.reply.fenced() {
            let waiting = self.waiting.take().expect("waiting request");
            waiting
                .reply
                .finish(waiting.buffer, ProposalOutcome::NotAdmitted);
            return Ok(true);
        }
        let limits = self.driver.limits();
        if waiting.buffer.len() > limits.max_operations - state.pending_operations
            || waiting.buffer.body_bytes() > limits.max_body_bytes - state.pending_body_bytes
            || !self
                .journal
                .validation_has_capacity(&waiting.buffer.0, state.accepted.op)
        {
            return Ok(changed);
        }
        let mut waiting = self.waiting.take().expect("waiting request");
        waiting.reply.scheduled();
        self.pending = Some(Pending::Propose(
            self.journal
                .propose_append(self.driver.begin_validation()?, waiting.buffer)
                .map_err(|rejected| rejected.reason)?,
        ));
        self.propose_started_at = crate::profiling::start();
        self.validating = Some(waiting.reply);
        Ok(true)
    }
}

impl LocalActor {
    pub(super) fn observe_time(&mut self, now: std::time::Duration) {
        self.now = now;
    }
}
