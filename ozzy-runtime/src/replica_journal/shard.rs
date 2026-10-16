//! Storage progress polled inside the partition actor. No spawned journal task,
//! worker, runtime, timer or file syscall. Jobs use the owner's shared backend.

mod dispatch;
mod jobs;
mod owner_work;

use super::commands::{Command, Gate};
use super::{JournalError, OwnedJournal};
use futures::{
    FutureExt, StreamExt,
    future::LocalBoxFuture,
    stream::{FuturesOrdered, FuturesUnordered},
};
use std::{
    collections::VecDeque,
    task::{Context, Poll},
};

/// Finite per-partition scheduling bounds, independent of shared device limits.
#[derive(Debug, Clone, Copy)]
pub struct ShardJournalConfig {
    /// Admitted commands, including detached reads and barriers.
    pub commands: usize,
    /// Concurrent physical writes. Results install in submission order.
    pub write_depth: usize,
    /// Maximum ready scheduling steps before yielding to other partition actors.
    pub turn_steps: usize,
    /// Exclusive-create attempts before a segment roll fails closed.
    pub roll_probes: usize,
}

impl Default for ShardJournalConfig {
    fn default() -> Self {
        Self {
            commands: 8,
            write_depth: 1,
            turn_steps: 16,
            roll_probes: 16,
        }
    }
}

/// Direct journal owner. Only suspended operations and physical completions
/// remain retained; there is no command queue back to this application shard.
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent progress, failure and shutdown observations"
)]
pub struct ShardJournal {
    state: Option<Box<State>>,
    active: Option<CommandWork>,
    capacity_wait: Option<LocalBoxFuture<'static, ()>>,
    poller: super::execution::PollWaker,
    jobs: FuturesUnordered<Job>,
    writes: FuturesOrdered<LocalBoxFuture<'static, super::OwnedCompletedWrite>>,
    completed: VecDeque<Completed>,
    config: ShardJournalConfig,
    timestamp: Box<dyn Fn() -> u64>,
    idle: bool,
    closing: bool,
    failed: bool,
    shutdown: Option<LocalBoxFuture<'static, bool>>,
    finished: bool,
    retain: bool,
}

#[derive(Debug, Clone, Copy)]
struct ControlCapacity {
    accepted: u64,
    available: usize,
}

impl ControlCapacity {
    fn observe(owner: &OwnedJournal) -> Self {
        Self {
            accepted: owner.accepted().op.0,
            available: owner
                .identity_capacity()
                .map_or(0, |(used, capacity)| capacity - used),
        }
    }
}

impl std::fmt::Debug for ShardJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardJournal")
            .field("active", &self.active.is_some())
            .field("writes", &self.writes.len())
            .field("jobs", &self.jobs.len())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

type Install = Box<dyn FnOnce(&mut State) -> bool>;
type Job = LocalBoxFuture<'static, Install>;
type CommandWork = LocalBoxFuture<'static, (Box<State>, Option<Job>, bool)>;

struct State {
    owner: OwnedJournal,
    sequence: u64,
    sync: Option<super::sync::Key>,
    sync_busy: bool,
    rolling: bool,
    cleanup_class: usize,
}

impl State {
    fn next_request(&mut self) -> Result<ozzy_proto::RequestId, JournalError> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(JournalError::CompletionMismatch)?;
        Ok(ozzy_proto::RequestId::from_bytes(
            u128::from(self.sequence).to_be_bytes(),
        ))
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "bounded inline completion slots avoid another allocation per write"
)]
enum Completed {
    Write(super::OwnedCompletedWrite),
    Job(Install),
}

/// A turn waits for settled writes when it admits and validates in one
/// command, or when its validation may read storage.
fn turn_needs_settled(turn: &super::Turn, owner: &OwnedJournal) -> bool {
    (turn.admit.is_some() && (turn.propose.is_some() || turn.validate.is_some()))
        || turn
            .propose
            .as_ref()
            .is_some_and(|(_, buffer)| owner.validation_may_read(&buffer.0))
        || turn
            .validate
            .as_ref()
            .is_some_and(|(_, buffer)| owner.validation_may_read(buffer))
}

/// The owner state taken for one admitted command.
pub(super) struct Admitted<'a> {
    journal: &'a mut ShardJournal,
    state: Box<State>,
}

impl Admitted<'_> {
    /// Run the command. Pure owner work completes in the call. A suspended
    /// file operation is retained and wakes the partition that last polled.
    pub(super) fn start(self, command: Command) {
        let Self { journal, state } = self;
        journal.idle = false;
        journal.active = Some(dispatch::run(state, command, (journal.timestamp)()).boxed_local());
        let waker = journal.poller.waker();
        journal.poll_active(&mut Context::from_waker(&waker));
    }
}

impl ShardJournal {
    pub(super) fn new(
        owner: OwnedJournal,
        config: ShardJournalConfig,
        timestamp: impl Fn() -> u64 + 'static,
    ) -> Self {
        Self {
            state: Some(Box::new(State {
                owner,
                sequence: 0,
                sync: None,
                sync_busy: false,
                rolling: false,
                cleanup_class: 0,
            })),
            active: None,
            capacity_wait: None,
            poller: super::execution::PollWaker::new(),
            jobs: FuturesUnordered::new(),
            writes: FuturesOrdered::new(),
            completed: VecDeque::with_capacity(config.commands + config.write_depth),
            config,
            timestamp: Box::new(timestamp),
            idle: true,
            closing: false,
            failed: false,
            shutdown: None,
            finished: false,
            retain: false,
        }
    }
    pub(super) fn is_closed(&self) -> bool {
        self.closing || self.failed || self.finished
    }
    pub(super) fn open_reader(
        &self,
        ticket: ozzy_replication::driver::ValidationTicket,
        partition: ozzy_proto::PartitionIncarnation,
        from: Option<ozzy_proto::Offset>,
    ) -> Result<super::PartitionReadCursor, JournalError> {
        self.idle_owner()?.open_reader(ticket, partition, from)
    }
    pub(super) fn coordinator_group(
        &self,
        ticket: ozzy_replication::driver::ValidationTicket,
        coordinator: ozzy_proto::PartitionIncarnation,
        name: &str,
    ) -> Result<Option<std::sync::Arc<ozzy_core::state::CoordinatorGroup>>, JournalError> {
        self.idle_owner()?
            .coordinator_group(ticket, coordinator, name)
    }
    pub(super) fn facts(
        &self,
        partition: ozzy_proto::PartitionIncarnation,
    ) -> Result<Option<super::PartitionFacts>, JournalError> {
        self.idle_owner()?.facts(partition)
    }
    /// The owner, unless a command holds it. A held owner is busy; the caller
    /// retries on its next turn.
    fn idle_owner(&self) -> Result<&OwnedJournal, JournalError> {
        self.state
            .as_ref()
            .map(|state| &state.owner)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::WouldBlock).into())
    }

    pub(super) fn available(&self) -> usize {
        usize::from(
            !self.is_closed()
                && self.state.as_ref().is_some_and(|state| !state.rolling)
                && self.completed.is_empty(),
        )
    }
    pub(super) fn close(&mut self) {
        self.closing = true;
    }
    pub(super) fn control_capacity(&self, accepted: ozzy_replication::OpNumber) -> usize {
        let Some(state) = &self.state else {
            return 0;
        };
        let observed = ControlCapacity::observe(&state.owner);
        accepted
            .0
            .checked_sub(observed.accepted)
            .and_then(|pending| usize::try_from(pending).ok())
            .map_or(0, |pending| observed.available.saturating_sub(pending))
    }
    pub(super) fn settled(&self) -> bool {
        self.state.as_ref().is_some_and(|state| {
            state.owner.writes_settled()
                && self.writes.is_empty()
                && !state.sync_busy
                && !state.rolling
        })
    }
    /// Whether a retirement still runs its job or awaits installation. A
    /// command holding the owner counts as busy.
    pub(super) fn retention_busy(&self) -> bool {
        self.state
            .as_ref()
            .is_none_or(|state| state.owner.retention_busy())
    }
    /// Whether a cleanup job runs. A command holding the owner counts as busy.
    pub(super) fn cleaning(&self) -> bool {
        self.state
            .as_ref()
            .is_none_or(|state| state.owner.cleaning())
    }
    pub(super) fn validation_ready(&self, buffer: &super::AppendBuffer) -> bool {
        use crate::profiling::{Event, event};
        let Some(state) = self.state.as_ref().filter(|_| self.available() != 0) else {
            event(Event::ValidationOwnerWait);
            return false;
        };
        if !state.owner.validation_memory_ready() {
            event(Event::ValidationMemoryWait);
            return false;
        }
        if !state.owner.validation_apply_ready() {
            event(Event::ValidationApplyWait);
            return false;
        }
        if state.owner.validation_may_read(buffer) && !self.settled() {
            event(Event::ValidationSettleWait);
            return false;
        }
        true
    }
    /// Take the owner for one command when its gate lets it start now. An
    /// available owner is never rolling.
    pub(super) fn admit(&mut self, gate: Gate<'_>) -> Option<Admitted<'_>> {
        if self.available() == 0 {
            return None;
        }
        let settled = self.settled();
        let owner = &self.state.as_ref()?.owner;
        let (capacity, settle) = match gate {
            Gate::Free => (true, false),
            Gate::Settled => (true, true),
            Gate::Validate(buffer) => (true, owner.validation_may_read(buffer)),
            Gate::Admit(validated) => (owner.can_admit(validated), false),
            Gate::Turn(turn) => (
                turn.admit
                    .as_mut()
                    .is_none_or(|(_, validated)| owner.can_admit(validated)),
                turn_needs_settled(turn, owner),
            ),
        };
        if (!capacity || settle) && !settled {
            crate::profiling::event(if capacity {
                crate::profiling::Event::JournalSettleWait
            } else {
                crate::profiling::Event::JournalCapacityWait
            });
            return None;
        }
        let state = self.state.take()?;
        Some(Admitted {
            journal: self,
            state,
        })
    }
    fn poll_active(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(active) = &mut self.active else {
            return false;
        };
        let Poll::Ready((state, job, failed)) = active.as_mut().poll(cx) else {
            return false;
        };
        self.active = None;
        self.state = Some(state);
        if let Some(job) = job {
            self.jobs.push(job);
        }
        self.failed |= failed;
        cx.waker().wake_by_ref();
        true
    }
    pub(super) fn poll_finished(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        self.poller.observe(cx);
        if self.finished {
            return Poll::Ready(self.failed);
        }
        if let Some(shutdown) = &mut self.shutdown {
            self.failed |= std::task::ready!(shutdown.as_mut().poll(cx));
            self.shutdown = None;
            self.finished = true;
            return Poll::Ready(self.failed);
        }
        for _ in 0..self.config.turn_steps {
            if self.state.is_some() {
                if self.step_owner() {
                    continue;
                }
                if self.drained() {
                    if self.retain && !self.failed {
                        self.finished = true;
                        return Poll::Ready(false);
                    }
                    if let Some(state) = self.state.take() {
                        self.shutdown = Some(
                            async move { state.owner.shutdown().await.is_err() }.boxed_local(),
                        );
                        return self.poll_finished(cx);
                    }
                }
            }
            if !self.poll_progress(cx) {
                self.poll_capacity_wait(cx);
                return Poll::Pending;
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    /// Closing or failed with no owner work left to start or finish.
    fn drained(&self) -> bool {
        (self.closing || self.failed) && self.idle && self.jobs.is_empty() && self.writes.is_empty()
    }

    /// Poll held, write, and job work once. `true` when any of it finished.
    fn poll_progress(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = self.poll_active(cx);
        if let Poll::Ready(Some(done)) = self.writes.poll_next_unpin(cx) {
            self.completed.push_back(Completed::Write(done));
            progressed = true;
        }
        if let Poll::Ready(Some(done)) = self.jobs.poll_next_unpin(cx) {
            self.completed.push_back(Completed::Job(done));
            progressed = true;
        }
        progressed
    }

    /// Wake once validation memory frees up, so a waiting owner is not lost.
    fn poll_capacity_wait(&mut self, cx: &mut Context<'_>) {
        if self.capacity_wait.is_none() {
            self.capacity_wait = self
                .state
                .as_ref()
                .and_then(|state| state.owner.validation_memory_wait())
                .map(FutureExt::boxed_local);
        }
        if self
            .capacity_wait
            .as_mut()
            .is_some_and(|wait| wait.as_mut().poll(cx).is_ready())
        {
            self.capacity_wait = None;
            if let Some(state) = &self.state {
                state.owner.clear_validation_memory_wait();
            }
            cx.waker().wake_by_ref();
        }
    }
}

impl ShardJournal {
    async fn take_recovering(
        &mut self,
        generations: super::OwnedRecoveryGenerations,
    ) -> Result<(super::ShardRecoveringJournal, super::RecoveryStartup), JournalError> {
        self.retain = true;
        self.close();
        if std::future::poll_fn(|cx| self.poll_finished(cx)).await {
            return Err(JournalError::Faulted);
        }
        let mut owner = self.state.take().ok_or(JournalError::Faulted)?.owner;
        let (config, io, memory) = Box::pin(owner.recovery_parts()).await?;
        owner.shutdown().await?;
        let (mut owner, startup) = super::OwnedRecoveringJournal::start(
            config,
            io,
            generations,
            super::OwnedRecoveryOpen::Quarantine,
        )
        .await?;
        if let Some(memory) = memory {
            owner.bind_append_source(memory)?;
        }
        let timestamp = std::mem::replace(&mut self.timestamp, Box::new(|| 0));
        let journal = super::ShardRecoveringJournal::from_owned(owner, self.config, timestamp)?;
        Ok((journal, startup))
    }
}

impl super::ReplicaJournal {
    pub(crate) async fn into_recovering(
        mut self,
        generations: super::OwnedRecoveryGenerations,
    ) -> Result<(super::ShardRecoveringJournal, super::RecoveryStartup), JournalError> {
        match &mut self.execution {
            super::execution::Execution::Normal(owner) => {
                Box::pin(owner.take_recovering(generations)).await
            }
            super::execution::Execution::Recovery(_) => Err(JournalError::Configuration),
        }
    }
}
