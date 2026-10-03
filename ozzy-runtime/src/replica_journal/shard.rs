//! Storage progress polled inside the partition actor. No spawned journal task,
//! worker, runtime, timer or file syscall. Jobs use the owner's shared backend.

mod dispatch;
mod jobs;

use super::commands::{Action, Command};
use super::{JournalError, OwnedJournal, OwnedWriteStep};
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

// Always yield once, even with an entirely memory-resident workload. This does
// not consult a wall clock and also works under a manually polled simulator.
pub(super) async fn yield_turn() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

fn needs_settled_writes(action: &Action, owner: &OwnedJournal) -> bool {
    matches!(
        action,
        Action::Promise { .. }
            | Action::Sync { .. }
            | Action::CaptureHistory { .. }
            | Action::BeginInstall { .. }
            | Action::FinishInstall { .. }
            | Action::AbortInstall { .. }
            | Action::Activate { .. }
            | Action::CleanupOrphans { .. }
            | Action::CleanupMetadata { .. }
            | Action::ValidateStorage { .. }
            | Action::Recovery(super::recovery::RecoveryAction::Pin { .. })
    ) || match action {
        Action::Propose { buffer, .. } => owner.validation_may_read(&buffer.0),
        Action::Validate { buffer, .. } => owner.validation_may_read(buffer),
        Action::Turn { turn, .. } => {
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
        _ => false,
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
        self.state
            .as_ref()
            .expect("available journal owner")
            .owner
            .open_reader(ticket, partition, from)
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
    pub(super) fn validation_ready(&self, buffer: &super::AppendBuffer) -> bool {
        self.available() != 0
            && self
                .state
                .as_ref()
                .is_some_and(|state| !state.owner.validation_may_read(buffer) || self.settled())
    }
    #[expect(
        clippy::result_large_err,
        reason = "refusal returns caller-owned buffers without allocating"
    )]
    pub(super) fn submit(&mut self, mut command: Command) -> Result<(), Command> {
        if self.available() == 0 {
            return Err(command);
        }
        let state = self.state.as_mut().expect("available owner");
        let settled = state.owner.writes_settled()
            && self.writes.is_empty()
            && !state.sync_busy
            && !state.rolling;
        let capacity = match &mut command.action {
            Action::Admit { validated, .. } => state.owner.can_admit(validated),
            Action::Turn { turn, .. } => turn
                .admit
                .as_mut()
                .is_none_or(|(_, validated)| state.owner.can_admit(validated)),
            _ => true,
        };
        if (!capacity || needs_settled_writes(&command.action, &state.owner)) && !settled
            || (matches!(command.action, Action::BeginPipelinedSync { .. }) && state.rolling)
        {
            return Err(command);
        }
        self.idle = false;
        self.active = Some(
            dispatch::run(
                self.state.take().expect("available owner"),
                command,
                (self.timestamp)(),
            )
            .boxed_local(),
        );
        // Pure owner work completes in the call. A suspended file operation is
        // retained here and receives the actor's real waker on its next poll.
        let mut cx = Context::from_waker(std::task::Waker::noop());
        self.poll_active(&mut cx);
        Ok(())
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
    #[expect(
        clippy::too_many_lines,
        reason = "one bounded owner progress and shutdown turn"
    )]
    pub(super) fn poll_finished(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
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
            if let Some(state) = &mut self.state {
                if let Some(done) = self.completed.pop_front() {
                    self.failed |= match done {
                        Completed::Write(done) => state.owner.complete_write(done).is_err(),
                        Completed::Job(install) => install(state),
                    };
                    self.failed |= state.owner.is_faulted();
                    continue;
                }
                self.idle = self.failed;
                if !self.failed && self.writes.len() < self.config.write_depth {
                    match state.owner.prepare_write() {
                        Ok(OwnedWriteStep::Idle) => self.idle = true,
                        Ok(OwnedWriteStep::Write(work)) => {
                            self.writes.push_back(
                                async move {
                                    let started = crate::profiling::start();
                                    let done = work.write().await;
                                    crate::profiling::finish(
                                        crate::profiling::Stage::JournalPhysicalWrite,
                                        started,
                                    );
                                    done
                                }
                                .boxed_local(),
                            );
                            continue;
                        }
                        Ok(OwnedWriteStep::RollRequired)
                            if self.writes.is_empty() && !state.sync_busy =>
                        {
                            match state.owner.begin_roll(self.config.roll_probes) {
                                Ok(work) => {
                                    state.rolling = true;
                                    self.jobs.push(
                                        async move {
                                            let done = work.publish().await;
                                            Box::new(move |state: &mut State| {
                                                state.rolling = false;
                                                state.owner.complete_roll(done).is_err()
                                            })
                                                as Install
                                        }
                                        .boxed_local(),
                                    );
                                }
                                Err(_) => self.failed = true,
                            }
                            continue;
                        }
                        Ok(OwnedWriteStep::Waiting | OwnedWriteStep::RollRequired) => {}
                        Err(_) => {
                            self.failed = true;
                            continue;
                        }
                    }
                }
                if !self.failed
                    && !state.sync_busy
                    && self.writes.is_empty()
                    && state.owner.identity_refresh_ready()
                {
                    let mut state = self.state.take().expect("available owner");
                    self.active = Some(
                        async move {
                            let failed = state.owner.refresh_settled_identities().await.is_err();
                            (state, None, failed)
                        }
                        .boxed_local(),
                    );
                    continue;
                }
                if (self.closing || self.failed)
                    && self.idle
                    && self.jobs.is_empty()
                    && self.writes.is_empty()
                {
                    let state = self.state.take().expect("settled owner");
                    self.shutdown =
                        Some(async move { state.owner.shutdown().await.is_err() }.boxed_local());
                    return self.poll_finished(cx);
                }
            }
            let mut progressed = self.poll_active(cx);
            if let Poll::Ready(Some(done)) = self.writes.poll_next_unpin(cx) {
                self.completed.push_back(Completed::Write(done));
                progressed = true;
            }
            if let Poll::Ready(Some(done)) = self.jobs.poll_next_unpin(cx) {
                self.completed.push_back(Completed::Job(done));
                progressed = true;
            }
            if !progressed {
                return Poll::Pending;
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
