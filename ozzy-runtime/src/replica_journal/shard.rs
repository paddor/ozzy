//! Storage progress polled inside the partition actor. No spawned journal task,
//! worker, runtime, timer or file syscall. Jobs use the owner's shared backend.

mod dispatch;
mod jobs;

use super::commands::{Action, Command};
use super::{JournalError, JournalExecution, OwnedJournal, OwnedWriteStep};
use crate::command_channel::NotifiedReceiver;
use futures::{
    FutureExt, StreamExt,
    future::LocalBoxFuture,
    stream::{FuturesOrdered, FuturesUnordered},
};
use std::{
    cell::Cell,
    collections::VecDeque,
    rc::Rc,
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

/// Local execution retained inside `ReplicaActor<ShardJournal>`. Dropping a
/// waiter does not cancel it. Dropping the actor fences authority; backend jobs
/// retain their handles/buffers until settled. Explicit shutdown drains first.
pub struct ShardJournal {
    running: Option<LocalBoxFuture<'static, bool>>,
    failed: bool,
    control: Rc<Cell<ControlCapacity>>,
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
            .field("running", &self.running.is_some())
            .field("failed", &self.failed)
            .field("control", &self.control)
            .finish()
    }
}

impl JournalExecution for ShardJournal {
    fn control_capacity(&self, accepted: ozzy_replication::OpNumber) -> usize {
        let observed = self.control.get();
        // Core admission can lead journal installation by one turn. Reserve
        // every unseen operation as a possible control claim, including commands
        // queued behind an apply. Never spend the same slot twice.
        accepted
            .0
            .checked_sub(observed.accepted)
            .and_then(|pending| usize::try_from(pending).ok())
            .map_or(0, |pending| observed.available.saturating_sub(pending))
    }

    fn poll_finished(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if let Some(running) = &mut self.running {
            self.failed = std::task::ready!(running.as_mut().poll(cx));
            self.running = None;
        }
        Poll::Ready(self.failed)
    }
}

type Install = Box<dyn FnOnce(&mut State) -> bool>;
type Job = LocalBoxFuture<'static, Install>;
type CommandWork = LocalBoxFuture<'static, (State, Option<Job>, bool)>;

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

impl ShardJournal {
    pub(super) fn new(
        owner: OwnedJournal,
        receiver: NotifiedReceiver<Command>,
        config: ShardJournalConfig,
        timestamp: impl Fn() -> u64 + 'static,
    ) -> Self {
        let control = Rc::new(Cell::new(ControlCapacity::observe(&owner)));
        Self {
            running: Some(run(owner, receiver, config, timestamp, control.clone()).boxed_local()),
            failed: false,
            control,
        }
    }
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

fn needs_settled_writes(action: &Action) -> bool {
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
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "one bounded owner loop with independent command and file futures"
)]
async fn run(
    owner: OwnedJournal,
    receiver: NotifiedReceiver<Command>,
    config: ShardJournalConfig,
    timestamp: impl Fn() -> u64,
    control: Rc<Cell<ControlCapacity>>,
) -> bool {
    let mut state = Some(State {
        owner,
        sequence: 0,
        sync: None,
        sync_busy: false,
        rolling: false,
        cleanup_class: 0,
    });
    let mut receiver = Some(receiver);
    let mut active: Option<CommandWork> = None;
    let mut held: Option<Command> = None;
    let mut jobs = FuturesUnordered::<Job>::new();
    let mut writes = FuturesOrdered::<LocalBoxFuture<'static, super::OwnedCompletedWrite>>::new();
    let mut completed = VecDeque::with_capacity(config.commands + config.write_depth);
    let mut failed = false;
    let mut steps = 0;
    loop {
        steps += 1;
        if steps >= config.turn_steps {
            steps = 0;
            yield_turn().await;
        }
        if failed {
            receiver = None;
            held = None;
        }
        let mut runnable = false;
        let mut finished = false;
        if let Some(current) = &mut state {
            control.set(ControlCapacity::observe(&current.owner));
            if let Some(done) = completed.pop_front() {
                failed |= match done {
                    Completed::Write(done) => current.owner.complete_write(done).is_err(),
                    Completed::Job(install) => install(current),
                };
                failed |= current.owner.is_faulted();
                continue;
            }
            let mut idle = failed;
            if !failed && writes.len() < config.write_depth {
                match current.owner.prepare_write() {
                    Ok(OwnedWriteStep::Idle) => idle = true,
                    Ok(OwnedWriteStep::Write(work)) => {
                        writes.push_back(
                            async move {
                                let started_at = crate::profiling::start();
                                let done = work.write().await;
                                crate::profiling::finish(
                                    crate::profiling::Stage::JournalPhysicalWrite,
                                    started_at,
                                );
                                done
                            }
                            .boxed_local(),
                        );
                        continue;
                    }
                    Ok(OwnedWriteStep::RollRequired) if writes.is_empty() && !current.sync_busy => {
                        if let Ok(work) = current.owner.begin_roll(config.roll_probes) {
                            current.rolling = true;
                            jobs.push(
                                async move {
                                    let done = work.publish().await;
                                    Box::new(move |state: &mut State| {
                                        state.rolling = false;
                                        state.owner.complete_roll(done).is_err()
                                    }) as Install
                                }
                                .boxed_local(),
                            );
                        } else {
                            failed = true;
                        }
                        continue;
                    }
                    Ok(OwnedWriteStep::Waiting | OwnedWriteStep::RollRequired) => {}
                    Err(_) => {
                        failed = true;
                        continue;
                    }
                }
            }
            if !failed
                && !current.sync_busy
                && writes.is_empty()
                && current.owner.identity_refresh_ready()
            {
                let mut refreshing = state.take().expect("idle owner");
                active = Some(
                    async move {
                        let failed = refreshing.owner.refresh_settled_identities().await.is_err();
                        (refreshing, None, failed)
                    }
                    .boxed_local(),
                );
                continue;
            }
            if !failed
                && held.is_none()
                && let Some(receiver) = &mut receiver
            {
                // Async receive below observes closure or registers readiness.
                held = receiver.try_recv().ok();
            }
            runnable = held.as_mut().is_some_and(|command| {
                let capacity = match &mut command.action {
                    Action::Admit { validated, .. } => current.owner.can_admit(validated),
                    Action::Turn { turn, .. } => turn
                        .admit
                        .as_mut()
                        .is_none_or(|(_, validated)| current.owner.can_admit(validated)),
                    _ => true,
                };
                (capacity || (idle && writes.is_empty() && !current.sync_busy))
                    && (!needs_settled_writes(&command.action)
                        || (idle && writes.is_empty() && !current.sync_busy))
                    && (!matches!(command.action, Action::BeginPipelinedSync { .. })
                        || !current.rolling)
            });
            finished = receiver.is_none()
                && held.is_none()
                && jobs.is_empty()
                && writes.is_empty()
                && idle;
        }
        if runnable {
            active = Some(
                dispatch::run(
                    state.take().expect("idle owner"),
                    held.take().expect("runnable command"),
                    timestamp(),
                )
                .boxed_local(),
            );
            continue;
        }
        if finished {
            break;
        }
        // The command future temporarily owns the same journal. Independent
        // file futures still get polled; their results wait for its return.
        tokio::select! {
            biased;
            result = async { active.as_mut().expect("active command").await }, if active.is_some() => {
                let (returned, job, faulted) = result;
                active = None;
                state = Some(returned);
                if let Some(job) = job { jobs.push(job); }
                failed |= faulted;
            }
            done = writes.next(), if !writes.is_empty() => completed.push_back(Completed::Write(done.expect("pending write"))),
            done = jobs.next(), if !jobs.is_empty() => completed.push_back(Completed::Job(done.expect("pending job"))),
            command = async { receiver.as_mut().expect("open receiver").recv_async().await }, if receiver.is_some() && held.is_none() && !failed => {
                match command { Ok(command) => held = Some(command), Err(_) => receiver = None }
            }
        }
    }
    let state = state.expect("no executing command at shutdown");
    failed | state.owner.shutdown().await.is_err()
}
