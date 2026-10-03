//! Serial recovery commands polled by their nonvoting partition actor.

use super::RecoveryStorage;
use crate::replica_journal::{
    AppendBuffer, InstallationConfig, JournalCompletion, JournalError, JournalStartup, OwnedConfig,
    OwnedJournal, OwnedRecoveringJournal, OwnedRecoveryGenerations, OwnedRecoveryOpen,
    PublishedRecovery, ReceivedChunk, RecoveryPlan, RecoveryStartup, Rejected, ReplicaJournal,
    ShardJournalConfig, SubmitError,
    commands::{Action, Command, finish},
    receiving::ReceiveAction,
};
use futures::{FutureExt, future::LocalBoxFuture};
use ozzy_replication::{
    JournalGeneration,
    recovery::{Recovery, RecoveryTicket},
};
use std::{
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::Semaphore;

#[expect(
    clippy::struct_excessive_bools,
    reason = "recovery retention and independent shutdown observations"
)]
pub(in crate::replica_journal) struct Execution {
    running: Option<LocalBoxFuture<'static, (Box<OwnedRecoveringJournal>, bool)>>,
    pub(in crate::replica_journal) retain: bool,
    pub(in crate::replica_journal) owner: Option<Box<OwnedRecoveringJournal>>,
    failed: bool,
    closing: bool,
    finished: bool,
    shutdown: Option<LocalBoxFuture<'static, Result<(), JournalError>>>,
}
impl std::fmt::Debug for Execution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRecoveryOwner")
            .field("running", &self.running.is_some())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}
impl Execution {
    pub(in crate::replica_journal) fn available(&self) -> usize {
        usize::from(!self.is_closed() && self.owner.is_some())
    }
    pub(in crate::replica_journal) fn is_closed(&self) -> bool {
        self.failed || self.closing || self.finished
    }
    pub(in crate::replica_journal) fn close(&mut self) {
        self.closing = true;
    }
    #[expect(
        clippy::result_large_err,
        reason = "refusal returns the original recovery action"
    )]
    pub(in crate::replica_journal) fn submit(&mut self, command: Command) -> Result<(), Command> {
        if self.available() == 0 {
            return Err(command);
        }
        self.running = Some(
            run(
                self.owner.take().expect("available recovery owner"),
                command,
            )
            .boxed_local(),
        );
        let _ = self.poll_work(&mut Context::from_waker(std::task::Waker::noop()));
        Ok(())
    }
    fn poll_work(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(running) = &mut self.running {
            let (owner, failed) = std::task::ready!(running.as_mut().poll(cx));
            self.running = None;
            self.owner = Some(owner);
            self.failed |= failed;
        }
        Poll::Ready(())
    }
    pub(in crate::replica_journal) fn poll_finished(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if self.finished {
            return Poll::Ready(self.failed);
        }
        std::task::ready!(self.poll_work(cx));
        if !self.is_closed() {
            return Poll::Pending;
        }
        if self.retain && !self.failed {
            self.finished = true;
            return Poll::Ready(false);
        }
        if self.shutdown.is_none() {
            let owner = self.owner.take().expect("settled recovery owner");
            self.shutdown = Some(async move { owner.shutdown().await }.boxed_local());
        }
        self.failed |= std::task::ready!(
            self.shutdown
                .as_mut()
                .expect("closing owner")
                .as_mut()
                .poll(cx)
        )
        .is_err();
        self.shutdown = None;
        self.finished = true;
        Poll::Ready(self.failed)
    }
}

/// Recovery-only command handle whose execution stays inside its actor. Retry
/// and adoption retain the same shared backend and injected timestamp source.
pub struct ShardRecoveringJournal {
    journal: ReplicaJournal,
    config: OwnedConfig,
    io: ozzy_io::Local,
    memory: Option<crate::memory::Owner>,
    scheduler: ShardJournalConfig,
    timestamp: Rc<dyn Fn() -> u64>,
}

impl std::fmt::Debug for ShardRecoveringJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRecoveringJournal")
            .field("journal", &self.journal)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ShardRecoveringJournal {
    /// Consume a freshly opened nonvoting owner. Outstanding transfer attempts
    /// must finish on their original owner; they cannot change execution mode.
    pub fn from_owned(
        owner: OwnedRecoveringJournal,
        scheduler: ShardJournalConfig,
        timestamp: impl Fn() -> u64 + 'static,
    ) -> Result<Self, JournalError> {
        Self::build(owner, scheduler, Rc::new(timestamp))
    }

    fn build(
        owner: OwnedRecoveringJournal,
        scheduler: ShardJournalConfig,
        timestamp: Rc<dyn Fn() -> u64>,
    ) -> Result<Self, JournalError> {
        if scheduler.commands < 2
            || scheduler.commands > 65536
            || scheduler.write_depth == 0
            || scheduler.write_depth > 64
            || scheduler.turn_steps == 0
            || scheduler.turn_steps > 1024
            || scheduler.roll_probes == 0
        {
            return Err(JournalError::Configuration);
        }
        let (config, io, generations, buffers) = owner.shard_parts()?;
        let memory = owner.append_memory.clone();
        let journal = ReplicaJournal {
            backlog: config.writeback,
            replicated: config.configuration.configuration().policy()
                == ozzy_replication::QuorumPolicy::Replicated,
            capacity: Arc::new(Semaphore::new(scheduler.commands)),
            command_capacity: scheduler.commands,
            read_capacity: Arc::new(Semaphore::new(0)),
            buffers,
            append_memory: memory.as_ref().map(crate::memory::Owner::allocator),
            append_limits: config.append_limits,
            operations: config.limits.operations,
            buffer_generation: generations.attempt,
            execution: crate::replica_journal::execution::Execution::Recovery(Execution {
                running: None,
                retain: false,
                owner: Some(Box::new(owner)),
                failed: false,
                closing: false,
                finished: false,
                shutdown: None,
            }),
        };
        Ok(Self {
            journal,
            config,
            io,
            memory,
            scheduler,
            timestamp,
        })
    }

    async fn take_owner(&mut self) -> Result<OwnedRecoveringJournal, JournalError> {
        self.journal.execution.recovery().retain = true;
        self.journal.shutdown().await?;
        self.journal
            .execution
            .recovery()
            .owner
            .take()
            .map(|owner| *owner)
            .ok_or(JournalError::Faulted)
    }
}

async fn run(
    mut owner: Box<OwnedRecoveringJournal>,
    command: Command,
) -> (Box<OwnedRecoveringJournal>, bool) {
    let Command {
        action,
        _permit: permit,
    } = command;
    let Action::Receiving(action) = action else {
        unreachable!("recovery admits only transfer work");
    };
    let failed = match action {
        ReceiveAction::Begin {
            ticket,
            config,
            done,
        } => finish(done, owner.begin_recovery(ticket, config).await, permit),
        ReceiveAction::Chunk {
            ticket,
            buffer,
            done,
        } => finish(done, owner.receive_chunk(ticket, buffer).await, permit),
        ReceiveAction::Finish { ticket, done } => {
            finish(done, owner.finish_recovery(ticket).await, permit)
        }
        ReceiveAction::Abort { ticket, done } => {
            finish(done, owner.abort_recovery(ticket).await, permit)
        }
    };
    let failed = failed || owner.is_faulted();
    (owner, failed)
}

impl RecoveryStorage for ShardRecoveringJournal {
    fn lease_append_buffer(&self) -> Result<AppendBuffer, SubmitError> {
        self.journal.lease_append_buffer()
    }
    fn begin_recovery(
        &mut self,
        ticket: RecoveryTicket,
        config: InstallationConfig,
    ) -> Result<JournalCompletion<RecoveryPlan>, SubmitError> {
        self.journal
            .submit(
                ticket,
                |ticket, done| {
                    Action::Receiving(ReceiveAction::Begin {
                        ticket,
                        config,
                        done,
                    })
                },
                |action| match action {
                    Action::Receiving(ReceiveAction::Begin { ticket, .. }) => ticket,
                    _ => unreachable!("preserved action"),
                },
            )
            .map_err(|error| error.reason)
    }
    fn receive_chunk(
        &mut self,
        ticket: RecoveryTicket,
        buffer: AppendBuffer,
    ) -> Result<JournalCompletion<ReceivedChunk>, Rejected<AppendBuffer>> {
        self.journal.submit(
            buffer,
            |buffer, done| {
                Action::Receiving(ReceiveAction::Chunk {
                    ticket,
                    buffer,
                    done,
                })
            },
            |action| match action {
                Action::Receiving(ReceiveAction::Chunk { buffer, .. }) => buffer,
                _ => unreachable!("preserved action"),
            },
        )
    }
    fn finish_recovery(
        &mut self,
        ticket: RecoveryTicket,
    ) -> Result<JournalCompletion<PublishedRecovery>, SubmitError> {
        self.journal
            .submit(
                ticket,
                |ticket, done| Action::Receiving(ReceiveAction::Finish { ticket, done }),
                |action| match action {
                    Action::Receiving(ReceiveAction::Finish { ticket, .. }) => ticket,
                    _ => unreachable!("preserved action"),
                },
            )
            .map_err(|error| error.reason)
    }
    fn abort_recovery(
        &mut self,
        ticket: RecoveryTicket,
    ) -> Result<JournalCompletion<RecoveryTicket>, SubmitError> {
        self.journal
            .submit(
                ticket,
                |ticket, done| Action::Receiving(ReceiveAction::Abort { ticket, done }),
                |action| match action {
                    Action::Receiving(ReceiveAction::Abort { ticket, .. }) => ticket,
                    _ => unreachable!("preserved action"),
                },
            )
            .map_err(|error| error.reason)
    }
    fn poll_stopped(&mut self, cx: &mut Context<'_>) -> Poll<JournalError> {
        self.journal.poll_stopped(cx)
    }
    async fn shutdown(&mut self) -> Result<(), JournalError> {
        self.journal.shutdown().await
    }
    async fn restart(
        mut self,
        full: bool,
        generations: OwnedRecoveryGenerations,
    ) -> Result<(Self, RecoveryStartup), JournalError> {
        self.journal.shutdown().await?;
        let mode = if full {
            OwnedRecoveryOpen::ResumeFull
        } else {
            OwnedRecoveryOpen::Resume
        };
        let (mut owner, startup) =
            OwnedRecoveringJournal::start(self.config, self.io, generations, mode).await?;
        if let Some(memory) = self.memory {
            owner.bind_append_source(memory)?;
        }
        Ok((Self::build(owner, self.scheduler, self.timestamp)?, startup))
    }
    async fn adopt(
        mut self,
        recovery: &mut Recovery,
        publication: PublishedRecovery,
        abandoned: bool,
        generation: JournalGeneration,
    ) -> Result<(ReplicaJournal, JournalStartup), JournalError> {
        let owner = self.take_owner().await?;
        let (owner, startup) = if abandoned {
            owner.shutdown().await?;
            let (mut owner, startup) = OwnedJournal::open(self.config, self.io, generation).await?;
            if let Some(memory) = self.memory {
                owner.bind_append_source(memory)?;
            }
            (owner, startup)
        } else {
            owner
                .into_journal(recovery, publication, generation)
                .await?
        };
        let timestamp = self.timestamp;
        Ok((
            owner.into_shard_journal(self.scheduler, move || timestamp())?,
            startup,
        ))
    }
}
