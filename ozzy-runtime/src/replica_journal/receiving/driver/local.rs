//! Serial recovery commands polled by their nonvoting partition actor.

use super::RecoveryStorage;
use crate::replica_journal::{
    AppendBuffer, InstallationConfig, JournalCompletion, JournalError, JournalExecution,
    JournalStartup, OwnedConfig, OwnedJournal, OwnedRecoveringJournal, OwnedRecoveryGenerations,
    OwnedRecoveryOpen, PublishedRecovery, ReceivedChunk, RecoveryPlan, RecoveryStartup, Rejected,
    ReplicaJournal, ShardJournal, ShardJournalConfig, SubmitError, WritePipelineConfig,
    commands::{Action, Command, finish},
    receiving::ReceiveAction,
};
use futures::{FutureExt, future::LocalBoxFuture};
use ozzy_replication::{
    JournalGeneration,
    recovery::{Recovery, RecoveryTicket},
};
use std::{
    cell::Cell,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::Semaphore;

type Returned = Result<Option<OwnedRecoveringJournal>, JournalError>;

struct Execution {
    running: Option<LocalBoxFuture<'static, Returned>>,
    retain: Rc<Cell<bool>>,
    owner: Option<OwnedRecoveringJournal>,
    failed: bool,
}

impl std::fmt::Debug for Execution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardRecoveryExecution")
            .field("running", &self.running.is_some())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl JournalExecution for Execution {
    fn poll_finished(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        if let Some(running) = &mut self.running {
            let result = std::task::ready!(running.as_mut().poll(cx));
            self.running = None;
            match result {
                Ok(owner) => self.owner = owner,
                Err(_) => self.failed = true,
            }
        }
        Poll::Ready(self.failed)
    }
}

/// Recovery-only command handle whose execution stays inside its actor. Retry
/// and adoption retain the same shared backend and injected timestamp source.
pub struct ShardRecoveringJournal {
    journal: ReplicaJournal<Execution>,
    config: OwnedConfig,
    io: ozzy_io::Local,
    memory: Option<crate::memory::AllocationSource>,
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
        let (sender, receiver) = crate::command_channel::notified_channel(scheduler.commands);
        let retain = Rc::new(Cell::new(false));
        let journal = ReplicaJournal {
            write_pipeline: WritePipelineConfig::for_backlog(config.writeback),
            replicated: config.configuration.configuration().policy()
                == ozzy_replication::QuorumPolicy::Replicated,
            sender: Some(sender),
            capacity: Arc::new(Semaphore::new(scheduler.commands)),
            command_capacity: scheduler.commands,
            read_capacity: Arc::new(Semaphore::new(0)),
            buffers,
            append_memory: memory
                .as_ref()
                .map(crate::memory::AllocationSource::allocator),
            append_limits: config.append_limits,
            operations: config.limits.operations,
            buffer_generation: generations.attempt,
            execution: Execution {
                running: Some(run(owner, receiver, retain.clone()).boxed_local()),
                retain,
                owner: None,
                failed: false,
            },
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
        self.journal.execution.retain.set(true);
        self.journal.shutdown().await?;
        self.journal
            .execution
            .owner
            .take()
            .ok_or(JournalError::Faulted)
    }
}

async fn run(
    mut owner: OwnedRecoveringJournal,
    mut receiver: crate::command_channel::NotifiedReceiver<Command>,
    retain: Rc<Cell<bool>>,
) -> Returned {
    while let Ok(Command {
        action,
        _permit: permit,
    }) = receiver.recv_async().await
    {
        let Action::Receiving(action) = action else {
            return Err(JournalError::Configuration);
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
        if failed || owner.is_faulted() {
            return Err(JournalError::Faulted);
        }
        crate::replica_journal::shard::yield_turn().await;
    }
    if retain.get() {
        Ok(Some(owner))
    } else {
        owner.shutdown().await?;
        Ok(None)
    }
}

impl RecoveryStorage for ShardRecoveringJournal {
    type Normal = ShardJournal;
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
    ) -> Result<(ReplicaJournal<Self::Normal>, JournalStartup), JournalError> {
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
