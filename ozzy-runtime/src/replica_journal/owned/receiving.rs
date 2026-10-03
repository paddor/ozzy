//! Nonvoting recovery owned by the application shard. All file work uses the
//! supplied backend; no normal append API or implicit formatting is available.

mod transfer;

use super::{
    AppendBuffer, AsyncGroupJournal, AsyncJournalFormat, CommitMode, Digest, JournalError,
    JournalGeneration, JournalStartup, Local, OwnedConfig, OwnedJournal, QuorumPolicy,
    SegmentHeader, SubmitError, authority,
};
use crate::replica_journal::{PublishedRecovery, RecoveryStartup, receiving::RecoveryPlan};
use ozzy_journal_segment::{
    AsyncRecoveryDirectory, AsyncSealedRepair, AsyncSuffixInstaller, SuffixStreamLimits,
};
use ozzy_replication::recovery::{Recovery, RecoveryError, RecoveryTicket};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Explicit startup intent. Existing broker data is never silently reformatted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOpen {
    /// Create an absent, marker-backed replacement store.
    FormatNew {
        /// Fixed capacity of the first, nonvoting segment file.
        segment_capacity: u64,
    },
    /// Resume an exact nonvoting marker, preferring bounded sealed-file repair.
    Resume,
    /// Resume the marker but discard selective repair in favor of full transfer.
    ResumeFull,
    /// Durably remove an existing store's voting eligibility; preserve old files.
    Quarantine,
}

/// Caller-supplied fresh identities. Simulation need not generate wall-clock UUIDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryGenerations {
    /// Binds the recovery core, transfer arenas and externally authorized writer.
    pub attempt: JournalGeneration,
    /// Distinct temporary writer used before fresh group authority arrives.
    pub temporary: JournalGeneration,
}

#[derive(Debug)]
enum State {
    Journal(Box<AsyncGroupJournal>),
    Directory(Box<AsyncRecoveryDirectory>),
    Repair(Box<AsyncSealedRepair>),
    Installing(Box<(AsyncSuffixInstaller, SuffixStreamLimits)>),
    Fenced,
}

/// Exclusive recovery-only owner. Poll its futures alongside other partition
/// actors. Any failed or canceled mutation fences this incarnation until reopen.
#[derive(Debug)]
pub struct RecoveringJournal {
    config: OwnedConfig,
    io: Local,
    generations: RecoveryGenerations,
    state: State,
    attempt: Option<RecoveryTicket>,
    published: Option<PublishedRecovery>,
    buffers: Arc<Semaphore>,
    pub(in crate::replica_journal) append_memory: Option<crate::memory::AllocationSource>,
    append_leased: std::cell::Cell<bool>,
    faulted: bool,
}

impl RecoveringJournal {
    pub(in crate::replica_journal) fn shard_parts(
        &self,
    ) -> Result<(OwnedConfig, Local, RecoveryGenerations, Arc<Semaphore>), JournalError> {
        self.healthy()?;
        if self.attempt.is_some() || self.published.is_some() {
            return Err(JournalError::CompletionMismatch);
        }
        Ok((
            self.config.clone(),
            self.io.clone(),
            self.generations,
            self.buffers.clone(),
        ))
    }

    /// Establish nonvoting ownership using explicit startup intent and identities.
    /// Quarantine validates the supported profile before changing eligibility.
    pub async fn start(
        config: OwnedConfig,
        io: Local,
        generations: RecoveryGenerations,
        mode: RecoveryOpen,
    ) -> Result<(Self, RecoveryStartup), JournalError> {
        config.validate(generations.attempt)?;
        if generations.temporary.0 == 0 || generations.attempt == generations.temporary {
            return Err(JournalError::Configuration);
        }
        let configuration = config.configuration.configuration();
        let bytes = config.configuration.encode();
        let state = if let RecoveryOpen::FormatNew { segment_capacity } = mode {
            if segment_capacity > config.limits.io.max_segment_bytes {
                return Err(JournalError::Configuration);
            }
            let first_segment = SegmentHeader::new(
                config.identity.group_id,
                1,
                None,
                Digest::ZERO,
                segment_capacity,
            )
            .map_err(ozzy_journal_segment::DirectoryError::from)?;
            let journal = AsyncGroupJournal::format_recovering(
                config.root.clone(),
                io.clone(),
                AsyncJournalFormat {
                    identity: config.identity,
                    configuration_epoch: configuration.scope().configuration_epoch,
                    commit_mode: CommitMode::External,
                    first_segment,
                    configuration: bytes.to_vec(),
                },
                generations.temporary,
                config.limits,
            )
            .await?;
            State::Journal(Box::new(journal))
        } else {
            let mut directory = if mode == RecoveryOpen::Quarantine {
                let directory = AsyncRecoveryDirectory::open_for_repair(
                    config.root.clone(),
                    io.clone(),
                    config.identity,
                    &bytes,
                    config.limits,
                )
                .await?;
                Self::validate_directory(&config, &directory)?;
                directory.quarantine_for_recovery(&bytes).await?
            } else {
                AsyncRecoveryDirectory::open_recovering(
                    config.root.clone(),
                    io.clone(),
                    config.identity,
                    &bytes,
                    config.limits,
                )
                .await?
            };
            Self::validate_directory(&config, &directory)?;
            let repair = mode != RecoveryOpen::ResumeFull
                && configuration.policy() == QuorumPolicy::Durable
                && directory
                    .sealed_damage(config.limits.io.max_segment_bytes)
                    .await?
                    .is_some_and(|ranges| !ranges.is_empty());
            if repair {
                State::Directory(Box::new(directory))
            } else {
                State::Journal(Box::new(
                    directory
                        .recover_nonvoting(
                            &bytes,
                            generations.temporary,
                            config.limits.directory_entries,
                        )
                        .await?,
                ))
            }
        };
        let startup = RecoveryStartup {
            configuration,
            local: config.identity.replica_node_id,
            generation: generations.attempt,
        };
        let buffers = Arc::new(Semaphore::new(config.append_buffers));
        Ok((
            Self {
                config,
                io,
                generations,
                state,
                attempt: None,
                published: None,
                buffers,
                append_memory: None,
                append_leased: std::cell::Cell::new(false),
                faulted: false,
            },
            startup,
        ))
    }

    fn validate_directory(
        config: &OwnedConfig,
        directory: &AsyncRecoveryDirectory,
    ) -> Result<(), JournalError> {
        authority::validate_manifest(
            directory.manifest(),
            config
                .configuration
                .configuration()
                .scope()
                .configuration_epoch,
            config.limits.io.max_segment_bytes,
        )
    }

    /// True after an interrupted or failed state-changing operation.
    pub const fn is_faulted(&self) -> bool {
        self.faulted
    }

    fn healthy(&self) -> Result<(), JournalError> {
        if self.faulted {
            Err(JournalError::Faulted)
        } else {
            Ok(())
        }
    }

    fn take_state(&mut self) -> State {
        std::mem::replace(&mut self.state, State::Fenced)
    }

    fn require_ticket(&self, ticket: RecoveryTicket) -> Result<(), JournalError> {
        if self.attempt != Some(ticket) {
            return Err(RecoveryError::StaleTransfer.into());
        }
        Ok(())
    }

    /// Lease one bounded transfer arena. The receiver never accepts another
    /// owner's arena, even when the records belong to the same group.
    pub fn lease_append_buffer(&self) -> Result<AppendBuffer, SubmitError> {
        if self.faulted {
            return Err(SubmitError::Stopped);
        }
        let permit = self
            .buffers
            .clone()
            .try_acquire_owned()
            .map_err(|_| SubmitError::Full)?;
        self.append_leased.set(true);
        Ok(AppendBuffer::new_with_memory(
            self.generations.attempt,
            self.config.append_limits,
            permit,
            self.append_memory
                .as_ref()
                .map(crate::memory::AllocationSource::allocator),
        ))
    }

    /// Share the application's canonical payload budget before any transfer lease.
    pub fn bind_append_memory(
        &mut self,
        memory: &crate::memory::Owner,
    ) -> Result<(), JournalError> {
        self.bind_append_source(crate::memory::AllocationSource::Shared(memory.clone()))
    }

    /// Use only allocation capacity explicitly reserved by the shared shard
    /// owner. The allowance survives recovery and is initially permitted to be
    /// empty. Reserve enough physical allocations before advertising intake.
    pub fn bind_append_capacity(
        &mut self,
        capacity: &crate::memory::Capacity,
    ) -> Result<(), JournalError> {
        self.bind_append_source(crate::memory::AllocationSource::Reserved(capacity.clone()))
    }

    pub(in crate::replica_journal) fn bind_append_source(
        &mut self,
        memory: crate::memory::AllocationSource,
    ) -> Result<(), JournalError> {
        if self.append_memory.is_some() || self.append_leased.get() {
            return Err(JournalError::Configuration);
        }
        self.append_memory = Some(memory);
        Ok(())
    }

    /// Retire unpublished work; retain the nonvoting marker and original files.
    /// This incarnation cannot begin a second attempt. Reopen with fresh IDs.
    pub async fn abort_recovery(
        &mut self,
        ticket: RecoveryTicket,
    ) -> Result<RecoveryTicket, JournalError> {
        self.healthy()?;
        self.faulted = true;
        self.require_ticket(ticket)?;
        self.state = match self.take_state() {
            State::Repair(repair) => State::Directory(Box::new(repair.abort())),
            State::Installing(staging) => State::Journal(Box::new(staging.0.abort().await?)),
            other => {
                self.state = other;
                return Err(RecoveryError::StaleTransfer.into());
            }
        };
        self.faulted = false;
        Ok(ticket)
    }

    /// Validate this owner's exact publication against the live recovery core,
    /// then reopen for fenced election, never same-view voting. Release transfer
    /// arenas first. The new incarnation must differ from both recovery writers.
    pub fn into_journal(
        mut self,
        recovery: &mut Recovery,
        publication: PublishedRecovery,
        generation: JournalGeneration,
    ) -> impl std::future::Future<Output = Result<(OwnedJournal, JournalStartup), JournalError>> + '_
    {
        Box::pin(async move {
            self.healthy()?;
            self.require_ticket(publication.ticket)?;
            if self.published != Some(publication)
                || generation.0 == 0
                || generation == self.generations.attempt
                || generation == self.generations.temporary
                || self.buffers.available_permits() != self.config.append_buffers
            {
                return Err(RecoveryError::StaleTransfer.into());
            }
            let recovered = if let Some(repaired) = publication.repaired {
                recovery.complete_repair(publication.ticket, repaired)?
            } else {
                recovery.complete(
                    publication.ticket,
                    publication.ticket.source().accepted,
                    publication.applied,
                )?
            };
            let State::Journal(journal) = self.take_state() else {
                return Err(JournalError::Faulted);
            };
            journal.close().await?;
            let (mut journal, startup) =
                OwnedJournal::open(self.config, self.io, generation).await?;
            if let Some(memory) = self.append_memory {
                journal.bind_append_source(memory)?;
            }
            if startup.recovered() != Some(recovered) {
                journal.shutdown().await?;
                return Err(JournalError::CompletionMismatch);
            }
            Ok((journal, startup))
        })
    }

    /// Close healthy recovery state. Abandoning an unfinished attempt grants no
    /// restart authority. Shared backend shutdown still drains deferred closes.
    pub fn shutdown(mut self) -> impl std::future::Future<Output = Result<(), JournalError>> {
        Box::pin(async move {
            self.healthy()?;
            match self.take_state() {
                State::Journal(journal) => journal.close().await?,
                State::Installing(staging) => staging.0.abort().await?.close().await?,
                State::Repair(repair) => drop(repair.abort()),
                State::Directory(directory) => drop(directory),
                State::Fenced => return Err(JournalError::Faulted),
            }
            Ok(())
        })
    }
}
