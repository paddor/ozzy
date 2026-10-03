//! Journal and canonical state owned directly by an application-shard actor.
//! No command lane, task, file descriptor or execution pool is created here.

mod config;
pub use config::OwnedConfig;
mod local;
mod mode;
use mode::Mode;
mod donor;
mod history;
mod identities;
pub use donor::{CompletedRecoveryRead, PreparedRecoveryRead};
mod install;
mod maintenance;
pub use maintenance::{
    CleanedStorage, CompletedStorageValidation, PreparedStorageValidation, StorageCleanup,
};
mod ownership;
mod producer_session;
mod proposal;
mod read;
pub use read::{CompletedDelivery, CompletedRead, PartitionDelivery, PreparedRead};
mod replay;
pub use replay::{CompletedReplay, PreparedReplay};
mod receiving;
pub use receiving::{RecoveringJournal, RecoveryGenerations, RecoveryOpen};
mod validation;
mod writeback;
pub use writeback::{
    CompletedRoll, CompletedSync, CompletedWrite, PreparedRoll, PreparedSync, PreparedWrite,
    WriteStep, WrittenRecords,
};

use std::sync::Arc;

use ownership::JournalOwner;

use ozzy_core::state::CanonicalImages;
use ozzy_io::Local;
use ozzy_journal_segment::{
    AsyncCanonicalRecoveryCandidate, AsyncGroupJournal, AsyncJournalFormat, AsyncJournalHistory,
    AsyncJournalIdentityIndex, AsyncJournalLimits, CanonicalRecoveryLimits, CommitMode,
    SegmentHeader,
};
use ozzy_replication::{
    Digest, FrozenLog, JournalGeneration, PipelineLimits, Prefix, PromiseTicket, QuorumPolicy,
    RecoveredState, Scope,
};
use tokio::sync::Semaphore;

use super::{
    AppendBuffer, JournalError, JournalStartup, MAX_APPEND_OPERATIONS, SubmitError, authority,
};

/// Exclusive shard-local storage state. Futures borrow this owner while disk
/// jobs execute elsewhere. Poll them alongside unrelated actors and timers.
/// Failed or canceled mutations fence further use until reopen.
///
/// Storage work is prepared on this owner, executed as independent futures,
/// then installed here. `into_shard_journal` makes the existing `ReplicaActor`
/// poll these operations locally through its bounded command contract.
#[derive(Debug)]
pub struct OwnedJournal {
    journal: JournalOwner,
    configuration: Mode,
    scope: Scope,
    images: Option<CanonicalImages<AsyncJournalIdentityIndex>>,
    selected: Option<AsyncCanonicalRecoveryCandidate>,
    history: Option<AsyncJournalHistory>,
    replay: replay::Replay,
    storage_validation: maintenance::Validation,
    donors: [Option<donor::Source>; 3],
    applied: Prefix,
    pending: std::collections::VecDeque<Prefix>,
    writeback: writeback::Writeback,
    reader: Option<ozzy_journal_segment::AsyncJournalPartitionIndex>,
    read_limits: ozzy_journal_segment::AsyncPartitionReadLimits,
    read_key: std::rc::Rc<()>,
    limits: AsyncJournalLimits,
    recovery: CanonicalRecoveryLimits,
    buffer_generation: JournalGeneration,
    buffers: Arc<Semaphore>,
    append_memory: Option<crate::memory::AllocationSource>,
    append_leased: std::cell::Cell<bool>,
    append_limits: PipelineLimits,
    faulted: bool,
}

impl OwnedJournal {
    /// Transfer this owner into an actor-polled adapter. No journal task or
    /// thread is spawned. The caller supplies wall-clock timestamps; tests can
    /// inject a deterministic clock. Backend workers remain shared by shards.
    pub fn into_shard_journal(
        self,
        config: super::ShardJournalConfig,
        timestamp: impl Fn() -> u64 + 'static,
    ) -> Result<super::ReplicaJournal<super::ShardJournal>, JournalError> {
        self.healthy()?;
        self.writeback.require_idle()?;
        self.journal.ready()?;
        if config.commands < 2
            || config.commands > 65536
            || config.write_depth == 0
            || config.write_depth > 64
            || config.turn_steps == 0
            || config.turn_steps > 1024
            || config.roll_probes == 0
            || self.recovery.retained_identities < self.append_limits.max_operations
        {
            return Err(JournalError::Configuration);
        }
        let (sender, receiver) = super::mpsc::notified_channel(config.commands);
        let mut write_pipeline = super::WritePipelineConfig::for_backlog(self.writeback.limits);
        write_pipeline.write_group_target_bytes = self.writeback.group_bytes;
        write_pipeline.aio_depth = config.write_depth;
        let journal = super::ReplicaJournal {
            write_pipeline,
            replicated: self.configuration.memory_voting(),
            sender: Some(sender),
            capacity: Arc::new(Semaphore::new(config.commands)),
            command_capacity: config.commands,
            read_capacity: Arc::new(Semaphore::new(self.read_limits.concurrent_reads)),
            buffers: self.buffers.clone(),
            append_memory: self
                .append_memory
                .as_ref()
                .map(crate::memory::AllocationSource::allocator),
            append_limits: self.append_limits,
            operations: self.limits.operations,
            buffer_generation: self.buffer_generation,
            execution: super::ShardJournal::new(self, receiver, config, timestamp),
        };
        Ok(journal)
    }

    /// Format an absent store and establish fresh bootstrap evidence. The
    /// caller injects a fresh writer generation; simulation need not use UUIDs.
    pub async fn format_new(
        config: OwnedConfig,
        io: Local,
        generation: JournalGeneration,
        capacity: u64,
    ) -> Result<(Self, JournalStartup), JournalError> {
        config.validate(generation)?;
        let configuration = config.configuration.configuration();
        let first_segment =
            SegmentHeader::new(config.identity.group_id, 1, None, Digest::ZERO, capacity)
                .map_err(ozzy_journal_segment::DirectoryError::from)?;
        let journal = AsyncGroupJournal::format(
            config.root.clone(),
            io,
            AsyncJournalFormat {
                identity: config.identity,
                configuration_epoch: configuration.scope().configuration_epoch,
                commit_mode: CommitMode::External,
                first_segment,
                configuration: config.configuration.encode().to_vec(),
            },
            generation,
            config.limits,
        )
        .await?;
        Box::pin(Self::start(config, journal, generation, true)).await
    }

    /// Open exact existing authority. Reject unsupported history before mutable
    /// segment recovery. Even an empty intact restart remains fenced for election.
    pub async fn open(
        config: OwnedConfig,
        io: Local,
        generation: JournalGeneration,
    ) -> Result<(Self, JournalStartup), JournalError> {
        config.validate(generation)?;
        let bytes = config.configuration.encode();
        let opening = AsyncGroupJournal::prepare_open(
            config.root.clone(),
            io,
            config.identity,
            Some(&bytes),
            config.limits,
        )
        .await?;
        authority::validate_manifest(
            opening.manifest(),
            config
                .configuration
                .configuration()
                .scope()
                .configuration_epoch,
            config.limits.io.max_segment_bytes,
        )?;
        let journal = opening.recover(generation).await?;
        Box::pin(Self::start(config, journal, generation, false)).await
    }

    async fn start(
        config: OwnedConfig,
        mut journal: AsyncGroupJournal,
        generation: JournalGeneration,
        fresh: bool,
    ) -> Result<(Self, JournalStartup), JournalError> {
        let configuration = config.configuration.configuration();
        let candidate = journal.recover_canonical_candidate(config.recovery).await?;
        let accepted = prefix(journal.accepted_position()?);
        if configuration.policy() == QuorumPolicy::Replicated {
            if !fresh {
                journal.require_drained_memory_history().await?;
            }
            journal.mark_memory_voting_running().await?;
        }
        let images = if fresh {
            Some(candidate.activate(&journal)?)
        } else {
            None
        };
        let manifest = journal.manifest();
        let scope = Scope {
            view: manifest.promised_view,
            ..configuration.scope()
        };
        let startup = JournalStartup {
            configuration,
            local: config.identity.replica_node_id,
            generation,
            recovered: (!fresh).then_some(RecoveredState {
                scope,
                log: FrozenLog {
                    last_normal_view: manifest.last_normal_view,
                    accepted,
                    committed: prefix(journal.committed_position()?),
                },
            }),
        };
        let reader = if fresh {
            Some(
                ozzy_journal_segment::AsyncJournalPartitionIndex::open(&journal, config.reads)
                    .await?,
            )
        } else {
            None
        };
        Ok((
            Self::from_open(
                &config,
                Mode::Replicated(configuration),
                journal,
                images,
                reader,
            )?,
            startup,
        ))
    }

    fn from_open<C>(
        config: &OwnedConfig<C>,
        configuration: Mode,
        journal: AsyncGroupJournal,
        images: Option<CanonicalImages<AsyncJournalIdentityIndex>>,
        reader: Option<ozzy_journal_segment::AsyncJournalPartitionIndex>,
    ) -> Result<Self, JournalError> {
        let scope = Scope {
            view: journal.manifest().promised_view,
            ..configuration.scope()
        };
        let applied = prefix(journal.committed_position()?);
        let generation = journal.writer().durable_position().generation();
        Ok(Self {
            journal: JournalOwner::Ready(Box::new(journal)),
            configuration,
            scope,
            images,
            selected: None,
            history: None,
            replay: replay::Replay::default(),
            storage_validation: maintenance::Validation::default(),
            donors: std::array::from_fn(|_| None),
            applied,
            pending: std::collections::VecDeque::new(),
            writeback: writeback::Writeback::new(config.writeback, config.write_group_bytes),
            reader,
            read_limits: config.reads,
            read_key: std::rc::Rc::new(()),
            limits: config.limits,
            recovery: config.recovery,
            buffer_generation: generation,
            buffers: Arc::new(Semaphore::new(config.append_buffers)),
            append_memory: None,
            append_leased: std::cell::Cell::new(false),
            append_limits: config.append_limits,
            faulted: false,
        })
    }

    /// Installed scope only. Pending metadata publication cannot advance it.
    pub const fn scope(&self) -> Scope {
        self.scope
    }

    /// Lease a bounded reusable arena. Leases remain valid across installation
    /// generations of this owner, never across an owner restart.
    pub fn lease_append_buffer(&self) -> Result<AppendBuffer, SubmitError> {
        if self.is_faulted() {
            return Err(SubmitError::Stopped);
        }
        let lease = self
            .buffers
            .clone()
            .try_acquire_owned()
            .map_err(|_| SubmitError::Full)?;
        self.append_leased.set(true);
        Ok(AppendBuffer::new_with_memory(
            self.buffer_generation,
            self.append_limits,
            lease,
            self.append_memory
                .as_ref()
                .map(crate::memory::AllocationSource::allocator),
        ))
    }

    /// Bind canonical arenas to a shard's shared payload owner before leasing.
    /// Leases are lazy. Storage and transport aliases keep physical bytes charged.
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

    /// Inspect recovered canonical state only after this owner has activated it.
    /// Intact restart and election promises expose no old normal authority.
    pub fn images(&self) -> Result<&CanonicalImages<AsyncJournalIdentityIndex>, JournalError> {
        self.healthy()?;
        self.images
            .as_ref()
            .ok_or(JournalError::InstallationMismatch)
    }

    /// A canceled mutation is uncertain even if its physical work later succeeds.
    pub fn is_faulted(&self) -> bool {
        self.faulted || self.journal.is_faulted() || self.writeback.is_faulted()
    }

    fn healthy(&self) -> Result<(), JournalError> {
        if self.is_faulted() {
            Err(JournalError::Faulted)
        } else {
            Ok(())
        }
    }

    /// Persist only this writer's exact frozen history and promised view.
    /// The returned ticket, after all barriers, may complete the replica driver.
    pub async fn persist_promise(
        &mut self,
        ticket: PromiseTicket,
    ) -> Result<PromiseTicket, JournalError> {
        self.healthy()?;
        self.configuration.replicated()?;
        self.writeback.require_idle()?;
        self.faulted = true;
        let journal = self.journal.ready()?;
        let next = authority::promise_manifest(
            self.scope,
            journal.manifest(),
            journal.writer().written_position(),
            journal.writer().durable_position(),
            prefix(journal.accepted_position()?),
            ticket,
        )?;
        if let Some(next) = next {
            self.images = None;
            self.reader = None;
            self.replay.clear();
            self.storage_validation.clear();
            self.donors = std::array::from_fn(|_| None);
            self.selected = None;
            self.journal.ready_mut()?.install_metadata(next).await?;
            self.scope = ticket.scope();
            self.pending.clear();
        }
        self.faulted = false;
        Ok(ticket)
    }

    /// After the actor stops voting and admission, synchronize its installed
    /// history and close asynchronously. The shared backend remains available
    /// to other partitions. Dropping this future supplies no shutdown evidence.
    pub fn shutdown(mut self) -> impl std::future::Future<Output = Result<(), JournalError>> {
        // Cold consuming transition. Keep the owner's canonical images out of
        // every caller's inline future layout; normal operations stay unboxed.
        Box::pin(async move {
            self.healthy()?;
            self.writeback.require_idle()?;
            if let JournalOwner::Installing(pending) = &self.journal {
                let ticket = pending.ticket;
                Box::pin(self.abort_installation(ticket)).await?;
            }
            // Release derived readers before draining directory protection.
            self.history = None;
            self.reader = None;
            self.replay.clear();
            self.storage_validation.clear();
            self.donors = std::array::from_fn(|_| None);
            self.images = None;
            self.selected = None;
            let mut journal = self.journal.take_ready()?;
            if self.configuration.memory_voting() {
                journal.publish_drained_memory_history().await?;
            }
            journal.close().await?;
            Ok(())
        })
    }
}

fn prefix(position: ozzy_journal_segment::LogPosition) -> Prefix {
    Prefix {
        op: ozzy_replication::OpNumber(position.op_number),
        digest: position.digest,
    }
}

#[cfg(test)]
mod tests;
