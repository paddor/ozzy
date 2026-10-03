//! Replica journal admission, state, and execution adapters.
//!
//! `ShardJournal` runs beside its partition actor and sends filesystem work to
//! shared backend workers. Commands and completions are bounded.

mod append;
mod authority;
mod canonical;
mod commands;
mod execution;
pub use execution::JournalExecution;
mod shard;
pub use shard::{ShardJournal, ShardJournalConfig};
mod history;
mod install;
mod owned;
pub use owned::{
    CleanedStorage as OwnedCleanedStorage, CompletedDelivery as OwnedCompletedDelivery,
    CompletedRead as OwnedCompletedRead, CompletedRecoveryRead as OwnedCompletedRecoveryRead,
    CompletedReplay as OwnedCompletedReplay, CompletedRoll as OwnedCompletedRoll,
    CompletedStorageValidation as OwnedCompletedStorageValidation,
    CompletedSync as OwnedCompletedSync, CompletedWrite as OwnedCompletedWrite, OwnedConfig,
    OwnedJournal, PartitionDelivery as OwnedPartitionDelivery, PreparedRead as OwnedPreparedRead,
    PreparedRecoveryRead as OwnedPreparedRecoveryRead, PreparedReplay as OwnedPreparedReplay,
    PreparedRoll as OwnedPreparedRoll, PreparedStorageValidation as OwnedPreparedStorageValidation,
    PreparedSync as OwnedPreparedSync, PreparedWrite as OwnedPreparedWrite,
    RecoveringJournal as OwnedRecoveringJournal, RecoveryGenerations as OwnedRecoveryGenerations,
    RecoveryOpen as OwnedRecoveryOpen, StorageCleanup as OwnedStorageCleanup,
    WriteStep as OwnedWriteStep, WrittenRecords as OwnedWrittenRecords,
};
mod producer;
mod proposal;
mod read;
mod receiving;
mod records;
mod recovery;
mod sync;
mod turn;
mod validation;
pub use validation::ValidatedStorage;

pub use append::{AdmittedAppend, AppendBuffer, MAX_APPEND_OPERATIONS, ValidatedAppend};
use commands::Command;
pub use commands::Rejected;
pub use history::{FetchedHistory, HistoryPosition, ReplicationPositions};
pub use install::{InstallationConfig, InstalledChunk, InstalledJournal};
pub use proposal::{AppendAdmissionError, ProducerAppend, ProposalBuffer, ProposalValidation};
pub(crate) use read::PartitionReadBuffer;
pub(crate) use read::delivery::ReadDelivery;
pub use read::{
    PartitionReadCursor, PartitionReadError, PartitionReadLease, PartitionReadLimits, ReadPartition,
};
pub use receiving::{
    PublishedRecovery, ReceivedChunk, RecoveryPlan, RecoveryStartup, RecoveryStorage,
    ShardRecoveringJournal,
};
pub use recovery::PinnedRecovery;
pub use sync::ReadyJournalSync;
pub use turn::{Turn, TurnResult};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use ozzy_journal::operation::OperationLimits;
use ozzy_proto::NodeId;
use ozzy_replication::driver::{DriverError, ReplicaDriver, Timing};
use ozzy_replication::{
    Configuration, JournalGeneration, NormalReplica, PipelineLimits, PromiseTicket, RecoveredState,
    ViewChange,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::command_channel as mpsc;
use crate::completion;

/// Who performs segment data writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    /// The device writer pool, with blocking syscalls. Default off Linux.
    Pool,
    /// The journal owner, through Linux kernel AIO, up to `aio_depth` batches
    /// in flight per shard. Requires `direct_io`. Default on Linux.
    Aio,
}

impl Default for IoBackend {
    fn default() -> Self {
        if cfg!(target_os = "linux") {
            Self::Aio
        } else {
            Self::Pool
        }
    }
}

/// Fixed RAM backlog and physical grouping target for the device writer.
#[derive(Debug, Clone, Copy)]
pub struct WritePipelineConfig {
    /// Canonical operations and bytes admitted but not yet installed as written.
    /// Excludes the kernel's dirty page cache; writeback throttling propagates
    /// through this bounded queue. Independent of command slots and chunk size.
    pub backlog: PipelineLimits,
    /// Uncompressed write-group target. One larger permitted operation stays intact.
    pub write_group_target_bytes: usize,
    /// Dedicated CPU workers for LZ4 groups. Zero encodes on the journal owner.
    /// Raw groups use the owner directly. Independent of physical write concurrency.
    pub compression_workers: usize,
    /// Optional bytes per buffered write syscall. Groups still complete atomically
    /// at the owner; this does not change compression or record boundaries.
    pub write_call_bytes: Option<std::num::NonZeroUsize>,
    /// Zero the active segment's unused remainder at start and the next segment
    /// ahead of its roll (`O_DSYNC` journals only), so writes skip the
    /// filesystem's unwritten-extent conversion. Off by default: it doubles
    /// device writes, which costs more than the conversion when bandwidth is
    /// the limit.
    ///
    /// TODO: reuse retired segment files as successors instead of zeroing new
    /// ones, so their blocks are already written without extra device writes.
    pub zero_ahead: bool,
    /// Write segment groups through a separate `O_DIRECT` descriptor, bypassing
    /// the page cache. Each batch is copied into 4 KiB-aligned staging. On by
    /// default on Linux; the file system must support direct I/O.
    pub direct_io: bool,
    /// Who performs segment data writes. Rolls, zeroing and `DURABLE`
    /// publication stay on the device writer pool.
    pub io_backend: IoBackend,
    /// Kernel AIO data writes in flight per shard, 1 to 64. Default 1. More
    /// can overlap synced writes. A power loss can leave a hole; recovery
    /// discards that hole and all later writes only after the durable position.
    ///
    /// TODO: treat the configured or measured depth as a ceiling. Reduce active
    /// depth when write latency worsens, then climb slowly back to just below
    /// that ceiling.
    pub aio_depth: usize,
}

impl WritePipelineConfig {
    /// Default grouping and workers for an explicit backlog. The write-group
    /// target never exceeds the backlog's bytes.
    pub fn for_backlog(backlog: PipelineLimits) -> Self {
        let default = Self::default();
        Self {
            backlog,
            write_group_target_bytes: default.write_group_target_bytes.min(backlog.max_body_bytes),
            ..default
        }
    }
}

impl Default for WritePipelineConfig {
    fn default() -> Self {
        Self {
            // Holds an incompressible 3 GB/s stream through a ~35 ms roll.
            backlog: PipelineLimits {
                max_operations: 4096,
                max_body_bytes: 128 * 1024 * 1024,
            },
            write_group_target_bytes: 4 * 1024 * 1024,
            compression_workers: 0,
            write_call_bytes: None,
            zero_ahead: false,
            direct_io: cfg!(target_os = "linux"),
            io_backend: IoBackend::default(),
            aio_depth: 1,
        }
    }
}

/// Storage-validated startup evidence. Consumed once to create the protocol driver.
///
/// Intact reopen always takes the higher-view recovery path, including empty logs.
/// This value neither authenticates peers nor establishes a live quorum.
#[derive(Debug)]
pub struct JournalStartup {
    configuration: Configuration,
    local: NodeId,
    generation: JournalGeneration,
    recovered: Option<RecoveredState>,
}

impl JournalStartup {
    /// Configuration independently checked against the journal's durable bytes.
    pub const fn configuration(&self) -> Configuration {
        self.configuration
    }

    /// Persistent voter identity independently checked during journal startup.
    pub const fn local(&self) -> NodeId {
        self.local
    }

    /// Fresh writer incarnation for this start. Production uses UUIDs;
    /// controlled shard-local execution may inject deterministic generations.
    pub const fn generation(&self) -> JournalGeneration {
        self.generation
    }

    /// Stored intact history; `None` means successful explicit fresh format only.
    pub const fn recovered(&self) -> Option<RecoveredState> {
        self.recovered
    }

    /// Construct normal bootstrap or fenced intact restart, never old normal authority.
    /// An already promised but never installed election may resume its view.
    /// Supply process-relative time after startup, so disk recovery consumes no
    /// election deadline. Live pipeline allocation is independent of retained WAL size.
    pub fn into_driver(
        self,
        now: Duration,
        timing: Timing,
        limits: PipelineLimits,
    ) -> Result<ReplicaDriver, DriverError> {
        match self.recovered {
            None => ReplicaDriver::from_normal(
                NormalReplica::bootstrap(self.configuration, self.local, self.generation, limits)?,
                now,
                timing,
            ),
            Some(recovered) => ReplicaDriver::from_view_change(
                if self.configuration.policy() == ozzy_replication::QuorumPolicy::Replicated {
                    ViewChange::recover_drained(
                        self.configuration,
                        self.local,
                        self.generation,
                        recovered,
                        limits,
                    )?
                } else {
                    ViewChange::recover_intact(
                        self.configuration,
                        self.local,
                        self.generation,
                        recovered,
                        limits,
                    )?
                },
                now,
                timing,
            ),
        }
    }
}

/// An admitted disk action. Dropping its future does not cancel or undo disk I/O.
#[derive(Debug)]
pub struct JournalCompletion<T> {
    receiver: completion::Receiver<Result<T, JournalError>>,
}

impl<T> Future for JournalCompletion<T> {
    type Output = Result<T, JournalError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|result| result.unwrap_or(Err(JournalError::Stopped)))
    }
}

/// One bounded command lane whose execution is polled by the application actor.
///
/// Call `shutdown` before dropping the owning runtime or opening a successor.
/// It closes admission and awaits the shard task after draining admitted commands.
/// Canceling that wait retains the task handle for a later shutdown call.
/// Dropping the handle closes admission; shutdown completion must be awaited.
/// Its caller must poll execution alongside completion observers and drain
/// shutdown before releasing its shared backend.
#[derive(Debug)]
pub struct ReplicaJournal<E = ShardJournal> {
    write_pipeline: WritePipelineConfig,
    replicated: bool,
    sender: Option<mpsc::NotifiedSender<Command>>,
    capacity: Arc<Semaphore>,
    command_capacity: usize,
    read_capacity: Arc<Semaphore>,
    buffers: Arc<Semaphore>,
    append_memory: Option<crate::memory::Allocator>,
    append_limits: PipelineLimits,
    operations: OperationLimits,
    buffer_generation: JournalGeneration,
    execution: E,
}

impl<E> ReplicaJournal<E> {
    pub(crate) const fn write_pipeline(&self) -> WritePipelineConfig {
        self.write_pipeline
    }

    /// Replicated-persisting policy: confirmation precedes persistence.
    pub(crate) const fn replicated(&self) -> bool {
        self.replicated
    }
    pub(crate) const fn operation_limits(&self) -> OperationLimits {
        self.operations
    }

    pub(crate) fn available_command_slots(&self) -> usize {
        self.capacity.available_permits()
    }

    /// Configured outstanding command bound, including executing work.
    pub(crate) const fn command_capacity(&self) -> usize {
        self.command_capacity
    }
}

impl<E> ReplicaJournal<E> {
    /// Submit an exact core-issued promise without waiting for the worker or disk.
    /// Only successful completion permits `ReplicaDriver::complete_promise`.
    /// Any action error fences the worker and closes every queued completion.
    pub fn persist_promise(
        &mut self,
        ticket: PromiseTicket,
    ) -> Result<JournalCompletion<PromiseTicket>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| commands::Action::Promise { ticket, done },
            |action| match action {
                commands::Action::Promise { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    fn begin_shutdown(&mut self) {
        self.read_capacity.close();
        self.sender = None;
    }
}

impl<E: JournalExecution> ReplicaJournal<E> {
    pub(crate) fn validation_has_capacity(
        &self,
        buffer: &AppendBuffer,
        accepted: ozzy_replication::OpNumber,
    ) -> bool {
        let available = self.execution.control_capacity(accepted);
        // An exhausted overlay drains all intake so continuous APPEND traffic
        // cannot prevent the fully applied boundary needed by index refresh.
        available != 0
            && buffer
                .operations()
                .filter(|operation| {
                    operation.kind != ozzy_journal::operation::OperationKind::Append
                })
                .count()
                <= available
    }

    /// Drain admitted commands without blocking the calling application thread.
    /// Safe to cancel and call again; admission remains closed once shutdown begins.
    pub async fn shutdown(&mut self) -> Result<(), JournalError> {
        self.begin_shutdown();
        if std::future::poll_fn(|cx| self.execution.poll_finished(cx)).await {
            Err(JournalError::Faulted)
        } else {
            Ok(())
        }
    }

    /// Observe terminal background failure even without an outstanding command.
    /// Canceling this wait neither closes admission nor cancels storage work.
    pub(crate) async fn stopped(&mut self) -> JournalError {
        std::future::poll_fn(|cx| self.poll_stopped(cx)).await
    }

    pub(crate) fn poll_stopped(&mut self, cx: &mut Context<'_>) -> Poll<JournalError> {
        self.execution.poll_finished(cx).map(|failed| {
            if failed {
                JournalError::Faulted
            } else {
                JournalError::Stopped
            }
        })
    }
}

impl<E> Drop for ReplicaJournal<E> {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// Local admission backpressure, not an operation's persistence outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubmitError {
    /// Fixed command budget is occupied. Retry submission without changing the ticket.
    #[error("replica journal command queue full")]
    Full,
    /// Admission closed or the worker exited. Do not assume a pending action failed to persist.
    #[error("replica journal worker stopped")]
    Stopped,
}

/// Storage lifecycle/action failure. None permits replacement of an existing voter.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// Explicit single-broker authority rejected a storage transition.
    #[error(transparent)]
    Local(#[from] ozzy_replication::local::Error),
    /// A normal partition-read rejection, without changing the cursor or journal.
    #[error(transparent)]
    Read(#[from] PartitionReadError),
    /// Recovery admission, streamed validation, or fenced handoff rejected this evidence.
    #[error(transparent)]
    Recovery(#[from] ozzy_replication::recovery::RecoveryError),
    /// Complete replacement history could not be safely admitted as intact state.
    #[error(transparent)]
    RecoveryPublication(#[from] ozzy_journal_segment::RecoveryPublicationError),
    /// Producer identity, session, sequence, or retained retry validation failed.
    #[error(transparent)]
    ProducerAppend(#[from] AppendAdmissionError),
    /// Configured group/voter or resource bounds are invalid.
    #[error("invalid replica journal configuration")]
    Configuration,
    /// Wrong commit policy, missing full-WAL prefix, or unsupported checkpoint state.
    #[error("journal is outside the supported full-WAL replica profile")]
    UnsupportedHistory,
    /// Promise does not name this exact writer, configuration, view, and frozen history.
    #[error("replica promise does not match the live journal")]
    PromiseMismatch,
    /// Payload would exceed its configured arena bounds. No content was added.
    #[error("replica append buffer capacity exhausted")]
    AppendCapacity,
    /// Another segment would exceed configured manifest count/byte bounds.
    #[error("replica journal manifest capacity exhausted")]
    RollCapacity,
    /// Validation or write no longer matches this active application/journal image.
    #[error("replica append does not match the live journal image")]
    AppendMismatch,
    /// Sync/apply action does not name a covered prefix of this writer.
    #[error("replica completion does not match the live journal prefix")]
    CompletionMismatch,
    /// No exact captured source, a different source is still pinned, or a stale request scope.
    #[error("replica history request does not match the pinned source")]
    HistorySourceMismatch,
    /// Installation or activation does not match this worker's exact pending action.
    #[error("replica installation does not match the live journal action")]
    InstallationMismatch,
    /// Worker/response disappeared. An admitted I/O action may already have completed.
    #[error("replica journal worker stopped before reporting completion")]
    Stopped,
    /// Worker fenced on an action failure or panic. Reopen is required.
    #[error("replica journal worker faulted")]
    Faulted,
    /// Worker thread startup failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Exact journal validation or persistence failure.
    #[error(transparent)]
    Directory(#[from] ozzy_journal_segment::DirectoryError),
    /// Intact canonical history cannot be replayed under the application rules/limits.
    #[error(transparent)]
    Canonical(#[from] ozzy_journal_segment::CanonicalStateRecoveryError),
    /// Canonical body syntax is invalid. Validation has no write effects.
    #[error(transparent)]
    Operation(#[from] ozzy_journal::operation::OperationCodecError),
    /// Application validation or installation failed.
    #[error(transparent)]
    Images(#[from] ozzy_core::state::CanonicalImagesError),
    /// Bounded source capture/read failure. Corrupt or unreadable source faults the worker.
    #[error(transparent)]
    History(#[from] ozzy_journal_segment::HistoryError),
    /// Exact retry lookup failed. A missing/corrupt authoritative record fences the worker.
    #[error(transparent)]
    Index(#[from] ozzy_journal_segment::JournalIndexError),
    /// Persistent identity replacement failed exact lineage or claim validation.
    #[error(transparent)]
    IdentityHandoff(#[from] ozzy_journal_segment::JournalIdentityHandoffError),
    /// Selected-history staging/publication failed. Reopen is required.
    #[error(transparent)]
    Installation(#[from] ozzy_journal_segment::SuffixReplacementError),
}
