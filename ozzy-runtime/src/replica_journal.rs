//! Replica journal admission, state, and execution adapters.
//!
//! `ShardJournal` runs beside its partition actor and sends filesystem work to
//! shared backend workers. Commands and completions are bounded.

mod append;
mod authority;
mod canonical;
mod commands;
mod execution;
mod shard;
pub use shard::{ShardJournal, ShardJournalConfig};
mod history;
mod install;
mod owned;
pub use owned::{
    CleanedStorage as OwnedCleanedStorage, CompletedCheckpointRead as OwnedCompletedCheckpointRead,
    CompletedDelivery as OwnedCompletedDelivery, CompletedRead as OwnedCompletedRead,
    CompletedRecoveryRead as OwnedCompletedRecoveryRead, CompletedReplay as OwnedCompletedReplay,
    CompletedRoll as OwnedCompletedRoll,
    CompletedStorageValidation as OwnedCompletedStorageValidation,
    CompletedSync as OwnedCompletedSync, CompletedWrite as OwnedCompletedWrite, OwnedConfig,
    OwnedJournal, PartitionDelivery as OwnedPartitionDelivery,
    PreparedCheckpointRead as OwnedPreparedCheckpointRead, PreparedRead as OwnedPreparedRead,
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
    CheckpointProgress, PublishedRecovery, ReceivedChunk, RecoveryPlan, RecoveryStartup,
    RecoveryStorage, ShardRecoveringJournal,
};
pub use recovery::{PinnedRecovery, RecoveryCheckpointRead};
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

use crate::completion;

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

/// Journal state and suspended physical work polled by the application owner.
/// Call `shutdown` before dropping the owning runtime or opening a successor.
/// Cancellation retains accepted work for a later shutdown call. Poll execution
/// alongside completion observers; drain it before releasing the shared backend.
#[derive(Debug)]
pub struct ReplicaJournal {
    backlog: PipelineLimits,
    replicated: bool,
    capacity: Arc<Semaphore>,
    command_capacity: usize,
    read_capacity: Arc<Semaphore>,
    buffers: Arc<Semaphore>,
    append_memory: Option<crate::memory::Allocator>,
    append_limits: PipelineLimits,
    operations: OperationLimits,
    buffer_generation: JournalGeneration,
    execution: execution::Execution,
}

impl ReplicaJournal {
    pub(crate) const fn generation(&self) -> JournalGeneration {
        self.buffer_generation
    }

    pub(crate) const fn backlog(&self) -> PipelineLimits {
        self.backlog
    }

    /// Replicated-persisting policy: confirmation precedes persistence.
    pub(crate) const fn replicated(&self) -> bool {
        self.replicated
    }
    pub(crate) const fn operation_limits(&self) -> OperationLimits {
        self.operations
    }

    pub(crate) fn available_command_slots(&self) -> usize {
        if self.execution.available() == 0 {
            0
        } else {
            self.capacity.available_permits()
        }
    }

    pub(crate) fn settled(&self) -> bool {
        self.execution.settled()
    }

    /// Configured outstanding command bound, including executing work.
    pub(crate) const fn command_capacity(&self) -> usize {
        self.command_capacity
    }
}

impl ReplicaJournal {
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
        self.execution.close();
    }
}

impl ReplicaJournal {
    pub(crate) fn validation_has_capacity(
        &self,
        buffer: &AppendBuffer,
        accepted: ozzy_replication::OpNumber,
    ) -> bool {
        let available = self.execution.control_capacity(accepted);
        // An exhausted overlay drains all intake so continuous APPEND traffic
        // cannot prevent the fully applied boundary needed by index refresh.
        available != 0
            && self.execution.validation_ready(buffer)
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
    #[cfg(test)]
    pub(crate) async fn stopped(&mut self) -> JournalError {
        std::future::poll_fn(|cx| self.poll_stopped(cx)).await
    }

    /// Reevaluate completion guards when a suspended command returns its owner.
    pub(crate) async fn availability_changed(
        &mut self,
        available: bool,
    ) -> Result<(), JournalError> {
        std::future::poll_fn(|cx| {
            if let Poll::Ready(error) = self.poll_stopped(cx) {
                return Poll::Ready(Err(error));
            }
            if (self.available_command_slots() != 0) == available {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        })
        .await
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

impl Drop for ReplicaJournal {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// Local admission backpressure, not an operation's persistence outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubmitError {
    /// Owner is busy or its operation budget is occupied. Retry with the same ticket.
    #[error("journal owner busy or operation capacity full")]
    Full,
    /// Admission closed or the worker exited. Do not assume a pending action failed to persist.
    #[error("replica journal worker stopped")]
    Stopped,
}

/// Storage lifecycle/action failure. None permits replacement of an existing voter.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// Retention summaries violate ordering or configured bounds.
    #[error(transparent)]
    RetentionPlan(#[from] ozzy_core::retention::PlanError),
    /// Immutable checkpoint chunk validation failed.
    #[error(transparent)]
    CheckpointBytes(#[from] ozzy_journal_segment::CheckpointError),
    /// Canonical checkpoint construction or decoding failed.
    #[error(transparent)]
    Checkpoint(#[from] ozzy_journal_segment::CanonicalCheckpointError),
    /// Retained partition floors are invalid.
    #[error(transparent)]
    Retention(#[from] ozzy_journal_segment::RetentionError),
    /// Explicit single-broker authority rejected a storage transition.
    #[error(transparent)]
    Local(#[from] ozzy_replication::local::Error),
    /// A normal partition-read rejection, without changing the cursor or journal.
    #[error(transparent)]
    Read(#[from] PartitionReadError),
    /// Missing or ambiguous application record ID in retained history.
    #[error(transparent)]
    Seek(#[from] ozzy_core::reader::seek::SeekError),
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
    /// Wrong storage policy or an unanchored retained history.
    #[error("journal is outside the supported storage profile")]
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

/// Bounded owner-local retention turn, using the mode's normal proposal path.
#[derive(Debug)]
pub struct RetentionTurn {
    /// Obsolete normal/election source released before checkpoint publication.
    pub released: Option<ozzy_replication::LogSource>,
    /// False means the canonical partition policy has no retention limits.
    pub enabled: bool,
    /// Progress exposed another eligible step; continue after a foreground turn.
    pub more_work: bool,
    /// Retry-floor and trim operations awaiting ordinary confirmation.
    pub proposal: Option<ProposalBuffer>,
}

impl ReplicaJournal {
    /// Plan one sealed segment or retire a previously confirmed prefix.
    pub fn retention_turn(
        &mut self,
        ticket: ozzy_replication::driver::ValidationTicket,
        seed: ozzy_proto::OperationId,
        leader: bool,
    ) -> Result<JournalCompletion<RetentionTurn>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| commands::Action::Retention {
                ticket,
                seed,
                leader,
                done,
            },
            |action| match action {
                commands::Action::Retention { ticket, .. } => ticket,
                _ => unreachable!("preserved retention action"),
            },
        )
        .map_err(|error| error.reason)
    }
}
