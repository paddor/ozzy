//! Replica journal admission, state, and execution adapters.
//!
//! `ShardJournal` runs beside its partition actor and sends filesystem work to
//! shared backend workers. Commands and completions are bounded.

mod append;
mod authority;
mod canonical;
mod commands;
mod error;
mod execution;
pub use error::{JournalError, SubmitError};
mod shard;
pub use owned::dedup::IdentityRetention;
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
    CompletedSync as OwnedCompletedSync, CompletedWrite as OwnedCompletedWrite, DedupLimits,
    DelayLimits, GroupSource, OwnedConfig, OwnedJournal,
    PartitionDelivery as OwnedPartitionDelivery, PartitionFacts,
    PreparedCheckpointRead as OwnedPreparedCheckpointRead, PreparedRead as OwnedPreparedRead,
    PreparedRecoveryRead as OwnedPreparedRecoveryRead, PreparedReplay as OwnedPreparedReplay,
    PreparedRoll as OwnedPreparedRoll, PreparedStorageValidation as OwnedPreparedStorageValidation,
    PreparedSync as OwnedPreparedSync, PreparedWrite as OwnedPreparedWrite,
    RecoveringJournal as OwnedRecoveringJournal, RecoveryGenerations as OwnedRecoveryGenerations,
    RecoveryOpen as OwnedRecoveryOpen, ReleaseTurn, ReplayRequest as OwnedReplayRequest,
    StorageCleanup as OwnedStorageCleanup, WriteStep as OwnedWriteStep,
    WrittenRecords as OwnedWrittenRecords, dedup_limits, retry_span_snapshot_bytes,
};
mod assignment;
mod producer;
pub use assignment::{GroupAssignError, GroupAssignRequest, GroupAssignState};
mod progress;
pub use progress::{GroupAssignment, GroupProgressError, GroupProgressRequest, GroupProgressState};
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
pub use proposal::{
    AppendAdmissionError, ProducerAppend, ProposalBuffer, ProposalDeferral, ProposalValidation,
};
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
pub use turn::{ReceivedValidation, Turn, TurnResult};

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
                ViewChange::recover_intact(
                    self.configuration,
                    self.local,
                    self.generation,
                    recovered,
                    limits,
                )?,
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
    unapplied: usize,
    dedup_window_millis: Option<u64>,
    capacity: Arc<Semaphore>,
    command_capacity: usize,
    read_capacity: Arc<Semaphore>,
    buffers: Arc<Semaphore>,
    append_memory: Option<crate::memory::Allocator>,
    append_limits: PipelineLimits,
    operations: OperationLimits,
    buffer_generation: JournalGeneration,
    /// The shard's storage lane, for its completed-job count.
    io: ozzy_io::Local,
    execution: execution::Execution,
}

impl ReplicaJournal {
    pub(crate) const fn deduplication_enabled(&self) -> bool {
        self.dedup_window_millis.is_some()
    }

    pub(crate) fn retention_interval(
        &self,
        configured: Option<std::time::Duration>,
    ) -> Option<std::time::Duration> {
        let identity = self
            .dedup_window_millis
            .map(|window| std::time::Duration::from_millis(window.min(1000)));
        match (configured, identity) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    pub(crate) const fn generation(&self) -> JournalGeneration {
        self.buffer_generation
    }

    pub(crate) const fn backlog(&self) -> PipelineLimits {
        self.backlog
    }

    /// Operations the canonical images and dedup index can hold accepted but
    /// not yet applied.
    pub(crate) const fn unapplied_operations(&self) -> usize {
        self.unapplied
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

    /// Physical jobs completed on this shard's storage lane. While a command
    /// runs, a moving count shows the device still completes its jobs.
    pub(crate) fn completed_jobs(&self) -> u64 {
        self.io.completed()
    }

    pub(crate) fn settled(&self) -> bool {
        self.execution.settled()
    }

    /// Whether a retirement's checkpoint write, prefix scan, or installation
    /// is still outstanding. Its writes continue; the next retention turn waits.
    pub(crate) fn retention_busy(&self) -> bool {
        self.execution.retention_busy()
    }

    /// Whether a storage cleanup job runs beside writes. Installation and
    /// explicit cleanup wait for it.
    pub(crate) fn cleaning(&self) -> bool {
        self.execution.cleaning()
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
        self.command(commands::Gate::Settled, |done| {
            commands::Action::Append(commands::AppendAction::Promise { ticket, done })
        })
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
        if available == 0 {
            crate::profiling::event(crate::profiling::Event::ValidationIdentityWait);
            return false;
        }
        self.execution.validation_ready(buffer)
            && buffer
                .operations()
                .filter(|operation| operation.kind.has_operation_id())
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

/// Bounded owner-local retention turn, using the mode's normal proposal path.
#[derive(Debug)]
pub struct RetentionTurn {
    /// Obsolete normal/election source released before checkpoint publication.
    pub released: Option<ozzy_replication::LogSource>,
    /// False means neither record nor identity retention needs periodic turns.
    pub enabled: bool,
    /// Confirmed identities physically released by this bounded turn.
    pub expired_identities: usize,
    /// Progress exposed another eligible step; continue after a foreground turn.
    pub more_work: bool,
    /// Retry-floor and trim operations awaiting ordinary confirmation.
    pub proposal: Option<ProposalBuffer>,
}

impl ReplicaJournal {
    /// Expire confirmed identities between foreground turns. Unlike a
    /// retention turn, this waits for no settled accepted or committed work.
    pub fn retain_identities(
        &mut self,
        ticket: ozzy_replication::driver::ValidationTicket,
        leader: bool,
    ) -> Result<JournalCompletion<IdentityRetention>, SubmitError> {
        self.command(commands::Gate::Free, |done| {
            commands::Action::Maintenance(commands::MaintenanceAction::RetainIdentities {
                ticket,
                leader,
                done,
            })
        })
    }

    /// Pick due held records and build their `Release`, or carry held
    /// records forward toward the retention target. The source reads run
    /// beside writes; the proposal goes through ordinary validation.
    pub fn release_turn(
        &mut self,
        ticket: ozzy_replication::driver::ValidationTicket,
    ) -> Result<JournalCompletion<ReleaseTurn>, SubmitError> {
        self.command(commands::Gate::Free, |done| {
            commands::Action::Maintenance(commands::MaintenanceAction::Release { ticket, done })
        })
    }

    /// Load the next sealed held rows beside writes, on the leader or a
    /// follower, once few are loaded.
    pub fn load_held(
        &mut self,
        ticket: ozzy_replication::driver::ValidationTicket,
    ) -> Result<JournalCompletion<ReleaseTurn>, SubmitError> {
        self.command(commands::Gate::Free, |done| {
            commands::Action::Maintenance(commands::MaintenanceAction::LoadHeld { ticket, done })
        })
    }

    /// Whether the copy wants a held-row load. `false` while the owner runs
    /// a command.
    pub(crate) fn held_load_wanted(&self) -> bool {
        self.execution.held_load_wanted()
    }

    /// Time until the earliest committed held record is due, by the journal
    /// clock, or zero while a carry is pending. `None` while nothing is held
    /// or the owner runs a command.
    pub(crate) fn release_wait(&self) -> Option<std::time::Duration> {
        self.execution
            .release_wait()
            .map(std::time::Duration::from_millis)
    }

    /// Plan one sealed segment or retire a previously confirmed prefix.
    pub fn retention_turn(
        &mut self,
        ticket: ozzy_replication::driver::ValidationTicket,
        seed: ozzy_proto::OperationId,
        leader: bool,
    ) -> Result<JournalCompletion<RetentionTurn>, SubmitError> {
        self.command(commands::Gate::Settled, |done| {
            commands::Action::Maintenance(commands::MaintenanceAction::Retention {
                ticket,
                seed,
                leader,
                done,
            })
        })
    }
}
