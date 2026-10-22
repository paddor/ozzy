//! Why an election event was rejected.

use crate::ReplicationError;

/// Rejected election event. Missing history/quorums wait; conflicts fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ViewChangeError {
    /// Existing journal/normal scope, role, or membership check failed.
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    /// Timers/messages cannot regress or reuse the target view.
    #[error("view must increase")]
    ViewNotHigher,
    /// A restart cannot safely wrap its highest durable view back to zero.
    #[error("durable view number exhausted")]
    ViewExhausted,
    /// All accepted old-view operations must finish synchronization first.
    #[error("old-view journal work has not settled")]
    StoragePending,
    /// Only one metadata promise action can be in flight.
    #[error("a view promise is already pending")]
    PromisePending,
    /// No new metadata write is necessary for this requested view.
    #[error("requested view already promised")]
    AlreadyPromised,
    /// Completion is not the exact outstanding metadata action.
    #[error("stale or foreign view-promise completion")]
    StalePromise,
    /// No election message before successful durable promise completion.
    #[error("requested view promise has not completed")]
    PromiseRequired,
    /// Need own durable promise plus another distinct first-phase voter.
    #[error("start-view-change quorum missing")]
    StartQuorumMissing,
    /// Selection needs the primary's own report and another distinct voter.
    #[error("do-view-change quorum missing")]
    ReportQuorumMissing,
    /// Fetch and validate the frozen selected history before selecting it.
    #[error("selected lineage evidence missing")]
    HistoryMissing,
    /// Malformed positions or last-normal-view cannot become evidence.
    #[error("invalid frozen view-change report")]
    InvalidReport,
    /// A protected prefix or immutable report conflicts with this lineage.
    #[error("conflicting view-change history")]
    ConflictingHistory,
    /// Do not change the selected quorum while installation may be in flight.
    #[error("view selection already frozen")]
    SelectionFrozen,
    /// Primary cannot install without its completed quorum selection.
    #[error("view has not been selected")]
    SelectionRequired,
    /// A replacement writer needs a fresh, globally unique incarnation.
    #[error("installation reused the old writer generation")]
    ReusedGeneration,
    /// Completion does not identify this exact pending installation.
    #[error("stale or foreign installation completion")]
    StaleInstallation,
    /// Successful completion has already transferred ownership to a new role.
    #[error("installation already completed")]
    InstallationCompleted,
    /// Committed canonical state must be rebuilt before role activation.
    #[error("installed application state is not at the required committed prefix")]
    ApplicationPending,
    /// Storage or lineage fault requires recovery, not another local election.
    #[error("view change faulted")]
    Faulted,
}
