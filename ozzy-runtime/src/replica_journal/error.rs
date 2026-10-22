//! Journal admission backpressure and fail-closed storage errors.

use super::{AppendAdmissionError, GroupAssignError, GroupProgressError, PartitionReadError};

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
    /// Bounded owner-local identity classification or installation failed.
    #[error(transparent)]
    Dedup(#[from] ozzy_core::dedup::Error),
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
    /// Group assignment, confirmed cursor, or retained history rejected a request.
    #[error(transparent)]
    GroupProgress(#[from] GroupProgressError),
    /// Source ownership or coordinator plan validation failed.
    #[error(transparent)]
    GroupAssign(#[from] GroupAssignError),
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
    /// Another segment would exceed the segment limit, or new work would take
    /// the room kept for operations that free space.
    #[error("replica journal segment capacity exhausted")]
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
