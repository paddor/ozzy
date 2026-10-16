//! Group-directory and bounded-replay failures.

use std::io;

use ozzy_journal::operation::OperationCodecError;
use thiserror::Error;

use crate::{
    CheckpointError, CodecError, DecodedOperation, IndexBuildError, MetadataError, RetentionError,
    WriterError,
};

/// One accepted operation borrowed from a bounded replay segment buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayedOperation<'a> {
    /// Canonical operation selected for replay or indexing.
    pub operation: &'a DecodedOperation<'a>,
    /// Whether this replayed operation belongs to confirmed history.
    pub committed: bool,
}

/// Physical-journal or application-state failure during bounded replay.
#[derive(Debug, Error)]
pub enum ReplayError<E: std::error::Error + 'static> {
    #[error(transparent)]
    /// A journal operation rejected its input or exact source.
    Journal(DirectoryError),
    #[error("operation replay was rejected by the state builder")]
    /// Operation replay was rejected by the state builder.
    Visitor(#[source] E),
}

/// Group-directory lifecycle, identity, or persistence failure.
#[derive(Debug, Error)]
pub enum DirectoryError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error(transparent)]
    /// Physical segment framing or integrity validation failed.
    Codec(#[from] CodecError),
    #[error(transparent)]
    /// Journal metadata validation failed.
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    /// Canonical operation-body validation failed.
    Operation(#[from] OperationCodecError),
    #[error(transparent)]
    /// The segment writer rejected this transition.
    Writer(#[from] WriterError),
    #[error(transparent)]
    /// Derived index construction or validation failed.
    Index(#[from] IndexBuildError),
    #[error(transparent)]
    /// Checkpoint construction or validation failed.
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    /// Retention or deletion protection rejected this transition.
    Retention(#[from] RetentionError),
    #[error("group directory parent is missing")]
    /// Group directory parent is missing.
    MissingParent,
    #[error("group directory already exists")]
    /// Group directory already exists.
    StoreAlreadyExists,
    #[error("{0} is not a regular file")]
    /// The named artifact is not a regular file.
    NotRegularFile(&'static str),
    #[error("{0} is not a directory")]
    /// The named artifact is not a directory.
    NotDirectory(&'static str),
    #[error("configured group identity does not match stored identity")]
    /// Configured group identity does not match stored identity.
    IdentityMismatch,
    /// Expected immutable adapter configuration is missing or differs from disk.
    #[error("configured consensus membership does not match stored configuration")]
    ConfigurationMismatch,
    /// A prior memory-voting session did not publish exact drained history.
    #[error("memory-voting history is unproven; nonvoting group recovery is required")]
    MemoryHistoryUnproven,
    /// Immutable configuration must be nonempty and fit its fixed storage bound.
    #[error("invalid group configuration length")]
    ConfigurationLength,
    #[error("CURRENT does not identify stored group manifest")]
    /// CURRENT does not identify stored group manifest.
    CurrentMismatch,
    #[error("initial segment does not match group genesis")]
    /// Initial segment does not match group genesis.
    InvalidInitialSegment,
    #[error("manifest successor generation or identity is invalid")]
    /// Manifest successor generation or identity is invalid.
    ManifestGeneration,
    #[error("manifest references invalid or missing segment {0}")]
    /// Manifest references invalid or missing segment.
    SegmentMismatch(u64),
    #[error("manifest contains no active segment")]
    /// Manifest contains no active segment.
    MissingActiveSegment,
    #[error("active segment changes require an atomic writer roll")]
    /// Active segment changes require an atomic writer roll.
    ActiveSegmentChangeRequiresRoll,
    #[error("checkpoint changes require checkpoint installation")]
    /// Checkpoint changes require checkpoint installation.
    CheckpointChangeRequiresInstall,
    #[error("cannot checkpoint the empty journal")]
    /// Cannot checkpoint the empty journal.
    CheckpointAtGenesis,
    #[error("local durable progress must be published before checkpoint capture")]
    /// Local durable progress must be published before checkpoint capture.
    LocalProgressUnpublished,
    #[error("checkpoint does not match its source manifest or current retained journal")]
    /// Checkpoint does not match its source manifest or current retained journal.
    CheckpointMismatch,
    #[error("checkpoint position must advance beyond the selected checkpoint")]
    /// Checkpoint position must advance beyond the selected checkpoint.
    CheckpointRegression,
    #[error("physical retention requires an installed checkpoint")]
    /// Physical retention requires an installed checkpoint.
    RetentionRequiresCheckpoint,
    #[error("a storage cleanup pass is already running for this journal")]
    /// A storage cleanup pass is already running for this journal.
    CleanupBusy,
    #[error("retention scan budget is zero or cannot hold one eligible segment")]
    /// Retention scan budget is zero or cannot hold one eligible segment.
    RetentionScanBudget,
    #[error("scanned retirement prefix no longer leads the selected journal")]
    /// Scanned retirement prefix no longer leads the selected journal.
    RetentionPrefixMismatch,
    #[error("configuration changes require configuration installation")]
    /// Configuration changes require configuration installation.
    ConfigurationChangeRequiresInstall,
    #[error("journal commit mode is immutable after format")]
    /// Journal commit mode is immutable after format.
    CommitModeChange,
    #[error("ordinary metadata installation cannot regress hard state")]
    /// Ordinary metadata installation cannot regress hard state.
    HardStateRegression,
    #[error("operation position {0} is beyond the synchronized journal prefix")]
    /// Operation position is beyond the synchronized journal prefix.
    PositionNotDurable(u64),
    #[error("operation position {0} does not match the retained journal")]
    /// Operation position does not match the retained journal.
    PositionMismatch(u64),
    #[error("active segment contains writes not covered by a successful sync")]
    /// Active segment contains writes not covered by a successful sync.
    ActiveSegmentNotDurable,
    #[error("buffered roll completion does not match the live journal")]
    /// Buffered roll completion does not match the live journal.
    RollPublicationMismatch,
    #[error("prepared successor does not match the live journal generation")]
    /// Prepared successor does not match the live journal generation.
    PreparedSegmentMismatch,
    #[error("an empty active segment cannot be rolled")]
    /// An empty active segment cannot be rolled.
    EmptyActiveSegment,
    #[error("physical segment ID space exhausted")]
    /// Physical segment ID space exhausted.
    SegmentIdExhausted,
    /// No successor name was available within the caller's roll work budget.
    #[error("segment roll exhausted {limit} occupied-name probes")]
    RollProbeLimit {
        #[doc = "Configured maximum for the reported resource."]
        limit: usize,
    },
    #[error("segment {0} is active, absent, or otherwise not sealed")]
    /// Segment is active, absent, or otherwise not sealed.
    SegmentNotSealed(u64),
    #[error("immutable metadata path already contains different bytes")]
    /// Immutable metadata path already contains different bytes.
    ImmutableConflict,
    #[error("{object} size {actual} does not equal {expected}")]
    /// The physical file length differs from its exact required size.
    WrongFileSize {
        /// Named physical or encoded object that failed validation.
        object: &'static str,
        /// Observed size, count, or fenced field value.
        actual: u64,
        /// Expected size, count, or fenced field value.
        expected: u64,
    },
    #[error("{object} size {actual} exceeds limit {limit}")]
    /// The physical file exceeds its configured size bound.
    FileLimit {
        /// Named physical or encoded object that failed validation.
        object: &'static str,
        /// Observed size, count, or fenced field value.
        actual: u64,
        /// Configured maximum for the reported resource.
        limit: usize,
    },
}
