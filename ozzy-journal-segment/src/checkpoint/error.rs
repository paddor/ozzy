//! Checkpoint format, publication, and content failures.

use ozzy_proto::layout::LayoutError;
use std::io;
use thiserror::Error;

/// Checkpoint format, publication, or content failure.
#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error("wrong checkpoint manifest magic")]
    /// The named object has an incorrect format magic.
    WrongMagic,
    #[error("unsupported checkpoint version {0}")]
    /// The encoded format version is unsupported.
    UnsupportedVersion(u16),
    #[error("unsupported checkpoint flags, widths, or reserved fields")]
    /// Unsupported checkpoint flags, widths, or reserved fields.
    UnsupportedFields,
    #[error("checkpoint metadata is truncated")]
    /// Input bytes end before a complete encoded object.
    Truncated,
    #[error("checkpoint lengths do not agree")]
    /// Checkpoint lengths do not agree.
    LengthMismatch,
    #[error("{0} identity is zero")]
    /// A required persistent identity is zero.
    ZeroIdentity(&'static str),
    #[error("checkpoint operation position is invalid")]
    /// Checkpoint operation position is invalid.
    InvalidPosition,
    #[error("checkpoint specification is invalid")]
    /// Checkpoint specification is invalid.
    InvalidSpec,
    #[error("checkpoint state is empty")]
    /// Checkpoint state is empty.
    EmptyState,
    #[error("checkpoint chunk size is invalid")]
    /// Checkpoint chunk size is invalid.
    InvalidChunkBytes,
    #[error("checkpoint resource limits are invalid")]
    /// Checkpoint resource limits are invalid.
    InvalidLimits,
    #[error("checkpoint chunk sequence or fields are invalid")]
    /// Checkpoint chunk sequence or fields are invalid.
    InvalidChunk,
    #[error("{0} digest mismatch")]
    /// The named object does not match its expected integrity digest.
    DigestMismatch(&'static str),
    #[error("checkpoint belongs to another group or store")]
    /// Checkpoint belongs to another group or store.
    IdentityMismatch,
    #[error("checkpoint is missing a required chunk")]
    /// Checkpoint is missing a required chunk.
    MissingChunk,
    #[error("checkpoint directory contains an unexpected file")]
    /// Checkpoint directory contains an unexpected file.
    UnexpectedFile,
    #[error("checkpoint staging namespace is exhausted")]
    /// Checkpoint staging namespace is exhausted.
    StagingConflict,
    #[error("immutable checkpoint path contains different content")]
    /// Immutable checkpoint path contains different content.
    ImmutableConflict,
    #[error("{0} is not a regular file")]
    /// The named artifact is not a regular file.
    NotRegularFile(&'static str),
    #[error("{0} is not a directory")]
    /// The named artifact is not a directory.
    NotDirectory(&'static str),
    #[error("checkpoint integer or length arithmetic overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    /// The named resource exceeds its configured bound.
    LimitExceeded {
        /// Resource bound that rejected the operation.
        kind: &'static str,
        /// Observed size, count, or fenced field value.
        actual: u64,
        /// Configured maximum for the reported resource.
        limit: u64,
    },
}

impl From<LayoutError> for CheckpointError {
    fn from(error: LayoutError) -> Self {
        match error {
            LayoutError::Truncated { .. } => Self::Truncated,
            LayoutError::Trailing(_) => Self::LengthMismatch,
            LayoutError::Overflow { .. } => Self::LengthOverflow,
        }
    }
}
