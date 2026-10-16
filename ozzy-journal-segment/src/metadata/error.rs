//! Errors from encoding, decoding, and validating journal metadata.

use ozzy_proto::layout::LayoutError;
use thiserror::Error;

/// Immutable metadata format or invariant failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataError {
    #[error("truncated {object}: need {needed} bytes, have {available}")]
    /// Input bytes end before a complete encoded object.
    Truncated {
        /// Named physical or encoded object that failed validation.
        object: &'static str,
        /// Required bytes for a complete encoded field.
        needed: usize,
        /// Available bytes, capacity, or canonical prefix at failure.
        available: usize,
    },
    #[error("wrong {0} magic")]
    /// The named object has an incorrect format magic.
    WrongMagic(&'static str),
    #[error("unsupported metadata version {0}")]
    /// The encoded format version is unsupported.
    UnsupportedVersion(u16),
    #[error("unsupported nonzero flags or reserved bytes in {0}")]
    /// Unsupported nonzero flags or reserved bytes in.
    UnsupportedFields(&'static str),
    #[error("{0} digest mismatch")]
    /// The named object does not match its expected integrity digest.
    DigestMismatch(&'static str),
    #[error("{0} identity is zero")]
    /// A required persistent identity is zero.
    ZeroIdentity(&'static str),
    #[error("store generation is zero")]
    /// Store generation is zero.
    InvalidStoreGeneration,
    #[error("manifest generation or parent is invalid")]
    /// Manifest generation or parent is invalid.
    InvalidGeneration,
    #[error("operation position and digest do not agree")]
    /// Operation position and digest do not agree.
    InvalidLogPosition,
    #[error("manifest accepted position precedes committed position")]
    /// Manifest accepted position precedes committed position.
    CommitBeyondAccepted,
    #[error("last installed normal view exceeds promised view")]
    /// Last installed normal view exceeds promised view.
    InvalidView,
    #[error("checkpoint reference is invalid")]
    /// Checkpoint reference is invalid.
    InvalidCheckpoint,
    #[error("segment reference or ordering is invalid")]
    /// Segment reference or ordering is invalid.
    InvalidSegmentReference,
    #[error("manifest requires exactly one final active segment")]
    /// Manifest requires exactly one final active segment.
    InvalidActiveSegment,
    #[error("CURRENT reference is invalid")]
    /// CURRENT reference is invalid.
    InvalidCurrent,
    #[error("integer or length arithmetic overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    /// The named resource exceeds its configured bound.
    LimitExceeded {
        /// Resource bound that rejected the operation.
        kind: &'static str,
        /// Observed size, count, or fenced field value.
        actual: usize,
        /// Configured maximum for the reported resource.
        limit: usize,
    },
    #[error("metadata file has {0} trailing bytes")]
    /// Metadata file has trailing bytes.
    TrailingBytes(usize),
}

impl From<LayoutError> for MetadataError {
    fn from(error: LayoutError) -> Self {
        match error {
            LayoutError::Truncated { needed, available } => Self::Truncated {
                object: "metadata field",
                needed,
                available,
            },
            LayoutError::Trailing(bytes) => Self::TrailingBytes(bytes),
            LayoutError::Overflow { .. } => Self::LengthOverflow,
        }
    }
}
