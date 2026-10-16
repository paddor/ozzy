//! Errors from encoding, decoding, and validating segment index files.

use ozzy_proto::layout::LayoutError;
use thiserror::Error;

impl From<LayoutError> for IndexFileError {
    fn from(error: LayoutError) -> Self {
        match error {
            LayoutError::Truncated { needed, available } => Self::Truncated { needed, available },
            LayoutError::Trailing(_) => Self::InvalidSectionLayout,
            LayoutError::Overflow { .. } => Self::LengthOverflow,
        }
    }
}

/// Immutable index construction, validation, or lookup failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IndexFileError {
    #[error("index file is truncated: need {needed} bytes, have {available}")]
    /// Input bytes end before a complete encoded object.
    Truncated {
        #[doc = "Required bytes for a complete encoded field."]
        needed: usize,
        #[doc = "Available bytes, capacity, or canonical prefix at failure."]
        available: usize,
    },
    #[error("wrong segment index magic")]
    /// The named object has an incorrect format magic.
    WrongMagic,
    #[error("unsupported segment index version {0}")]
    /// The encoded format version is unsupported.
    UnsupportedVersion(u16),
    #[error("invalid segment index header")]
    /// Invalid segment index header.
    InvalidHeader,
    #[error("unsupported segment index flags")]
    /// A field contains unsupported nonzero flags.
    UnsupportedFlags,
    #[error("unsupported segment index entry width")]
    /// Unsupported segment index entry width.
    UnsupportedEntryWidth,
    #[error("nonzero reserved segment index bytes")]
    /// Reserved bytes are nonzero.
    NonZeroReserved,
    #[error("segment index file length is {actual}; expected {expected}")]
    /// Segment index file length is; expected.
    FileLengthMismatch {
        #[doc = "Expected size, count, or fenced field value."]
        expected: usize,
        #[doc = "Observed size, count, or fenced field value."]
        actual: usize,
    },
    #[error("segment index digest mismatch")]
    /// The named object does not match its expected integrity digest.
    DigestMismatch,
    #[error("invalid segment index source identity")]
    /// Invalid segment index source identity.
    InvalidSource,
    #[error("invalid segment index section layout")]
    /// Invalid segment index section layout.
    InvalidSectionLayout,
    #[error("offset and message index counts differ")]
    /// Offset and message index counts differ.
    RecordIndexCountMismatch,
    #[error("{0} index keys are unsorted or duplicated")]
    /// The named object index keys are unsorted or duplicated.
    UnsortedOrDuplicate(&'static str),
    #[error("derived index operation location is invalid")]
    /// Derived index operation location is invalid.
    InvalidLocation,
    #[error("message index entry has no corresponding offset entry")]
    /// Message index entry has no corresponding offset entry.
    MissingOffsetEntry,
    #[error("invalid segment index resource limits")]
    /// Invalid segment index resource limits.
    InvalidLimits,
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
}
