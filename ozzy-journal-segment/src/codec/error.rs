//! Errors from encoding, decoding, and scanning segment bytes.

use ozzy_proto::layout::LayoutError;
use thiserror::Error;

/// Physical format validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    #[error("segment ID zero is reserved")]
    /// Segment ID zero is reserved.
    InvalidSegmentId,
    #[error("invalid predecessor segment identity")]
    /// Invalid predecessor segment identity.
    InvalidPredecessor,
    #[error("invalid aligned segment capacity {0}")]
    /// Invalid aligned segment capacity.
    InvalidSegmentCapacity(u64),
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
    #[error("unsupported disk format version {0}")]
    /// The encoded format version is unsupported.
    UnsupportedVersion(u16),
    #[error("unsupported operation kind {0}")]
    /// The canonical operation discriminator is unsupported.
    UnsupportedOperationKind(u16),
    #[error("unsupported entry codec {0}")]
    /// The physical body codec is unsupported.
    UnsupportedCodec(u16),
    #[error("{0} body compression failed")]
    /// Canonical body compression failed.
    CompressionFailed(&'static str),
    #[error("{0} compression scratch allocation failed")]
    /// Bounded compression scratch storage could not grow.
    CompressionScratchAllocation(&'static str),
    #[error("{0} body decompression failed")]
    /// The compressed body is malformed or has the wrong decoded size.
    DecompressionFailed(&'static str),
    #[error("unsupported nonzero flags in {0}")]
    /// A field contains unsupported nonzero flags.
    UnsupportedFlags(&'static str),
    #[error("nonzero reserved bytes in {0}")]
    /// Reserved bytes are nonzero.
    NonZeroReserved(&'static str),
    #[error("{0} digest mismatch")]
    /// The named object does not match its expected integrity digest.
    DigestMismatch(&'static str),
    #[error("integer or length arithmetic overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
    #[error("physical group must contain at least one operation")]
    /// Physical group must contain at least one operation.
    EmptyGroup,
    #[error("canonical body digest count does not match operation count")]
    /// Canonical body digest count does not match operation count.
    BodyDigestCountMismatch,
    #[error("prepared bodies do not match canonical operations")]
    /// Prepared bodies do not match canonical operations.
    PreparedBodyMismatch,
    #[error("physical group number zero is reserved")]
    /// Physical group number zero is reserved.
    InvalidGroupNumber,
    #[error("physical group offset {0} is not valid or aligned")]
    /// Physical group offset is not valid or aligned.
    InvalidGroupOffset(u64),
    #[error("operation entry offset {0} is not valid or aligned")]
    /// Operation entry offset is not valid or aligned.
    InvalidEntryOffset(u64),
    #[error("operation belongs to another group")]
    /// Operation belongs to another group.
    WrongGroup,
    #[error("operation chain expected op {expected_op}")]
    /// Canonical operation numbers or predecessor digests are not consecutive.
    ChainMismatch {
        #[doc = "Next required canonical operation number."]
        expected_op: u64,
    },
    #[error("operation number space exhausted")]
    /// Operation number space exhausted.
    OperationNumberExhausted,
    #[error("entry has invalid length fields")]
    /// Entry has invalid length fields.
    InvalidEntryLength,
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
    #[error("physical group exceeds segment capacity")]
    /// Physical group exceeds segment capacity.
    GroupExceedsSegment,
    #[error("segment decoded body bytes exceed limit: {actual} > {limit}")]
    /// Segment decoded body bytes exceed limit.
    SegmentDecodedBodyLimit {
        #[doc = "Observed size, count, or fenced field value."]
        actual: usize,
        #[doc = "Configured maximum for the reported resource."]
        limit: usize,
    },
    #[error("segment file length exceeds configured capacity")]
    /// Segment file length exceeds configured capacity.
    SegmentExceedsCapacity,
    #[error("nonzero entry or physical-group padding")]
    /// Nonzero entry or physical-group padding.
    NonZeroPadding,
    #[error("physical seal metadata does not match scanned group")]
    /// Physical seal metadata does not match scanned group.
    SealMismatch,
}

impl From<LayoutError> for CodecError {
    fn from(error: LayoutError) -> Self {
        match error {
            LayoutError::Truncated { needed, available } => Self::Truncated {
                object: "layout field",
                needed,
                available,
            },
            LayoutError::Trailing(_) => Self::InvalidEntryLength,
            LayoutError::Overflow { .. } => Self::LengthOverflow,
        }
    }
}
