//! Bounded canonical body validation failures.

use thiserror::Error;

/// Canonical operation body codec failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OperationCodecError {
    #[error("unsupported operation kind {0}")]
    /// The canonical operation discriminator is unsupported.
    UnsupportedOperationKind(u16),
    #[error("truncated operation body: need {needed} bytes, have {available}")]
    /// Canonical body bytes end before a complete field.
    Truncated {
        /// Required bytes for this field.
        needed: usize,
        /// Available bytes in the input suffix.
        available: usize,
    },
    #[error("operation body has {0} trailing bytes")]
    /// Bytes remain after a complete canonical operation body.
    TrailingBytes(usize),
    #[error("invalid UTF-8 in {0}")]
    /// A stream or topic name is not valid UTF-8.
    InvalidUtf8(&'static str),
    #[error("{0} must not be empty")]
    /// A required name or value is empty.
    EmptyValue(&'static str),
    #[error("invalid optional-value tag {0}")]
    /// An optional field has an unsupported presence tag.
    InvalidOptionTag(u8),
    #[error("invalid progress-owner tag {0}")]
    /// The progress owner discriminator is unsupported.
    InvalidProgressOwnerTag(u8),
    #[error("retention policy version or flags are unsupported")]
    /// Retention-policy version or flags are unsupported.
    InvalidRetention,
    #[error("append operation must contain a batch and each batch a record")]
    /// An APPEND has no batches or a batch has no records.
    EmptyAppend,
    #[error("append record must contain at least one message part")]
    /// A record has no payload parts.
    EmptyRecordParts,
    #[error("prepared append payload is invalid or does not match its raw record view")]
    /// The prepared payload is invalid or conflicts with the raw view.
    InvalidPreparedPayload,
    #[error("APPEND payload compression failed")]
    /// Whole-APPEND payload compression failed.
    AppendCompression,
    #[error("append sequence or offset range overflows")]
    /// A producer sequence or record offset range overflows.
    AppendPositionOverflow,
    #[error("consumer-group progress requires an assignment epoch")]
    /// Consumer-group progress lacks an assignment fence.
    MissingAssignmentEpoch,
    #[error("standalone subscription progress cannot carry an assignment epoch")]
    /// Standalone subscription progress carries an assignment fence.
    UnexpectedAssignmentEpoch,
    #[error("integer or length arithmetic overflow")]
    /// Encoded integer or byte-count arithmetic overflows.
    LengthOverflow,
    #[error("operation output allocation failed")]
    /// Canonical output storage could not grow.
    OutputAllocation,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    /// Canonical metadata or payload exceeds a configured resource bound.
    LimitExceeded {
        /// Resource bound that rejected the operation.
        kind: &'static str,
        /// Observed size or count.
        actual: usize,
        /// Configured maximum size or count.
        limit: usize,
    },
}
