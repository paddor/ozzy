//! Structural wire failures.

use ozzy_proto::EnvelopeError;
use ozzy_proto::layout::LayoutError;

use crate::ReplicationError;
use crate::flow::FlowError;

/// Structural wire failure. No rejection changes replica or journal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    /// Invalid receipt metadata or mismatched local receive incarnation.
    #[error(transparent)]
    Flow(#[from] FlowError),
    /// Response does not match the live source, request, range, or response bound.
    #[error("replica history response does not match the outstanding request")]
    Transfer,
    /// Invalid election view, frozen generation, or accepted/commit relationship.
    #[error("invalid replica election history descriptor")]
    History,
    /// A batch/request is empty, too large, or exceeds an operation/frame work bound.
    #[error("replica command exceeds operation or byte limits")]
    Limit,
    /// Unsupported canonical operation kind.
    #[error("unsupported replica operation kind {0}")]
    OperationKind(u16),
    /// Canonical envelope does not continue the announced scope/hash chain.
    #[error("replica canonical operation chain mismatch")]
    Chain,
    /// Body or envelope digest does not match the received canonical bytes.
    #[error("replica canonical operation digest mismatch")]
    Digest,
    /// Invalid native envelope or directional frame bound.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// Invalid configured voter supplied to the binding.
    #[error(transparent)]
    Configuration(#[from] ReplicationError),
    /// Sender, embedded voter, or session does not match authenticated binding.
    #[error("replica peer or session mismatch")]
    Peer,
    /// Group/configuration epoch/digest does not match the configured group.
    #[error("replica configuration scope mismatch")]
    Scope,
    /// Invalid genesis/digest pairing or unrepresentable successor operation.
    #[error("invalid replica log prefix")]
    Prefix,
    /// A control command cannot carry payload bytes.
    #[error("unexpected replica payload")]
    Payload,
    /// Truncated fields or extra metadata.
    #[error("replica metadata length mismatch")]
    Length,
    /// Output storage cannot hold the complete encoded message.
    #[error("replica encoding buffer too small")]
    Capacity,
    /// Command correlation/response flag does not match its exchange family.
    #[error("invalid replica command correlation")]
    Correlation,
    /// Opcode does not have an implemented replica codec.
    #[error("unsupported replica command")]
    UnsupportedCommand,
}

impl From<LayoutError> for WireError {
    fn from(error: LayoutError) -> Self {
        match error {
            LayoutError::Truncated { .. } | LayoutError::Trailing(_) => Self::Length,
            LayoutError::Overflow { .. } => Self::Capacity,
        }
    }
}
