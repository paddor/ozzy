//! The actor's fail-closed error.

use ozzy_replication::{driver::DriverError, wire};

use super::HistoryFailure;
use crate::replica_journal::{JournalError, SubmitError};
use crate::replica_transport::{EnqueueError, OutboxError};

/// Fail-closed actor error. No variant grants authority to weaken commit policy.
#[derive(Debug, thiserror::Error)]
pub enum ActorError {
    /// Explicit single-broker state rejected a transition.
    #[error(transparent)]
    Local(#[from] ozzy_replication::local::Error),
    /// Nonvoting recovery rejected authority or history evidence.
    #[error(transparent)]
    Recovery(#[from] ozzy_replication::recovery::RecoveryError),
    /// Volatile receipt/repair policy failed; never interpreted as a disk vote.
    #[error(transparent)]
    Flow(#[from] ozzy_replication::flow::TransmitError),
    /// Storage-validated startup belongs to another worker incarnation.
    #[error("replica startup does not belong to this journal worker")]
    StartupMismatch,
    /// Inconsistent or unrepresentable startup bounds.
    #[error("invalid replica actor limits")]
    Limits,
    /// A bounded actor invariant could not be fulfilled by verified history.
    #[error(transparent)]
    History(Box<HistoryFailure>),
    /// Production protocol core rejected an authority or history transition.
    #[error(transparent)]
    Driver(#[from] DriverError),
    /// A real journal action failed or disappeared.
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// Worker admission closed or unexpectedly exhausted its reserved action budget.
    #[error(transparent)]
    Submit(#[from] SubmitError),
    /// An internally generated packet failed wire validation.
    #[error(transparent)]
    Wire(#[from] wire::WireError),
    /// Configured outbox or OMQ transmission failure.
    #[error(transparent)]
    Outbox(#[from] OutboxError),
    /// Invalid internally generated message, not ordinary queue backpressure.
    #[error("invalid replica outgoing message: {0:?}")]
    Enqueue(EnqueueError),
    /// Local socket failure; never counted as evidence about a remote operation.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
}
