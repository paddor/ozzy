//! Journal execution placement. Command admission and replication authority do
//! not depend on whether progress is polled locally or by the legacy worker.

use std::task::{Context, Poll};

/// Execution owned by a journal adapter. Implementations retain pending work
/// across canceled polls and report terminal failure only after settling it.
/// No polling method may block the calling application thread.
pub trait JournalExecution: std::fmt::Debug {
    /// Conservative control-identity room at the actor's accepted position.
    /// Zero pauses new validation while application and persistence drain.
    /// Legacy execution has no shard-local capacity observation.
    fn control_capacity(&self, _accepted: ozzy_replication::OpNumber) -> usize {
        usize::MAX
    }

    /// Drive admitted work. Pending work must arrange a wakeup. On termination,
    /// return whether this journal faulted; later polls return the same result.
    fn poll_finished(&mut self, cx: &mut Context<'_>) -> Poll<bool>;
}
