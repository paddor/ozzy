//! The next instant at which a recovery round acts on time alone.

use super::{Duration, RecoveryActor, RecoveryStorage};
use crate::replica_actor::deadline::Earliest;

impl<J: RecoveryStorage> RecoveryActor<J> {
    /// Request and fetch retries, and the stall that abandons this attempt.
    pub(super) fn next_deadline(&self, now: Duration) -> Option<Duration> {
        Earliest::after(now)
            .each(self.retries.iter().map(|retry| retry.at))
            .at(self.fetch.as_ref().map(|(_, retry)| retry.at))
            .at(self.checkpoint_fetch.as_ref().map(|(_, retry)| retry.at))
            .at(self.progress_at.saturating_add(self.timing.stalled))
            .get()
    }
}
