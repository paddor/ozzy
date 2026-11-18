//! The next instant at which polling the driver acts without new input.

use super::{ReplicaDriver, Role};
use std::time::Duration;

impl ReplicaDriver {
    /// The earliest future instant at which `poll` acts on time alone: a
    /// heartbeat, retransmission, or exit retry, or a contact, progress,
    /// persistence, storage, doubt, or election timeout. Instants that already
    /// passed were handled by the last poll.
    pub fn next_deadline(&self) -> Option<Duration> {
        let timers = &self.timers;
        match &self.role {
            Role::Fenced(_) => None,
            Role::Installing(_) => {
                self.soonest(&[Some(timers.election_deadline), self.exit_retry_at])
            }
            Role::ViewChanging(_) => self.soonest(&[
                Some(timers.election_deadline),
                Some(timers.retry_at),
                self.exit_retry_at,
            ]),
            Role::Normal(normal) => {
                let snapshot = normal.snapshot();
                let primary = self.leads(snapshot.scope.view);
                let outstanding =
                    snapshot.accepted != snapshot.committed || !snapshot.ready_for_appends;
                let unpersisted = snapshot.accepted.op > snapshot.journal.durable;
                self.soonest(&[
                    (primary && !self.resting()).then_some(timers.retry_at),
                    (primary && self.covered.is_empty()).then(|| self.doubt_at()),
                    (!primary && self.covered.is_empty()).then_some(timers.contact_deadline),
                    outstanding.then_some(timers.progress_deadline),
                    unpersisted.then_some(timers.persistence_deadline),
                    timers
                        .storage_busy_since
                        .map(|since| since.saturating_add(self.timing.primary_timeout)),
                    self.exit_retry_at,
                ])
            }
        }
    }

    /// The earliest of `candidates` that is still ahead of the clock.
    fn soonest(&self, candidates: &[Option<Duration>]) -> Option<Duration> {
        let ahead = candidates.iter().flatten().copied();
        ahead.filter(|deadline| *deadline > self.now).min()
    }
}
