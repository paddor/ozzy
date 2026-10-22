//! The next instant at which polling the driver acts without new input.

use super::{ReplicaDriver, Role, VoterIndex};
use std::time::Duration;

impl ReplicaDriver {
    /// The earliest future instant at which `poll` acts on time alone: a
    /// heartbeat, retransmission, or exit retry, or a contact, progress,
    /// persistence, storage, or election timeout. Instants that already
    /// passed were handled by the last poll.
    pub fn next_deadline(&self) -> Option<Duration> {
        let candidates = match &self.role {
            Role::Fenced(_) => [None; 6],
            Role::Installing(_) => [
                Some(self.election_deadline),
                self.exit_retry_at,
                None,
                None,
                None,
                None,
            ],
            Role::ViewChanging(_) => [
                Some(self.election_deadline),
                Some(self.retry_at),
                self.exit_retry_at,
                None,
                None,
                None,
            ],
            Role::Normal(normal) => {
                let snapshot = normal.snapshot();
                let primary = VoterIndex::primary(snapshot.scope.view) == self.local;
                let outstanding =
                    snapshot.accepted != snapshot.committed || !snapshot.ready_for_appends;
                let unpersisted = snapshot.accepted.op > snapshot.journal.durable;
                [
                    primary.then_some(self.retry_at),
                    (!primary).then_some(self.contact_deadline),
                    outstanding.then_some(self.progress_deadline),
                    unpersisted.then_some(self.persistence_deadline),
                    self.storage_busy_since
                        .map(|since| since.saturating_add(self.timing.primary_timeout)),
                    self.exit_retry_at,
                ]
            }
        };
        candidates
            .into_iter()
            .flatten()
            .filter(|deadline| *deadline > self.now)
            .min()
    }
}
