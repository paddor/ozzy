//! The earliest future instant among a partition's timers.

use super::{PartitionActor, ReplicaActor};
use std::time::Duration;

/// Keeps the earliest of several candidate instants that lies after `now`.
/// Instants at or before `now` were handled by the visit that asks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Earliest {
    now: Duration,
    earliest: Option<Duration>,
}

impl Earliest {
    pub(crate) const fn after(now: Duration) -> Self {
        Self {
            now,
            earliest: None,
        }
    }

    #[must_use]
    pub(crate) fn at(self, instant: impl Into<Option<Duration>>) -> Self {
        let earliest = instant
            .into()
            .filter(|instant| *instant > self.now)
            .into_iter()
            .chain(self.earliest)
            .min();
        Self { earliest, ..self }
    }

    #[must_use]
    pub(crate) fn each(self, instants: impl IntoIterator<Item = Duration>) -> Self {
        instants.into_iter().fold(self, Self::at)
    }

    pub(crate) const fn get(self) -> Option<Duration> {
        self.earliest
    }
}

impl ReplicaActor {
    /// The earliest future instant at which `advance` acts on time alone.
    pub(super) fn next_deadline(&self, now: Duration) -> Option<Duration> {
        let next = Earliest::after(now)
            .at(self.driver.next_deadline())
            .at(self.ack_at)
            .at(self.maintenance.storage_validation.map(|turn| turn.next()))
            .at(self.maintenance.metadata_cleanup.map(|turn| turn.next()))
            .at(self.maintenance.orphan_cleanup.map(|turn| turn.next()))
            .at(self.maintenance.retention.at())
            .at(self.release_deadline(now))
            .at(self.held_load_deadline(now))
            .at(self.evidence_deadline());
        let next = match &self.startup_probe {
            Some(probe) => probe.deadlines(next),
            None => next,
        };
        let next = match &self.transfer {
            Some(transfer) => transfer.deadlines(next),
            None => next,
        };
        next.each(self.flow_probes()).get()
    }

    /// Probe times of the peers a primary polls for receipt status.
    fn flow_probes(&self) -> impl Iterator<Item = Duration> + '_ {
        let primary = self.driver.normal().is_some_and(|normal| {
            self.configuration.primary(normal.snapshot().scope.view) == self.local
        });
        self.work
            .flow
            .peers
            .iter()
            .flatten()
            .filter(move |_| primary)
            .filter_map(ozzy_replication::flow::Transmitter::next_probe)
    }
}

impl PartitionActor {
    /// The earliest future instant at which this actor acts on time alone.
    pub(super) fn next_deadline(&self, now: Duration) -> Option<Duration> {
        match self {
            Self::Local(actor) => actor.next_deadline(now),
            Self::Replicated(actor) => actor.next_deadline(now),
            Self::Recovering(recovery) => recovery.next_deadline(now),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn earliest_keeps_the_first_instant_after_now() {
        let at = Duration::from_millis;
        let next = Earliest::after(at(10))
            .at(at(5))
            .at(at(10))
            .at(None)
            .each([at(30), at(20)])
            .at(at(25));
        assert_eq!(next.get(), Some(at(20)));
        assert_eq!(Earliest::after(at(10)).at(at(9)).get(), None);
    }
}
