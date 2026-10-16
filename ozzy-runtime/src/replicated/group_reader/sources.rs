//! Membership-owned observations and drain deadlines. The application keeps
//! completion holes; only the progress owner seals a window's terminal result.

use crate::{replicated::processing::progress::Observer, signal::CloseSignal};
use ozzy_proto::{
    PartitionIncarnation,
    group::{Owned, Target},
};
use std::{collections::BTreeMap, time::Duration};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub(super) struct Observation {
    pub(super) owned: Owned,
    pub(super) progress: Observer,
    pub(super) stop: CloseSignal,
    pub(super) generation: u64,
    pub(super) draining: bool,
}

type Key = (u64, PartitionIncarnation, u64);

impl Observation {
    fn key(&self) -> Key {
        (self.generation, self.owned.partition, self.owned.epoch)
    }
}

struct Tracked {
    observation: Observation,
    deadline: Option<Duration>,
    released: bool,
}

pub(super) struct Sources {
    entries: BTreeMap<Key, Tracked>,
    drain: Duration,
}

impl Sources {
    pub(super) fn new(drain: Duration) -> Self {
        Self {
            entries: BTreeMap::new(),
            drain,
        }
    }

    pub(super) fn replace(&mut self, observations: &[Observation]) {
        self.entries.retain(|key, entry| {
            if observations
                .iter()
                .any(|observation| observation.key() == *key)
            {
                return true;
            }
            entry.observation.stop.close();
            false
        });
        for observation in observations {
            self.entries
                .entry(observation.key())
                .and_modify(|entry| entry.observation.draining = observation.draining)
                .or_insert_with(|| Tracked {
                    observation: observation.clone(),
                    deadline: None,
                    released: false,
                });
        }
    }

    pub(super) fn owned(
        &mut self,
        targets: &[Target],
        generation: u64,
        now: Duration,
    ) -> Vec<Owned> {
        let mut owned = Vec::with_capacity(self.entries.len());
        for entry in self.entries.values_mut() {
            if entry.released {
                continue;
            }
            let observation = &entry.observation;
            let current = targets.iter().any(|target| {
                target.partition == observation.owned.partition
                    && target.plan_revision == observation.owned.plan_revision
            });
            if observation.generation != generation || observation.progress.stopping().is_some() {
                entry.released = true;
            } else if observation.draining || !current {
                let deadline = *entry.deadline.get_or_insert(now.saturating_add(self.drain));
                // Confirmed is acquired first: seeing that prefix also observes
                // the delivery that preceded the cumulative submission.
                entry.released = observation.progress.confirmed()
                    >= observation.progress.delivered()
                    || now >= deadline;
            }
            if entry.released {
                observation.stop.close();
            } else {
                owned.push(observation.owned);
            }
        }
        owned
    }

    pub(super) fn deadline(&self) -> Option<Duration> {
        self.entries
            .values()
            .filter(|entry| !entry.released)
            .filter_map(|entry| entry.deadline)
            .min()
    }

    pub(super) fn fence(&mut self) {
        for entry in self.entries.values_mut() {
            entry.released = true;
            entry.observation.stop.close();
        }
    }
}

impl Drop for Sources {
    fn drop(&mut self) {
        self.fence();
    }
}
