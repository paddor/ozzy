//! Where each copy's log starts. Age retention moves it up, and every
//! record it passes must have been old enough by then.

use super::Sim;
use crate::{broker::RETENTION_AGE, clock};

impl Sim {
    /// The age the checks hold retention to, if it runs.
    pub(super) fn age(&self) -> Option<u64> {
        self.options.retention.then_some(RETENTION_AGE)
    }

    /// Check each copy's log start that moved up since the last look. A
    /// busy journal reports nothing; its copy is looked at later.
    pub(super) fn watch_floors(&mut self) -> Result<(), String> {
        let Some(age) = self.age() else {
            return Ok(());
        };
        let wall = clock::wall_millis(self.clock.now());
        for broker in &self.brokers {
            for (partition, ledger) in self.ledgers.iter_mut().enumerate() {
                let Some(facts) = broker.facts(partition) else {
                    continue;
                };
                let seen = &mut self.floors[partition][broker.index];
                let floor = facts.retained_from.get();
                if floor > *seen {
                    *seen = floor;
                    ledger
                        .retire(floor, wall, age, &self.producers)
                        .map_err(|reason| {
                            format!("broker {}: partition {partition}: {reason}", broker.index)
                        })?;
                }
            }
        }
        Ok(())
    }
}
