//! Safety checks after every tick, convergence after the healing drain.
//!
//! Every committed or applied prefix a replica reports, and every confirmed
//! proposal, goes into one map from operation to chain digest. Two sources
//! that disagree on an operation fail the run. The digest covers the whole
//! chain, so equal final prefixes mean equal histories.

use crate::{broker::Broker, network::BROKERS};
use ozzy_replication::{Digest, Prefix};
use ozzy_runtime::replica_actor::ActorStatus;
use std::collections::{BTreeMap, btree_map::Entry};

#[derive(Debug, Clone, Copy)]
enum Source {
    Committed(usize),
    Applied(usize),
    Confirmed,
}

#[derive(Debug)]
struct Observed {
    digest: Digest,
    first: Source,
    /// Brokers that reported this operation.
    brokers: u8,
}

#[derive(Debug, Default)]
pub(crate) struct Checker {
    digests: BTreeMap<u64, Observed>,
    views: [u64; BROKERS],
    /// The newest view each broker served in normal status. Entering it
    /// published it, so it survives a restart.
    normal: [u64; BROKERS],
    committed: [u64; BROKERS],
    /// Operations that two brokers reported with the same digest.
    pub compared: u64,
}

impl Checker {
    pub(crate) fn observe(&mut self, broker: usize, status: &ActorStatus) -> Result<(), String> {
        let view = status.scope.view;
        if view < self.views[broker] {
            return Err(format!(
                "broker {broker}: view went back from {} to {view}",
                self.views[broker]
            ));
        }
        self.views[broker] = view;
        let Some(normal) = status.normal else {
            return Ok(());
        };
        self.normal[broker] = view;
        let committed = normal.committed.op.0;
        if committed < self.committed[broker] {
            return Err(format!(
                "broker {broker}: commit went back from {} to {committed}",
                self.committed[broker]
            ));
        }
        self.committed[broker] = committed;
        self.record(normal.committed, Source::Committed(broker))?;
        self.record(normal.applied, Source::Applied(broker))
    }

    /// A broker lost power and started again. Its view may fall back to the
    /// last one it published, and its commit to what it held durably.
    pub(crate) fn restarted(&mut self, broker: usize) {
        self.views[broker] = self.normal[broker];
        self.committed[broker] = 0;
    }

    pub(crate) fn confirm(&mut self, through: Prefix) -> Result<(), String> {
        self.record(through, Source::Confirmed)
    }

    fn record(&mut self, prefix: Prefix, source: Source) -> Result<(), String> {
        if prefix.op.0 == 0 {
            return Ok(());
        }
        let broker = match source {
            Source::Committed(broker) | Source::Applied(broker) => 1 << broker,
            Source::Confirmed => 0,
        };
        match self.digests.entry(prefix.op.0) {
            Entry::Vacant(entry) => {
                entry.insert(Observed {
                    digest: prefix.digest,
                    first: source,
                    brokers: broker,
                });
            }
            Entry::Occupied(mut entry) => {
                let observed = entry.get_mut();
                if observed.digest != prefix.digest {
                    return Err(format!(
                        "op {}: {source:?} reports digest {:?}, {:?} reported {:?}",
                        prefix.op.0, prefix.digest, observed.first, observed.digest
                    ));
                }
                let before = observed.brokers;
                observed.brokers |= broker;
                if before.is_power_of_two() && observed.brokers.count_ones() == 2 {
                    self.compared += 1;
                }
            }
        }
        Ok(())
    }
}

/// A ready leader that admits proposals.
fn leads(status: &ActorStatus) -> bool {
    status.application_ready
        && status
            .normal
            .is_some_and(|normal| normal.backups.is_some() && normal.ready_for_appends)
}

/// The ready leader of the newest view and that view.
pub(crate) fn leader(brokers: &[Broker]) -> Option<(usize, u64)> {
    brokers
        .iter()
        .map(|broker| (broker.index, broker.status()))
        .filter(|(_, status)| leads(status))
        .max_by_key(|(_, status)| status.scope.view)
        .map(|(index, status)| (index, status.scope.view))
}

/// The common applied prefix once one ready leader leads the newest view and
/// every broker is ready in that view with the same applied prefix.
pub(crate) fn converged(brokers: &[Broker]) -> Option<Prefix> {
    let statuses: Vec<_> = brokers.iter().map(Broker::status).collect();
    let leader = statuses
        .iter()
        .filter(|status| leads(status))
        .max_by_key(|status| status.scope.view)?;
    let applied = leader.normal?.applied;
    statuses
        .iter()
        .all(|status| {
            status.application_ready
                && status.scope == leader.scope
                && status.normal.map(|normal| normal.applied) == Some(applied)
        })
        .then_some(applied)
}
