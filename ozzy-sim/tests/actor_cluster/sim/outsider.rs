//! A permanent outsider, as in
//! [TigerBeetle's liveness mode](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness/).
//! Some ticks before the drain every fault ends and the network heals, but
//! one broker fails for good. Clients give up on it once it stops
//! answering, as an SDK does once its requests time out: a down or paused
//! one at once, any other once it leads no partition's newest view. Until
//! then new work reaches it, so a stalled or deaf leader notices; after,
//! proposals waiting on its disk or its peers never complete. The other
//! two must keep confirming proposals and records, then converge without
//! it. The outsider's history is still checked every tick.

use super::Sim;
use crate::{broker::session, check::leader, network::BROKERS, records::Ledger};

/// Ticks from the outsider's failure to the drain: 30 s. Earlier faults may
/// have backed election timeouts off to 8 s; a view attempt the outsider
/// broke and one it leads may each wait that long before the two elect.
pub(crate) const OUTSIDER_TICKS: u64 = 3000;

/// How the outsider fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stuck {
    /// The host is gone: its process runs nothing, and its peers' links to
    /// it drop.
    Down,
    /// The process runs nothing. Its links stay up, so frames to it pile up.
    Pause,
    /// The disk completes nothing. Process and links keep going.
    Stall,
    /// Frames to it stop.
    Deaf,
    /// Frames from it stop.
    Mute,
}

const STUCK: [Stuck; 5] = [
    Stuck::Down,
    Stuck::Pause,
    Stuck::Stall,
    Stuck::Deaf,
    Stuck::Mute,
];

#[derive(Debug)]
pub(crate) struct Outsider {
    pub broker: usize,
    pub kind: Stuck,
    /// Proposals confirmed, and records confirmed by partition, when it
    /// failed.
    confirmed: usize,
    records: Vec<usize>,
    /// Clients gave up on it.
    avoided: bool,
}

impl Sim {
    /// The last tick before the outsider fails, if one does.
    pub(super) fn liveness_start(&self) -> Option<u64> {
        self.options
            .outsider
            .map(|ticks| self.options.ticks - ticks)
    }

    pub(super) fn outsider_index(&self) -> Option<usize> {
        self.outsider.as_ref().map(|outsider| outsider.broker)
    }

    /// An outsider whose process runs nothing.
    pub(super) fn stopped_outsider(&self) -> Option<usize> {
        self.outsider
            .as_ref()
            .filter(|outsider| matches!(outsider.kind, Stuck::Down | Stuck::Pause))
            .map(|outsider| outsider.broker)
    }

    /// The seed picks the kind in turn, so five seeds in a row cover every
    /// kind. The broker is the first partition's leader at even odds, else
    /// any broker.
    pub(super) fn start_outsider(&mut self, tick: u64) -> Result<(), String> {
        let kind = STUCK[(self.seed % STUCK.len() as u64) as usize];
        let rng = &mut self.outsiders;
        let broker = match leader(&self.brokers, 0) {
            Some((leader, _)) if rng.chance(0.5) => leader,
            _ => rng.below(BROKERS as u64) as usize,
        };
        self.trace.word(tick);
        self.trace.word(broker as u64);
        match kind {
            Stuck::Down => {
                self.network.crash(broker);
                let now = self.clock.now();
                for peer in (0..BROKERS).filter(|peer| *peer != broker) {
                    let session = session(peer, broker, self.epoch(peer, broker));
                    self.brokers[peer].disconnect(broker, session, now)?;
                    self.connected[peer][broker] = false;
                    self.connected[broker][peer] = false;
                }
            }
            Stuck::Pause => {}
            Stuck::Stall => self.brokers[broker].disk.stall(u64::MAX),
            Stuck::Deaf => self.network.cut_one_way(broker, false),
            Stuck::Mute => self.network.cut_one_way(broker, true),
        }
        self.outsider = Some(Outsider {
            broker,
            kind,
            confirmed: self.workload.confirmed.len(),
            records: self.ledgers.iter().map(Ledger::len).collect(),
            avoided: false,
        });
        self.watch_outsider(tick);
        Ok(())
    }

    /// Clients give up on the outsider once it stops answering them.
    pub(super) fn watch_outsider(&mut self, tick: u64) {
        let Some(outsider) = &self.outsider else {
            return;
        };
        let broker = outsider.broker;
        let silent = matches!(outsider.kind, Stuck::Down | Stuck::Pause)
            || (0..self.options.partitions).all(|group| {
                leader(&self.brokers, group).is_none_or(|(newest, _)| newest != broker)
            });
        if outsider.avoided || !silent {
            return;
        }
        self.workload.avoid(broker, tick);
        self.producers.avoid(broker, tick);
        if let Some(outsider) = &mut self.outsider {
            outsider.avoided = true;
        }
    }

    /// A restart gives a stalled outsider a healthy disk; stall it again.
    pub(super) fn hold_outsider(&mut self) {
        if let Some(outsider) = &self.outsider
            && outsider.kind == Stuck::Stall
        {
            self.brokers[outsider.broker].disk.stall(u64::MAX);
        }
    }

    /// The other two confirmed proposals, and records of every partition,
    /// after the outsider failed.
    pub(super) fn check_outsider(&self) -> Result<(), String> {
        let Some(outsider) = &self.outsider else {
            return Ok(());
        };
        let failed = format!(
            "broker {} became a {:?} outsider",
            outsider.broker, outsider.kind
        );
        if self.workload.confirmed.len() == outsider.confirmed {
            return Err(format!("no proposal confirmed after {failed}"));
        }
        if self.options.records {
            for (partition, ledger) in self.ledgers.iter().enumerate() {
                if ledger.len() == outsider.records[partition] {
                    return Err(format!(
                        "partition {partition}: no record confirmed after {failed}"
                    ));
                }
            }
        }
        Ok(())
    }
}
