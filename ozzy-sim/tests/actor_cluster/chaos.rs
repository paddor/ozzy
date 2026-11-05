//! Random overlapping faults, as a Jepsen nemesis draws them: power loss,
//! process kills, pauses, disk stalls and failures, cuts in either
//! direction, and connection resets between brokers or to clients. Any two
//! brokers may be down at once, so quorum comes and goes; only safety holds
//! meanwhile. Every fault ends when the drain starts, and the group must then
//! converge.

use crate::{network::BROKERS, seeds::Rng};

/// Per-tick chance that a new fault starts.
const START: f64 = 1.0 / 120.0;
/// Fault lengths in ticks: a floor, then an exponential mean, then a cap.
const FLOOR: u64 = 50;
const MEAN: f64 = 400.0;
const CAP: u64 = 1500;
/// A reset connection comes back within this many ticks.
const RECONNECT: u64 = 50;
/// At most this many brokers are down, paused, stalled, or failing at once.
const DOWN: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Power fails; the broker restarts over what reached media, cut off.
    PowerLoss,
    /// The process dies; the broker restarts over its cache, cut off.
    Kill,
    /// The process runs nothing.
    Pause,
    /// The disk completes nothing.
    Stall,
    /// Every write and sync fails until the broker exits on one and its
    /// supervisor restarts it.
    FailDisk,
    /// Frames to and from the broker stop.
    Isolate,
    /// Frames to the broker stop.
    Deaf,
    /// Frames from the broker stop.
    Mute,
    /// The connection to `peer` drops with its queued frames, then comes
    /// back with a fresh session.
    LinkReset,
    /// Every client connection to the broker drops; clients reconnect at
    /// once with fresh sessions.
    ClientReset,
}

const KINDS: [Kind; 10] = [
    Kind::PowerLoss,
    Kind::Kill,
    Kind::Pause,
    Kind::Stall,
    Kind::FailDisk,
    Kind::Isolate,
    Kind::Deaf,
    Kind::Mute,
    Kind::LinkReset,
    Kind::ClientReset,
];

impl Kind {
    /// Whether it takes its broker down, so it counts against `DOWN` and
    /// excludes other such faults on that broker.
    pub(crate) const fn down(self) -> bool {
        matches!(
            self,
            Self::PowerLoss | Self::Kill | Self::Pause | Self::Stall | Self::FailDisk
        )
    }

    /// Whether it cuts its broker's links.
    pub(crate) const fn cut(self) -> bool {
        matches!(self, Self::Isolate | Self::Deaf | Self::Mute)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Fault {
    pub kind: Kind,
    pub broker: usize,
    /// The other end of a `LinkReset`.
    pub peer: usize,
    pub until: u64,
}

#[derive(Debug)]
pub(crate) struct Chaos {
    rng: Rng,
    pub active: Vec<Fault>,
    /// Faults started, by kind, for failure reports.
    pub started: [u64; KINDS.len()],
}

impl Chaos {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            rng: Rng::new(seed),
            active: Vec::new(),
            started: [0; KINDS.len()],
        }
    }

    /// Whether a fault of `kind` holds `broker` now.
    pub(crate) fn holds(&self, broker: usize, kind: Kind) -> bool {
        self.active
            .iter()
            .any(|fault| fault.broker == broker && fault.kind == kind)
    }

    /// Faults that end by `tick`, removed from the active set.
    pub(crate) fn ended(&mut self, tick: u64) -> Vec<Fault> {
        let (ended, active) = self.active.iter().partition(|fault| fault.until <= tick);
        self.active = active;
        ended
    }

    /// Every active fault, removed: the drain heals them all.
    pub(crate) fn end_all(&mut self) -> Vec<Fault> {
        std::mem::take(&mut self.active)
    }

    /// A new fault to start at `tick`, if one is drawn and the rules allow
    /// it. `connected(a, b)` tells whether two brokers have a link to reset.
    pub(crate) fn draw(
        &mut self,
        tick: u64,
        connected: impl Fn(usize, usize) -> bool,
    ) -> Option<Fault> {
        if !self.rng.chance(START) {
            return None;
        }
        let kind = KINDS[self.rng.below(KINDS.len() as u64) as usize];
        let broker = self.rng.below(BROKERS as u64) as usize;
        let peer = (broker + 1 + self.rng.below(BROKERS as u64 - 1) as usize) % BROKERS;
        let length = match kind {
            Kind::LinkReset => 1 + self.rng.below(RECONNECT),
            Kind::ClientReset => 0,
            _ => (FLOOR + self.rng.exponential(MEAN)).min(CAP),
        };
        let down: Vec<usize> = self
            .active
            .iter()
            .filter(|fault| fault.kind.down())
            .map(|fault| fault.broker)
            .collect();
        let allowed = match kind {
            _ if kind.down() => !down.contains(&broker) && down.len() < DOWN,
            _ if kind.cut() => !self
                .active
                .iter()
                .any(|fault| fault.broker == broker && fault.kind.cut()),
            Kind::LinkReset => connected(broker, peer),
            _ => true,
        };
        if !allowed {
            return None;
        }
        let fault = Fault {
            kind,
            broker,
            peer,
            until: tick + length,
        };
        self.started[KINDS.iter().position(|known| *known == kind).unwrap()] += 1;
        if length > 0 {
            self.active.push(fault);
        }
        Some(fault)
    }

    /// Faults started so far, by kind.
    pub(crate) fn summary(&self) -> Vec<(Kind, u64)> {
        KINDS.iter().copied().zip(self.started).collect()
    }

    /// Faults started so far.
    pub(crate) fn total(&self) -> u64 {
        self.started.iter().sum()
    }
}
