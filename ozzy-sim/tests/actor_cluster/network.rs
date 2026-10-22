//! Broker links with OMQ and broker transport semantics.
//!
//! Each directed path `(from, to, class)` is one bounded FIFO, like one OMQ
//! connection: delays never reorder it, a full PEER path refuses the send
//! with `Full`, and a full PUB path drops the newest frame. PEER traffic is
//! never lost on a live link. Clogs and partitions stop delivery on paths;
//! their senders then fill the paths and feel backpressure.

use crate::seeds::{Rng, Trace};
use bytes::Bytes;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{NodeId, Opcode};
use std::{
    collections::VecDeque,
    task::{Context, Waker},
};

pub(crate) const BROKERS: usize = 3;
/// Shard input lane bounds of a receiving broker.
const CONTROL_LANE: usize = 64;
const DATA_LANE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
    /// Control PEER: votes, probes, receipts.
    Control,
    /// Data PEER: flow PREPAREs and bounded repair.
    Data,
    /// Lossy follower publication.
    Pub,
}

const CLASSES: [Class; 3] = [Class::Control, Class::Data, Class::Pub];

/// Fault parameters. `swarm` draws each value from its seed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Profile {
    /// Lost PUB copies, standing in for slow-subscriber drops.
    pub pub_loss: f64,
    /// Fixed and mean exponential extra delay per packet, in ticks.
    pub delay_floor: u64,
    pub delay_mean: f64,
    /// Control and PUB path capacity; a data path holds a quarter.
    pub capacity: usize,
    /// Per-tick probability that a partition starts or heals.
    pub partition_start: f64,
    pub partition_heal: f64,
    /// Per-path, per-tick clog probability and mean clog length in ticks.
    pub clog: f64,
    pub clog_mean: f64,
    /// Mean disk completion delay in 1 ms steps.
    pub disk_mean: f64,
}

impl Profile {
    pub(crate) const NONE: Self = Self {
        pub_loss: 0.0,
        delay_floor: 1,
        delay_mean: 0.0,
        capacity: 64,
        partition_start: 0.0,
        partition_heal: 0.0,
        clog: 0.0,
        clog_mean: 0.0,
        disk_mean: 1.0,
    };

    pub(crate) const LIGHT: Self = Self {
        pub_loss: 0.02,
        delay_floor: 1,
        delay_mean: 3.0,
        capacity: 64,
        partition_start: 0.005,
        partition_heal: 0.05,
        clog: 0.002,
        clog_mean: 10.0,
        disk_mean: 1.0,
    };

    /// A fault-free network over slow disks.
    pub(crate) const SLOW: Self = Self {
        disk_mean: 8.0,
        ..Self::NONE
    };

    pub(crate) const HEAVY: Self = Self {
        pub_loss: 0.10,
        delay_floor: 1,
        delay_mean: 8.0,
        capacity: 64,
        partition_start: 0.02,
        partition_heal: 0.02,
        clog: 0.01,
        clog_mean: 10.0,
        disk_mean: 3.0,
    };

    /// Every value from the seed. Heal probability and capacity have floors,
    /// so the network still heals and paths still hold a few frames.
    pub(crate) fn swarm(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let delay_floor = 1 + rng.below(3);
        Self {
            pub_loss: rng.range(0.0, 0.15),
            delay_floor,
            delay_mean: rng.range(0.0, 10.0 - delay_floor as f64),
            capacity: 8 + rng.below(57) as usize,
            partition_start: rng.range(0.0, 0.03),
            partition_heal: rng.range(0.01, 0.10),
            clog: rng.range(0.0, 0.015),
            clog_mean: rng.range(2.0, 30.0),
            disk_mean: rng.range(0.0, 8.0),
        }
    }

    pub(crate) fn named(name: &str, swarm_seed: u64) -> Option<Self> {
        Some(match name {
            "none" => Self::NONE,
            "light" => Self::LIGHT,
            "heavy" => Self::HEAVY,
            "slow" => Self::SLOW,
            "swarm" => Self::swarm(swarm_seed),
            _ => return None,
        })
    }

    fn capacity(&self, class: Class) -> usize {
        match class {
            Class::Data => (self.capacity / 4).max(2),
            Class::Control | Class::Pub => self.capacity,
        }
    }
}

/// Delivery counters, for failure reports and vacuity checks.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Stats {
    pub sent: u64,
    pub delivered: u64,
    pub refused: u64,
    pub pub_dropped: u64,
    pub partitions: u64,
    pub clogs: u64,
}

/// Input lanes of one receiving shard, filled by the network and drained by
/// the shard turn. Entries carry the sending broker.
#[derive(Debug, Default)]
pub(crate) struct Inbox {
    pub control: VecDeque<(usize, Message)>,
    pub data: VecDeque<(usize, Message)>,
}

#[derive(Debug, Default)]
struct Path {
    queue: VecDeque<(u64, Message)>,
    last_due: u64,
    clogged_until: u64,
}

#[derive(Debug)]
pub(crate) struct Network {
    pub profile: Profile,
    rng: Rng,
    paths: Vec<Path>,
    /// Directed links a partition blocks.
    blocked: [[bool; BROKERS]; BROKERS],
    partitioned: bool,
    /// The broker of a scripted fault. Random partitions wait for it.
    isolated: Option<usize>,
    /// The final drain stops new faults.
    healed: bool,
    /// Shards whose send was refused, woken when one of their paths drains.
    refused: [Option<Waker>; BROKERS],
    pub stats: Stats,
}

impl Network {
    pub(crate) fn new(profile: Profile, seed: u64) -> Self {
        Self {
            profile,
            rng: Rng::new(seed),
            paths: (0..BROKERS * BROKERS * CLASSES.len())
                .map(|_| Path::default())
                .collect(),
            blocked: [[false; BROKERS]; BROKERS],
            partitioned: false,
            isolated: None,
            healed: false,
            refused: std::array::from_fn(|_| None),
            stats: Stats::default(),
        }
    }

    fn path(&mut self, from: usize, to: usize, class: Class) -> &mut Path {
        &mut self.paths[index(from, to, class)]
    }

    /// One outbound frame of shard `from`. The first frame routes it: a
    /// destination broker, or the group topic of a PUB copy.
    pub(crate) fn submit(
        &mut self,
        from: usize,
        tick: u64,
        cx: &Context<'_>,
        mut message: Message,
        trace: &mut Trace,
    ) -> Result<(), TrySendError> {
        let (class, to) = classify(&message);
        let capacity = self.profile.capacity(class);
        if let Some(to) = to
            && self.path(from, to, class).queue.len() >= capacity
        {
            self.stats.refused += 1;
            self.refused[from] = Some(cx.waker().clone());
            return Err(TrySendError::Full(message));
        }
        message.pop_front();
        let message = Message::with_prefix(Bytes::copy_from_slice(node(from).as_bytes()), message);
        match to {
            Some(to) => self.enqueue(from, to, class, tick, message, trace),
            None => {
                for to in (0..BROKERS).filter(|to| *to != from) {
                    self.enqueue(from, to, Class::Pub, tick, message.clone(), trace);
                }
            }
        }
        Ok(())
    }

    fn enqueue(
        &mut self,
        from: usize,
        to: usize,
        class: Class,
        tick: u64,
        message: Message,
        trace: &mut Trace,
    ) {
        trace.word(tick);
        trace.word(index(from, to, class) as u64);
        trace.message(&message);
        if class == Class::Pub
            && (self.path(from, to, class).queue.len() >= self.profile.capacity(class)
                || self.rng.chance(self.profile.pub_loss))
        {
            self.stats.pub_dropped += 1;
            trace.word(u64::MAX);
            return;
        }
        let delay = self.profile.delay_floor + self.rng.exponential(self.profile.delay_mean);
        let path = self.path(from, to, class);
        let due = (tick + delay).max(path.last_due);
        path.last_due = due;
        path.queue.push_back((due, message));
        self.stats.sent += 1;
        trace.word(due);
    }

    /// Start or heal partitions and clogs for this tick.
    pub(crate) fn step(&mut self, tick: u64) {
        if self.healed {
            return;
        }
        if self.isolated.is_some() {
            // A scripted isolation holds; random partitions wait.
        } else if self.partitioned {
            if self.rng.chance(self.profile.partition_heal) {
                self.blocked = [[false; BROKERS]; BROKERS];
                self.partitioned = false;
            }
        } else if self.rng.chance(self.profile.partition_start) {
            // With three brokers every split isolates one of them, either
            // both ways or in one direction only.
            let isolated = self.rng.below(BROKERS as u64) as usize;
            let direction = self.rng.below(3);
            self.split(isolated, direction);
            self.partitioned = true;
            self.stats.partitions += 1;
        }
        for slot in 0..self.paths.len() {
            if self.paths[slot].clogged_until <= tick && self.rng.chance(self.profile.clog) {
                let length = 1 + self.rng.exponential(self.profile.clog_mean);
                self.paths[slot].clogged_until = tick + length;
                self.stats.clogs += 1;
            }
        }
    }

    /// Cut `isolated` off: 0 both ways, 1 outbound only, 2 inbound only.
    fn split(&mut self, isolated: usize, direction: u64) {
        for (from, row) in self.blocked.iter_mut().enumerate() {
            for (to, blocked) in row.iter_mut().enumerate() {
                *blocked = (from == isolated) != (to == isolated)
                    && match direction {
                        0 => true,
                        1 => from == isolated,
                        _ => to == isolated,
                    };
            }
        }
    }

    /// Cut `broker` off both ways until `rejoin`, replacing a random
    /// partition. Clogs continue.
    pub(crate) fn isolate(&mut self, broker: usize) {
        self.cut(broker, 0);
    }

    /// As `isolate`, in one direction only: frames from `broker` when
    /// `outbound`, else frames to it.
    pub(crate) fn cut_one_way(&mut self, broker: usize, outbound: bool) {
        self.cut(broker, if outbound { 1 } else { 2 });
    }

    /// Heal a random partition and start none until `rejoin`, while a
    /// scripted fault of `broker` keeps its links up. The other two must
    /// be able to replace it.
    pub(crate) fn hold_partitions(&mut self, broker: usize) {
        self.blocked = [[false; BROKERS]; BROKERS];
        self.partitioned = false;
        self.isolated = Some(broker);
    }

    fn cut(&mut self, broker: usize, direction: u64) {
        self.split(broker, direction);
        self.partitioned = false;
        self.isolated = Some(broker);
        self.stats.partitions += 1;
    }

    /// `broker` lost power: frames on its links are gone with them. It
    /// stays cut off until `rejoin`.
    pub(crate) fn crash(&mut self, broker: usize) {
        for from in 0..BROKERS {
            for to in (0..BROKERS).filter(|to| *to != from && (from == broker || *to == broker)) {
                for class in CLASSES {
                    self.paths[index(from, to, class)].queue.clear();
                }
            }
            // Senders refused on a cleared path may send again.
            if let Some(waker) = self.refused[from].take()
                && from != broker
            {
                waker.wake();
            }
        }
        self.isolate(broker);
    }

    pub(crate) fn rejoin(&mut self) {
        self.blocked = [[false; BROKERS]; BROKERS];
        self.isolated = None;
    }

    /// Clear every random fault and start no new ones. A scripted
    /// isolation lasts until `rejoin`.
    pub(crate) fn heal(&mut self) {
        self.healed = true;
        self.partitioned = false;
        if self.isolated.is_none() {
            self.blocked = [[false; BROKERS]; BROKERS];
        }
        for path in &mut self.paths {
            path.clogged_until = 0;
        }
    }

    /// Move due frames into `to`'s lanes. A full control or data lane pauses
    /// its PEER path; a PUB frame that finds the data lane full is dropped.
    pub(crate) fn deliver(&mut self, to: usize, tick: u64, inbox: &mut Inbox, trace: &mut Trace) {
        for offset in 0..BROKERS {
            let from = (to + offset + tick as usize) % BROKERS;
            if from == to || self.blocked[from][to] {
                continue;
            }
            let mut drained = false;
            for class in CLASSES {
                let path = &mut self.paths[index(from, to, class)];
                if path.clogged_until > tick {
                    continue;
                }
                while let Some((due, _)) = path.queue.front()
                    && *due <= tick
                {
                    let lane = match class {
                        Class::Control => &mut inbox.control,
                        Class::Data | Class::Pub => &mut inbox.data,
                    };
                    let full = lane.len()
                        >= if class == Class::Control {
                            CONTROL_LANE
                        } else {
                            DATA_LANE
                        };
                    if full && class != Class::Pub {
                        break;
                    }
                    let (_, message) = path.queue.pop_front().unwrap();
                    drained = true;
                    trace.word(tick);
                    trace.word(index(from, to, class) as u64);
                    if full {
                        self.stats.pub_dropped += 1;
                        trace.word(u64::MAX);
                    } else {
                        self.stats.delivered += 1;
                        lane.push_back((from, message));
                    }
                }
            }
            if drained && let Some(waker) = self.refused[from].take() {
                waker.wake();
            }
        }
    }

    /// Frames still queued on any path.
    pub(crate) fn queued(&self) -> usize {
        self.paths.iter().map(|path| path.queue.len()).sum()
    }
}

/// The class and destination of an outbound shard frame, as the broker's
/// outbound queue classifies it (`ozzy-broker/src/serving/outbound.rs`).
fn classify(message: &Message) -> (Class, Option<usize>) {
    let code = opcode(message);
    if code == Some(Opcode::PreparePub as u8) {
        return (Class::Pub, None);
    }
    let route = message.part_slice(0).expect("routed shard frame");
    let to = broker(route).unwrap_or_else(|| panic!("unexpected shard route {route:?}"));
    let data = [
        Opcode::PrepareFlow,
        Opcode::Ops,
        Opcode::SnapshotChunk,
        Opcode::Records,
    ]
    .iter()
    .any(|data| code == Some(*data as u8));
    (if data { Class::Data } else { Class::Control }, Some(to))
}

/// Envelope opcode of a packet after its routing frame. Compact receipts
/// have no envelope.
pub(crate) fn opcode(message: &Message) -> Option<u8> {
    if message.len() != 4 {
        return None;
    }
    message
        .part_slice(1)
        .and_then(|header| header.get(5))
        .copied()
}

pub(crate) fn node(broker: usize) -> NodeId {
    NodeId::from_bytes([broker as u8 + 1; 16])
}

fn broker(route: &[u8]) -> Option<usize> {
    let first = *route.first()?;
    (route.len() == 16
        && route.iter().all(|byte| *byte == first)
        && (1..=BROKERS as u8).contains(&first))
    .then(|| usize::from(first - 1))
}

fn index(from: usize, to: usize, class: Class) -> usize {
    let class = CLASSES.iter().position(|c| *c == class).unwrap();
    (from * BROKERS + to) * CLASSES.len() + class
}
