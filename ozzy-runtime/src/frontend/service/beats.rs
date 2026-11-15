//! Broker beats: one periodic message per broker pair that proves the
//! sender's dispatcher and shards alive. The dispatcher sends and receives
//! them, as it does clock samples. Shards publish their epochs here and read
//! what each peer last said.
//!
//! A replica group that rests sends no heartbeat of its own. Its voters
//! instead agree on each other's [`BeatSource`], and each side trusts the
//! other while that broker's beats still name the same shard and epoch. A
//! shard changes its epoch to withdraw everything said under the old one.

use super::{Class, Kind, NodeId, ReceiveError, Service, routed};
use crate::signal::StateSignal;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use ozzy_proto::{
    Envelope, EnvelopeLimits, LinkSessionId, Opcode,
    beat::{self, ShardEpoch},
};
use ozzy_replication::BeatSource;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

/// Timing of the beats between brokers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BeatTiming {
    /// Period between beats to each peer.
    pub interval: Duration,
    /// A peer silent for this long proves nothing. Half of it without a
    /// turn drops a local shard from the beats.
    pub timeout: Duration,
}

/// One shard of this broker.
#[derive(Debug)]
struct Local {
    shard: u32,
    epoch: AtomicU64,
    /// Nanoseconds of its last turn plus one, or zero before the first.
    turned: AtomicU64,
}

/// What one broker peer last said, and when.
#[derive(Debug)]
struct Peer {
    node: NodeId,
    /// Nanoseconds of its last beat plus one, or zero before the first.
    heard: AtomicU64,
    said: ArcSwapOption<Said>,
    /// The timeout passed and the shards were told.
    silent: AtomicBool,
}

#[derive(Debug, Eq, PartialEq)]
struct Said {
    session: LinkSessionId,
    shards: Vec<ShardEpoch>,
}

struct Shared {
    timing: BeatTiming,
    clock: Box<dyn Fn() -> Duration + Send + Sync>,
    shards: Box<[Local]>,
    peers: Box<[Peer]>,
    changed: StateSignal,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Beats")
            .field("timing", &self.timing)
            .field("shards", &self.shards)
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
}

/// Beat state of one broker, shared by its dispatcher and shards.
#[derive(Clone, Debug)]
pub struct Beats(Arc<Shared>);

fn stamp(now: Duration) -> u64 {
    u64::try_from(now.as_nanos())
        .unwrap_or(u64::MAX - 1)
        .saturating_add(1)
}

fn age(stamp: u64, now: Duration) -> Option<Duration> {
    let at = Duration::from_nanos(stamp.checked_sub(1)?);
    Some(now.saturating_sub(at))
}

impl Beats {
    /// State for `shards` of this broker and its broker `peers`. `clock` is
    /// one monotonic clock for every thread. Epochs start at `seed`, which
    /// should differ between runs of a broker.
    pub fn new(
        shards: impl IntoIterator<Item = u32>,
        peers: impl IntoIterator<Item = NodeId>,
        timing: BeatTiming,
        seed: u64,
        clock: impl Fn() -> Duration + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(Shared {
            timing,
            clock: Box::new(clock),
            shards: shards
                .into_iter()
                .map(|shard| Local {
                    shard,
                    epoch: AtomicU64::new(seed),
                    turned: AtomicU64::new(0),
                })
                .collect(),
            peers: peers
                .into_iter()
                .map(|node| Peer {
                    node,
                    heard: AtomicU64::new(0),
                    said: ArcSwapOption::empty(),
                    silent: AtomicBool::new(false),
                })
                .collect(),
            changed: StateSignal::default(),
        }))
    }

    /// The timing these beats run on.
    pub fn timing(&self) -> BeatTiming {
        self.0.timing
    }

    fn local(&self, shard: u32) -> Option<&Local> {
        self.0.shards.iter().find(|local| local.shard == shard)
    }

    fn peer(&self, peer: NodeId) -> Option<&Peer> {
        self.0.peers.iter().find(|state| state.node == peer)
    }

    /// Note that `shard` took a turn. A shard that stops turning leaves the
    /// beats, so its peers stop trusting it.
    pub fn turn(&self, shard: u32) {
        if let Some(local) = self.local(shard) {
            // Only this shard's thread writes, and readers need no order
            // with other state.
            local
                .turned
                .store(stamp((self.0.clock)()), Ordering::Relaxed);
        }
    }

    /// What a voter on `shard` sends its peers now. `None` for a shard
    /// these beats do not report.
    pub fn source(&self, shard: u32) -> Option<BeatSource> {
        // The epoch is read by the dispatcher thread: Acquire pairs with
        // the Release in `withdraw`.
        self.local(shard).map(|local| BeatSource {
            shard,
            epoch: local.epoch.load(Ordering::Acquire),
        })
    }

    /// Withdraw everything voters on `shard` sent under its current epoch.
    pub fn withdraw(&self, shard: u32) {
        if let Some(local) = self.local(shard) {
            local.epoch.fetch_add(1, Ordering::Release);
        }
    }

    /// Whether `peer`'s beats on `session` still name `source` and the last
    /// one is recent.
    pub fn covers(&self, peer: NodeId, session: LinkSessionId, source: BeatSource) -> bool {
        let Some(state) = self.peer(peer) else {
            return false;
        };
        // Published by the dispatcher after `said`; a stale reading only
        // delays or hastens the answer by one beat.
        let heard = age(state.heard.load(Ordering::Acquire), (self.0.clock)());
        heard.is_some_and(|heard| heard < self.0.timing.timeout)
            && state.said.load().as_deref().is_some_and(|said| {
                said.session == session
                    && said
                        .shards
                        .iter()
                        .any(|named| named.shard == source.shard && named.epoch == source.epoch)
            })
    }

    /// How long ago `peer`'s last beat arrived. `None` before the first.
    pub fn heard(&self, peer: NodeId) -> Option<Duration> {
        let state = self.peer(peer)?;
        age(state.heard.load(Ordering::Acquire), (self.0.clock)())
    }

    /// Capture before reading, then wait with [`Self::changed_after`].
    pub fn generation(&self) -> u64 {
        self.0.changed.generation()
    }

    /// Wait until a peer fell silent or changed what it says. Voters that
    /// trusted it must look again.
    pub async fn changed_after(&self, generation: u64) {
        self.0.changed.changed_after(generation).await;
    }

    /// Keep one beat of `peer`, as the dispatcher does on receipt.
    pub fn observe(
        &self,
        peer: NodeId,
        session: LinkSessionId,
        shards: impl IntoIterator<Item = ShardEpoch>,
    ) {
        let Some(state) = self.peer(peer) else {
            return;
        };
        let said = Said {
            session,
            shards: shards.into_iter().collect(),
        };
        let changed = state.said.load().as_deref() != Some(&said);
        if changed {
            state.said.store(Some(Arc::new(said)));
        }
        state
            .heard
            .store(stamp((self.0.clock)()), Ordering::Release);
        state.silent.store(false, Ordering::Relaxed);
        if changed {
            self.0.changed.notify_changed();
        }
    }

    /// Tell the shards once about each peer that fell silent, as the
    /// dispatcher does on every tick.
    pub fn age_out(&self) {
        let now = (self.0.clock)();
        for state in &self.0.peers {
            let stale = age(state.heard.load(Ordering::Acquire), now)
                .is_some_and(|heard| heard >= self.0.timing.timeout);
            // Only the dispatcher thread reads and writes this flag.
            if stale && !state.silent.swap(true, Ordering::Relaxed) {
                self.0.changed.notify_changed();
            }
        }
    }

    /// The shards a beat names now: those that turned within half the
    /// timeout.
    pub fn report(&self, shards: &mut Vec<ShardEpoch>) {
        let now = (self.0.clock)();
        shards.clear();
        shards.extend(self.0.shards.iter().filter_map(|local| {
            let turned = age(local.turned.load(Ordering::Relaxed), now)?;
            (turned < self.0.timing.timeout / 2).then(|| ShardEpoch {
                shard: local.shard,
                epoch: local.epoch.load(Ordering::Acquire),
            })
        }));
    }
}

/// The dispatcher's beat schedule.
#[derive(Debug)]
pub(super) struct BeatSchedule {
    beats: Beats,
    next: Duration,
    shards: Vec<ShardEpoch>,
    metadata: Vec<u8>,
}

impl Service {
    /// Send and receive beats for `beats`. Install before serving; a
    /// service without it sends none and ignores those it gets.
    pub fn install_beats(&mut self, beats: Beats) {
        self.beats = Some(BeatSchedule {
            beats,
            next: Duration::ZERO,
            shards: Vec::new(),
            metadata: Vec::new(),
        });
    }

    /// When [`Self::beat_tick`] is next due, on the beats' clock.
    pub fn beat_deadline(&self) -> Option<Duration> {
        self.beats.as_ref().map(|schedule| schedule.next)
    }

    /// Send each broker peer a beat when due, and note peers that fell
    /// silent. A beat that does not fit its queue is dropped; the next one
    /// replaces it.
    pub fn beat_tick(&mut self) {
        let Some(mut schedule) = self.beats.take() else {
            return;
        };
        let now = (schedule.beats.0.clock)();
        if now >= schedule.next {
            schedule.next = now.saturating_add(schedule.beats.0.timing.interval);
            schedule.beats.age_out();
            schedule.beats.report(&mut schedule.shards);
            self.send_beats(&mut schedule);
        }
        self.beats = Some(schedule);
    }

    fn send_beats(&mut self, schedule: &mut BeatSchedule) {
        let links: Vec<_> = (self.peers.iter())
            .filter(|(_, state)| state.access.kind == Kind::Broker)
            .filter_map(|(&peer, state)| Some((peer, state.link?.binding.session)))
            .collect();
        for (peer, session) in links {
            let envelope = Envelope {
                opcode: Opcode::Beat,
                response: false,
                request_id: None,
                sender: self.dispatcher.local,
                session: Some(session),
            };
            let limits = EnvelopeLimits::default();
            if let Ok(header) =
                beat::encode(envelope, &schedule.shards, limits, &mut schedule.metadata)
            {
                let frames = vec![
                    Bytes::copy_from_slice(&header),
                    Bytes::copy_from_slice(&schedule.metadata),
                    Bytes::new(),
                ];
                let _ = self.try_reply(Class::Control, routed(peer, frames));
            }
        }
    }

    /// Keep one beat from a broker on its current session.
    pub(super) fn receive_beat(
        &mut self,
        peer: NodeId,
        kind: Kind,
        packet: ozzy_proto::Packet<'_>,
        limits: EnvelopeLimits,
    ) -> Result<(), ReceiveError> {
        let shards = beat::decode(packet, limits).map_err(|_| ReceiveError::Peer)?;
        let session = self.links.get(peer).map(|link| link.binding.session);
        let Some(session) = session.filter(|_| kind == Kind::Broker) else {
            return Err(ReceiveError::Peer);
        };
        if packet.envelope.sender != peer || packet.envelope.session != Some(session) {
            return Err(ReceiveError::Peer);
        }
        if let Some(schedule) = &self.beats {
            schedule.beats.observe(peer, session, shards);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn session(byte: u8) -> LinkSessionId {
        LinkSessionId::from_bytes([byte; 16])
    }

    const TIMING: BeatTiming = BeatTiming {
        interval: Duration::from_millis(100),
        timeout: Duration::from_secs(1),
    };

    const NANO: Duration = Duration::from_nanos(1);

    fn beats() -> (Beats, Arc<Mutex<Duration>>) {
        let time = Arc::new(Mutex::new(Duration::from_secs(10)));
        let read = time.clone();
        let beats = Beats::new([0, 4], [node(2), node(3)], TIMING, 7, move || {
            *read.lock().unwrap()
        });
        (beats, time)
    }

    fn pass(time: &Mutex<Duration>, by: Duration) {
        *time.lock().unwrap() += by;
    }

    fn named(shard: u32, epoch: u64) -> ShardEpoch {
        ShardEpoch { shard, epoch }
    }

    #[test]
    fn a_beat_covers_the_shards_and_session_it_names_until_the_timeout() {
        let (beats, time) = beats();
        let source = BeatSource { shard: 1, epoch: 5 };
        assert!(!beats.covers(node(2), session(1), source), "never heard");
        assert_eq!(beats.heard(node(2)), None);
        beats.observe(node(2), session(1), [named(1, 5), named(2, 9)]);
        assert!(beats.covers(node(2), session(1), source));
        assert!(!beats.covers(node(3), session(1), source), "another peer");
        assert!(
            !beats.covers(node(2), session(2), source),
            "another session"
        );
        let other = BeatSource { shard: 2, epoch: 5 };
        assert!(!beats.covers(node(2), session(1), other), "another epoch");
        pass(&time, TIMING.timeout.checked_sub(NANO).unwrap());
        assert!(beats.covers(node(2), session(1), source));
        pass(&time, NANO);
        assert!(!beats.covers(node(2), session(1), source), "silent");
        assert_eq!(beats.heard(node(2)), Some(TIMING.timeout));
    }

    #[test]
    fn a_changed_beat_and_a_silent_peer_signal_once() {
        let (beats, time) = beats();
        let mut seen = beats.generation();
        let mut signaled = || {
            let generation = beats.generation();
            std::mem::replace(&mut seen, generation) != generation
        };
        beats.observe(node(2), session(1), [named(1, 5)]);
        assert!(signaled(), "the first beat");
        beats.observe(node(2), session(1), [named(1, 5)]);
        beats.age_out();
        assert!(!signaled(), "the same beat again");
        beats.observe(node(2), session(1), [named(1, 6)]);
        assert!(signaled(), "a withdrawn epoch");
        beats.observe(node(2), session(2), [named(1, 6)]);
        assert!(signaled(), "a new session");
        pass(&time, TIMING.timeout);
        beats.age_out();
        assert!(signaled(), "silence");
        beats.age_out();
        assert!(!signaled(), "still silent");
        beats.observe(node(2), session(2), [named(1, 6)]);
        assert!(!signaled(), "speaking again changes nothing said");
        pass(&time, TIMING.timeout);
        beats.age_out();
        assert!(signaled(), "silent again");
    }

    #[test]
    fn a_shard_reports_its_epoch_while_it_turns() {
        let (beats, time) = beats();
        let mut shards = Vec::new();
        beats.report(&mut shards);
        assert_eq!(shards, [], "no shard turned yet");
        beats.turn(0);
        beats.turn(4);
        beats.turn(9);
        pass(&time, (TIMING.timeout / 2).checked_sub(NANO).unwrap());
        beats.turn(4);
        beats.withdraw(4);
        beats.withdraw(9);
        assert_eq!(beats.source(4), Some(BeatSource { shard: 4, epoch: 8 }));
        assert_eq!(beats.source(9), None);
        beats.report(&mut shards);
        assert_eq!(shards, [named(0, 7), named(4, 8)]);
        pass(&time, NANO);
        beats.report(&mut shards);
        assert_eq!(shards, [named(4, 8)], "shard 0 stopped turning");
    }
}
