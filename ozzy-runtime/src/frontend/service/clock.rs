//! Broker clock sampling. The dispatcher owns the guard, as it owns link
//! negotiation: a clock belongs to a host, and every peer message but this one
//! routes to a partition. Other threads read the published immutable status.

use super::{Class, Kind, NodeId, ReceiveError, Service, routed};
use arc_swap::ArcSwap;
use bytes::Bytes;
use ozzy_proto::{Envelope, EnvelopeLimits, Opcode, clock};
use ozzy_replication::clock::{ClockGuard, ClockVerdict, SAMPLE_INTERVAL};
use std::{sync::Arc, time::Duration};

/// What the guard last concluded.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClockStatus {
    /// Release, age retention, and idle expiry wait for the clocks to agree.
    pub paused: bool,
    /// Each peer's fresh offset in milliseconds, peer minus local. Empty on a
    /// single broker, which has no peers and no guard.
    pub offsets: Vec<(NodeId, Option<i64>)>,
}

/// What the dispatcher publishes for every thread.
#[derive(Debug, Default)]
struct Published {
    status: ClockStatus,
    /// Wall minus monotonic time when the clocks last agreed. A clock step
    /// moves it at once, long before the next sample can show it.
    anchor: Option<i128>,
    tolerance_millis: u64,
}

/// One writer's status, read by every thread. Only the dispatcher writes it.
#[derive(Clone, Debug)]
pub struct ClockTrust(Arc<ArcSwap<Published>>);

impl ClockTrust {
    /// A broker with no peers: nothing to compare with, and never paused.
    pub fn unguarded() -> Self {
        Self(Arc::default())
    }

    /// A broker with peers starts paused until their samples arrive, so a
    /// restart with a bad clock decides nothing on its first turn.
    pub fn guarded(peers: impl IntoIterator<Item = NodeId>, tolerance_millis: u64) -> Self {
        Self(Arc::new(ArcSwap::from_pointee(Published {
            status: ClockStatus {
                paused: true,
                offsets: peers.into_iter().map(|peer| (peer, None)).collect(),
            },
            anchor: None,
            tolerance_millis,
        })))
    }

    /// Whether clock-driven decisions may run at this reading: the clocks last
    /// agreed, and this clock has not stepped since. Reads `now` only when
    /// there is an anchor to compare with.
    pub fn trusted(&self, now: impl FnOnce() -> ClockNow) -> bool {
        let published = self.0.load();
        !published.status.paused
            && published
                .anchor
                .is_none_or(|anchor| !stepped(anchor, now().anchor(), published.tolerance_millis))
    }

    /// The latest published status.
    pub fn status(&self) -> ClockStatus {
        self.0.load().status.clone()
    }
}

/// Both clocks of this broker, read together.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClockNow {
    /// Wall time in Unix milliseconds.
    pub wall_millis: u64,
    /// Monotonic time on the deadlines' origin.
    pub monotonic: Duration,
}

impl ClockNow {
    /// Wall minus monotonic time. A running clock keeps it nearly constant.
    fn anchor(self) -> i128 {
        i128::from(self.wall_millis) - i128::try_from(self.monotonic.as_millis()).unwrap_or(0)
    }
}

fn stepped(anchor: i128, now: i128, tolerance_millis: u64) -> bool {
    (now - anchor).unsigned_abs() > u128::from(tolerance_millis)
}

/// The dispatcher's guard and its sampling schedule.
pub struct ClockSampling {
    guard: ClockGuard,
    trust: ClockTrust,
    tolerance_millis: u64,
    now: Box<dyn Fn() -> ClockNow>,
    next_sample: Duration,
    /// Wall minus monotonic time at the last look.
    anchor: Option<i128>,
    /// When the guard began to disagree. A burst of samples queued behind a
    /// stalled thread disagrees for an instant; only a lasting one warns.
    disagreed_since: Option<Duration>,
    warned: bool,
}

impl std::fmt::Debug for ClockSampling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClockSampling")
            .field("guard", &self.guard)
            .field("next_sample", &self.next_sample)
            .field("warned", &self.warned)
            .finish_non_exhaustive()
    }
}

impl ClockSampling {
    /// Compare with `peers` within `trust`'s tolerance and publish to it.
    pub fn new(
        peers: impl IntoIterator<Item = NodeId>,
        trust: ClockTrust,
        now: impl Fn() -> ClockNow + 'static,
    ) -> Self {
        let tolerance_millis = trust.0.load().tolerance_millis;
        Self {
            guard: ClockGuard::new(peers, tolerance_millis),
            trust,
            tolerance_millis,
            now: Box::new(now),
            next_sample: Duration::ZERO,
            anchor: None,
            disagreed_since: None,
            warned: false,
        }
    }

    /// Age out stale samples and publish what the rest say. Returns this
    /// broker's wall time once a sample to each broker peer is due.
    pub fn tick(&mut self) -> Option<u64> {
        let now = (self.now)();
        self.forget_after_step(now);
        self.evaluate(now);
        if now.monotonic < self.next_sample {
            return None;
        }
        self.next_sample = now.monotonic.saturating_add(SAMPLE_INTERVAL);
        Some(now.wall_millis)
    }

    /// Keep `peer`'s sample of its wall time, received on its current
    /// session, and publish the verdict.
    pub fn observe(&mut self, peer: NodeId, wall_millis: u64) {
        let now = (self.now)();
        self.forget_after_step(now);
        self.guard
            .observe(peer, wall_millis, now.wall_millis, now.monotonic);
        self.evaluate(now);
    }

    /// When [`Self::tick`] next returns a sample, on the deadlines' origin.
    pub fn deadline(&self) -> Duration {
        self.next_sample
    }

    /// Send the next sample without waiting for the interval, as for a new
    /// broker link.
    pub fn sample_soon(&mut self) {
        self.next_sample = Duration::ZERO;
    }

    /// Offsets measured before a clock step mean nothing after it. Keeping them
    /// would let one new sample and one old one outvote the truth.
    fn forget_after_step(&mut self, now: ClockNow) {
        let anchor = now.anchor();
        if self
            .anchor
            .is_some_and(|last| stepped(last, anchor, self.tolerance_millis))
        {
            self.guard.forget();
        }
        self.anchor = Some(anchor);
    }

    /// Publish the guard's conclusion, and warn once a disagreement lasts.
    fn evaluate(&mut self, clock: ClockNow) {
        let now = clock.monotonic;
        let verdict = self.guard.verdict(now);
        let status = ClockStatus {
            paused: verdict != ClockVerdict::Agreed,
            offsets: self.guard.offsets(now).collect(),
        };
        match verdict {
            ClockVerdict::Agreed => {
                self.disagreed_since = None;
                if std::mem::take(&mut self.warned) {
                    warn(&format!(
                        "broker clock agrees again; release, age retention, and idle expiry \
                         resumed; offsets {}",
                        offsets(&status)
                    ));
                }
            }
            ClockVerdict::Disagreed => {
                let since = *self.disagreed_since.get_or_insert(now);
                if !self.warned && now.saturating_sub(since) >= SAMPLE_INTERVAL {
                    self.warned = true;
                    warn(&format!(
                        "broker clock is over {} ms from its peers; release, age retention, and \
                         idle expiry paused; offsets {}",
                        self.tolerance_millis,
                        offsets(&status)
                    ));
                }
            }
            // Startup and lost peers: nothing commits without them anyway.
            ClockVerdict::Unknown => {}
        }
        let anchor = (verdict == ClockVerdict::Agreed).then(|| clock.anchor());
        self.trust.0.store(Arc::new(Published {
            status,
            anchor,
            tolerance_millis: self.tolerance_millis,
        }));
    }
}

/// A paused broker holds due records, so this also goes to stderr: the bounded
/// log alone is only seen by someone already looking.
fn warn(message: &str) {
    crate::profiling::log(crate::profiling::Level::Warn, message);
    eprintln!("{message}");
}

fn offsets(status: &ClockStatus) -> String {
    let rows: Vec<_> = (status.offsets.iter())
        .map(|(peer, offset)| {
            offset.map_or_else(|| format!("{peer} none"), |ms| format!("{peer} {ms} ms"))
        })
        .collect();
    rows.join(", ")
}

impl Service {
    /// Sample this broker's clock against its broker peers. Install before
    /// serving; a service without it answers no samples and sends none.
    pub fn install_clock(&mut self, sampling: ClockSampling) {
        self.clock = Some(sampling);
    }

    /// When [`Self::clock_tick`] is next due, on the deadlines' origin.
    pub fn clock_deadline(&self) -> Option<Duration> {
        self.clock.as_ref().map(ClockSampling::deadline)
    }

    /// Age out stale samples and send each broker peer this broker's wall time
    /// when due. A sample that does not fit its queue is dropped; the next one
    /// replaces it.
    pub fn clock_tick(&mut self) {
        let Some(wall_millis) = self.clock.as_mut().and_then(ClockSampling::tick) else {
            return;
        };
        let links: Vec<_> = (self.peers.iter())
            .filter(|(_, state)| state.access.kind == Kind::Broker)
            .filter_map(|(&peer, state)| Some((peer, state.link?.binding.session)))
            .collect();
        for (peer, session) in links {
            let envelope = Envelope {
                opcode: Opcode::ClockSample,
                response: false,
                request_id: None,
                sender: self.dispatcher.local,
                session: Some(session),
            };
            let limits = EnvelopeLimits::default();
            if let Ok((header, wall)) = clock::encode_sample(envelope, wall_millis, limits) {
                let frames = vec![
                    Bytes::copy_from_slice(&header),
                    Bytes::copy_from_slice(&wall),
                    Bytes::new(),
                ];
                let _ = self.try_reply(Class::Control, routed(peer, frames));
            }
        }
    }

    /// A new broker link sends its first sample without waiting for the tick.
    pub(super) fn sample_soon(&mut self) {
        if let Some(clock) = &mut self.clock {
            clock.sample_soon();
        }
    }

    /// Keep one sample from a broker on its current session.
    pub(super) fn receive_clock(
        &mut self,
        peer: NodeId,
        kind: Kind,
        packet: ozzy_proto::Packet<'_>,
        limits: EnvelopeLimits,
    ) -> Result<(), ReceiveError> {
        let wall = clock::decode_sample(packet, limits).map_err(|_| ReceiveError::Peer)?;
        let session = self.links.get(peer).map(|link| link.binding.session);
        if kind != Kind::Broker
            || packet.envelope.sender != peer
            || session.is_none()
            || packet.envelope.session != session
        {
            return Err(ReceiveError::Peer);
        }
        if let Some(clock) = &mut self.clock {
            clock.observe(peer, wall);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    struct Clock(Rc<Cell<ClockNow>>);

    impl Clock {
        fn sampling() -> (Self, ClockSampling, ClockTrust) {
            let shared = Rc::new(Cell::new(ClockNow {
                wall_millis: 100_000_000,
                monotonic: Duration::ZERO,
            }));
            let trust = ClockTrust::guarded([node(1), node(2)], 1000);
            let read = shared.clone();
            let sampling =
                ClockSampling::new([node(1), node(2)], trust.clone(), move || read.get());
            (Self(shared), sampling, trust)
        }

        fn pass(&self, time: Duration) {
            let now = self.0.get();
            self.0.set(ClockNow {
                wall_millis: now.wall_millis + u64::try_from(time.as_millis()).unwrap(),
                monotonic: now.monotonic + time,
            });
        }

        fn read(&self) -> ClockNow {
            self.0.get()
        }

        /// Step only the wall clock.
        fn step(&self, millis: i64) {
            let now = self.0.get();
            self.0.set(ClockNow {
                wall_millis: now.wall_millis.checked_add_signed(millis).unwrap(),
                ..now
            });
        }

        /// Both peers' samples sent with their wall clocks `offset` from ours.
        fn observe(&self, sampling: &mut ClockSampling, offset: i64) {
            let now = self.read();
            sampling.forget_after_step(now);
            let wall = now.wall_millis.checked_add_signed(offset).unwrap();
            for peer in [node(1), node(2)] {
                sampling
                    .guard
                    .observe(peer, wall, now.wall_millis, now.monotonic);
            }
            sampling.evaluate(now);
        }
    }

    #[test]
    fn a_single_broker_is_never_paused() {
        let trust = ClockTrust::unguarded();
        assert!(trust.trusted(|| panic!("no anchor to compare with")));
        assert_eq!(trust.status(), ClockStatus::default());
    }

    #[test]
    fn a_broker_with_peers_starts_paused() {
        let trust = ClockTrust::guarded([node(1)], 1000);
        assert!(!trust.trusted(|| panic!("paused needs no reading")));
        assert_eq!(trust.status().offsets, [(node(1), None)]);
    }

    #[test]
    fn evaluation_publishes_agreement_and_loss_of_samples() {
        let (clock, mut sampling, trust) = Clock::sampling();
        let now = clock.read();
        sampling
            .guard
            .observe(node(1), now.wall_millis + 5, now.wall_millis, now.monotonic);
        sampling.evaluate(now);
        assert!(trust.trusted(|| clock.read()));
        assert_eq!(
            trust.status().offsets,
            [(node(1), Some(5)), (node(2), None)]
        );
        clock.pass(Duration::from_secs(6));
        sampling.evaluate(clock.read());
        assert!(!trust.trusted(|| clock.read()));
        assert!(!sampling.warned, "lost samples stay quiet");
    }

    #[test]
    fn only_a_lasting_disagreement_warns_and_its_end_warns_once() {
        let (clock, mut sampling, trust) = Clock::sampling();
        clock.observe(&mut sampling, -3_600_000);
        assert!(!trust.trusted(|| clock.read()), "the pause does not wait");
        assert!(!sampling.warned);
        clock.pass(Duration::from_millis(5));
        clock.observe(&mut sampling, 0);
        assert!(trust.trusted(|| clock.read()));
        assert_eq!(sampling.disagreed_since, None, "the burst is forgotten");

        clock.observe(&mut sampling, -3_600_000);
        clock.pass(SAMPLE_INTERVAL);
        clock.observe(&mut sampling, -3_600_000);
        assert!(sampling.warned);
        clock.pass(SAMPLE_INTERVAL);
        clock.observe(&mut sampling, 0);
        assert!(trust.trusted(|| clock.read()));
        assert!(!sampling.warned);
    }

    #[test]
    fn a_clock_step_is_untrusted_on_the_next_read_in_either_direction() {
        for step in [3_600_000, -3_600_000] {
            let (clock, mut sampling, trust) = Clock::sampling();
            clock.observe(&mut sampling, 0);
            assert!(trust.trusted(|| clock.read()));
            clock.step(step);
            // Nothing has told the dispatcher yet.
            assert!(!trust.status().paused);
            assert!(!trust.trusted(|| clock.read()));
            clock.step(-step);
            assert!(trust.trusted(|| clock.read()), "a step back is fine");
        }
    }

    #[test]
    fn time_passing_and_steps_inside_the_tolerance_stay_trusted() {
        let (clock, mut sampling, trust) = Clock::sampling();
        clock.observe(&mut sampling, 0);
        clock.pass(Duration::from_secs(3600));
        clock.step(999);
        assert!(trust.trusted(|| clock.read()));
    }

    #[test]
    fn offsets_from_before_a_step_cannot_outvote_the_truth() {
        let (clock, mut sampling, trust) = Clock::sampling();
        clock.observe(&mut sampling, 0);
        clock.step(3_600_000);
        // The first sample after the step, against one measured before it.
        let now = clock.read();
        sampling.forget_after_step(now);
        sampling.guard.observe(
            node(1),
            now.wall_millis - 3_600_000,
            now.wall_millis,
            now.monotonic,
        );
        sampling.evaluate(now);
        assert!(trust.status().paused, "{:?}", trust.status());
        assert!(!trust.trusted(|| clock.read()));
    }
}
