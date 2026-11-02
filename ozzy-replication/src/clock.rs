//! Whether this broker's wall clock agrees with the other brokers. No I/O: the
//! owner supplies each sample and reads both clocks. Monotonic time alone decides
//! which samples still count, so a wall clock step cannot revive an old one.

use std::time::Duration;

use ozzy_proto::NodeId;

/// How often a broker sends its wall time to each peer.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// How long a received sample counts.
pub const SAMPLE_FRESH: Duration = Duration::from_secs(5);

/// What the fresh samples say about the local clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockVerdict {
    /// The clocks agree within the tolerance.
    Agreed,
    /// Fresh samples put the local clock outside the tolerance.
    Disagreed,
    /// No fresh sample: nothing to compare with.
    Unknown,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    offset_millis: i64,
    arrived: Duration,
}

/// The latest sample of each peer. A delayed sample looks like a slow peer, so
/// it can pause decisions but never approve a wrong clock: the median needs a
/// majority of the clocks within the tolerance.
#[derive(Debug, Clone)]
pub struct ClockGuard {
    tolerance_millis: u64,
    peers: Vec<(NodeId, Option<Sample>)>,
}

impl ClockGuard {
    /// Compare with `peers`, allowing `tolerance_millis` of disagreement.
    pub fn new(peers: impl IntoIterator<Item = NodeId>, tolerance_millis: u64) -> Self {
        Self {
            tolerance_millis,
            peers: peers.into_iter().map(|peer| (peer, None)).collect(),
        }
    }

    /// Keep `peer`'s sample: its wall time as sent, the local wall time at
    /// receipt, and the local monotonic time. Unconfigured peers are ignored.
    pub fn observe(
        &mut self,
        peer: NodeId,
        peer_wall_millis: u64,
        local_wall_millis: u64,
        now: Duration,
    ) {
        let difference = i128::from(peer_wall_millis) - i128::from(local_wall_millis);
        let offset_millis =
            i64::try_from(difference).unwrap_or(if difference < 0 { i64::MIN } else { i64::MAX });
        if let Some((_, slot)) = self.peers.iter_mut().find(|(id, _)| *id == peer) {
            *slot = Some(Sample {
                offset_millis,
                arrived: now,
            });
        }
    }

    /// Drop every sample. Their offsets were measured against a clock that has
    /// since stepped.
    pub fn forget(&mut self) {
        for (_, sample) in &mut self.peers {
            *sample = None;
        }
    }

    /// Each peer's fresh offset in milliseconds, peer minus local.
    pub fn offsets(&self, now: Duration) -> impl Iterator<Item = (NodeId, Option<i64>)> + '_ {
        self.peers.iter().map(move |(peer, sample)| {
            let fresh = sample.filter(|sample| now.saturating_sub(sample.arrived) <= SAMPLE_FRESH);
            (*peer, fresh.map(|sample| sample.offset_millis))
        })
    }

    /// Take the median of zero (this clock) and the fresh offsets. With two
    /// clocks both count, because neither can outvote the other.
    pub fn verdict(&self, now: Duration) -> ClockVerdict {
        let mut clocks: Vec<i64> = self
            .offsets(now)
            .filter_map(|(_, offset)| offset)
            .chain([0])
            .collect();
        if clocks.len() < 2 {
            return ClockVerdict::Unknown;
        }
        clocks.sort_unstable();
        let lower = (clocks.len() - 1) / 2;
        let upper = clocks.len() / 2;
        let agreed = [lower, upper].iter().all(|&index| {
            clocks
                .get(index)
                .is_some_and(|offset| offset.unsigned_abs() <= self.tolerance_millis)
        });
        if agreed {
            ClockVerdict::Agreed
        } else {
            ClockVerdict::Disagreed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3_600_000;
    const WALL: u64 = 10 * HOUR;
    const TOLERANCE: u64 = 1000;

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn at(seconds: u64) -> Duration {
        Duration::from_secs(seconds)
    }

    fn guard() -> ClockGuard {
        ClockGuard::new([node(1), node(2)], TOLERANCE)
    }

    #[test]
    fn no_fresh_sample_is_unknown() {
        assert_eq!(guard().verdict(at(0)), ClockVerdict::Unknown);
    }

    #[test]
    fn a_fast_local_clock_disagrees_with_both_peers() {
        let mut guard = guard();
        guard.observe(node(1), WALL, WALL + HOUR, at(0));
        guard.observe(node(2), WALL, WALL + HOUR, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Disagreed);
    }

    #[test]
    fn a_slow_local_clock_disagrees_with_both_peers() {
        let mut guard = guard();
        guard.observe(node(1), WALL + HOUR, WALL, at(0));
        guard.observe(node(2), WALL + HOUR, WALL, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Disagreed);
    }

    #[test]
    fn one_bad_peer_is_outvoted_whichever_way_it_is_wrong() {
        for bad in [WALL + HOUR, WALL - HOUR] {
            let mut guard = guard();
            guard.observe(node(1), bad, WALL, at(0));
            guard.observe(node(2), WALL, WALL, at(0));
            assert_eq!(guard.verdict(at(0)), ClockVerdict::Agreed);
        }
    }

    #[test]
    fn a_clock_between_two_bad_peers_agrees() {
        let mut guard = guard();
        guard.observe(node(1), WALL + HOUR, WALL, at(0));
        guard.observe(node(2), WALL - HOUR, WALL, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Agreed);
    }

    #[test]
    fn one_sample_cannot_tell_which_clock_is_wrong() {
        let mut guard = guard();
        guard.observe(node(1), WALL + HOUR, WALL, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Disagreed);
        guard.observe(node(1), WALL - HOUR, WALL, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Disagreed);
        guard.observe(node(1), WALL + TOLERANCE, WALL, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Agreed);
    }

    #[test]
    fn the_tolerance_is_inclusive() {
        for (peer, expected) in [
            (WALL + TOLERANCE, ClockVerdict::Agreed),
            (WALL - TOLERANCE, ClockVerdict::Agreed),
            (WALL + TOLERANCE + 1, ClockVerdict::Disagreed),
            (WALL - TOLERANCE - 1, ClockVerdict::Disagreed),
        ] {
            let mut guard = guard();
            guard.observe(node(1), peer, WALL, at(0));
            guard.observe(node(2), peer, WALL, at(0));
            assert_eq!(guard.verdict(at(0)), expected, "peer wall {peer}");
        }
    }

    #[test]
    fn one_slow_link_never_pauses() {
        let mut guard = guard();
        // Sent an hour before receipt, against a prompt sample from the other.
        guard.observe(node(1), WALL - HOUR, WALL, at(0));
        guard.observe(node(2), WALL - 5, WALL, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Agreed);
    }

    #[test]
    fn stale_samples_stop_counting() {
        let mut guard = guard();
        guard.observe(node(1), WALL, WALL, at(0));
        guard.observe(node(2), WALL, WALL, at(3));
        assert_eq!(guard.verdict(at(5)), ClockVerdict::Agreed);
        // Only the newer one is left: it alone decides.
        assert_eq!(guard.verdict(at(6)), ClockVerdict::Agreed);
        assert_eq!(
            guard.offsets(at(6)).collect::<Vec<_>>(),
            [(node(1), None), (node(2), Some(0))]
        );
        assert_eq!(guard.verdict(at(9)), ClockVerdict::Unknown);
    }

    #[test]
    fn a_stale_bad_sample_leaves_the_good_one_alone() {
        let mut guard = guard();
        guard.observe(node(1), WALL + HOUR, WALL, at(0));
        guard.observe(node(2), WALL, WALL, at(4));
        assert_eq!(guard.verdict(at(6)), ClockVerdict::Agreed);
    }

    #[test]
    fn a_wall_clock_step_does_not_revive_a_stale_sample() {
        let mut guard = guard();
        guard.observe(node(1), WALL, WALL, at(0));
        guard.observe(node(2), WALL, WALL, at(0));
        // Freshness only reads the monotonic time passed in.
        assert_eq!(guard.verdict(at(6)), ClockVerdict::Unknown);
        guard.observe(node(1), WALL, WALL - HOUR, at(6));
        assert_eq!(guard.verdict(at(6)), ClockVerdict::Disagreed);
    }

    #[test]
    fn a_new_sample_replaces_the_old_one() {
        let mut guard = guard();
        guard.observe(node(1), WALL + HOUR, WALL, at(0));
        guard.observe(node(2), WALL + HOUR, WALL, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Disagreed);
        guard.observe(node(1), WALL, WALL, at(1));
        guard.observe(node(2), WALL, WALL, at(1));
        assert_eq!(guard.verdict(at(1)), ClockVerdict::Agreed);
    }

    #[test]
    fn extreme_wall_times_saturate() {
        let mut guard = guard();
        guard.observe(node(1), u64::MAX, 0, at(0));
        guard.observe(node(2), 0, u64::MAX, at(0));
        assert_eq!(
            guard.offsets(at(0)).collect::<Vec<_>>(),
            [(node(1), Some(i64::MAX)), (node(2), Some(i64::MIN))]
        );
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Agreed);
        guard.observe(node(2), u64::MAX, 0, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Disagreed);
    }

    #[test]
    fn an_unconfigured_peer_is_ignored() {
        let mut guard = guard();
        guard.observe(node(9), WALL, WALL, at(0));
        assert_eq!(guard.verdict(at(0)), ClockVerdict::Unknown);
    }
}
