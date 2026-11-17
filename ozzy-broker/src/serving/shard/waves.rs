//! Logs opened at start that have not found their leader yet. A shard
//! opens its logs a window at a time: each reopened copy promises,
//! installs and activates on disk, and all copies doing that at once queue
//! on the device for longer than an election waits. Brokers list their
//! logs in the same order, so the copies of a log open about together.

use ozzy_proto::GroupId;
use std::time::Duration;

/// Logs that may open or settle at once, at first. Three brokers on one
/// disk of about 1,400 flushes per second hold elections of 32 logs within
/// their timeouts, and no longer those of 64.
const WINDOW: usize = 16;
/// How long a log holds its place without a leader: the longest wait of
/// one election.
const SETTLE: Duration = Duration::from_secs(8);

pub(super) struct Settling {
    /// Opened logs without a known leader, and when each gives up its place.
    waiting: Vec<(GroupId, Duration)>,
    window: usize,
}

impl Settling {
    pub(super) const fn new() -> Self {
        Self {
            waiting: Vec::new(),
            window: WINDOW,
        }
    }

    /// Whether another log may open beside `opening` open jobs.
    pub(super) const fn room(&self, opening: usize) -> bool {
        opening + self.waiting.len() < self.window
    }

    pub(super) fn opened(&mut self, log: GroupId, now: Duration) {
        self.waiting.push((log, now.saturating_add(SETTLE)));
    }

    /// Release the places of logs that `settled`, and of logs that waited
    /// too long. A log that finds no leader in time has peers that are
    /// down or slow, and holding the others back would not help it: each
    /// such log doubles the window, so a broker that starts alone still
    /// opens all its logs soon.
    pub(super) fn observe(&mut self, now: Duration, settled: impl Fn(GroupId) -> bool) {
        let mut window = self.window;
        self.waiting.retain(|&(log, until)| {
            if settled(log) {
                return false;
            }
            if now >= until {
                window = window.saturating_mul(2);
                return false;
            }
            true
        });
        self.window = window;
    }

    /// When the next waiting log gives up its place.
    pub(super) fn next_deadline(&self) -> Option<Duration> {
        self.waiting.iter().map(|&(_, until)| until).min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(index: u8) -> GroupId {
        GroupId::from_bytes([index; 16])
    }

    #[test]
    fn a_settled_log_gives_its_place_to_the_next() {
        let mut settling = Settling::new();
        for index in 0..WINDOW {
            assert!(settling.room(0));
            settling.opened(log(index as u8), Duration::ZERO);
        }
        assert!(!settling.room(0));
        settling.observe(Duration::from_millis(1), |id| id == log(3));
        assert!(settling.room(0));
        assert!(!settling.room(1));
        assert_eq!(settling.next_deadline(), Some(SETTLE));
    }

    #[test]
    fn logs_without_a_leader_widen_the_window() {
        let mut settling = Settling::new();
        for index in 0..WINDOW {
            settling.opened(log(index as u8), Duration::ZERO);
        }
        settling.observe(SETTLE, |_| false);
        assert_eq!(settling.next_deadline(), None);
        // Each of them doubled it: nothing is held back any more.
        assert!(settling.room(1 << 16));
    }
}
