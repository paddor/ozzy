//! One reading of the broker clock, as a journal command sees it.

use std::time::Duration;

/// Wall time, and whether the brokers' clocks agree on it. Stamps always use
/// the wall time. Decisions that depend on it wait for agreement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockReading {
    /// Wall time in Unix milliseconds.
    pub wall_millis: u64,
    /// The clock agrees with the other brokers, or there are none.
    pub trusted: bool,
    /// Monotonic time on the shard's deadline origin, read with the wall
    /// time. The leader's deduplication time advances by it, so no wall
    /// clock step moves a dedup window.
    pub monotonic: Duration,
}

impl ClockReading {
    /// The time a clock-driven decision may use.
    #[must_use]
    pub const fn decision(self) -> Option<u64> {
        if self.trusted {
            Some(self.wall_millis)
        } else {
            None
        }
    }
}

/// A plain time is trusted: an owner with no peers has nothing to compare with.
/// Its monotonic time is tokio's, since this thread first read one, so a
/// test that pauses and advances tokio time moves it.
impl From<u64> for ClockReading {
    fn from(wall_millis: u64) -> Self {
        thread_local! {
            static ORIGIN: tokio::time::Instant = tokio::time::Instant::now();
        }
        Self {
            wall_millis,
            trusted: true,
            monotonic: ORIGIN.with(tokio::time::Instant::elapsed),
        }
    }
}
