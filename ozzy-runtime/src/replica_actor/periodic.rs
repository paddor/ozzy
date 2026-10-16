//! A turn that comes due once per interval.

use std::time::Duration;

/// When a periodic turn is next due, the interval that sets it, and what each
/// turn may use.
#[derive(Debug, Clone, Copy)]
pub(super) struct Periodic<B = ()> {
    interval: Duration,
    next: Duration,
    pub(super) budget: B,
}

impl<B> Periodic<B> {
    /// The first turn is due one interval after start.
    pub(super) const fn new(interval: Duration, budget: B) -> Self {
        Self {
            interval,
            next: interval,
            budget,
        }
    }

    pub(super) const fn next(&self) -> Duration {
        self.next
    }

    /// A turn started at `now`; the next is due one interval later.
    pub(super) fn restart(&mut self, now: Duration) {
        self.next = now.saturating_add(self.interval);
    }

    /// The turn left work behind: due again right away.
    pub(super) fn repeat(&mut self, now: Duration) {
        self.next = now;
    }
}
