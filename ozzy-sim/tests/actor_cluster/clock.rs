//! Virtual time. Only the simulator advances it, one 10 ms tick at a time.
//! Brokers see monotonic time as a `Duration` and Unix time in milliseconds.

use std::{cell::Cell, rc::Rc, time::Duration};

pub(crate) const TICK: Duration = Duration::from_millis(10);

/// 2026-01-01T00:00:00Z, so Unix timestamps look current.
const EPOCH_MILLIS: u64 = 1_767_225_600_000;

/// Shared by the simulator and the journal timestamp closures of its brokers.
/// Clones read the same time.
#[derive(Debug, Clone, Default)]
pub(crate) struct Clock(Rc<Cell<u64>>);

impl Clock {
    pub(crate) fn tick(&self) -> u64 {
        self.0.get()
    }

    pub(crate) fn advance(&self) {
        self.0.set(self.0.get() + 1);
    }

    pub(crate) fn now(&self) -> Duration {
        TICK * u32::try_from(self.tick()).expect("tick count fits u32")
    }

    pub(crate) fn unix_millis(&self) -> u64 {
        EPOCH_MILLIS + self.tick() * TICK.as_millis() as u64
    }
}
