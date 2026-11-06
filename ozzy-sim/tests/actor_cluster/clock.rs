//! Virtual time. Only the simulator advances it, one 10 ms tick at a time.
//! Brokers see monotonic time as a `Duration` and Unix time in milliseconds,
//! each host's wall clock off by its own seeded offset.

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

    /// True Unix time, as clients read it.
    pub(crate) fn unix_millis(&self) -> u64 {
        wall_millis(self.now())
    }
}

/// True Unix time at monotonic time `now`.
pub(crate) fn wall_millis(now: Duration) -> u64 {
    EPOCH_MILLIS + u64::try_from(now.as_millis()).expect("time fits u64")
}

/// One host's wall clock: true time plus an offset that a clock step
/// changes. A broker restart keeps its host's clock.
#[derive(Debug, Clone, Default)]
pub(crate) struct Host {
    clock: Clock,
    offset: Rc<Cell<i64>>,
}

impl Host {
    pub(crate) fn new(clock: &Clock) -> Self {
        Self {
            clock: clock.clone(),
            offset: Rc::default(),
        }
    }

    pub(crate) fn wall_millis(&self) -> u64 {
        self.clock
            .unix_millis()
            .checked_add_signed(self.offset.get())
            .expect("wall clock in range")
    }

    /// Monotonic time, which no step moves.
    pub(crate) fn now(&self) -> Duration {
        self.clock.now()
    }

    pub(crate) fn offset(&self) -> i64 {
        self.offset.get()
    }

    /// Step the wall clock to `offset` from true time.
    pub(crate) fn step(&self, offset: i64) {
        self.offset.set(offset);
    }
}
