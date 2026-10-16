//! Thread-owned event values that any thread can total. Each thread writes
//! only its own values. An exiting thread folds its values into the retired
//! totals, so totals keep counting every thread that ever recorded.

use crate::{
    Event,
    names::{EVENT_KINDS, EVENT_NAMES},
};
use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError,
    atomic::{AtomicU64, Ordering::Relaxed},
};

const EVENTS: usize = EVENT_NAMES.len();

/// Live threads' values. Locked only to register, retire, or total.
static LIVE: Mutex<Vec<Arc<Values>>> = Mutex::new(Vec::new());
/// Exited threads' values, written only under the `LIVE` lock.
static RETIRED: Values = Values::new();

/// One thread's event values.
#[derive(Debug)]
pub(crate) struct Values([AtomicU64; EVENTS]);

impl Values {
    const fn new() -> Self {
        Self([const { AtomicU64::new(0) }; EVENTS])
    }

    /// Zeroed values that totals include from now on.
    pub(crate) fn register() -> Arc<Self> {
        let values = Arc::new(Self::new());
        live().push(values.clone());
        values
    }

    /// Only the registering thread writes, so a relaxed load and store lose
    /// no update.
    pub(crate) fn update(&self, event: Event, change: impl FnOnce(u64) -> u64) {
        let value = &self.0[event as usize];
        value.store(change(value.load(Relaxed)), Relaxed);
    }

    pub(crate) fn get(&self) -> [u64; EVENTS] {
        std::array::from_fn(|index| self.0[index].load(Relaxed))
    }

    /// Combine these values into `totals` by each event's kind.
    fn merge_into(&self, totals: &mut [u64; EVENTS]) {
        for ((total, value), kind) in totals.iter_mut().zip(&self.0).zip(EVENT_KINDS) {
            *total = kind.merge(*total, value.load(Relaxed));
        }
    }

    /// Fold these values into the retired totals and stop listing them.
    pub(crate) fn retire(self: &Arc<Self>) {
        let mut live = live();
        let mut retired = RETIRED.get();
        self.merge_into(&mut retired);
        for (value, retired) in RETIRED.0.iter().zip(retired) {
            value.store(retired, Relaxed);
        }
        live.retain(|values| !Arc::ptr_eq(values, self));
    }
}

fn live() -> MutexGuard<'static, Vec<Arc<Values>>> {
    LIVE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Process-wide event values at one moment, combined by each event's kind.
/// Threads may be mid-update; this is not a visibility fence.
#[derive(Clone, Debug)]
pub struct Totals([u64; EVENTS]);

impl Totals {
    /// One event's total.
    pub const fn get(&self, event: Event) -> u64 {
        self.0[event as usize]
    }
}

/// Every thread's event values, including threads that exited.
pub fn totals() -> Totals {
    let live = live();
    let mut totals = RETIRED.get();
    for values in live.iter() {
        values.merge_into(&mut totals);
    }
    Totals(totals)
}
