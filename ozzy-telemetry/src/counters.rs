//! Thread-owned event values that any thread can total. Each thread writes
//! only its own values. An exiting thread folds its values into the retired
//! totals, so totals keep counting every thread that ever recorded.

use crate::{
    Event, Kind,
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
static RETIRED: Values = Values::new(String::new());

/// One thread's event values.
#[derive(Debug)]
pub(crate) struct Values {
    thread: String,
    values: [AtomicU64; EVENTS],
}

impl Values {
    const fn new(thread: String) -> Self {
        Self {
            thread,
            values: [const { AtomicU64::new(0) }; EVENTS],
        }
    }

    /// Zeroed values of `thread` that totals include from now on.
    pub(crate) fn register(thread: String) -> Arc<Self> {
        let values = Arc::new(Self::new(thread));
        live().push(values.clone());
        values
    }

    /// Only the registering thread writes, so a relaxed load and store lose
    /// no update.
    pub(crate) fn update(&self, event: Event, change: impl FnOnce(u64) -> u64) {
        let value = &self.values[event as usize];
        value.store(change(value.load(Relaxed)), Relaxed);
    }

    pub(crate) fn get(&self) -> [u64; EVENTS] {
        std::array::from_fn(|index| self.values[index].load(Relaxed))
    }

    /// Fold these values into the retired totals and stop listing them.
    pub(crate) fn retire(self: &Arc<Self>) {
        let mut live = live();
        let mut retired = RETIRED.get();
        merge(&mut retired, &self.get());
        for (value, retired) in RETIRED.values.iter().zip(retired) {
            value.store(retired, Relaxed);
        }
        live.retain(|values| !Arc::ptr_eq(values, self));
    }
}

/// Combine one thread's `values` into `totals` by each event's kind.
fn merge(totals: &mut [u64; EVENTS], values: &[u64; EVENTS]) {
    for ((total, value), kind) in totals.iter_mut().zip(values).zip(EVENT_KINDS) {
        *total = kind.merge(*total, *value);
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

    /// Every event's published name, kind, and value, in declaration order.
    pub fn rows(&self) -> impl Iterator<Item = (&'static str, Kind, u64)> + '_ {
        EVENT_NAMES
            .into_iter()
            .zip(EVENT_KINDS)
            .zip(self.0)
            .map(|((name, kind), value)| (name, kind, value))
    }
}

/// Every thread's event values, including threads that exited.
pub fn totals() -> Totals {
    let live = live();
    let mut totals = RETIRED.get();
    for values in live.iter() {
        merge(&mut totals, &values.get());
    }
    Totals(totals)
}

/// Event values of each live thread and of all exited threads, read together.
#[derive(Clone, Debug)]
pub struct Threads {
    /// Live threads that recorded an event, in registration order, by thread
    /// name (or ID when unnamed). Names can repeat.
    pub live: Vec<(String, Totals)>,
    /// Exited threads' values, combined by each event's kind.
    pub exited: Totals,
    /// Every thread's values, as [`totals`] returns them.
    pub totals: Totals,
}

/// Each thread's event values. Holds the registry lock while copying, so
/// thread registration waits; poll it at human rates, not per operation.
pub fn threads() -> Threads {
    let live = live();
    let exited = RETIRED.get();
    let mut totals = exited;
    let threads = live
        .iter()
        .map(|values| {
            let now = values.get();
            merge(&mut totals, &now);
            (values.thread.clone(), Totals(now))
        })
        .collect();
    Threads {
        live: threads,
        exited: Totals(exited),
        totals: Totals(totals),
    }
}
