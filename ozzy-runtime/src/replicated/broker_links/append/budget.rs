//! Aggregate transport reservations, including unused reply capacity and aliases.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::AppendLinkLimits;
use crate::signal::StateSignal;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Cost {
    pub(super) writers: usize,
    pub(super) requests: usize,
    pub(super) records: usize,
    pub(super) bytes: usize,
    pub(super) progress_bytes: usize,
}

#[derive(Debug, Default)]
struct Used {
    writers: AtomicUsize,
    requests: AtomicUsize,
    records: AtomicUsize,
    bytes: AtomicUsize,
}

#[derive(Debug)]
pub(super) struct Budget {
    limits: Option<AppendLinkLimits>,
    used: Used,
    // Opening and dropping a writer is rare. APPEND admission never takes this.
    progress: Mutex<BTreeMap<usize, usize>>,
    progress_max: AtomicUsize,
    pub(super) changed: StateSignal,
}

fn fits(held: usize, extra: usize, limit: usize) -> bool {
    held.checked_add(extra).is_some_and(|sum| sum <= limit)
}

#[allow(
    deprecated,
    reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
)]
fn reserve(counter: &AtomicUsize, extra: usize, limit: usize) -> bool {
    if extra == 0 {
        return true;
    }
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
            held.checked_add(extra).filter(|&sum| sum <= limit)
        })
        .is_ok()
}

fn release(counter: &AtomicUsize, amount: usize) {
    if amount != 0 {
        let previous = counter.fetch_sub(amount, Ordering::AcqRel);
        assert!(previous >= amount, "SDK APPEND reservation underflow");
    }
}

impl Budget {
    pub(super) fn new(limits: Option<AppendLinkLimits>) -> Self {
        Self {
            limits,
            used: Used::default(),
            progress: Mutex::new(BTreeMap::new()),
            progress_max: AtomicUsize::new(0),
            changed: StateSignal::default(),
        }
    }

    pub(super) fn possible(&self, cost: Cost) -> bool {
        self.fits(cost, false)
    }

    pub(super) fn available(&self, cost: Cost) -> bool {
        self.fits(cost, true)
    }

    fn fits(&self, cost: Cost, current: bool) -> bool {
        let Some(limits) = self.limits else {
            return false;
        };
        let held = |counter: &AtomicUsize| {
            if current {
                counter.load(Ordering::Acquire)
            } else {
                0
            }
        };
        let progress = if current {
            self.progress_max.load(Ordering::Acquire)
        } else {
            0
        };
        fits(held(&self.used.writers), cost.writers, limits.writers)
            && fits(held(&self.used.requests), cost.requests, limits.requests)
            && fits(held(&self.used.records), cost.records, limits.records)
            && fits(held(&self.used.bytes), cost.bytes, limits.bytes)
            && (cost.writers == 0
                || held(&self.used.bytes)
                    .checked_add(cost.bytes)
                    .and_then(|bytes| bytes.checked_add(progress.max(cost.progress_bytes)))
                    .is_some_and(|bytes| bytes <= limits.bytes))
    }

    pub(super) fn acquire(self: &Arc<Self>, cost: Cost) -> Option<Arc<Lease>> {
        let limits = self.limits?;
        let mut progress =
            (cost.writers != 0).then(|| self.progress.lock().expect("SDK writer budget poisoned"));
        let headroom = progress.as_ref().map_or(0, |_| {
            self.progress_max
                .load(Ordering::Acquire)
                .max(cost.progress_bytes)
        });
        let bytes_limit = limits.bytes.checked_sub(headroom)?;

        if !reserve(&self.used.writers, cost.writers, limits.writers) {
            return None;
        }
        if !reserve(&self.used.requests, cost.requests, limits.requests) {
            release(&self.used.writers, cost.writers);
            self.changed.notify_changed();
            return None;
        }
        if !reserve(&self.used.records, cost.records, limits.records) {
            release(&self.used.requests, cost.requests);
            release(&self.used.writers, cost.writers);
            self.changed.notify_changed();
            return None;
        }
        if !reserve(&self.used.bytes, cost.bytes, bytes_limit) {
            release(&self.used.records, cost.records);
            release(&self.used.requests, cost.requests);
            release(&self.used.writers, cost.writers);
            self.changed.notify_changed();
            return None;
        }
        if let Some(progress) = progress.as_mut() {
            *progress.entry(cost.progress_bytes).or_default() += 1;
            self.progress_max.store(
                progress.last_key_value().map_or(0, |(&bytes, _)| bytes),
                Ordering::Release,
            );
        }
        Some(Arc::new(Lease {
            budget: self.clone(),
            cost,
        }))
    }

    fn rollback(&self, cost: Cost) {
        release(&self.used.bytes, cost.bytes);
        release(&self.used.records, cost.records);
        release(&self.used.requests, cost.requests);
        release(&self.used.writers, cost.writers);
        self.changed.notify_changed();
    }
}

#[derive(Debug)]
pub(super) struct Lease {
    budget: Arc<Budget>,
    cost: Cost,
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.cost.writers != 0 {
            let mut progress = self
                .budget
                .progress
                .lock()
                .expect("SDK writer budget poisoned");
            let count = progress
                .get_mut(&self.cost.progress_bytes)
                .expect("reserved SDK progress");
            *count -= 1;
            if *count == 0 {
                progress.remove(&self.cost.progress_bytes);
            }
            self.budget.progress_max.store(
                progress.last_key_value().map_or(0, |(&bytes, _)| bytes),
                Ordering::Release,
            );
        }
        self.budget.rollback(self.cost);
    }
}
