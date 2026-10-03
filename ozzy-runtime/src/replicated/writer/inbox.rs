//! Unbatched inbox bounded by records and owned body/table bytes. Capacity returns at
//! request preparation, never ACK.

use crate::signal::StateSignal;
#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub(super) struct Inbox {
    queued: AtomicUsize,
    queued_bytes: AtomicUsize,
    pub(super) capacity: usize,
    pub(super) capacity_bytes: usize,
    changed: Arc<StateSignal>,
}

impl Inbox {
    pub(super) fn new(capacity: usize, capacity_bytes: usize, changed: Arc<StateSignal>) -> Self {
        Self {
            queued: AtomicUsize::new(0),
            queued_bytes: AtomicUsize::new(0),
            capacity,
            capacity_bytes,
            changed,
        }
    }

    /// Reserve one record of `bytes`. An empty inbox admits any permitted
    /// record, so a record larger than the byte bound still progresses.
    pub(super) fn reserve(&self, bytes: usize) -> Option<Reservation<'_>> {
        self.queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                (queued < self.capacity).then(|| queued + 1)
            })
            .ok()?;
        let fits = self
            .queued_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                let total = queued.checked_add(bytes)?;
                (queued == 0 || total <= self.capacity_bytes).then_some(total)
            });
        if fits.is_err() {
            self.queued.fetch_sub(1, Ordering::AcqRel);
            self.changed.notify_changed();
            return None;
        }
        Some(Reservation {
            inbox: self,
            bytes,
            published: false,
        })
    }

    pub(super) fn available(&self, bytes: usize) -> bool {
        let queued_bytes = self.queued_bytes.load(Ordering::Acquire);
        self.queued.load(Ordering::Acquire) < self.capacity
            && (queued_bytes == 0
                || queued_bytes
                    .checked_add(bytes)
                    .is_some_and(|total| total <= self.capacity_bytes))
    }

    pub(super) fn release(&self, count: usize, bytes: usize) {
        if count != 0 {
            let previous = self.queued.fetch_sub(count, Ordering::AcqRel);
            assert!(previous >= count, "inbox release exceeds admission");
            let previous = self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
            assert!(previous >= bytes, "inbox byte release exceeds admission");
            self.changed.notify_changed();
        }
    }

    #[cfg(test)]
    pub(super) fn used(&self) -> usize {
        self.queued.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn used_bytes(&self) -> usize {
        self.queued_bytes.load(Ordering::Acquire)
    }
}

pub(super) struct Reservation<'a> {
    inbox: &'a Inbox,
    bytes: usize,
    published: bool,
}

impl Reservation<'_> {
    pub(super) fn publish(mut self) {
        self.published = true;
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.published {
            self.inbox.release(1, self.bytes);
        }
    }
}
