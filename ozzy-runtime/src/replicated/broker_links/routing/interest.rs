//! One partition's routing interest, raised by callers on any thread and
//! read by the watchers on the SDK driver thread.

use crate::signal::StateSignal;
#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicU64, Ordering};

/// Revision 0 until the partition is first used, then one more for each
/// refresh. Interest is never withdrawn. Every change rings the routing
/// signal after it is stored, and a watcher captures that signal before it
/// reads revisions, so a change made before a scan is never lost.
#[derive(Debug, Default)]
pub(super) struct Interest(AtomicU64);

impl Interest {
    /// Mark the partition used. Only its first use rings `changed`.
    pub(super) fn register(&self, changed: &StateSignal) {
        if self
            .0
            .compare_exchange(0, 1, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            changed.notify_changed();
        }
    }

    /// Ask for fresh snapshots, marking the partition used if it was not.
    pub(super) fn refresh(&self, changed: &StateSignal) {
        self.0.fetch_add(1, Ordering::Release);
        changed.notify_changed();
    }

    /// The current revision, 0 without interest.
    pub(super) fn revision(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}
