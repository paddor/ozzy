//! One partition's wake mark, set from any thread, taken by its shard.

#[cfg(not(all(test, ozzy_loom)))]
use futures::task::AtomicWaker;
#[cfg(all(test, ozzy_loom))]
use loom::future::AtomicWaker;
#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicBool, Ordering};
use std::cell::Cell;
use std::sync::Arc;
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;

/// A producer publishes work, then wakes: it sets the mark and wakes the
/// shard. The shard registers its waker before reading marks and takes a
/// mark only to visit, so the visit observes everything published before it.
/// Both sides change the mark with a read-modify-write, a handoff Loom models
/// exactly. A partition that wakes itself during its own visit only sets
/// the mark; the visitor checks it on the same thread afterwards.
#[derive(Debug)]
pub(super) struct Woken {
    set: AtomicBool,
    shard: Arc<AtomicWaker>,
}

impl Woken {
    /// Start marked, so a new partition gets its first visit.
    pub(super) fn new(shard: Arc<AtomicWaker>) -> Self {
        Self {
            set: AtomicBool::new(true),
            shard,
        }
    }

    pub(super) fn is_set(&self) -> bool {
        self.set.load(Ordering::Acquire)
    }

    /// Keep a partition for the next poll without waking the shard now.
    pub(super) fn keep(&self) {
        self.set.store(true, Ordering::Release);
    }

    /// Clear the mark right before visiting. Work published before a mark
    /// this returns is visible to the visit; a later mark stays set.
    pub(super) fn take(&self) -> bool {
        self.set.swap(false, Ordering::AcqRel)
    }
}

std::thread_local! {
    /// The mark of the partition this thread visits right now.
    static VISITED: Cell<*const Woken> = const { Cell::new(std::ptr::null()) };
}

/// A partition visit on this thread. Until dropped, the partition's own
/// wakes from this thread set its mark without waking the shard.
#[derive(Debug)]
pub(super) struct Visiting(());

impl Visiting {
    pub(super) fn enter(woken: &Arc<Woken>) -> Self {
        VISITED.set(Arc::as_ptr(woken));
        Self(())
    }
}

impl Drop for Visiting {
    fn drop(&mut self) {
        VISITED.set(std::ptr::null());
    }
}

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.set.swap(true, Ordering::AcqRel);
        if VISITED.get() != Arc::as_ptr(self) {
            self.shard.wake();
        }
    }
}
