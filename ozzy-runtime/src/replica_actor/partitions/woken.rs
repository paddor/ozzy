//! One partition's wake mark, set from any thread, taken by its shard.

#[cfg(not(all(test, ozzy_loom)))]
use futures::task::AtomicWaker;
#[cfg(all(test, ozzy_loom))]
use loom::future::AtomicWaker;
#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::cell::Cell;
use std::sync::Arc;
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Wake, Waker};

/// The marks of one shard's partitions as one bit each, and the shard's
/// waker. The shard finds its marked partitions here without asking each.
#[derive(Debug)]
pub(super) struct Ready {
    shard: AtomicWaker,
    words: Box<[AtomicU64]>,
}

impl Ready {
    /// Room for `marks` partitions, none marked.
    pub(super) fn new(marks: usize) -> Self {
        Self {
            shard: AtomicWaker::new(),
            words: (0..marks.div_ceil(64)).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// The shard registers before it drains, so a later mark wakes it.
    pub(super) fn register(&self, waker: &Waker) {
        #[cfg(not(all(test, ozzy_loom)))]
        self.shard.register(waker);
        #[cfg(all(test, ozzy_loom))]
        self.shard.register_by_ref(waker);
    }

    pub(super) fn wake(&self) {
        self.shard.wake();
    }

    fn mark(&self, bit: usize) {
        // Release: the drain that takes this bit sees the mark set before it.
        self.words[bit / 64].fetch_or(1 << (bit % 64), Ordering::Release);
    }

    /// Move every bit into `into`, which has a word per word here.
    pub(super) fn drain(&self, into: &mut [u64]) {
        for (word, into) in self.words.iter().zip(into) {
            if word.load(Ordering::Acquire) != 0 {
                *into |= word.swap(0, Ordering::AcqRel);
            }
        }
    }
}

/// A producer publishes work, then wakes: it sets the mark and its bit, and
/// wakes the shard. The shard registers its waker before reading bits and
/// takes a mark only to visit, so the visit observes everything published
/// before it. Both sides change the mark with a read-modify-write, a handoff
/// Loom models exactly. A partition that wakes itself during its own visit
/// only sets the mark; the visitor checks it on the same thread afterwards.
#[derive(Debug)]
pub(super) struct Woken {
    set: AtomicBool,
    ready: Arc<Ready>,
    /// This partition's bit in `ready`, fixed while it lives in the shard.
    bit: usize,
}

impl Woken {
    /// Start marked, so a new partition gets its first visit.
    pub(super) fn new(ready: Arc<Ready>, bit: usize) -> Self {
        ready.mark(bit);
        Self {
            set: AtomicBool::new(true),
            ready,
            bit,
        }
    }

    pub(super) fn is_set(&self) -> bool {
        self.set.load(Ordering::Acquire)
    }

    /// Keep a partition for the next poll without waking the shard now.
    pub(super) fn keep(&self) {
        self.set.store(true, Ordering::Release);
        self.ready.mark(self.bit);
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
            self.ready.mark(self.bit);
            self.ready.wake();
        }
    }
}
