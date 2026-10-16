//! Parked worker threads per queue class. Publishing a job wakes one parked
//! worker of its class instead of every worker in the pool.

use std::sync::atomic::{AtomicU64, Ordering, fence};
use std::sync::{Arc, OnceLock};
use std::task::Wake;
use std::thread::Thread;

/// Queue class index of ordinary jobs.
pub(crate) const DATA: usize = 0;
/// Queue class index of reserved progress jobs.
pub(crate) const PROGRESS: usize = 1;

/// Bit `i` of `idle[class]` is set while worker `i` of that class is parked or
/// about to park. A worker sets its bit and then checks its queue once more
/// before parking; a waker changes the state it waits on and then claims one
/// set bit. The `SeqCst` fences on both sides make one of them see the other.
#[derive(Debug)]
pub(crate) struct Parking {
    threads: [Vec<OnceLock<Thread>>; 2],
    idle: [AtomicU64; 2],
}

/// One worker's place in [`Parking`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Seat {
    class: usize,
    index: usize,
}

impl Seat {
    pub(crate) const fn new(class: usize, index: usize) -> Self {
        Self { class, index }
    }

    const fn bit(self) -> u64 {
        1 << self.index
    }
}

impl Parking {
    /// At most 64 workers per class.
    pub(crate) fn new(data: usize, progress: usize) -> Self {
        let seats = |count| (0..count).map(|_| OnceLock::new()).collect();
        Self {
            threads: [seats(data), seats(progress)],
            idle: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }

    /// Record the calling thread as the one parking at `seat`.
    pub(crate) fn register(&self, seat: Seat) -> bool {
        self.threads[seat.class]
            .get(seat.index)
            .is_some_and(|slot| slot.set(std::thread::current()).is_ok())
    }

    /// Announce idleness. Check for work once more before parking.
    pub(crate) fn idle(&self, seat: Seat) {
        self.idle[seat.class].fetch_or(seat.bit(), Ordering::Relaxed);
        fence(Ordering::SeqCst);
    }

    /// Withdraw an announcement after finding work or waking.
    pub(crate) fn busy(&self, seat: Seat) {
        self.idle[seat.class].fetch_and(!seat.bit(), Ordering::Relaxed);
    }

    /// Wake one idle worker of `class` after publishing work it can take.
    /// Returns whether one was idle.
    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    pub(crate) fn wake_one(&self, class: usize) -> bool {
        fence(Ordering::SeqCst);
        let Ok(idle) =
            self.idle[class].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |idle| {
                (idle != 0).then(|| idle & (idle - 1))
            })
        else {
            return false;
        };
        let index = idle.trailing_zeros() as usize;
        if let Some(thread) = self.threads[class].get(index).and_then(OnceLock::get) {
            thread.unpark();
        }
        true
    }

    /// Wake every worker, for lifecycle changes that each one must observe.
    pub(crate) fn wake_all(&self) {
        for thread in self.threads.iter().flatten().filter_map(OnceLock::get) {
            thread.unpark();
        }
    }
}

/// A task waker that unparks one thread, for a blocking loop that waits on
/// an `AtomicWaker`.
pub(crate) struct Unpark(pub(crate) Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
