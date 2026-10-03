//! Persistent readiness, state changes, and closure above transport-only wakeups.
//!
//! Notifications only schedule another poll. The atomics below own readiness;
//! registering before checking them closes the observation-to-wait race.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

/// Coalesced work readiness for one draining owner and any number of producers.
#[derive(Debug, Default)]
pub(crate) struct DataSignal {
    pending: AtomicBool,
    wake: Notify,
}

impl DataSignal {
    /// Publish work before calling this. Repeated marks coalesce until a drain.
    ///
    /// Reads before writing, so a flag that is already set costs no cache-line
    /// write. The fence pairs with the one in `drain`: either this read sees
    /// the cleared flag, or the drain sees the published work.
    pub(crate) fn mark(&self) {
        fence(Ordering::SeqCst);
        if self.pending.load(Ordering::Relaxed) {
            return;
        }
        if !self.pending.swap(true, Ordering::AcqRel) {
            self.wake.notify_one();
        }
    }

    /// Observe readiness without consuming it. Canceling a wait cannot steal work.
    pub(crate) async fn ready(&self) {
        loop {
            let mut notified = pin!(self.wake.notified());
            notified.as_mut().enable();
            if self.pending.load(Ordering::Acquire) {
                return;
            }
            // An earlier drain can leave a stored wake permit. Recheck persistent
            // readiness after consuming it instead of reporting phantom work.
            notified.await;
        }
    }

    /// Clear readiness before inspecting work, never afterward. Marks published
    /// during the callback survive it. A bounded drain must mark remaining work.
    pub(crate) fn drain<R>(&self, drain: impl FnOnce() -> R) -> R {
        self.pending.swap(false, Ordering::AcqRel);
        fence(Ordering::SeqCst);
        drain()
    }
}

impl std::task::Wake for DataSignal {
    fn wake(self: Arc<Self>) {
        self.mark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.mark();
    }
}

/// Broadcast changes to independently checked state or capacity predicates.
#[derive(Debug, Default)]
pub(crate) struct StateSignal {
    generation: AtomicU64,
    wake: Notify,
}

impl StateSignal {
    /// Capture this before checking the state whose change a caller awaits.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Publish the changed state before broadcasting. Generations never wrap.
    pub(crate) fn notify_changed(&self) {
        self.generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |generation| {
                generation.checked_add(1)
            })
            .expect("state signal generation exhausted");
        self.wake.notify_waiters();
    }

    /// Wait for a change since the captured generation, including before polling.
    /// This observes state without consuming another waiter's notification.
    pub(crate) async fn changed_after(&self, seen: u64) {
        loop {
            let mut notified = pin!(self.wake.notified());
            notified.as_mut().enable();
            if self.generation() != seen {
                return;
            }
            notified.await;
        }
    }

    /// Check a predicate and wait without a gap between observation and readiness.
    pub(crate) async fn wait_for<T>(&self, mut predicate: impl FnMut() -> Option<T>) -> T {
        loop {
            let seen = self.generation();
            if let Some(value) = predicate() {
                return value;
            }
            self.changed_after(seen).await;
        }
    }
}

/// Persistent, idempotent closure shared by every current and future waiter.
#[derive(Debug, Clone, Default)]
pub(crate) struct CloseSignal {
    state: Arc<CloseState>,
}

#[derive(Debug, Default)]
struct CloseState {
    closed: AtomicBool,
    wake: Arc<Notify>,
}

impl CloseSignal {
    pub(crate) fn close(&self) {
        if !self.state.closed.swap(true, Ordering::AcqRel) {
            self.state.wake.notify_waiters();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    /// Own the wait without allocating another task, box, or shared state.
    pub(crate) fn closed(&self) -> Closed {
        Closed {
            state: self.state.clone(),
            notified: self.state.wake.clone().notified_owned(),
        }
    }
}

pin_project! {
    /// Owned close wait. Closure remains observable after cancellation or repoll.
    #[derive(Debug)]
    pub(crate) struct Closed {
        state: Arc<CloseState>,
        #[pin]
        notified: OwnedNotified,
    }
}

impl Future for Closed {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        this.notified.as_mut().enable();
        if this.state.closed.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        this.notified.poll(cx)
    }
}

#[cfg(test)]
mod tests;
