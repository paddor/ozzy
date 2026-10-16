//! The one home of async wakeup primitives: persistent readiness, state
//! changes, and closure above transport-only wakeups.
//!
//! Notifications only schedule another poll. The atomics below own readiness;
//! registering before checking them closes the observation-to-wait race.

#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;

/// Tokio's `Notify` orders a notification before any later registration
/// through its waiter lock and state, so that waiter also observes what was
/// published before the notification. Loom does not instrument those atomics.
/// This counter models the same edge under Loom and is empty otherwise. Both
/// sides use read-modify-writes, which Loom resolves to the newest value like
/// the lock they stand for.
#[derive(Debug, Default)]
struct NotifyOrder {
    #[cfg(all(test, ozzy_loom))]
    calls: AtomicU64,
}

#[allow(clippy::unused_self, reason = "empty unless modeled under Loom")]
impl NotifyOrder {
    /// Call just before notifying.
    fn notifying(&self) {
        #[cfg(all(test, ozzy_loom))]
        self.calls.fetch_add(1, Ordering::AcqRel);
    }

    /// Call just after enabling a wait, before checking state.
    fn registered(&self) {
        #[cfg(all(test, ozzy_loom))]
        self.calls.fetch_add(0, Ordering::AcqRel);
    }
}

/// Coalesced work readiness for one draining owner and any number of producers.
#[derive(Debug, Default)]
pub struct DataSignal {
    pending: AtomicBool,
    wake: Notify,
    order: NotifyOrder,
}

impl DataSignal {
    /// Publish work before calling this. Repeated marks coalesce until a drain.
    ///
    /// Reads before writing, so a flag that is already set costs no cache-line
    /// write. The fence pairs with the one in `drain`: either this read sees
    /// the cleared flag, or the drain sees the published work.
    pub fn mark(&self) {
        fence(Ordering::SeqCst);
        if self.pending.load(Ordering::Relaxed) {
            return;
        }
        if !self.pending.swap(true, Ordering::AcqRel) {
            self.order.notifying();
            self.wake.notify_one();
        }
    }

    /// Observe readiness without consuming it. Canceling a wait cannot steal work.
    pub async fn ready(&self) {
        loop {
            let mut notified = pin!(self.wake.notified());
            notified.as_mut().enable();
            self.order.registered();
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
    pub fn drain<R>(&self, drain: impl FnOnce() -> R) -> R {
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
pub struct StateSignal {
    generation: AtomicU64,
    wake: Notify,
    order: NotifyOrder,
}

impl StateSignal {
    /// Capture this before checking the state whose change a caller awaits.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Publish the changed state before broadcasting. Waiters compare
    /// generations for inequality, so wrapping after 2^64 changes is harmless.
    pub fn notify_changed(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.order.notifying();
        self.wake.notify_waiters();
    }

    /// Wait for a change since the captured generation, including before polling.
    /// This observes state without consuming another waiter's notification.
    pub async fn changed_after(&self, seen: u64) {
        loop {
            let mut notified = pin!(self.wake.notified());
            notified.as_mut().enable();
            self.order.registered();
            if self.generation() != seen {
                return;
            }
            notified.await;
        }
    }

    /// Check a predicate and wait without a gap between observation and readiness.
    pub async fn wait_for<T>(&self, mut predicate: impl FnMut() -> Option<T>) -> T {
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
pub struct CloseSignal {
    state: Arc<CloseState>,
}

#[derive(Debug, Default)]
struct CloseState {
    closed: AtomicBool,
    wake: Arc<Notify>,
    order: NotifyOrder,
}

impl CloseSignal {
    /// Close once. Every current and future waiter observes it.
    pub fn close(&self) {
        if !self.state.closed.swap(true, Ordering::AcqRel) {
            self.state.order.notifying();
            self.state.wake.notify_waiters();
        }
    }

    /// Whether this signal is closed.
    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::Acquire)
    }

    /// Own the wait without allocating another task, box, or shared state.
    pub fn closed(&self) -> Closed {
        Closed {
            state: self.state.clone(),
            notified: self.state.wake.clone().notified_owned(),
        }
    }
}

pin_project! {
    /// Owned close wait. Closure remains observable after cancellation or repoll.
    #[derive(Debug)]
    pub struct Closed {
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
        this.state.order.registered();
        if this.state.closed.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        this.notified.poll(cx)
    }
}

/// Yield to the executor exactly once. Uses no clock, so it also works under
/// a manually polled simulator.
pub async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

#[cfg(all(test, not(ozzy_loom)))]
#[path = "signal/tests.rs"]
mod tests;
