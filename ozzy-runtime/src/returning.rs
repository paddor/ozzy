//! Values that go back to their owner when their holder drops them.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use futures::task::AtomicWaker;
use tokio::sync::mpsc;

use crate::signal::StateSignal;

/// Where a held value goes when its holder drops it.
pub(crate) trait GiveBack<T> {
    fn give_back(&mut self, value: T);
}

/// Holds a value for its whole life, then gives it back on drop. Only `Drop`
/// moves the value out, so every other access finds it present.
pub(crate) struct Returning<T, B: GiveBack<T>> {
    value: Option<T>,
    back: B,
}

impl<T, B: GiveBack<T>> Returning<T, B> {
    pub(crate) fn new(value: T, back: B) -> Self {
        Self {
            value: Some(value),
            back,
        }
    }
}

impl<T, B: GiveBack<T>> Deref for Returning<T, B> {
    type Target = T;

    #[expect(clippy::expect_used, reason = "only drop moves the value out")]
    fn deref(&self) -> &T {
        self.value.as_ref().expect("value held until drop")
    }
}

impl<T, B: GiveBack<T>> DerefMut for Returning<T, B> {
    #[expect(clippy::expect_used, reason = "only drop moves the value out")]
    fn deref_mut(&mut self) -> &mut T {
        self.value.as_mut().expect("value held until drop")
    }
}

impl<T, B: GiveBack<T>> Drop for Returning<T, B> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            self.back.give_back(value);
        }
    }
}

impl<T: fmt::Debug, B: GiveBack<T>> fmt::Debug for Returning<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Returning").field(&self.value).finish()
    }
}

/// How a value given back tells its owner.
pub(crate) trait Notice {
    fn notice(&self);
}

/// The owner registered before checking its lane.
impl Notice for AtomicWaker {
    fn notice(&self) {
        self.wake();
    }
}

/// The owner and other waiters recheck their capacity predicates.
impl Notice for StateSignal {
    fn notice(&self) {
        self.notify_changed();
    }
}

/// Values on their way back to one owner, and the notice that tells it. A
/// holder reserves its place when it takes a value, so giving the value back
/// can neither fail nor block, from any thread.
#[derive(Debug)]
pub(crate) struct ReturnLane<T, N> {
    sender: mpsc::Sender<T>,
    returned: mpsc::Receiver<T>,
    notice: Arc<N>,
}

/// One reserved place on a [`ReturnLane`]. A value without a slot has no
/// owner to return to and is simply dropped.
#[derive(Debug)]
pub(crate) struct ReturnSlot<T, N> {
    permit: mpsc::OwnedPermit<T>,
    notice: Arc<N>,
}

impl<T, N: Notice> ReturnLane<T, N> {
    pub(crate) fn new(slots: usize, notice: Arc<N>) -> Self {
        let (sender, returned) = mpsc::channel(slots);
        Self {
            sender,
            returned,
            notice,
        }
    }

    pub(crate) fn notice(&self) -> &Arc<N> {
        &self.notice
    }

    /// Reserve the place of a value about to be handed out.
    pub(crate) fn reserve(&self) -> Option<ReturnSlot<T, N>> {
        let permit = self.sender.clone().try_reserve_owned().ok()?;
        Some(ReturnSlot {
            permit,
            notice: self.notice.clone(),
        })
    }

    pub(crate) fn try_take(&mut self) -> Option<T> {
        self.returned.try_recv().ok()
    }

    /// Take at most `max` returned values, and notice again if more remain.
    pub(crate) fn drain(&mut self, max: usize, mut take: impl FnMut(T)) {
        for _ in 0..max {
            let Some(value) = self.try_take() else {
                return;
            };
            take(value);
        }
        if !self.returned.is_empty() {
            self.notice.notice();
        }
    }
}

impl<T, N: Notice> ReturnSlot<T, N> {
    /// Return `value`, telling the owner only when `notice` asks for it.
    pub(crate) fn send(self, value: T, notice: bool) {
        self.permit.send(value);
        if notice {
            self.notice.notice();
        }
    }
}

impl<T, N: Notice> GiveBack<T> for Option<ReturnSlot<T, N>> {
    fn give_back(&mut self, value: T) {
        if let Some(slot) = self.take() {
            slot.send(value, true);
        }
    }
}

/// One value lent out at a time, back through a one-place lane.
#[derive(Debug)]
pub(crate) struct Lent<T, N> {
    home: Option<T>,
    lane: ReturnLane<T, N>,
}

impl<T, N: Notice> Lent<T, N> {
    pub(crate) fn new(value: T, notice: Arc<N>) -> Self {
        Self {
            home: Some(value),
            lane: ReturnLane::new(1, notice),
        }
    }

    /// Lend the value when no holder has it, with its return place reserved.
    pub(crate) fn try_lend(&mut self) -> Option<(T, ReturnSlot<T, N>)> {
        let value = self.home.take().or_else(|| self.lane.try_take())?;
        let Some(slot) = self.lane.reserve() else {
            self.home = Some(value);
            return None;
        };
        Some((value, slot))
    }
}
