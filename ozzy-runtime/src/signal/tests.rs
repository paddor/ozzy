use std::cell::Cell;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use super::{CloseSignal, DataSignal, StateSignal};

#[derive(Debug, Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn marks_before_wait_and_during_drain_remain_ready() {
    let signal = DataSignal::default();
    signal.mark();
    signal.mark();
    assert!(poll(pin!(signal.ready())).is_ready());
    assert!(
        poll(pin!(signal.ready())).is_ready(),
        "wait never consumes readiness"
    );
    assert_eq!(
        signal.drain(|| {
            signal.mark();
            42
        }),
        42
    );
    assert!(poll(pin!(signal.ready())).is_ready());
    signal.drain(|| ());
    assert!(
        poll(pin!(signal.ready())).is_pending(),
        "stale wake permits are not readiness"
    );
}

#[test]
fn canceled_data_wait_cannot_steal_readiness_or_a_later_wake() {
    let signal = DataSignal::default();
    {
        let mut canceled = pin!(signal.ready());
        assert!(poll(canceled.as_mut()).is_pending());
        signal.mark();
    }
    assert!(poll(pin!(signal.ready())).is_ready());
    signal.drain(|| ());
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let mut waiting = pin!(signal.ready());
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    signal.mark();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert!(poll(waiting.as_mut()).is_ready());
}

#[test]
fn bounded_drain_explicitly_reschedules_remaining_work() {
    let signal = DataSignal::default();
    let remaining = Cell::new(3);
    signal.mark();
    for _ in 0..3 {
        assert!(poll(pin!(signal.ready())).is_ready());
        signal.drain(|| {
            remaining.set(remaining.get() - 1);
            if remaining.get() != 0 {
                signal.mark();
            }
        });
    }
    assert!(poll(pin!(signal.ready())).is_pending());
}

#[test]
fn data_mark_from_another_thread_during_drain_is_not_cleared() {
    let signal = DataSignal::default();
    signal.mark();
    signal.drain(|| {
        std::thread::scope(|threads| {
            threads.spawn(|| signal.mark()).join().unwrap();
        });
    });
    assert!(poll(pin!(signal.ready())).is_ready());
}

#[test]
fn state_change_before_registration_and_inside_predicate_is_observed() {
    let signal = StateSignal::default();
    let seen = signal.generation();
    signal.notify_changed();
    assert!(poll(pin!(signal.changed_after(seen))).is_ready());
    assert!(poll(pin!(signal.changed_after(signal.generation()))).is_pending());
    let calls = Cell::new(0);
    let mut waiting = pin!(signal.wait_for(|| {
        calls.set(calls.get() + 1);
        if calls.get() == 1 {
            // Change lands after wait_for captures its generation, before its
            // predicate returns None. No registered waiter exists yet.
            signal.notify_changed();
            None
        } else {
            Some(17)
        }
    }));
    assert_eq!(poll(waiting.as_mut()), Poll::Ready(17));
    assert_eq!(calls.get(), 2);
}

#[test]
fn state_change_broadcasts_and_survives_canceled_waiters() {
    let signal = StateSignal::default();
    let seen = signal.generation();
    let first_counter = Arc::new(WakeCount::default());
    let second_counter = Arc::new(WakeCount::default());
    let first_waker = Waker::from(first_counter.clone());
    let second_waker = Waker::from(second_counter.clone());
    let mut first = pin!(signal.changed_after(seen));
    let mut second = pin!(signal.changed_after(seen));
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    {
        let mut canceled = pin!(signal.changed_after(seen));
        assert!(poll(canceled.as_mut()).is_pending());
    }
    signal.notify_changed();
    assert_eq!(first_counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_counter.0.load(Ordering::Relaxed), 1);
    assert!(poll(first.as_mut()).is_ready());
    assert!(poll(second.as_mut()).is_ready());
    assert!(poll(pin!(signal.changed_after(seen))).is_ready());
}

#[test]
fn canceled_predicate_wait_does_not_consume_state_changes() {
    let signal = StateSignal::default();
    let value = Cell::new(None);
    {
        let mut canceled = pin!(signal.wait_for(|| value.get()));
        assert!(poll(canceled.as_mut()).is_pending());
        value.set(Some(23));
        signal.notify_changed();
    }
    assert_eq!(poll(pin!(signal.wait_for(|| value.get()))), Poll::Ready(23));
}

#[test]
fn concurrent_state_change_registration_never_loses_a_wake() {
    for _ in 0..32 {
        let signal = StateSignal::default();
        let seen = signal.generation();
        std::thread::scope(|threads| {
            let changer = threads.spawn(|| signal.notify_changed());
            let counter = Arc::new(WakeCount::default());
            let waker = Waker::from(counter.clone());
            let mut waiting = pin!(signal.changed_after(seen));
            let was_pending = waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending();
            changer.join().unwrap();
            if was_pending {
                assert!(
                    counter.0.load(Ordering::Relaxed) > 0,
                    "pending state waiter needs a wake"
                );
                assert!(poll(waiting.as_mut()).is_ready());
            }
        });
    }
}

#[test]
fn exhausted_state_generation_fails_without_wrapping() {
    let signal = StateSignal::default();
    signal.generation.store(u64::MAX, Ordering::Release);
    assert!(std::panic::catch_unwind(|| signal.notify_changed()).is_err());
    assert_eq!(signal.generation(), u64::MAX);
}

#[test]
fn close_before_creation_or_first_poll_is_persistent_and_repeatable() {
    let signal = CloseSignal::default();
    let before = signal.closed();
    assert!(!signal.is_closed());
    signal.close();
    signal.close();
    assert!(signal.is_closed());
    let mut before = pin!(before);
    assert!(poll(before.as_mut()).is_ready());
    assert!(poll(before.as_mut()).is_ready());
    assert!(poll(pin!(signal.closed())).is_ready());
    let owned = signal.closed();
    drop(signal);
    assert!(poll(pin!(owned)).is_ready());
}

#[test]
fn close_wakes_every_registered_waiter_and_cancellation_cannot_steal_it() {
    let signal = CloseSignal::default();
    let first_counter = Arc::new(WakeCount::default());
    let second_counter = Arc::new(WakeCount::default());
    let first_waker = Waker::from(first_counter.clone());
    let second_waker = Waker::from(second_counter.clone());
    let mut first = pin!(signal.closed());
    let mut second = pin!(signal.closed());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        second
            .as_mut()
            .poll(&mut Context::from_waker(&second_waker))
            .is_pending()
    );
    {
        let mut canceled = pin!(signal.closed());
        assert!(poll(canceled.as_mut()).is_pending());
    }
    signal.clone().close();
    assert_eq!(first_counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_counter.0.load(Ordering::Relaxed), 1);
    assert!(poll(first.as_mut()).is_ready());
    assert!(poll(second.as_mut()).is_ready());
    assert!(poll(pin!(signal.closed())).is_ready());
}

#[test]
fn concurrent_close_registration_never_leaves_a_pending_waiter() {
    for _ in 0..32 {
        let signal = CloseSignal::default();
        std::thread::scope(|threads| {
            let clone = signal.clone();
            let closer = threads.spawn(move || clone.close());
            let counter = Arc::new(WakeCount::default());
            let waker = Waker::from(counter.clone());
            let mut waiting = pin!(signal.closed());
            let was_pending = waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending();
            closer.join().unwrap();
            if was_pending {
                assert!(
                    counter.0.load(Ordering::Relaxed) > 0,
                    "pending close waiter needs a wake"
                );
            }
            assert!(poll(waiting.as_mut()).is_ready());
        });
    }
}
