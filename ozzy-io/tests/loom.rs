//! Model the persistent signal protocols, not Tokio's Notify internals.
#![cfg(ozzy_loom)]
#![allow(
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "test code"
)]

#[path = "../src/signal.rs"]
mod signal;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;
use std::future::Future;
use std::pin::{Pin, pin};
use std::task::{Context, Poll, Waker};

fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn publication_racing_a_drain_is_observed_or_leaves_readiness_set() {
    loom::model(|| {
        let work = Arc::new(AtomicBool::new(false));
        let signal = Arc::new(signal::DataSignal::default());
        signal.mark();
        let publisher = {
            let work = work.clone();
            let signal = signal.clone();
            thread::spawn(move || {
                work.store(true, Ordering::Release);
                signal.mark();
            })
        };
        let observed = signal.drain(|| work.load(Ordering::Acquire));
        publisher.join().unwrap();
        assert!(observed || poll(pin!(signal.ready())).is_ready());
    });
}

#[test]
fn state_change_racing_a_captured_generation_is_observed() {
    loom::model(|| {
        let state = Arc::new(AtomicBool::new(false));
        let signal = Arc::new(signal::StateSignal::default());
        let changer = {
            let state = state.clone();
            let signal = signal.clone();
            thread::spawn(move || {
                state.store(true, Ordering::Release);
                signal.notify_changed();
            })
        };
        let seen = signal.generation();
        let observed = state.load(Ordering::Acquire);
        changer.join().unwrap();
        assert!(observed || signal.generation() != seen);
    });
}

#[test]
fn close_racing_a_wait_is_observed_by_that_wait() {
    loom::model(|| {
        let signal = signal::CloseSignal::default();
        let closer = {
            let signal = signal.clone();
            thread::spawn(move || signal.close())
        };
        let mut closed = pin!(signal.closed());
        let early = poll(closed.as_mut());
        closer.join().unwrap();
        assert!(early.is_ready() || poll(closed).is_ready());
        assert!(signal.is_closed());
    });
}
