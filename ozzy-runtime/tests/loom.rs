//! Model the production SDK counters and persistent readiness, not OMQ internals.
#![cfg(ozzy_loom)]
#![expect(
    dead_code,
    reason = "production modules expose more than these models use"
)]

#[path = "../src/replicated/writer/completion.rs"]
mod completion;
#[path = "../src/replicated/writer/inbox.rs"]
mod inbox;
#[path = "../src/signal.rs"]
mod signal;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use loom::thread;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll};

#[test]
fn confirmed_prefix_publishes_exact_offsets_before_observation() {
    for offsets in [[0, u64::MAX], [7, 50]] {
        loom::model(move || {
            let prefix = Arc::new(AtomicU64::new(0));
            let mut pool = completion::Pool::default();
            let records = Arc::new(offsets.map(|offset| {
                pool.allocate(ozzy_proto::MessageId::from_bytes(
                    u128::from(offset).to_be_bytes(),
                ))
            }));
            drop(pool);
            let publisher = {
                let prefix = prefix.clone();
                let records = records.clone();
                thread::spawn(move || {
                    for (index, offset) in offsets.into_iter().enumerate() {
                        records[index].publish(offset);
                        prefix.store(index as u64 + 1, Ordering::Release);
                        thread::yield_now();
                    }
                })
            };
            let confirmed = prefix.load(Ordering::Acquire);
            for index in 0..confirmed as usize {
                assert_eq!(records[index].offset(), offsets[index]);
            }
            publisher.join().unwrap();
            assert_eq!(prefix.load(Ordering::Acquire), 2);
            for (record, offset) in records.iter().zip(offsets) {
                assert_eq!(record.offset(), offset);
            }
        });
    }
}

#[test]
fn concurrent_admission_cancellation_and_release_restore_capacity() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.check(|| {
        let changed = std::sync::Arc::new(signal::StateSignal::default());
        let inbox = Arc::new(inbox::Inbox::new(2, 5, changed));
        let (sent, received) = loom::sync::mpsc::channel();
        let publisher = {
            let inbox = inbox.clone();
            thread::spawn(move || {
                let admitted = inbox.reserve(3).map(inbox::Reservation::publish);
                sent.send(admitted.is_some()).unwrap();
            })
        };
        let canceled = {
            let inbox = inbox.clone();
            thread::spawn(move || {
                // The oversized record may occupy an empty inbox. Canceling
                // before publication must return both record and byte capacity.
                let reservation = inbox.reserve(7);
                thread::yield_now();
                drop(reservation);
            })
        };
        if received.recv().unwrap() {
            inbox.release(1, 3);
        }
        publisher.join().unwrap();
        canceled.join().unwrap();
        assert_eq!(inbox.used(), 0);
        assert_eq!(inbox.used_bytes(), 0);
        assert!(inbox.available(5));
        assert!(inbox.reserve(5).is_some());
        assert_eq!(inbox.used_bytes(), 0);
    });
}

#[test]
fn capacity_change_between_observation_and_wait_is_not_lost() {
    loom::model(|| {
        let changed = std::sync::Arc::new(signal::StateSignal::default());
        let inbox = Arc::new(inbox::Inbox::new(1, 5, changed.clone()));
        inbox.reserve(5).unwrap().publish();
        let released = {
            let inbox = inbox.clone();
            thread::spawn(move || inbox.release(1, 5))
        };
        let seen = changed.generation();
        let available = inbox.available(5);
        released.join().unwrap();
        // Tokio's Notify atomics are not Loom-instrumented. Model the production
        // generation protocol here; normal unit tests cover registration/wakes.
        assert!(
            available || changed.generation() != seen,
            "seen={seen} generation={} available={available} queued={} bytes={}",
            changed.generation(),
            inbox.used(),
            inbox.used_bytes()
        );
    });
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
        let mut ready = pin!(signal.ready());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(observed || matches!(ready.as_mut().poll(&mut cx), Poll::Ready(())));
    });
}
