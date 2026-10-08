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
#[path = "../src/replicated/writer/progress.rs"]
mod progress;
#[path = "../src/signal.rs"]
mod signal;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;
use ozzy_proto::ProducerId;
use ozzy_runtime::replicated::{
    AppendKey, Error, Policy, RecordReceipt, WriterConfig, WriterError,
};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll};

#[test]
fn confirmed_prefix_publishes_exact_offsets_before_observation() {
    for offsets in [[0, u64::MAX], [7, 50]] {
        loom::model(move || {
            let progress = Arc::new(progress::Progress::new(&config()));
            let mut pool = completion::Pool::default();
            let records = Arc::new(offsets.map(|offset| {
                pool.allocate(ozzy_proto::MessageId::from_bytes(
                    u128::from(offset).to_be_bytes(),
                ))
            }));
            drop(pool);
            let publisher = {
                let progress = progress.clone();
                let records = records.clone();
                thread::spawn(move || {
                    for (index, offset) in offsets.into_iter().enumerate() {
                        progress
                            .confirm(index as u64, index as u64 + 1, offset, 2, |count| {
                                assert_eq!(count, 1);
                                records[index].publish(offset);
                            })
                            .unwrap();
                        thread::yield_now();
                    }
                })
            };
            let confirmed = progress.confirmed();
            for index in 0..confirmed as usize {
                assert_eq!(records[index].offset(), offsets[index]);
            }
            publisher.join().unwrap();
            assert_eq!(progress.confirmed(), 2);
            for (record, offset) in records.iter().zip(offsets) {
                assert_eq!(record.offset(), offset);
            }
        });
    }
}

fn config() -> WriterConfig {
    WriterConfig {
        policy: Policy::QuorumReplicatedPersisting,
        partition: ozzy_proto::PartitionIncarnation::from_bytes([1; 16]),
        owner_epoch: 1,
        producer_id: ProducerId::from_bytes([2; 16]),
        producer_epoch: 1,
        next_sequence: 0,
        limits: ozzy_runtime::replicated::DataLimits {
            envelope: ozzy_proto::EnvelopeLimits {
                max_metadata_bytes: 512,
                max_payload_bytes: 128,
            },
            max_records: 2,
            max_parts: 1,
            max_record_bytes: 64,
        },
        compress_payloads: false,
        batch_target_bytes: 128,
        max_producers: 1,
        inflight_appends: 2,
    }
}

#[test]
fn terminal_failure_racing_confirmation_seals_the_real_progress_prefix() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.check(|| {
        let progress = Arc::new(progress::Progress::new(&config()));
        let mut pool = completion::Pool::default();
        let records = Arc::new([7_u64, 50].map(|offset| {
            pool.allocate(ozzy_proto::MessageId::from_bytes(
                u128::from(offset).to_be_bytes(),
            ))
        }));
        let publisher = {
            let progress = progress.clone();
            let records = records.clone();
            thread::spawn(move || {
                for (index, offset) in [7, 50].into_iter().enumerate() {
                    let result =
                        progress.confirm(index as u64, index as u64 + 1, offset, 2, |count| {
                            assert_eq!(count, 1);
                            records[index].publish(offset);
                        });
                    if let Err(error) = result {
                        assert!(matches!(error, WriterError::Closed));
                        break;
                    }
                    thread::yield_now();
                }
            })
        };
        let failure = {
            let progress = progress.clone();
            thread::spawn(move || progress.fail(WriterError::Closed))
        };
        let observed = [progress.observe(1), progress.observe(2)];
        for (index, result) in observed.iter().enumerate() {
            if matches!(result, Some(Ok(()))) {
                assert_eq!(records[index].offset(), [7, 50][index]);
            }
        }
        publisher.join().unwrap();
        failure.join().unwrap();
        for (index, result) in observed.into_iter().enumerate() {
            let final_result = progress.observe(index as u64 + 1);
            match result {
                Some(Ok(())) => assert!(matches!(final_result, Some(Ok(())))),
                Some(Err(WriterError::Closed)) => {
                    assert!(matches!(final_result, Some(Err(WriterError::Closed))));
                }
                None => assert!(final_result.is_some()),
                other => panic!("unexpected observation: {other:?}"),
            }
        }
        assert!(matches!(
            progress.observe(3),
            Some(Err(WriterError::Closed))
        ));
        let confirmed = progress.confirmed();
        assert!(matches!(
            progress.confirm(confirmed, confirmed + 1, 100, 3, |_| {
                panic!("terminal failure published another receipt")
            }),
            Err(WriterError::Closed)
        ));
        assert_eq!(progress.confirmed(), confirmed);
    });
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
