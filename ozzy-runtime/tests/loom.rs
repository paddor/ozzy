//! Model the production SDK counters and persistent readiness, not OMQ internals.
#![cfg(ozzy_loom)]
#![expect(
    dead_code,
    reason = "production modules expose more than these models use"
)]
#![allow(
    unreachable_pub,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "test code"
)]

#[path = "../src/replicated/broker_links/append/budget.rs"]
mod budget;
#[path = "../src/replicated/writer/completion.rs"]
mod completion;
#[path = "../src/replicated/writer/inbox.rs"]
mod inbox;
#[path = "../src/replicated/broker_links/routing/interest.rs"]
mod interest;
#[path = "../src/replicated/processing/progress.rs"]
mod processing_progress;
#[path = "../src/replicated/writer/progress.rs"]
mod progress;
#[path = "../../ozzy-io/src/signal.rs"]
mod signal;
#[path = "../src/replica_actor/partitions/woken.rs"]
mod woken;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};
use loom::thread;
use ozzy_proto::{ProducerId, append::AppendDisposition};
use ozzy_runtime::replicated::{
    AppendKey, AppendLinkLimits, Error, Policy, ProcessingIdentity, ProgressError, RecordReceipt,
    WriterConfig, WriterError,
};
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll};

#[path = "loom/processing.rs"]
mod processing;

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
                            .confirm(index as u64, index as u64 + 1, 2, |count| {
                                assert_eq!(count, 1);
                                records[index].publish(offset, AppendDisposition::Duplicate);
                            })
                            .unwrap();
                        thread::yield_now();
                    }
                })
            };
            let confirmed = progress.confirmed();
            for index in 0..confirmed as usize {
                assert_eq!(records[index].offset(), offsets[index]);
                assert_eq!(records[index].disposition(), AppendDisposition::Duplicate);
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
        policy: Policy::QuorumDurable,
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
                    let result = progress.confirm(index as u64, index as u64 + 1, 2, |count| {
                        assert_eq!(count, 1);
                        records[index].publish(offset, AppendDisposition::Duplicate);
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
            progress.confirm(confirmed, confirmed + 1, 3, |_| {
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
fn an_interest_raised_before_a_scan_is_never_lost() {
    loom::model(|| {
        let changed = std::sync::Arc::new(signal::StateSignal::default());
        let interest = Arc::new(interest::Interest::default());
        let caller = {
            let changed = changed.clone();
            let interest = interest.clone();
            thread::spawn(move || {
                interest.register(&changed);
                interest.refresh(&changed);
            })
        };
        // The watcher's scan: capture the routing signal, then read.
        let seen = changed.generation();
        let revision = interest.revision();
        caller.join().unwrap();
        assert!(
            revision == interest.revision() || changed.generation() != seen,
            "revision {revision} was read after its signal was captured"
        );
    });
}

#[test]
fn a_wake_racing_a_partition_visit_is_seen_or_left_marked() {
    loom::model(|| {
        let shard = std::sync::Arc::new(loom::future::AtomicWaker::new());
        // A new partition starts marked: an earlier wake is still pending
        // when the producer marks it again during the visit.
        let mark = std::sync::Arc::new(woken::Woken::new(shard));
        let work = Arc::new(AtomicBool::new(false));
        let producer = {
            let mark = mark.clone();
            let work = work.clone();
            thread::spawn(move || {
                work.store(true, Ordering::Release);
                std::task::Wake::wake_by_ref(&mark);
            })
        };
        let seen = mark.is_set() && mark.take() && work.load(Ordering::Acquire);
        producer.join().unwrap();
        assert!(seen || mark.is_set(), "the wake was neither seen nor kept");
    });
}

#[test]
fn a_writer_lease_dropped_during_admission_frees_its_charge_or_signals() {
    let writer = |bytes, progress_bytes| budget::Cost {
        writers: 1,
        bytes,
        progress_bytes,
        ..budget::Cost::default()
    };
    let request = budget::Cost {
        requests: 1,
        records: 1,
        bytes: 60,
        ..budget::Cost::default()
    };
    loom::model(move || {
        let budget = std::sync::Arc::new(budget::Budget::new(Some(AppendLinkLimits {
            writers: 2,
            requests: 1,
            records: 1,
            bytes: 100,
        })));
        let first = budget.acquire(writer(50, 20)).unwrap();
        let closed = thread::spawn(move || drop(first));
        // A blocked APPEND: capture the signal, then check capacity.
        let seen = budget.changed.generation();
        let available = budget.available(request);
        // Another writer opens while the first one's headroom is released.
        let second = budget.acquire(writer(10, 30)).unwrap();
        closed.join().unwrap();
        assert!(
            available || budget.changed.generation() != seen,
            "the freed charge was neither seen nor signaled"
        );
        assert!(budget.acquire(request).is_some());
        // Only the second writer's bytes and headroom stay reserved.
        assert!(budget.available(writer(60, 0)));
        assert!(!budget.available(writer(61, 0)));
        drop(second);
        assert!(budget.available(writer(100, 0)), "every charge returned");
    });
}
