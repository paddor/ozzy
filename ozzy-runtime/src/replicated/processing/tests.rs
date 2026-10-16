use super::{progress::RESERVATION_BYTES, window::Window, *};
use futures::FutureExt;
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::sync::Semaphore;

fn identity() -> ProcessingIdentity {
    ProcessingIdentity {
        group: ConsumerGroupId::from_bytes([1; 16]),
        partition: PartitionIncarnation::from_bytes([2; 16]),
        member: ConsumerMemberId::from_bytes([3; 16]),
        assignment_epoch: 1,
        policy: Policy::QuorumDurable,
    }
}

fn window(records: usize) -> (Window, progress::Publisher) {
    Window::new(
        identity(),
        Offset::ZERO,
        records,
        &Arc::new(Semaphore::new(4096)),
    )
    .unwrap()
}

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

#[test]
fn processing_intent_racing_confirmation_or_close_wakes_a_registered_owner() {
    for close in [false, true] {
        for _ in 0..128 {
            let (mut publisher, mut submission) = progress::Publisher::new(identity(), 0, None);
            submission.publish(1).unwrap();
            let flag = Arc::new(WakeFlag::default());
            let waker = Waker::from(flag.clone());
            let gate = Arc::new(Barrier::new(2));
            let caller = {
                let gate = gate.clone();
                std::thread::spawn(move || {
                    gate.wait();
                    submission.publish(2).unwrap();
                    if close {
                        submission.close(ProgressError::Fenced);
                    }
                    submission
                })
            };
            publisher.confirm(1, 1).unwrap();
            gate.wait();
            let _submission = {
                let mut ready = pin!(publisher.ready());
                let early = ready.as_mut().poll(&mut Context::from_waker(&waker));
                let submission = caller.join().unwrap();
                if early.is_pending() {
                    assert!(
                        flag.0.load(Ordering::Acquire),
                        "registered owner lost its wake"
                    );
                    assert_eq!(
                        ready.as_mut().poll(&mut Context::from_waker(&waker)),
                        Poll::Ready(if close {
                            Err(ProgressError::Fenced)
                        } else {
                            Ok(2)
                        })
                    );
                } else {
                    assert!(matches!(
                        early,
                        Poll::Ready(Ok(2) | Err(ProgressError::Fenced))
                    ));
                }
                submission
            };
            assert_eq!(
                publisher.ready().now_or_never(),
                Some(if close {
                    Err(ProgressError::Fenced)
                } else {
                    Ok(2)
                })
            );
        }
    }
}

#[tokio::test]
async fn submitted_processing_closes_the_whole_ready_hole_without_another_caller_poll() {
    let (mut window, mut publisher) = window(3);
    let first = window.deliver(Offset::ZERO).unwrap();
    let second = window.deliver(Offset::new(1)).unwrap();
    let third = window.deliver(Offset::new(2)).unwrap();
    let second = window.submit(second).unwrap();
    let third = window.submit(third).unwrap();
    assert_eq!(publisher.submitted(), 0);
    let mut ready = Box::pin(publisher.ready());
    assert!(futures::poll!(ready.as_mut()).is_pending());
    drop(ready);
    let first = window.submit(first).unwrap();
    assert_eq!(publisher.ready().await.unwrap(), 3);
    assert_eq!(window.processed(), Offset::new(3));
    assert!(
        first.try_confirmed().is_none(),
        "intent is not confirmation"
    );
    publisher.confirm(3, 3).unwrap();
    for pending in [first, second, third] {
        assert!(pending.confirmed().await.is_ok());
    }
    window.close(ProgressError::Fenced);
    assert_eq!(publisher.ready().await, Err(ProgressError::Fenced));
    publisher.fail(ProgressError::Fenced);
}

#[tokio::test]
async fn processing_intent_is_unique_monotonic_and_closure_preserves_its_first_reason() {
    let (mut publisher, mut submission) = progress::Publisher::new(identity(), 0, None);
    assert_eq!(submission.publish(u64::MAX), Ok(()));
    assert_eq!(
        submission.publish(u64::MAX - 1),
        Err(ProgressError::Response)
    );
    assert_eq!(publisher.ready().await, Ok(u64::MAX));
    submission.close(ProgressError::Fenced);
    submission.close(ProgressError::Closed);
    assert_eq!(submission.publish(u64::MAX), Err(ProgressError::Fenced));
    drop(submission);
    assert_eq!(publisher.ready().await, Err(ProgressError::Fenced));
    let observer = publisher.observer();
    assert_eq!(observer.closing().await, ProgressError::Fenced);
    assert_eq!(
        observer.observe(1),
        None,
        "only the SDK owner seals failure"
    );
    publisher.fail(ProgressError::Fenced);
    assert_eq!(observer.observe(1), Some(Err(ProgressError::Fenced)));
}

#[tokio::test]
async fn processing_holes_block_cumulative_confirmation_and_release_in_bounded_turns() {
    let (mut window, mut publisher) = window(3);
    let first = window.deliver(Offset::ZERO).unwrap();
    let second = window.deliver(Offset::new(1)).unwrap();
    let third = window.deliver(Offset::new(2)).unwrap();
    let second = window.acknowledge(second).unwrap();
    let third = window.acknowledge(third).unwrap();
    assert!(!window.advance(3));
    assert_eq!(window.processed(), Offset::ZERO);
    assert_eq!(
        publisher.confirm(3, window.processed().get()),
        Err(ProgressError::Response)
    );
    assert!(second.try_confirmed().is_none());
    assert!(third.try_confirmed().is_none());
    assert!(!window.has_capacity());
    let first = window.acknowledge(first).unwrap();
    assert!(window.advance(1));
    assert_eq!(window.processed(), Offset::new(1));
    publisher.confirm(1, window.processed().get()).unwrap();
    assert_eq!(first.confirmed().await.unwrap().cursor, Offset::new(1));
    assert!(second.try_confirmed().is_none());
    assert!(!window.has_capacity());
    assert!(!window.retire(1));
    assert!(window.has_capacity());
    assert!(window.advance(1));
    assert!(!window.advance(1));
    assert_eq!(window.processed(), Offset::new(3));
    publisher.confirm(3, window.processed().get()).unwrap();
    assert!(window.retire(1));
    assert!(!window.retire(1));
    for (pending, cursor) in [(first, 1), (second, 2), (third, 3)] {
        let receipt = pending.confirmed().await.unwrap();
        assert_eq!(receipt.cursor, Offset::new(cursor));
        assert_eq!(receipt.identity, identity());
    }
}

#[test]
fn dropped_token_leaves_an_unprocessed_hole_and_dropped_wait_does_not_undo_submission() {
    let (mut window, mut publisher) = window(3);
    drop(window.deliver(Offset::ZERO).unwrap());
    let second = window.deliver(Offset::new(1)).unwrap();
    drop(window.acknowledge(second).unwrap());
    let third = window.deliver(Offset::new(2)).unwrap();
    let third = window.acknowledge(third).unwrap();
    assert!(!window.advance(3));
    assert_eq!(window.processed(), Offset::ZERO);
    assert_eq!(publisher.confirm(3, 0), Err(ProgressError::Response));
    publisher.fail(ProgressError::Fenced);
    assert_eq!(third.try_confirmed(), Some(Err(ProgressError::Fenced)));
    assert!(!window.has_capacity());
}

#[tokio::test]
async fn canceled_progress_wait_can_be_restarted_after_confirmation() {
    let (mut window, mut publisher) = window(1);
    let token = window.deliver(Offset::ZERO).unwrap();
    assert_eq!(token.identity(), identity());
    assert_eq!(token.offset(), Offset::ZERO);
    let pending = window.acknowledge(token).unwrap();
    let mut wait = Box::pin(pending.confirmed());
    assert!(futures::poll!(&mut wait).is_pending());
    drop(wait);
    assert!(!window.advance(1));
    publisher.confirm(1, window.processed().get()).unwrap();
    assert_eq!(pending.confirmed().await.unwrap().cursor, Offset::new(1));
    drop(window);
    drop(publisher);
    assert_eq!(pending.confirmed().await.unwrap().cursor, Offset::new(1));
}

#[test]
fn foreign_and_recreated_windows_reject_tokens_even_with_the_same_assignment_identity() {
    let (mut first, _first_owner) = window(2);
    let (mut second, _second_owner) = window(2);
    let token = first.deliver(Offset::ZERO).unwrap();
    assert!(matches!(
        second.acknowledge(token),
        Err(ProgressError::Token)
    ));
    let token = first.deliver(Offset::new(1)).unwrap();
    drop(first);
    let (mut replacement, _replacement_owner) = window(2);
    assert!(matches!(
        replacement.acknowledge(token),
        Err(ProgressError::Token)
    ));
    assert_eq!(second.processed(), Offset::ZERO);
    assert_eq!(replacement.processed(), Offset::ZERO);
}

#[test]
fn processing_metadata_remains_charged_through_final_observers() {
    let budget = Arc::new(Semaphore::new(4096));
    let (mut window, mut publisher) = Window::new(identity(), Offset::ZERO, 1, &budget).unwrap();
    let token = window.deliver(Offset::ZERO).unwrap();
    let pending = window.acknowledge(token).unwrap();
    window.advance(1);
    publisher.confirm(1, 1).unwrap();
    drop(window);
    drop(publisher);
    assert_eq!(budget.available_permits(), 4096 - RESERVATION_BYTES);
    assert!(pending.try_confirmed().unwrap().is_ok());
    drop(pending);
    assert_eq!(budget.available_permits(), 4096);
    let (mut window, publisher) = Window::new(identity(), Offset::ZERO, 1, &budget).unwrap();
    let token = window.deliver(Offset::ZERO).unwrap();
    drop(window);
    drop(publisher);
    assert_eq!(budget.available_permits(), 4096 - RESERVATION_BYTES);
    drop(token);
    assert_eq!(budget.available_permits(), 4096);
}

#[test]
fn failed_processing_admission_returns_reservations_and_preserves_positions() {
    let budget = Arc::new(Semaphore::new(1));
    assert!(matches!(
        Window::new(identity(), Offset::ZERO, 1, &budget),
        Err(ProgressError::Capacity)
    ));
    assert_eq!(budget.available_permits(), 1);
    let queue_bytes = size_of::<Window>() + 1;
    let partial = Arc::new(Semaphore::new(queue_bytes));
    assert!(matches!(
        Window::new(identity(), Offset::ZERO, 1, &partial),
        Err(ProgressError::Capacity)
    ));
    assert_eq!(partial.available_permits(), queue_bytes);
    for records in [0, usize::MAX] {
        assert!(matches!(
            Window::new(identity(), Offset::ZERO, records, &budget),
            Err(ProgressError::Configuration)
        ));
        assert_eq!(budget.available_permits(), 1);
    }
    let (mut window, _publisher) = window(1);
    assert!(matches!(
        window.deliver(Offset::new(1)),
        Err(ProgressError::Response)
    ));
    let token = window.deliver(Offset::ZERO).unwrap();
    assert!(matches!(
        window.deliver(Offset::new(1)),
        Err(ProgressError::Capacity)
    ));
    assert!(matches!(
        window.deliver(Offset::ZERO),
        Err(ProgressError::Response)
    ));
    drop(token);
    assert_eq!(window.processed(), Offset::ZERO);
}

#[test]
fn maximal_exclusive_offset_and_reordered_confirmations_do_not_wrap_or_regress() {
    let (mut window, mut publisher) = Window::new(
        identity(),
        Offset::new(u64::MAX - 1),
        1,
        &Arc::new(Semaphore::new(4096)),
    )
    .unwrap();
    let token = window.deliver(Offset::new(u64::MAX - 1)).unwrap();
    let pending = window.acknowledge(token).unwrap();
    assert!(!window.advance(1));
    assert_eq!(window.processed(), Offset::new(u64::MAX));
    publisher.confirm(u64::MAX, u64::MAX).unwrap();
    publisher.confirm(u64::MAX - 1, u64::MAX).unwrap();
    assert_eq!(
        pending.try_confirmed().unwrap().unwrap().cursor,
        Offset::new(u64::MAX)
    );
    assert!(!window.retire(1));
    assert!(matches!(
        window.deliver(Offset::new(u64::MAX)),
        Err(ProgressError::Response)
    ));
    publisher.fail(ProgressError::Fenced);
    assert_eq!(
        publisher.confirm(u64::MAX, u64::MAX),
        Err(ProgressError::Fenced)
    );
    drop(publisher);
    assert!(pending.try_confirmed().unwrap().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processing_observers_keep_confirmed_prefix_after_owner_failure() {
    for _ in 0..32 {
        let (mut window, mut publisher) = window(2);
        let first = window.deliver(Offset::ZERO).unwrap();
        let second = window.deliver(Offset::new(1)).unwrap();
        let first = Arc::new(window.acknowledge(first).unwrap());
        let second = window.acknowledge(second).unwrap();
        assert!(!window.advance(2));
        let observers = (0..2)
            .map(|_| {
                let first = first.clone();
                tokio::spawn(async move { first.confirmed().await })
            })
            .collect::<Vec<_>>();
        let failed = tokio::spawn(async move { second.confirmed().await });
        tokio::task::yield_now().await;
        publisher.confirm(1, window.processed().get()).unwrap();
        tokio::task::yield_now().await;
        publisher.fail(ProgressError::Fenced);
        drop(publisher);
        drop(window);
        for observer in observers {
            assert_eq!(observer.await.unwrap().unwrap().cursor, Offset::new(1));
        }
        assert_eq!(failed.await.unwrap(), Err(ProgressError::Fenced));
        assert!(first.try_confirmed().unwrap().is_ok());
    }
}
