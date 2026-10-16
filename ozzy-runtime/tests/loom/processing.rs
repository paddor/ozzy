use super::*;

#[test]
fn confirmed_processing_observation_also_observes_its_delivered_prefix() {
    loom::model(|| {
        let (mut publisher, mut submission) =
            processing_progress::Publisher::new(identity(), 0, None);
        let observer = publisher.observer();
        let caller = thread::spawn(move || {
            submission.record_delivery(1);
            submission.publish(1).unwrap();
            submission
        });
        let captured = publisher.observe_submission();
        if captured == Some(Ok(1)) {
            publisher.confirm(1, 1).unwrap();
        }
        if observer.confirmed() == 1 {
            assert_eq!(observer.delivered(), 1);
        }
        let _submission = caller.join().unwrap();
        assert_eq!(observer.delivered(), 1);
        assert_eq!(
            publisher.observe_submission(),
            if captured == Some(Ok(1)) {
                None
            } else {
                Some(Ok(1))
            }
        );
    });
}

#[test]
fn processing_new_intent_racing_confirmation_remains_ready() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(|| {
        let (mut publisher, mut submission) =
            processing_progress::Publisher::new(identity(), 0, None);
        submission.publish(1).unwrap();
        let caller = thread::spawn(move || {
            submission.publish(2).unwrap();
            submission
        });
        publisher.confirm(1, 1).unwrap();
        // Model ready()'s actual state predicate, without uninstrumented
        // Tokio Notify. Production real-thread tests cover registration/wakes.
        let early = publisher.observe_submission();
        let _submission = caller.join().unwrap();
        assert!(matches!(early, None | Some(Ok(2))));
        assert_eq!(publisher.observe_submission(), Some(Ok(2)));
        // A completed earlier request must not clear a newer submission.
        assert_eq!(publisher.submitted(), 2);
        publisher.confirm(2, 2).unwrap();
        assert_eq!(publisher.observer().observe(2), Some(Ok(())));
    });
}

#[test]
fn processing_submission_close_racing_owner_read_seals_without_reopening() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(3);
    model.check(|| {
        let (mut publisher, mut submission) =
            processing_progress::Publisher::new(identity(), 0, None);
        let caller = thread::spawn(move || {
            submission.publish(1).unwrap();
            submission.close(ProgressError::Fenced);
            submission.close(ProgressError::Closed);
            assert_eq!(submission.publish(2), Err(ProgressError::Fenced));
            drop(submission);
        });
        let early = publisher.observe_submission();
        caller.join().unwrap();
        if early == Some(Ok(1)) {
            publisher.confirm(1, 1).unwrap();
        }
        let before = publisher.observer().observe(1);
        assert_eq!(
            publisher.observe_submission(),
            Some(Err(ProgressError::Fenced))
        );
        publisher.fail(ProgressError::Fenced);
        assert_eq!(publisher.confirm(2, 2), Err(ProgressError::Fenced));
        let after = publisher.observer().observe(1);
        if let Some(before) = before {
            assert_eq!(after, Some(before));
        } else {
            assert_eq!(after, Some(Err(ProgressError::Fenced)));
        }
        assert_eq!(publisher.submitted(), 1);
    });
}

fn identity() -> ProcessingIdentity {
    ProcessingIdentity {
        group: ozzy_proto::ConsumerGroupId::from_bytes([1; 16]),
        partition: ozzy_proto::PartitionIncarnation::from_bytes([2; 16]),
        member: ozzy_proto::ConsumerMemberId::from_bytes([3; 16]),
        assignment_epoch: 1,
        policy: Policy::QuorumDurable,
    }
}

#[test]
fn processing_confirmation_and_terminal_failure_keep_observations_stable() {
    for confirmed in 0..=2 {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(move || {
            let (mut publisher, _submission) =
                processing_progress::Publisher::new(identity(), 0, None);
            let observer = publisher.observer();
            let owner = thread::spawn(move || {
                publisher.confirm(confirmed, 2).unwrap();
                thread::yield_now();
                publisher.fail(ProgressError::Fenced);
                assert_eq!(publisher.confirm(2, 2), Err(ProgressError::Fenced));
                publisher.fail(ProgressError::Closed);
            });
            let early = [observer.observe(1), observer.observe(2)];
            owner.join().unwrap();
            assert_eq!(observer.confirmed(), confirmed);
            for (index, early) in early.into_iter().enumerate() {
                let final_result = observer.observe(index as u64 + 1);
                assert_eq!(
                    final_result,
                    Some(if index as u64 + 1 <= confirmed {
                        Ok(())
                    } else {
                        Err(ProgressError::Fenced)
                    })
                );
                if let Some(early) = early {
                    assert_eq!(Some(early), final_result);
                }
            }
        });
    }
}

#[test]
fn processing_owner_drop_preserves_confirmations_and_closes_unconfirmed_waiters() {
    loom::model(|| {
        let (mut publisher, _submission) = processing_progress::Publisher::new(identity(), 0, None);
        let observer = publisher.observer();
        let owner = thread::spawn(move || {
            publisher.confirm(1, 1).unwrap();
            drop(publisher);
        });
        let early = [observer.observe(1), observer.observe(2)];
        owner.join().unwrap();
        assert_eq!(observer.observe(1), Some(Ok(())));
        assert_eq!(observer.observe(2), Some(Err(ProgressError::Closed)));
        for (index, result) in early.into_iter().enumerate() {
            if let Some(result) = result {
                assert_eq!(Some(result), observer.observe(index as u64 + 1));
            }
        }
    });
}

#[test]
fn processing_wait_observes_confirmation_or_close_after_the_owner_settles() {
    for confirm in [false, true] {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(3);
        model.check(move || {
            let (mut publisher, _submission) =
                processing_progress::Publisher::new(identity(), 0, None);
            let observer = publisher.observer();
            let owner = thread::spawn(move || {
                if confirm {
                    publisher.confirm(1, 1).unwrap();
                }
                drop(publisher);
            });
            // Model the cursor/failure ordering and wait registration, whose
            // order the signal module shadows for Loom. Real-thread tests cover
            // Tokio's wakes.
            let mut cx = Context::from_waker(std::task::Waker::noop());
            let mut wait = pin!(observer.wait(1));
            let early = wait.as_mut().poll(&mut cx);
            owner.join().unwrap();
            let expected = if confirm {
                Ok(())
            } else {
                Err(ProgressError::Closed)
            };
            if early.is_pending() {
                assert_eq!(wait.as_mut().poll(&mut cx), Poll::Ready(expected));
            } else {
                assert_eq!(early, Poll::Ready(expected));
            }
        });
    }
}
