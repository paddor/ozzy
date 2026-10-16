use super::*;
use crate::{
    replica_actor::PartitionStatus,
    replicated::{
        ProcessingIdentity, ProgressError, ReaderStart, RetryPolicy, TopicCheckpoint, TopicReader,
        TopicReaderConfig,
        processing::{driver, window::Window},
    },
};
use ozzy_proto::{ConsumerGroupId, ConsumerMemberId, progress};
use std::sync::Arc;
use tokio::sync::Semaphore;

fn request(authority: ozzy_proto::append::Authority, cursor: Option<u64>) -> progress::Request {
    progress::Request {
        authority,
        assignment: progress::Assignment {
            group: ConsumerGroupId::from_bytes([61; 16]),
            partition: partition(),
            member: ConsumerMemberId::from_bytes([62; 16]),
            epoch: 1,
        },
        policy: Policy::LocalDurable,
        cursor: cursor.map(Offset::new),
    }
}

fn accepted(harness: &Harness) -> u64 {
    let Some(PartitionStatus::Local(snapshot)) = harness.actors.status(harness.group) else {
        panic!("local source partition")
    };
    snapshot.accepted.op.0
}

fn identity() -> ProcessingIdentity {
    ProcessingIdentity {
        group: ConsumerGroupId::from_bytes([61; 16]),
        partition: partition(),
        member: ConsumerMemberId::from_bytes([62; 16]),
        assignment_epoch: 1,
        policy: Policy::LocalDurable,
    }
}

async fn assign(
    harness: &mut Harness,
    request: crate::replica_journal::GroupAssignRequest,
) -> crate::replica_journal::GroupAssignState {
    let (mut submitter, mut buffer) = harness.assignment_service.take().unwrap();
    buffer
        .prepare_group_assignment(request, Policy::LocalDurable)
        .unwrap();
    let pending = submitter.try_submit(buffer).unwrap();
    let mut reply = harness.drive(pending, true).await.unwrap();
    assert!(
        matches!(
            reply.outcome,
            crate::replica_actor::ProposalOutcome::Committed { .. }
        ),
        "{:?}",
        reply.outcome
    );
    let state = reply.buffer.group_assignment_state().unwrap();
    reply.buffer.clear();
    harness.assignment_service = Some((submitter, reply.buffer));
    state
}

async fn reader_at(
    harness: &mut Harness,
    links: &BrokerLinks,
    topic: TopicId,
    cursor: Offset,
) -> TopicReader {
    harness
        .drive(
            TopicReader::open(
                links.clone(),
                "orders",
                TopicReaderConfig {
                    start: ReaderStart::Checkpoint(TopicCheckpoint {
                        topic,
                        positions: vec![(0, cursor)],
                    }),
                    partitions: Some(vec![0]),
                    ..Default::default()
                },
            ),
            true,
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
async fn sdk_processing_handoff_fences_old_tokens_and_retries_the_captured_start() {
    let (mut harness, links, _, hint) = setup_progress(1, Some((2, 4096, 2))).await;
    harness.publish = false;
    let sockets = links.socket_count();
    let topic = harness.drive(links.topic("orders"), true).await.unwrap();
    let routes = links.routes(topic.clone()).await.unwrap();
    let budget = Arc::new(Semaphore::new(4096));
    let (mut old, publisher) = Window::new(identity(), Offset::ZERO, 2, &budget).unwrap();
    let old_owner =
        driver::start(routes.clone(), 0, publisher, RetryPolicy::default(), None).unwrap();
    let mut reader = reader_at(&mut harness, &links, topic.id(), Offset::ZERO).await;
    let record = harness.drive(reader.next(), true).await.unwrap();
    let token = old.deliver(record.offset).unwrap();
    let confirmed = old.submit(token).unwrap();
    assert_eq!(
        harness
            .drive(confirmed.confirmed(), true)
            .await
            .unwrap()
            .cursor,
        Offset::new(1)
    );
    let record = harness.drive(reader.next(), true).await.unwrap();
    assert_eq!(record.offset, Offset::new(1));
    let late_token = old.deliver(record.offset).unwrap();

    let intent = crate::replica_journal::GroupAssignRequest {
        authority: hint.authority,
        group: identity().group,
        partition: identity().partition,
        plan_revision: 2,
        member: Some(ConsumerMemberId::from_bytes([81; 16])),
        initial: ozzy_proto::group::Start::Earliest,
        operation: OperationId::from_bytes([82; 16]),
    };
    let activation = assign(&mut harness, intent).await;
    assert_eq!((activation.epoch, activation.cursor), (2, Offset::new(1)));
    let late = old.submit(late_token).unwrap();
    assert_eq!(
        harness.drive(late.confirmed(), true).await,
        Err(ProgressError::Fenced)
    );
    harness.drive(old_owner, true).await.unwrap();
    assert_eq!(confirmed.confirmed().await.unwrap().cursor, Offset::new(1));
    harness.drive(reader.close(), true).await.unwrap();
    drop((old, reader));

    let new_identity = ProcessingIdentity {
        member: activation.member.unwrap(),
        assignment_epoch: activation.epoch,
        ..identity()
    };
    let (mut new, publisher) = Window::new(new_identity, activation.cursor, 2, &budget).unwrap();
    let new_owner = driver::start(routes, 0, publisher, RetryPolicy::default(), None).unwrap();
    let mut reader = reader_at(&mut harness, &links, topic.id(), activation.cursor).await;
    let mut pending = Vec::new();
    for offset in [1, 2] {
        let record = harness.drive(reader.next(), true).await.unwrap();
        assert_eq!(record.offset, Offset::new(offset));
        let token = new.deliver(record.offset).unwrap();
        pending.push(new.submit(token).unwrap());
    }
    for pending in &pending {
        harness.drive(pending.confirmed(), true).await.unwrap();
    }
    let before = accepted(&harness);
    let retry = assign(&mut harness, intent).await;
    assert_eq!(
        retry.cursor,
        Offset::new(1),
        "lost assignment reply does not query later progress"
    );
    assert_eq!(
        accepted(&harness),
        before,
        "same plan never reassigns the partition"
    );
    let resume = harness
        .drive(
            links.group_progress(
                hint.primary,
                progress::Request {
                    assignment: progress::Assignment {
                        member: new_identity.member,
                        epoch: new_identity.assignment_epoch,
                        ..request(hint.authority, None).assignment
                    },
                    ..request(hint.authority, None)
                },
            ),
            true,
        )
        .await
        .unwrap();
    assert_eq!(resume.cursor, Offset::new(3));
    let revoked = assign(
        &mut harness,
        crate::replica_journal::GroupAssignRequest {
            plan_revision: 3,
            member: None,
            operation: OperationId::from_bytes([83; 16]),
            ..intent
        },
    )
    .await;
    assert_eq!((revoked.member, revoked.cursor), (None, Offset::new(3)));
    assert!(matches!(
        harness
            .drive(
                links.group_progress(
                    hint.primary,
                    progress::Request {
                        assignment: progress::Assignment {
                            member: new_identity.member,
                            epoch: 2,
                            ..request(hint.authority, None).assignment
                        },
                        ..request(hint.authority, None)
                    }
                ),
                true
            )
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));
    harness.drive(reader.close(), true).await.unwrap();
    drop((new, reader));
    harness.drive(new_owner, true).await.unwrap();
    drop((pending, confirmed, late));
    assert_eq!(budget.available_permits(), 4096);
    assert_eq!(links.socket_count(), sockets);
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_processing_reads_acknowledges_and_resumes_from_persisted_broker_progress() {
    let (mut harness, links, _, hint) = setup_progress(1, Some((2, 4096, 2))).await;
    harness.publish = false;
    let sockets = links.socket_count();
    let topic = harness.drive(links.topic("orders"), true).await.unwrap();
    let routes = links.routes(topic.clone()).await.unwrap();
    let budget = Arc::new(Semaphore::new(4096));
    let mut old_waiters = Vec::new();
    for (from, end) in [(0, 2), (2, 3)] {
        let resume = harness
            .drive(
                links.group_progress(hint.primary, request(hint.authority, None)),
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            resume.cursor,
            Offset::new(from),
            "resume comes from the broker, not a local checkpoint"
        );
        let (mut window, publisher) = Window::new(identity(), resume.cursor, 2, &budget).unwrap();
        let owner =
            driver::start(routes.clone(), 0, publisher, RetryPolicy::default(), None).unwrap();
        let mut reader = reader_at(&mut harness, &links, topic.id(), resume.cursor).await;
        let mut tokens = Vec::new();
        for offset in from..end {
            let record = harness.drive(reader.next(), true).await.unwrap();
            assert_eq!(record.offset, Offset::new(offset));
            assert_eq!(record.incarnation, identity().partition);
            assert_eq!(record.partition, 0);
            tokens.push(window.deliver(record.offset).unwrap());
        }
        // Complete the later record first. The processing owner must keep the hole.
        while let Some(token) = tokens.pop() {
            old_waiters.push(window.submit(token).unwrap());
        }
        for pending in &old_waiters {
            harness.drive(pending.confirmed(), true).await.unwrap();
        }
        harness.drive(reader.close(), true).await.unwrap();
        drop(reader);
        drop(window);
        harness.drive(owner, true).await.unwrap();
    }
    let resume = harness
        .drive(
            links.group_progress(hint.primary, request(hint.authority, None)),
            true,
        )
        .await
        .unwrap();
    assert_eq!(resume.cursor, Offset::new(3));
    assert!(budget.available_permits() < 4096);
    drop(old_waiters);
    assert_eq!(budget.available_permits(), 4096);
    assert_eq!(
        links.socket_count(),
        sockets,
        "reader recreation and progress reuse the existing sockets"
    );
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_processing_owner_closes_ready_holes_without_further_caller_polls() {
    let (mut harness, links, _, _) = setup_progress(1, None).await;
    let topic = harness.drive(links.topic("orders"), true).await.unwrap();
    let budget = Arc::new(Semaphore::new(4096));
    let (mut window, publisher) = Window::new(identity(), Offset::ZERO, 3, &budget).unwrap();
    let owner = driver::start(
        links.routes(topic).await.unwrap(),
        0,
        publisher,
        RetryPolicy::default(),
        None,
    )
    .unwrap();
    let first = window.deliver(Offset::ZERO).unwrap();
    let second = window.deliver(Offset::new(1)).unwrap();
    let third = window.deliver(Offset::new(2)).unwrap();
    let before = accepted(&harness);
    let first = window.submit(first).unwrap();
    harness
        .until(false, "admit first processing cursor", |h| {
            accepted(h) > before
        })
        .await;
    let third = window.submit(third).unwrap();
    let second = window.submit(second).unwrap();
    assert!(first.try_confirmed().is_none());
    assert!(second.try_confirmed().is_none());
    assert!(third.try_confirmed().is_none());

    // Awaiting observations never drives the window or submits another cursor.
    assert_eq!(
        harness.drive(third.confirmed(), true).await.unwrap().cursor,
        Offset::new(3)
    );
    assert_eq!(
        first.try_confirmed().unwrap().unwrap().cursor,
        Offset::new(1)
    );
    assert_eq!(
        second.try_confirmed().unwrap().unwrap().cursor,
        Offset::new(2)
    );
    assert_eq!(
        accepted(&harness),
        before + 2,
        "one cumulative request closes two ready records"
    );
    drop(window);
    harness.drive(owner, true).await.unwrap();
    assert!(
        budget.available_permits() < 4096,
        "confirmation aliases retain metadata"
    );
    assert_eq!(third.confirmed().await.unwrap().cursor, Offset::new(3));
    drop((first, second, third));
    assert_eq!(budget.available_permits(), 4096);
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_processing_owner_recovers_lost_reply_with_the_latest_cumulative_intent() {
    let (mut harness, links, clock, hint) = setup_progress(1, None).await;
    let topic = harness.drive(links.topic("orders"), true).await.unwrap();
    let budget = Arc::new(Semaphore::new(4096));
    let (mut window, publisher) = Window::new(identity(), Offset::ZERO, 2, &budget).unwrap();
    let owner = driver::start(
        links.routes(topic).await.unwrap(),
        0,
        publisher,
        RetryPolicy::default(),
        None,
    )
    .unwrap();
    let token = window.deliver(Offset::ZERO).unwrap();
    let first = window.submit(token).unwrap();
    let before = accepted(&harness);
    harness.drop_progress = true;
    harness
        .until(true, "lose first processing confirmation", |h| {
            !h.drop_progress
        })
        .await;
    assert_eq!(accepted(&harness), before + 1);
    assert!(
        first.try_confirmed().is_none(),
        "disk commit is not an SDK confirmation"
    );
    let token = window.deliver(Offset::new(1)).unwrap();
    let second = window.submit(token).unwrap();
    clock.advance(Duration::from_secs(2)).unwrap();
    assert_eq!(
        harness
            .drive_advancing(&clock, second.confirmed(), true)
            .await
            .unwrap()
            .cursor,
        Offset::new(2)
    );
    assert_eq!(
        first.try_confirmed().unwrap().unwrap().cursor,
        Offset::new(1)
    );
    let committed = accepted(&harness);
    assert_eq!(committed, before + 2);
    let source = harness
        .drive(
            links.group_progress(hint.primary, request(hint.authority, None)),
            true,
        )
        .await
        .unwrap();
    assert_eq!(source.cursor, Offset::new(2));
    assert_eq!(accepted(&harness), committed);
    drop(window);
    harness.drive(owner, true).await.unwrap();
    drop((first, second));
    assert_eq!(budget.available_permits(), 4096);
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_processing_owner_resends_a_lost_request_without_a_second_commit() {
    let (mut harness, links, clock, hint) = setup_progress(1, None).await;
    let topic = harness.drive(links.topic("orders"), true).await.unwrap();
    let budget = Arc::new(Semaphore::new(4096));
    let (mut window, publisher) = Window::new(identity(), Offset::ZERO, 2, &budget).unwrap();
    let owner = driver::start(
        links.routes(topic).await.unwrap(),
        0,
        publisher,
        RetryPolicy::default(),
        None,
    )
    .unwrap();
    let token = window.deliver(Offset::ZERO).unwrap();
    let first = window.submit(token).unwrap();
    let before = accepted(&harness);
    harness.drop_progress_request = true;
    harness
        .until(true, "lose the first progress request", |h| {
            !h.drop_progress_request
        })
        .await;
    assert_eq!(
        accepted(&harness),
        before,
        "a lost request journals nothing"
    );
    assert!(first.try_confirmed().is_none());
    // The retry carries the same cumulative cursor and commits it once.
    clock.advance(Duration::from_secs(2)).unwrap();
    assert_eq!(
        harness
            .drive_advancing(&clock, first.confirmed(), true)
            .await
            .unwrap()
            .cursor,
        Offset::new(1)
    );
    assert_eq!(accepted(&harness), before + 1);
    let source = harness
        .drive(
            links.group_progress(hint.primary, request(hint.authority, None)),
            true,
        )
        .await
        .unwrap();
    assert_eq!(source.cursor, Offset::new(1));
    drop(window);
    harness.drive(owner, true).await.unwrap();
    drop(first);
    assert_eq!(budget.available_permits(), 4096);
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_processing_owner_stop_releases_waiters_before_an_admitted_write_finishes() {
    for (reason, abort) in [
        (ProgressError::Closed, false),
        (ProgressError::Fenced, false),
        (ProgressError::Closed, true),
    ] {
        let (mut harness, links, _, hint) = setup_progress(1, None).await;
        let topic = harness.drive(links.topic("orders"), true).await.unwrap();
        let budget = Arc::new(Semaphore::new(4096));
        let (mut window, publisher) = Window::new(identity(), Offset::ZERO, 1, &budget).unwrap();
        let owner = driver::start(
            links.routes(topic).await.unwrap(),
            0,
            publisher,
            RetryPolicy::default(),
            None,
        )
        .unwrap();
        let token = window.deliver(Offset::ZERO).unwrap();
        let before = accepted(&harness);
        let pending = window.submit(token).unwrap();
        harness
            .until(false, "hold admitted processing write", |h| {
                accepted(h) > before
            })
            .await;
        if reason == ProgressError::Fenced {
            window.close(reason);
            assert!(matches!(
                window.deliver(Offset::new(1)),
                Err(ProgressError::Fenced)
            ));
        }
        if abort {
            owner.abort();
            assert!(
                harness
                    .drive(owner, false)
                    .await
                    .unwrap_err()
                    .is_cancelled()
            );
            assert!(matches!(
                window.deliver(Offset::new(1)),
                Err(ProgressError::Closed)
            ));
            drop(window);
        } else {
            drop(window);
            harness.drive(owner, false).await.unwrap();
        }
        assert_eq!(pending.confirmed().await, Err(reason));
        assert!(budget.available_permits() < 4096);
        harness
            .until(true, "settle stopped processing write", |h| {
                let Some(PartitionStatus::Local(snapshot)) = h.actors.status(h.group) else {
                    return false;
                };
                snapshot.applied == snapshot.accepted
            })
            .await;
        let source = harness
            .drive(
                links.group_progress(hint.primary, request(hint.authority, None)),
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            source.cursor,
            Offset::new(1),
            "stopping an observer does not cancel broker commit"
        );
        assert_eq!(
            pending.confirmed().await,
            Err(reason),
            "a sealed observation cannot reopen"
        );
        drop(pending);
        assert_eq!(budget.available_permits(), 4096);
        harness.drive(links.shutdown(), true).await.unwrap();
        harness.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_processing_owner_preserves_source_assignment_fencing() {
    let (mut harness, links, _, _) = setup_progress(1, None).await;
    let topic = harness.drive(links.topic("orders"), true).await.unwrap();
    let budget = Arc::new(Semaphore::new(4096));
    let mut wrong = identity();
    wrong.member = ConsumerMemberId::from_bytes([99; 16]);
    let (mut window, publisher) = Window::new(wrong, Offset::ZERO, 1, &budget).unwrap();
    let owner = driver::start(
        links.routes(topic).await.unwrap(),
        0,
        publisher,
        RetryPolicy::default(),
        None,
    )
    .unwrap();
    let token = window.deliver(Offset::ZERO).unwrap();
    let before = accepted(&harness);
    let pending = window.submit(token).unwrap();
    assert_eq!(
        harness.drive(pending.confirmed(), true).await,
        Err(ProgressError::Fenced)
    );
    harness.drive(owner, true).await.unwrap();
    assert_eq!(accepted(&harness), before);
    drop(window);
    assert_eq!(pending.confirmed().await, Err(ProgressError::Fenced));
    drop(pending);
    assert_eq!(budget.available_permits(), 4096);
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_progress_over_inproc_waits_for_disk_and_recovers_after_a_lost_reply() {
    let (mut harness, links, clock, hint) = setup_progress(1, None).await;
    assert_eq!(links.socket_count(), 2, "progress reuses broker sockets");
    let before = accepted(&harness);
    let mut pending = pin!(links.group_progress(hint.primary, request(hint.authority, Some(2))));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    harness
        .until(false, "progress admission", |h| accepted(h) > before)
        .await;
    assert!(
        futures::poll!(pending.as_mut()).is_pending(),
        "accepted is not durable"
    );
    let query = harness
        .drive(
            links.group_progress(hint.primary, request(hint.authority, None)),
            false,
        )
        .await
        .unwrap();
    assert_eq!((query.cursor, query.end), (Offset::ZERO, Offset::new(3)));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    harness.drop_progress = true;
    harness
        .until(true, "drop durable progress reply", |h| !h.drop_progress)
        .await;
    assert!(futures::poll!(pending.as_mut()).is_pending());
    let confirmed_prefix = accepted(&harness);
    let query = harness
        .drive(
            links.group_progress(hint.primary, request(hint.authority, None)),
            true,
        )
        .await
        .unwrap();
    assert_eq!(query.cursor, Offset::new(2));
    clock.advance(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        harness.drive(pending.as_mut(), true).await,
        Err(BrokerLinkError::Timeout)
    ));
    let retried = harness
        .drive(
            links.group_progress(hint.primary, request(hint.authority, Some(1))),
            true,
        )
        .await
        .unwrap();
    assert_eq!(retried.cursor, Offset::new(2));
    assert_eq!(
        accepted(&harness),
        confirmed_prefix,
        "retry and queries do not append"
    );
    let mut wrong = request(hint.authority, Some(2));
    wrong.assignment.epoch += 1;
    assert!(matches!(
        harness
            .drive(links.group_progress(hint.primary, wrong), true)
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_progress_cancellation_keeps_commit_and_retention_errors_do_not_reset_it() {
    for history in [
        ProgressHistory::Available,
        ProgressHistory::Expired {
            cursor: Offset::new(1),
        },
        ProgressHistory::Expired {
            cursor: Offset::ZERO,
        },
    ] {
        let (mut harness, links, _, hint) = Box::pin(setup_shared_directory_profile(
            &[],
            1,
            None,
            false,
            Some(history),
        ))
        .await;
        if matches!(history, ProgressHistory::Available) {
            let before = accepted(&harness);
            let pending = links.group_progress(hint.primary, request(hint.authority, Some(2)));
            let mut pending = Box::pin(pending);
            assert!(futures::poll!(pending.as_mut()).is_pending());
            harness
                .until(false, "admit canceled progress", |h| accepted(h) > before)
                .await;
            drop(pending);
            let query = harness
                .drive(
                    links.group_progress(hint.primary, request(hint.authority, None)),
                    true,
                )
                .await
                .unwrap();
            // A query can race physical completion, and must never read speculative state.
            if query.cursor == Offset::ZERO {
                harness
                    .until(true, "apply canceled progress", |h| {
                        let Some(PartitionStatus::Local(snapshot)) = h.actors.status(h.group)
                        else {
                            return false;
                        };
                        snapshot.applied == snapshot.accepted
                    })
                    .await;
            }
            let query = harness
                .drive(
                    links.group_progress(hint.primary, request(hint.authority, None)),
                    true,
                )
                .await
                .unwrap();
            assert_eq!(query.cursor, Offset::new(2));
        } else if let ProgressHistory::Expired { cursor: persisted } = history {
            let before = accepted(&harness);
            assert!(
                matches!(harness.drive(links.group_progress(hint.primary, request(hint.authority, None)), true).await, Err(BrokerLinkError::Progress(ProgressError::HistoryExpired { cursor, retained_from })) if cursor == persisted && retained_from == Offset::new(3))
            );
            let covered = harness
                .drive(
                    links.group_progress(
                        hint.primary,
                        request(hint.authority, Some(persisted.get())),
                    ),
                    true,
                )
                .await
                .unwrap();
            assert_eq!(
                (covered.cursor, covered.retained_from),
                (persisted, Offset::new(3))
            );
            assert_eq!(accepted(&harness), before);
        }
        harness.drive(links.shutdown(), true).await.unwrap();
        harness.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
async fn sdk_activation_over_shared_links_allocates_epochs_and_reports_typed_rejections() {
    use ozzy_proto::group::{self, AssignRequest, Start};
    let (mut harness, links, _, hint) = setup_progress(1, None).await;
    let sockets = links.socket_count();
    let request = AssignRequest {
        authority: hint.authority,
        partition: identity().partition,
        group: identity().group,
        member: Some(ConsumerMemberId::from_bytes([81; 16])),
        plan_revision: 2,
        initial: Start::Earliest,
        policy: Policy::LocalDurable,
    };
    let before = accepted(&harness);
    let mut pending = pin!(links.group_assign(hint.primary, request));
    assert!(futures::poll!(pending.as_mut()).is_pending());
    harness
        .until(false, "activation admission", |h| accepted(h) > before)
        .await;
    assert!(
        futures::poll!(pending.as_mut()).is_pending(),
        "accepted activation is not confirmed"
    );
    let activated = harness.drive(pending, true).await.unwrap();
    assert_eq!(
        (
            activated.epoch,
            activated.plan_revision,
            activated.cursor,
            activated.end
        ),
        (2, 2, Offset::ZERO, Offset::new(3))
    );
    let confirmed = accepted(&harness);
    let repeated = harness
        .drive(links.group_assign(hint.primary, request), true)
        .await
        .unwrap();
    assert_eq!((repeated.epoch, repeated.cursor), (2, Offset::ZERO));
    assert_eq!(
        accepted(&harness),
        confirmed,
        "repeated plans do not append"
    );
    assert!(matches!(
        harness
            .drive(
                links.group_assign(
                    hint.primary,
                    AssignRequest {
                        plan_revision: 1,
                        ..request
                    }
                ),
                true
            )
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));
    assert!(matches!(
        harness
            .drive(
                links.group_assign(
                    hint.primary,
                    AssignRequest {
                        member: Some(ConsumerMemberId::from_bytes([99; 16])),
                        ..request
                    }
                ),
                true
            )
            .await,
        Err(BrokerLinkError::Rejected { code: 8, .. })
    ));
    assert_eq!(
        harness
            .drive(
                links.group_assign(
                    hint.primary,
                    AssignRequest {
                        group: ConsumerGroupId::from_bytes([70; 16]),
                        plan_revision: 1,
                        initial: Start::Offset(Offset::new(9)),
                        ..request
                    }
                ),
                true
            )
            .await
            .map(|_| ())
            .unwrap_err()
            .to_string(),
        BrokerLinkError::InitialPosition(group::InitialPosition {
            cursor: Offset::new(9),
            retained_from: Offset::ZERO,
            end: Offset::new(3),
        })
        .to_string()
    );
    assert!(matches!(
        harness
            .drive(
                links.group_progress(
                    hint.primary,
                    progress::Request {
                        assignment: progress::Assignment {
                            member: request.member.unwrap(),
                            epoch: 2,
                            ..request_at(hint.authority).assignment
                        },
                        ..request_at(hint.authority)
                    }
                ),
                true
            )
            .await,
        Ok(progress::Confirmed { cursor, .. }) if cursor == Offset::ZERO
    ));
    assert!(matches!(
        harness
            .drive(
                links.group_progress(hint.primary, request_at(hint.authority)),
                true
            )
            .await,
        Err(BrokerLinkError::Progress(ProgressError::Fenced))
    ));
    assert_eq!(links.socket_count(), sockets);
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

fn request_at(authority: ozzy_proto::append::Authority) -> progress::Request {
    request(authority, None)
}
