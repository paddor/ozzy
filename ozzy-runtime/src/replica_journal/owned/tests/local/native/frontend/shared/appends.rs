use super::*;
use crate::replicated::{RecordInput, RetryPolicy, Writer, WriterConfig};
use ozzy_proto::MessageId;

pub(super) fn writer_config(producer: u8, next_sequence: u64) -> WriterConfig {
    WriterConfig {
        policy: Policy::LocalDurable,
        partition: partition(),
        owner_epoch: 1,
        producer_id: ProducerId::from_bytes([producer; 16]),
        producer_epoch: 1,
        next_sequence,
        limits: limits(),
        compress_payloads: true,
        batch_target_bytes: 1024,
        max_producers: 1,
        inflight_appends: 1,
    }
}

pub(super) fn retry() -> RetryPolicy {
    RetryPolicy {
        handshake_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_millis(100),
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(100),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shared_append_retry_preserves_other_writer_and_metadata_sessions() {
    tokio::time::timeout(Duration::from_secs(10), scenario())
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn shared_append_session_replacement_discards_old_confirmation() {
    tokio::time::timeout(Duration::from_secs(10), replacement())
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn lost_predecessor_remains_retryable_when_later_append_arrives_first() {
    tokio::time::timeout(Duration::from_secs(10), predecessor())
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn admission_refusal_keeps_earlier_confirmation_correlated() {
    tokio::time::timeout(Duration::from_secs(10), refused_successor())
        .await
        .unwrap();
}

async fn refused_successor() {
    let (mut harness, links, clock, _) = setup_shared_with_writers(&[]).await;
    let topic = harness.drive(links.topic("orders"), false).await.unwrap();
    let routes = links.routes(topic).unwrap();
    let mut config = writer_config(40, 0);
    config.inflight_appends = 2;
    // Each record fills its batch, so the second APPEND pipelines behind the
    // first instead of waiting for its confirmation.
    config.batch_target_bytes = 1;
    let mut writer = Writer::open_shared(&routes, 0, config, retry())
        .await
        .unwrap();
    let producer = ProducerId::from_bytes([40; 16]);
    harness.hold_confirmation = Some(producer);
    let first = writer
        .send(RecordInput::copy_from_slice(
            MessageId::from_bytes([50; 16]),
            b"first",
        ))
        .await
        .unwrap();
    harness
        .until(true, "first confirmation was not held", |h| {
            h.held_confirmation.is_some()
        })
        .await;
    harness.refuse_append = Some((producer, 1));
    let second = writer
        .send(RecordInput::copy_from_slice(
            MessageId::from_bytes([51; 16]),
            b"second",
        ))
        .await
        .unwrap();
    harness
        .until(false, "second APPEND was not refused", |h| {
            h.refuse_append.is_none()
        })
        .await;
    for _ in 0..1000 {
        harness.pump(false);
        tokio::task::yield_now().await;
    }
    clock
        .advance(clock.now().saturating_add(Duration::from_millis(15)))
        .unwrap();
    for _ in 0..1000 {
        harness.pump(false);
        tokio::task::yield_now().await;
    }
    assert_eq!(
        harness
            .appends
            .iter()
            .filter(|attempt| attempt.0 == producer && attempt.1 == 0)
            .count(),
        1,
        "credit refusal replayed an earlier admitted request"
    );
    assert!(first.try_confirmed().is_none());
    harness
        .server
        .send(harness.held_confirmation.take().unwrap())
        .await
        .unwrap();
    let (first, second) = harness
        .drive_advancing(
            &clock,
            async { (first.confirmed().await, second.confirmed().await) },
            true,
        )
        .await;
    assert_eq!(first.unwrap().offset, 0);
    assert_eq!(second.unwrap().offset, 1);
    writer.close().await.unwrap();
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

async fn predecessor() {
    let (mut harness, links, clock, _) = setup_shared_with_writers(&[]).await;
    let topic = harness.drive(links.topic("orders"), false).await.unwrap();
    let routes = links.routes(topic).unwrap();
    let mut config = writer_config(40, 0);
    config.inflight_appends = 2;
    // Each record fills its batch, so the second APPEND pipelines behind the
    // first instead of waiting for its confirmation.
    config.batch_target_bytes = 1;
    let mut writer = Writer::open_shared(&routes, 0, config, retry())
        .await
        .unwrap();
    harness.drop_append = Some((ProducerId::from_bytes([40; 16]), 0));
    let first = writer
        .send(RecordInput::copy_from_slice(
            MessageId::from_bytes([50; 16]),
            b"first",
        ))
        .await
        .unwrap();
    for _ in 0..10000 {
        harness.pump(true);
        if harness.drop_append.is_none() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        harness.drop_append.is_none(),
        "first APPEND never reached delivery gate"
    );
    let second = writer
        .send(RecordInput::copy_from_slice(
            MessageId::from_bytes([51; 16]),
            b"second",
        ))
        .await
        .unwrap();
    let mut rejected = false;
    for _ in 0..10000 {
        harness.pump(true);
        if harness
            .appends
            .iter()
            .any(|attempt| attempt.1 == 1 && harness.capacity_replies.contains(&attempt.3))
        {
            rejected = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        rejected,
        "missing predecessor did not receive retryable rejection"
    );
    assert!(first.try_confirmed().is_none());
    assert!(second.try_confirmed().is_none());
    let mut completion = pin!(async { (first.confirmed().await, second.confirmed().await) });
    let mut receipts = None;
    for _ in 0..10000 {
        harness.pump(true);
        if let Poll::Ready(value) = futures::poll!(completion.as_mut()) {
            receipts = Some(value);
            break;
        }
        clock
            .advance(clock.now().saturating_add(Duration::from_millis(1)))
            .unwrap();
        tokio::task::yield_now().await;
    }
    let (first, second) = receipts.expect("predecessor retry did not settle");
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(
        (first.offset, first.key.first_sequence, first.message_id),
        (0, 0, MessageId::from_bytes([50; 16]))
    );
    assert_eq!(
        (second.offset, second.key.first_sequence, second.message_id),
        (1, 1, MessageId::from_bytes([51; 16]))
    );
    assert_eq!(harness.openings.len(), 1);
    writer.close().await.unwrap();
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

#[allow(clippy::too_many_lines)]
async fn replacement() {
    let (mut harness, links, clock, authority) = setup_shared().await;
    let topic = harness.drive(links.topic("orders"), false).await.unwrap();
    let routes = links.routes(topic).unwrap();
    let opened = harness
        .drive(
            links.open_producer(
                authority.primary,
                Open {
                    authority: authority.authority,
                    partition: partition(),
                    producer: ProducerId::from_bytes([40; 16]),
                    mode: Mode::Resume,
                    expected_epoch: None,
                    operation: OperationId::from_bytes([41; 16]),
                },
                Policy::LocalDurable,
            ),
            true,
        )
        .await
        .unwrap();
    let mut writer =
        Writer::connect_shared(&routes, 0, writer_config(40, opened.next_sequence), retry())
            .await
            .unwrap();
    let old = links.session(authority.primary).unwrap();
    let openings = harness.openings.len();
    harness.hold_confirmation = Some(ProducerId::from_bytes([40; 16]));
    let pending = writer
        .send(RecordInput::copy_from_slice(
            MessageId::from_bytes([50; 16]),
            b"record",
        ))
        .await
        .unwrap();
    let mut held = false;
    for _ in 0..10000 {
        harness.pump(true);
        if harness.held_confirmation.is_some() {
            held = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(held);
    assert!(pending.try_confirmed().is_none());
    harness.drop_confirmation = Some(ProducerId::from_bytes([40; 16]));
    harness.service.start(link(70, 80).binding.peer).unwrap();
    let mut replaced = false;
    for _ in 0..10000 {
        harness.pump(true);
        clock
            .advance(clock.now().saturating_add(Duration::from_millis(1)))
            .unwrap();
        if links
            .session(authority.primary)
            .is_some_and(|current| current != old)
            && harness.drop_confirmation.is_none()
        {
            replaced = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        replaced,
        "writer did not retry through the new shared session"
    );
    assert!(pending.try_confirmed().is_none());
    let current = links.session(authority.primary).unwrap();
    harness
        .server
        .send(harness.held_confirmation.take().unwrap())
        .await
        .unwrap();
    for _ in 0..100 {
        harness.pump(true);
        assert!(
            pending.try_confirmed().is_none(),
            "obsolete session confirmed a fresh attempt"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(
        harness
            .drive(links.topic("orders"), true)
            .await
            .unwrap()
            .partition_count(),
        1
    );
    clock.advance(Duration::from_secs(1)).unwrap();
    let receipt = harness
        .drive_advancing(&clock, pending.confirmed(), true)
        .await
        .unwrap();
    assert_eq!(receipt.message_id, MessageId::from_bytes([50; 16]));
    assert_eq!(receipt.key.first_sequence, opened.next_sequence);
    assert_eq!(receipt.offset, 0);
    assert_eq!(receipt.policy, Policy::LocalDurable);
    assert_eq!(links.session(authority.primary), Some(current));
    assert_eq!(links.socket_count(), 2);
    assert_eq!(harness.openings.len(), openings);

    let attempts = harness
        .appends
        .iter()
        .filter(|(producer, _, _, _, _)| *producer == ProducerId::from_bytes([40; 16]))
        .collect::<Vec<_>>();
    assert!(attempts.len() >= 3);
    assert_eq!(attempts[0].4, old);
    assert!(attempts[1..].iter().all(|attempt| attempt.4 == current));
    let ids = attempts
        .iter()
        .map(|attempt| attempt.3)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), attempts.len());
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.2 == vec![MessageId::from_bytes([50; 16])])
    );
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[allow(clippy::too_many_lines)]
async fn scenario() {
    let (mut harness, links, clock, authority) = setup_shared().await;
    let topic = harness.drive(links.topic("orders"), false).await.unwrap();
    let routes = links.routes(topic).unwrap();
    let session = links.session(authority.primary).unwrap();
    let open = |writer, operation| Open {
        authority: authority.authority,
        partition: partition(),
        producer: ProducerId::from_bytes([writer; 16]),
        mode: Mode::Resume,
        expected_epoch: None,
        operation: OperationId::from_bytes([operation; 16]),
    };
    let (a, b) = harness
        .drive(
            futures::future::join(
                links.open_producer(authority.primary, open(40, 41), Policy::LocalDurable),
                links.open_producer(authority.primary, open(30, 31), Policy::LocalDurable),
            ),
            true,
        )
        .await;
    let first = a.unwrap().next_sequence;
    let second = b.unwrap().next_sequence;
    let mut a = Writer::connect_shared(&routes, 0, writer_config(40, first), retry())
        .await
        .unwrap();
    let mut b = Writer::connect_shared(&routes, 0, writer_config(30, second), retry())
        .await
        .unwrap();
    assert_eq!(links.socket_count(), 2);
    assert!(
        harness.watch.is_none(),
        "idle writer registered a partition"
    );
    harness.drop_confirmation = Some(ProducerId::from_bytes([40; 16]));
    let mut pending_a = Vec::new();
    let mut pending_b = Vec::new();
    for (writer, pending, base) in [
        (&mut a, &mut pending_a, 50_u8),
        (&mut b, &mut pending_b, 60),
    ] {
        for index in 0..2 {
            pending.push(
                writer
                    .send(RecordInput::copy_from_slice(
                        MessageId::from_bytes([base + index; 16]),
                        &[base, index],
                    ))
                    .await
                    .unwrap(),
            );
        }
    }
    for _ in 0..100 {
        harness.pump(false);
        assert!(
            pending_a
                .iter()
                .chain(&pending_b)
                .all(|pending| pending.try_confirmed().is_none()),
            "transport receipt confirmed unfinished disk work"
        );
        tokio::task::yield_now().await;
    }
    let healthy = harness
        .drive(
            futures::future::try_join_all(
                pending_b
                    .iter()
                    .map(crate::replicated::PendingRecord::confirmed),
            ),
            true,
        )
        .await
        .unwrap();
    // Each writer sends from its own thread. The first writer's request can
    // arrive after the second writer's confirmations.
    harness
        .until(
            true,
            "first writer's confirmation never arrived",
            |harness| harness.drop_confirmation.is_none(),
        )
        .await;
    assert!(
        pending_a
            .iter()
            .any(|pending| pending.try_confirmed().is_none())
    );
    assert_eq!(links.session(authority.primary), Some(session));
    assert_eq!(
        harness
            .drive(links.topic("orders"), true)
            .await
            .unwrap()
            .partition_count(),
        1
    );
    clock.advance(Duration::from_secs(1)).unwrap();
    let retried = harness
        .drive_advancing(
            &clock,
            futures::future::try_join_all(
                pending_a
                    .iter()
                    .map(crate::replicated::PendingRecord::confirmed),
            ),
            true,
        )
        .await
        .unwrap();
    let mut offsets = healthy
        .iter()
        .chain(&retried)
        .map(|receipt| receipt.offset)
        .collect::<Vec<_>>();
    offsets.sort_unstable();
    assert_eq!(offsets, vec![0, 1, 2, 3]);
    assert!(
        healthy
            .iter()
            .chain(&retried)
            .all(|receipt| receipt.policy == Policy::LocalDurable)
    );
    for (index, receipt) in retried.iter().enumerate() {
        assert_eq!(
            receipt.message_id,
            MessageId::from_bytes([50 + index as u8; 16])
        );
        assert_eq!(receipt.key.first_sequence, first + index as u64);
    }
    assert!(a.stats().records > 2);
    assert_eq!(b.stats().records, 2);
    assert_eq!(links.session(authority.primary), Some(session));
    assert_eq!(links.socket_count(), 2);
    for (producer, sequence, ids, _, frame_session) in &harness.appends {
        assert_eq!(*frame_session, session);
        let (start, base) = if *producer == ProducerId::from_bytes([40; 16]) {
            (first, 50_u8)
        } else {
            (second, 60_u8)
        };
        for (index, id) in ids.iter().enumerate() {
            assert_eq!(
                *id,
                MessageId::from_bytes([base + (*sequence - start) as u8 + index as u8; 16])
            );
        }
    }
    harness
        .drive(futures::future::try_join(a.close(), b.close()), true)
        .await
        .unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}
