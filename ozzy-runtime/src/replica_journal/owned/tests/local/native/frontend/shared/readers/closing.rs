//! Closing an old subscription must not wait for its disconnected broker.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn reader_close_after_disconnect_keeps_checkpoint_and_settles_without_a_timeout() {
    for opening in [true, false] {
        disconnected(opening).await;
    }
}

async fn disconnected(opening: bool) {
    let (mut harness, links, clock, authority) =
        setup_shared_profile(&[], 1, Some((4, 8192, 1))).await;
    harness.publish = false;
    harness.readers.drop_subscribed = opening;
    let writer = seed_writer(&mut harness, &links).await;
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    wake::wait_for_request(&mut harness, &mut reader, Opcode::Subscribe).await;
    if !opening {
        let record = harness.drive(reader.next(), true).await.unwrap();
        assert_eq!(record.offset, Offset::ZERO);
        assert_eq!(record.payload[0].as_ref(), b"confirmed");
    }
    let checkpoint = reader.checkpoint();
    disconnect(&harness, &links, authority.primary).await;
    // The detached cleanup stays active when its first observer is canceled.
    drop(reader.close());
    tokio::time::timeout(SETTLE, reader.close())
        .await
        .expect("old subscription cleanup waited for broker reconnection")
        .unwrap();
    assert_eq!(reader.checkpoint(), checkpoint);
    assert_eq!(clock.now(), Duration::ZERO);
    drop(writer);
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn reader_close_after_fencing_does_not_need_occupied_control_slots() {
    for replacement in [false, true] {
        occupied(replacement).await;
    }
}

async fn occupied(replacement: bool) {
    let (mut harness, links, clock, authority) =
        setup_shared_profile(&[], 1, Some((4, 8192, 1))).await;
    harness.publish = false;
    let mut writer = seed_writer(&mut harness, &links).await;
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    wake::wait_for_request(&mut harness, &mut reader, Opcode::Subscribe).await;
    drop(harness.drive(reader.next(), true).await.unwrap());
    harness
        .until(true, "reader control completed", |_| {
            links.control_capacity(authority.primary) == 4
        })
        .await;
    let baseline = harness.openings.len();
    let opening = Open {
        authority: authority.authority,
        partition: multiple::incarnation(0),
        producer: ProducerId::from_bytes([88; 16]),
        mode: Mode::Create,
        expected_epoch: None,
        operation: OperationId::from_bytes([89; 16]),
    };
    let mut replies = (0..4)
        .map(|_| Box::pin(links.open_producer(authority.primary, opening, Policy::LocalDurable)))
        .collect::<Vec<_>>();
    for reply in &mut replies {
        assert!(futures::poll!(reply.as_mut()).is_pending());
    }
    // The transport decodes SUBSCRIBED while the cursor stays idle. All four
    // producer observers can then retain completed raw response leases.
    harness
        .until(true, "four producer controls admitted", |harness| {
            harness.openings.len() == baseline + 4
        })
        .await;
    for _ in 0..256 {
        harness.pump(true);
        tokio::task::yield_now().await;
    }
    assert_eq!(links.control_capacity(authority.primary), 0);
    if replacement {
        let session = links.session(authority.primary).unwrap();
        harness.session = None;
        harness.service.start(link(70, 80).binding.peer).unwrap();
        harness
            .until(true, "replacement session established", |_| {
                links
                    .session(authority.primary)
                    .is_some_and(|current| current != session)
            })
            .await;
    } else {
        disconnect(&harness, &links, authority.primary).await;
    }
    assert_eq!(links.control_capacity(authority.primary), 0);
    tokio::time::timeout(SETTLE, reader.close())
        .await
        .expect("fenced cleanup waited for another user's raw reply lease")
        .unwrap();
    assert_eq!(clock.now(), Duration::ZERO);
    drop(replies);
    if replacement {
        harness
            .until(true, "raw response admission returned", |_| {
                links.control_capacity(authority.primary) == 4
            })
            .await;
        let pending = writer
            .send(
                RecordInput::copy_from_slice(MessageId::from_bytes([51; 16]), b"after replacement"),
                None,
            )
            .await
            .unwrap();
        harness
            .drive_advancing(&clock, pending.confirmed(), true)
            .await
            .unwrap();
        assert!(
            harness
                .readers
                .requests
                .iter()
                .all(|(opcode, _)| *opcode != Opcode::Unsubscribe)
        );
    }
    drop(writer);
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

async fn seed_writer(harness: &mut Harness, links: &BrokerLinks) -> SharedTopicWriter {
    let mut writer = wake::open_writer(harness, links).await;
    let pending = writer
        .send(
            RecordInput::copy_from_slice(MessageId::from_bytes([50; 16]), b"confirmed"),
            None,
        )
        .await
        .unwrap();
    harness.drive(pending.confirmed(), true).await.unwrap();
    writer
}

#[tokio::test(flavor = "current_thread")]
async fn reader_close_keeps_a_timeout_when_the_original_session_is_still_live() {
    let (mut harness, links, clock, authority) =
        setup_shared_profile(&[], 1, Some((4, 8192, 1))).await;
    harness.publish = false;
    let writer = seed_writer(&mut harness, &links).await;
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    drop(harness.drive(reader.next(), true).await.unwrap());
    let session = links.session(authority.primary).unwrap();
    harness.readers.drop_unsubscribed = 1;
    let closing = reader.close();
    harness
        .until(true, "unsubscribe accepted and reply lost", |harness| {
            harness.readers.unsubscribed != 0
        })
        .await;
    clock.advance(Duration::from_secs(5)).unwrap();
    let error = harness.drive(closing, true).await.unwrap_err();
    assert!(
        matches!(error, crate::replicated::TopicReaderError::Broker(BrokerLinkError::Failed(ref reason)) if reason == "SDK broker request timed out")
    );
    assert_eq!(links.session(authority.primary), Some(session));
    assert_eq!(reader.checkpoint().positions, vec![(0, Offset::new(1))]);
    drop(writer);
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}

async fn disconnect(harness: &Harness, links: &BrokerLinks, broker: NodeId) {
    harness
        .server
        .clone()
        .close_with_linger(Some(Duration::ZERO))
        .await
        .unwrap();
    tokio::time::timeout(SETTLE, async {
        while links.session(broker).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual OMQ disconnect did not fence the SDK session");
}
