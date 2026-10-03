//! A quiet partition resumes on actual replay input without a timer advance.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn publication_gap_starts_replay_without_waiting_for_refresh() {
    tokio::time::timeout(Duration::from_secs(10), publication_gap())
        .await
        .unwrap();
}

async fn publication_gap() {
    let (mut harness, links, clock, authority) =
        setup_shared_profile(&[], 1, Some((8, 8192, 4))).await;
    harness.publish = false;
    harness.readers.hold_records = true;
    let mut writer = open_writer(&mut harness, &links).await;
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    for n in 0..3 {
        let pending = writer
            .send(
                RecordInput::copy_from_slice(MessageId::from_bytes([50 + n; 16]), &[n; 8]),
                None,
            )
            .await
            .unwrap();
        harness.drive(pending.confirmed(), true).await.unwrap();
    }
    let source = reader::Source::Group {
        authority: Authority {
            group_id: harness.groups[0],
            ..authority.authority
        },
        partition: multiple::incarnation(0),
        owner_epoch: 1,
    };
    // Establish and then retire the initial replay subscription using PUB.
    wait_for_request(&mut harness, &mut reader, Opcode::Subscribe).await;
    links.inject_reader_publication(
        authority.primary,
        &publication(authority.primary, source, 0, &[(50, 0)], harness.wire),
    );
    let first = harness.drive(reader.next(), true).await.unwrap();
    assert_eq!(first.offset, Offset::ZERO);
    drop(first);
    wait_for_request(&mut harness, &mut reader, Opcode::Unsubscribe).await;
    harness.readers.hold_records = false;
    links.inject_reader_publication(
        authority.primary,
        &publication(authority.primary, source, 2, &[(52, 2)], harness.wire),
    );
    let mut repaired = None;
    {
        let mut next = pin!(reader.next());
        for _ in 0..10000 {
            harness.pump(true);
            if let Poll::Ready(record) = futures::poll!(next.as_mut()) {
                repaired = Some(record.unwrap());
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    let repaired =
        repaired.expect("observed PUB gap must start PEER repair before the refresh timer");
    assert_eq!(
        (repaired.offset, repaired.message_id),
        (Offset::new(1), MessageId::from_bytes([51; 16]))
    );
    drop(repaired);
    let last = harness.drive(reader.next(), true).await.unwrap();
    assert_eq!(
        (last.offset, last.message_id),
        (Offset::new(2), MessageId::from_bytes([52; 16]))
    );
    drop(last);
    assert_eq!(clock.now(), Duration::ZERO);
    assert_eq!(reader.checkpoint().positions, vec![(0, Offset::new(3))]);
    harness.drive(reader.close(), true).await.unwrap();
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn quiet_partition_replay_wakes_after_canceled_read_without_clock_advance() {
    tokio::time::timeout(Duration::from_secs(10), scenario())
        .await
        .unwrap();
}

async fn open_writer(harness: &mut Harness, links: &BrokerLinks) -> SharedTopicWriter {
    harness
        .drive(
            SharedTopicWriter::open_with_producer(
                links,
                "orders",
                ProducerId::from_bytes([40; 16]),
                SharedTopicWriterConfig::new(harness.wire),
                appends::retry(),
            ),
            true,
        )
        .await
        .unwrap()
}

pub(super) async fn wait_for_request(
    harness: &mut Harness,
    reader: &mut TopicReader,
    opcode: Opcode,
) {
    let mut next = pin!(reader.next());
    for _ in 0..10000 {
        harness.pump(true);
        assert!(futures::poll!(next.as_mut()).is_pending());
        if harness.readers.requests.iter().any(|(op, _)| *op == opcode) {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("reader did not request {opcode:?}");
}

async fn scenario() {
    let (mut harness, links, clock, _) = setup_shared_profile(&[], 2, Some((8, 8192, 4))).await;
    harness.publish = false;
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    {
        let mut next = pin!(reader.next());
        for _ in 0..1000 {
            harness.pump(true);
            assert!(futures::poll!(next.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
        // The initial route watch may arrive after the first poll. Let that
        // initial refresh settle before testing a quiet established replay.
        clock.advance(Duration::from_millis(100)).unwrap();
        for _ in 0..1000 {
            harness.pump(true);
            assert!(futures::poll!(next.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(
        harness
            .readers
            .requests
            .iter()
            .filter(|(opcode, _)| *opcode == Opcode::Subscribe)
            .count(),
        2,
        "both replay subscriptions must be requested"
    );
    let quiet_at = clock.now();
    assert_eq!(
        reader.checkpoint().positions,
        vec![(0, Offset::ZERO), (1, Offset::ZERO)]
    );
    let mut writer = harness
        .drive(
            SharedTopicWriter::open_with_producer(
                &links,
                "orders",
                ProducerId::from_bytes([40; 16]),
                SharedTopicWriterConfig::new(harness.wire),
                appends::retry(),
            ),
            true,
        )
        .await
        .unwrap();
    for number in 0..2 {
        let key = (0_u32..100)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == number)
            .unwrap();
        let id = MessageId::from_bytes([50 + number as u8; 16]);
        let pending = writer
            .send(RecordInput::copy_from_slice(id, b"record"), Some(&key))
            .await
            .unwrap();
        harness.drive(pending.confirmed(), true).await.unwrap();
        let record = harness.drive(reader.next(), true).await.unwrap();
        assert_eq!(record.partition, number);
        assert_eq!(record.offset, Offset::ZERO);
        assert_eq!(record.message_id, id);
        assert_eq!(record.payload[0].as_ref(), b"record");
        assert_eq!(clock.now(), quiet_at);
    }
    assert_eq!(reader.stats().replayed_records, 2);
    assert_eq!(reader.stats().live_records, 0);
    assert_eq!(
        reader.checkpoint().positions,
        vec![(0, Offset::new(1)), (1, Offset::new(1))]
    );
    harness.drive(reader.close(), true).await.unwrap();
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn received_records_are_delivered_while_another_partition_is_quiet() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut harness, links, _, _) = setup_shared_profile(&[], 2, Some((8, 8192, 4))).await;
        harness.publish = false;
        let mut writer = harness
            .drive(
                SharedTopicWriter::open_with_producer(
                    &links,
                    "orders",
                    ProducerId::from_bytes([40; 16]),
                    SharedTopicWriterConfig::new(harness.wire),
                    appends::retry(),
                ),
                true,
            )
            .await
            .unwrap();
        let key = (0_u32..100)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == 0)
            .unwrap();
        let mut pending = Vec::new();
        for n in 0..4 {
            let id = MessageId::from_bytes([50 + n; 16]);
            pending.push(
                writer
                    .send(RecordInput::copy_from_slice(id, b"record"), Some(&key))
                    .await
                    .unwrap(),
            );
        }
        for pending in pending {
            harness.drive(pending.confirmed(), true).await.unwrap();
        }
        assert_eq!(harness.appends.last().unwrap().2.len(), 4);
        let mut reader = harness
            .drive(
                TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
                true,
            )
            .await
            .unwrap();
        let first = harness.drive(reader.next(), true).await.unwrap();
        assert_eq!((first.partition, first.offset), (0, Offset::ZERO));
        // The first message carried more than one record. Partition 1 has
        // nothing. No input, timer, or broker turn happens until the reader
        // has delivered what it received.
        let mut next = 1;
        while let Poll::Ready(record) = futures::poll!(pin!(reader.next())) {
            let record = record.unwrap();
            assert_eq!((record.partition, record.offset), (0, Offset::new(next)));
            next += 1;
        }
        assert!(next > 1, "a received record waits for unrelated input");
        while next < 4 {
            let record = harness.drive(reader.next(), true).await.unwrap();
            assert_eq!((record.partition, record.offset), (0, Offset::new(next)));
            next += 1;
        }
        harness.drive(reader.close(), true).await.unwrap();
        harness.drive(writer.close(), true).await.unwrap();
        harness.drive(links.shutdown(), true).await.unwrap();
        harness.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unroutable_first_replay_frame_reopens_at_the_exact_offset() {
    tokio::time::timeout(Duration::from_secs(10), replay_gap())
        .await
        .unwrap();
}

async fn replay_gap() {
    let (mut harness, links, clock, _) = setup_shared_profile(&[], 1, Some((8, 8192, 4))).await;
    harness.publish = false;
    let mut writer = open_writer(&mut harness, &links).await;
    for n in 0..8 {
        let pending = writer
            .send(
                RecordInput::copy_from_slice(MessageId::from_bytes([50 + n; 16]), &[n; 8]),
                None,
            )
            .await
            .unwrap();
        harness.drive(pending.confirmed(), true).await.unwrap();
    }
    harness.readers.unroutable_records = 1;
    let mut reader = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    for n in 0..8 {
        let record = harness.drive(reader.next(), true).await.unwrap();
        assert_eq!(record.offset, Offset::new(u64::from(n)));
        assert_eq!(record.message_id, MessageId::from_bytes([50 + n; 16]));
        assert_eq!(record.payload[0].as_ref(), &[n; 8]);
    }
    let subscriptions = harness
        .readers
        .requests
        .iter()
        .filter(|(opcode, _)| *opcode == Opcode::Subscribe)
        .map(|(_, subscription)| *subscription)
        .collect::<Vec<_>>();
    assert_eq!(subscriptions.len(), 2);
    assert_eq!(subscriptions[0].id, subscriptions[1].id);
    assert_ne!(subscriptions[0].generation, subscriptions[1].generation);
    assert_eq!(clock.now(), Duration::ZERO);
    assert_eq!(reader.checkpoint().positions, vec![(0, Offset::new(8))]);
    harness.drive(reader.close(), true).await.unwrap();
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}
