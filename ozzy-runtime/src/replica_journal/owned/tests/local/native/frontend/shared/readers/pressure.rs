//! A slow application retaining payload aliases must not churn shared PEER work.

use super::*;
use crate::replicated::TopicCheckpoint;

#[tokio::test(flavor = "current_thread")]
async fn paused_reader_control_replies_do_not_block_producer_attachment() {
    let (mut harness, links, clock, _) = setup_shared_profile(&[], 2, Some((4, 8192, 4))).await;
    harness.publish = false;
    harness.readers.hold_records = true;
    let mut readers = Vec::new();
    for _ in 0..2 {
        readers.push(
            harness
                .drive(
                    TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
                    true,
                )
                .await
                .unwrap(),
        );
    }
    pause_controls(&mut harness, &mut readers, Opcode::Subscribe).await;
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
    append(&mut harness, &mut writer, 0).await;
    append(&mut harness, &mut writer, 1).await;
    harness.readers.hold_records = false;
    while let Some(message) = harness.readers.records.pop_front() {
        harness.data_server.try_send(message).unwrap();
    }
    for reader in &mut readers {
        let mut partitions = BTreeSet::new();
        for _ in 0..2 {
            let record = harness.drive(reader.next(), true).await.unwrap();
            assert!(partitions.insert(record.partition));
            let value = record.partition as u8;
            assert_eq!(record.message_id, MessageId::from_bytes([50 + value; 16]));
            assert_eq!(record.payload[0].as_ref(), &[value; 8]);
        }
    }
    // Duplicate confirmed publications switch replay to live without adding
    // any new records. All four cursors must retire their replay subscriptions.
    while let Some(publication) = harness.publications.pop_front() {
        links.inject_reader_publication(harness.local, &publication);
    }
    pause_controls(&mut harness, &mut readers, Opcode::Unsubscribe).await;
    let identity = writer.identity();
    harness.drive(writer.close(), true).await.unwrap();
    let mut resumed = harness
        .drive(
            SharedTopicWriter::resume(
                &links,
                "orders",
                identity,
                SharedTopicWriterConfig::new(harness.wire),
                appends::retry(),
            ),
            true,
        )
        .await
        .unwrap();
    append(&mut harness, &mut resumed, 2).await;
    for reader in &mut readers {
        harness.drive(reader.close(), true).await.unwrap();
    }
    assert_eq!(clock.now(), Duration::ZERO);
    harness.drive(resumed.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

async fn pause_controls(harness: &mut Harness, readers: &mut [TopicReader], opcode: Opcode) {
    let mut waiting = readers
        .iter_mut()
        .map(|reader| Box::pin(reader.next()))
        .collect::<Vec<_>>();
    harness
        .until(true, "four reader control requests admitted", |harness| {
            for next in &mut waiting {
                assert!(
                    next.as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
            }
            harness
                .readers
                .requests
                .iter()
                .filter(|(sent, _)| *sent == opcode)
                .count()
                == 4
        })
        .await;
    // Cancel next-record observers, then leave both readers completely idle.
    // Each cursor still owns its in-flight control completion.
}

#[tokio::test(flavor = "current_thread")]
async fn retained_payloads_pause_shared_data_without_blocking_control() {
    tokio::time::timeout(Duration::from_secs(10), pressure())
        .await
        .unwrap();
}

async fn pressure() {
    let (mut harness, links, clock, _) = setup_shared_profile(&[], 1, Some((4, 8192, 4))).await;
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
    let mut slow = harness
        .drive(
            TopicReader::open(links.clone(), "orders", TopicReaderConfig::default()),
            true,
        )
        .await
        .unwrap();
    let mut retained = Vec::new();
    for n in 0..7 {
        append(&mut harness, &mut writer, n).await;
        let record = harness.drive(slow.next(), true).await.unwrap();
        assert_eq!(record.offset, Offset::new(u64::from(n)));
        retained.push(record.payload[0].clone());
    }
    append(&mut harness, &mut writer, 7).await;
    let checkpoint = slow.checkpoint();
    assert_eq!(checkpoint.positions, vec![(0, Offset::new(7))]);
    {
        let mut next = pin!(slow.next());
        for _ in 0..1000 {
            harness.pump(true);
            assert!(futures::poll!(next.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    }
    assert_eq!(clock.now(), Duration::ZERO);
    assert_eq!(slow.checkpoint(), checkpoint);
    assert_eq!(
        harness
            .readers
            .requests
            .iter()
            .filter(|(opcode, _)| *opcode == Opcode::Subscribe)
            .count(),
        1,
        "retained aliases must not trigger another subscription"
    );
    let mut healthy = harness
        .drive(
            TopicReader::open(
                links.clone(),
                "orders",
                TopicReaderConfig {
                    start: crate::replicated::ReaderStart::Checkpoint(TopicCheckpoint {
                        topic: checkpoint.topic,
                        positions: vec![(0, Offset::new(7))],
                    }),
                    ..TopicReaderConfig::default()
                },
            ),
            true,
        )
        .await
        .unwrap();
    // Replay shares the broker data connection. Its paused source can hold a
    // second subscription's data, while the control open above still completes.
    {
        let mut next = pin!(healthy.next());
        for _ in 0..64 {
            harness.pump(true);
            assert!(futures::poll!(next.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    }
    // Releasing backing resumes the exact source without a clock or wire grant.
    drop(retained.pop());
    let record = harness.drive(healthy.next(), true).await.unwrap();
    assert_eq!(record.offset, Offset::new(7));
    assert_eq!(record.message_id, MessageId::from_bytes([57; 16]));
    drop(record);
    harness.drive(healthy.close(), true).await.unwrap();
    // The resumed connection retained the slow reader's original frame.
    let record = harness.drive(slow.next(), true).await.unwrap();
    assert_eq!(record.offset, Offset::new(7));
    assert_eq!(record.message_id, MessageId::from_bytes([57; 16]));
    assert_eq!(record.payload[0].as_ref(), &[7; 8]);
    assert_eq!(slow.checkpoint().positions, vec![(0, Offset::new(8))]);
    drop((record, retained));
    harness.drive(slow.close(), true).await.unwrap();
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

async fn append(harness: &mut Harness, writer: &mut SharedTopicWriter, n: u8) {
    let pending = writer
        .send(
            RecordInput::copy_from_slice(MessageId::from_bytes([50 + n; 16]), &[n; 8]),
            None,
        )
        .await
        .unwrap();
    harness.drive(pending.confirmed(), true).await.unwrap();
}
