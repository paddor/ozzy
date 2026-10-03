//! A slow application retaining payload aliases must not churn shared PEER work.

use super::*;
use crate::replicated::TopicCheckpoint;

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
                    checkpoint: Some(TopicCheckpoint {
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
