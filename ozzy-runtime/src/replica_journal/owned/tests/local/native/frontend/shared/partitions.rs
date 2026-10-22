//! Explicit placement with controlled admission and confirmation loss.

use super::*;
use crate::replicated::{
    RecordInput, SharedTopicWriter, SharedTopicWriterConfig, TopicWriterError,
};
use futures::FutureExt;
use ozzy_proto::MessageId;

fn record(id: u8) -> RecordInput {
    RecordInput::copy_from_slice(MessageId::from_bytes([id; 16]), &[id; 1024])
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_and_keyed_records_from_clones_share_one_append() {
    tokio::time::timeout(Duration::from_secs(10), mixed_batch())
        .await
        .unwrap();
}

async fn mixed_batch() {
    let (mut harness, links, _, _) = setup_shared_partitions(&[], 2).await;
    let options = SharedTopicWriterConfig {
        max_producers: 2,
        ..SharedTopicWriterConfig::new(limits())
    };
    let mut writer = harness
        .drive(
            SharedTopicWriter::open(&links, "orders", options, appends::retry()),
            true,
        )
        .await
        .unwrap();
    let mut clone = writer.try_clone().unwrap();
    let key = (0_u32..100)
        .map(u32::to_be_bytes)
        .find(|key| writer.metadata().keyed_partition(key).number == 1)
        .unwrap();
    // The broker cannot open this producer until a pump turn. Every record
    // is ready before its first APPEND forms, across both caller lanes.
    let mut pending = Vec::new();
    for id in 1..=4 {
        let record = RecordInput::copy_from_slice(MessageId::from_bytes([id; 16]), &[id; 16]);
        pending.push(if id % 2 == 0 {
            clone.send(record, Some(&key)).await.unwrap()
        } else {
            writer.send_to_partition(record, 1).await.unwrap()
        });
    }
    let captured = writer.flush();
    harness.drive(captured, true).await.unwrap();
    for (sequence, pending) in pending.into_iter().enumerate() {
        assert_eq!(pending.partition(), 1);
        assert_eq!(pending.sequence(), sequence as u64);
        let receipt = pending.confirmed().await.unwrap();
        assert_eq!(receipt.record.offset, sequence as u64);
    }
    assert_eq!(harness.appends.len(), 1);
    assert_eq!(
        harness.appends[0].2,
        [1, 2, 3, 4].map(|id| MessageId::from_bytes([id; 16]))
    );
    assert_eq!(writer.partition_stats(1).unwrap().max_records, 4);
    assert_eq!(writer.partition_stats(0).unwrap().records, 0);
    drop(clone);
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_explicit_admission_preserves_sequences_and_other_partitions() {
    tokio::time::timeout(Duration::from_secs(10), cancellation())
        .await
        .unwrap();
}

async fn cancellation() {
    let (mut harness, links, _, _) = setup_shared_partitions(&[], 2).await;
    let mut options = SharedTopicWriterConfig::new(limits());
    options.batch_target_bytes = 64;
    let mut writer = harness
        .drive(
            SharedTopicWriter::open(&links, "orders", options, appends::retry()),
            true,
        )
        .await
        .unwrap();
    assert!(matches!(
        writer
            .send_to_partition(
                RecordInput::copy_from_slice(MessageId::from_bytes([0; 16]), b"invalid"),
                0
            )
            .await,
        Err(TopicWriterError::Send {
            source: crate::replicated::WriterError::RecordLimits,
            ..
        })
    ));
    // No broker turns: producer opening cannot complete or free intake space.
    let first = writer.send_to_partition(record(50), 0).await.unwrap();
    assert_eq!(first.sequence(), 0);
    assert!(
        writer
            .send_to_partition(record(51), 0)
            .now_or_never()
            .is_none()
    );
    assert!(matches!(
        writer.send_to_partition(record(52), 2).now_or_never(),
        Some(Err(TopicWriterError::UnknownPartition { partition: 2 }))
    ));
    let other = writer.send_to_partition(record(53), 1).await.unwrap();
    assert_eq!(other.sequence(), 0);
    // Dropping an admitted observer still leaves its record in the flush prefix.
    drop(first);
    let prefix = writer.flush();
    harness.drive(other.confirmed(), true).await.unwrap();
    harness.drive(prefix, true).await.unwrap();
    let next = writer.send_to_partition(record(54), 0).await.unwrap();
    assert_eq!(next.sequence(), 1);
    let next = harness.drive(next.confirmed(), true).await.unwrap();
    assert_eq!(next.partition, 0);
    assert_eq!(next.record.offset, 1);
    assert_eq!(writer.partition_stats(0).unwrap().records, 2);
    assert_eq!(writer.partition_stats(1).unwrap().records, 1);
    let mut ids = harness
        .appends
        .iter()
        .flat_map(|attempt| attempt.2.iter().copied())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, [50, 53, 54].map(|id| MessageId::from_bytes([id; 16])));
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_partition_retry_after_lost_confirmation_keeps_identity_and_offset() {
    tokio::time::timeout(Duration::from_secs(10), lost_confirmation())
        .await
        .unwrap();
}

async fn lost_confirmation() {
    let (mut harness, links, clock, _) = setup_shared_partitions(&[], 2).await;
    let mut writer = harness
        .drive(
            SharedTopicWriter::open(
                &links,
                "orders",
                SharedTopicWriterConfig::new(limits()),
                appends::retry(),
            ),
            true,
        )
        .await
        .unwrap();
    harness.drop_confirmation = Some(writer.producer());
    let pending = writer.send_to_partition(record(60), 1).await.unwrap();
    harness
        .until(true, "confirmation was not dropped", |harness| {
            harness.drop_confirmation.is_none()
        })
        .await;
    assert!(pending.try_confirmed().is_none());
    assert_eq!(pending.partition(), 1);
    assert_eq!(pending.sequence(), 0);
    let healthy = writer.send_to_partition(record(61), 0).await.unwrap();
    harness.drive(healthy.confirmed(), true).await.unwrap();
    clock.advance(Duration::from_secs(1)).unwrap();
    let receipt = harness
        .drive_advancing(&clock, pending.confirmed(), true)
        .await
        .unwrap();
    assert_eq!(receipt.partition, 1);
    assert_eq!(
        receipt.record.partition,
        writer.metadata().partition(1).unwrap().incarnation
    );
    assert_eq!(receipt.record.key.first_sequence, 0);
    assert_eq!(receipt.record.message_id, MessageId::from_bytes([60; 16]));
    assert_eq!(receipt.record.offset, 0);
    let attempts = harness
        .appends
        .iter()
        .filter(|attempt| attempt.2 == [MessageId::from_bytes([60; 16])])
        .collect::<Vec<_>>();
    assert!(attempts.len() >= 2);
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.0 == writer.producer() && attempt.1 == 0)
    );
    let requests = attempts
        .iter()
        .map(|attempt| attempt.3)
        .collect::<BTreeSet<_>>();
    assert_eq!(requests.len(), attempts.len());
    let next = writer.send_to_partition(record(62), 1).await.unwrap();
    assert_eq!(next.sequence(), 1);
    let next = harness.drive(next.confirmed(), true).await.unwrap();
    assert_eq!(next.record.offset, 1);
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}
