use super::*;
use crate::replicated::{RecordInput, SharedTopicWriter, SharedTopicWriterConfig, WriterStats};
use ozzy_proto::MessageId;

#[tokio::test(flavor = "current_thread")]
async fn shared_topic_writers_open_and_confirm_independently_on_existing_links() {
    tokio::time::timeout(Duration::from_secs(10), scenario())
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn shared_topic_keeps_sparse_partitions_and_stalled_storage_independent() {
    tokio::time::timeout(Duration::from_secs(10), multiple())
        .await
        .unwrap();
}

#[allow(clippy::too_many_lines)]
async fn multiple() {
    let (mut harness, links, _, authority) = setup_shared_partitions(&[], 2).await;
    let options = || SharedTopicWriterConfig {
        limits: limits(),
        compress_payloads: true,
        batch_target_bytes: 1024,
        max_producers: 2,
        inflight_appends: 1,
    };
    let mut writer = harness
        .drive(
            SharedTopicWriter::open_with_producer(
                &links,
                "orders",
                ProducerId::from_bytes([40; 16]),
                options(),
                appends::retry(),
            ),
            false,
        )
        .await
        .unwrap();
    let mut clone = writer.try_clone().unwrap();
    assert!(writer.try_clone().is_err());
    assert_eq!(writer.metadata().partition_count(), 2);
    assert_eq!(writer.partition_stats(0), Some(WriterStats::default()));
    assert_eq!(writer.partition_stats(1), Some(WriterStats::default()));
    assert_eq!(writer.partition_stats(2), None);
    assert_eq!(writer.partition_stats(u32::MAX), None);
    assert!(
        writer.metadata().partition(0).unwrap().incarnation
            > writer.metadata().partition(1).unwrap().incarnation,
        "test must distinguish numeric order from identity order"
    );
    assert_eq!(harness.openings.len(), 0);
    let key = |number| {
        (0_u32..100)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == number)
            .unwrap()
    };
    let first_key = key(0);
    let second_key = key(1);
    let record = |id| RecordInput::copy_from_slice(MessageId::from_bytes([id; 16]), b"record");
    let healthy = writer.send(record(50), Some(&second_key)).await.unwrap();
    assert_eq!(healthy.partition(), 1);
    let healthy = harness.drive(healthy.confirmed(), true).await.unwrap();
    assert_eq!(healthy.record.offset, 0);
    assert_eq!(healthy.record.key.first_sequence, 0);
    assert_eq!(harness.openings.len(), 1, "sparse partition opened early");
    let blocked = writer.send(record(51), Some(&first_key)).await.unwrap();
    assert_eq!(blocked.partition(), 0);
    for _ in 0..10000 {
        harness.pump(false);
        let jobs = harness.controller.jobs();
        if !jobs.is_empty() {
            harness.held_io.extend(jobs.into_iter().map(|(id, _)| id));
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(!harness.held_io.is_empty());
    assert!(blocked.try_confirmed().is_none());
    assert_eq!(writer.partition_stats(0), Some(WriterStats::default()));
    let a = writer.send(record(52), Some(&second_key)).await.unwrap();
    let b = clone.send(record(53), Some(&second_key)).await.unwrap();
    assert_eq!(a.sequence(), 1);
    assert_eq!(b.sequence(), 2);
    while writer.partition_stats(1).unwrap().records < 2 {
        harness.pump(false);
        tokio::task::yield_now().await;
    }
    assert!(a.try_confirmed().is_none());
    assert!(b.try_confirmed().is_none());
    let captured = writer.flush();
    let mut captured = pin!(captured);
    assert!(futures::poll!(captured.as_mut()).is_pending());
    let (a, b) = harness
        .drive(futures::future::join(a.confirmed(), b.confirmed()), true)
        .await;
    for (receipt, offset) in [(a.unwrap(), 1), (b.unwrap(), 2)] {
        assert_eq!(receipt.partition, 1);
        assert_eq!(receipt.record.offset, offset);
        assert_eq!(receipt.record.key.producer_id, writer.producer());
        assert_eq!(receipt.record.policy, Policy::LocalDurable);
    }
    let active = writer.partition_stats(1).unwrap();
    assert_eq!(active.records, 3);
    assert!((1..=3).contains(&active.requests));
    assert!((1..=3).contains(&active.max_records));
    assert_eq!(active.max_inflight_appends, 1);
    assert_eq!(clone.partition_stats(1), Some(active));
    assert_eq!(writer.partition_stats(0), Some(WriterStats::default()));
    assert!(
        blocked.try_confirmed().is_none(),
        "stalled storage was completed"
    );
    assert!(futures::poll!(captured.as_mut()).is_pending());
    // Two more idle partition owners fill the global four-writer bound.
    let idle = harness
        .drive(
            SharedTopicWriter::open(&links, "orders", options(), appends::retry()),
            true,
        )
        .await
        .unwrap();
    assert_ne!(idle.producer(), writer.producer());
    assert!(
        harness
            .drive(
                SharedTopicWriter::open(&links, "orders", options(), appends::retry()),
                true
            )
            .await
            .is_err()
    );
    assert_eq!(
        harness.openings.len(),
        2,
        "unused owner sent a producer opening"
    );
    harness.drive(idle.close(), true).await.unwrap();
    for id in std::mem::take(&mut harness.held_io) {
        harness.controller.execute(id, Effect::Normal).unwrap();
        harness.controller.deliver(id).unwrap();
    }
    let recovered = harness.drive(blocked.confirmed(), true).await.unwrap();
    assert_eq!(recovered.partition, 0);
    assert_eq!(recovered.record.key.producer_id, writer.producer());
    assert_eq!(recovered.record.key.first_sequence, 0);
    assert_eq!(recovered.record.offset, 0);
    harness.drive(captured, true).await.unwrap();
    drop(clone);
    let keyless = writer.send(record(54), None).await.unwrap();
    assert_eq!(keyless.partition(), 1);
    assert_eq!(keyless.sequence(), 3);
    harness.drive(keyless.confirmed(), true).await.unwrap();
    let keyless = writer.send(record(55), None).await.unwrap();
    assert_eq!(keyless.partition(), 0);
    assert_eq!(keyless.sequence(), 1);
    harness.drive(keyless.confirmed(), true).await.unwrap();
    assert_eq!(links.socket_count(), 2);
    assert_eq!(links.session(authority.primary), harness.session);
    // Drained unused owners returned their aggregate reservation.
    let idle = harness
        .drive(
            SharedTopicWriter::open(&links, "orders", options(), appends::retry()),
            true,
        )
        .await
        .unwrap();
    harness.drive(idle.close(), true).await.unwrap();
    harness.drive(writer.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

async fn scenario() {
    let (mut harness, links, _, authority) = setup_shared_with_writers(&[]).await;
    let options = || SharedTopicWriterConfig {
        limits: limits(),
        compress_payloads: true,
        batch_target_bytes: 1024,
        max_producers: 1,
        inflight_appends: 1,
    };
    let (a, b) = harness
        .drive(
            futures::future::join(
                SharedTopicWriter::open(&links, "orders", options(), appends::retry()),
                SharedTopicWriter::open(&links, "orders", options(), appends::retry()),
            ),
            false,
        )
        .await;
    let mut a = a.unwrap();
    let mut b = b.unwrap();
    assert_eq!(a.metadata(), b.metadata());
    assert_ne!(a.producer(), b.producer());
    assert_eq!(harness.openings.len(), 0);
    assert!(harness.watch.is_none());
    let session = links.session(authority.primary).unwrap();
    let first = a
        .send(
            RecordInput::copy_from_slice(MessageId::from_bytes([50; 16]), b"a"),
            Some(b"customer"),
        )
        .await
        .unwrap();
    let second = b
        .send(
            RecordInput::copy_from_slice(MessageId::from_bytes([60; 16]), b"b"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(first.topic(), a.metadata().id());
    assert_eq!(
        first.partition(),
        a.metadata().keyed_partition(b"customer").number
    );
    assert_eq!(first.sequence(), 0);
    assert_eq!(second.sequence(), 0);
    let (first, second) = harness
        .drive(
            futures::future::join(first.confirmed(), second.confirmed()),
            true,
        )
        .await;
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.topic, a.metadata().id());
    assert_eq!(second.topic, b.metadata().id());
    assert_eq!(first.partition, 0);
    assert_eq!(second.partition, 0);
    assert_eq!(first.record.key.producer_id, a.producer());
    assert_eq!(second.record.key.producer_id, b.producer());
    assert_eq!(first.record.key.first_sequence, 0);
    assert_eq!(second.record.key.first_sequence, 0);
    let mut offsets = [first.record.offset, second.record.offset];
    offsets.sort_unstable();
    assert_eq!(offsets, [0, 1]);
    assert_eq!(first.record.policy, Policy::LocalDurable);
    assert_eq!(second.record.policy, Policy::LocalDurable);
    assert_eq!(links.session(authority.primary), Some(session));
    assert_eq!(links.socket_count(), 2);
    harness.drive(a.flush(), true).await.unwrap();
    harness.drive(a.close(), true).await.unwrap();
    harness.drive(b.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}
