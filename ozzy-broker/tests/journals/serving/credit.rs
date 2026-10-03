//! Writer credit between requests of the production shard.

use super::*;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

const REQUESTS: u64 = 8;

#[tokio::test(flavor = "current_thread")]
async fn production_writers_keep_credit_between_requests() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for policy in [Confirmation::LocalDurable, Confirmation::DiskQuorum] {
            scenario(policy).await;
        }
    })
    .await
    .unwrap();
}

async fn scenario(policy: Confirmation) {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let mode = if policy == Confirmation::LocalDurable {
        DeploymentMode::Single
    } else {
        DeploymentMode::Three
    };
    let deployment = deployment(root.path(), mode, policy, 1);
    let sdk = links(&runtime, &deployment[0].0).await;
    let brokers = start_brokers(&runtime, deployment).await;
    // Two writers share the only partition.
    let mut writers = Vec::new();
    for _ in 0..2 {
        writers.push(
            live_many(
                &brokers,
                "open writer",
                SharedTopicWriter::open(
                    &sdk,
                    "orders",
                    SharedTopicWriterConfig::new(limits()),
                    RetryPolicy::default(),
                ),
            )
            .await
            .unwrap(),
        );
    }
    let mut offsets = Vec::new();
    for sequence in 0..REQUESTS {
        let mut pending = Vec::new();
        for writer in &mut writers {
            let id = MessageId::from_bytes(*Uuid::now_v7().as_bytes());
            let admitted = writer
                .send(RecordInput::copy_from_slice(id, b"record"), None)
                .await
                .unwrap();
            assert_eq!(admitted.sequence(), sequence);
            pending.push((admitted, id, writer.producer()));
        }
        for (pending, id, producer) in pending {
            let receipt = live_many(&brokers, "confirm record", pending.confirmed())
                .await
                .unwrap();
            assert_eq!(receipt.record.message_id, id);
            assert_eq!(receipt.record.key.producer_id, producer);
            assert_eq!(receipt.record.key.first_sequence, sequence);
            offsets.push(receipt.record.offset);
        }
    }
    offsets.sort_unstable();
    assert_eq!(offsets, (0..REQUESTS * 2).collect::<Vec<_>>());
    for writer in &writers {
        let stats = writer.partition_stats(0).unwrap();
        assert_eq!(stats.records - (stats.requests - REQUESTS), REQUESTS);
        // Only a writer's first request finds no credit and is sent twice.
        assert!(
            stats.requests <= REQUESTS + 1,
            "{policy:?}: {} requests sent for {REQUESTS} records",
            stats.requests
        );
    }
    for writer in writers {
        live_many(&brokers, "close writer", writer.close())
            .await
            .unwrap();
    }
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn production_writer_confirms_while_its_requests_keep_growing() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = tempfile::tempdir().unwrap();
        let runtime = WriterRuntime::new().unwrap();
        let deployment = deployment(
            root.path(),
            DeploymentMode::Single,
            Confirmation::LocalDurable,
            1,
        );
        let sdk = links(&runtime, &deployment[0].0).await;
        let brokers = start_brokers(&runtime, deployment).await;
        let mut writer = live_many(
            &brokers,
            "open writer",
            SharedTopicWriter::open(
                &sdk,
                "orders",
                SharedTopicWriterConfig::new(DataLimits {
                    max_records: 64,
                    max_parts: 64,
                    ..limits()
                }),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        // Records keep arriving while the first request waits for credit, so
        // every resent request carries more records than the refused one.
        let mut pending = Vec::new();
        let mut requests = None;
        for _ in 0..150 {
            let id = MessageId::from_bytes(*Uuid::now_v7().as_bytes());
            pending.push(
                writer
                    .send(RecordInput::copy_from_slice(id, b"record"), None)
                    .await
                    .unwrap(),
            );
            if requests.is_none() && pending[0].try_confirmed().is_some() {
                requests = Some(writer.partition_stats(0).unwrap().requests);
            }
            tokio::time::sleep(Duration::from_millis(4)).await;
        }
        // The first requests find no credit. Their resend is admitted. One
        // more request can leave before this test sees the confirmation.
        assert!(
            requests.is_some_and(|requests| requests <= 4),
            "{requests:?} requests were sent before the first confirmation"
        );
        for pending in pending {
            live_many(&brokers, "confirm record", pending.confirmed())
                .await
                .unwrap();
        }
        live_many(&brokers, "close writer", writer.close())
            .await
            .unwrap();
        sdk.shutdown().await.unwrap();
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn production_reader_advances_while_partition_writers_keep_sending() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for policy in [Confirmation::LocalDurable, Confirmation::DiskQuorum] {
            reader_during_sustained_writes(policy).await;
        }
    })
    .await
    .unwrap();
}

async fn reader_during_sustained_writes(policy: Confirmation) {
    const READ: u64 = 128;
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let mode = if policy == Confirmation::LocalDurable {
        DeploymentMode::Single
    } else {
        DeploymentMode::Three
    };
    let deployment = deployment(root.path(), mode, policy, 2);
    let sdk = links(&runtime, &deployment[0].0).await;
    let brokers = start_brokers(&runtime, deployment).await;
    let mut reader = live_many(
        &brokers,
        "open saturated reader",
        TopicReader::open(
            sdk.clone(),
            "orders",
            TopicReaderConfig {
                partitions: Some(vec![0]),
                ..TopicReaderConfig::default()
            },
        ),
    )
    .await
    .unwrap();
    let mut opening = Box::pin(reader.next());
    assert!(futures::poll!(opening.as_mut()).is_pending());
    drop(opening);
    let stop = Arc::new(AtomicBool::new(false));
    let admitted = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    let mut writers = Vec::new();
    for number in 0..2 {
        let writer = live_many(
            &brokers,
            "open saturated writer",
            SharedTopicWriter::open(
                &sdk,
                "orders",
                SharedTopicWriterConfig::new(limits()),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        let key = (0_u32..1000)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == number as u32)
            .unwrap();
        let stop = stop.clone();
        let admitted = admitted.clone();
        writers.push(tokio::spawn(keep_partition_busy(
            writer, key, number, stop, admitted,
        )));
    }
    while admitted
        .iter()
        .any(|count| count.load(Ordering::Acquire) < 64)
    {
        tokio::task::yield_now().await;
    }
    for offset in 0..READ {
        let stage = format!(
            "{policy:?} read offset {offset}, admitted {:?}, reader {:?}",
            admitted
                .iter()
                .map(|count| count.load(Ordering::Acquire))
                .collect::<Vec<_>>(),
            reader.stats()
        );
        let record = live_many(&brokers, &stage, reader.next()).await.unwrap();
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset.get(), offset);
        assert_eq!(record.payload.len(), 1);
        assert_eq!(record.payload[0].as_ref(), offset.to_le_bytes());
        assert_eq!(
            record.message_id,
            MessageId::from_bytes(u128::from(offset + 1).to_le_bytes())
        );
    }
    assert!(reader.stats().replayed_records > 0);
    assert!(writers.iter().all(|task| !task.is_finished()));
    stop.store(true, Ordering::Release);
    for task in writers {
        task.await.unwrap();
    }
    live_many(&brokers, "close saturated reader", reader.close())
        .await
        .unwrap();
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

async fn keep_partition_busy(
    mut writer: SharedTopicWriter,
    key: [u8; 4],
    number: usize,
    stop: Arc<AtomicBool>,
    admitted: Arc<[AtomicU64; 2]>,
) {
    const WINDOW: usize = 8;
    let mut pending = VecDeque::new();
    let mut sequence = 0_u64;
    while !stop.load(Ordering::Acquire) {
        let id = MessageId::from_bytes(u128::from(sequence + 1).to_le_bytes());
        pending.push_back(
            writer
                .send(
                    RecordInput::copy_from_slice(id, &sequence.to_le_bytes()),
                    Some(&key),
                )
                .await
                .unwrap(),
        );
        sequence += 1;
        admitted[number].store(sequence, Ordering::Release);
        if pending.len() == WINDOW {
            pending.pop_front().unwrap().confirmed().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for record in pending {
        record.confirmed().await.unwrap();
    }
    writer.close().await.unwrap();
}
