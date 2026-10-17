//! Numeric placement shares one producer pipeline with automatic routing.

use super::*;
use ozzy_runtime::replicated::{SharedTopicPendingRecord, TopicWriterError};
use std::path::PathBuf;

#[tokio::test(flavor = "current_thread")]
async fn explicit_partition_writes_mix_with_keys_clones_and_other_producers_in_all_modes() {
    for mode in [DeploymentMode::Single, DeploymentMode::Three] {
        mixed(mode).await;
    }
}

async fn mixed(mode: DeploymentMode) {
    let runtime = WriterRuntime::new().unwrap();
    let root = PathBuf::from(format!("/ozzy-explicit-partitions-{}", Uuid::now_v7()));
    let deployment = deployment(&root, mode, 4);
    let sdk = links(&runtime, &deployment[0].0).await;
    let (brokers, _) = Box::pin(start_controlled(&runtime, deployment, true)).await;
    let config = SharedTopicWriterConfig {
        max_producers: 2,
        ..SharedTopicWriterConfig::new(limits())
    };
    let mut writer = live_many(
        &brokers,
        "open explicitly routed producer",
        SharedTopicWriter::open(&sdk, "orders", config.clone(), RetryPolicy::default()),
    )
    .await
    .unwrap();
    let mut clone = writer.try_clone().unwrap();
    for partition in [4, u32::MAX] {
        assert!(matches!(
            writer.send_to_partition(record(99), partition).await,
            Err(TopicWriterError::UnknownPartition { partition: rejected }) if rejected == partition
        ));
    }
    let first = writer.send(record(1), None).await.unwrap();
    confirm(&brokers, &writer, first, (0, 0, 0, 1)).await;
    let key = (0_u32..1000)
        .map(u32::to_be_bytes)
        .find(|key| writer.metadata().keyed_partition(key).number == 0)
        .unwrap();
    let explicit = writer.send_to_partition(record(2), 0).await.unwrap();
    let keyed = clone.send(record(3), Some(&key)).await.unwrap();
    let last = clone.send_to_partition(record(4), 3).await.unwrap();
    let captured = writer.flush();
    confirm(&brokers, &writer, explicit, (0, 1, 1, 2)).await;
    confirm(&brokers, &writer, keyed, (0, 2, 2, 3)).await;
    confirm(&brokers, &writer, last, (3, 0, 0, 4)).await;
    live_many(&brokers, "flush mixed routes", captured)
        .await
        .unwrap();
    let keyless = writer.send(record(5), None).await.unwrap();
    confirm(&brokers, &writer, keyless, (1, 0, 0, 5)).await;
    let mut other = live_many(
        &brokers,
        "open independent producer",
        SharedTopicWriter::open(&sdk, "orders", config, RetryPolicy::default()),
    )
    .await
    .unwrap();
    assert_ne!(other.producer(), writer.producer());
    let independent = other.send_to_partition(record(6), 0).await.unwrap();
    confirm(&brokers, &other, independent, (0, 0, 3, 6)).await;
    read_back(&brokers, &sdk).await;
    assert_eq!(
        sdk.socket_count(),
        if mode == DeploymentMode::Single { 3 } else { 5 }
    );
    drop(clone);
    live_many(&brokers, "close mixed producer", writer.close())
        .await
        .unwrap();
    live_many(&brokers, "close independent producer", other.close())
        .await
        .unwrap();
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

fn record(id: u8) -> RecordInput {
    RecordInput::multipart(
        MessageId::from_bytes([id; 16]),
        [
            bytes::Bytes::from(vec![id; 256]),
            bytes::Bytes::new(),
            bytes::Bytes::copy_from_slice(&[id, 0]),
        ],
    )
}

async fn confirm(
    brokers: &[Broker],
    writer: &SharedTopicWriter,
    pending: SharedTopicPendingRecord,
    expected: (u32, u64, u64, u8),
) {
    let (partition, sequence, offset, id) = expected;
    assert_eq!(pending.topic(), writer.metadata().id());
    assert_eq!(pending.partition(), partition);
    assert_eq!(pending.sequence(), sequence);
    let receipt = live_many(
        brokers,
        "confirm mixed partition record",
        pending.confirmed(),
    )
    .await
    .unwrap();
    assert_eq!(receipt.topic, writer.metadata().id());
    assert_eq!(receipt.partition, partition);
    assert_eq!(
        receipt.record.partition,
        writer.metadata().partition(partition).unwrap().incarnation
    );
    assert_eq!(receipt.record.key.producer_id, writer.producer());
    assert_eq!(receipt.record.key.producer_epoch, 1);
    assert_eq!(receipt.record.key.first_sequence, sequence);
    assert_eq!(receipt.record.message_id, MessageId::from_bytes([id; 16]));
    assert_eq!(receipt.record.offset, offset);
    assert_eq!(receipt.record.policy, writer.metadata().policy());
}

async fn read_back(brokers: &[Broker], sdk: &BrokerLinks) {
    let expected: [&[u8]; 4] = [&[1, 2, 3, 6], &[5], &[], &[4]];
    let mut positions = [0; 4];
    let mut reader = live_many(
        brokers,
        "read explicitly selected partitions",
        TopicReader::open(sdk.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    for _ in 0..6 {
        let record = live_many(brokers, "read exact mixed record", reader.next())
            .await
            .unwrap();
        let partition = record.partition as usize;
        let offset = positions[partition];
        let id = expected[partition][offset];
        assert_eq!(record.offset.get(), offset as u64);
        assert_eq!(record.message_id, MessageId::from_bytes([id; 16]));
        assert_eq!(record.payload.len(), 3);
        assert_eq!(record.payload[0].as_ref(), [id; 256]);
        assert!(record.payload[1].is_empty());
        assert_eq!(record.payload[2].as_ref(), [id, 0]);
        positions[partition] += 1;
    }
    assert_eq!(positions, [4, 1, 0, 1]);
    live_many(brokers, "close mixed reader", reader.close())
        .await
        .unwrap();
}
