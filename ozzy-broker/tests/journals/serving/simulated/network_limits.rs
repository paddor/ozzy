//! Broker-wide transport limits with the production owners and memory storage.

use super::*;
use ozzy_config::ByteRateLimit;

mod stress;

#[tokio::test(flavor = "current_thread")]
async fn configured_network_limits_leave_inproc_sdk_and_replication_unpaced() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for mode in [DeploymentMode::Single, DeploymentMode::Three] {
            inproc(mode).await;
        }
    })
    .await
    .unwrap();
}

async fn inproc(mode: DeploymentMode) {
    let runtime = WriterRuntime::new().unwrap();
    let root = std::path::PathBuf::from(format!("/ozzy-network-inproc-{}", Uuid::now_v7()));
    let deployment = deployment_with(&root, mode, 2, |deployment| {
        for (name, broker) in &mut deployment.brokers {
            let limit = Some(ByteRateLimit {
                bytes_per_second: 1,
                burst_bytes: 1,
            });
            broker.topology.omq.ingress = limit;
            broker.topology.omq.egress = limit;
            broker.endpoints.admin =
                Some(format!("inproc://network-admin-{}-{name}", Uuid::now_v7()));
        }
    });
    let checked = deployment[0].0.clone();
    let (brokers, _, images) = start_images(runtime.context().clone(), deployment, true).await;
    assert_eq!(
        runtime.context().network_rate_limits(),
        omq_tokio::NetworkRateLimits {
            ingress: Some(omq_tokio::ByteRateLimit::new(1, 1)),
            egress: Some(omq_tokio::ByteRateLimit::new(1, 1)),
        }
    );
    let sdk = links(&runtime, &checked).await;
    exercise(&brokers, &sdk).await;
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    for image in images {
        image.await.unwrap();
    }
}

async fn exercise(brokers: &[Broker], sdk: &BrokerLinks) {
    let mut writer = live_many(
        brokers,
        "open inproc producer with network limits",
        SharedTopicWriter::open(
            sdk,
            "orders",
            SharedTopicWriterConfig::new(limits()),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    let mut pending = Vec::new();
    for id in 1..=8 {
        pending.push(
            writer
                .send_to_partition(
                    RecordInput::copy_from_slice(MessageId::from_bytes([id; 16]), &[id; 512]),
                    u32::from(id % 2),
                )
                .await
                .unwrap(),
        );
    }
    for (index, record) in pending.into_iter().enumerate() {
        let receipt = live_many(brokers, "confirm unpaced inproc record", record.confirmed())
            .await
            .unwrap();
        assert_eq!(receipt.partition, (index as u32 + 1) % 2);
        assert_eq!(receipt.record.offset, index as u64 / 2);
        assert_eq!(receipt.record.policy, writer.metadata().policy());
    }
    let mut reader = live_many(
        brokers,
        "open inproc reader with network limits",
        TopicReader::open(sdk.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    let mut positions = [0; 2];
    for _ in 0..8 {
        let record = live_many(brokers, "read unpaced inproc record", reader.next())
            .await
            .unwrap();
        let partition = record.partition as usize;
        let offset = positions[partition];
        let id = 2 * offset as u8 + if partition == 0 { 2 } else { 1 };
        assert_eq!(record.offset.get(), offset);
        assert_eq!(record.message_id, MessageId::from_bytes([id; 16]));
        assert_eq!(record.payload.len(), 1);
        assert_eq!(record.payload[0].as_ref(), [id; 512]);
        positions[partition] += 1;
    }
    assert_eq!(positions, [4, 4]);
    live_many(brokers, "close unpaced reader", reader.close())
        .await
        .unwrap();
    live_many(brokers, "close unpaced writer", writer.close())
        .await
        .unwrap();
}
