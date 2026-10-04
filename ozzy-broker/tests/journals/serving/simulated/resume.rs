//! Producer attachment across partition-local sequences and leader changes.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn resume_and_takeover_preserve_independent_partition_sequences_after_leader_loss() {
    for policy in [
        Confirmation::LocalDurable,
        Confirmation::DiskQuorum,
        Confirmation::ReplicatedPersisting,
    ] {
        resume_partitions(policy).await;
    }
}

async fn resume_partitions(policy: Confirmation) {
    let runtime = WriterRuntime::new().unwrap();
    let root = std::path::PathBuf::from(format!("/ozzy-resume-partitions-{}", Uuid::now_v7()));
    let deployment = deployment(
        &root,
        if policy == Confirmation::LocalDurable {
            DeploymentMode::Single
        } else {
            DeploymentMode::Three
        },
        policy,
        4,
    );
    let nodes = deployment
        .iter()
        .map(|(checked, _)| {
            ozzy_proto::NodeId::from_bytes(*checked.identity.brokers[&checked.plan.name].as_bytes())
        })
        .collect::<Vec<_>>();
    let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
    let replacement = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
    let (mut brokers, _, mut images) = start_images(&runtime, deployment, true).await;
    let config = SharedTopicWriterConfig::new(limits());
    let retry = RetryPolicy::default();
    let mut writer = live_many(
        &brokers,
        "create partitioned producer",
        SharedTopicWriter::open(&producer, "orders", config.clone(), retry),
    )
    .await
    .unwrap();
    let identity = writer.identity();
    let keys = (0..4)
        .map(|partition| {
            (0_u32..1000)
                .map(u32::to_be_bytes)
                .find(|key| writer.metadata().keyed_partition(key).number == partition)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut next = [0; 4];
    for round in 0..4 {
        partition_wave(&brokers, &mut writer, &keys[..=round], &mut next, 1).await;
    }
    assert_eq!(next, [4, 3, 2, 1]);
    let routes = producer.routes(writer.metadata().clone()).unwrap();
    let old_route = routes.route(0).unwrap().unwrap();
    live_many(&brokers, "close partitioned producer", writer.close())
        .await
        .unwrap();
    if policy != Confirmation::LocalDurable {
        let leader = nodes
            .iter()
            .position(|&node| Some(node) == old_route.leader)
            .unwrap();
        brokers.remove(leader).shutdown().await.unwrap();
        images.remove(leader).await.unwrap();
    }
    let mut resumed = live_many(
        &brokers,
        "resume every partition after leader loss",
        SharedTopicWriter::resume(&producer, "orders", identity, config.clone(), retry),
    )
    .await
    .unwrap();
    partition_wave(&brokers, &mut resumed, &keys, &mut next, 1).await;
    if policy != Confirmation::LocalDurable {
        let current = routes.route(0).unwrap().unwrap();
        assert_ne!(current.leader, old_route.leader);
        assert!(current.view > old_route.view);
    }
    assert_eq!(next, [5, 4, 3, 2]);
    reject_foreign_topic_takeover(&brokers, &producer, identity, config.clone(), retry).await;
    let mut takeover = live_many(
        &brokers,
        "take over every partition",
        SharedTopicWriter::takeover(&replacement, "orders", identity, config, retry),
    )
    .await
    .unwrap();
    assert_old_epochs_fenced(&brokers, &mut resumed, &keys).await;
    partition_wave(&brokers, &mut takeover, &keys, &mut [0; 4], 2).await;
    live_many(&brokers, "close partitioned takeover", takeover.close())
        .await
        .unwrap();
    drop(resumed);
    producer.shutdown().await.unwrap();
    replacement.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    for image in images {
        image.await.unwrap();
    }
}

fn record(id: u8) -> RecordInput {
    RecordInput::single(
        MessageId::from_bytes([id; 16]),
        bytes::Bytes::from_static(b"partitioned resume"),
    )
}

async fn partition_wave(
    brokers: &[Broker],
    writer: &mut SharedTopicWriter,
    keys: &[[u8; 4]],
    next: &mut [u64; 4],
    epoch: u64,
) {
    let mut pending = Vec::new();
    for (partition, key) in keys.iter().enumerate() {
        pending.push((
            partition,
            writer
                .send(record(partition as u8 + 1), Some(key))
                .await
                .unwrap(),
        ));
    }
    for (partition, pending) in pending {
        let receipt = live_many(
            brokers,
            "confirm partition resume wave",
            pending.confirmed(),
        )
        .await
        .unwrap();
        assert_eq!(receipt.partition as usize, partition);
        assert_eq!(receipt.record.key.first_sequence, next[partition]);
        assert_eq!(receipt.record.key.producer_epoch, epoch);
        next[partition] += 1;
    }
}

async fn reject_foreign_topic_takeover(
    brokers: &[Broker],
    producer: &BrokerLinks,
    identity: ozzy_runtime::replicated::ProducerIdentity,
    config: SharedTopicWriterConfig,
    retry: RetryPolicy,
) {
    let mut invalid = identity;
    invalid.topic = ozzy_proto::TopicId::from_bytes([99; 16]);
    assert!(matches!(
        live_many(
            brokers,
            "reject takeover for another topic incarnation",
            SharedTopicWriter::takeover(producer, "orders", invalid, config, retry),
        )
        .await,
        Err(ozzy_runtime::replicated::TopicWriterError::Configuration)
    ));
}

async fn assert_old_epochs_fenced(
    brokers: &[Broker],
    writer: &mut SharedTopicWriter,
    keys: &[[u8; 4]],
) {
    for key in keys {
        let pending = writer.send(record(200), Some(key)).await.unwrap();
        assert!(
            live_many(brokers, "fence old partition epoch", pending.confirmed())
                .await
                .is_err()
        );
    }
}
