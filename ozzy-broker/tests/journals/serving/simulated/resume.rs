//! Producer attachment across partition-local sequences and leader changes.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn explicit_partition_writes_retry_resume_and_take_over_after_leader_loss() {
    for mode in [DeploymentMode::Single, DeploymentMode::Three] {
        resume_partitions(mode).await;
    }
}

async fn resume_partitions(mode: DeploymentMode) {
    let runtime = WriterRuntime::new().unwrap();
    let root = std::path::PathBuf::from(format!("/ozzy-resume-partitions-{}", Uuid::now_v7()));
    let deployment = deployment(&root, mode, 4);
    let nodes = deployment
        .iter()
        .map(|(checked, _)| {
            ozzy_proto::NodeId::from_bytes(*checked.identity.brokers[&checked.plan.name].as_bytes())
        })
        .collect::<Vec<_>>();
    let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
    let replacement = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
    let (mut brokers, mut controls, mut images) =
        start_images(runtime.context().clone(), deployment, true).await;
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
    let mut next = [0; 4];
    for round in 0..4 {
        partition_wave(&brokers, &mut writer, round + 1, &mut next, 1).await;
    }
    assert_eq!(next, [4, 3, 2, 1]);
    let routes = producer.routes(writer.metadata().clone()).await.unwrap();
    let old_route = routes.route(0).unwrap().unwrap();
    if mode != DeploymentMode::Single {
        let leader = nodes
            .iter()
            .position(|&node| Some(node) == old_route.leader)
            .unwrap();
        // Kill the leader with an admitted APPEND waiting for local storage.
        // Its original observer must confirm the same partition and sequence.
        controls[leader].hold_record_writes(true);
        let pending = writer.send_to_partition(record(99), 0).await.unwrap();
        assert_eq!(pending.sequence(), next[0]);
        live_many(&brokers, "hold explicit APPEND before leader loss", async {
            while controls[leader].held_writes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(pending.try_confirmed().is_none());
        controls[leader].crash(false);
        // Storage failure can surface before or during shutdown.
        let _ = brokers.remove(leader).shutdown().await;
        controls.remove(leader);
        images.remove(leader).await.unwrap();
        let receipt = live_many(
            &brokers,
            "retry explicit APPEND after leader loss",
            pending.confirmed(),
        )
        .await
        .unwrap();
        assert_eq!(receipt.partition, 0);
        assert_eq!(
            receipt.record.partition,
            writer.metadata().partition(0).unwrap().incarnation
        );
        assert_eq!(receipt.record.key.first_sequence, next[0]);
        assert_eq!(receipt.record.offset, next[0]);
        assert_eq!(receipt.record.message_id, MessageId::from_bytes([99; 16]));
        next[0] += 1;
    }
    live_many(&brokers, "close partitioned producer", writer.close())
        .await
        .unwrap();
    let mut resumed = live_many(
        &brokers,
        "resume every partition after leader loss",
        SharedTopicWriter::resume(&producer, "orders", identity, config.clone(), retry),
    )
    .await
    .unwrap();
    partition_wave(&brokers, &mut resumed, 4, &mut next, 1).await;
    if mode != DeploymentMode::Single {
        let current = routes.route(0).unwrap().unwrap();
        assert_ne!(current.leader, old_route.leader);
        assert!(current.view > old_route.view);
    }
    assert_eq!(
        next,
        [5 + u64::from(mode == DeploymentMode::Three), 4, 3, 2]
    );
    reject_foreign_topic_takeover(&brokers, &producer, identity, config.clone(), retry).await;
    let mut takeover = live_many(
        &brokers,
        "take over every partition",
        SharedTopicWriter::takeover(&replacement, "orders", identity, config, retry),
    )
    .await
    .unwrap();
    assert_old_epochs_fenced(&brokers, &mut resumed).await;
    partition_wave(&brokers, &mut takeover, 4, &mut [0; 4], 2).await;
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
    partitions: usize,
    next: &mut [u64; 4],
    epoch: u64,
) {
    let mut pending = Vec::new();
    for partition in 0..partitions {
        pending.push((
            partition,
            writer
                .send_to_partition(record(partition as u8 + 1), partition as u32)
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

async fn assert_old_epochs_fenced(brokers: &[Broker], writer: &mut SharedTopicWriter) {
    for partition in 0..4 {
        let pending = writer
            .send_to_partition(record(200), partition)
            .await
            .unwrap();
        assert!(
            live_many(brokers, "fence old partition epoch", pending.confirmed())
                .await
                .is_err()
        );
    }
}
