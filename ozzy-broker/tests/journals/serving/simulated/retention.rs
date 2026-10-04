//! Retained checkpoint recovery through real brokers and SDKs, with memory files.

use super::*;
use ozzy_config::TopicRetention;
use ozzy_journal_segment::{Manifest, MetadataLimits, decode_current, decode_manifest};
use std::path::PathBuf;

pub(super) fn selected(image: &Image, root: &Path) -> Manifest {
    let current = decode_current(
        image
            .bytes(&root.join("CURRENT"), false)
            .unwrap_or_else(|error| panic!("selected {}: {error}", root.display())),
    )
    .unwrap();
    decode_manifest(
        image
            .bytes(
                &root.join(format!("MANIFEST.{}", current.generation)),
                false,
            )
            .unwrap(),
        MetadataLimits::default(),
    )
    .unwrap()
}

async fn append_one(brokers: &[Broker], writer: &mut SharedTopicWriter, number: u128) {
    let pending = writer
        .send(
            RecordInput::single(
                MessageId::from_bytes(number.to_be_bytes()),
                bytes::Bytes::from(vec![number as u8; 512]),
            ),
            None,
        )
        .await
        .unwrap();
    live_many(
        brokers,
        "confirm retained recovery record",
        pending.confirmed(),
    )
    .await
    .unwrap();
}

async fn read_retained(brokers: &[Broker], consumer: &BrokerLinks, end: u64) {
    let mut reader = live_many(
        brokers,
        "open retained payload reader",
        TopicReader::open(consumer.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    let mut next = None;
    loop {
        let record = live_many(brokers, "read retained payload", reader.next())
            .await
            .unwrap();
        let offset = record.offset.get();
        if let Some(expected) = next {
            assert_eq!(offset, expected);
        } else {
            assert!(offset > 0 && offset < end - 1);
        }
        let number = u128::from(offset + 1);
        assert_eq!(
            record.message_id,
            MessageId::from_bytes(number.to_be_bytes())
        );
        assert_eq!(record.payload.len(), 1);
        assert_eq!(record.payload[0].as_ref(), &[number as u8; 512]);
        next = Some(offset + 1);
        if offset + 1 == end {
            break;
        }
    }
    live_many(brokers, "close retained payload reader", reader.close())
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn lagging_follower_recovers_retained_checkpoint_then_supplies_the_required_quorum() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let runtime = WriterRuntime::new().unwrap();
        let root = PathBuf::from(format!("/ozzy-retained-recovery-{}", Uuid::now_v7()));
        let deployment = deployment_with(&root, DeploymentMode::Three, policy, 1, |config| {
            let topic = config.topics.get_mut("orders").unwrap();
            topic.retention = TopicRetention {
                max_age_secs: None,
                max_bytes: Some(1024 * 1024),
            };
        });
        let restart = deployment[2].clone();
        let directory = restart.0.plan.partitions[0].directory.clone();
        let recovered_node = ozzy_proto::NodeId::from_bytes(
            *restart.0.identity.brokers[&restart.0.plan.name].as_bytes(),
        );
        let leader_directory = deployment[0].0.plan.partitions[0].directory.clone();
        let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let consumer = role_links(&runtime, &deployment[0].0, handshake::CONSUMER).await;
        let (mut brokers, mut controls, mut images) =
            start_images(&runtime, deployment, true).await;
        let mut writer = live_many(
            &brokers,
            "open retained recovery writer",
            SharedTopicWriter::open(
                &producer,
                "orders",
                SharedTopicWriterConfig::new(limits()),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        append_one(&brokers, &mut writer, 1).await;
        brokers.remove(2).shutdown().await.unwrap();
        controls.remove(2);
        let old_image = images.remove(2).await.unwrap();
        for number in 2..=600 {
            append_one(&brokers, &mut writer, number).await;
        }
        tokio::time::sleep(Duration::from_secs(4)).await;
        let leader_image = controls[0].image().await;
        assert!(
            selected(&leader_image, &leader_directory)
                .checkpoint
                .is_some()
        );
        assert!(selected(&old_image, &directory).checkpoint.is_none());
        for round in 0..2 {
            let (checked, local) = restart.clone();
            let (restarted, control, task) =
                start_image(&runtime, checked, local, true, old_image.clone()).await;
            brokers.push(restarted);
            controls.push(control);
            images.push(task);
            live_many(&brokers, "recover lagging retained checkpoint", async {
                loop {
                    let image = controls[2].image().await;
                    if selected(&image, &directory).checkpoint.is_some()
                        && ozzy_replication::ConfigurationRecord::decode(
                            image
                                .bytes(&directory.join("CONFIGURATION"), false)
                                .unwrap(),
                        )
                        .is_ok()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            if round == 0 {
                brokers.remove(2).shutdown().await.unwrap();
                controls.remove(2);
                images.remove(2).await.unwrap();
            }
        }
        brokers.remove(1).shutdown().await.unwrap();
        controls.remove(1);
        images.remove(1).await.unwrap();
        append_one(&brokers, &mut writer, 601).await;
        assert_recovered_leader(&producer, &writer, recovered_node);
        read_retained(&brokers, &consumer, 601).await;
        writer.close().await.unwrap();
        producer.shutdown().await.unwrap();
        consumer.shutdown().await.unwrap();
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
        for image in images {
            image.await.unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn disk_quorum_cluster_restart_with_an_ancient_replica_preserves_retained_history() {
    restart_with_ancient_replica(Confirmation::DiskQuorum).await;
}

#[tokio::test(flavor = "current_thread")]
async fn replicated_persisting_cluster_restart_with_an_ancient_replica_preserves_retained_history()
{
    restart_with_ancient_replica(Confirmation::ReplicatedPersisting).await;
}

async fn restart_with_ancient_replica(policy: Confirmation) {
    let runtime = WriterRuntime::new().unwrap();
    let root = PathBuf::from(format!("/ozzy-ancient-restart-{}", Uuid::now_v7()));
    let deployment = deployment_with(&root, DeploymentMode::Three, policy, 1, |config| {
        config.topics.get_mut("orders").unwrap().retention = TopicRetention {
            max_age_secs: None,
            max_bytes: Some(1024 * 1024),
        };
    });
    let restart = deployment.clone();
    let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
    let (mut brokers, mut controls, mut images) = start_images(&runtime, deployment, true).await;
    let config = SharedTopicWriterConfig::new(limits());
    let mut writer = live_many(
        &brokers,
        "open ancient restart writer",
        SharedTopicWriter::open(&producer, "orders", config.clone(), RetryPolicy::default()),
    )
    .await
    .unwrap();
    let identity = writer.identity();
    append_one(&brokers, &mut writer, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    brokers.remove(0).shutdown().await.unwrap();
    controls.remove(0);
    let ancient = images.remove(0).await.unwrap();
    for number in 2..=600 {
        append_one(&brokers, &mut writer, number).await;
    }
    writer.close().await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    let mut stopped = Vec::new();
    for image in images {
        stopped.push(image.await.unwrap());
    }
    stopped.insert(0, ancient);
    let ancient_directory = &restart[0].0.plan.partitions[0].directory;
    let (evidence, offset) = if policy == Confirmation::DiskQuorum {
        ("DURABLE", 72)
    } else {
        ("MEMORY_VOTING", 88)
    };
    let ancient_tail = u64::from_be_bytes(
        stopped[0]
            .bytes(&ancient_directory.join(evidence), false)
            .unwrap()[offset..offset + 8]
            .try_into()
            .unwrap(),
    );
    assert!(ancient_tail > 0);
    for (image, (checked, _)) in stopped[1..].iter().zip(&restart[1..]) {
        let manifest = selected(image, &checked.plan.partitions[0].directory);
        assert!(manifest.checkpoint.is_some());
        assert!(manifest.segments[0].first_chain.next_op_number() > ancient_tail + 1);
    }
    let mut brokers = Vec::new();
    let mut controls = Vec::new();
    let mut images = Vec::new();
    let mut stopped = restart
        .into_iter()
        .zip(stopped)
        .map(Some)
        .collect::<Vec<_>>();
    // The ancient copy reports before the second healthy copy can join the
    // candidate's quorum. Healthy history must remain eligible throughout.
    for index in [0, 2, 1] {
        let ((checked, local), image) = stopped[index].take().unwrap();
        let (broker, control, image) = start_image(&runtime, checked, local, true, image).await;
        brokers.push(broker);
        controls.push(control);
        images.push(image);
        if index == 2 {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    let mut writer = live_many(
        &brokers,
        "resume with ancient replica after complete cluster restart",
        SharedTopicWriter::resume(
            &producer,
            "orders",
            identity,
            config,
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    append_one(&brokers, &mut writer, 601).await;
    writer.close().await.unwrap();
    producer.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    for image in images {
        image.await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn retained_history_survives_complete_cluster_restart_and_producer_resume() {
    use ozzy_proto::Offset;
    use ozzy_runtime::replicated::ReaderStart;
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let runtime = WriterRuntime::new().unwrap();
        let root = PathBuf::from(format!("/ozzy-retained-restart-{}", Uuid::now_v7()));
        let deployment = deployment_with(&root, DeploymentMode::Three, policy, 1, |config| {
            config.topics.get_mut("orders").unwrap().retention = TopicRetention {
                max_age_secs: None,
                max_bytes: Some(1024 * 1024),
            };
        });
        let restart = deployment.clone();
        let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let consumer = role_links(&runtime, &deployment[0].0, handshake::CONSUMER).await;
        let (brokers, controls, images) = start_images(&runtime, deployment, true).await;
        let config = SharedTopicWriterConfig::new(limits());
        let mut writer = live_many(
            &brokers,
            "open full retained restart writer",
            SharedTopicWriter::open(&producer, "orders", config.clone(), RetryPolicy::default()),
        )
        .await
        .unwrap();
        let identity = writer.identity();
        for number in 1..=600 {
            append_one(&brokers, &mut writer, number).await;
        }
        writer.close().await.unwrap();
        let tail = retained_tail(&brokers, &controls, &restart).await;
        let mut reader = live_many(
            &brokers,
            "resolve time seek before disconnect",
            TopicReader::open(
                consumer.clone(),
                "orders",
                TopicReaderConfig {
                    start: ReaderStart::Timestamp(0),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        let mut first = live_many(&brokers, "read before disconnect", reader.next())
            .await
            .unwrap();
        assert!(first.offset.get() > 0);
        // Copies can roll at different operations. A future leader may advance
        // the earliest retained floor, so continue from their common active tail.
        while first.offset.get() < tail {
            first = live_many(&brokers, "advance into retained tail", reader.next())
                .await
                .unwrap();
        }
        let floor = first.offset.get();
        assert!(floor > 0 && floor + 5 < 600);
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
        let mut restarted = Vec::new();
        let mut restarted_images = Vec::new();
        for ((checked, local), image) in restart.into_iter().zip(images) {
            let image = image.await.unwrap();
            let (broker, _, image) = start_image(&runtime, checked, local, true, image).await;
            restarted.push(broker);
            restarted_images.push(image);
        }
        let mut writer = resumed_writer(&restarted, &producer, identity, config).await;
        let next = live_many(
            &restarted,
            "continue resolved seek after reconnect",
            reader.next(),
        )
        .await
        .unwrap();
        assert_eq!(next.offset, Offset::new(floor + 1));
        live_many(&restarted, "close reconnected seek", reader.close())
            .await
            .unwrap();
        read_cold_seeks(&restarted, &consumer, identity.topic, floor).await;
        let mut latest = parked_latest(&restarted, &consumer).await;
        append_one(&restarted, &mut writer, 601).await;
        let record = live_many(&restarted, "deliver after latest", latest.next())
            .await
            .unwrap();
        assert_eq!(record.offset, Offset::new(600));
        latest.close().await.unwrap();
        read_retained(&restarted, &consumer, 601).await;
        writer.close().await.unwrap();
        producer.shutdown().await.unwrap();
        consumer.shutdown().await.unwrap();
        for broker in restarted {
            broker.shutdown().await.unwrap();
        }
        for image in restarted_images {
            image.await.unwrap();
        }
    }
}

async fn retained_tail(
    brokers: &[Broker],
    controls: &[std::sync::Arc<ozzy_sim::broker::Control>],
    deployment: &[(CheckedConfig, ozzy_config::BrokerIdentity)],
) -> u64 {
    live_many(
        brokers,
        "checkpoint retained history on every copy",
        async {
            loop {
                let mut tail = 0;
                let mut ready = true;
                for (control, (checked, _)) in controls.iter().zip(deployment) {
                    let image = control.image().await;
                    let manifest = selected(&image, &checked.plan.partitions[0].directory);
                    ready &= manifest.checkpoint.is_some();
                    // This fixture appends one record per operation. Control
                    // operations only increase this conservative offset bound.
                    tail = tail.max(
                        manifest
                            .segments
                            .last()
                            .unwrap()
                            .first_chain
                            .next_op_number()
                            - 1,
                    );
                }
                if ready {
                    assert!(tail + 5 < 600);
                    return tail;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
}

async fn read_cold_seeks(
    brokers: &[Broker],
    consumer: &BrokerLinks,
    topic: ozzy_proto::TopicId,
    floor: u64,
) {
    use ozzy_proto::Offset;
    use ozzy_runtime::replicated::{ReaderStart, TopicCheckpoint};
    for start in [
        ReaderStart::record_id(
            0,
            MessageId::from_bytes(u128::from(floor + 6).to_be_bytes()),
        ),
        ReaderStart::Checkpoint(TopicCheckpoint {
            topic,
            positions: vec![(0, Offset::new(floor + 5))],
        }),
    ] {
        let mut reader = live_many(
            brokers,
            "seek cold retained segments",
            TopicReader::open(
                consumer.clone(),
                "orders",
                TopicReaderConfig {
                    start,
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        let record = live_many(brokers, "read cold seek payload", reader.next())
            .await
            .unwrap();
        assert_eq!(record.offset, Offset::new(floor + 5));
        assert_eq!(record.payload[0].as_ref(), &[(floor + 6) as u8; 512]);
        live_many(brokers, "close cold seek", reader.close())
            .await
            .unwrap();
    }
}

async fn parked_latest(brokers: &[Broker], consumer: &BrokerLinks) -> TopicReader {
    use ozzy_proto::Offset;
    use ozzy_runtime::replicated::ReaderStart;
    let mut latest = live_many(
        brokers,
        "resolve latest after restart",
        TopicReader::open(
            consumer.clone(),
            "orders",
            TopicReaderConfig {
                start: ReaderStart::Latest,
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    // Poll subscription setup while the confirmed end is still 600.
    live_many(brokers, "park latest at confirmed end", async {
        loop {
            {
                let mut next = std::pin::pin!(latest.next());
                assert!(futures::poll!(next.as_mut()).is_pending());
            }
            if latest.checkpoint().positions == [(0, Offset::new(600))] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    latest
}

async fn resumed_writer(
    brokers: &[Broker],
    producer: &BrokerLinks,
    identity: ozzy_runtime::replicated::ProducerIdentity,
    config: SharedTopicWriterConfig,
) -> SharedTopicWriter {
    live_many(
        brokers,
        "resume retained producer",
        SharedTopicWriter::resume(producer, "orders", identity, config, RetryPolicy::default()),
    )
    .await
    .unwrap()
}

fn assert_recovered_leader(
    producer: &BrokerLinks,
    writer: &SharedTopicWriter,
    recovered_node: ozzy_proto::NodeId,
) {
    assert_eq!(
        producer
            .routes(writer.metadata().clone())
            .unwrap()
            .route(0)
            .unwrap()
            .unwrap()
            .leader,
        Some(recovered_node),
        "payload reads must come from the recovered replica"
    );
}
