//! Memory-only storage driving the production broker and OMQ inproc owners.

use super::*;
use ozzy_io::simulation::Image;
use ozzy_sim::broker::{
    provision, start_controlled, start_image, start_image_selected, start_images,
};

mod churn;
mod resume;
mod retention;

mod tests {
    use super::*;
    use futures::FutureExt;

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "one SDK seek schedule across all confirmation policies"
    )]
    async fn history_seek_resolves_id_policy_and_timestamp_over_inproc_without_disk_io() {
        use ozzy_runtime::replicated::{IdPolicy, ReaderStart, TopicReaderError};
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let runtime = WriterRuntime::new().unwrap();
            let deployment = fixture(policy);
            let producer = role_links_with_config(
                &runtime,
                &deployment[0].0,
                handshake::PRODUCER,
                limits(),
                |config| {
                    let mut node = [61; 16];
                    node[0] = 0;
                    config.local = NodeId::from_bytes(node);
                },
            )
            .await;
            let consumer = role_links(&runtime, &deployment[0].0, handshake::CONSUMER).await;
            let (brokers, _) = Box::pin(start_controlled(&runtime, deployment, true)).await;
            let mut writer = live_many(
                &brokers,
                "open seek fixture",
                SharedTopicWriter::open(
                    &producer,
                    "orders",
                    SharedTopicWriterConfig::new(limits()),
                    RetryPolicy::default(),
                ),
            )
            .await
            .unwrap();
            let duplicate = MessageId::from_bytes([37; 16]);
            for id in [duplicate, MessageId::from_bytes([38; 16]), duplicate] {
                let pending = writer
                    .send(
                        RecordInput::single(id, bytes::Bytes::from_static(b"seek fixture")),
                        None,
                    )
                    .await
                    .unwrap();
                live_many(&brokers, "confirm seek fixture", pending.confirmed())
                    .await
                    .unwrap();
            }
            for (start, expected) in [
                (ReaderStart::Timestamp(0), 0),
                (
                    ReaderStart::RecordId {
                        partition: 0,
                        id: duplicate,
                        policy: IdPolicy::FirstRetained,
                    },
                    0,
                ),
                (
                    ReaderStart::RecordId {
                        partition: 0,
                        id: duplicate,
                        policy: IdPolicy::LastRetained,
                    },
                    2,
                ),
                (
                    ReaderStart::record_id(0, MessageId::from_bytes([38; 16])),
                    1,
                ),
            ] {
                let mut reader = live_many(
                    &brokers,
                    "open history seek",
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
                let record = live_many(&brokers, "resolve and read history seek", reader.next())
                    .await
                    .unwrap();
                assert_eq!(record.offset, ozzy_proto::Offset::new(expected));
                live_many(&brokers, "close history seek", reader.close())
                    .await
                    .unwrap();
            }
            for id in [duplicate, MessageId::from_bytes([39; 16])] {
                let mut reader = live_many(
                    &brokers,
                    "open rejected ID seek",
                    TopicReader::open(
                        consumer.clone(),
                        "orders",
                        TopicReaderConfig {
                            start: ReaderStart::record_id(0, id),
                            ..Default::default()
                        },
                    ),
                )
                .await
                .unwrap();
                let error = live_many(&brokers, "reject ambiguous or missing ID", reader.next())
                    .await
                    .unwrap_err();
                if id == duplicate {
                    assert!(
                        matches!(error, TopicReaderError::AmbiguousRecordId { first, last, .. }
                        if first.get() == 0 && last.get() == 2),
                        "{error:?}"
                    );
                } else {
                    assert!(
                        matches!(error, TopicReaderError::RecordNotFound { .. }),
                        "{error:?}"
                    );
                }
                live_many(&brokers, "close rejected ID seek", reader.close())
                    .await
                    .unwrap();
            }
            live_many(&brokers, "close seek writer", writer.close())
                .await
                .unwrap();
            producer.shutdown().await.unwrap();
            consumer.shutdown().await.unwrap();
            for broker in brokers {
                broker.shutdown().await.unwrap();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn identity_resume_and_takeover_use_real_inproc_brokers_without_disk_io() {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let runtime = WriterRuntime::new().unwrap();
            let deployment = fixture(policy);
            let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
            let (brokers, _) = Box::pin(start_controlled(&runtime, deployment, true)).await;
            let config = SharedTopicWriterConfig::new(limits());
            let retry = RetryPolicy::default();
            let mut first = live_many(
                &brokers,
                "fresh producer",
                SharedTopicWriter::open(&producer, "orders", config.clone(), retry),
            )
            .await
            .unwrap();
            let identity = first.identity();
            let send = |n| {
                RecordInput::single(
                    MessageId::from_bytes([n; 16]),
                    bytes::Bytes::from_static(b"resume record"),
                )
            };
            let pending = first.send(send(31), None).await.unwrap();
            let receipt = live_many(&brokers, "first record", pending.confirmed())
                .await
                .unwrap();
            assert_eq!(receipt.record.key.first_sequence, 0);
            live_many(&brokers, "close original", first.close())
                .await
                .unwrap();
            let mut resumed = live_many(
                &brokers,
                "resume identity",
                SharedTopicWriter::resume(&producer, "orders", identity, config.clone(), retry),
            )
            .await
            .unwrap();
            assert_eq!(resumed.identity(), identity);
            let pending = resumed.send(send(32), None).await.unwrap();
            let receipt = live_many(&brokers, "resumed record", pending.confirmed())
                .await
                .unwrap();
            assert_eq!(receipt.record.key.first_sequence, 1);
            let mut takeover = live_many(
                &brokers,
                "take over live writer",
                SharedTopicWriter::takeover(&producer, "orders", identity, config, retry),
            )
            .await
            .unwrap();
            let pending = resumed.send(send(33), None).await.unwrap();
            assert!(
                live_many(&brokers, "old writer fenced", pending.confirmed())
                    .await
                    .is_err()
            );
            let pending = takeover.send(send(34), None).await.unwrap();
            let receipt = live_many(&brokers, "new epoch record", pending.confirmed())
                .await
                .unwrap();
            assert_eq!(receipt.record.key.first_sequence, 0);
            assert_eq!(receipt.record.key.producer_epoch, 2);
            live_many(&brokers, "close takeover", takeover.close())
                .await
                .unwrap();
            drop(resumed);
            producer.shutdown().await.unwrap();
            for broker in brokers {
                broker.shutdown().await.unwrap();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn producer_owner_crash_resumes_confirmed_prefix_without_a_local_outbox() {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let transport = WriterRuntime::new().unwrap();
            let original = WriterRuntime::with_context(transport.context().clone()).unwrap();
            let deployment = fixture(policy);
            let first_links = role_links(&original, &deployment[0].0, handshake::PRODUCER).await;
            let replacement = WriterRuntime::with_context(transport.context().clone()).unwrap();
            let links = role_links(&replacement, &deployment[0].0, handshake::PRODUCER).await;
            let (brokers, _) = Box::pin(start_controlled(&transport, deployment, true)).await;
            let config = SharedTopicWriterConfig::new(limits());
            let mut writer = live_many(
                &brokers,
                "open before crash",
                SharedTopicWriter::open(
                    &first_links,
                    "orders",
                    config.clone(),
                    RetryPolicy::default(),
                ),
            )
            .await
            .unwrap();
            let identity = writer.identity();
            let pending = writer
                .send(
                    RecordInput::single(
                        MessageId::from_bytes([35; 16]),
                        bytes::Bytes::from_static(b"confirmed before owner crash"),
                    ),
                    None,
                )
                .await
                .unwrap();
            live_many(&brokers, "confirm before crash", pending.confirmed())
                .await
                .unwrap();
            original.abort_owner();
            drop(writer);
            drop(first_links);
            drop(original);
            let mut writer = live_many(
                &brokers,
                "resume after owner crash",
                SharedTopicWriter::resume(
                    &links,
                    "orders",
                    identity,
                    config,
                    RetryPolicy::default(),
                ),
            )
            .await
            .unwrap();
            let pending = writer
                .send(
                    RecordInput::single(
                        MessageId::from_bytes([36; 16]),
                        bytes::Bytes::from_static(b"new record after owner crash"),
                    ),
                    None,
                )
                .await
                .unwrap();
            let receipt = live_many(&brokers, "confirm after owner crash", pending.confirmed())
                .await
                .unwrap();
            assert_eq!(receipt.record.key.first_sequence, 1);
            assert_eq!(receipt.record.offset, 1);
            live_many(&brokers, "close replacement", writer.close())
                .await
                .unwrap();
            links.shutdown().await.unwrap();
            for broker in brokers {
                broker.shutdown().await.unwrap();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "one held-I/O scenario checks all confirmation policies"
    )]
    async fn held_record_io_keeps_control_live_and_respects_confirmation_policy() {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let runtime = WriterRuntime::new().unwrap();
            let deployment = fixture(policy);
            let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
            let consumer = role_links(&runtime, &deployment[0].0, handshake::CONSUMER).await;
            let (brokers, controls) = Box::pin(start_controlled(&runtime, deployment, true)).await;
            let mut writer = live_many(
                &brokers,
                "open writer",
                SharedTopicWriter::open(
                    &producer,
                    "orders",
                    SharedTopicWriterConfig::new(limits()),
                    RetryPolicy::default(),
                ),
            )
            .await
            .unwrap();
            let warmup = writer
                .send(
                    RecordInput::single(
                        MessageId::from_bytes([26; 16]),
                        bytes::Bytes::from_static(b"warmup"),
                    ),
                    None,
                )
                .await
                .unwrap();
            live_many(
                &brokers,
                "establish producer before holding disk",
                warmup.confirmed(),
            )
            .await
            .unwrap();
            for device in &controls {
                device.hold_record_writes(true);
            }
            let id = MessageId::from_bytes([27; 16]);
            let pending = writer
                .send(
                    RecordInput::single(id, bytes::Bytes::from_static(b"held record")),
                    None,
                )
                .await
                .unwrap();
            live_many(&brokers, "observe held physical record write", async {
                while !controls.iter().any(|device| device.held_writes() > 0) {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            if policy == Confirmation::ReplicatedPersisting {
                live_many(
                    &brokers,
                    "two RAM copies confirm RP while disk is held",
                    pending.confirmed(),
                )
                .await
                .unwrap();
            } else {
                assert!(pending.confirmed().now_or_never().is_none());
            }
            let mut reader = live_many(
                &brokers,
                "metadata over separate control socket while disk is held",
                TopicReader::open(consumer.clone(), "orders", TopicReaderConfig::default()),
            )
            .await
            .unwrap();
            if policy != Confirmation::ReplicatedPersisting {
                assert!(pending.confirmed().now_or_never().is_none());
            }
            for device in &controls {
                device.hold_record_writes(false);
            }
            let confirmation = live_many(
                &brokers,
                "release physical record write",
                pending.confirmed(),
            )
            .await
            .unwrap();
            assert_eq!(confirmation.record.message_id, id);
            let warmup = live_many(&brokers, "read warmup", reader.next())
                .await
                .unwrap();
            assert_eq!(warmup.message_id, MessageId::from_bytes([26; 16]));
            drop(warmup);
            let record = live_many(&brokers, "read confirmed physical record", reader.next())
                .await
                .unwrap();
            assert_eq!(record.message_id, id);
            assert_eq!(record.payload[0].as_ref(), b"held record");
            reader.close().await.unwrap();
            writer.close().await.unwrap();
            producer.shutdown().await.unwrap();
            consumer.shutdown().await.unwrap();
            for broker in brokers {
                broker.shutdown().await.unwrap();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_physical_record_write_stops_the_real_broker_without_confirmation() {
        let runtime = WriterRuntime::new().unwrap();
        let deployment = fixture(Confirmation::LocalDurable);
        let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let (brokers, controls) = Box::pin(start_controlled(&runtime, deployment, false)).await;
        let mut writer = live_many(
            &brokers,
            "open writer",
            SharedTopicWriter::open(
                &producer,
                "orders",
                SharedTopicWriterConfig::new(limits()),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        controls[0].fail_next_record_write();
        let pending = writer
            .send(
                RecordInput::single(
                    MessageId::from_bytes([28; 16]),
                    bytes::Bytes::from_static(b"failed record"),
                ),
                None,
            )
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), brokers[0].closed())
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(controls[0].failures(), 1);
        assert!(
            pending
                .confirmed()
                .now_or_never()
                .is_none_or(|result| result.is_err())
        );
        drop(writer);
        producer.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "one SDK retention and resume schedule across all policies"
    )]
    async fn configured_retention_advances_sdk_earliest_and_keeps_producer_identity() {
        use ozzy_config::TopicRetention;
        use ozzy_runtime::replicated::ReaderStart;
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let runtime = WriterRuntime::new().unwrap();
            let root = std::path::PathBuf::from(format!("/ozzy-retention-{}", Uuid::now_v7()));
            let deployment = deployment_with(
                &root,
                if policy == Confirmation::LocalDurable {
                    DeploymentMode::Single
                } else {
                    DeploymentMode::Three
                },
                policy,
                1,
                |config| {
                    let topic = config.topics.get_mut("orders").unwrap();
                    topic.segment_bytes = 1024 * 1024;
                    topic.max_append_bytes = 64 * 1024;
                    topic.retention = TopicRetention {
                        max_age_secs: None,
                        max_bytes: Some(1024 * 1024),
                    };
                },
            );
            let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
            let consumer = role_links(&runtime, &deployment[0].0, handshake::CONSUMER).await;
            let (brokers, _) = Box::pin(start_controlled(&runtime, deployment, true)).await;
            let config = SharedTopicWriterConfig::new(limits());
            let mut writer = live_many(
                &brokers,
                "open retained writer",
                SharedTopicWriter::open(
                    &producer,
                    "orders",
                    config.clone(),
                    RetryPolicy::default(),
                ),
            )
            .await
            .unwrap();
            let identity = writer.identity();
            for index in 0..600u128 {
                let pending = writer
                    .send(
                        RecordInput::single(
                            MessageId::from_bytes((index + 1).to_be_bytes()),
                            bytes::Bytes::from(vec![index as u8; 512]),
                        ),
                        None,
                    )
                    .await
                    .unwrap();
                live_many(&brokers, "confirm retained record", pending.confirmed())
                    .await
                    .unwrap();
            }
            live_many(&brokers, "close retained writer", writer.close())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(4)).await;
            let mut reader = live_many(
                &brokers,
                "open retained earliest",
                TopicReader::open(
                    consumer.clone(),
                    "orders",
                    TopicReaderConfig {
                        start: ReaderStart::Earliest,
                        ..Default::default()
                    },
                ),
            )
            .await
            .unwrap();
            let record = live_many(&brokers, "read retained earliest", reader.next())
                .await
                .unwrap();
            assert!(
                record.offset.get() > 0,
                "retention did not run for {policy:?}"
            );
            live_many(&brokers, "close retained reader", reader.close())
                .await
                .unwrap();
            let mut resumed = live_many(
                &brokers,
                "resume retained producer",
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
            let pending = resumed
                .send(
                    RecordInput::single(
                        MessageId::from_bytes([101; 16]),
                        bytes::Bytes::from_static(b"after retention"),
                    ),
                    None,
                )
                .await
                .unwrap();
            let receipt = live_many(&brokers, "confirm retained resume", pending.confirmed())
                .await
                .unwrap();
            assert_eq!(receipt.record.offset, 600);
            live_many(&brokers, "close retained resume", resumed.close())
                .await
                .unwrap();
            for broker in brokers {
                broker.shutdown().await.unwrap();
            }
            producer.shutdown().await.unwrap();
            consumer.shutdown().await.unwrap();
        }
    }

    fn fixture(policy: Confirmation) -> Vec<(CheckedConfig, BrokerIdentity)> {
        let root = std::path::PathBuf::from(format!("/ozzy-simulation-{}", Uuid::now_v7()));
        deployment(
            &root,
            if policy == Confirmation::LocalDurable {
                DeploymentMode::Single
            } else {
                DeploymentMode::Three
            },
            policy,
            1,
        )
    }
}
