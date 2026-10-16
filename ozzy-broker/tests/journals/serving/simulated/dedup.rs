use super::*;
use ozzy_proto::append::AppendDisposition::{Duplicate, Stored};

fn dedup_deployment(
    policy: Confirmation,
    entries: u64,
    window_millis: u64,
) -> Vec<(CheckedConfig, BrokerIdentity)> {
    dedup_partitions(policy, entries, window_millis, 1)
}

fn dedup_partitions(
    policy: Confirmation,
    entries: u64,
    window_millis: u64,
    partitions: u32,
) -> Vec<(CheckedConfig, BrokerIdentity)> {
    let root = std::path::PathBuf::from(format!("/ozzy-dedup-{}", Uuid::now_v7()));
    deployment_with(
        &root,
        if policy == Confirmation::LocalDurable {
            DeploymentMode::Single
        } else {
            DeploymentMode::Three
        },
        policy,
        partitions,
        |deployment| {
            deployment.topics.get_mut("orders").unwrap().deduplication =
                Some(ozzy_config::TopicDeduplication {
                    window_millis,
                    max_entries: entries,
                    max_bytes: 4 * 1024 * 1024,
                });
        },
    )
}

#[tokio::test(flavor = "current_thread")]
async fn redundant_sdk_producers_confirm_original_offsets_in_all_modes() {
    for policy in [
        Confirmation::LocalDurable,
        Confirmation::DiskQuorum,
        Confirmation::ReplicatedPersisting,
    ] {
        let runtime = WriterRuntime::new().unwrap();
        let deployment = dedup_deployment(policy, 64, 300_000);
        let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let redundant = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let consumer = role_links(&runtime, &deployment[0].0, handshake::CONSUMER).await;
        let (brokers, _) = Box::pin(start_controlled(&runtime, deployment, true)).await;
        let mut writers = Vec::new();
        for links in [&producer, &redundant] {
            writers.push(
                live_many(
                    &brokers,
                    "open deduplicating SDK producer",
                    SharedTopicWriter::open(
                        links,
                        "orders",
                        SharedTopicWriterConfig::new(limits()),
                        RetryPolicy::default(),
                    ),
                )
                .await
                .unwrap(),
            );
        }
        for (writer, id, payload, offset, disposition) in [
            (0, 7, b"first".as_slice(), 0, Stored),
            (0, 7, b"same producer replay", 0, Duplicate),
            (1, 7, b"discarded", 0, Duplicate),
            (1, 8, b"second", 1, Stored),
            (0, 8, b"discarded again", 1, Duplicate),
            (0, 7, b"older offset", 0, Duplicate),
            (0, 9, b"third", 2, Stored),
        ] {
            let pending = writers[writer]
                .send(
                    RecordInput::single(
                        MessageId::from_bytes([id; 16]),
                        bytes::Bytes::from_static(payload),
                    ),
                    None,
                )
                .await
                .unwrap();
            let receipt = live_many(
                &brokers,
                "confirm deduplicated SDK record",
                pending.confirmed(),
            )
            .await
            .unwrap();
            assert_eq!(receipt.record.offset, offset, "{policy:?}");
            assert_eq!(receipt.record.disposition, disposition, "{policy:?}");
        }
        let mut reader = live_many(
            &brokers,
            "open deduplicated history",
            TopicReader::open(consumer.clone(), "orders", TopicReaderConfig::default()),
        )
        .await
        .unwrap();
        for (offset, id, payload) in [
            (0, 7, b"first".as_slice()),
            (1, 8, b"second"),
            (2, 9, b"third"),
        ] {
            let record = live_many(&brokers, "read first-wins payload", reader.next())
                .await
                .unwrap();
            assert_eq!(record.offset.get(), offset);
            assert_eq!(record.message_id, MessageId::from_bytes([id; 16]));
            assert_eq!(record.payload[0].as_ref(), payload);
        }
        live_many(&brokers, "close deduplicated reader", reader.close())
            .await
            .unwrap();
        for writer in writers {
            live_many(&brokers, "close deduplicating writer", writer.close())
                .await
                .unwrap();
        }
        for links in [producer, redundant, consumer] {
            links.shutdown().await.unwrap();
        }
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_dedup_index_resumes_sdk_producers_after_idle_expiry_in_all_modes() {
    use futures::FutureExt;
    for policy in [
        Confirmation::LocalDurable,
        Confirmation::DiskQuorum,
        Confirmation::ReplicatedPersisting,
    ] {
        let runtime = WriterRuntime::new().unwrap();
        let deployment = dedup_deployment(policy, 2, 1000);
        let wire = DataLimits {
            max_records: 2,
            max_parts: 2,
            ..limits()
        };
        let producer =
            role_links_with_limits(&runtime, &deployment[0].0, handshake::PRODUCER, wire).await;
        let consumer =
            role_links_with_limits(&runtime, &deployment[0].0, handshake::CONSUMER, wire).await;
        let (brokers, _) = Box::pin(start_controlled(&runtime, deployment, true)).await;
        let mut writer = live_many(
            &brokers,
            "open capacity-bounded SDK producer",
            SharedTopicWriter::open(
                &producer,
                "orders",
                SharedTopicWriterConfig::new(wire),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        for id in 1..=2 {
            let pending = writer
                .send(
                    RecordInput::single(
                        MessageId::from_bytes([id; 16]),
                        bytes::Bytes::from_static(b"live"),
                    ),
                    None,
                )
                .await
                .unwrap();
            live_many(&brokers, "fill identity capacity", pending.confirmed())
                .await
                .unwrap();
        }
        let pending = writer
            .send(
                RecordInput::single(
                    MessageId::from_bytes([3; 16]),
                    bytes::Bytes::from_static(b"resumed"),
                ),
                None,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(pending.confirmed().now_or_never().is_none(), "{policy:?}");
        let receipt = live_many(
            &brokers,
            "resume after confirmed idle watermark",
            pending.confirmed(),
        )
        .await
        .unwrap();
        assert_eq!(receipt.record.offset, 2);
        let mut reader = live_many(
            &brokers,
            "open history after identity expiry",
            TopicReader::open(consumer.clone(), "orders", TopicReaderConfig::default()),
        )
        .await
        .unwrap();
        for id in 1..=3 {
            let record = live_many(
                &brokers,
                "identity expiry preserves stored history",
                reader.next(),
            )
            .await
            .unwrap();
            assert_eq!(record.message_id, MessageId::from_bytes([id; 16]));
            assert_eq!(record.offset.get(), u64::from(id - 1));
        }
        live_many(&brokers, "close expiry reader", reader.close())
            .await
            .unwrap();
        live_many(&brokers, "close resumed writer", writer.close())
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
async fn keyed_takeover_keeps_first_wins_offsets_across_producer_epochs() {
    for policy in [
        Confirmation::LocalDurable,
        Confirmation::DiskQuorum,
        Confirmation::ReplicatedPersisting,
    ] {
        let runtime = WriterRuntime::new().unwrap();
        let deployment = dedup_partitions(policy, 64, 300_000, 4);
        let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let redundant = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let replacement = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let (brokers, _) = Box::pin(start_controlled(&runtime, deployment, true)).await;
        let open = |links| {
            SharedTopicWriter::open(
                links,
                "orders",
                SharedTopicWriterConfig::new(limits()),
                RetryPolicy::default(),
            )
        };
        let mut writer = live_many(&brokers, "open keyed producer", open(&producer))
            .await
            .unwrap();
        let mut copy = live_many(&brokers, "open redundant producer", open(&redundant))
            .await
            .unwrap();
        // Two symbols on different partitions; each keeps its own offsets.
        let keys: Vec<[u8; 4]> = (0..2)
            .map(|partition| {
                (0_u32..1000)
                    .map(u32::to_be_bytes)
                    .find(|key| writer.metadata().keyed_partition(key).number == partition)
                    .unwrap()
            })
            .collect();
        let send = async |writer: &mut SharedTopicWriter, id: u8, key: usize| {
            let pending = writer
                .send(
                    RecordInput::single(
                        MessageId::from_bytes([id; 16]),
                        bytes::Bytes::from_static(b"quote"),
                    ),
                    Some(&keys[key]),
                )
                .await
                .unwrap();
            live_many(&brokers, "confirm keyed record", pending.confirmed())
                .await
                .map(|receipt| (receipt.record.offset, receipt.record.disposition))
        };
        for (id, key, offset) in [(7, 0, 0), (8, 1, 0), (9, 0, 1)] {
            let stored = send(&mut writer, id, key).await.unwrap();
            assert_eq!(stored, (offset, Stored), "{policy:?}");
            let duplicate = send(&mut copy, id, key).await.unwrap();
            assert_eq!(duplicate, (offset, Duplicate), "{policy:?}");
        }
        let identity = writer.identity();
        let mut takeover = live_many(
            &brokers,
            "take over the keyed producer",
            SharedTopicWriter::takeover(
                &replacement,
                "orders",
                identity,
                SharedTopicWriterConfig::new(limits()),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        // The old epoch is fenced; identities outlive it.
        assert!(send(&mut writer, 10, 0).await.is_err(), "{policy:?}");
        for (id, key, expected) in [
            (7, 0, (0, Duplicate)),
            (8, 1, (0, Duplicate)),
            (11, 1, (1, Stored)),
        ] {
            let outcome = send(&mut takeover, id, key).await.unwrap();
            assert_eq!(outcome, expected, "{policy:?}");
        }
        let outcome = send(&mut copy, 11, 1).await.unwrap();
        assert_eq!(outcome, (1, Duplicate), "{policy:?}");
        // Dropping the fenced writer first must not stall the SDK thread the
        // takeover shares with it.
        drop(writer);
        live_many(&brokers, "close takeover writer", takeover.close())
            .await
            .unwrap();
        live_many(&brokers, "close redundant writer", copy.close())
            .await
            .unwrap();
        for links in [producer, redundant, replacement] {
            links.shutdown().await.unwrap();
        }
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
    }
}
