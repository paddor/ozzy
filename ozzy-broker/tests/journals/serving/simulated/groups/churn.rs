//! Redundant producers and a group reader through crashes of a live copy.

use super::*;
use ozzy_proto::append::AppendDisposition;
use ozzy_runtime::replicated::{GroupReader, GroupReaderConfig};
use std::collections::BTreeMap;

const WAVES: u128 = 4;
const RECORDS: u128 = 16;

#[tokio::test(flavor = "current_thread")]
async fn disk_quorum_redundant_producers_and_a_group_reader_survive_copy_crashes() {
    churn(Confirmation::DiskQuorum).await;
}

#[tokio::test(flavor = "current_thread")]
async fn replicated_persisting_redundant_producers_and_a_group_reader_survive_copy_crashes() {
    churn(Confirmation::ReplicatedPersisting).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "keep the crash schedule, receipts, and delivery checks together"
)]
async fn churn(policy: Confirmation) {
    let mut cluster = Cluster::configured(policy, None, |deployment| {
        deployment.topics.get_mut("orders").unwrap().deduplication =
            Some(ozzy_config::TopicDeduplication {
                window_millis: 300_000,
                max_entries: 1024,
                max_bytes: 4 * 1024 * 1024,
            });
    })
    .await;
    let producers = [
        role_links(&cluster.runtime, &cluster.configs[0].0, handshake::PRODUCER).await,
        role_links(&cluster.runtime, &cluster.configs[0].0, handshake::PRODUCER).await,
    ];
    let consumer = role_links_with_config(
        &cluster.runtime,
        &cluster.configs[0].0,
        handshake::CONSUMER,
        limits(),
        |config| {
            config.parameters.capabilities |= handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
            config.parameters.required_capabilities |=
                handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
        },
    )
    .await;
    let mut writers = Vec::new();
    for links in &producers {
        writers.push(
            live_many(
                &cluster.brokers,
                "open redundant producer",
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
    let mut config = GroupReaderConfig::new(ConsumerMemberId::from_bytes([1; 16]));
    config.heartbeat = Duration::from_millis(50);
    let mut reader = live_many(
        &cluster.brokers,
        "open churn group",
        GroupReader::open(consumer.clone(), "orders", "churn-workers", config.clone()),
    )
    .await
    .unwrap();
    // Independent oracle: each identity's first-wins coordinates, and the
    // next offset the group must deliver on each partition.
    let mut stored = BTreeMap::new();
    let mut next = BTreeMap::<u32, u64>::new();
    for wave in 0..WAVES {
        let mut pending = Vec::new();
        for number in 0..RECORDS {
            let id = MessageId::from_bytes((wave * RECORDS + number + 1).to_be_bytes());
            for writer in &mut writers {
                // Keying by identity routes both copies to the same partition.
                let send = writer
                    .send(
                        RecordInput::copy_from_slice(id, id.as_bytes()),
                        Some(id.as_bytes()),
                    )
                    .await
                    .unwrap();
                pending.push((id, send));
            }
        }
        // Crash a copy while both producers' records are outstanding.
        let crashed = (wave % 2 == 1).then_some((wave / 2) as usize % 3);
        if let Some(index) = crashed {
            cluster.crash(index, false).await;
        }
        let mut copies = BTreeMap::<MessageId, Vec<_>>::new();
        for (id, send) in pending {
            let receipt = live_many(
                &cluster.brokers,
                "confirm redundant record",
                send.confirmed(),
            )
            .await
            .unwrap();
            copies.entry(id).or_default().push(receipt.record);
        }
        for (id, records) in copies {
            let [first, second] = records.as_slice() else {
                panic!("{policy:?}: {id:?} has {} receipts", records.len());
            };
            assert_eq!(
                (first.partition, first.offset),
                (second.partition, second.offset),
                "{policy:?}: both copies of {id:?} confirm the first-wins offset"
            );
            assert!(
                records
                    .iter()
                    .filter(|record| record.disposition == AppendDisposition::Stored)
                    .count()
                    <= 1,
                "{policy:?}: {id:?} stored twice"
            );
            assert!(stored.insert(id, (first.partition, first.offset)).is_none());
        }
        if let Some(index) = crashed {
            cluster.wait_recovered(index).await;
        }
        for _ in 0..RECORDS {
            let delivered = live_many(&cluster.brokers, "read churn group", reader.next())
                .await
                .unwrap();
            let record = &delivered.record;
            let (incarnation, offset) = stored[&record.message_id];
            assert_eq!(
                (record.incarnation, record.offset.get()),
                (incarnation, offset)
            );
            let expected = next.entry(record.partition).or_default();
            assert_eq!(record.offset.get(), *expected, "{policy:?}: group order");
            *expected += 1;
            let acknowledged = reader.acknowledge(delivered.token).unwrap();
            let receipt = live_many(
                &cluster.brokers,
                "confirm churn processing",
                acknowledged.confirmed(),
            )
            .await
            .unwrap();
            assert_eq!(receipt.cursor.get(), *expected);
        }
    }
    // A new instance of the member resumes after every confirmed cursor.
    live_many(&cluster.brokers, "close churn group", reader.close())
        .await
        .unwrap();
    let mut reader = live_many(
        &cluster.brokers,
        "reopen churn group",
        GroupReader::open(consumer.clone(), "orders", "churn-workers", config),
    )
    .await
    .unwrap();
    let id = MessageId::from_bytes((WAVES * RECORDS + 1).to_be_bytes());
    for writer in &mut writers {
        let send = writer
            .send(
                RecordInput::copy_from_slice(id, id.as_bytes()),
                Some(id.as_bytes()),
            )
            .await
            .unwrap();
        live_many(&cluster.brokers, "confirm resume record", send.confirmed())
            .await
            .unwrap();
    }
    let resumed = live_many(&cluster.brokers, "read after reopen", reader.next())
        .await
        .unwrap();
    assert_eq!(resumed.record.message_id, id, "{policy:?}: no redelivery");
    let expected = next.entry(resumed.record.partition).or_default();
    assert_eq!(resumed.record.offset.get(), *expected);
    let acknowledged = reader.acknowledge(resumed.token).unwrap();
    let receipt = live_many(
        &cluster.brokers,
        "confirm resumed processing",
        acknowledged.confirmed(),
    )
    .await
    .unwrap();
    assert_eq!(receipt.cursor.get(), *expected + 1);
    live_many(&cluster.brokers, "close reopened group", reader.close())
        .await
        .unwrap();
    for writer in writers {
        live_many(&cluster.brokers, "close redundant producer", writer.close())
            .await
            .unwrap();
    }
    for links in producers.into_iter().chain([consumer]) {
        links.shutdown().await.unwrap();
    }
    cluster.shutdown().await;
}
