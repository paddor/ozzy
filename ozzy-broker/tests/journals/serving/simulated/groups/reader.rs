use super::*;
use ozzy_runtime::replicated::{GroupReader, GroupReaderConfig};
use std::collections::BTreeSet;

fn config(member: u8) -> GroupReaderConfig {
    let mut config = GroupReaderConfig::new(ConsumerMemberId::from_bytes([member; 16]));
    config.heartbeat = Duration::from_millis(10);
    config.drain = Duration::from_millis(50);
    config.window = 1;
    config
}

#[tokio::test(flavor = "current_thread")]
async fn processing_continues_after_the_writer_stops_with_bounded_control_memory() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let cluster = Cluster::new(policy).await;
        let links = role_links_with_config(
            &cluster.runtime,
            &cluster.configs[0].0,
            handshake::PRODUCER | handshake::CONSUMER,
            limits(),
            |config| {
                config.parameters.capabilities |=
                    handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
                config.parameters.required_capabilities |=
                    handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
            },
        )
        .await;
        let mut writer = live_many(
            &cluster.brokers,
            "seed idle progress sources",
            SharedTopicWriter::open(
                &links,
                "orders",
                SharedTopicWriterConfig::new(limits()),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        let key = (0_u32..1000)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == 0)
            .unwrap();
        for sequence in 1_u128..=128 {
            let pending = writer
                .send(
                    RecordInput::copy_from_slice(
                        MessageId::from_bytes(sequence.to_be_bytes()),
                        b"idle processing",
                    ),
                    Some(&key),
                )
                .await
                .unwrap();
            live_many(
                &cluster.brokers,
                "confirm idle progress fixture",
                pending.confirmed(),
            )
            .await
            .unwrap();
        }
        live_many(
            &cluster.brokers,
            "stop idle progress writer",
            writer.close(),
        )
        .await
        .unwrap();
        let mut reader = GroupReader::open(links.clone(), "orders", "idle-workers", config(3))
            .await
            .unwrap();
        for offset in 0..128 {
            let record = live_many(&cluster.brokers, "read idle progress source", reader.next())
                .await
                .unwrap();
            assert_eq!(record.record.partition, 0);
            assert_eq!(record.record.offset.get(), offset);
            assert_eq!(record.record.payload[0].as_ref(), b"idle processing");
            let pending = reader.acknowledge(record.token).unwrap();
            let receipt = live_many(
                &cluster.brokers,
                "confirm idle processing",
                pending.confirmed(),
            )
            .await
            .unwrap();
            assert_eq!(receipt.cursor.get(), offset + 1);
        }
        reader.close().await.unwrap();
        links.shutdown().await.unwrap();
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stalled_application_releases_revoked_sources_before_the_broker_deadline() {
    for policy in [
        Confirmation::LocalDurable,
        Confirmation::DiskQuorum,
        Confirmation::ReplicatedPersisting,
    ] {
        stalled(policy).await;
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keep the stalled application and handoff evidence in one schedule"
)]
async fn stalled(policy: Confirmation) {
    let cluster = Cluster::new(policy).await;
    let links = role_links_with_config(
        &cluster.runtime,
        &cluster.configs[0].0,
        handshake::PRODUCER | handshake::CONSUMER,
        limits(),
        |config| {
            config.parameters.capabilities |= handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
            config.parameters.required_capabilities |=
                handshake::OWNER_GROUPS | handshake::OWNER_PROGRESS;
        },
    )
    .await;
    let mut writer = live_many(
        &cluster.brokers,
        "seed stalled member sources",
        SharedTopicWriter::open(
            &links,
            "orders",
            SharedTopicWriterConfig::new(limits()),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    for number in 0..4 {
        let key = (0_u32..1000)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == number)
            .unwrap();
        for sequence in 0..3_u128 {
            let id = MessageId::from_bytes((u128::from(number) * 3 + sequence + 1).to_be_bytes());
            let pending = writer
                .send(
                    RecordInput::copy_from_slice(id, b"processing handoff"),
                    Some(&key),
                )
                .await
                .unwrap();
            live_many(
                &cluster.brokers,
                "confirm stalled member source",
                pending.confirmed(),
            )
            .await
            .unwrap();
        }
    }
    let mut first = GroupReader::open(links.clone(), "orders", "workers", config(2))
        .await
        .unwrap();
    let mut seen = BTreeSet::new();
    let mut unacknowledged = None;
    while seen.len() < 4 {
        let record = live_many(
            &cluster.brokers,
            "deliver all initial sources",
            first.next(),
        )
        .await
        .unwrap();
        seen.insert(record.record.partition);
        if record.record.partition == 3 {
            assert_eq!(record.record.offset, Offset::ZERO);
            unacknowledged = Some(record.token);
        } else {
            let pending = first.acknowledge(record.token).unwrap();
            live_many(
                &cluster.brokers,
                "confirm incumbent processing",
                pending.confirmed(),
            )
            .await
            .unwrap();
        }
    }
    let old = unacknowledged.unwrap();
    // Leave the incumbent application completely idle. Its membership task
    // must release moved sources and fence the unconfirmed window by itself.
    let mut second = GroupReader::open(links.clone(), "orders", "workers", config(1))
        .await
        .unwrap();
    let repeated = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let record = second.next().await.unwrap();
            assert_eq!(record.record.payload[0].as_ref(), b"processing handoff");
            if record.record.partition == 3 {
                break record;
            }
            assert_eq!(record.record.partition, 2);
            drop(second.acknowledge(record.token).unwrap());
        }
    })
    .await
    .expect("local drain must release before the five-second broker deadline");
    assert_eq!(repeated.record.offset, Offset::ZERO);
    assert!(repeated.token.identity().assignment_epoch > old.identity().assignment_epoch);
    assert!(matches!(first.acknowledge(old), Err(ProgressError::Fenced)));
    let pending = second.acknowledge(repeated.token).unwrap();
    live_many(
        &cluster.brokers,
        "confirm repeated processing",
        pending.confirmed(),
    )
    .await
    .unwrap();
    first.close().await.unwrap();
    second.close().await.unwrap();
    writer.close().await.unwrap();
    links.shutdown().await.unwrap();
    cluster.shutdown().await;
}
