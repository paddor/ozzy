//! Automatic consumer groups over real brokers: membership, handoff, resume.

use super::*;
use crate::admin_client::AdminClient;
use ozzy_proto::{
    ConsumerMemberId,
    admin::{Reply, Request},
};
use ozzy_runtime::replicated::{GroupReader, GroupReaderConfig, GroupReaderError};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

type Seen = Arc<Mutex<Vec<(u8, u32, u64)>>>;

#[tokio::test(flavor = "current_thread")]
async fn production_group_readers_divide_partitions_and_resume_persisted_cursors() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for mode in [DeploymentMode::Single, DeploymentMode::Three] {
            scenario(mode).await;
        }
    })
    .await
    .unwrap();
}

async fn group_links(runtime: &WriterRuntime, checked: &CheckedConfig) -> BrokerLinks {
    role_links_with_config(runtime, checked, handshake::CONSUMER, limits(), |config| {
        config.parameters = handshake::Parameters::group(limits(), handshake::CONSUMER).unwrap();
        config.parameters.capabilities |= handshake::OWNER_ROUTING;
    })
    .await
}

fn config(member: u8) -> GroupReaderConfig {
    let mut config = GroupReaderConfig::new(ConsumerMemberId::from_bytes([member; 16]));
    config.heartbeat = Duration::from_millis(50);
    config
}

async fn write(
    brokers: &[Broker],
    writer: &mut SharedTopicWriter,
    keys: &[[u8; 4]; 2],
    count: u64,
) {
    let mut pending = Vec::new();
    for key in keys {
        for _ in 0..count {
            let id = MessageId::from_bytes(*Uuid::now_v7().as_bytes());
            pending.push(
                writer
                    .send(RecordInput::copy_from_slice(id, b"record"), Some(key))
                    .await
                    .unwrap(),
            );
        }
    }
    for pending in pending {
        live_many(brokers, "confirm record", pending.confirmed())
            .await
            .unwrap();
    }
}

/// Broker 0's admin endpoint shows the group's two members and an owned,
/// committed cursor on each partition, whether it leads them or not.
async fn assert_admin_shows_group(brokers: &[Broker], runtime: &WriterRuntime, endpoint: &str) {
    let mut client = AdminClient::connect(runtime.context(), endpoint).await;
    live_many(brokers, "admin shows the group", async {
        loop {
            let Reply::Partitions(partitions) = client.ask(Request::Partitions).await else {
                panic!("partitions reply expected");
            };
            let logs = || partitions.rows.iter().filter_map(|row| row.log.as_ref());
            let members = logs()
                .flat_map(|log| &log.groups)
                .find(|group| group.name == "workers")
                .map(|group| group.members.len());
            let cursors = logs()
                .flat_map(|log| &log.cursors)
                .filter(|cursor| cursor.member.is_some() && cursor.offset.is_some())
                .count();
            if members == Some(2) && cursors == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    client.close().await;
}

/// Deliver and acknowledge until stopped, then close cooperatively.
async fn consume(
    mut reader: GroupReader,
    member: u8,
    seen: Seen,
    mut stop: watch::Receiver<bool>,
) -> Result<(), GroupReaderError> {
    loop {
        let delivered = tokio::select! {
            delivered = reader.next() => delivered?,
            _ = stop.wait_for(|stop| *stop) => break,
        };
        seen.lock().unwrap().push((
            member,
            delivered.record.partition,
            delivered.record.offset.get(),
        ));
        drop(reader.acknowledge(delivered.token)?);
    }
    reader.close().await
}

async fn until_seen(
    brokers: &[Broker],
    seen: &Seen,
    stage: &str,
    done: impl Fn(&[(u8, u32, u64)]) -> bool,
) {
    live_many(brokers, stage, async {
        while !done(&seen.lock().unwrap()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one membership and resume schedule across real brokers"
)]
async fn scenario(mode: DeploymentMode) {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let admin = format!("inproc://groups-admin-{}", Uuid::now_v7());
    let deployment = deployment_with(root.path(), mode, 2, |config| {
        for (name, broker) in &mut config.brokers {
            broker.endpoints.admin = Some(format!("{admin}-{name}"));
        }
    });
    let admin = format!("{admin}-{}", deployment[0].0.plan.name);
    let sdk = links(&runtime, &deployment[0].0).await;
    let first_links = group_links(&runtime, &deployment[0].0).await;
    let second_links = group_links(&runtime, &deployment[0].0).await;
    let brokers = start_brokers(&runtime, deployment).await;
    let mut writer = live_many(
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
    .unwrap();
    let keys = [0, 1].map(|number| {
        (0_u32..1000)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == number)
            .unwrap()
    });
    write(&brokers, &mut writer, &keys, 8).await;

    // One member owns both partitions and starts at the earliest record.
    let seen = Seen::default();
    let (stop, stopped) = watch::channel(false);
    let first = live_many(
        &brokers,
        "open first group reader",
        GroupReader::open(first_links.clone(), "orders", "workers", config(1)),
    )
    .await
    .unwrap();
    let first = tokio::spawn(consume(first, 1, seen.clone(), stopped.clone()));
    until_seen(
        &brokers,
        &seen,
        "first member reads both partitions",
        |seen| seen.len() == 16,
    )
    .await;

    // A second member takes one partition after the first one releases it.
    let second = live_many(
        &brokers,
        "open second group reader",
        GroupReader::open(second_links.clone(), "orders", "workers", config(2)),
    )
    .await
    .unwrap();
    let second = tokio::spawn(consume(second, 2, seen.clone(), stopped.clone()));
    write(&brokers, &mut writer, &keys, 8).await;
    // Keep writing until the handoff completes and the new owner delivers.
    let mut written = 16;
    live_many(&brokers, "second member receives a partition", async {
        while !seen.lock().unwrap().iter().any(|&(member, ..)| member == 2) {
            write(&brokers, &mut writer, &keys, 1).await;
            written += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    until_seen(&brokers, &seen, "members read every record", |seen| {
        seen.len() == 2 * written
    })
    .await;
    assert_admin_shows_group(&brokers, &runtime, &admin).await;
    stop.send(true).unwrap();
    live_many(&brokers, "close first member", first)
        .await
        .unwrap()
        .unwrap();
    live_many(&brokers, "close second member", second)
        .await
        .unwrap()
        .unwrap();
    {
        let seen = seen.lock().unwrap();
        let unique: BTreeSet<_> = seen
            .iter()
            .map(|&(_, partition, offset)| (partition, offset))
            .collect();
        assert_eq!(unique.len(), seen.len(), "{mode:?}: repeated delivery");
        assert_eq!(unique.len(), 2 * written, "{mode:?}");
        // Each partition's latest record went to its final, distinct owner.
        let last = |number| {
            seen.iter()
                .filter(|&&(_, partition, _)| partition == number)
                .max_by_key(|&&(_, _, offset)| offset)
                .unwrap()
                .0
        };
        assert_ne!(last(0), last(1), "{mode:?}: one owner per partition");
    }

    // A new member resumes at the persisted cursors, not the creation selector.
    write(&brokers, &mut writer, &keys, 1).await;
    let resumed = Seen::default();
    let (stop, stopped) = watch::channel(false);
    let third = live_many(
        &brokers,
        "open third group reader",
        GroupReader::open(first_links.clone(), "orders", "workers", config(3)),
    )
    .await
    .unwrap();
    let third = tokio::spawn(consume(third, 3, resumed.clone(), stopped));
    until_seen(&brokers, &resumed, "third member resumes", |seen| {
        seen.len() == 2
    })
    .await;
    stop.send(true).unwrap();
    live_many(&brokers, "close third member", third)
        .await
        .unwrap()
        .unwrap();
    let mut resumed = resumed.lock().unwrap().clone();
    resumed.sort_unstable();
    let end = written as u64;
    assert_eq!(resumed, [(3, 0, end), (3, 1, end)], "{mode:?}");

    live_many(&brokers, "close writer", writer.close())
        .await
        .unwrap();
    for links in [sdk, first_links, second_links] {
        links.shutdown().await.unwrap();
    }
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}
