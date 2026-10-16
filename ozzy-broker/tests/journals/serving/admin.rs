//! The admin endpoint answers broker, counter, partition, and connection
//! requests, and deletes consumer groups.

use super::*;
use crate::admin_client::AdminClient;
use ozzy_proto::{
    ConsumerMemberId,
    admin::{self, Reply, Request},
};
use ozzy_runtime::replicated::{GroupReader, GroupReaderConfig};
use std::{collections::BTreeSet, num::NonZeroU64};

async fn assert_counters(client: &mut AdminClient) {
    let Reply::Counters(counters) = client.ask(Request::Counters).await else {
        panic!("counters reply expected");
    };
    assert!(counters.events.iter().any(|(name, _)| name == "shard_turn"));
    assert_eq!(counters.totals.len(), counters.events.len());
    assert!(
        counters
            .threads
            .iter()
            .any(|(name, _)| name.starts_with("ozzy/shard/"))
    );
    assert_eq!(counters.threads.last().unwrap().0, "exited");
}

/// Every shard answers, and every partition has a configured leader once the
/// brokers serve. Leaders report both followers' acknowledgments. An idle
/// journal answers with its committed state; a busy one is asked again.
async fn assert_partitions(client: &mut AdminClient, broker: &admin::Broker) {
    let groups = broker
        .partitions
        .iter()
        .map(|partition| partition.group)
        .collect::<BTreeSet<_>>();
    let members = broker.brokers.iter().map(|(_, id)| *id).collect::<Vec<_>>();
    let partitions = loop {
        let Reply::Partitions(partitions) = client.ask(Request::Partitions).await else {
            panic!("partitions reply expected");
        };
        if partitions.rows.iter().all(|row| row.log.is_some()) {
            break partitions;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(partitions.missing_shards, 0);
    assert_eq!(
        partitions
            .rows
            .iter()
            .map(|row| row.group)
            .collect::<BTreeSet<_>>(),
        groups
    );
    for row in &partitions.rows {
        assert_eq!(row.state, admin::PartitionState::Normal);
        assert!(row.leader.is_some_and(|leader| members.contains(&leader)));
        assert!(row.durable <= row.written && row.written <= row.accepted);
        let followers = row
            .followers
            .iter()
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        if row.leader == Some(broker.id) {
            let others = members.iter().copied().filter(|id| *id != broker.id);
            assert_eq!(followers, others.collect());
        } else {
            assert!(followers.is_empty());
        }
        let log = row.log.as_ref().unwrap();
        assert_eq!((log.first_offset, log.next_offset), (0, 0));
        assert_eq!(
            (log.retention_age_millis, log.retention_bytes),
            (NonZeroU64::new(3_600_000), None)
        );
        assert_eq!(log.policy_revision, 1);
        assert!(log.segments >= 1 && log.segment_bytes > 0, "{log:?}");
        assert_eq!(
            (log.dedup_entries, log.dedup_limit),
            (0, NonZeroU64::new(1024))
        );
    }
}

/// Links to both other brokers come up; no client is connected.
async fn assert_connections(client: &mut AdminClient, broker: &admin::Broker) {
    let others = broker
        .brokers
        .iter()
        .map(|(_, id)| *id)
        .filter(|id| *id != broker.id)
        .collect::<Vec<_>>();
    loop {
        let Reply::Connections(connections) = client.ask(Request::Connections).await else {
            panic!("connections reply expected");
        };
        assert_eq!(connections.clients, 0);
        let ids = connections.brokers.iter().map(|(id, _)| *id);
        assert!(ids.eq(others.iter().copied()));
        if connections.brokers.iter().all(|(_, up)| *up) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_endpoint_reports_broker_counters_partitions_and_links() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = std::path::PathBuf::from(format!("/ozzy-simulation-{}", Uuid::now_v7()));
        let runtime = WriterRuntime::new().unwrap();
        let deployment = deployment_with(&root, DeploymentMode::Three, 4, |config| {
            for (name, broker) in &mut config.brokers {
                broker.endpoints.admin = Some(format!("inproc://admin-test-{name}"));
            }
            let orders = config.topics.get_mut("orders").unwrap();
            orders.retention.max_age_secs = Some(3600);
            orders.deduplication = Some(ozzy_config::TopicDeduplication {
                window_millis: 60_000,
                max_entries: 1024,
                max_bytes: 4 * 1024 * 1024,
            });
        });
        let shards = deployment
            .iter()
            .map(|(checked, _)| (checked.plan.name.clone(), checked.plan.shards.len() as u32))
            .collect::<Vec<_>>();
        let brokers = Box::pin(ozzy_sim::broker::start_brokers(&runtime, deployment, false)).await;
        for broker in &brokers {
            broker.ready().await.unwrap();
        }
        // The fixture gives later brokers more shards, with sparse IDs.
        for (index, (name, shards)) in shards.iter().enumerate() {
            let endpoint = format!("inproc://admin-test-{name}");
            let mut client = AdminClient::connect(runtime.context(), &endpoint).await;
            let Reply::Broker(broker) = client.ask(Request::Broker).await else {
                panic!("broker reply expected");
            };
            assert_eq!(client.broker(), Some(broker.id));
            assert_eq!(&broker.name, name);
            assert_eq!(broker.brokers.len(), 3);
            assert_eq!(broker.partitions.len(), 4);
            assert_eq!(broker.shards, *shards);
            assert_partitions(&mut client, &broker).await;
            assert_connections(&mut client, &broker).await;
            if index == 0 {
                assert_counters(&mut client).await;
                let mut unknown = admin::encode_request(9, &Request::Broker).unwrap();
                unknown[0] = 0x7f;
                assert_eq!(
                    client.ask_raw(unknown).await,
                    Reply::Error("unknown request".into())
                );
            }
            client.close().await;
        }
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}

/// The admin owner logs each deletion; the recent log has it.
async fn assert_deletion_logged(client: &mut AdminClient, group: ozzy_proto::ConsumerGroupId) {
    let Reply::Logs(logs) = client.ask(Request::Logs).await else {
        panic!("logs reply expected");
    };
    let deleted = format!("admin deleted consumer group orders/workers ({group})");
    assert!(
        logs.iter()
            .any(|entry| entry.message == deleted && entry.thread == "ozzy/admin"),
        "{logs:?}"
    );
}

/// Any broker deletes a group, wherever its partitions lead: the admin
/// owner routes through its own client link.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_endpoint_deletes_a_consumer_group() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let root = std::path::PathBuf::from(format!("/ozzy-simulation-{}", Uuid::now_v7()));
        let runtime = WriterRuntime::new().unwrap();
        let deployment = deployment_with(&root, DeploymentMode::Three, 4, |config| {
            for (name, broker) in &mut config.brokers {
                broker.endpoints.admin = Some(format!("inproc://admin-delete-{name}"));
            }
        });
        let checked = deployment[0].0.clone();
        let last = deployment[2].0.plan.name.clone();
        let brokers = Box::pin(ozzy_sim::broker::start_brokers(&runtime, deployment, false)).await;
        for broker in &brokers {
            broker.ready().await.unwrap();
        }
        let mut writer = ozzy_sim::client::Client::open_with_runtime(&checked, &runtime).await;
        let pending = writer.queue(0).await;
        writer.confirm(pending).await;
        let links = writer.group_links().await;
        let config = GroupReaderConfig::new(ConsumerMemberId::new());
        let mut reader = GroupReader::open(links.clone(), "orders", "workers", config)
            .await
            .unwrap();
        let record = reader.next().await.unwrap();
        drop(reader.acknowledge(record.token).unwrap());
        reader.close().await.unwrap();
        let inspection = links.inspect_group("orders", "workers").await.unwrap();
        let group = inspection.unwrap().group;

        let endpoint = format!("inproc://admin-delete-{last}");
        let mut client = AdminClient::connect(runtime.context(), &endpoint).await;
        let delete = |topic: &str| Request::DeleteGroup {
            topic: topic.into(),
            name: "workers".into(),
        };
        assert_eq!(
            client.ask(delete("orders")).await,
            Reply::GroupDeleted(Some(group))
        );
        assert_eq!(
            links.inspect_group("orders", "workers").await.unwrap(),
            None
        );
        assert_eq!(
            client.ask(delete("orders")).await,
            Reply::GroupDeleted(None)
        );
        assert!(matches!(
            client.ask(delete("missing")).await,
            Reply::Error(_)
        ));
        assert_deletion_logged(&mut client, group).await;
        client.close().await;
        links.shutdown().await.unwrap();
        writer.close().await;
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}
