//! The admin endpoint answers broker, counter, partition, and connection
//! requests.

use super::*;
use crate::admin_client::AdminClient;
use ozzy_proto::admin::{self, Reply, Request};
use std::collections::BTreeSet;

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
                let mut unknown = admin::encode_request(9, Request::Broker);
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
