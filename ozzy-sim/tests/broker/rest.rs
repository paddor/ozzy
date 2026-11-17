//! Rest: idle replicated partitions send nothing and rely on the beats
//! between their brokers. They speak again for work, fail over when the
//! leader's broker falls silent, and agree again after a view change.

use super::log::{append, appends_and_reads, leader};
use super::*;
use ozzy_broker::log::{Log, LogRole};
use ozzy_runtime::profiling::{Event, totals};

/// Four partitions, each with two agreements counted on both sides.
const ALL: u64 = 16;

fn count(event: Event) -> u64 {
    totals().get(event)
}

/// Rest agreements standing now, over every broker of this process.
fn held() -> u64 {
    count(Event::ReplicaRest) - count(Event::ReplicaRestEnd)
}

async fn until_resting() {
    while held() != ALL {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn cluster() -> (broker::Cluster, Vec<Log>) {
    let cluster = broker::Cluster::configured(DeploymentMode::Three, None, |config| {
        config.topics.get_mut("orders").unwrap().retention = ozzy_config::TopicRetention::default();
    })
    .await;
    let logs = (cluster.brokers.iter())
        .map(|broker| broker.log("orders", 0).unwrap())
        .collect();
    (cluster, logs)
}

#[tokio::test(flavor = "current_thread")]
async fn idle_partitions_rest_until_an_append_makes_them_speak() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (cluster, logs) = cluster().await;
        let (index, view, end) = appends_and_reads(&logs).await;
        // Startup work, such as a late bootstrap record, can still end a
        // rest. After that nothing does while nothing happens.
        let counts = || {
            let ended = count(Event::ReplicaRestEnd);
            (
                count(Event::ReplicaRest),
                ended,
                count(Event::ShardEpochChange),
            )
        };
        let (_, ended, epochs) = loop {
            until_resting().await;
            let before = counts();
            tokio::time::sleep(Duration::from_millis(1500)).await;
            if counts() == before {
                break before;
            }
        };
        assert_eq!(logs[index].status().borrow().role, LogRole::Leader);
        // The leader and both followers of the partition speak again.
        let appended = append(&logs[index], view, end, &[7]).await.unwrap();
        assert_eq!(appended.end.get(), end + 1);
        assert!(count(Event::ReplicaRestEnd) >= ended + 4);
        until_resting().await;
        assert_eq!(
            count(Event::ShardEpochChange),
            epochs,
            "no voter left a view"
        );
        cluster.shutdown().await;
    })
    .await
    .expect("resting partitions made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn a_resting_partition_fails_over_when_its_leader_falls_silent() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (cluster, logs) = cluster().await;
        let (index, view, end) = appends_and_reads(&logs).await;
        until_resting().await;
        let cut = std::time::Instant::now();
        cluster.isolate(index, true);
        let (next, status) = leader(&logs, Some(view)).await;
        assert_ne!(next, index);
        // One second of silence, then the election.
        assert!(
            cut.elapsed() < Duration::from_secs(5),
            "{:?}",
            cut.elapsed()
        );
        // The old leader hears no beat either, so it doubts.
        let mut old = logs[index].status();
        old.wait_for(|status| status.role != LogRole::Leader)
            .await
            .unwrap();
        let start = status.lead.unwrap().start_end.get();
        assert_eq!(start, end);
        let appended = append(&logs[next], status.view, start, &[7]).await.unwrap();
        assert_eq!(appended.end.get(), end + 1);
        cluster.isolate(index, false);
        until_resting().await;
        cluster.shutdown().await;
    })
    .await
    .expect("resting failover made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn a_step_down_changes_the_epoch_and_every_partition_rests_again() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let (cluster, logs) = cluster().await;
        let (index, view, _) = appends_and_reads(&logs).await;
        until_resting().await;
        let target = (index + 1) % 3;
        logs[index]
            .step_down(view, cluster.node(target))
            .unwrap()
            .await
            .unwrap();
        let (next, _) = leader(&logs, Some(view)).await;
        assert_eq!(next, target);
        // The old leader left a view its followers still trusted it in.
        while count(Event::ShardEpochChange) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        until_resting().await;
        cluster.shutdown().await;
    })
    .await
    .expect("resting step-down made no bounded progress");
}

/// A step-down withdraws what the voters of one part of a shard said. The
/// logs of the other parts stay at rest.
#[tokio::test(flavor = "current_thread")]
async fn a_step_down_ends_the_rest_of_its_part_of_the_shard_only() {
    use ozzy_broker::LogSpec;
    use ozzy_proto::{GroupId, PartitionIncarnation};
    use ozzy_runtime::frontend::Beats;
    const LOGS: u8 = 24;
    tokio::time::timeout(Duration::from_secs(120), async {
        let cluster = broker::Cluster::configured(DeploymentMode::Three, None, |config| {
            config.topics.get_mut("orders").unwrap().retention =
                ozzy_config::TopicRetention::default();
            config.logs = Some(ozzy_config::LogClass {
                segment_bytes: 4 * 1024 * 1024,
                max_append_bytes: 1024 * 1024,
                max_logs: usize::from(LOGS),
            });
        })
        .await;
        let nodes = [cluster.node(0), cluster.node(1), cluster.node(2)];
        let id = |index: u8| GroupId::from_bytes([index; 16]);
        for index in 1..=LOGS {
            let spec = LogSpec {
                id: id(index),
                incarnation: PartitionIncarnation::from_bytes([index + 100; 16]),
                members: nodes.to_vec(),
                segment_bytes: None,
            };
            for broker in &cluster.brokers {
                broker.create_log(spec.clone()).await.unwrap();
            }
        }
        // The log whose part of its shard the fewest logs share.
        let sharing = |index: u8| {
            let part = Beats::part(id(index));
            let shared = (1..=LOGS).filter(|other| Beats::part(id(*other)) == part);
            shared.count() as u64
        };
        let first = (1..=LOGS).min_by_key(|index| sharing(*index)).unwrap();
        let sharing = sharing(first);
        let logs: Vec<Log> = (cluster.brokers.iter())
            .map(|broker| broker.runtime_log(id(first)).unwrap())
            .collect();
        let (index, status) = leader(&logs, None).await;
        // Every partition and every log holds four agreements.
        let all = ALL + 4 * u64::from(LOGS);
        let rested = || async {
            loop {
                while held() != all {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let before = count(Event::ReplicaRestEnd);
                tokio::time::sleep(Duration::from_millis(1500)).await;
                if count(Event::ReplicaRestEnd) == before && held() == all {
                    break before;
                }
            }
        };
        let ended = rested().await;
        let epochs = count(Event::ShardEpochChange);
        let target = (index + 1) % 3;
        logs[index]
            .step_down(status.view, cluster.node(target))
            .unwrap()
            .await
            .unwrap();
        let (next, _) = leader(&logs, Some(status.view)).await;
        assert_eq!(next, target);
        while count(Event::ShardEpochChange) == epochs {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let ended = rested().await - ended;
        // The logs that share the part may speak again, and so may the
        // four partitions. The other logs must not, and they are most.
        assert!(sharing <= 4, "{sharing} logs share the part");
        assert!(ended >= 4, "the log itself spoke");
        assert!(ended <= 8 * (sharing + 4), "{ended} agreements ended");
        cluster.shutdown().await;
    })
    .await
    .expect("resting logs made no bounded progress");
}
