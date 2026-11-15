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
