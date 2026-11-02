//! Network partitions between live brokers. A leader cut off from its peers
//! keeps running and keeps its client links, yet loses its partitions to the
//! other two, and rejoins once the cut heals.

use super::*;

/// The broker leading `partition`, by index.
async fn leading(
    clock: &SdkClock,
    cluster: &broker::Cluster,
    client: &Client,
    partition: u32,
) -> usize {
    let leader = progress(clock, client.leader(partition)).await;
    (0..cluster.configs.len())
        .find(|&index| cluster.node(index) == leader)
        .unwrap()
}

/// Wait until broker `index` has fresh clock samples from both peers, or from
/// neither.
async fn until_heard(clock: &SdkClock, cluster: &broker::Cluster, index: usize, heard: bool) {
    progress(clock, async {
        while (cluster.brokers[index].clock().offsets.iter())
            .any(|(_, offset)| offset.is_some() != heard)
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

/// Cut `target` from both peers, write through the other two, heal, and write
/// again. Every record must confirm once and replay in order.
async fn partitioned(isolate_leader: bool) {
    let clock = SdkClock::manual();
    let cluster = progress(
        &clock,
        broker::Cluster::configured(DeploymentMode::Three, Some(clock.clone()), |_| {}),
    )
    .await;
    let mut client = progress(
        &clock,
        Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
    )
    .await;
    let pending = progress(&clock, client.queue(0)).await;
    progress(&clock, client.confirm(pending)).await;
    let leader = leading(&clock, &cluster, &client, 0).await;
    let target = if isolate_leader {
        leader
    } else {
        (leader + 1) % 3
    };
    cluster.isolate(target, true);
    // Clock samples ride the control plane: none reach the cut-off broker.
    until_heard(&clock, &cluster, target, false).await;
    let pending = progress(&clock, client.queue(1)).await;
    progress(&clock, client.confirm(pending)).await;
    let during = leading(&clock, &cluster, &client, 0).await;
    if isolate_leader {
        assert_ne!(during, target, "the cut-off leader still leads");
    } else {
        assert_eq!(during, leader, "cutting a follower moved the leader");
    }
    cluster.isolate(target, false);
    until_heard(&clock, &cluster, target, true).await;
    let pending = progress(&clock, client.queue(2)).await;
    progress(&clock, client.confirm(pending)).await;
    progress(&clock, client.replay()).await;
    progress(&clock, client.close()).await;
    progress(&clock, cluster.shutdown()).await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_an_isolated_leader_is_replaced_and_rejoins() {
    tokio::time::timeout(Duration::from_secs(60), partitioned(true))
        .await
        .expect("an isolated leader stopped progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_an_isolated_follower_leaves_writes_alone() {
    tokio::time::timeout(Duration::from_secs(60), partitioned(false))
        .await
        .expect("an isolated follower stopped progress");
}
