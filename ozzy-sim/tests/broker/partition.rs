//! Network partitions between live brokers. Cuts drop broker traffic only:
//! every broker keeps running and keeps its client links. A view moves only
//! when two brokers suspect the leader, so a cut that leaves the leader one
//! follower in both directions must not move it, while a leader that can
//! neither hear nor reach a quorum must lose its partitions.

use super::*;
use futures::FutureExt;
use ozzy_runtime::replicated::TopicReader;

/// Three brokers on manual time and one SDK client writing waves.
struct Split {
    clock: SdkClock,
    cluster: broker::Cluster,
    client: Client,
    wave: usize,
}

impl Split {
    async fn start() -> Self {
        Self::start_isolated(None).await
    }

    /// Start with broker `isolated` cut off, when given.
    async fn start_isolated(isolated: Option<usize>) -> Self {
        let clock = SdkClock::manual();
        let manual = Some(clock.clone());
        let cluster = progress(&clock, async {
            match isolated {
                Some(index) => {
                    broker::Cluster::configured_isolated(
                        DeploymentMode::Three,
                        manual,
                        index,
                        |_| {},
                    )
                    .await
                }
                None => broker::Cluster::configured(DeploymentMode::Three, manual, |_| {}).await,
            }
        })
        .await;
        let client = progress(
            &clock,
            Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
        )
        .await;
        let mut split = Self {
            clock,
            cluster,
            client,
            wave: 0,
        };
        // Leadership can still move until the first write confirms.
        split.write().await;
        // Every broker link is up before a scenario cuts one.
        for index in (0..3).filter(|&index| Some(index) != isolated) {
            for peer in (0..3).filter(|&peer| peer != index && Some(peer) != isolated) {
                split.until_hears(index, peer, true).await;
            }
        }
        split
    }

    /// Admit one wave to every partition without waiting for it.
    async fn queue(&mut self) -> Vec<ozzy_sim::client::Pending> {
        self.wave += 1;
        progress(&self.clock, self.client.queue(self.wave)).await
    }

    /// Write one wave to every partition and confirm it.
    async fn write(&mut self) {
        let pending = self.queue().await;
        progress(&self.clock, self.client.confirm(pending)).await;
    }

    /// The broker leading `partition`, by index.
    async fn leading(&self, partition: u32) -> usize {
        let leader = progress(&self.clock, self.client.leader(partition)).await;
        (0..self.cluster.configs.len())
            .find(|&index| self.cluster.node(index) == leader)
            .unwrap()
    }

    /// Wait until broker `index` has a fresh clock sample from `peer`, or
    /// none. Samples ride the control plane, so this shows the link's state.
    async fn until_hears(&self, index: usize, peer: usize, heard: bool) {
        let peer = self.cluster.node(peer);
        progress(&self.clock, async {
            while (self.cluster.brokers[index].clock().offsets.iter())
                .any(|&(node, offset)| node == peer && offset.is_some() != heard)
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }

    /// Wait until broker `index` hears both peers, or neither.
    async fn until_heard(&self, index: usize, heard: bool) {
        for peer in (0..3).filter(|&peer| peer != index) {
            self.until_hears(index, peer, heard).await;
        }
    }

    /// Read and check every confirmed record past `positions`.
    async fn read(&self, reader: &mut TopicReader, positions: Vec<usize>) {
        progress(&self.clock, self.client.read(reader, positions)).await;
    }

    /// Advance broker time by `millis`.
    async fn pass(&self, millis: u64) {
        let deadline = self.clock.now() + Duration::from_millis(millis);
        progress(&self.clock, self.clock.until(deadline)).await;
    }

    /// Write once more, then check every confirmed record replays once and
    /// in order.
    async fn finish(mut self) {
        self.write().await;
        progress(&self.clock, self.client.replay()).await;
        progress(&self.clock, self.client.close()).await;
        progress(&self.clock, self.cluster.shutdown()).await;
    }
}

/// Run `scenario` with a bounded real-time budget, boxed: broker scenarios
/// hold large futures.
fn bounded<'a>(
    scenario: impl std::future::Future<Output = ()> + 'a,
    what: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
    Box::pin(async move {
        tokio::time::timeout(Duration::from_secs(60), scenario)
            .await
            .unwrap_or_else(|_| panic!("{what} stopped progress"));
    })
}

/// Cut `target` from both peers, write through the other two, heal, and write
/// again.
async fn isolated(isolate_leader: bool) {
    let mut split = Split::start().await;
    let leader = split.leading(0).await;
    let target = if isolate_leader {
        leader
    } else {
        (leader + 1) % 3
    };
    split.cluster.isolate(target, true);
    split.until_heard(target, false).await;
    split.write().await;
    let during = split.leading(0).await;
    if isolate_leader {
        assert_ne!(during, target, "the cut-off leader still leads");
    } else {
        assert_eq!(during, leader, "cutting a follower moved the leader");
    }
    split.cluster.isolate(target, false);
    split.until_heard(target, true).await;
    split.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_an_isolated_leader_is_replaced_and_rejoins() {
    bounded(isolated(true), "an isolated leader").await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_an_isolated_follower_leaves_writes_alone() {
    bounded(isolated(false), "an isolated follower").await;
}

/// A cut the leader survives with one follower in both directions: only one
/// broker can suspect it, so it keeps leading through several waves, and the
/// cut-off side catches up once healed.
async fn leader_keeps(cut: impl Fn(&broker::Cluster, usize, bool)) {
    let mut split = Split::start().await;
    let leader = split.leading(0).await;
    cut(&split.cluster, leader, true);
    for _ in 0..3 {
        split.write().await;
        split.pass(2_000).await;
    }
    assert_eq!(
        split.leading(0).await,
        leader,
        "one suspicion moved the view"
    );
    cut(&split.cluster, leader, false);
    for index in 0..3 {
        split.until_heard(index, true).await;
    }
    split.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_leader_cut_from_one_follower_keeps_leading() {
    bounded(
        leader_keeps(|cluster, leader, cut| cluster.cut(leader, (leader + 1) % 3, cut)),
        "a leader cut from one follower",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_followers_cut_from_each_other_leave_the_leader_alone() {
    bounded(
        leader_keeps(|cluster, leader, cut| {
            cluster.cut((leader + 1) % 3, (leader + 2) % 3, cut);
        }),
        "followers cut from each other",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_follower_that_cannot_hear_the_leader_leaves_it_alone() {
    bounded(
        leader_keeps(|cluster, leader, cut| cluster.cut_one_way(leader, (leader + 1) % 3, cut)),
        "a follower deaf to the leader",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_follower_deaf_to_the_other_follower_leaves_the_leader_alone() {
    bounded(
        leader_keeps(|cluster, leader, cut| {
            cluster.cut_one_way((leader + 1) % 3, (leader + 2) % 3, cut);
        }),
        "a follower deaf to the other follower",
    )
    .await;
}

/// The leader loses one direction to both followers. It hears no vote when
/// `deaf`, or no follower hears it otherwise. Either way it cannot commit,
/// both followers come to suspect it, and the view must move while it runs.
async fn one_sided_leader(deaf: bool) {
    let mut split = Split::start().await;
    let leader = split.leading(0).await;
    for follower in (0..3).filter(|&index| index != leader) {
        if deaf {
            split.cluster.cut_one_way(follower, leader, true);
            split.until_hears(leader, follower, false).await;
        } else {
            split.cluster.cut_one_way(leader, follower, true);
            split.until_hears(follower, leader, false).await;
        }
    }
    split.write().await;
    assert_ne!(
        split.leading(0).await,
        leader,
        "a one-sided leader still leads"
    );
    split.cluster.isolate(leader, false);
    split.until_heard(leader, true).await;
    split.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_leader_that_hears_no_follower_is_replaced() {
    bounded(one_sided_leader(true), "a deaf leader").await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_leader_no_follower_hears_is_replaced() {
    bounded(one_sided_leader(false), "a mute leader").await;
}

/// With every broker cut from both others no quorum exists: writes admitted
/// meanwhile must not confirm. Once healed they confirm, each once.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_full_split_confirms_nothing_until_it_heals() {
    bounded(
        async {
            let mut split = Split::start().await;
            for index in 0..3 {
                split.cluster.isolate(index, true);
            }
            for index in 0..3 {
                split.until_heard(index, false).await;
            }
            let pending = split.queue().await;
            split.pass(10_000).await;
            assert!(
                pending
                    .iter()
                    .all(|pending| pending.record.confirmed().now_or_never().is_none()),
                "a write confirmed without a quorum"
            );
            for index in 0..3 {
                split.cluster.isolate(index, false);
            }
            progress(&split.clock, split.client.confirm(pending)).await;
            split.finish().await;
        },
        "a full split",
    )
    .await;
}

/// Each broker in turn is cut off while a wave goes through the other two and
/// rejoins before the next. Every confirmed record replays once, in order.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_cutting_off_each_broker_in_turn_keeps_every_write_once() {
    bounded(
        async {
            let mut split = Split::start().await;
            for round in 0..6 {
                let target = round % 3;
                split.cluster.isolate(target, true);
                split.until_heard(target, false).await;
                split.write().await;
                split.cluster.isolate(target, false);
                split.until_heard(target, true).await;
                split.write().await;
            }
            split.finish().await;
        },
        "cutting off each broker in turn",
    )
    .await;
}

/// A broker cut off from the start misses the views the other two form
/// without it. Once healed it must join every group, so that either of the
/// others can then be cut off in turn.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_broker_cut_off_from_the_start_joins_every_group_once_healed() {
    bounded(
        async {
            let mut split = Split::start_isolated(Some(2)).await;
            split.cluster.isolate(2, false);
            split.until_heard(2, true).await;
            for target in [0, 1] {
                split.cluster.isolate(target, true);
                split.until_heard(target, false).await;
                split.write().await;
                split.cluster.isolate(target, false);
                split.until_heard(target, true).await;
            }
            split.finish().await;
        },
        "a broker cut off from the start",
    )
    .await;
}

/// A broker that missed a view and cannot hear that view's primary joins it
/// through the follower it does hear. Once the primary is cut off too, the two
/// form a view of their own in every group. Brokers 0 and 1 elect while
/// broker 2 is cut off, so a group whose first primary is broker 2 moves on
/// to a view broker 0 leads.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_broker_that_missed_a_view_joins_it_through_a_follower() {
    bounded(
        async {
            let mut split = Split::start_isolated(Some(2)).await;
            split.cluster.cut(1, 2, false);
            split.until_hears(2, 1, true).await;
            split.until_hears(1, 2, true).await;
            split.cluster.isolate(0, true);
            split.until_heard(0, false).await;
            split.write().await;
            split.cluster.isolate(0, false);
            for index in 0..3 {
                split.until_heard(index, true).await;
            }
            split.finish().await;
        },
        "a broker that missed a view",
    )
    .await;
}

/// A reader opened before the leader is cut off keeps reading while the cut
/// lasts: the next leader's records reach it before the cut heals, and so do
/// the records written after, each once and in order.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_reader_moves_off_a_cut_off_leader() {
    bounded(
        async {
            let mut split = Split::start().await;
            let mut reader = progress(&split.clock, split.client.reader(false)).await;
            let start = vec![0; split.client.positions().len()];
            split.read(&mut reader, start).await;
            let leader = split.leading(0).await;
            split.cluster.isolate(leader, true);
            split.until_heard(leader, false).await;
            let positions = split.client.positions();
            split.write().await;
            split.read(&mut reader, positions).await;
            split.cluster.isolate(leader, false);
            split.until_heard(leader, true).await;
            let positions = split.client.positions();
            split.write().await;
            split.read(&mut reader, positions).await;
            progress(&split.clock, reader.close()).await.unwrap();
            split.finish().await;
        },
        "a reader on a cut-off leader",
    )
    .await;
}

/// Links flap while writes wait. Cuts run shorter than the primary timeout,
/// past it, and past the election timeout; heals run shorter and longer than a
/// heartbeat. Every queued write confirms once the links hold, once and in
/// order.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_flapping_links_keep_every_write_once() {
    bounded(
        async {
            let mut split = Split::start().await;
            let first = split.leading(0).await;
            // The broker cut off, or every broker when `None`, how long the cut
            // lasts, and how long the heal after it, in milliseconds.
            let flaps = [
                (Some(first), 600, 150),
                (None, 1_200, 50),
                (Some(first), 1_500, 250),
                (None, 2_500, 150),
                (Some((first + 1) % 3), 900, 100),
                (Some(first), 1_100, 50),
                (None, 700, 300),
            ];
            let mut pending = Vec::new();
            for (target, cut, healed) in flaps {
                pending.extend(split.queue().await);
                for index in target.map_or(0..3, |index| index..index + 1) {
                    split.cluster.isolate(index, true);
                }
                split.pass(cut).await;
                for index in 0..3 {
                    split.cluster.isolate(index, false);
                }
                split.pass(healed).await;
            }
            progress(&split.clock, split.client.confirm(pending)).await;
            for index in 0..3 {
                split.until_heard(index, true).await;
            }
            split.finish().await;
        },
        "flapping links",
    )
    .await;
}

/// The leader is cut off while its followers cannot hear each other either.
/// Each suspects it alone, and its election attempts go unanswered through
/// growing timeouts. Once the followers hear each other they form a view
/// without the leader, and the writes queued meanwhile confirm.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_followers_that_suspected_the_leader_apart_form_a_view_once_they_meet() {
    bounded(
        async {
            let mut split = Split::start().await;
            let leader = split.leading(0).await;
            for index in 0..3 {
                split.cluster.isolate(index, true);
            }
            for index in 0..3 {
                split.until_heard(index, false).await;
            }
            let pending = split.queue().await;
            // Past several election timeouts as they double.
            split.pass(10_000).await;
            split.cluster.cut((leader + 1) % 3, (leader + 2) % 3, false);
            progress(&split.clock, split.client.confirm(pending)).await;
            split.write().await;
            assert_ne!(
                split.leading(0).await,
                leader,
                "the cut-off leader still leads"
            );
            split.cluster.isolate(leader, false);
            split.until_heard(leader, true).await;
            split.finish().await;
        },
        "followers that suspected the leader apart",
    )
    .await;
}

/// A follower cut off from both peers loses power and restarts still cut off,
/// so it can recover nothing until the cut heals. Once healed it recovers from
/// its peers, and with the leader cut off it forms the quorum for the next
/// wave.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_follower_that_crashes_while_cut_off_rejoins_the_quorum() {
    bounded(
        async {
            let mut split = Split::start().await;
            let leader = split.leading(0).await;
            let follower = (leader + 1) % 3;
            split.cluster.isolate(follower, true);
            split.until_heard(follower, false).await;
            split.write().await;
            progress(&split.clock, split.cluster.crash(follower, true)).await;
            split.write().await;
            split.cluster.isolate(follower, false);
            split.until_heard(follower, true).await;
            progress(&split.clock, split.cluster.wait_recovered(follower)).await;
            split.cluster.isolate(leader, true);
            split.until_heard(leader, false).await;
            split.write().await;
            split.cluster.isolate(leader, false);
            split.until_heard(leader, true).await;
            split.finish().await;
        },
        "a follower that crashes while cut off",
    )
    .await;
}

/// A broker cut off from the start misses several waves. Once healed it loses
/// the leader again as soon as it hears it, so it can catch up only through
/// the other follower. With the leader then cut off, the two form the quorum
/// over the whole history.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_lagging_broker_cut_from_the_leader_catches_up_through_a_follower() {
    bounded(
        async {
            let mut split = Split::start_isolated(Some(2)).await;
            for _ in 0..4 {
                split.write().await;
            }
            let leader = split.leading(0).await;
            let follower = 1 - leader;
            split.cluster.isolate(2, false);
            split.until_hears(2, leader, true).await;
            split.cluster.cut(leader, 2, true);
            split.write().await;
            split.until_hears(2, follower, true).await;
            split.cluster.isolate(leader, true);
            split.until_heard(leader, false).await;
            split.write().await;
            split.cluster.isolate(leader, false);
            for index in 0..3 {
                split.until_heard(index, true).await;
            }
            split.finish().await;
        },
        "a lagging broker cut from the leader",
    )
    .await;
}

/// A leader cut off while the other two form a newer view stays primary of its
/// old view. It heals towards the newer view's backup just as that view's
/// primary is cut off, so it never hears the newer view. The backup suspects
/// the lost primary, and the old leader must form a view with it within a few
/// election timeouts, not wait for work it cannot commit.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_stale_leader_joins_the_view_it_missed_once_that_view_loses_its_leader() {
    bounded(
        async {
            let mut split = Split::start().await;
            let old = split.leading(0).await;
            split.cluster.isolate(old, true);
            split.until_heard(old, false).await;
            split.write().await;
            let new = split.leading(0).await;
            let backup = 3 - old - new;
            split.cluster.isolate(new, true);
            split.cluster.cut(old, backup, false);
            split.until_hears(old, backup, true).await;
            split.until_hears(backup, old, true).await;
            let since = split.clock.now();
            split.write().await;
            let waited = split.clock.now().saturating_sub(since);
            assert!(
                waited < Duration::from_secs(10),
                "the write waited {waited:?} of broker time"
            );
            split.cluster.isolate(new, false);
            for index in 0..3 {
                split.until_heard(index, true).await;
            }
            split.finish().await;
        },
        "a stale leader",
    )
    .await;
}
