//! The clock guard, with real brokers on manual per-broker wall clocks. A broker
//! whose clock disagrees stops clock-driven decisions and keeps serving.

use super::*;
use futures::FutureExt;

/// Far past every tolerance and retention age below, and small enough that a
/// slow clock stays above zero.
const SKEW: i64 = 500_000;

const AGE: ozzy_config::TopicRetention = ozzy_config::TopicRetention {
    max_age_secs: Some(5),
    max_bytes: None,
};

struct Guarded {
    clock: SdkClock,
    cluster: broker::Cluster,
    client: Client,
    writer: SharedTopicWriter,
}

impl Guarded {
    async fn start(retention: ozzy_config::TopicRetention) -> Self {
        Self::start_held(None, retention).await
    }

    async fn start_held(
        held: Option<(usize, usize)>,
        retention: ozzy_config::TopicRetention,
    ) -> Self {
        let clock = SdkClock::manual();
        let cluster = held_cluster(DeploymentMode::Three, &clock, 64, held, Some(retention)).await;
        let client = progress(
            &clock,
            Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
        )
        .await;
        let writer = writer(&clock, &client).await;
        let guarded = Self {
            clock,
            cluster,
            client,
            writer,
        };
        for index in 0..3 {
            guarded.until_paused(index, false).await;
        }
        guarded
    }

    /// Advance time until broker `index` reaches this pause state.
    async fn until_paused(&self, index: usize, paused: bool) {
        progress(&self.clock, async {
            while self.cluster.brokers[index].clock().paused != paused {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }

    /// Set broker `index`'s wall clock `skew` milliseconds from true time.
    fn skew(&self, index: usize, skew: i64) {
        self.cluster
            .set_wall_clock(index, EPOCH_MILLIS.checked_add_signed(skew).unwrap());
    }

    async fn leader(&self) -> usize {
        leader(&self.clock, &self.cluster, &self.client).await
    }

    async fn floor(&self) -> u64 {
        progress(&self.clock, self.client.retained_floor())
            .await
            .get()
    }

    async fn finish(self) {
        let Self {
            clock,
            cluster,
            client,
            writer,
        } = self;
        drop(writer);
        progress(&clock, client.close()).await;
        progress(&clock, cluster.shutdown()).await;
    }
}

/// `reader` gets nothing before broker time `until`.
async fn nothing_until(clock: &SdkClock, reader: &mut TopicReader, until: u64) {
    tokio::select! {
        record = reader.next() => panic!("released early: {:?}", record.map(|r| r.offset)),
        () = wait_until(clock, until) => {}
    }
}

/// A leader's clock goes bad: age retention, release, and new schedules stop
/// while other writes and reads go on. Fixing it resumes them on time, and every
/// record arrives once.
async fn paused_leader(skew: i64) {
    let mut guarded = Guarded::start(AGE).await;
    confirm(&guarded.clock, &mut guarded.writer, [record(1, None)]).await;
    let leader = guarded.leader().await;

    // Past the age by true time. A fast clock alone would have expired it.
    guarded.skew(leader, skew);
    guarded.until_paused(leader, true).await;
    assert_eq!(guarded.cluster.brokers[leader].clock().offsets.len(), 2);
    wait_until(&guarded.clock, broker_now(&guarded.clock) + 8_000).await;
    assert_eq!(guarded.floor().await, 0, "age retention ran while paused");

    guarded.skew(leader, 0);
    guarded.until_paused(leader, false).await;
    progress(&guarded.clock, async {
        while guarded.client.retained_floor().await.get() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;

    let now = broker_now(&guarded.clock);
    let due = now + 6_000;
    let receipts = confirm(&guarded.clock, &mut guarded.writer, [record(2, Some(due))]).await;
    assert_eq!(receipts[0].0, AppendDisposition::Scheduled);
    assert!(
        broker_now(&guarded.clock) < due,
        "released before the fault"
    );

    // A step shows on the leader's next read: nothing comes due early.
    guarded.skew(leader, skew);
    let receipts = confirm(&guarded.clock, &mut guarded.writer, [record(3, None)]).await;
    assert_eq!(receipts, [(AppendDisposition::Stored, 1)], "writes stopped");
    let mut reader = partition_reader(&guarded.clock, &guarded.client).await;
    let stored = progress(&guarded.clock, reader.next()).await.unwrap();
    assert_eq!(stored.message_id.as_bytes()[0], 3, "reads stopped");
    guarded.until_paused(leader, true).await;

    // New scheduled records retry rather than take a stamp from this clock.
    let requests = guarded.writer.partition_stats(0).unwrap().requests;
    let waiting = guarded
        .writer
        .send_to_partition(record(4, Some(due + 1_000)), 0)
        .await
        .unwrap();
    progress(&guarded.clock, async {
        while guarded.writer.partition_stats(0).unwrap().requests < requests + 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    nothing_until(&guarded.clock, &mut reader, due + 4_000).await;
    assert!(waiting.confirmed().now_or_never().is_none());

    guarded.skew(leader, 0);
    guarded.until_paused(leader, false).await;
    let fixed = broker_now(&guarded.clock);
    let released = progress(&guarded.clock, reader.next()).await.unwrap();
    assert_eq!(released.message_id.as_bytes()[0], 2, "held record lost");
    assert_eq!(released.offset.get(), 2);
    let receipt = progress(&guarded.clock, waiting.confirmed()).await.unwrap();
    assert_eq!(receipt.record.disposition, AppendDisposition::Scheduled);
    let released = progress(&guarded.clock, reader.next()).await.unwrap();
    assert_eq!(
        released.message_id.as_bytes()[0],
        4,
        "scheduled record lost"
    );
    assert_eq!(released.offset.get(), 3);
    // Far less than the skew: no bad stamp keys these records.
    assert!(broker_now(&guarded.clock) < fixed + 30_000);
    reader.close().await.unwrap();
    // A repeated release would take the next offset first.
    let receipts = confirm(&guarded.clock, &mut guarded.writer, [record(5, None)]).await;
    assert_eq!(receipts, [(AppendDisposition::Stored, 4)]);
    guarded.finish().await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_fast_leader_pauses_release_and_age_retention() {
    tokio::time::timeout(Duration::from_secs(60), paused_leader(SKEW))
        .await
        .expect("fast leader made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_slow_leader_pauses_release_and_age_retention() {
    tokio::time::timeout(Duration::from_secs(60), paused_leader(-SKEW))
        .await
        .expect("slow leader made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_fast_follower_leaves_the_leader_on_time() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut guarded = Guarded::start(AGE).await;
        let leader = guarded.leader().await;
        let follower = (leader + 1) % 3;
        guarded.skew(follower, SKEW);
        guarded.until_paused(follower, true).await;
        let now = broker_now(&guarded.clock);
        let due = now + 3_000;
        confirm(&guarded.clock, &mut guarded.writer, [record(1, Some(due))]).await;
        let records = released(&guarded.clock, &guarded.client, 1).await;
        assert_eq!(records[0].0, 1);
        assert!(records[0].2 >= due, "released before its due time");
        // The other two clocks never moved.
        let status = guarded.cluster.brokers[leader].clock();
        assert!(!status.paused, "{status:?}");
        assert!(
            !guarded.cluster.brokers[3 - leader - follower]
                .clock()
                .paused
        );
        assert!(guarded.cluster.brokers[follower].clock().paused);
        guarded.finish().await;
    })
    .await
    .expect("fast follower made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_samples_lost_from_both_peers_pause_until_they_return() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut guarded = Guarded::start(AGE).await;
        // Higher index first; see `Cluster::stop`.
        let (clock, cluster) = (&guarded.clock, &mut guarded.cluster);
        let second = progress(clock, cluster.stop(2)).await;
        let first = progress(clock, cluster.stop(1)).await;
        guarded.until_paused(0, true).await;
        let status = guarded.cluster.brokers[0].clock();
        assert!(status.offsets.iter().all(|(_, offset)| offset.is_none()));
        let (clock, cluster) = (&guarded.clock, &mut guarded.cluster);
        progress(clock, cluster.restore_clean(1, first)).await;
        progress(clock, cluster.restore_clean(2, second)).await;
        guarded.until_paused(0, false).await;
        guarded.finish().await;
    })
    .await
    .expect("lost samples made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_offsets_inside_the_tolerance_never_pause() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut guarded = Guarded::start(AGE).await;
        guarded.skew(0, 400);
        guarded.skew(1, -400);
        let due = broker_now(&guarded.clock) + 3_000;
        confirm(
            &guarded.clock,
            &mut guarded.writer,
            [record(1, Some(due)), record(2, None)],
        )
        .await;
        let until = broker_now(&guarded.clock) + 6_000;
        progress(&guarded.clock, async {
            while broker_now(&guarded.clock) < until {
                for broker in &guarded.cluster.brokers {
                    assert!(!broker.clock().paused, "{:?}", broker.clock());
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let records = released(&guarded.clock, &guarded.client, 2).await;
        let ids: Vec<_> = records.iter().map(|&(id, _, _)| id).collect();
        assert_eq!(ids, [2, 1]);
        guarded.finish().await;
    })
    .await
    .expect("agreeing clocks made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_a_restart_with_a_fast_clock_pauses_from_the_first_turn() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut guarded = Guarded::start(AGE).await;
        let (clock, cluster) = (&guarded.clock, &mut guarded.cluster);
        let image = progress(clock, cluster.stop(2)).await;
        guarded.skew(2, SKEW);
        let (clock, cluster) = (&guarded.clock, &mut guarded.cluster);
        progress(clock, cluster.restore_clean(2, image)).await;
        assert!(guarded.cluster.brokers[2].clock().paused);
        // Stays paused once samples arrive, while the other two carry on.
        progress(&guarded.clock, async {
            while guarded.cluster.brokers[2]
                .clock()
                .offsets
                .iter()
                .any(|(_, offset)| offset.is_none())
            {
                assert!(guarded.cluster.brokers[2].clock().paused);
                tokio::task::yield_now().await;
            }
        })
        .await;
        let status = guarded.cluster.brokers[2].clock();
        assert!(status.paused, "{status:?}");
        assert!(!guarded.cluster.brokers[0].clock().paused);
        assert!(!guarded.cluster.brokers[1].clock().paused);
        guarded.skew(2, 0);
        guarded.until_paused(2, false).await;
        guarded.finish().await;
    })
    .await
    .expect("restart with a fast clock made no bounded progress");
}

/// Byte retention reads no clock, and a carry toward it needs none.
mod stress {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn inproc_memory_a_paused_leader_still_retires_by_bytes() {
        tokio::time::timeout(Duration::from_secs(90), async {
            let retention = ozzy_config::TopicRetention {
                max_age_secs: None,
                max_bytes: Some(1024 * 1024),
            };
            let mut guarded = Guarded::start_held(Some((4, 8)), retention).await;
            let leader = guarded.leader().await;
            let now = broker_now(&guarded.clock);
            let stored = (1..=4_u8).map(|id| record(id, None));
            let held = (5..=12_u8).map(|id| record(id, Some(now + 50_000)));
            confirm(&guarded.clock, &mut guarded.writer, stored.chain(held)).await;
            guarded.skew(leader, SKEW);
            guarded.until_paused(leader, true).await;
            // Eight held rows rolled the segment before this write.
            confirm(&guarded.clock, &mut guarded.writer, [record(13, None)]).await;
            let client = &guarded.client;
            progress(&guarded.clock, async {
                while client.retained_floor().await.get() == 0 {
                    assert!(broker_now(&guarded.clock) < now + 50_000);
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(guarded.cluster.brokers[leader].clock().paused);
            guarded.finish().await;
        })
        .await
        .expect("byte retention on a paused leader made no bounded progress");
    }
}
