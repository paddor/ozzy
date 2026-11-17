//! Logs created and deleted while the brokers run: `Broker::create_log`,
//! `delete_log`, `logs` and `runtime_log`, in single and three mode.

use super::log::{append, appends_and_reads, leader, read_all};
use super::*;
use ozzy_broker::log::{Log, LogRole};
use ozzy_broker::{CreateError, DeleteError, LogSpec, LogState};
use ozzy_proto::{GroupId, Offset, PartitionIncarnation};

fn class(max_logs: usize) -> ozzy_config::LogClass {
    ozzy_config::LogClass {
        segment_bytes: 4 * 1024 * 1024,
        max_append_bytes: 1024 * 1024,
        max_logs,
    }
}

pub(super) async fn cluster(mode: DeploymentMode, max_logs: usize) -> broker::Cluster {
    broker::Cluster::configured(mode, None, |config| config.logs = Some(class(max_logs))).await
}

pub(super) fn spec(id: u8, members: &[ozzy_proto::NodeId]) -> LogSpec {
    LogSpec {
        id: GroupId::from_bytes([id; 16]),
        incarnation: PartitionIncarnation::from_bytes([id + 100; 16]),
        members: members.to_vec(),
        segment_bytes: None,
    }
}

/// Handles of log `id` on every running broker, in broker order.
pub(super) fn handles(cluster: &broker::Cluster, id: GroupId) -> Vec<Log> {
    cluster
        .brokers
        .iter()
        .map(|broker| broker.runtime_log(id).unwrap())
        .collect()
}

/// Broker `index`'s copy of `id`, once its shard opened it.
pub(super) async fn copy(cluster: &broker::Cluster, index: usize, id: GroupId) -> Log {
    loop {
        if let Some(log) = cluster.brokers[index].runtime_log(id) {
            return log;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A replicated log created on all three brokers elects the first member,
/// takes appends, comes back after a follower restart, and is gone after
/// its deletion, with a tombstone on every broker.
#[tokio::test(flavor = "current_thread")]
async fn a_replicated_log_is_created_served_and_deleted_at_run_time() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = cluster(DeploymentMode::Three, 4).await;
        let nodes = [cluster.node(1), cluster.node(2), cluster.node(0)];
        let stream = spec(1, &nodes);
        assert_eq!(cluster.brokers[0].logs(), vec![]);
        for broker in &cluster.brokers {
            assert_eq!(broker.create_log(stream.clone()).await, Ok(()));
            // The same spec again is the same log.
            assert_eq!(broker.create_log(stream.clone()).await, Ok(()));
            assert_eq!(broker.logs(), vec![(stream.id, LogState::Ready)]);
            let other = LogSpec {
                incarnation: PartitionIncarnation::from_bytes([9; 16]),
                ..stream.clone()
            };
            assert_eq!(broker.create_log(other).await, Err(CreateError::Exists));
            let resized = LogSpec {
                segment_bytes: Some(64 * 1024 * 1024),
                ..stream.clone()
            };
            assert_eq!(broker.create_log(resized).await, Err(CreateError::Exists));
        }
        let logs = handles(&cluster, stream.id);
        let (index, view, end) = appends_and_reads(&logs).await;
        assert_eq!(index, 1, "the first member leads the first view");
        // The configured topic still works beside it.
        let orders: Vec<_> = cluster
            .brokers
            .iter()
            .map(|broker| broker.log("orders", 0).unwrap())
            .collect();
        appends_and_reads(&orders).await;

        // A follower reopens the log from its record and rejoins.
        let image = cluster.stop(2).await;
        let appended = append(&logs[index], view, end, &[4]).await.unwrap();
        cluster.restore_clean(2, image).await;
        // Recorded logs are back once the broker is ready.
        cluster.brokers[2].ready().await.unwrap();
        assert_eq!(
            cluster.brokers[2].logs(),
            vec![(stream.id, LogState::Ready)]
        );
        let mut status = cluster.brokers[2].runtime_log(stream.id).unwrap().status();
        let rejoined = *status
            .wait_for(|status| status.role == LogRole::Follower)
            .await
            .unwrap();
        assert_eq!(rejoined.view, view);
        // Only the leader and the returned follower are left to commit, so
        // the follower first repairs what it missed.
        let image = cluster.stop(0).await;
        let last = append(&logs[index], view, appended.end.get(), &[5])
            .await
            .unwrap();
        assert_eq!(last.end.get(), end + 2);
        cluster.restore_clean(0, image).await;
        cluster.brokers[0].ready().await.unwrap();

        for broker in &cluster.brokers {
            assert_eq!(broker.delete_log(stream.id).await, Ok(()));
            assert_eq!(broker.delete_log(stream.id).await, Ok(()));
            assert!(broker.runtime_log(stream.id).is_none());
            assert_eq!(broker.logs(), vec![(stream.id, LogState::Deleted)]);
            assert_eq!(
                broker.create_log(stream.clone()).await,
                Err(CreateError::Deleted)
            );
            assert_eq!(
                broker.delete_log(GroupId::from_bytes([77; 16])).await,
                Err(DeleteError::Unknown)
            );
        }
        // An old handle no longer reaches a log.
        let stopped = *logs[index].status().borrow();
        assert_eq!(stopped.role, LogRole::Stopped);
        // The tombstone survives a restart.
        let image = cluster.stop(2).await;
        cluster.restore_clean(2, image).await;
        // Recorded logs are back once the broker is ready.
        cluster.brokers[2].ready().await.unwrap();
        assert_eq!(
            cluster.brokers[2].logs(),
            vec![(stream.id, LogState::Deleted)]
        );
        // The configured topic outlived the log beside it.
        let orders: Vec<_> = cluster
            .brokers
            .iter()
            .map(|broker| broker.log("orders", 0).unwrap())
            .collect();
        let (index, status) = leader(&orders, None).await;
        let end = status.committed_end.get();
        append(&orders[index], status.view, end, &[8])
            .await
            .unwrap();
        cluster.shutdown().await;
    })
    .await
    .expect("runtime log made no bounded progress");
}

/// A log with one member lives on that broker alone, beside replicated
/// logs, up to the configured number of logs.
#[tokio::test(flavor = "current_thread")]
async fn a_local_log_lives_on_its_one_member_in_a_three_broker_cluster() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let cluster = cluster(DeploymentMode::Three, 2).await;
        let own = spec(2, &[cluster.node(0)]);
        assert_eq!(
            cluster.brokers[1].create_log(own.clone()).await,
            Err(CreateError::Invalid("the only member is another broker"))
        );
        let two = spec(3, &[cluster.node(0), cluster.node(1)]);
        assert!(matches!(
            cluster.brokers[0].create_log(two).await,
            Err(CreateError::Invalid(_))
        ));
        for bytes in [4096, 1024 * 1024 + 1, 1024 * 1024] {
            let small = LogSpec {
                segment_bytes: Some(bytes),
                ..own.clone()
            };
            assert!(matches!(
                cluster.brokers[0].create_log(small).await,
                Err(CreateError::Invalid(_))
            ));
        }
        assert_eq!(cluster.brokers[0].create_log(own.clone()).await, Ok(()));
        let log = cluster.brokers[0].runtime_log(own.id).unwrap();
        let (_, view, end) = appends_and_reads(std::slice::from_ref(&log)).await;
        assert_eq!(view, 0);
        let nodes = [cluster.node(0), cluster.node(1), cluster.node(2)];
        let shared = spec(4, &nodes);
        for broker in &cluster.brokers {
            assert_eq!(broker.create_log(shared.clone()).await, Ok(()));
        }
        appends_and_reads(&handles(&cluster, shared.id)).await;
        assert_eq!(
            cluster.brokers[0]
                .create_log(spec(5, &[cluster.node(0)]))
                .await,
            Err(CreateError::Full)
        );
        // Deleting the local log makes room and leaves the shared one alone.
        assert_eq!(cluster.brokers[0].delete_log(own.id).await, Ok(()));
        assert_eq!(
            cluster.brokers[0]
                .create_log(spec(5, &[cluster.node(0)]))
                .await,
            Ok(())
        );
        // The deleted log's handle reaches nothing.
        assert_eq!(log.status().borrow().role, LogRole::Stopped);
        assert!(end > 0 && log.read(Offset::ZERO, Offset::new(end), 1, 64).is_err());
        cluster.shutdown().await;
    })
    .await
    .expect("local runtime log made no bounded progress");
}

/// A single broker creates logs, keeps them and their records across a
/// restart, and refuses them without a log class.
#[tokio::test(flavor = "current_thread")]
async fn a_single_broker_keeps_its_runtime_logs_across_a_restart() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let plain = broker::Cluster::new(DeploymentMode::Single).await;
        let node = plain.node(0);
        assert_eq!(
            plain.brokers[0].create_log(spec(1, &[node])).await,
            Err(CreateError::Disabled)
        );
        plain.shutdown().await;

        let mut cluster = cluster(DeploymentMode::Single, 3).await;
        let node = cluster.node(0);
        let (kept, dropped) = (spec(1, &[node]), spec(2, &[node]));
        for spec in [&kept, &dropped] {
            assert_eq!(cluster.brokers[0].create_log(spec.clone()).await, Ok(()));
        }
        let log = cluster.brokers[0].runtime_log(kept.id).unwrap();
        let (_, view, end) = appends_and_reads(std::slice::from_ref(&log)).await;
        assert_eq!(cluster.brokers[0].delete_log(dropped.id).await, Ok(()));
        let image = cluster.stop(0).await;
        cluster.restore_clean(0, image).await;
        // Recorded logs are back once the broker is ready.
        cluster.brokers[0].ready().await.unwrap();
        assert_eq!(
            cluster.brokers[0].logs(),
            vec![(kept.id, LogState::Ready), (dropped.id, LogState::Deleted)]
        );
        let log = cluster.brokers[0].runtime_log(kept.id).unwrap();
        let (_, status) = leader(std::slice::from_ref(&log), None).await;
        assert_eq!((status.view, status.committed_end.get()), (view, end));
        assert_eq!(read_all(&log, 0, end).await.len() as u64, end);
        let appended = append(&log, view, end, &[7]).await.unwrap();
        assert_eq!(appended.end.get(), end + 1);
        cluster.shutdown().await;
    })
    .await
    .expect("single runtime log made no bounded progress");
}

/// A replicated log on every broker, led by broker 1, with three records.
async fn led_log(cluster: &broker::Cluster) -> (LogSpec, u64, u64) {
    let nodes = [cluster.node(1), cluster.node(2), cluster.node(0)];
    let stream = spec(1, &nodes);
    for broker in &cluster.brokers {
        assert_eq!(broker.create_log(stream.clone()).await, Ok(()));
    }
    let (index, view, end) = appends_and_reads(&handles(cluster, stream.id)).await;
    assert_eq!(index, 1, "the first member leads the first view");
    (stream, view, end)
}

/// The status of broker `index`'s copy of `id` once it leads.
async fn leads(cluster: &broker::Cluster, index: usize, id: GroupId) -> (Log, u64, u64) {
    cluster.brokers[index].ready().await.unwrap();
    let log = cluster.brokers[index].runtime_log(id).unwrap();
    let mut status = log.status();
    let leading = *status
        .wait_for(|status| status.role == LogRole::Leader)
        .await
        .unwrap();
    (log, leading.view, leading.committed_end.get())
}

/// A leader that stops cleanly and comes straight back leads the same view
/// again: it stored everything it sent. Its followers, which never
/// stopped, take its new writer incarnation, and either is enough to
/// commit.
#[tokio::test(flavor = "current_thread")]
async fn a_replicated_log_keeps_its_view_across_a_clean_leader_restart() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = cluster(DeploymentMode::Three, 4).await;
        let (stream, view, end) = led_log(&cluster).await;
        let image = cluster.stop(1).await;
        cluster.restore_clean(1, image).await;
        let (log, resumed, committed) = leads(&cluster, 1, stream.id).await;
        assert_eq!(resumed, view, "the clean restart moved the view");
        assert_eq!(committed, end);
        let appended = append(&log, view, end, &[4]).await.unwrap();
        assert_eq!(appended.end.get(), end + 1);
        for follower in [2, 0] {
            let image = cluster.stop(follower).await;
            let at = appended.end.get() + u64::from(follower == 0);
            let last = append(&log, view, at, &[5]).await.unwrap();
            assert_eq!(last.end.get(), at + 1);
            cluster.restore_clean(follower, image).await;
            cluster.brokers[follower].ready().await.unwrap();
        }
        assert_eq!(read_all(&log, 0, end + 3).await.len() as u64, end + 3);
        let leading = *log.status().borrow();
        assert_eq!((leading.role, leading.view), (LogRole::Leader, view));
        cluster.shutdown().await;
    })
    .await
    .expect("clean leader restart made no bounded progress");
}

/// After every broker stopped cleanly and started again, the log has its
/// leader and its view back without an election, twice in a row.
#[tokio::test(flavor = "current_thread")]
async fn a_replicated_log_keeps_its_view_across_a_cluster_restart() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = cluster(DeploymentMode::Three, 4).await;
        let (stream, view, mut end) = led_log(&cluster).await;
        for round in 0..2 {
            let mut images = Vec::new();
            for index in [2, 1, 0] {
                images.push(cluster.stop(index).await);
            }
            for index in 0..3 {
                cluster.restore_clean(index, images.pop().unwrap()).await;
            }
            let (log, resumed, committed) = leads(&cluster, 1, stream.id).await;
            assert_eq!(resumed, view, "restart {round} moved the view");
            assert_eq!(committed, end);
            assert_eq!(read_all(&log, 0, end).await.len() as u64, end);
            let appended = append(&log, view, end, &[6 + round]).await.unwrap();
            end = appended.end.get();
        }
        cluster.shutdown().await;
    })
    .await
    .expect("cluster restart made no bounded progress");
}

/// A leader stops cleanly and comes back to a single follower that lacks
/// the end of its log. The leader resumes its view, but its log is on disk
/// only and the follower's acknowledgment falls inside it, so the view
/// cannot commit. The two elect the next view, which holds the whole log.
#[tokio::test(flavor = "current_thread")]
async fn a_resumed_leader_serves_a_follower_that_is_behind() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = cluster(DeploymentMode::Three, 4).await;
        let (stream, view, end) = led_log(&cluster).await;
        cluster.isolate(0, true);
        let leading = cluster.brokers[1].runtime_log(stream.id).unwrap();
        let ahead = append(&leading, view, end, &[4]).await.unwrap().end.get();
        cluster.stop(2).await;
        let image = cluster.stop(1).await;
        cluster.isolate(0, false);
        cluster.restore_clean(1, image).await;
        copy(&cluster, 1, stream.id).await;
        let logs = handles(&cluster, stream.id);
        let (index, status) = leader(&logs, None).await;
        assert_eq!(
            status.committed_end.get(),
            ahead,
            "the new view lost a record"
        );
        let log = &logs[index];
        let appended = append(log, status.view, ahead, &[5]).await.unwrap();
        assert_eq!(appended.end.get(), ahead + 1);
        assert_eq!(read_all(log, 0, ahead + 1).await.len() as u64, ahead + 1);
        cluster.shutdown().await;
    })
    .await
    .expect("resumed leader made no bounded progress");
}
