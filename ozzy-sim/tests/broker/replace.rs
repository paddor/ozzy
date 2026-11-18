//! A broker that lost the store of a runtime log, or every store it had:
//! its copies come back nonvoting, take the log of the other two, and only
//! then vote again.

use super::log::{append, leader, read_all};
use super::runtime_logs::{cluster, copy, handles, led_log};
use super::*;
use ozzy_broker::log::{Log, LogRole};
use ozzy_proto::GroupId;

/// Wait until the copy behind `log` votes again, as a leader or as a
/// follower. A copy that recovered asks for the next view, so it may lead
/// it.
async fn votes(log: &Log) {
    let mut status = log.status();
    status
        .wait_for(|status| matches!(status.role, LogRole::Leader | LogRole::Follower))
        .await
        .unwrap();
}

/// The running copies of `id` elect a leader above `view` that holds
/// every record below `end`.
async fn elects_with(cluster: &broker::Cluster, id: GroupId, view: u64, end: u64) {
    let logs = handles(cluster, id);
    let (index, status) = leader(&logs, Some(view)).await;
    assert!(
        status.committed_end.get() >= end,
        "the new leader lost records"
    );
    assert_eq!(read_all(&logs[index], 0, end).await.len() as u64, end);
    let appended = append(&logs[index], status.view, status.committed_end.get(), &[9]).await;
    assert_eq!(appended.unwrap().end.get(), status.committed_end.get() + 1);
}

/// A copy whose store is gone when its broker starts is made again as a
/// nonvoting copy, takes the log of the other two, and votes: it and the
/// other follower then hold everything without the old leader.
#[tokio::test(flavor = "current_thread")]
async fn a_copy_that_lost_its_store_takes_the_log_of_the_other_two() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = cluster(DeploymentMode::Three, 4).await;
        let (stream, view, end) = led_log(&cluster, 1, [1, 2, 0]).await;
        let store = cluster.log_store(2, stream.id);
        cluster.restart_without(2, &store).await;
        votes(&copy(&cluster, 2, stream.id).await).await;
        cluster.stop(1).await;
        elects_with(&cluster, stream.id, view, end).await;
        cluster.shutdown().await;
    })
    .await
    .expect("lost store made no bounded progress");
}

/// A broker that stops in the middle of such a recovery resumes it at its
/// next start.
#[tokio::test(flavor = "current_thread")]
async fn a_restart_in_the_middle_of_a_recovery_resumes_it() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = cluster(DeploymentMode::Three, 4).await;
        let (stream, view, end) = led_log(&cluster, 1, [1, 2, 0]).await;
        // Alone, the copy cannot finish.
        cluster.isolate(2, true);
        let store = cluster.log_store(2, stream.id);
        cluster.restart_without(2, &store).await;
        let recovering = copy(&cluster, 2, stream.id).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_ne!(recovering.status().borrow().role, LogRole::Leader);
        let image = cluster.stop(2).await;
        assert!(
            ozzy_journal_segment::is_recovery_marker(
                image.bytes(&store.join("CONFIGURATION"), true).unwrap()
            ),
            "the copy left no marker of its recovery"
        );
        cluster.isolate(2, false);
        cluster.restore_clean(2, image).await;
        votes(&copy(&cluster, 2, stream.id).await).await;
        cluster.stop(1).await;
        elects_with(&cluster, stream.id, view, end).await;
        cluster.shutdown().await;
    })
    .await
    .expect("interrupted recovery made no bounded progress");
}

/// A broker that lost every store creates its logs again as nonvoting
/// copies, which take the log of the other two and then vote.
#[tokio::test(flavor = "current_thread")]
async fn a_replaced_broker_takes_its_logs_from_the_other_two() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = cluster(DeploymentMode::Three, 4).await;
        let (stream, view, end) = led_log(&cluster, 1, [2, 1, 0]).await;
        cluster.replace(1).await;
        assert_eq!(cluster.brokers[1].create_log(stream.clone()).await, Ok(()));
        votes(&copy(&cluster, 1, stream.id).await).await;
        cluster.stop(2).await;
        elects_with(&cluster, stream.id, view, end).await;
        cluster.shutdown().await;
    })
    .await
    .expect("replaced broker made no bounded progress");
}

/// A record is committed by the leader and one follower while the third
/// broker hears nothing. Then that follower loses every store and the
/// leader stops. The replacement creates the log again, but as a nonvoting
/// copy: with the broker that lacks the record it elects nobody. A fresh
/// voting copy would, and the record would be gone. Once the leader is
/// back, the replacement takes the log, votes, and holds the record.
#[tokio::test(flavor = "current_thread")]
async fn a_replaced_broker_cannot_help_elect_a_leader_that_lacks_a_record() {
    tokio::time::timeout(Duration::from_secs(90), async {
        let mut cluster = cluster(DeploymentMode::Three, 4).await;
        let (stream, view, end) = led_log(&cluster, 1, [2, 1, 0]).await;
        cluster.isolate(0, true);
        let leading = cluster.brokers[2].runtime_log(stream.id).unwrap();
        let committed = append(&leading, view, end, &[7]).await.unwrap().end.get();
        let stopped = cluster.stop(2).await;
        cluster.replace(1).await;
        cluster.isolate(0, false);
        let replacement = &cluster.brokers[1];
        assert!(replacement.replacing());
        assert_eq!(replacement.create_log(stream.clone()).await, Ok(()));
        tokio::time::sleep(Duration::from_secs(6)).await;
        for log in handles(&cluster, stream.id) {
            assert_ne!(
                log.status().borrow().role,
                LogRole::Leader,
                "a leader without the record"
            );
        }

        cluster.restore_clean(2, stopped).await;
        votes(&copy(&cluster, 1, stream.id).await).await;
        // From here on the broker makes voting copies again.
        cluster.brokers[1].end_replacement().unwrap();
        assert!(!cluster.brokers[1].replacing());
        led_log(&cluster, 2, [1, 2, 0]).await;

        cluster.stop(2).await;
        elects_with(&cluster, stream.id, view, committed).await;
        cluster.shutdown().await;
    })
    .await
    .expect("replaced broker made no bounded progress");
}
