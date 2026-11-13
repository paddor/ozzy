//! An embedder's log through `Broker::log`: owner appends at expected
//! offsets, refusals that name the next offset or view, reads, and the
//! leader's status across a failover.

use super::*;
use bytes::Bytes;
use ozzy_broker::log::{
    AppendError, Appended, Log, LogRecord, LogRole, LogStatus, StoredRecord, SubmitError,
};
use ozzy_proto::{MessageId, Offset};

fn record(id: u8, body: &'static [u8]) -> LogRecord {
    LogRecord {
        id: MessageId::from_bytes([id; 16]),
        parts: [Bytes::from_static(body), Bytes::from_static(b"tail")]
            .into_iter()
            .collect(),
    }
}

/// Logs of partition 0 on every running broker, in broker order.
fn logs(cluster: &broker::Cluster) -> Vec<Log> {
    cluster
        .brokers
        .iter()
        .map(|broker| broker.log("orders", 0).unwrap())
        .collect()
}

/// The broker that leads partition 0 in a view above `after`, and its status.
async fn leader(logs: &[Log], after: Option<u64>) -> (usize, LogStatus) {
    let mut statuses: Vec<_> = logs.iter().map(Log::status).collect();
    loop {
        for (index, status) in statuses.iter_mut().enumerate() {
            let status = *status.borrow_and_update();
            if status.role == LogRole::Leader && after.is_none_or(|view| status.view > view) {
                return (index, status);
            }
        }
        let changes = statuses
            .iter_mut()
            .map(|status| Box::pin(status.changed()))
            .collect::<Vec<_>>();
        futures::future::select_all(changes).await.0.unwrap();
    }
}

async fn append(log: &Log, view: u64, at: u64, ids: &[u8]) -> Result<Appended, AppendError> {
    let records = ids.iter().map(|&id| record(id, b"body")).collect();
    log.append(view, Offset::new(at), records).unwrap().await
}

/// Every record from `from` to the committed end, read in small pieces.
async fn read_all(log: &Log, from: u64, end: u64) -> Vec<StoredRecord> {
    let mut records = Vec::new();
    let mut next = from;
    while next < end {
        let batch = log
            .read(Offset::new(next), Offset::new(end), 2, 1024)
            .unwrap()
            .await
            .unwrap();
        assert_eq!(batch.first, Offset::new(next));
        assert_ne!(batch.records.len(), 0);
        next += batch.records.len() as u64;
        records.extend(batch.records);
    }
    records
}

/// The leader takes pipelined appends at expected offsets, refuses others
/// with the next offset or the current view, and reads them back.
async fn appends_and_reads(logs: &[Log]) -> (usize, u64, u64) {
    let (index, status) = leader(logs, None).await;
    let lead = status.lead.unwrap();
    assert_eq!(lead.start_end, status.committed_end);
    assert!(lead.installed_commit_end <= lead.start_end);
    let (log, view, start) = (&logs[index], status.view, lead.start_end.get());
    // The second batch is queued at the first one's end before it commits.
    let first = log
        .append(
            view,
            Offset::new(start),
            vec![record(1, b"one"), record(2, b"two")],
        )
        .unwrap();
    let second = log
        .append(view, Offset::new(start + 2), vec![record(3, b"three")])
        .unwrap();
    let first = first.await.unwrap();
    assert_eq!((first.first.get(), first.end.get()), (start, start + 2));
    assert!(first.time_millis > 0);
    let second = second.await.unwrap();
    assert_eq!(second.end.get(), start + 3);
    for at in [start, start + 2, start + 4] {
        assert_eq!(
            append(log, view, at, &[9]).await,
            Err(AppendError::Offset {
                next: Offset::new(start + 3)
            })
        );
    }
    assert_eq!(
        append(log, view + 1, start + 3, &[9]).await,
        Err(AppendError::View {
            named: view + 1,
            current: view
        })
    );
    for (other, log) in logs.iter().enumerate().filter(|&(other, _)| other != index) {
        assert_eq!(
            append(log, view, start + 3, &[9]).await,
            Err(AppendError::NotLeader),
            "broker {other} does not lead"
        );
    }
    assert_eq!(
        log.append(
            view,
            Offset::new(start + 3),
            vec![LogRecord {
                id: MessageId::from_bytes([0; 16]),
                parts: [Bytes::new()].into_iter().collect(),
            }]
        )
        .unwrap_err(),
        SubmitError::Invalid
    );
    let read = read_all(log, start, start + 3).await;
    // Each record carries the time its append returned.
    let stored = |time_millis, record| StoredRecord {
        time_millis,
        record,
    };
    assert_eq!(
        read,
        vec![
            stored(first.time_millis, record(1, b"one")),
            stored(first.time_millis, record(2, b"two")),
            stored(second.time_millis, record(3, b"three")),
        ]
    );
    let tail = log
        .read(Offset::new(start + 3), Offset::new(start + 10), 8, 1024)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(tail.records.len(), 0);
    let mut status = log.status();
    let status = status
        .wait_for(|status| status.committed_end.get() == start + 3)
        .await
        .unwrap();
    assert_eq!(status.view, view);
    (index, view, start + 3)
}

#[tokio::test(flavor = "current_thread")]
async fn owner_log_appends_at_expected_offsets_and_reads_in_every_mode() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for mode in [DeploymentMode::Single, DeploymentMode::Three] {
            let cluster = broker::Cluster::configured(mode, None, |config| {
                config.topics.get_mut("orders").unwrap().retention =
                    ozzy_config::TopicRetention::default();
            })
            .await;
            appends_and_reads(&logs(&cluster)).await;
            cluster.shutdown().await;
        }
    })
    .await
    .expect("owner log made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn owner_log_moves_to_a_new_leader_at_the_committed_end() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cluster = broker::Cluster::configured(DeploymentMode::Three, None, |config| {
            config.topics.get_mut("orders").unwrap().retention =
                ozzy_config::TopicRetention::default();
        })
        .await;
        let (index, view, end) = appends_and_reads(&logs(&cluster)).await;
        let image = cluster.stop(index).await;
        let survivors = logs(&cluster);
        let (next, status) = leader(&survivors, Some(view)).await;
        let lead = status.lead.unwrap();
        assert_eq!(lead.start_end.get(), end);
        assert!(lead.installed_commit_end <= lead.start_end);
        let log = &survivors[next];
        assert_eq!(read_all(log, 0, end).await.len() as u64, end);
        let appended = append(log, status.view, end, &[4, 5]).await.unwrap();
        assert_eq!(appended.end.get(), end + 2);
        assert_eq!(
            append(log, view, end + 2, &[6]).await,
            Err(AppendError::View {
                named: view,
                current: status.view
            })
        );
        cluster.restore(index, image).await;
        cluster.shutdown().await;
    })
    .await
    .expect("owner log failover made no bounded progress");
}
