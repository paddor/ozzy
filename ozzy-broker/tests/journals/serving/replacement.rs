//! A broker whose directory was emptied comes back as a replacement: on
//! real files, through `provision_replacement`, with peers that keep the
//! origin of its stores.

use super::provision::{built_with, joined, local_identity, peer_record, provisioned};
use super::*;
use ozzy_broker::log::LogRecord;
use ozzy_broker::{LogSpec, provision_replacement};
use ozzy_proto::{GroupId, Offset, PartitionIncarnation};

const ID: GroupId = GroupId::from_bytes([3; 16]);

fn record(id: u8) -> LogRecord {
    LogRecord {
        id: MessageId::from_bytes([id; 16]),
        parts: [bytes::Bytes::from_static(b"body")].into_iter().collect(),
    }
}

/// The copy that leads the log, once one does, with its view and end.
async fn leader(brokers: &[&Broker]) -> (Log, u64, u64) {
    let leading = || {
        let logs = brokers.iter().filter_map(|broker| broker.runtime_log(ID));
        logs.map(|log| (*log.status().borrow(), log))
            .find(|(status, _)| status.role == LogRole::Leader)
    };
    let mut found = None;
    eventually("the log gets a leader", || {
        found = leading();
        found.is_some()
    })
    .await;
    let (status, log) = found.unwrap();
    (log, status.view, status.committed_end.get())
}

/// Wait until `broker`'s copy of the log votes.
async fn voting(broker: &Broker) {
    eventually("the copy votes", || votes(broker.runtime_log(ID))).await;
}

#[tokio::test(flavor = "current_thread")]
async fn an_emptied_broker_comes_back_as_a_replacement() {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let class = log_class(2 * 1024 * 1024, 256 * 1024, 4);
    let deployment = built_with(root.path(), DeploymentMode::Three, 9, Some(class));
    let start = async |checked, local| start_broker(&runtime, checked, local).await;
    let mut brokers = Vec::new();
    for index in 0..3 {
        let (checked, local) = provisioned(&deployment, root.path(), index).await.unwrap();
        brokers.push(start(checked, local).await);
    }
    let identity = deployment.derive_identity().unwrap();
    let node =
        |index: usize| NodeId::from_bytes(*identity.brokers[&format!("broker-{index}")].as_bytes());
    let stream = LogSpec {
        id: ID,
        incarnation: PartitionIncarnation::from_bytes([4; 16]),
        members: vec![node(0), node(1), node(2)],
        segment_bytes: None,
    };
    for broker in &brokers {
        joined(broker).await;
        broker.create_log(stream.clone()).await.unwrap();
    }
    let all: Vec<&Broker> = brokers.iter().collect();
    let (log, view, end) = leader(&all).await;
    let records = vec![record(1), record(2)];
    let appended = log.append(view, Offset::new(end), records).unwrap().await;
    let end = appended.unwrap().end.get();

    // Broker 2 loses its directory and is provisioned as a replacement.
    brokers.pop().unwrap().shutdown().await.unwrap();
    std::fs::remove_dir_all(root.path().join("broker-2")).unwrap();
    let replaced = async || {
        let identity = local_identity(root.path(), 2);
        provision_replacement(deployment.clone(), "broker-2", &identity, &host())
            .await
            .unwrap()
    };
    let (checked, local) = replaced().await;
    let origin = local.origin();
    let marker = root.path().join("broker-2/data/OZZY_REPLACING");
    assert!(marker.exists());
    let store = root.path().join("broker-2/data/topics/meta/partitions/0");
    assert!(!store.exists(), "a replacement formats no replicated store");
    let replacement = start(checked, local).await;
    assert!(replacement.replacing());

    // Its peers take the new origin, and its copies take their logs.
    joined(&replacement).await;
    for index in 0..2 {
        let record = peer_record(root.path(), index);
        assert_eq!(record.origins[&identity.brokers["broker-2"]], origin);
    }
    replacement.create_log(stream.clone()).await.unwrap();
    voting(&replacement).await;
    replacement.end_replacement().unwrap();
    assert!(!replacement.replacing() && !marker.exists());

    // It is an ordinary broker from here on, also after a restart, and
    // holds the records with either of the others.
    replacement.shutdown().await.unwrap();
    let (checked, local) = replaced().await;
    assert_eq!(local.origin(), origin);
    let replacement = start(checked, local).await;
    assert!(!replacement.replacing());
    voting(&replacement).await;
    brokers.pop().unwrap().shutdown().await.unwrap();
    let (log, view, committed) = leader(&[&brokers[0], &replacement]).await;
    assert!(committed >= end, "the log lost records: {committed} {end}");
    let read = log
        .read(Offset::new(0), Offset::new(end), 16, 4096)
        .unwrap();
    assert_eq!(read.await.unwrap().records.len() as u64, end);
    let next = vec![record(3)];
    let appended = log
        .append(view, Offset::new(committed), next)
        .unwrap()
        .await;
    assert_eq!(appended.unwrap().end.get(), committed + 1);
    replacement.shutdown().await.unwrap();
    brokers.pop().unwrap().shutdown().await.unwrap();
}
