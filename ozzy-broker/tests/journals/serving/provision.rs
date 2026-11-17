//! First-start provisioning from a deployment built in code.

use super::*;
use ozzy_broker::log::LogRole;
use ozzy_broker::{StartupError, provision};
use ozzy_config::{
    Broker as BrokerConfig, Cluster, Device, Endpoints, ResourceLimits, Topic, ValidatedDeployment,
};
use std::path::PathBuf;

const TOPIC: &str = "meta";

/// One device per broker below `root`, one small topic, no TOML.
pub(super) fn built(root: &Path, mode: DeploymentMode, cluster: u128) -> ValidatedDeployment {
    built_with(root, mode, cluster, None)
}

/// As [`built`], with a class of runtime logs.
pub(super) fn built_with(
    root: &Path,
    mode: DeploymentMode,
    cluster: u128,
    logs: Option<ozzy_config::LogClass>,
) -> ValidatedDeployment {
    let count = if mode == DeploymentMode::Single { 1 } else { 3 };
    let brokers = (0..count)
        .map(|index| {
            let namespace = Uuid::now_v7();
            let mut device = Device::new(root.join(format!("broker-{index}/data")), "local");
            device.workers.backend = IoBackend::Pool;
            device.workers.write_threads = 1;
            let broker = BrokerConfig {
                id: None,
                endpoints: Endpoints {
                    peer: format!("inproc://{namespace}-peer"),
                    data_peer: format!("inproc://{namespace}-data"),
                    reader_pub: format!("inproc://{namespace}-readers"),
                    follower_pub: (count == 3).then(|| format!("inproc://{namespace}-followers")),
                    admin: None,
                },
                devices: BTreeMap::from([("data".to_owned(), device)]),
                topology: ozzy_config::Topology::default(),
            };
            (format!("broker-{index}"), broker)
        })
        .collect();
    let topic = Topic {
        partitions: 1,
        segment_bytes: 1024 * 1024,
        max_append_bytes: 64 * 1024,
        ..Topic::default()
    };
    Deployment {
        cluster: Cluster {
            id: Some(Uuid::from_u128(cluster)),
            mode,
            max_clock_offset_millis: 1000,
        },
        limits: ResourceLimits::default(),
        brokers,
        topics: BTreeMap::from([(TOPIC.to_owned(), topic)]),
        logs,
    }
    .validate()
    .unwrap()
}

pub(super) fn local_identity(root: &Path, index: usize) -> PathBuf {
    root.join(format!("broker-{index}/local.identity"))
}

fn staged(root: &Path) -> PathBuf {
    root.join("broker-0/local.identity.provisioning")
}

fn store(root: &Path) -> PathBuf {
    root.join(format!("broker-0/data/topics/{TOPIC}/partitions/0"))
}

pub(super) async fn provisioned(
    deployment: &ValidatedDeployment,
    root: &Path,
    index: usize,
) -> Result<(CheckedConfig, BrokerIdentity), StartupError> {
    let name = format!("broker-{index}");
    provision(
        deployment.clone(),
        &name,
        &local_identity(root, index),
        &host(),
    )
    .await
}

async fn led(brokers: &[Broker]) {
    let leads = |broker: &Broker| {
        broker
            .log(TOPIC, 0)
            .is_some_and(|log| log.status().borrow().role == LogRole::Leader)
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        while !brokers.iter().any(leads) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the topic gets a leader");
}

#[tokio::test(flavor = "current_thread")]
async fn three_brokers_provision_themselves_and_agree_without_a_shared_file() {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = built(root.path(), DeploymentMode::Three, 7);
    let mut first = Vec::new();
    for round in 0..2 {
        let mut brokers = Vec::new();
        let mut locals = Vec::new();
        for index in 0..3 {
            let (checked, local) = provisioned(&deployment, root.path(), index).await.unwrap();
            assert_eq!(checked.identity, deployment.derive_identity().unwrap());
            locals.push(local.clone());
            let context = runtime.context().clone();
            brokers.push(
                Broker::start_trusted_with_context(checked, local, context)
                    .await
                    .unwrap(),
            );
        }
        led(&brokers).await;
        for broker in brokers {
            broker.shutdown().await.unwrap();
        }
        if round == 0 {
            first = locals;
        } else {
            assert_eq!(first, locals, "a second call keeps the stores");
        }
    }
    assert!(!staged(root.path()).exists());
}

#[tokio::test(flavor = "current_thread")]
async fn interrupted_provisioning_is_finished_by_the_next_call() {
    let root = tempfile::tempdir().unwrap();
    let deployment = built(root.path(), DeploymentMode::Single, 7);
    let (_, local) = provisioned(&deployment, root.path(), 0).await.unwrap();
    let published = local_identity(root.path(), 0);

    // Stopped after formatting, before the local identity got its name.
    std::fs::rename(&published, staged(root.path())).unwrap();
    let stray = store(root.path()).join("stray");
    std::fs::write(&stray, b"partial").unwrap();
    let (_, again) = provisioned(&deployment, root.path(), 0).await.unwrap();
    assert_eq!(again, local);
    assert!(!stray.exists(), "the unfinished store was formatted anew");
    assert!(!staged(root.path()).exists());

    // Stopped right after the local identity was staged.
    std::fs::rename(&published, staged(root.path())).unwrap();
    std::fs::remove_dir_all(root.path().join("broker-0/data")).unwrap();
    let (checked, again) = provisioned(&deployment, root.path(), 0).await.unwrap();
    assert_eq!(again, local);

    // Stopped between naming the local identity and dropping the staged one.
    std::fs::hard_link(&published, staged(root.path())).unwrap();
    let (_, again) = provisioned(&deployment, root.path(), 0).await.unwrap();
    assert_eq!(again, local);
    assert!(!staged(root.path()).exists());

    let broker = Broker::start_trusted(checked, local).await.unwrap();
    led(std::slice::from_ref(&broker)).await;
    broker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn a_provisioned_broker_never_gets_new_stores() {
    let root = tempfile::tempdir().unwrap();
    let deployment = built(root.path(), DeploymentMode::Single, 7);
    provisioned(&deployment, root.path(), 0).await.unwrap();
    let data = root.path().join("broker-0/data");

    // A store that went missing stays missing.
    std::fs::remove_dir_all(store(root.path())).unwrap();
    let (checked, local) = provisioned(&deployment, root.path(), 0).await.unwrap();
    assert!(!store(root.path()).exists());
    assert!(Broker::start_trusted(checked, local).await.is_err());

    // So does a volume, as when its mount is absent.
    std::fs::remove_dir_all(&data).unwrap();
    assert!(provisioned(&deployment, root.path(), 0).await.is_err());
    assert!(!data.exists());
}

#[tokio::test(flavor = "current_thread")]
async fn a_volume_of_another_identity_is_refused_and_kept() {
    let root = tempfile::tempdir().unwrap();
    let deployment = built(root.path(), DeploymentMode::Single, 7);
    provisioned(&deployment, root.path(), 0).await.unwrap();
    let kept = store(root.path()).join("kept");
    std::fs::write(&kept, b"records").unwrap();

    // The local identity is gone, its stores are not.
    std::fs::remove_file(local_identity(root.path(), 0)).unwrap();
    assert!(provisioned(&deployment, root.path(), 0).await.is_err());
    assert!(provisioned(&deployment, root.path(), 0).await.is_err());
    assert_eq!(std::fs::read(&kept).unwrap(), b"records");

    // Nor does another cluster take the volume over.
    let other = built(root.path(), DeploymentMode::Single, 8);
    std::fs::remove_file(staged(root.path())).unwrap();
    assert!(provisioned(&other, root.path(), 0).await.is_err());
    assert_eq!(std::fs::read(&kept).unwrap(), b"records");
}

#[tokio::test(flavor = "current_thread")]
async fn a_deployment_without_a_cluster_id_is_not_provisioned() {
    let root = tempfile::tempdir().unwrap();
    let mut config = built(root.path(), DeploymentMode::Single, 7)
        .deployment()
        .clone();
    config.cluster.id = None;
    let deployment = config.validate().unwrap();
    assert!(provisioned(&deployment, root.path(), 0).await.is_err());
    assert!(!root.path().join("broker-0").exists());
}

fn role(broker: &Broker) -> LogRole {
    broker.log(TOPIC, 0).unwrap().status().borrow().role
}

pub(super) async fn joined(broker: &Broker) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !matches!(role(broker), LogRole::Leader | LogRole::Follower) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the copy joins a view");
}

#[tokio::test(flavor = "current_thread")]
async fn peers_refuse_a_broker_that_comes_back_on_new_stores() {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = built(root.path(), DeploymentMode::Three, 7);
    let start = async |index: usize| {
        let (checked, local) = provisioned(&deployment, root.path(), index).await.unwrap();
        let origin = local.origin();
        let context = runtime.context().clone();
        let broker = Broker::start_trusted_with_context(checked, local, context)
            .await
            .unwrap();
        (broker, origin)
    };
    let mut brokers = Vec::new();
    for index in 0..3 {
        brokers.push(start(index).await);
    }
    for (broker, _) in &brokers {
        joined(broker).await;
    }
    let record = |index: usize| {
        let path = root.path().join(format!("broker-{index}/data/OZZY_PEERS"));
        ozzy_config::PeerRecord::decode(&std::fs::read_to_string(path).unwrap()).unwrap()
    };
    let id = |name: &str| deployment.derive_identity().unwrap().brokers[name];
    for index in 0..2 {
        assert_eq!(record(index).origins[&id("broker-2")], brokers[2].1);
    }

    // An intact restart keeps the origin and joins again.
    let (stopped, first) = brokers.pop().unwrap();
    stopped.shutdown().await.unwrap();
    let (restarted, origin) = start(2).await;
    assert_eq!(origin, first);
    joined(&restarted).await;
    restarted.shutdown().await.unwrap();

    // Its directory is emptied: it provisions as if new.
    std::fs::remove_dir_all(root.path().join("broker-2")).unwrap();
    let (wiped, origin) = start(2).await;
    assert_ne!(origin, first);
    for index in 0..2 {
        assert_eq!(record(index).origins[&id("broker-2")], first);
    }

    // With the other intact copy gone, the leader has no follower left:
    // what the emptied copy would acknowledge counts for nothing.
    let (follower, _) = brokers.pop().unwrap();
    follower.shutdown().await.unwrap();
    let (leader, _) = brokers.pop().unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while role(&leader) == LogRole::Leader {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the leader notices it is alone");
    for _ in 0..300 {
        assert_ne!(role(&leader), LogRole::Leader);
        assert_ne!(role(&wiped), LogRole::Leader);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The intact copy returns and the two serve again.
    let (follower, _) = start(1).await;
    led(&[leader, follower, wiped]).await;
}
