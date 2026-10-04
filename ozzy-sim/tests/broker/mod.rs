//! Full product smoke gate sharing the broker integration storage and SDK oracle.
use ozzy_config::{Confirmation, DeploymentMode, HostResources};
use ozzy_runtime::replicated::WriterRuntime;
use ozzy_sim::{broker, client::Client};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
mod clock;
mod crash;
mod runner;

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_all_modes_share_the_real_broker_and_sdk_harness() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let runtime = WriterRuntime::new().unwrap();
            let root = PathBuf::from(format!("/ozzy-full-stack-{}", uuid::Uuid::now_v7()));
            let resources = HostResources {
                cpus: BTreeMap::from([(0, Some(0))]),
                memory_nodes: [0].into(),
                linux_aio: true,
            };
            let mode = match policy {
                Confirmation::LocalDurable => DeploymentMode::Single,
                Confirmation::DiskQuorum | Confirmation::ReplicatedPersisting => {
                    DeploymentMode::Three
                }
            };
            let configs =
                broker::deployment_with_resources(&root, mode, policy, 2, &resources, |_| {});
            let checked = configs[0].0.clone();
            let brokers = broker::start_brokers(&runtime, configs, true).await;
            let mut client = Client::open_with_runtime(&checked, &runtime).await;
            let mut reader = client.reader(false).await;
            for wave in 0..6 {
                let positions = client.positions();
                let pending = client.queue_varied(wave).await;
                client.confirm(pending).await;
                client.read(&mut reader, positions).await;
            }
            let positions = client.positions();
            assert_eq!(client.shared_producers(6).await, 12);
            client.read(&mut reader, positions).await;
            client = client.reopen_producer(true).await;
            client = client.reopen_producer(false).await;
            let positions = client.positions();
            let pending = client.queue(7).await;
            client.confirm(pending).await;
            client.read(&mut reader, positions).await;
            client.replay().await;
            reader.close().await.unwrap();
            client.close().await;
            for broker in brokers {
                broker.shutdown().await.unwrap();
            }
        }
    })
    .await
    .expect("full-broker memory simulation made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_sequential_sdk_identities_reclaim_client_slots_in_every_mode() {
    tokio::time::timeout(Duration::from_secs(120), async {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let cluster = broker::Cluster::new(policy).await;
            let mut client =
                Client::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
            for wave in 0..40 {
                client = client.reconnect(&cluster.runtime, true).await;
                let pending = client.queue(wave).await;
                client.confirm(pending).await;
                client.replay().await;
                client.discard_verified();
            }
            client.close().await;
            cluster.shutdown().await;
        }
    })
    .await
    .expect("sequential identities exhausted broker admission or stalled");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_recovery_retries_while_a_donor_completion_is_held() {
    tokio::time::timeout(Duration::from_secs(90), async {
        for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
            let mut cluster = broker::Cluster::new(policy).await;
            let mut client =
                Client::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
            let pending = client.queue(0).await;
            client.confirm(pending).await;
            let primary = client.leader(0);
            let donor = cluster
                .configs
                .iter()
                .position(|(config, _)| {
                    ozzy_proto::NodeId::from_bytes(
                        *config.identity.brokers[&config.plan.name].as_bytes(),
                    ) == primary
                })
                .unwrap();
            let requester = (donor + 1) % 3;
            let control = cluster.controls[donor].clone();
            control.hold_completions(true);
            cluster.restart_fresh(requester).await;
            let held = async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while control.pending_completions() == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("recovery never submitted physical donor work");
                // The production recovery actor abandons an unprogressing
                // attempt after ten seconds and sends a fresh nonce on reopen.
                tokio::time::sleep(Duration::from_secs(12)).await;
            };
            tokio::select! {
                () = held => {},
                (result, index, _) = futures::future::select_all(
                    cluster.brokers.iter().map(|broker| Box::pin(broker.closed()))
                ) => panic!("broker {index} failed during delayed donor pin: {result:?}"),
            }
            control.hold_completions(false);
            cluster.wait_recovered(requester).await;
            client.replay().await;
            let pending = client.queue(1).await;
            client.confirm(pending).await;
            client.replay().await;
            client.close().await;
            cluster.shutdown().await;
        }
    })
    .await
    .expect("delayed donor recovery did not regain bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_failure_artifact_preserves_images_and_replays_its_fault_prefix() {
    let root = tempfile::tempdir().unwrap();
    let prefix = root.path().join("prefix.jsonl");
    std::fs::write(
        &prefix,
        "{\"wave\":0,\"pattern\":1,\"action\":\"FailWrite\"}\n",
    )
    .unwrap();
    let config = |artifacts, replay| ozzy_sim::soak::Config {
        policy: Confirmation::LocalDurable,
        seed: 1,
        duration: Duration::from_secs(30),
        waves: 1,
        interval: Duration::ZERO,
        progress_timeout: Duration::from_secs(10),
        artifacts,
        replay: Some(replay),
        time: ozzy_sim::soak::Time::default(),
        resources: ozzy_sim::soak::Resources::default(),
        actions: ozzy_sim::soak::default_actions(),
    };
    let first = root.path().join("first");
    let error = ozzy_sim::soak::run(&config(first.clone(), prefix))
        .await
        .unwrap_err();
    assert!(
        error.contains("broker") && error.contains("exited"),
        "{error}"
    );
    let evidence: serde_json::Value =
        serde_json::from_reader(std::fs::File::open(first.join("failure.json")).unwrap()).unwrap();
    assert_ne!(
        evidence["record_evidence"]["submitted"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    let image: ozzy_io::simulation::Image =
        serde_json::from_reader(std::fs::File::open(first.join("image-0.json")).unwrap()).unwrap();
    let encoded = serde_json::to_vec(&image).unwrap();
    let _: ozzy_io::simulation::Image = serde_json::from_slice(&encoded).unwrap();
    let second = root.path().join("second");
    let replay = ozzy_sim::soak::run(&config(second, first.join("schedule.jsonl")))
        .await
        .unwrap_err();
    assert!(
        replay.contains("broker") && replay.contains("exited"),
        "{replay}"
    );
}
