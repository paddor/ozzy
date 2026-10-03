//! Functional production worker startup checks. No performance assertions.

#[path = "native/process.rs"]
mod process;
mod support;
#[path = "native/timed.rs"]
mod timed;

use ozzy_bench::native::{BrokerWorker, DeploymentArtifact, PreparedBroker};
use std::{fmt::Write, fs, net::TcpListener, path::Path, time::Duration};

#[tokio::test(flavor = "current_thread")]
async fn native_broker_workers_initialize_restart_and_drain_each_policy() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for policy in ["local-durable", "disk-quorum", "replicated-persisting"] {
            startup(policy).await;
        }
    })
    .await
    .unwrap();
}

async fn startup(policy: &str) {
    let directory = support::storage("native-production-");
    let (source, reservations) = document(directory.path(), policy);
    let deployment = DeploymentArtifact::initialize(directory.path(), &source).unwrap();
    assert_eq!(
        fs::read_to_string(&deployment.configuration).unwrap(),
        source
    );
    let shared = fs::read(&deployment.identity).unwrap();
    let names = deployment.checked("broker-0").unwrap().identity.brokers;
    let mut workers = Vec::new();
    let mut bindings = Vec::new();
    for name in names.keys() {
        let worker = BrokerWorker {
            deployment: deployment.clone(),
            broker: name.clone(),
            identity: directory.path().join(format!("{name}.identity")),
        };
        assert!(worker.start_trusted().await.is_err());
        assert!(
            !worker.identity.exists(),
            "startup initialized local identity"
        );
        assert!(
            !directory.path().join(name).exists(),
            "startup initialized storage"
        );
        worker.initialize().await.unwrap();
        let checked = deployment.checked(name).unwrap();
        assert_eq!(checked.plan.partitions.len(), 4);
        for partition in checked.plan.partitions {
            assert!(partition.directory.join("CONFIGURATION").is_file());
        }
        let local = fs::read(&worker.identity).unwrap();
        assert!(worker.initialize().await.is_err());
        assert_eq!(fs::read(&worker.identity).unwrap(), local);
        bindings.push(local);
        workers.push(worker);
    }
    drop(reservations);
    for _ in 0..2 {
        let mut running = Vec::new();
        for (index, worker) in workers.iter().enumerate() {
            let broker = worker.start_trusted().await.unwrap();
            assert_eq!(broker.application_threads(), index + 1);
            assert_eq!(broker.dispatcher_threads(), 1);
            assert_eq!(broker.io_threads(), 1);
            running.push(broker);
        }
        for broker in running {
            broker.shutdown().await.unwrap();
        }
        assert_eq!(fs::read(&deployment.identity).unwrap(), shared);
        for (worker, expected) in workers.iter().zip(&bindings) {
            assert_eq!(fs::read(&worker.identity).unwrap(), *expected);
        }
    }
}

#[test]
fn native_artifact_refuses_invalid_input_and_existing_files() {
    let directory = support::storage("native-invalid-");
    assert!(
        DeploymentArtifact::initialize(directory.path(), "[cluster]\nmode = \"single\"").is_err()
    );
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    let (source, _reservations) = document(directory.path(), "local-durable");
    fs::write(
        directory.path().join("deployment.identity"),
        b"existing identity",
    )
    .unwrap();
    assert!(DeploymentArtifact::initialize(directory.path(), &source).is_err());
    assert!(!directory.path().join("deployment.toml").exists());
    assert_eq!(
        fs::read(directory.path().join("deployment.identity")).unwrap(),
        b"existing identity"
    );
    fs::write(directory.path().join("deployment.toml"), b"existing TOML").unwrap();
    assert!(DeploymentArtifact::initialize(directory.path(), &source).is_err());
    assert_eq!(
        fs::read(directory.path().join("deployment.toml")).unwrap(),
        b"existing TOML"
    );
}

#[test]
fn shared_deployment_install_preserves_exact_records_and_prepared_resources() {
    let origin = support::storage("native-origin-");
    let (source, _reservations) = document(origin.path(), "local-durable");
    let shared = DeploymentArtifact::initialize(origin.path(), &source).unwrap();
    let identity = fs::read_to_string(&shared.identity).unwrap();
    let copy = support::storage("native-copy-");
    for invalid in ["broken record", &format!("{identity}corrupt")] {
        assert!(DeploymentArtifact::install(copy.path(), &source, invalid).is_err());
        assert_eq!(fs::read_dir(copy.path()).unwrap().count(), 0);
    }
    assert!(DeploymentArtifact::install(copy.path(), "invalid config", &identity).is_err());
    assert_eq!(fs::read_dir(copy.path()).unwrap().count(), 0);
    let copied = DeploymentArtifact::install(copy.path(), &source, &identity).unwrap();
    assert_eq!(fs::read_to_string(&copied.configuration).unwrap(), source);
    assert_eq!(fs::read_to_string(&copied.identity).unwrap(), identity);
    assert!(DeploymentArtifact::install(copy.path(), &source, &identity).is_err());
    assert_eq!(fs::read_to_string(&copied.configuration).unwrap(), source);
    assert_eq!(fs::read_to_string(&copied.identity).unwrap(), identity);

    let occupied = support::storage("native-occupied-");
    fs::write(occupied.path().join("deployment.identity"), "existing").unwrap();
    assert!(DeploymentArtifact::install(occupied.path(), &source, &identity).is_err());
    assert!(!occupied.path().join("deployment.toml").exists());
    assert_eq!(
        fs::read_to_string(occupied.path().join("deployment.identity")).unwrap(),
        "existing"
    );

    let resources = PreparedBroker::new(copy.path(), 0, "127.0.0.1".parse().unwrap(), 1).unwrap();
    assert!(resources.install(&source, &identity).is_err());
    assert_eq!(fs::read_dir(resources.directory()).unwrap().count(), 0);
    let mut config = source.parse::<toml::Value>().unwrap();
    let broker = &mut config["brokers"]["broker-0"];
    broker["endpoints"]["peer"] = toml::Value::String(resources.endpoints().peer.clone());
    broker["endpoints"]["reader_pub"] =
        toml::Value::String(resources.endpoints().reader_pub.clone());
    broker["devices"]["ssd"]["root"] =
        toml::Value::String(resources.root().to_str().unwrap().into());
    let source = toml::to_string(&config).unwrap();
    let worker = resources.install(&source, &identity).unwrap();
    assert!(!resources.root().exists(), "install formatted a store");
    assert!(
        !worker.identity.exists(),
        "install generated local identity"
    );
    assert_eq!(
        fs::read_to_string(&worker.deployment.identity).unwrap(),
        identity
    );
    assert_eq!(
        fs::read_to_string(&worker.deployment.configuration).unwrap(),
        source
    );
}

fn document(root: &Path, policy: &str) -> (String, Vec<TcpListener>) {
    let brokers = if policy == "local-durable" { 1 } else { 3 };
    let reservations = (0..brokers * 2)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let mut source = format!(
        "[cluster]\nmode = \"{}\"\n\
         [topics.orders]\nconfirmation = \"{policy}\"\npartitions = 4\n\
         segment_bytes = 1048576\nmax_append_bytes = 65536\n",
        if brokers == 1 { "single" } else { "three" }
    );
    for index in 0..brokers {
        let peer = reservations[index * 2].local_addr().unwrap().port();
        let readers = reservations[index * 2 + 1].local_addr().unwrap().port();
        let storage = toml::Value::String(
            root.join(format!("broker-{index}"))
                .to_str()
                .unwrap()
                .into(),
        );
        write!(
            source,
            "[brokers.broker-{index}.endpoints]\n\
             peer = \"tcp://127.0.0.1:{peer}\"\nreader_pub = \"tcp://127.0.0.1:{readers}\"\n\
             [brokers.broker-{index}.devices.ssd]\nroot = {storage}\ncontroller = \"ssd\"\n\
             [brokers.broker-{index}.devices.ssd.workers]\nbackend = \"pool\"\n\
             write_threads = 1\nmax_inflight = 8\nqueued_jobs = 32\nqueued_bytes = 8388608\n\
             progress_jobs = 8\nprogress_bytes = 1048576\nopen_handles = 256\n"
        )
        .unwrap();
        for shard in 0..=index {
            write!(source,
                "[[brokers.broker-{index}.topology.shards]]\nid = {}\ndevice = \"ssd\"\n\
                 [brokers.broker-{index}.topology.shards.budget]\n\
                 append_slots = 32\nresident_bytes = 8388608\ncontrol_slots = 64\ncontrol_bytes = 1048576\n",
                shard * 7
            ).unwrap();
        }
    }
    (source, reservations)
}
