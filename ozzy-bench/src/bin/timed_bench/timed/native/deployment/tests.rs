use super::*;
use crate::bench::Args;
use clap::Parser;
use ozzy_bench::native::PreparedBroker;
use std::net::TcpListener;

fn configuration(system: &str, storage: &Path) -> Config {
    let mut args = Args::parse_from([
        "bench",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "1",
        "--system",
        system,
        "--partitions",
        "5",
        "--window",
        "7",
        "--producer-workers",
        "3",
        "--reader-workers",
        "4",
    ]);
    args.storage_dir = storage.into();
    Config::production(args).unwrap()
}

fn storage() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("native-deployment-")
        .tempdir_in(std::env::current_exe().unwrap().parent().unwrap())
        .unwrap()
}

#[test]
fn prepared_deployment_binds_persisted_identities_and_actual_sdk_windows() {
    for system in ["single-durable", "disk-quorum", "replicated-persisting"] {
        let parent = storage();
        let config = configuration(system, parent.path());
        let mut resources = (0..config.args.system.brokers())
            .map(|index| {
                PreparedBroker::new(
                    parent.path(),
                    index,
                    "127.0.0.1".parse().unwrap(),
                    config.args.system.brokers(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let brokers = resources
            .iter()
            .enumerate()
            .map(|(index, resource)| settings::Broker {
                name: format!("broker-{index}"),
                root: resource.root(),
                endpoints: resource.endpoints().clone(),
            })
            .collect();
        let prepared = Prepared::from_brokers(&config, brokers, parent.path()).unwrap();
        let checked = prepared.artifact.checked("broker-0").unwrap();
        assert_eq!(checked.identity.brokers.len(), config.args.system.brokers());
        assert_eq!(
            prepared.configuration["topics"]["benchmark"]["partitions"],
            5
        );
        assert_eq!(
            prepared.artifact.configuration.parent().unwrap(),
            parent.path()
        );
        for broker in &prepared.brokers {
            assert!(!broker.root.exists(), "preparation formatted a store");
            for endpoint in [&broker.endpoints.peer, &broker.endpoints.reader_pub]
                .into_iter()
                .chain(broker.endpoints.follower_pub.iter())
            {
                let address = endpoint.strip_prefix("tcp://").unwrap();
                assert!(
                    TcpListener::bind(address).is_err(),
                    "endpoint reservation lost"
                );
                assert_eq!(
                    prepared.configuration["brokers"][&broker.name]["endpoints"]
                        .as_object()
                        .unwrap()
                        .values()
                        .filter(|value| *value == endpoint)
                        .count(),
                    1
                );
            }
        }
        let clients = config.native.as_ref().unwrap().clients.as_ref().unwrap();
        for profile in clients.writers.iter().chain(&clients.readers) {
            let message = prepared.connect(profile).unwrap();
            assert_eq!(message["command"], "connect");
            assert_eq!(message["native"]["partitions"], 5);
            assert_eq!(message["native"]["topic"], "benchmark");
            for (key, value) in profile.as_object().unwrap() {
                assert_eq!(&message["native"][key], value);
            }
            for (actual, broker) in message["native"]["brokers"]
                .as_array()
                .unwrap()
                .iter()
                .zip(&prepared.brokers)
            {
                assert_eq!(
                    actual["node"],
                    checked.identity.brokers[&broker.name].to_string()
                );
                assert_eq!(actual["endpoint"], broker.endpoints.peer);
            }
        }
        assert!(prepared.connect(&Value::Null).is_err());
        for resource in &mut resources {
            resource.release_endpoints();
        }
        for broker in &prepared.brokers {
            let _listener =
                TcpListener::bind(broker.endpoints.peer.strip_prefix("tcp://").unwrap()).unwrap();
        }
        let roots = resources
            .iter()
            .map(|resource| resource.directory().to_owned())
            .collect::<Vec<_>>();
        drop(prepared);
        drop(resources);
        assert!(roots.iter().all(|root| !root.exists()));
    }
}

#[test]
fn invalid_worker_preparation_refuses_before_any_run_files() {
    let parent = storage();
    let missing = parent.path().join("storage-not-created");
    for (index, brokers, bind) in [(3, 3, "127.0.0.1"), (0, 2, "127.0.0.1"), (2, 3, "0.0.0.0")] {
        assert!(PreparedBroker::new(&missing, index, bind.parse().unwrap(), brokers).is_err());
        assert!(!missing.exists());
    }
}
