use super::*;
use crate::bench::Args;
use clap::Parser;

fn config(size: &str, extra: &[&str]) -> Config {
    Config::production(Args::parse_from(
        [
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
            "--record-bytes",
            size,
        ]
        .into_iter()
        .chain(extra.iter().copied()),
    ))
    .unwrap()
}

#[test]
fn client_windows_charge_idle_partitions_and_split_actual_worker_cohorts() {
    let config = config(
        "128",
        &[
            "--window",
            "7",
            "--partitions",
            "5",
            "--producer-workers",
            "3",
            "--reader-workers",
            "4",
            "--readers-per-partition",
            "2",
        ],
    );
    let clients = config.native.as_ref().unwrap().clients.as_ref().unwrap();
    assert_eq!(
        clients
            .writers
            .iter()
            .map(|profile| profile["append"]["writers"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![15, 10, 10]
    );
    assert_eq!(
        clients
            .writers
            .iter()
            .map(|profile| profile["append"]["requests"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![9, 6, 6]
    );
    assert_eq!(
        clients
            .readers
            .iter()
            .map(|profile| profile["reader"]["subscriptions"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![3, 3, 2, 2]
    );
    for profile in clients.writers.iter().chain(&clients.readers) {
        assert!(profile["control"]["bytes"].as_u64().unwrap() > 2 * 1024 * 1024);
        assert_eq!(
            profile["receive"]["metadata_bytes"],
            DIRECTORY_METADATA_BYTES
        );
    }
    assert!(clients.bytes > 0);
    assert!(config.native.as_ref().unwrap().reservation_bytes > clients.bytes);
}

#[test]
fn minimum_and_large_record_profiles_fit_the_broker_receive_declaration() {
    for size in ["16", "128", "1024", "8192"] {
        for records in ["4", "1024", "65536"] {
            let config = config(
                size,
                &["--system", "single-durable", "--request-records", records],
            );
            let native = config.native.as_ref().unwrap();
            let source = native
                .document(&[super::super::settings::Broker {
                    name: "broker-0".into(),
                    root: "/native-profile-check/broker-0".into(),
                    endpoints: ozzy_config::Endpoints {
                        peer: "tcp://127.0.0.1:40000".into(),
                        data_peer: "tcp://127.0.0.1:40002".into(),
                        reader_pub: "tcp://127.0.0.1:40001".into(),
                        follower_pub: None,
                    },
                }])
                .unwrap();
            let deployment = ozzy_config::Deployment::parse(&source)
                .unwrap()
                .validate()
                .unwrap();
            let body = deployment.deployment().topics["benchmark"].max_append_bytes;
            let payload = body - 89 - 24 * 2048;
            assert!(config.writer_limits().envelope.max_payload_bytes as u64 <= payload);
            let profile = &native.clients.as_ref().unwrap().writers[0];
            assert!(profile["append"]["bytes"].as_u64().unwrap() > 0);
            assert!(
                config.reader_records() * config.args.record_bytes
                    <= config.writer_limits().envelope.max_payload_bytes
            );
        }
    }
}

#[test]
fn oversized_aggregate_windows_refuse_during_configuration_preflight() {
    for flags in [
        vec!["--window", "32", "--writer-inflight-appends", "65536"],
        vec!["--window", "32", "--partitions", "65536"],
        vec!["--partitions", "65536", "--readers-per-partition", "32"],
    ] {
        let args = Args::parse_from(
            [
                "bench",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "1",
            ]
            .into_iter()
            .chain(flags),
        );
        assert!(Config::production(args).is_err());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn generated_deployment_and_sdk_reservations_write_and_replay_exact_records() {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for size in ["16", "128", "1024", "8192"] {
            production_round_trip(size).await;
        }
    })
    .await
    .unwrap();
}

async fn start(size: &str) -> (Config, tempfile::TempDir, ozzy_broker::Broker, Setup) {
    use ozzy_bench::native::{BrokerWorker, DeploymentArtifact};
    use std::net::TcpListener;

    let config = config(
        size,
        &[
            "--system",
            "single-durable",
            "--window",
            "4",
            "--partitions",
            "2",
            "--app-threads",
            "2",
            "--balanced-partitions",
            "--request-records",
            "4",
            "--reader-records",
            "4",
            "--reader-workers",
            "1",
            "--segment-mib",
            "4",
            "--io-backend",
            "pool",
            "--direct-io",
            "false",
        ],
    );
    let executable = std::env::current_exe().unwrap();
    let directory = tempfile::Builder::new()
        .prefix("native-profile-")
        .tempdir_in(executable.parent().unwrap())
        .unwrap();
    let reservations = [
        std::net::TcpListener::bind("127.0.0.1:0").unwrap(),
        TcpListener::bind("127.0.0.1:0").unwrap(),
        TcpListener::bind("127.0.0.1:0").unwrap(),
    ];
    let peer = format!("tcp://{}", reservations[0].local_addr().unwrap());
    let native = config.native.as_ref().unwrap();
    let source = native
        .document(&[super::super::settings::Broker {
            name: "local".into(),
            root: directory.path().join("data"),
            endpoints: ozzy_config::Endpoints {
                peer: peer.clone(),
                data_peer: format!("tcp://{}", reservations[2].local_addr().unwrap()),
                reader_pub: format!("tcp://{}", reservations[1].local_addr().unwrap()),
                follower_pub: None,
            },
        }])
        .unwrap();
    let deployment = DeploymentArtifact::initialize(directory.path(), &source).unwrap();
    let checked = deployment.checked("local").unwrap();
    let setup = Setup::parse(
        &config,
        &json!({"topic": "benchmark", "partitions": 2,
            "brokers": [{"node": checked.identity.brokers["local"].to_string(), "endpoint": peer, "data_endpoint": checked.deployment.deployment().brokers["local"].endpoints.data_peer}],
        }),
    )
    .unwrap();
    let worker = BrokerWorker {
        deployment,
        broker: "local".into(),
        identity: directory.path().join("local.identity"),
    };
    worker.initialize().await.unwrap();
    drop(reservations);
    let broker = worker.start_trusted().await.unwrap();
    (config, directory, broker, setup)
}

async fn production_round_trip(size: &str) {
    use bytes::Bytes;
    use ozzy_runtime::replicated::{
        RecordInput, RetryPolicy, SharedTopicWriter, TopicReader, TopicReaderConfig, WriterRuntime,
    };
    use std::collections::BTreeMap;
    let (config, _directory, broker, setup) = start(size).await;
    let clients = config.native.as_ref().unwrap().clients.as_ref().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let links = setup
        .writer_links(&runtime, &config, &clients.writers[0]["append"])
        .await
        .unwrap();
    let mut writers = Vec::new();
    for lane in 0..4 {
        let writer = SharedTopicWriter::open_with_producer(
            &links,
            "benchmark",
            ozzy_proto::ProducerId::from_bytes([lane + 1; 16]),
            writer_settings(&config),
            RetryPolicy::default(),
        )
        .await
        .unwrap();
        writers.push(writer);
    }
    let mut expected = BTreeMap::new();
    let mut pending = Vec::new();
    for (lane, writer) in writers.iter_mut().enumerate() {
        let key = (0_u64..1000)
            .map(u64::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number as usize == lane % 2)
            .unwrap();
        for sequence in 0..2 {
            let id = ozzy_proto::MessageId::from_bytes([10 * lane as u8 + sequence + 1; 16]);
            let body = Bytes::from(vec![sequence; config.args.record_bytes]);
            expected.insert(id, body.clone());
            pending.push(
                writer
                    .send(RecordInput::single(id, body), Some(&key))
                    .await
                    .unwrap(),
            );
        }
    }
    for record in pending {
        record.confirmed().await.unwrap();
    }
    for writer in writers {
        writer.close().await.unwrap();
    }
    let read_links = setup
        .reader_links(&runtime, &config, &clients.readers[0]["reader"])
        .await
        .unwrap();
    let mut reader = TopicReader::open(
        read_links.clone(),
        "benchmark",
        TopicReaderConfig::default(),
    )
    .await
    .unwrap();
    let mut offsets = [0, 0];
    for _ in 0..8 {
        let record = reader.next().await.unwrap();
        assert_eq!(record.offset.get(), offsets[record.partition as usize]);
        offsets[record.partition as usize] += 1;
        assert_eq!(
            record.payload.as_slice(),
            &[expected.remove(&record.message_id).unwrap()]
        );
    }
    assert!(expected.is_empty());
    assert_eq!(offsets, [4, 4]);
    reader.close().await.unwrap();
    read_links.shutdown().await.unwrap();
    links.shutdown().await.unwrap();
    broker.shutdown().await.unwrap();
}
