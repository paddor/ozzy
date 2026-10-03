use super::*;
use crate::bench::timed::config::Config;
use clap::Parser;

fn args(extra: &[&str]) -> Args {
    Args::parse_from(
        [
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
        ]
        .into_iter()
        .chain(extra.iter().copied()),
    )
}

fn broker_specs(root: &std::path::Path, count: usize) -> Vec<Broker> {
    (0..count)
        .map(|index| Broker {
            name: format!("broker-{index}"),
            root: root.join(format!("broker-{index}")),
            endpoints: Endpoints {
                peer: format!("tcp://127.0.0.1:{}", 30000 + index * 4),
                data_peer: format!("tcp://127.0.0.1:{}", 30003 + index * 4),
                reader_pub: format!("tcp://127.0.0.1:{}", 30001 + index * 4),
                follower_pub: Some(format!("tcp://127.0.0.1:{}", 30002 + index * 4)),
            },
        })
        .collect()
}

#[test]
fn small_segments_bound_broker_frames_and_sdk_payloads_for_large_records() {
    for policy in ["single-durable", "disk-quorum", "replicated-persisting"] {
        for size in ["4096", "8192"] {
            let config = Config::production(args(&[
                "--system",
                policy,
                "--segment-mib",
                "4",
                "--record-bytes",
                size,
                "--request-records",
                "256",
                "--writer-batch-records",
                "256",
            ]))
            .unwrap();
            let settings = config.native.as_ref().unwrap();
            let writer = config.writer_limits();
            assert!(settings.append_bytes <= settings.segment_bytes / 2);
            assert!(writer.envelope.max_payload_bytes >= config.args.record_bytes);
            assert!(
                writer.envelope.max_payload_bytes
                    + 89
                    + 24 * ozzy_runtime::replicated::MAX_APPEND_RECORDS
                    <= settings.append_bytes as usize
            );
            assert!(
                config.reader_records() * config.args.record_bytes
                    <= settings.append_payload_bytes()
            );
        }
    }
}

#[test]
fn production_deployment_round_trips_independent_counts_and_shared_budgets() {
    for policy in ["single-durable", "disk-quorum", "replicated-persisting"] {
        let config = Config::production(args(&[
            "--system",
            policy,
            "--window",
            "7",
            "--partitions",
            "5",
            "--app-threads",
            "3",
            "--balanced-partitions",
            "--broker-io-threads",
            "2",
            "--io-backend",
            "pool",
            "--direct-io",
            "false",
            "--backend-write-threads",
            "4",
            "--backend-max-inflight",
            "12",
            "--backend-queued-jobs",
            "48",
            "--backend-queued-mib",
            "24",
            "--backend-progress-jobs",
            "9",
            "--backend-progress-mib",
            "3",
            "--backend-open-handles",
            "256",
            "--shard-append-slots",
            "24",
            "--shard-resident-mib",
            "12",
            "--shard-control-slots",
            "8",
            "--shard-control-kib",
            "256",
            "--readers-per-partition",
            "2",
        ]))
        .unwrap();
        assert_eq!(config.writers, 7);
        assert_eq!(config.reader_slots(), 10);
        assert_eq!(config.args.reader_workers(), 4);
        let directory = tempfile::tempdir().unwrap();
        let settings = config.native.as_ref().unwrap();
        let brokers = broker_specs(directory.path(), config.args.system.brokers());
        let source = settings.document(&brokers).unwrap();
        let checked = ozzy_config::Deployment::parse(&source)
            .unwrap()
            .validate()
            .unwrap();
        let topic = &checked.deployment().topics["benchmark"];
        assert_eq!(topic.partitions, 5);
        assert_eq!(topic.confirmation, settings.confirmation);
        assert_eq!(topic.segment_bytes, 64 * 1024 * 1024);
        assert_eq!(
            topic.max_append_bytes,
            config.history.operation.max_body_bytes as u64
        );
        let host = ozzy_broker::host_resources().unwrap();
        for broker in &brokers {
            let plan = checked.broker_plan(&broker.name, &host).unwrap();
            assert_eq!(plan.omq_io_threads, 2);
            assert_eq!(plan.shards.len(), 3);
            assert_eq!(plan.controllers.len(), 1);
            let controller = &plan.controllers[0];
            assert_eq!(controller.shards, vec![0, 1, 2]);
            assert_eq!(controller.workers.backend, ozzy_config::IoBackend::Pool);
            assert_eq!(controller.workers.write_threads, 4);
            assert_eq!(controller.workers.max_inflight, 12);
            assert_eq!(controller.workers.queued_jobs, 48);
            assert_eq!(controller.workers.queued_bytes, 24 * 1024 * 1024);
            assert_eq!(controller.workers.progress_jobs, 9);
            assert_eq!(controller.workers.progress_bytes, 3 * 1024 * 1024);
            assert_eq!(controller.workers.open_handles, 256);
            for shard in plan.shards {
                assert_eq!(shard.budget.append_slots, 24);
                assert_eq!(shard.budget.resident_bytes, 12 * 1024 * 1024);
                assert_eq!(shard.budget.control_slots, 8);
                assert_eq!(shard.budget.control_bytes, 256 * 1024);
            }
            assert_eq!(
                plan.partitions
                    .iter()
                    .map(|partition| partition.shard)
                    .collect::<Vec<_>>(),
                vec![0, 1, 2, 0, 1]
            );
            assert_eq!(
                checked.deployment().brokers[&broker.name].endpoints,
                broker.endpoints
            );
        }
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

#[test]
fn production_defaults_keep_sixteen_shared_partitions() {
    let config = Config::production(args(&["--window", "2"])).unwrap();
    assert_eq!(config.writers, 2);
    assert_eq!(config.reader_slots(), 16);
    let settings = config.native.as_ref().unwrap();
    assert_eq!(settings.partitions, 16);
    assert_eq!(settings.io_threads, 1);
    assert_eq!(settings.shards, 1);
    assert_eq!(settings.workers.backend, ozzy_config::IoBackend::Aio);
    assert_eq!(settings.workers.write_threads, 2);
    assert_eq!(config.writer_limits().max_record_bytes, 128);
    assert_eq!(config.reader_limits().max_record_bytes, 128);
}

#[test]
fn production_refuses_unimplemented_overrides_and_invalid_admission_before_files() {
    let directory = tempfile::tempdir().unwrap();
    let invalid: &[&[&str]] = &[
        &["--segment-decoded-mib", "32"],
        &["--history-mib", "1024"],
        &["--io-threads", "2"],
        &["--direct-io", "false"],
        &[
            "--io-backend",
            "pool",
            "--direct-io",
            "false",
            "--aio-depth",
            "2",
        ],
        &["--backend-write-threads", "0"],
        &["--backend-max-inflight", "257"],
        &["--backend-queued-jobs", "1"],
        &["--backend-queued-mib", "0"],
        &["--backend-progress-jobs", "0"],
        &["--backend-progress-mib", "0"],
        &["--backend-open-handles", "0"],
        &["--shard-append-slots", "0"],
        &["--shard-resident-mib", "0"],
        &["--shard-control-slots", "0"],
        &["--shard-control-kib", "0"],
        &["--backend-queued-mib", "18446744073709551615"],
        &[
            "--app-threads",
            "2",
            "--backend-queued-mib",
            "1",
            "--record-bytes",
            "1024",
        ],
    ];
    for flags in invalid {
        let mut input = args(flags);
        input.storage_dir = directory.path().to_path_buf();
        assert!(Config::production(input).is_err(), "accepted {flags:?}");
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

#[test]
fn retired_timed_switches_are_rejected_at_parse() {
    for flags in [
        &["--disk-workers", "8"][..],
        &["--disk-owner-threads", "2"],
        &["--read-depth", "1024"],
        &["--broker-hwm", "128"],
        &["--storage-lane-capacity", "512"],
        &["--storage-group-records", "1024"],
        &["--operation-target-kib", "1024"],
        &["--write-group-target-kib", "1024"],
        &["--write-call-kib", "64"],
        &["--persistence-backlog-mib", "64"],
        &["--replication-cache-mib", "64"],
        &["--resident-read-mib", "64"],
        &["--packing-block-kib", "32"],
        &["--storage-group-kib", "128"],
        &["--writer-linger-us", "100"],
        &["--lz4-transport"],
        &["--mode", "latency"],
        &["--omq-profile", "latency"],
        &["--multipart"],
    ] {
        assert!(
            Args::try_parse_from(["bench"].into_iter().chain(flags.iter().copied())).is_err(),
            "accepted {flags:?}"
        );
    }
}

#[test]
fn native_document_refuses_invalid_fixed_endpoints_and_duplicate_brokers() {
    let config = Config::production(args(&["--system", "single-durable"])).unwrap();
    let settings = config.native.as_ref().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut brokers = broker_specs(directory.path(), 1);
    brokers[0].endpoints.peer = "tcp://127.0.0.1:0".into();
    assert!(settings.document(&brokers).is_err());
    brokers[0].endpoints.peer = brokers[0].endpoints.reader_pub.clone();
    assert!(settings.document(&brokers).is_err());
    let mut brokers = broker_specs(directory.path(), 1);
    brokers.push(Broker {
        name: brokers[0].name.clone(),
        root: directory.path().join("other"),
        endpoints: Endpoints {
            peer: "tcp://127.0.0.1:31000".into(),
            data_peer: "tcp://127.0.0.1:31002".into(),
            reader_pub: "tcp://127.0.0.1:31001".into(),
            follower_pub: None,
        },
    });
    assert!(settings.document(&brokers).is_err());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
