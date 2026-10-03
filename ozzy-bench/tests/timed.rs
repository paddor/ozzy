//! Functional timed-window and verification gates, not performance assertions.

mod support;

use std::process::Command;

#[test]
fn disk_and_sdk_packing_preserve_native_confirmation_and_reader_verification() {
    let storage = support::storage("native-codecs-");
    // Live readers stay in memory inside the unpersisted backlog and the newest
    // two segments. A replicated-persisting broker may write its whole 64 MiB
    // backlog while its application thread waits for a CPU, so those segments
    // must cover the backlog. The other modes keep small segments and roll often.
    for (system, codec, size, segment_mib) in [
        ("single-durable", "raw", "4096", "4"),
        ("single-durable", "lz4", "4096", "4"),
        ("disk-quorum", "lz4", "1024", "4"),
        ("disk-quorum", "raw", "1024", "4"),
        ("replicated-persisting", "lz4", "1024", "128"),
        ("replicated-persisting", "raw", "1024", "128"),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"));
        command
            .args([
                "--processes",
                "--network-ingress",
                "--streaming",
                "--system",
                system,
                "--payload-compression",
                if codec == "lz4" { "adaptive" } else { "off" },
                "--duration",
                "1",
                "--warmup",
                "0",
                "--window",
                "2",
                "--producer-workers",
                "1",
                "--reader-workers",
                "1",
                "--record-bytes",
                size,
                "--request-records",
                "256",
                "--writer-batch-records",
                "256",
                "--balanced-partitions",
                "--history-mib",
                "512",
                "--segment-mib",
                segment_mib,
                "--segment-decoded-mib",
                "64",
                "--storage-dir",
            ])
            .arg(storage.path());
        command.args(["--partitions", "2"]);
        let output = command.output().unwrap();
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "{system}/{codec}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        check_production_storage(&row);
        assert_eq!(
            row["payload_compression"],
            if codec == "lz4" {
                "adaptive-lz4"
            } else {
                "none"
            }
        );
        assert_eq!(
            row["total_confirmed_records"],
            row["total_verified_records"]
        );
        assert!(row["total_verified_records"].as_u64().unwrap() > 0);
    }
}

#[test]
fn sparse_scheduled_writers_confirm_without_filling_the_request_records() {
    let storage = support::storage("native-sparse-");
    let output = Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"))
        .args([
            "--processes",
            "--network-ingress",
            "--streaming",
            "--system",
            "single-durable",
            "--duration",
            "1",
            "--warmup",
            "0",
            "--window",
            "3",
            "--producer-workers",
            "2",
            "--reader-workers",
            "2",
            "--record-bytes",
            "128",
            "--records-per-second",
            "7",
            "--partitions",
            "3",
            "--io-backend",
            "pool",
            "--direct-io",
            "false",
            "--storage-dir",
        ])
        .arg(storage.path())
        .output()
        .unwrap();
    assert!(
        output.status.success() && output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(row["total_confirmed_records"], 7);
    assert_eq!(row["total_verified_records"], 7);
    let schedule = &row["scheduled_load"];
    assert_eq!(schedule["release_quantum_ns"], 0);
    assert!(
        schedule["release_timer"]
            .as_str()
            .unwrap()
            .contains("timerfd")
    );
    assert_eq!(schedule["records_per_second"], 7);
    assert_eq!(schedule["planned_measurement_records"], 7);
    for field in ["producer_ack", "reader_delivery", "scheduling_lag"] {
        assert_eq!(schedule[field]["samples"], 7);
    }
    // This is a progress assertion, not a speed threshold: a fill-window loop
    // would observe zero confirmations until final drain and invalidate the run.
    assert!(row["producer_records_per_second"].as_f64().unwrap() > 0.0);
    let totals: Vec<_> = row["writers"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|worker| worker["lanes"].as_array().unwrap())
        .map(|lane| {
            (
                lane["lane"].as_u64().unwrap(),
                lane["total"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(totals, [(0, 3), (1, 2), (2, 2)]);
}

#[test]
fn excessive_scheduled_backlog_fails_instead_of_reducing_the_offered_rate() {
    let storage = support::storage("native-overload-");
    let output = Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"))
        .args([
            "--processes",
            "--network-ingress",
            "--streaming",
            "--system",
            "single-durable",
            "--duration",
            "1",
            "--warmup",
            "0",
            "--window",
            "1",
            "--request-records",
            "1024",
            "--record-bytes",
            "128",
            "--records-per-second",
            "1000000000",
            "--partitions",
            "1",
            "--io-backend",
            "pool",
            "--direct-io",
            "false",
            "--storage-dir",
        ])
        .arg(storage.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("scheduled backlog exceeded"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn native_modes_have_live_verified_readers_and_exact_confirmation_boundaries() {
    let storage = support::storage("native-functional-");
    for (system, copies, policy) in [
        ("single-durable", 1, "local_durable"),
        ("disk-quorum", 2, "quorum_durable"),
        ("replicated-persisting", 2, "quorum_replicated_persisting"),
    ] {
        for size in ["128", "1024"] {
            let mut command = Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"));
            command.args([
                "--processes",
                "--network-ingress",
                "--system",
                system,
                "--duration",
                "1",
                "--warmup",
                "0",
                "--window",
                "3",
                "--producer-workers",
                "2",
                "--reader-workers",
                "2",
                "--record-bytes",
                size,
                "--history-mib",
                "512",
                "--segment-mib",
                "256",
            ]);
            command
                .args(["--streaming", "--request-records", "1024", "--storage-dir"])
                .arg(storage.path());
            command.args(["--partitions", "3"]);
            let output = command.output().unwrap();
            if !output.status.success() || !output.stderr.is_empty() {
                let storage = storage.keep();
                panic!(
                    "system={system} size={size}: {}; storage artifacts: {}",
                    String::from_utf8_lossy(&output.stderr),
                    storage.display(),
                );
            }
            let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(row["measurement_seconds"], 1.0);
            assert_eq!(
                row["total_verified_records"],
                row["total_confirmed_records"]
            );
            assert!(row["total_verified_records"].as_u64().unwrap() > 0);
            assert!(
                row["consumer_records_per_second"].as_f64().unwrap() > 0.0,
                "system={system} size={size}: {row}"
            );
            assert!(row.get("producer_outstanding_records_limit").is_none());
            assert_eq!(row["writer_inflight_appends"], 3);
            check_protocol_requests(&row, size.parse().unwrap());
            assert_eq!(row["producer_workers"], 2);
            assert_eq!(row["reader_workers"], 2);
            assert_eq!(row["topic_count"], 1);
            assert_eq!(row["partitions_per_topic"], 3);
            assert_eq!(row["partition_count"], 3);
            assert_eq!(row["broker_application_shards"], 1);
            assert_eq!(row["intervals"].as_array().unwrap().len(), 1);
            assert_eq!(
                row["brokers"].as_array().unwrap().len(),
                if copies == 1 { 1 } else { 3 }
            );
            assert_eq!(row["system"], system);
            assert_eq!(row["ack_copies"], copies);
            check_production_storage(&row);
            assert_eq!(row["segment_bytes"], 256 * 1024 * 1024);
            assert_eq!(row["commit_policy"], policy);
            for broker in row["brokers"].as_array().unwrap() {
                assert!(broker["usage"]["cpu_seconds"].as_f64().unwrap() > 0.0);
                assert!(broker["usage"]["elapsed_seconds"].as_f64().unwrap() > 1.0);
                assert_eq!(
                    broker["usage"]["allocation_counted"],
                    cfg!(feature = "allocation-counting")
                );
            }
            assert!(row["reader_delivery"]["samples"].as_u64().unwrap() > 0);
        }
    }
}

fn check_production_storage(row: &serde_json::Value) {
    assert_eq!(row["broker_runtime"], "production-deployment");
    assert_eq!(row["storage_copies"], row["replica_voters"]);
    assert_eq!(row["journals_per_broker"], row["partition_count"]);
    assert_eq!(row["broker_dispatch_threads"], 1);
    for broker in row["brokers"].as_array().unwrap() {
        assert_eq!(broker["event"], "drained");
        assert!(
            broker["drain_boundary"]
                .as_str()
                .unwrap()
                .contains("settled and stopped")
        );
        let topology = &broker["identity"]["topology"];
        assert_eq!(
            topology["application_threads"],
            row["broker_application_threads"]
        );
        assert_eq!(
            topology["partitions"].as_array().unwrap().len() as u64,
            row["partition_count"].as_u64().unwrap()
        );
        assert!(broker.get("operation").is_none());
    }
}

fn check_protocol_requests(row: &serde_json::Value, size: u64) {
    assert_eq!(row["writer_protocol"], "peer-appends");
    assert!(row["writer_wire_records_per_message"].is_null());
    assert_eq!(row["records_per_submission"], 1);
    assert!(row["records_per_append"].is_null());
    assert_eq!(row["writer_batch_records_max"], 1024);
    assert_eq!(
        row["writer_batch_payload_bytes_max"],
        (1024 * size).min(832 * 1024)
    );
    assert_eq!(row["writer_linger_us"], 0);
    for writer in row["writers"].as_array().unwrap() {
        for lane in writer["lanes"].as_array().unwrap() {
            let stats = &lane["protocol_requests"];
            let requests = stats["requests"].as_u64().unwrap();
            let records = stats["records"].as_u64().unwrap();
            // Socket admission counts retransmissions. Unique confirmations
            // and verified reader records are checked against total separately.
            assert!(records >= lane["total"].as_u64().unwrap());
            // APPEND statistics count encoded wire bytes; readers separately
            // verify every decoded byte. Adaptive LZ4 may shrink the payload.
            assert!((1..=size * records).contains(&stats["payload_bytes"].as_u64().unwrap()));
            assert!(requests > 0);
            assert!(requests <= records);
            assert!(stats["min_records"].as_u64().unwrap() >= 1);
            assert!(stats["max_records"].as_u64().unwrap() <= 1024);
            let bins = stats["record_count_buckets"].as_array().unwrap();
            assert_eq!(bins.len(), 32);
            assert!((1..=3).contains(&stats["max_inflight_appends"].as_u64().unwrap()));
            assert_eq!(
                bins.iter().map(|n| n.as_u64().unwrap()).sum::<u64>(),
                requests
            );
        }
    }
}

#[test]
fn disk_quorum_admits_replication_with_sixteen_readers() {
    let storage = support::storage("native-readers-");
    let output = Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"))
        .args([
            "--processes",
            "--network-ingress",
            "--system",
            "disk-quorum",
            "--duration",
            "1",
            "--warmup",
            "0",
            "--window",
            "16",
            "--producer-workers",
            "4",
            "--reader-workers",
            "1",
            "--record-bytes",
            "128",
            "--history-mib",
            "512",
            "--segment-mib",
            "64",
            "--partitions",
            "16",
            "--storage-dir",
        ])
        .arg(storage.path())
        .output()
        .unwrap();
    assert!(
        output.status.success() && output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(row["partition_count"], 16);
    assert_eq!(
        row["total_verified_records"],
        row["total_confirmed_records"]
    );
    assert!(row["consumer_records_per_second"].as_f64().unwrap() > 0.0);
}

#[test]
fn invalid_timed_arguments_fail_without_valid_output() {
    for extra in [
        vec![
            "--streaming",
            "--mode",
            "latency",
            "--records-per-second",
            "1000",
        ],
        vec!["--streaming", "--records-per-second", "0"],
        vec!["--duration", "NaN"],
        vec!["--duration", "5", "--records", "10"],
        vec!["--duration", "5", "--history-mib", "0"],
        vec!["--duration", "5", "--mode", "latency"],
        vec!["--duration", "5", "--batch", "1024"],
        vec!["--duration", "5", "--segment-mib", "0"],
        vec!["--duration", "5", "--segment-mib", "4097"],
        vec!["--duration", "5", "--disk-workers", "2"],
        vec!["--duration", "5", "--read-depth", "0"],
        vec!["--duration", "5", "--read-depth", "1025"],
        vec!["--duration", "5", "--app-threads", "0"],
        vec!["--duration", "5", "--app-threads", "33"],
        vec![
            "--duration",
            "5",
            "--system",
            "single-buffered",
            "--segment-mib",
            "2048",
        ],
        vec!["--streaming", "--duration", "5", "--codec", "lz4"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"))
            .args(["--processes", "--network-ingress"])
            .args(extra)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn single_durable_partitions_split_across_application_threads() {
    let storage = support::storage("native-shards-");
    let output = Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"))
        .args([
            "--processes",
            "--network-ingress",
            "--streaming",
            "--system",
            "single-durable",
            "--duration",
            "1",
            "--warmup",
            "0",
            "--window",
            "6",
            "--partitions",
            "6",
            "--balanced-partitions",
            "--producer-workers",
            "2",
            "--reader-workers",
            "2",
            "--record-bytes",
            "128",
            "--app-threads",
            "2",
            "--history-mib",
            "512",
            "--segment-mib",
            "64",
            "--storage-dir",
        ])
        .arg(storage.path())
        .output()
        .unwrap();
    assert!(
        output.status.success() && output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    check_production_storage(&row);
    assert_eq!(row["partition_count"], 6);
    assert_eq!(row["broker_application_shards"], 2);
    assert_eq!(row["broker_dispatch_threads"], 1);
    assert_eq!(row["broker_omq_io_threads"], 1);
    let partitions = row["brokers"][0]["identity"]["topology"]["partitions"]
        .as_array()
        .unwrap();
    assert_eq!(
        partitions
            .iter()
            .map(|partition| partition["shard"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [0, 1, 0, 1, 0, 1]
    );
    assert_eq!(
        row["total_confirmed_records"],
        row["total_verified_records"]
    );
    assert!(row["producer_records_per_second"].as_f64().unwrap() > 0.0);
    assert!(row["consumer_records_per_second"].as_f64().unwrap() > 0.0);
}

#[test]
fn payload_compression_is_decoded_and_verified_by_native_readers() {
    let storage = support::storage("sdk-compression-");
    for (system, size, pattern) in [
        ("single-durable", "4096", "--random-payload"),
        ("single-durable", "8192", "--json-payload"),
        ("disk-quorum", "4096", "--random-payload"),
        ("disk-quorum", "8192", "--json-payload"),
        ("replicated-persisting", "4096", "--random-payload"),
        ("replicated-persisting", "8192", "--json-payload"),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"));
        command
            .args([
                "--processes",
                "--network-ingress",
                "--streaming",
                "--system",
                system,
                "--duration",
                "3",
                "--warmup",
                "0",
                "--window",
                "2",
                "--producer-workers",
                "1",
                "--reader-workers",
                "1",
                "--reader-records",
                "32",
                "--record-bytes",
                size,
                "--request-records",
                "256",
                "--writer-batch-records",
                "256",
                "--history-mib",
                "512",
                "--segment-mib",
                "4",
                pattern,
                "--storage-dir",
            ])
            .arg(storage.path());
        // Keep two destinations. A fixed cohort checks large frames and codec
        // fallback without using saturation as a test gate.
        command.args(["--partitions", "2", "--records-per-second", "200"]);
        let output = command.output().unwrap();
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "{system}/{size}/{pattern}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        check_payload_compression(&row, system, size, pattern);
        check_production_storage(&row);
        assert_eq!(row["partition_count"], 2);
        assert_eq!(row["total_confirmed_records"], 600);
        assert_eq!(row["total_verified_records"], 600);
    }
}

fn check_payload_compression(row: &serde_json::Value, system: &str, size: &str, pattern: &str) {
    assert_eq!(row["payload_compression"], "adaptive-lz4");
    assert_eq!(
        row["payload_compression_threshold"],
        ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD
    );
    assert_eq!(row["native_reader_api"], "decoded-records");
    assert_eq!(
        row["total_confirmed_records"],
        row["total_verified_records"]
    );
    let lanes = row["writers"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|worker| worker["lanes"].as_array().unwrap());
    for lane in lanes {
        let stats = &lane["protocol_requests"];
        let original = stats["records"].as_u64().unwrap() * size.parse::<u64>().unwrap();
        let wire = stats["payload_bytes"].as_u64().unwrap();
        // Random bodies do not shrink, so adaptive compression sends them raw.
        if pattern == "--random-payload" {
            assert_eq!(wire, original, "{system}: random payload");
        } else {
            assert!(wire < original, "{system}: JSON payload was not compressed");
        }
    }
}

#[test]
fn native_remote_placement_checks_storage_cpu_masks_and_cleanup() {
    for policy in ["disk-quorum", "replicated-persisting"] {
        remote_placement(policy, "normal");
    }
}

#[test]
fn native_remote_executable_mismatch_refuses_before_formatting_and_cleans_resources() {
    remote_placement("disk-quorum", "mismatch");
}

#[test]
fn native_remote_warning_refuses_before_measurement_and_cleans_resources() {
    remote_placement("disk-quorum", "warning");
}

#[expect(
    clippy::too_many_lines,
    reason = "keep SSH launch, placement observations, and cleanup in one regression"
)]
fn remote_placement(policy: &str, mode: &str) {
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;
    let root = support::storage("native-placements-");
    let stores = [
        root.path().join("disk-zero"),
        root.path().join("disk-one"),
        root.path().join("remote user's disk"),
    ];
    for store in &stores {
        std::fs::create_dir(store).unwrap();
    }
    let ssh = root.path().join("ssh");
    let banner = if mode == "warning" {
        "echo 'warning: placement fixture' >&2\n"
    } else {
        ""
    };
    std::fs::write(
        &ssh,
        format!(
            r#"#!/bin/sh
{banner}while [ "$1" != -- ]; do shift; done
shift
shift
exec sh -c "$1"
"#
        ),
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let cpus = ozzy_bench::automation::isolation::cpus(None).unwrap();
    let masks: Vec<_> = (0..3).map(|i| vec![cpus[i % cpus.len()]]).collect();
    let executable = env!("CARGO_BIN_EXE_ozy_timed_bench");
    let mut remote_executable = std::path::PathBuf::from(executable);
    if mode == "mismatch" {
        use std::io::Write;
        remote_executable = root.path().join("different executable");
        std::fs::copy(executable, &remote_executable).unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&remote_executable)
            .unwrap()
            .write_all(b"different artifact")
            .unwrap();
    }
    let placements = root.path().join("placements.json");
    std::fs::write(&placements, json!([
        {"bind":"127.0.0.1", "storage_dir":stores[0], "cpus":masks[0]},
        {"bind":"127.0.0.1", "storage_dir":stores[1], "cpus":masks[1]},
        {"bind":"127.0.0.1", "ssh":"test-host", "executable":remote_executable, "storage_dir":stores[2], "cpus":masks[2]}
    ]).to_string()).unwrap();
    let started = std::time::Instant::now();
    let output = Command::new(executable)
        .env(
            "PATH",
            format!(
                "{}:{}",
                root.path().display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .args([
            "--processes",
            "--network-ingress",
            "--streaming",
            "--system",
            policy,
            "--duration",
            if mode == "warning" { "20" } else { "0.2" },
            "--warmup",
            if mode == "normal" { "1" } else { "0" },
            "--window",
            "2",
            "--producer-workers",
            "1",
            "--reader-workers",
            "1",
            "--record-bytes",
            "128",
            "--request-records",
            "64",
            "--partitions",
            "2",
            "--segment-mib",
            "4",
            "--control-bind",
            "tcp://127.0.0.1:0",
            "--placements",
        ])
        .arg(&placements)
        .arg("--storage-dir")
        .arg(root.path().join("must-not-be-used"))
        .output()
        .unwrap();
    if mode != "normal" {
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let cause = if mode == "warning" {
            "warning: placement fixture"
        } else {
            "prepared broker identity, build, or storage mismatch"
        };
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(cause),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(30));
        for store in &stores {
            assert_eq!(std::fs::read_dir(store).unwrap().count(), 0);
        }
        return;
    }
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        row["total_confirmed_records"],
        row["total_verified_records"]
    );
    assert!(row["total_verified_records"].as_u64().unwrap() > 0);
    assert!(!root.path().join("must-not-be-used").exists());
    for (index, store) in stores.iter().enumerate() {
        let broker = &row["brokers"][index];
        let devices = broker["identity"]["storage"].as_array().unwrap();
        assert_eq!(devices.len(), 1);
        let data_root = std::path::Path::new(devices[0]["root"].as_str().unwrap());
        assert_eq!(
            data_root.parent().unwrap().parent().unwrap(),
            std::fs::canonicalize(store).unwrap()
        );
        for thread in broker["usage"]["execution"]["threads"].as_array().unwrap() {
            assert_eq!(thread["cpus"], json!(masks[index]));
        }
        assert!(broker["usage"].get("journal_reads").is_none());
        assert_eq!(std::fs::read_dir(store).unwrap().count(), 0);
    }
}
