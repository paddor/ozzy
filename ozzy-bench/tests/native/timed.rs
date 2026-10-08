//! Full production adapter checks. Scheduled counts are correctness assertions.

use serde_json::Value;
use std::{path::Path, process::Command};

#[cfg(feature = "comparisons")]
#[test]
fn native_topology_verifies_shared_writers_and_idle_partitions() {
    comparison_topology("ozzy");
}

#[cfg(feature = "comparisons")]
#[test]
#[ignore = "requires the prepared pinned Iggy release server"]
fn iggy_topology_verifies_shared_writers_and_idle_partitions() {
    comparison_topology("iggy");
}

#[cfg(feature = "comparisons")]
#[test]
#[ignore = "requires the prepared pinned Redpanda release server"]
fn redpanda_topology_verifies_shared_writers_and_idle_partitions() {
    comparison_topology("redpanda");
}

#[cfg(feature = "comparisons")]
fn comparison_topology(implementation: &str) {
    use ozzy_bench::automation::{isolation, server};
    let storage = super::support::storage("native-comparison-topology-");
    match implementation {
        "iggy" => server::prepare(true, storage.path()).unwrap(),
        "redpanda" => server::redpanda::prepare(true).unwrap(),
        _ => (),
    }
    let cpu = isolation::cpus(None).unwrap()[0];
    for (partitions, writers) in [(2, 4), (4, 1)] {
        let root = storage.path().join(format!(
            "{implementation}-{}",
            uuid::Uuid::now_v7().simple()
        ));
        let mut external = (implementation != "ozzy").then(|| {
            server::External::start(implementation, &root, "durable", &[vec![cpu]]).unwrap()
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_ozzy_timed_bench"));
        command
            .args([
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "1",
                "--warmup",
                "0",
                "--records-per-second",
                "70",
                "--request-records",
                "32",
                "--writer-batch-records",
                "32",
                "--reader-records",
                "32",
                "--reader-payload-mib",
                "1",
                "--history-mib",
                "512",
                "--segment-mib",
                "4",
                "--payload-compression",
                "off",
                "--producer-workers",
                "1",
                "--reader-workers",
                "1",
                "--partitions",
                &partitions.to_string(),
                "--window",
                &writers.to_string(),
                "--storage-dir",
            ])
            .arg(storage.path());
        if external.is_none() {
            command.args(["--system", "single-durable"]);
        }
        if let Some(server) = &external {
            command.args([
                "--external-system",
                implementation,
                "--external-policy",
                "durable",
                "--external-endpoint",
                server.endpoint(),
            ]);
        }
        let output = command.output().unwrap();
        if let Some(server) = &mut external {
            server.monitor().check().unwrap();
            server.stop().unwrap();
        }
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "{implementation} partitions={partitions} writers={writers}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let row: Value = serde_json::from_slice(&output.stdout).unwrap();
        check_comparison(&row, implementation, partitions, writers);
    }
}

#[cfg(feature = "comparisons")]
fn check_comparison(row: &Value, implementation: &str, partitions: u64, writers: u64) {
    assert_eq!(row["total_confirmed_records"], 70);
    assert_eq!(row["total_verified_records"], 70);
    assert_eq!(row["producer_writers"], writers);
    assert_eq!(
        row["effective_topic_topology"],
        serde_json::json!({
            "logical_stream_count":1, "topic_count":1,
            "partitions_per_topic":partitions, "partition_count":partitions,
        })
    );
    for key in [
        "logical_stream_count",
        "topic_count",
        "partitions_per_topic",
        "partition_count",
    ] {
        assert_eq!(row[key], row["effective_topic_topology"][key]);
    }
    assert_eq!(
        row["writer_protocol"],
        match implementation {
            "ozzy" => "peer-appends",
            "iggy" => "iggy-send-messages",
            "redpanda" => "kafka-records",
            _ => unreachable!(),
        }
    );
}

#[test]
fn production_runner_verifies_shared_writer_reader_cohorts_and_drains_each_policy() {
    let storage = super::support::storage("native-timed-production-");
    for system in ["single-durable", "disk-quorum", "replicated-persisting"] {
        let output = Command::new(env!("CARGO_BIN_EXE_ozzy_timed_bench"))
            .args([
                "--processes",
                "--network-ingress",
                "--streaming",
                "--system",
                system,
                "--duration",
                "1",
                "--warmup",
                "0",
                "--records-per-second",
                "70",
                "--partitions",
                "5",
                "--window",
                "7",
                "--producer-workers",
                "3",
                "--reader-workers",
                "4",
                "--readers-per-partition",
                "2",
                "--app-threads",
                "3",
                "--request-records",
                "32",
                "--writer-batch-records",
                "32",
                "--reader-records",
                "32",
                "--reader-payload-mib",
                "1",
                "--segment-mib",
                "4",
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
            "{system}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let row: Value = serde_json::from_slice(&output.stdout).unwrap();
        check(&row, system);
    }
}

fn check(row: &Value, system: &str) {
    check_provenance(row);
    check_scheduled_cohort(row);
    check_group(row, system);
    assert_eq!(row["broker_runtime"], "production-deployment");
    assert_eq!(row["total_confirmed_records"], 70);
    assert_eq!(row["total_verified_records"], 70);
    assert_eq!(row["producer_writers"], 7);
    assert_eq!(row["partition_count"], 5);
    assert_eq!(row["logical_stream_count"], 1);
    assert_eq!(
        row["effective_topic_topology"],
        serde_json::json!({
            "logical_stream_count":1, "topic_count":1,
            "partitions_per_topic":5, "partition_count":5,
        })
    );
    assert_eq!(row["journals_per_broker"], 5);
    assert_eq!(row["producer_workers"], 3);
    let writer_workers = row["writers"].as_array().unwrap();
    assert_eq!(writer_workers.len(), 3);
    let mut writer_pids = std::collections::HashSet::new();
    for (index, worker) in writer_workers.iter().enumerate() {
        assert_eq!(worker["index"], index);
        assert!(writer_pids.insert(worker["pid"].as_u64().unwrap()));
    }
    assert_eq!(row["reader_workers"], 4);
    assert_eq!(row["broker_application_threads"], 3);
    assert_eq!(row["broker_dispatch_threads"], 1);
    assert_eq!(row["broker_omq_io_threads"], 1);
    assert!(row["transport"].as_str().unwrap().contains("PEER APPEND"));
    assert!(
        row["reader_transport"]
            .as_str()
            .unwrap()
            .contains("PUB/SUB")
    );
    for broker in row["brokers"].as_array().unwrap() {
        assert_eq!(broker["event"], "drained");
        assert_eq!(
            broker["identity"]["topology"]["observed_threads"]["omq_io"],
            1
        );
        assert!(broker["usage"]["cpu_seconds"].as_f64().unwrap() > 0.0);
        assert!(
            broker["drain_boundary"]
                .as_str()
                .unwrap()
                .contains("settled and stopped")
        );
        assert!(
            broker.get("operation").is_none(),
            "legacy snapshot masquerades as partition history"
        );
    }
    let delivered: u64 = row["readers"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|worker| worker["topic_readers"].as_array().unwrap())
        .map(|reader| reader["delivered"].as_u64().unwrap())
        .sum();
    assert_eq!(delivered, 140);
    let writers: Vec<_> = row["writers"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|worker| worker["lanes"].as_array().unwrap())
        .collect();
    assert_eq!(writers.len(), 7);
    assert!(writers.iter().all(|lane| lane["total"] == 10));
}

fn check_group(row: &Value, system: &str) {
    let (policy, brokers, copies) = match system {
        "single-durable" => ("local_durable", 1, 1),
        "disk-quorum" => ("quorum_durable", 3, 2),
        "replicated-persisting" => ("quorum_replicated_persisting", 3, 2),
        _ => unreachable!(),
    };
    assert_eq!(row["commit_policy"], policy);
    assert_eq!(row["ack_copies"], copies);
    assert_eq!(row["replica_voters"], brokers);
    assert_eq!(row["storage_copies"], brokers);
    let observed = row["brokers"].as_array().unwrap();
    assert_eq!(observed.len(), brokers);
    let mut pids = std::collections::HashSet::new();
    for broker in observed {
        assert!(pids.insert(broker["identity"]["pid"].as_u64().unwrap()));
        assert_eq!(
            broker["identity"]["executable_xxh3_128"],
            row["provenance"]["executable_xxh3_128"]
        );
    }
}

fn check_provenance(row: &Value) {
    let provenance = &row["provenance"];
    assert_eq!(
        provenance["executable_xxh3_128"],
        ozzy_bench::provenance::digest(Path::new(env!("CARGO_BIN_EXE_ozzy_timed_bench"))).unwrap()
    );
    assert_eq!(
        provenance["cargo_lock_xxh3_128"],
        ozzy_bench::provenance::digest(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.lock")
        )
        .unwrap()
    );
    let revision = Command::new("git")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(revision.status.success());
    assert_eq!(
        provenance["source_checkout_revision"],
        String::from_utf8(revision.stdout).unwrap().trim()
    );
    assert!(provenance["source_checkout_dirty"].is_boolean());
}

fn check_scheduled_cohort(row: &Value) {
    let scheduled = &row["scheduled_load"];
    assert_eq!(scheduled["records_per_second"], 70);
    assert_eq!(scheduled["planned_measurement_records"], 70);
    assert_eq!(scheduled["planned_total_records"], 70);
    assert_eq!(scheduled["producer_ack"]["samples"], 70);
    assert_eq!(scheduled["reader_delivery"]["samples"], 140);
    assert_eq!(scheduled["scheduling_lag"]["samples"], 70);
    assert_eq!(row["producer_ack"]["samples"], 70);
    assert_eq!(row["reader_delivery"]["samples"], 140);
    assert!(row["writer_drain_seconds"].is_number());
    assert!(row["reader_drain_seconds"].is_number());
}
