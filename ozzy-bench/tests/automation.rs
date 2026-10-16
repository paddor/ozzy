//! Regression coverage for measurement validity, isolation, and cached baselines.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "test code"
)]
use clap::Parser;
use ozzy_bench::automation::{
    artifact_root, compare, isolation, records, source, supervise, validation, workloads,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, path::Path, process::Command, time::Duration};

// Configuration fixtures describe CPUs independently of the executing host.
const VALIDATION_CPUS: &[usize] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

#[path = "automation/commands.rs"]
mod commands;
#[path = "automation/guards.rs"]
mod guards;
#[path = "automation/ledgers.rs"]
mod ledgers;
#[path = "automation/measurements.rs"]
mod measurements;
#[path = "automation/provenance.rs"]
mod provenance;
#[path = "automation/references.rs"]
mod references;
#[path = "automation/topology.rs"]
mod topology;

fn saved(implementation: &str, id: &str) -> Value {
    json!({"kind":"measurement","run_id":id,"repetition":1,
        "case":{"impl":implementation,"mode":"buffered","size":128,"codec":"raw","pattern":"json","rate":null},
        "measurements":{"confirmed_mib_s":5,"verified_mib_s":5},
        "source":{"compiler":"rustc","revision":id},"workload_sha256":"workload",
        "process_isolation":{"contract":isolation::CONTRACT,"observations":1,"after_shutdown":isolation::empty_proof()},
        "environment":{"cpu":"cpu","memory":"24GiB","filesystem":"ssd"},
        "configuration":{"impl":implementation,"repetitions":1,"duration":10,"warmup":5,"broker_cpus":[0,1]}})
}

fn save(root: &Path, row: &Value, complete: bool) {
    let implementation = row["case"]["impl"].as_str().unwrap();
    records::append(root, implementation, row).unwrap();
    if complete {
        records::append(
            root,
            implementation,
            &json!({"kind":"run-complete","run_id":row["run_id"]}),
        )
        .unwrap();
    }
}

/// A valid external (Iggy) comparison row; adapters share these checks.
fn external_row() -> Value {
    json!({"system":"iggy-buffered","native_client_protocol":false,"record_bytes":1024,
        "record_corpus":ozzy_bench::workload::JSON_EVENT_CORPUS,
        "topic_count":1,"partition_count":4,"partitions_per_topic":4,
        "total_verified_records":100,"total_confirmed_records":100,"producer_records_per_second":100,"consumer_records_per_second":100,
        "producer_ack":{"samples":100,"p50_us":1,"p99_us":2},"reader_delivery":{"samples":100,"p50_us":1,"p99_us":2},"reader_drain_seconds":0,
        "warmup_seconds":5,"measurement_seconds":10,"request_records":1024,"records_per_append":1024,
        "reader_records_max":16384,"reader_payload_bytes_max":16_777_216})
}

/// An Iggy comparison case that `external_row` satisfies.
fn external_case(size: u64, pattern: &str) -> Value {
    json!({"impl":"iggy","mode":"buffered","size":size,"pattern":pattern})
}

fn option<'a>(command: &'a [String], key: &str) -> &'a str {
    &command[command.iter().position(|v| v == key).unwrap() + 1]
}
