//! Functional worker checks through production storage, admission, and SDKs.

use super::*;
use crate::bench::Args;
use clap::Parser;
use futures::future::try_join_all;
use ozzy_bench::native::{BrokerWorker, DeploymentArtifact};
use serde_json::json;
use std::{net::TcpListener, time::Duration};

fn config(record_bytes: usize) -> Config {
    let mut args = Args::parse_from([
        "bench",
        "--system",
        "single-durable",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--window",
        "4",
        "--producer-workers",
        "1",
        "--duration",
        "1",
        "--warmup",
        "0",
        "--records-per-second",
        "2",
        "--request-records",
        "4",
    ]);
    args.group = Some(uuid::Uuid::now_v7());
    args.record_bytes = record_bytes;
    args.reader_workers = Some(1);
    args.reader_records = 4;
    args.readers_per_partition = 2;
    Config::new(args).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn production_shared_writers_use_pacing_with_interleaved_offsets_and_idle_lanes() {
    tokio::time::timeout(Duration::from_secs(30), exercise_pacing(128, false))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn production_topic_readers_replay_raw_and_packed_shared_writer_cohorts() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for size in [128, 4096] {
            exercise_pacing(size, true).await;
        }
    })
    .await
    .unwrap();
}

async fn start_broker(partitions: u32) -> (tempfile::TempDir, ozzy_broker::Broker, Value) {
    let executable = std::env::current_exe().unwrap();
    let directory = tempfile::Builder::new()
        .prefix("native-shared-writers-")
        .tempdir_in(executable.parent().unwrap())
        .unwrap();
    let reservations = [
        TcpListener::bind("127.0.0.1:0").unwrap(),
        TcpListener::bind("127.0.0.1:0").unwrap(),
    ];
    let peer = format!("tcp://{}", reservations[0].local_addr().unwrap());
    let readers = format!("tcp://{}", reservations[1].local_addr().unwrap());
    let root = toml::Value::String(directory.path().join("local").to_str().unwrap().into());
    // Four writers need independent backed grants while the reader copies run.
    let document = format!(
        "[cluster]\nmode = \"single\"\n\
         [topics.orders]\nconfirmation = \"local-durable\"\npartitions = {partitions}\n\
         segment_bytes = 1048576\nmax_append_bytes = 65536\n\
         [brokers.local.endpoints]\npeer = \"{peer}\"\nreader_pub = \"{readers}\"\n\
         [brokers.local.devices.ssd]\nroot = {root}\ncontroller = \"ssd\"\n\
         [brokers.local.devices.ssd.workers]\nbackend = \"pool\"\n\
         write_threads = 1\nmax_inflight = 8\nqueued_jobs = 32\nqueued_bytes = 8388608\n\
         progress_jobs = 8\nprogress_bytes = 1048576\nopen_handles = 256\n\
         [[brokers.local.topology.shards]]\nid = 7\ndevice = \"ssd\"\n\
         [brokers.local.topology.shards.budget]\nappend_slots = 128\n\
         resident_bytes = 67108864\ncontrol_slots = 64\ncontrol_bytes = 1048576\n"
    );
    let deployment = DeploymentArtifact::initialize(directory.path(), &document).unwrap();
    let checked = deployment.checked("local").unwrap();
    let setup = json!({
        "topic":"orders", "partitions":partitions,
        "brokers":[{"node":checked.identity.brokers["local"].to_string(), "endpoint":peer}],
        "append":{"writers":4*partitions, "requests":16*partitions,
            "records":128*partitions, "bytes":268_435_456_u64*u64::from(partitions)},
        "reader":{"subscriptions":2*partitions, "bytes":16_777_216, "queue_messages":4},
    });
    let worker = BrokerWorker {
        deployment,
        broker: "local".into(),
        identity: directory.path().join("local.identity"),
    };
    worker.initialize().await.unwrap();
    drop(reservations);
    let broker = worker.start_trusted().await.unwrap();
    (directory, broker, setup)
}

async fn exercise_pacing(record_bytes: usize, read: bool) {
    let (_directory, broker, setup) = start_broker(1).await;
    let config = config(record_bytes);
    let runtime = WriterRuntime::new().unwrap();
    let (writers, links) = connect_shared(&runtime, &config, &setup, 0..4)
        .await
        .unwrap();
    assert_eq!(links.socket_count(), 1);
    for lane in 0..4 {
        super::super::build_payload_pool(&config, lane).unwrap();
    }
    let group = super::super::config::group(&config.args).unwrap();
    let window = super::super::Window::new(
        super::super::metrics::monotonic_ns() + 20_000_000,
        &config.args,
    )
    .unwrap();
    window.wait().await;
    let rows = try_join_all(
        writers
            .into_iter()
            .enumerate()
            .map(|(lane, writer)| super::super::run_lane(writer, &config, group, lane, window)),
    )
    .await
    .unwrap();
    // Two writers share one partition. Both start at sequence zero, while the
    // second record has global offset one. The other two lanes send nothing.
    for (row, total) in rows.iter().zip([1, 1, 0, 0]) {
        assert_eq!(row["partition"], 0);
        assert_eq!(row["total"], total);
        assert_eq!(row["submitted_per_second"], json!([total]));
        let transmissions = row["protocol_requests"]["requests"].as_u64().unwrap();
        assert!(transmissions >= total);
        assert_eq!(row["protocol_requests"]["records"], transmissions);
        assert_eq!(
            row["scheduled_latency"][0]["bins"]
                .as_array()
                .unwrap()
                .iter()
                .map(|count| count.as_u64().unwrap())
                .sum::<u64>(),
            total,
        );
        if total == 0 {
            assert_eq!(transmissions, 0);
            assert_eq!(row["confirmed_partition_offset_range"], Value::Null);
        }
        assert_eq!(row["overloaded_stage"], Value::Null);
    }
    assert_eq!(rows[0]["confirmed_partition_offset_range"], json!([0, 0]));
    assert_eq!(rows[1]["confirmed_partition_offset_range"], json!([1, 1]));
    for row in &rows[..2] {
        let maximum = row["protocol_requests"]["max_payload_bytes"]
            .as_u64()
            .unwrap();
        if record_bytes == 128 {
            assert_eq!(maximum, 128);
        } else {
            assert!(
                maximum < record_bytes as u64,
                "large structured records must use packed APPENDs"
            );
        }
    }
    if read {
        check_readers(&runtime, &config, &setup, group, window, &rows).await;
    }
    links.shutdown().await.unwrap();
    broker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn production_topic_readers_assign_partition_copies_across_workers() {
    tokio::time::timeout(Duration::from_secs(30), partition_workers())
        .await
        .unwrap();
}

async fn partition_workers() {
    use super::super::super::reader::native;
    use super::super::super::results;
    let (_directory, broker, setup) = start_broker(2).await;
    let mut config = config(128);
    config.args.records_per_second = Some(10);
    let runtime = WriterRuntime::new().unwrap();
    let (writers, writing) = connect_shared(&runtime, &config, &setup, 0..4)
        .await
        .unwrap();
    for lane in 0..4 {
        super::super::build_payload_pool(&config, lane).unwrap();
    }
    let group = super::super::config::group(&config.args).unwrap();
    let window = super::super::Window::new(
        super::super::metrics::monotonic_ns() + 20_000_000,
        &config.args,
    )
    .unwrap();
    window.wait().await;
    let rows = try_join_all(
        writers
            .into_iter()
            .enumerate()
            .map(|(lane, writer)| super::super::run_lane(writer, &config, group, lane, window)),
    )
    .await
    .unwrap();
    let writers = [json!({"lanes":rows})];
    let totals = results::totals(&writers, config.writers).unwrap();
    assert_eq!(totals, [3, 3, 2, 2]);
    let (_finish, finished) = tokio::sync::watch::channel(Some(totals));
    for workers in [1, 3] {
        config.args.reader_workers = Some(workers);
        let mut links = Vec::new();
        let mut tasks = Vec::new();
        for index in 0..workers {
            let (assigned, connection) = native::connect(&runtime, &config, &setup, index)
                .await
                .unwrap();
            assert_eq!(connection.socket_count(), 2);
            links.push(connection);
            tasks.extend(assigned);
        }
        let reports = try_join_all(
            tasks
                .into_iter()
                .map(|task| native::run_task(task, &config, group, window, finished.clone())),
        )
        .await
        .unwrap();
        let mut readers = Vec::new();
        let mut delivered = 0;
        for (rows, detail) in reports {
            delivered += detail["delivered"].as_u64().unwrap();
            for position in detail["positions"].as_array().unwrap() {
                assert_eq!(position[1], 5);
            }
            readers.push(json!({"lanes":rows}));
        }
        assert_eq!(delivered, 20);
        for copy in 0..2 {
            let rows = results::reader_copy(&readers, copy).unwrap();
            assert_eq!(
                results::totals(&rows, config.writers).unwrap(),
                [3, 3, 2, 2]
            );
            results::verify_digests(&writers, &rows, config.writers).unwrap();
        }
        for links in links {
            links.shutdown().await.unwrap();
        }
    }
    writing.shutdown().await.unwrap();
    broker.shutdown().await.unwrap();
}

async fn check_readers(
    runtime: &WriterRuntime,
    config: &Config,
    setup: &Value,
    group: ozzy_proto::GroupId,
    window: super::super::Window,
    writers: &[Value],
) {
    use super::super::super::reader::native;
    let (tasks, links) = native::connect(runtime, config, setup, 0).await.unwrap();
    assert_eq!(tasks.len(), 2);
    // Both logical readers share one PEER and one SUB socket.
    assert_eq!(links.socket_count(), 2);
    let (_finish, finished) = tokio::sync::watch::channel(Some(vec![1, 1, 0, 0]));
    let reports = try_join_all(
        tasks
            .into_iter()
            .map(|task| native::run_task(task, config, group, window, finished.clone())),
    )
    .await
    .unwrap();
    for (rows, detail) in reports {
        assert_eq!(rows.len(), 4);
        assert_eq!(detail["positions"], json!([[0, 2]]));
        assert_eq!(detail["live_records"], 0);
        assert_eq!(detail["replayed_records"], 2);
        for (reader, writer) in rows.iter().zip(writers) {
            assert_eq!(reader["partition"], 0);
            assert_eq!(reader["total"], writer["total"]);
            assert_eq!(reader["digest_xxh3_128"], writer["digest_xxh3_128"]);
        }
    }
    links.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn shared_writer_rejects_unsupported_linger_before_connecting() {
    let mut config = config(128);
    config.args.writer_linger_us = 1;
    let runtime = WriterRuntime::new().unwrap();
    let cause = connect_shared(&runtime, &config, &Value::Null, 0..4)
        .await
        .err()
        .expect("linger is not supported by the shared SDK yet");
    assert!(cause.to_string().contains("--writer-linger-us 0"));
}
