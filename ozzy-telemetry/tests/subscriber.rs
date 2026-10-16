use omq_tokio::{Context, Options, SocketType};
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn configured_endpoint_without_a_subscriber_does_not_block_workers() {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "worker_publishes_on_ipc_endpoint", "--nocapture"])
        .env("OZZY_TELEMETRY_TEST_CHILD", "1")
        .env(
            "OZZY_TELEMETRY_ENDPOINT",
            format!("ipc://@ozzy-telemetry-absent-{}", std::process::id()),
        )
        .env("OMQ_IO_THREADS", "4")
        .env_remove("OZZY_BENCH_PROFILE")
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if status.is_err() {
        child.kill().unwrap();
        child.wait().unwrap();
    }
    assert!(status.unwrap().success());
}
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test(flavor = "current_thread")]
async fn standalone_endpoint_publishes_worker_telemetry_and_logs() {
    let endpoint = format!("ipc://@ozzy-telemetry-test-{}", std::process::id());
    let context = Context::new();
    let socket = context.socket(SocketType::Sub, Options::default().recv_hwm(32));
    socket.subscribe(Vec::<u8>::new()).await.unwrap();
    socket.bind(endpoint.parse().unwrap()).await.unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "worker_publishes_on_ipc_endpoint", "--nocapture"])
        .env("OZZY_TELEMETRY_TEST_CHILD", "1")
        .env("OZZY_TELEMETRY_ENDPOINT", endpoint)
        .env("OMQ_IO_THREADS", "4")
        .env_remove("OZZY_BENCH_PROFILE")
        .spawn()
        .unwrap();
    let messages = tokio::time::timeout(Duration::from_secs(5), async {
        let mut rows = BTreeMap::new();
        loop {
            let message = socket.recv().await.unwrap();
            let row: serde_json::Value =
                serde_json::from_slice(message.part_slice(0).unwrap()).unwrap();
            if row["events"]["io_completed"].as_u64().unwrap_or(0) > 0
                && row["logs"].as_array().is_some_and(|logs| !logs.is_empty())
            {
                rows.insert(row["thread_name"].as_str().unwrap().to_owned(), row);
                if rows.len() == 2 {
                    break rows;
                }
            }
        }
    })
    .await;
    if messages.is_err() {
        child.kill().unwrap();
    }
    let status = child.wait().unwrap();
    let rows = messages.unwrap();
    assert!(status.success());
    for name in ["telemetry-test-shard", "telemetry-test-disk"] {
        let row = &rows[name];
        assert!(
            row["stages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|stage| stage["stage"] == "io_execution")
        );
        assert_eq!(row["logs"][0]["message"], "worker ready");
    }
}

#[test]
fn worker_publishes_on_ipc_endpoint() {
    if std::env::var_os("OZZY_TELEMETRY_TEST_CHILD").is_none() {
        return;
    }
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let workers = ["telemetry-test-shard", "telemetry-test-disk"].map(|name| {
        let barrier = barrier.clone();
        std::thread::Builder::new()
            .name(name.into())
            .spawn(move || {
                assert!(ozzy_telemetry::enabled());
                ozzy_telemetry::event(ozzy_telemetry::Event::IoStarted);
                barrier.wait();
                #[cfg(target_os = "linux")]
                assert_eq!(telemetry_threads(), 1);
                for _ in 0..40 {
                    let started = ozzy_telemetry::start();
                    std::thread::sleep(Duration::from_millis(20));
                    ozzy_telemetry::finish(ozzy_telemetry::Stage::IoExecution, started);
                    ozzy_telemetry::event(ozzy_telemetry::Event::IoCompleted);
                    ozzy_telemetry::log(ozzy_telemetry::Level::Info, "worker ready");
                }
            })
            .unwrap()
    });
    for worker in workers {
        worker.join().unwrap();
    }
}

#[cfg(target_os = "linux")]
fn telemetry_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path().join("comm")).unwrap())
        .filter(|name| name.starts_with("ozzy/tele/"))
        .count()
}
