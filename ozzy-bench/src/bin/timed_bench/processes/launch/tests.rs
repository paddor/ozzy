use super::*;

#[test]
fn only_external_sdk_information_and_warnings_are_nonfatal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("worker.stderr");
    for (target, line) in [
        (
            "librdkafka",
            "2026-09-17T19:16:42.895537Z  INFO librdkafka: GETPID: acquired\n",
        ),
        (
            "iggy(?:::[A-Za-z0-9_]+)*",
            "2026-09-17T19:16:42.895537Z  INFO iggy::tcp::client: connected\n",
        ),
    ] {
        let allowed = external_log(target).unwrap();
        for severity in ["TRACE", "DEBUG", "INFO", "WARN"] {
            let information = line.replace("INFO", severity);
            std::fs::write(&path, &information).unwrap();
            assert!(fatal_stderr(&path, None).unwrap());
            assert!(
                !fatal_stderr(&path, Some(&allowed)).unwrap(),
                "{information}"
            );
            assert_eq!(std::fs::read_to_string(&path).unwrap(), information);
        }
        for diagnostic in [
            line.replace("INFO", "ERROR"),
            line.replace(" librdkafka:", " ozzy_runtime:")
                .replace(" iggy::tcp::client:", " ozzy_runtime:"),
            line.replace(" librdkafka:", " omq:")
                .replace(" iggy::tcp::client:", " omq:"),
            line.replace(" librdkafka:", " ozzy_bench: librdkafka:")
                .replace(" iggy::tcp::client:", " ozzy_bench: iggy:"),
            format!("{line}unstructured failure\n"),
            line.replace("INFO", "WARN")
                .replace(" librdkafka:", " ozzy_runtime:")
                .replace(" iggy::tcp::client:", " ozzy_runtime:"),
        ] {
            std::fs::write(&path, &diagnostic).unwrap();
            assert!(fatal_stderr(&path, Some(&allowed)).unwrap(), "{diagnostic}");
        }
    }
}

#[test]
fn timed_reader_and_writer_bounds_reach_child_processes() {
    use clap::Parser;
    let args = Args::parse_from([
        "bench",
        "--processes",
        "--network-ingress",
        "--duration",
        "5",
        "--json-payload",
        "--writer-inflight-appends",
        "3",
        "--reader-records",
        "2048",
        "--reader-payload-mib",
        "2",
        "--reader-workers",
        "12",
        "--readers-per-partition",
        "3",
        "--live-readers",
    ]);
    let mut arguments = vec![
        "bench".into(),
        "--processes".into(),
        "--network-ingress".into(),
    ];
    Process::execution_arguments(&args, &mut arguments);
    let child = Args::parse_from(arguments);
    assert_eq!(child.reader_workers(), 12);
    assert_eq!(child.readers_per_partition, 3);
    assert!(child.live_readers);
    assert_eq!(child.writer_inflight_appends, 3);
    assert_eq!(child.reader_records, 2048);
    assert_eq!(child.reader_payload_mib, 2);
    assert!(child.json_payload);
    assert!(!child.random_payload);
    let mut binary = args;
    binary.json_payload = false;
    binary.binary_payload = true;
    binary.record_bytes = 16;
    let mut arguments = vec![
        "bench".into(),
        "--processes".into(),
        "--network-ingress".into(),
    ];
    Process::execution_arguments(&binary, &mut arguments);
    let child = Args::parse_from(arguments);
    assert!(child.binary_payload);
    assert!(!child.json_payload);
}

#[test]
fn production_deployment_options_reach_child_processes() {
    use clap::Parser;
    let args = Args::parse_from([
        "bench",
        "--processes",
        "--network-ingress",
        "--duration",
        "1",
        "--partitions",
        "5",
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
    ]);
    let mut arguments = vec![
        "bench".into(),
        "--processes".into(),
        "--network-ingress".into(),
    ];
    Process::execution_arguments(&args, &mut arguments);
    let child = Args::parse_from(arguments);
    assert_eq!(child.native, args.native);
}

#[cfg(feature = "comparisons")]
#[test]
fn external_policy_reaches_workers_without_ozzy_mode_argument() {
    use clap::Parser;
    let args = Args::parse_from([
        "bench",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "1",
        "--external-system",
        "redpanda",
        "--external-policy",
        "buffered",
        "--external-endpoint",
        "127.0.0.1:19092",
    ]);
    let mut arguments = vec![
        "bench".into(),
        "--processes".into(),
        "--network-ingress".into(),
    ];
    Process::execution_arguments(&args, &mut arguments);
    assert!(!arguments.iter().any(|arg| arg == "--system"));
    let child = Args::parse_from(arguments);
    assert_eq!(child.external_system, args.external_system);
    assert_eq!(child.external_policy, args.external_policy);
    assert_eq!(child.external_endpoint, args.external_endpoint);
}

async fn child(directory: &Path, name: &str, script: &str) -> Process {
    let mut command = Command::new("sh");
    command.args(["-c", script]);
    Process::spawn_command(
        command,
        directory.join(name),
        false,
        Connection::listen(None).await.unwrap().0,
    )
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn warnings_and_silent_exits_abort_supervision_and_reap_siblings() {
    for script in ["exit 7", "echo worker-warning >&2; exec sleep 30"] {
        let directory = tempfile::tempdir().unwrap();
        let failed = child(directory.path(), "failed.stderr", script).await;
        let sibling = child(directory.path(), "sibling.stderr", "exec sleep 30").await;
        let pids = [failed.id(), sibling.id()];
        let monitors = [failed.monitor(), sibling.monitor()];
        tokio::time::timeout(Duration::from_secs(2), watch_diagnostics(&monitors))
            .await
            .unwrap();
        drop((failed, sibling));
        for pid in pids {
            assert!(!Path::new(&format!("/proc/{pid}")).exists());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_reaps_a_worker_waiting_for_control() {
    let directory = tempfile::tempdir().unwrap();
    let worker = child(directory.path(), "worker.stderr", "exec sleep 30").await;
    let pid = worker.id();
    let task = tokio::spawn(async move {
        let mut worker = worker;
        worker.receive("never").await
    });
    tokio::task::yield_now().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!Path::new(&format!("/proc/{pid}")).exists());
}

#[test]
fn broker_io_threads_reach_brokers_and_leave_clients_unchanged() {
    use clap::Parser;
    let args = Args::parse_from([
        "bench",
        "--processes",
        "--network-ingress",
        "--duration",
        "5",
        "--broker-io-threads",
        "2",
    ]);
    let placement = &Placement::load(None).unwrap()[0];
    let group = GroupId::from_bytes([1; 16]);
    for (role, threads) in [("--worker-index", "2"), ("--reader-worker", "1")] {
        let command = Process::command(&args, 0, group, placement, role).unwrap();
        let arguments: Vec<_> = command
            .get_args()
            .map(|arg| arg.to_str().unwrap().to_owned())
            .collect();
        let offset = arguments
            .iter()
            .position(|arg| arg == "--io-threads")
            .unwrap();
        assert_eq!(arguments[offset + 1], threads);
        assert!(!arguments.iter().any(|arg| arg == "--deployment-directory"));
    }
}

#[test]
fn obsolete_writer_and_broker_modes_are_rejected() {
    use clap::Parser;
    for flag in ["--writer-batching", "--broker-omq-on-shard"] {
        assert!(Args::try_parse_from(["bench", flag]).is_err());
    }
}
