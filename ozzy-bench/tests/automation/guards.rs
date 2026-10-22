//! Process isolation guards and worker supervision.

use super::*;

#[test]
fn dictionary_worker_has_its_own_isolation_class() {
    let worker = json!({"pid":123,"started":1,"group":99,
        "executable":"ozzy_lz4_dict_bench","arguments":["--worker"]});
    isolation::Guard::new("lz4-dictionary", None)
        .check_processes(99, vec![worker.clone()])
        .unwrap();
    assert!(
        isolation::Guard::new("ozzy", None)
            .check_processes(99, vec![worker.clone()])
            .is_err()
    );
    assert!(
        isolation::Guard::new("lz4-dictionary", None)
            .check_processes(100, vec![worker])
            .is_err()
    );
}

fn process(name: &str, pid: u32, group: u32, arguments: Value) -> Value {
    let mut row = json!({"pid":pid,"executable":name,"state":"S","group":group,"started":1});
    row["arguments"] = arguments;
    row
}

#[test]
fn guard_rejects_overlap_compilers_and_wrong_process_groups() {
    let server = process("iggy-server", 100, 100, json!([]));
    let client = process(
        "ozzy_timed_bench",
        201,
        200,
        json!([
            "bench",
            "--producer-worker",
            "0",
            "--external-system",
            "iggy"
        ]),
    );
    let mut guard = isolation::Guard::new("iggy", Some(100));
    guard
        .check_processes(200, vec![server.clone(), client.clone()])
        .unwrap();
    assert_eq!(guard.report().unwrap()["observations"], 1);
    let mut broker = client;
    broker["arguments"] = json!(["bench", "--worker-index", "0"]);
    assert!(
        guard
            .check_processes(200, vec![server.clone(), broker])
            .is_err()
    );
    assert!(guard.check_processes(200, vec![]).is_err());
    for name in [
        "iggy-server",
        "java",
        "redpanda",
        "rustc",
        "cargo",
        "perf",
        "ozzy_timed_bench",
    ] {
        assert!(
            isolation::Guard::new("ozzy", None)
                .check_processes(200, vec![process(name, 100, 100, json!([]))])
                .is_err(),
            "{name}"
        );
    }
    let mut profiled = isolation::Guard::new("ozzy", None)
        .with_profiling(true)
        .unwrap();
    profiled
        .check_processes(200, vec![process("perf", 201, 200, json!([]))])
        .unwrap();
    assert!(
        profiled
            .check_processes(200, vec![process("perf", 201, 201, json!([]))])
            .is_err()
    );
    for (implementation, name) in [
        ("iggy", "iggy-server"),
        ("kafka", "java"),
        ("redpanda", "redpanda"),
    ] {
        isolation::Guard::new(implementation, Some(100))
            .check_processes(200, vec![process(name, 100, 100, json!([]))])
            .unwrap();
    }
}

#[test]
fn stopped_broker_is_still_discovered() {
    let temp = tempfile::tempdir().unwrap();
    let exe = temp.path().join("iggy-server");
    fs::copy("/bin/sleep", &exe).unwrap();
    // Concurrent process tests can briefly inherit the copy's writable FD
    // between fork and exec. Linux returns ETXTBSY until that FD closes.
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let mut child = loop {
        // A multicall coreutils binary picks its program from argv[0]; the
        // audit reads the name from the executable path.
        let mut command = Command::new(&exe);
        std::os::unix::process::CommandExt::arg0(&mut command, "sleep");
        match command.arg("30").spawn() {
            Ok(child) => break child,
            Err(error)
                if error.raw_os_error() == Some(26) && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("start stopped-broker fixture: {error}"),
        }
    };
    let pid = child.id();
    assert!(
        Command::new("kill")
            .args(["-STOP", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
    let result = || {
        for _ in 0..100 {
            if let Some(row) = isolation::processes()
                .unwrap()
                .into_iter()
                .find(|p| p["pid"] == pid && p["state"] == "T")
            {
                assert!(
                    isolation::Guard::new("ozzy", None)
                        .check_processes(200, vec![row])
                        .is_err()
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("stopped server missing from process audit");
    };
    let result = std::panic::catch_unwind(result);
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn supervision_preserves_success_and_aborts_diagnostics_or_hangs() {
    let temp = tempfile::tempdir().unwrap();
    let command = vec![
        "/bin/sh".into(),
        "-c".into(),
        "printf '{\"valid\":true}\\n'".into(),
    ];
    assert_eq!(
        supervise::execute(
            &command,
            temp.path(),
            Duration::from_secs(2),
            &BTreeMap::new(),
            supervise::Monitors::default(),
        )
        .await
        .unwrap()["valid"],
        true
    );
    assert!(
        fs::read_to_string(temp.path().join("stdout"))
            .unwrap()
            .contains("valid")
    );
    for script in [
        "printf 'WARNING: broken\\n'; sleep 30",
        "printf 'ERROR: broken\\n'; sleep 30",
        "printf 'diagnostic' >&2; sleep 30",
        "exit 7",
        "sleep 30",
    ] {
        let cmd = vec!["/bin/sh".into(), "-c".into(), script.into()];
        let start = std::time::Instant::now();
        assert!(
            supervise::execute(
                &cmd,
                temp.path(),
                Duration::from_millis(200),
                &BTreeMap::new(),
                supervise::Monitors::default(),
            )
            .await
            .is_err()
        );
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}

#[test]
fn cluster_guard_requires_exactly_all_three_external_brokers() {
    let brokers = [100, 101, 102].map(|pid| process("iggy-server", pid, pid, json!([])));
    let mut guard = isolation::Guard::new("iggy", None).with_servers(&[100, 101, 102]);
    guard.check_processes(200, brokers.to_vec()).unwrap();
    assert!(guard.check_processes(200, brokers[..2].to_vec()).is_err());
    let mut extra = brokers.to_vec();
    extra.push(process("iggy-server", 103, 103, json!([])));
    assert!(guard.check_processes(200, extra).is_err());
    let mut mixed = brokers.to_vec();
    mixed.push(process(
        "ozzy_timed_bench",
        201,
        200,
        json!(["--worker-index", "0"]),
    ));
    assert!(guard.check_processes(200, mixed).is_err());
}
