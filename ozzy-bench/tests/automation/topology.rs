//! CPU placement and broker topology of local and distributed comparisons.

use super::*;

#[test]
fn native_cases_apply_broker_masks_and_keep_clients_in_their_own_pool() {
    let temp = tempfile::tempdir().unwrap();
    let args = compare::Args::parse_from(["compare", "--impl", "ozzy", "--sizes", "128"]);
    for case in args.cases() {
        let command = args
            .case_command(Path::new("bench"), &case, temp.path(), None)
            .unwrap();
        assert_eq!(option(&command, "-c"), "3,4,5");
        let placements = ozzy_bench::placement::Placement::load(Some(Path::new(option(
            &command,
            "--placements",
        ))))
        .unwrap();
        let count = if case["mode"] == "durable" { 1 } else { 3 };
        let expected = args.broker_cpus.brokers(count);
        for (index, placement) in placements.iter().take(count).enumerate() {
            assert_eq!(placement.cpus.as_ref().unwrap(), &expected[index]);
            assert_eq!(
                placement.storage_dir.as_deref(),
                Some(artifact_root().join("ozzy-bench").as_path())
            );
        }
        let brokers: Vec<_> = expected
            .iter()
            .map(|cpus| {
                let execution = json!({"threads":[{"cpus":cpus}]});
                json!({"identity":{"execution":execution}, "usage":{"execution":execution}})
            })
            .collect();
        let mut row = json!({"brokers":brokers});
        validation::production_cpu_placement(&row, &args.configuration().unwrap()).unwrap();
        row["brokers"][0]["identity"]["execution"]["threads"][0]["cpus"] =
            json!([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        assert!(
            validation::production_cpu_placement(&row, &args.configuration().unwrap()).is_err()
        );
        row["brokers"][0]["identity"]["execution"]["threads"] = json!([]);
        assert!(
            validation::production_cpu_placement(&row, &args.configuration().unwrap()).is_err()
        );
    }
}

#[test]
fn all_and_native_only_preserve_workload_and_ssd_cpu_budgets() {
    let all = compare::Args::parse_from(["compare"]);
    let native = compare::Args::parse_from(["compare", "--impl", "ozzy"]);
    assert!(native.cases().iter().all(|case| case["mode"] != "buffered"));
    assert!(
        all.cases()
            .iter()
            .any(|case| case["impl"] != "ozzy" && case["mode"] == "durable")
    );
    let buffered = compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "buffered"]);
    assert_eq!(buffered.cases().len(), 0);
    assert!(buffered.validate(VALIDATION_CPUS).is_err());
    assert!(
        buffered
            .command(
                Path::new("bench"),
                &json!({"impl":"ozzy","mode":"buffered"}),
                None
            )
            .is_err()
    );
    assert_eq!(
        native.cases(),
        all.cases()
            .into_iter()
            .filter(|c| c["impl"] == "ozzy")
            .collect::<Vec<_>>()
    );
    assert_eq!(native.sizes, [128, 1024, 8192]);
    assert_eq!(native.partitions, 8);
    assert_eq!(native.repetitions, 1);
    assert_eq!((native.warmup, native.duration), (5.0, 10.0));
    assert_eq!(native.patterns, ["events"]);
    assert_eq!(native.configuration().unwrap()["codecs"], json!(["raw"]));
    assert!(compare::Args::try_parse_from(["compare", "--codecs", "raw"]).is_err());
    for case in all.cases() {
        let cmd = all
            .command(Path::new("bench"), &case, Some("localhost:1234"))
            .unwrap();
        assert_eq!(
            cmd.iter().any(|v| v == "--binary-payload"),
            case["size"] == 16
        );
        assert_eq!(
            cmd.iter().any(|v| v == "--json-payload"),
            case["size"] != 16
        );
        let batch = all.request_records.to_string();
        assert_eq!(option(&cmd, "--request-records"), batch);
        assert_eq!(option(&cmd, "--writer-batch-records"), batch);
        assert_eq!(option(&cmd, "--reader-records"), "16384");
        assert_eq!(option(&cmd, "--reader-payload-mib"), "128");
        assert_eq!(option(&cmd, "--partitions"), "8");
        assert_eq!(option(&cmd, "--window"), "8");
        assert_eq!(option(&cmd, "--producer-workers"), "4");
        assert_eq!(option(&cmd, "--reader-workers"), "8");
        assert_eq!(
            option(&cmd, "--storage-dir"),
            artifact_root().join("ozzy-bench").to_str().unwrap()
        );
        if case["impl"] == "ozzy" {
            assert_eq!(option(&cmd, "--writer-inflight-appends"), "1");
            assert_eq!(option(&cmd, "-c"), "3,4,5");
            for flag in [
                "--disk-owner-threads",
                "--disk-workers",
                "--history-mib",
                "--history-operations",
                "--segment-decoded-mib",
                "--codec",
                "--writer-linger-us",
            ] {
                assert!(!cmd.iter().any(|arg| arg == flag), "{flag}");
            }
        }
    }
    let cluster =
        compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "disk-quorum"]);
    for case in cluster.cases() {
        let cmd = cluster.command(Path::new("bench"), &case, None).unwrap();
        assert!(!cmd.iter().any(|arg| arg == "--history-operations"));
    }
}

#[test]
fn saved_local_placements_do_not_waive_remote_audits() {
    let temp = tempfile::tempdir().unwrap();
    for (id, remote, label, accepted) in [
        ("local", false, "all-local", true),
        ("unlabeled", false, "", false),
        ("remote", true, "all-local", false),
    ] {
        let mut row = saved("ozzy", id);
        row["configuration"]["deployment"] = json!([
            {"bind":"127.0.0.1"}, {"bind":"127.0.0.1"}, {"bind":"127.0.0.1"}
        ]);
        row["configuration"]["deployment_kind"] = json!(label);
        if remote {
            row["configuration"]["deployment"][2]["ssh"] = json!("remote");
        }
        save(temp.path(), &row, true);
        assert_eq!(
            records::select(temp.path(), &[id.into()], None).is_ok(),
            accepted
        );
    }
}

#[test]
fn all_local_comparison_checks_every_broker_cpu_and_labels_topology() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("placements.json");
    let mut rows = json!([
        {"bind":"127.0.0.1","storage_dir":"/var/tmp","cpus":[0]},
        {"bind":"127.0.0.1","storage_dir":"/mnt/bench/tmp","cpus":[1]},
        {"bind":"127.0.0.1","storage_dir":"/var/tmp","cpus":[2]}
    ]);
    fs::write(&path, rows.to_string()).unwrap();
    let mut args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "disk-quorum",
        "--broker-cpus",
        "0,1,2",
        "--client-cpus",
        "3,4,5",
        "--placements",
        path.to_str().unwrap(),
        "--control-bind",
        "tcp://127.0.0.1:0",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert!(args.validate(&[0, 1, 2, 3]).is_err());
    assert_eq!(
        args.configuration().unwrap()["deployment_kind"],
        "all-local"
    );
    let isolated = args.configuration().unwrap();
    for row in rows.as_array_mut().unwrap() {
        row["cpus"] = json!([0, 1, 2]);
    }
    fs::write(&path, rows.to_string()).unwrap();
    args.validate(VALIDATION_CPUS).unwrap();
    assert_ne!(
        args.configuration().unwrap()["deployment"],
        isolated["deployment"]
    );
    for bad in [json!([0, 1]), json!([1, 2, 3]), json!([0, 1, 2, 6])] {
        rows[2]["cpus"] = bad;
        fs::write(&path, rows.to_string()).unwrap();
        assert!(args.validate(VALIDATION_CPUS).is_err());
    }
    for (i, row) in rows.as_array_mut().unwrap().iter_mut().enumerate() {
        row["cpus"] = json!([i]);
    }
    for bad in [0, 1, 3, 4, 5] {
        rows[2]["cpus"] = json!([bad]);
        fs::write(&path, rows.to_string()).unwrap();
        assert!(args.validate(VALIDATION_CPUS).is_err());
    }
    if std::thread::available_parallelism().is_ok_and(|cores| cores.get() >= 9) {
        // Check sibling CPU masks only when the VM exposes those CPU IDs.
        let broker_cpus = std::mem::replace(
            &mut args.broker_cpus,
            ozzy_bench::automation::cpus::BrokerCpus::parse("0,1,2,6,7,8").unwrap(),
        );
        for (i, row) in rows.as_array_mut().unwrap().iter_mut().enumerate() {
            row["cpus"] = json!([i, i + 6]);
        }
        fs::write(&path, rows.to_string()).unwrap();
        args.validate(VALIDATION_CPUS).unwrap();
        for bad in [json!([2, 7]), json!([2, 9]), json!([])] {
            rows[2]["cpus"] = bad;
            fs::write(&path, rows.to_string()).unwrap();
            assert!(args.validate(VALIDATION_CPUS).is_err());
        }
        args.implementation = "iggy".into();
        rows[2]["cpus"] = json!([2, 8]);
        fs::write(&path, rows.to_string()).unwrap();
        assert!(args.validate(VALIDATION_CPUS).is_err());
        args.implementation = "ozzy".into();
        args.broker_cpus = broker_cpus;
    }
    for (i, row) in rows.as_array_mut().unwrap().iter_mut().enumerate() {
        row["cpus"] = json!([i]);
    }
    rows[2]["cpus"] = json!([2]);
    fs::write(&path, rows.to_string()).unwrap();
    for implementation in ["ozzy", "iggy", "all"] {
        args.implementation = implementation.into();
        args.modes = vec!["disk-quorum".into()];
        args.validate(VALIDATION_CPUS).unwrap();
    }
    args.modes = vec!["durable".into()];
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.modes = vec!["disk-quorum".into()];
    args.implementation = "redpanda".into();
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.implementation = "ozzy".into();
    args.control_bind = None;
    assert!(args.validate(VALIDATION_CPUS).is_err());
}

#[test]
fn distributed_comparison_keeps_adapter_windows_and_freezes_topology() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("placements.json");
    let rows = json!([
        {"bind":"192.0.2.1","storage_dir":"/var/tmp","cpus":[0]},
        {"bind":"192.0.2.1","storage_dir":"/mnt/bench/tmp","cpus":[1]},
        {"bind":"192.0.2.2","ssh":"remote","executable":"/mnt/bench/tmp/bin/ozzy_timed_bench","storage_dir":"/mnt/bench/tmp","cpus":[0]}
    ]);
    fs::write(&path, rows.to_string()).unwrap();
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "all",
        "--modes",
        "disk-quorum",
        "--broker-cpus",
        "0,1,2",
        "--client-cpus",
        "3,4,5",
        "--placements",
        path.to_str().unwrap(),
        "--control-bind",
        "tcp://192.0.2.1:0",
        "--request-records",
        "2048",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration().unwrap()["deployment"], rows);
    for case in args.cases() {
        let command = args
            .command(
                Path::new("/bin/ozzy_timed_bench"),
                &case,
                Some("192.0.2.1:9000"),
            )
            .unwrap();
        assert_eq!(
            command.iter().any(|v| v == "--placements"),
            case["impl"] == "ozzy"
        );
        assert_eq!(
            args.case_configuration(&case).unwrap()["request_records"],
            2048
        );
    }
    let target = directory.path().join("frozen");
    fs::create_dir(&target).unwrap();
    let frozen = ozzy_bench::automation::distributed::freeze(&path, &target).unwrap();
    fs::write(&path, "[]").unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(frozen).unwrap()).unwrap(),
        rows
    );
    assert!(args.validate(VALIDATION_CPUS).is_err());
}
