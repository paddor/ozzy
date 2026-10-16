//! Comparison and workload case commands: adapters, storage limits, offered load and native controls.

use super::*;

#[test]
fn comparison_commands_declare_the_same_topic_partitions_for_every_adapter() {
    let compare = compare::Args::parse_from([
        "compare",
        "--impl",
        "all",
        "--sizes",
        "128",
        "--modes",
        "durable",
        "--partitions",
        "2",
    ]);
    compare.validate(VALIDATION_CPUS).unwrap();
    for case in compare.cases() {
        let command = compare
            .command(Path::new("bench"), &case, Some("127.0.0.1:19092"))
            .unwrap();
        assert_eq!(option(&command, "--partitions"), "2", "{}", case["impl"]);
        assert_eq!(option(&command, "--window"), "2", "{}", case["impl"]);
        assert_eq!(
            option(&command, "--producer-workers"),
            "1",
            "{}",
            case["impl"]
        );
    }
    let workloads = workloads::Args::parse_from([
        "workloads",
        "--dry-run",
        "--profiles",
        "single-durable,iggy-durable,redpanda-durable",
        "--connections",
        "2",
        "--workers",
        "1,2",
    ]);
    workloads.validate().unwrap();
    for case in workloads.cases() {
        let command = workloads
            .command(&case, Path::new("/var/tmp/ozzy-bench"))
            .unwrap();
        assert_eq!(option(&command, "--partitions"), "2", "{}", case["profile"]);
    }
}

#[test]
fn storage_group_records_is_not_a_live_override() {
    let args = compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "durable"]);
    args.validate(VALIDATION_CPUS).unwrap();
    let command = args
        .command(Path::new("bench"), &args.cases()[0], None)
        .unwrap();
    assert!(!command.iter().any(|arg| arg == "--storage-group-records"));
    assert!(compare::Args::try_parse_from(["compare", "--storage-group-records", "1024"]).is_err());
}

#[test]
fn storage_target_overrides_are_rejected() {
    for flag in ["--operation-target-kib", "--write-group-target-kib"] {
        assert!(
            compare::Args::try_parse_from(["compare", flag, "64"]).is_err(),
            "{flag}"
        );
    }
}

#[test]
fn append_window_override_is_native_only_and_nonzero() {
    let args = compare::Args::parse_from([
        "compare",
        "--writer-inflight-appends",
        "3",
        "--shard-resident-mib",
        "1024",
        "--writer-batch-target-kib",
        "4096",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration().unwrap()["writer_inflight_appends"], 3);
    assert_eq!(args.configuration().unwrap()["shard_resident_mib"], 1024);
    assert_eq!(
        args.configuration().unwrap()["native_batch_target_bytes"],
        4 * 1024 * 1024
    );
    for case in args.cases() {
        let command = args
            .command(Path::new("bench"), &case, Some("localhost:1234"))
            .unwrap();
        if case["impl"] == "ozzy" {
            assert_eq!(option(&command, "--writer-inflight-appends"), "3");
            assert_eq!(option(&command, "--shard-resident-mib"), "1024");
            assert_eq!(option(&command, "--writer-batch-target-kib"), "4096");
        } else {
            assert!(!command.iter().any(|arg| arg == "--writer-inflight-appends"));
            assert!(!command.iter().any(|arg| arg == "--shard-resident-mib"));
            assert!(!command.iter().any(|arg| arg == "--writer-batch-target-kib"));
        }
    }
    let mut invalid = args;
    invalid.writer_inflight_appends = 0;
    assert!(invalid.validate(VALIDATION_CPUS).is_err());
    assert!(compare::Args::try_parse_from(["compare", "--writer-batch-target-kib", "0"]).is_err());
    assert!(
        compare::Args::try_parse_from(["compare", "--writer-batch-target-kib", "16385"]).is_err()
    );
}

#[test]
fn offered_load_reaches_every_selected_implementation_without_native_queue_overrides() {
    for implementation in ["all", "ozzy", "iggy", "redpanda"] {
        let args = compare::Args::parse_from([
            "compare",
            "--impl",
            implementation,
            "--records-per-second",
            "1000",
        ]);
        args.validate(VALIDATION_CPUS).unwrap();
        for case in args.cases() {
            assert_eq!(case["rate"], 1000);
            let command = args
                .command(Path::new("bench"), &case, Some("localhost:1234"))
                .unwrap();
            assert_eq!(option(&command, "--records-per-second"), "1000");
            if case["impl"] == "iggy" {
                assert!(!command.iter().any(|a| a == "--writer-inflight-appends"));
            }
        }
        let mut bad = args;
        bad.records_per_second = Some(0);
        assert!(bad.validate(VALIDATION_CPUS).is_err());
    }
}

#[test]
fn mixed_runs_apply_one_request_ceiling_to_every_implementation() {
    for requested in [512, 2048, 8192] {
        let args = compare::Args::parse_from([
            "compare",
            "--request-records",
            &requested.to_string(),
            "--modes",
            "disk-quorum",
        ]);
        args.validate(VALIDATION_CPUS).unwrap();
        for case in args.cases() {
            let expected = requested;
            let command = args
                .command(Path::new("worker"), &case, Some("localhost:1234"))
                .unwrap();
            assert_eq!(option(&command, "--request-records"), expected.to_string());
            assert_eq!(
                option(&command, "--writer-batch-records"),
                expected
                    .min(ozzy_runtime::replicated::MAX_APPEND_RECORDS as u64)
                    .to_string()
            );
            assert_eq!(
                args.case_configuration(&case).unwrap()["request_records"],
                expected
            );
            assert!(!command.iter().any(|arg| arg == "--payload-lz4"));
            assert_eq!(
                args.case_configuration(&case).unwrap()["payload_compression"],
                if case["impl"] == "ozzy" {
                    "adaptive-lz4"
                } else {
                    "none"
                }
            );
        }
    }
}

#[test]
fn redpanda_runs_local_and_group_cases_through_its_own_external_adapter() {
    let mut args = compare::Args::parse_from(["compare", "--impl", "redpanda"]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.cases().len(), 6);
    args.modes = vec!["disk-quorum".into()];
    args.validate(VALIDATION_CPUS).unwrap();
    for case in args.cases() {
        assert_eq!(case["impl"], "redpanda");
        let command = args
            .command(Path::new("bench"), &case, Some("127.0.0.1:9092"))
            .unwrap();
        assert_eq!(option(&command, "--external-system"), "redpanda");
        assert_eq!(
            option(&command, "--external-policy"),
            case["mode"].as_str().unwrap()
        );
        assert!(!command.iter().any(|arg| arg == "--system"));
        assert!(!command.iter().any(|arg| arg == "--disk-workers"));
    }
    args.implementation = "all".into();
    assert!(args.cases().iter().any(|case| case["impl"] == "redpanda"));
}

#[test]
fn redpanda_results_require_the_requested_confirmation_boundary() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "redpanda",
        "--sizes",
        "1024",
        "--modes",
        "buffered",
        "--partitions",
        "4",
    ]);
    let case = &args.cases()[0];
    let mut row = external_row();
    row["system"] = json!("redpanda-buffered");
    row["write_caching"] = json!(true);
    row["reader_records_max"] = json!(1);
    row["reader_payload_bytes_max"] = json!(1024);
    row["records_per_append"] = json!(1);
    row["client_sdk"] = json!({"acks":"all","unconfirmed_records_per_writer":1024});
    validation::comparison(case, &row, &args.configuration().unwrap()).unwrap();
    for (pointer, value) in [
        ("/write_caching", json!(false)),
        ("/client_sdk/acks", json!("1")),
        ("/client_sdk/unconfirmed_records_per_writer", json!(2048)),
    ] {
        let mut bad = row.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(validation::comparison(case, &bad, &args.configuration().unwrap()).is_err());
    }
}

#[test]
fn native_controls_never_leak_into_external_or_cluster_commands() {
    let args = workloads::Args::parse_from([
        "workloads",
        "--dry-run",
        "--profiles",
        "single-durable,disk-quorum,iggy-buffered",
        "--workers",
        "1",
        "--writer-batch-records",
        "32",
        "--writer-inflight-appends",
        "3",
    ]);
    args.validate().unwrap();
    for case in args.cases() {
        let cmd = args
            .command(&case, Path::new("/var/tmp/ozzy-bench"))
            .unwrap();
        assert_eq!(option(&cmd, "--request-records"), "1024");
        assert_eq!(option(&cmd, "--reader-workers"), "4");
        if case["profile"] == "single-durable" {
            assert_eq!(option(&cmd, "--writer-batch-records"), "32");
        }
        assert!(!cmd.iter().any(|v| matches!(
            v.as_str(),
            "--history-mib"
                | "--history-operations"
                | "--disk-workers"
                | "--disk-owner-threads"
                | "--read-depth"
                | "--storage-group-records"
                | "--packing-block-kib"
                | "--storage-group-kib"
                | "--writer-linger-us"
                | "--io-threads"
        )));
        if case["profile"] == "iggy-buffered" {
            assert_eq!(option(&cmd, "--external-policy"), "buffered");
            assert!(!cmd.iter().any(|v| v == "--system"));
            assert!(!cmd.iter().any(|v| v == "--writer-inflight-appends"
                || v == "--writer-batch-records"
                || v == "--app-threads"));
        } else {
            assert_eq!(option(&cmd, "--writer-inflight-appends"), "3");
        }
    }
    for (flag, value) in [
        ("--app-threads", "5"),
        ("--reader-workers", "5"),
        ("--writer-batch-records", "65537"),
        ("--writer-inflight-appends", "0"),
    ] {
        let bad = workloads::Args::parse_from(["workloads", flag, value]);
        assert!(bad.validate().is_err(), "{flag}");
    }
    assert!(
        workloads::Args::parse_from(["workloads", "--profiles", "single-durable,iggy-buffered"])
            .validate()
            .is_err()
    );
}

#[test]
fn workloads_only_offer_live_native_profiles_and_controls() {
    let args = workloads::Args::parse_from(["workloads"]);
    args.validate().unwrap();
    assert_eq!(args.profiles, ["single-durable", "disk-quorum"]);
    for profile in ["single-volatile", "single-buffered"] {
        let args = workloads::Args::parse_from(["workloads", "--profiles", profile]);
        assert!(args.validate().is_err(), "{profile}");
    }
    for flag in [
        "--history-mib",
        "--history-operations",
        "--disk-workers",
        "--disk-owner-threads",
        "--read-depth",
        "--storage-group-records",
        "--packing-block-kib",
        "--storage-group-kib",
        "--writer-linger-us",
        "--io-threads",
    ] {
        assert!(
            workloads::Args::try_parse_from(["workloads", flag, "1"]).is_err(),
            "{flag}"
        );
    }
}

#[test]
fn iggy_cluster_cases_use_explicit_external_confirmation_modes() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "iggy",
        "--modes",
        "disk-quorum",
        "--sizes",
        "1024",
    ]);
    assert_eq!(args.cases().len(), 1);
    for case in args.cases() {
        let command = args
            .command(std::path::Path::new("bench"), &case, Some("127.0.0.1:8090"))
            .unwrap();
        assert!(
            command
                .windows(2)
                .any(|w| w == ["--external-system", "iggy"])
        );
        assert!(
            command
                .windows(2)
                .any(|w| w[0] == "--external-policy" && w[1] == case["mode"])
        );
    }
}

#[test]
fn adaptive_payload_compression_is_one_series_and_rejects_mixed_settings() {
    let args = compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "disk-quorum"]);
    let config = args.configuration().unwrap();
    assert_eq!(config["payload_compression"], "adaptive-lz4");
    assert_eq!(
        config["payload_compression_threshold"],
        ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD
    );
    assert_eq!(args.cases().len(), 3);
    assert!(args.cases().iter().all(|case| case["codec"] == "raw"));
    let temp = tempfile::tempdir().unwrap();
    let mut row = saved("ozzy", "encoded");
    row["configuration"]["payload_compression"] = json!("adaptive-lz4");
    row["configuration"]["payload_compression_threshold"] =
        json!(ozzy_runtime::replicated::PAYLOAD_COMPRESSION_THRESHOLD);
    save(temp.path(), &row, true);
    let data = records::select(temp.path(), &["encoded".into()], None).unwrap();
    assert_eq!(data["summary"][0]["case"]["codec"], "raw");
    assert_eq!(data["payload_compression"], "adaptive-lz4");
    let stored = records::completed(&temp.path().join("ozzy.jsonl")).unwrap();
    assert_eq!(stored[0]["case"]["codec"], "raw");
    let mut plain = row.clone();
    plain["run_id"] = json!("plain");
    plain["case"]["codec"] = json!("lz4");
    plain["configuration"]["payload_compression"] = json!("none");
    save(temp.path(), &plain, true);
    let error =
        records::select(temp.path(), &["encoded".into(), "plain".into()], None).unwrap_err();
    assert!(error.to_string().contains("payload compression settings"));
    let mut threshold = row.clone();
    threshold["run_id"] = json!("different-threshold");
    threshold["case"]["codec"] = json!("lz4");
    threshold["configuration"]["payload_compression_threshold"] = json!(16384);
    save(temp.path(), &threshold, true);
    let error = records::select(
        temp.path(),
        &["encoded".into(), "different-threshold".into()],
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("payload compression settings"));
}

#[test]
fn default_segment_limits_follow_record_size_and_explicit_limits_override() {
    let mut args =
        compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "disk-quorum"]);
    args.validate(VALIDATION_CPUS).unwrap();
    for case in args.cases() {
        let expected = if case["size"].as_u64().unwrap() >= 1024 {
            "1024"
        } else {
            "256"
        };
        let command = args.command(Path::new("bench"), &case, None).unwrap();
        assert_eq!(option(&command, "--segment-mib"), expected);
        assert!(!command.iter().any(|arg| arg == "--segment-decoded-mib"));
    }
    assert!(args.configuration().unwrap()["segment_mib"].is_null());

    args.segment_mib = Some(64);
    args.validate(VALIDATION_CPUS).unwrap();
    for case in args.cases() {
        let command = args.command(Path::new("bench"), &case, None).unwrap();
        assert_eq!(option(&command, "--segment-mib"), "64");
        assert!(!command.iter().any(|arg| arg == "--segment-decoded-mib"));
    }
    for invalid in [3, 1025] {
        args.segment_mib = Some(invalid);
        assert!(args.validate(VALIDATION_CPUS).is_err());
    }
    for flag in [
        "--segment-decoded-mib",
        "--compression-workers",
        "--zero-ahead",
        "--lz4-transport",
    ] {
        assert!(
            compare::Args::try_parse_from(["compare", flag, "128"]).is_err(),
            "{flag}"
        );
    }
}

#[test]
fn obsolete_broker_omq_placement_flag_is_rejected() {
    assert!(compare::Args::try_parse_from(["compare", "--broker-omq-on-shard"]).is_err());
}

#[test]
fn several_readers_per_partition_run_as_separate_ozzy_reader_processes() {
    let mut args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "disk-quorum",
        "--partitions",
        "2",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert!(
        args.configuration()
            .unwrap()
            .get("readers_per_partition")
            .is_none()
    );
    let case = args.cases().remove(0);
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert_eq!(option(&command, "--reader-workers"), "2");
    assert_eq!(option(&command, "--readers-per-partition"), "1");

    args.readers_per_partition = 16;
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration().unwrap()["readers_per_partition"], 16);
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert_eq!(option(&command, "--reader-workers"), "32");
    assert_eq!(option(&command, "--readers-per-partition"), "16");

    assert!(!command.iter().any(|arg| arg == "--broker-io-threads"));
    assert!(
        args.configuration()
            .unwrap()
            .get("broker_io_threads")
            .is_none()
    );
    args.broker_io_threads = Some(2);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration().unwrap()["broker_io_threads"], 2);
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert_eq!(option(&command, "--broker-io-threads"), "2");
    args.broker_io_threads = Some(33);
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.broker_io_threads = None;

    assert!(!command.iter().any(|arg| arg == "--live-readers"));
    assert!(args.configuration().unwrap().get("live_readers").is_none());
    args.live_readers = true;
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(args.configuration().unwrap()["live_readers"], true);
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert!(command.iter().any(|arg| arg == "--live-readers"));
    args.modes = vec!["buffered".into()];
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.modes = vec!["disk-quorum".into()];
    args.live_readers = false;

    args.readers_per_partition = 17;
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.readers_per_partition = 2;
    args.implementation = "all".into();
    assert!(args.validate(VALIDATION_CPUS).is_err());
}
