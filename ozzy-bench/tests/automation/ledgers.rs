//! Result ledgers, chart cohorts and cached baseline reuse.

use super::*;

#[test]
fn ledgers_append_and_exclude_failed_interrupted_runs() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("ozzy.jsonl");
    let first = saved("ozzy", "first");
    save(temp.path(), &first, true);
    let original = fs::read(&path).unwrap();
    for id in ["failed", "interrupted"] {
        save(temp.path(), &saved("ozzy", id), id == "failed");
    }
    records::append(
        temp.path(),
        "ozzy",
        &json!({"kind":"run-failed","run_id":"failed"}),
    )
    .unwrap();
    save(temp.path(), &saved("iggy", "baseline"), true);
    assert!(fs::read(&path).unwrap().starts_with(&original));
    assert_eq!(records::completed(&path).unwrap(), vec![first]);
}

#[test]
fn charts_select_supported_codecs_without_rewriting_history() {
    let temp = tempfile::tempdir().unwrap();
    for codec in ["raw", "lz4", "retired"] {
        let mut row = saved("ozzy", "mixed");
        row["case"]["codec"] = json!(codec);
        save(temp.path(), &row, false);
    }
    records::append(
        temp.path(),
        "ozzy",
        &json!({"kind":"run-complete","run_id":"mixed"}),
    )
    .unwrap();
    let path = temp.path().join("ozzy.jsonl");
    let before = fs::read(&path).unwrap();
    let data = records::select(temp.path(), &["mixed".into()], None).unwrap();
    let rows = data["summary"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row["case"]["codec"] == "raw"));
    assert!(rows.iter().any(|row| row["case"]["codec"] == "lz4"));
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn baseline_reuse_requires_matching_workload_host_and_configuration() {
    let temp = tempfile::tempdir().unwrap();
    save(temp.path(), &saved("ozzy", "new"), true);
    save(temp.path(), &saved("iggy", "old"), true);
    assert_eq!(
        records::select(temp.path(), &["new".into()], Some("old")).unwrap()["summary"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    for (field, value) in [
        ("workload_sha256", json!("different")),
        ("environment", json!({"cpu":"other"})),
        ("configuration", json!({"duration":20})),
    ] {
        let mut bad = saved("iggy", field);
        bad[field] = value;
        save(temp.path(), &bad, true);
        assert!(
            records::select(temp.path(), &["new".into()], Some(field))
                .unwrap_err()
                .to_string()
                .contains("incompatible")
        );
    }
}

#[test]
fn fresh_comparisons_keep_each_implementations_batch_ceiling() {
    let temp = tempfile::tempdir().unwrap();
    let mut native = saved("ozzy", "native");
    native["configuration"]["request_records"] = json!(2048);
    let mut iggy = saved("iggy", "iggy");
    iggy["configuration"]["request_records"] = json!(1024);
    save(temp.path(), &native, true);
    save(temp.path(), &iggy, true);
    let selected = records::select(temp.path(), &["native".into(), "iggy".into()], None).unwrap();
    assert_eq!(selected["batch_records"], json!({"ozzy":2048,"iggy":1024}));
    assert!(selected["references"].is_null());
    let mut changed = native.clone();
    changed["run_id"] = json!("changed");
    changed["case"]["codec"] = json!("lz4");
    changed["configuration"]["request_records"] = json!(1024);
    save(temp.path(), &changed, true);
    assert!(
        records::select(temp.path(), &["native".into(), "changed".into()], None)
            .unwrap_err()
            .to_string()
            .contains("batch ceilings")
    );
}

#[test]
fn chart_selection_keeps_effective_worker_payload_caps_per_mode() {
    let temp = tempfile::tempdir().unwrap();
    let mut row = saved("ozzy", "native");
    row["configuration"]["native_batch_target_bytes"] = json!(4 * 1024 * 1024);
    for (mode, size, bytes) in [
        ("durable", 128, 256 * 1024),
        ("durable", 8192, 832 * 1024),
        ("disk-quorum", 128, 128 * 1024),
    ] {
        row["case"]["mode"] = json!(mode);
        row["case"]["size"] = json!(size);
        row["raw"]["writer_batch_payload_bytes_max"] = json!(bytes);
        save(temp.path(), &row, true);
    }
    let selected = records::select(temp.path(), &["native".into()], None).unwrap();
    assert_eq!(
        selected["writer_payload_caps"],
        json!({
            "durable":832 * 1024,"disk-quorum":128 * 1024,
        })
    );
}

#[test]
fn cached_reference_keeps_its_controls_and_requires_matching_measurement_conditions() {
    let temp = tempfile::tempdir().unwrap();
    let mut native = saved("ozzy", "new");
    native["configuration"]["request_records"] = json!(8192);
    native["configuration"]["payload_compression_threshold"] = json!(1024);
    native["configuration"]["writer_inflight_appends"] = json!(1);
    native["configuration"]["native_batch_target_bytes"] = json!(2 * 1024 * 1024);
    native["configuration"]["disk_aio_depth"] = json!(8);
    native["configuration"]["disk_io_backend"] = json!("aio");
    native["configuration"]["disk_direct_io"] = json!(true);
    native["configuration"]["live_readers"] = json!(true);
    let mut reference = saved("iggy", "old");
    reference["configuration"]["request_records"] = json!(1024);
    reference["configuration"]["payload_compression_threshold"] = json!(4096);
    reference["configuration"]["writer_inflight_appends"] = json!(3);
    reference["configuration"]["native_batch_target_bytes"] = json!(4 * 1024 * 1024);
    reference["workload_sha256"] = json!("older-workload");
    save(temp.path(), &native, true);
    save(temp.path(), &reference, true);
    assert!(records::select(temp.path(), &["new".into()], Some("old")).is_err());
    let data = records::select_with_iggy_reference(temp.path(), &["new".into()], "old").unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 2);
    assert_eq!(data["batch_records"]["ozzy"], 8192);
    assert_eq!(data["batch_records"]["iggy"], 1024);
    assert_eq!(data["references"]["iggy"]["identical_workload"], false);
    for (field, value) in [
        ("disk_io_backend", json!("aio")),
        ("disk_direct_io", json!(true)),
        ("live_readers", json!(true)),
    ] {
        assert_eq!(data["compatibility"]["configuration"][field], value);
        assert!(
            data["references"]["iggy"]["compatibility"]["configuration"]
                .get(field)
                .is_none()
        );
        let mut mismatched = native.clone();
        mismatched["run_id"] = json!(field);
        mismatched["configuration"][field] = Value::Null;
        save(temp.path(), &mismatched, true);
        assert!(records::select(temp.path(), &["new".into(), field.into()], None).is_err());
    }
    assert_eq!(data["compatibility"]["configuration"]["disk_aio_depth"], 8);
    assert_eq!(
        data["compatibility"]["configuration"]["writer_inflight_appends"],
        1
    );
    assert_eq!(
        data["references"]["iggy"]["compatibility"]["configuration"]["writer_inflight_appends"],
        3
    );
    assert_eq!(
        data["compatibility"]["configuration"]["native_batch_target_bytes"],
        2 * 1024 * 1024
    );
    assert_eq!(
        data["references"]["iggy"]["compatibility"]["configuration"]["native_batch_target_bytes"],
        4 * 1024 * 1024
    );
    let mut different_native = native.clone();
    different_native["run_id"] = json!("different-native-target");
    different_native["configuration"]["native_batch_target_bytes"] = json!(4 * 1024 * 1024);
    save(temp.path(), &different_native, true);
    assert!(
        records::select(
            temp.path(),
            &["new".into(), "different-native-target".into()],
            None
        )
        .is_err()
    );
    for (field, value) in [("duration", json!(20)), ("warmup", json!(17))] {
        let mut bad = reference.clone();
        bad["run_id"] = json!(field);
        bad["configuration"][field] = value;
        save(temp.path(), &bad, true);
        assert!(records::select_with_iggy_reference(temp.path(), &["new".into()], field).is_err());
    }
}

#[test]
fn external_references_keep_both_series_and_current_native_provenance() {
    let temp = tempfile::tempdir().unwrap();
    let mut native = saved("ozzy", "new");
    native["environment"]["dependencies"] = json!(["new OMQ"]);
    native["configuration"]["broker_cpu_sets"] = json!([[0, 1]]);
    native["configuration"]["ramp"] = Value::Null;
    save(temp.path(), &native, true);
    for implementation in ["ozzy", "iggy", "redpanda"] {
        let mut reference = saved(implementation, "old");
        reference["workload_sha256"] = json!("older-workload");
        reference["environment"]["dependencies"] = json!(["old OMQ"]);
        save(temp.path(), &reference, true);
    }
    let data =
        records::select_with_external_reference(temp.path(), &["new".into()], "old").unwrap();
    assert_eq!(data["summary"].as_array().unwrap().len(), 3);
    assert_eq!(data["references"]["iggy"]["run_id"], "old");
    assert_eq!(data["references"]["redpanda"]["run_id"], "old");
    assert_eq!(
        data["compatibility"]["environment"]["dependencies"],
        json!(["new OMQ"])
    );
    assert_eq!(
        data["references"]["redpanda"]["compatibility"]["environment"]["dependencies"],
        json!(["old OMQ"])
    );
    let mut wrong_host = saved("iggy", "wrong-host");
    wrong_host["environment"]["host"] = json!("another machine");
    save(temp.path(), &wrong_host, true);
    assert!(
        records::select_with_external_reference(temp.path(), &["new".into()], "wrong-host")
            .is_err()
    );
    let missing = saved("iggy", "iggy-only");
    save(temp.path(), &missing, true);
    assert!(
        records::select_with_external_reference(temp.path(), &["new".into()], "iggy-only").is_err()
    );
}

#[test]
fn chart_protocol_uses_observed_traffic_without_rewriting_old_results() {
    let temp = tempfile::tempdir().unwrap();
    let mut row = saved("ozzy", "local-push");
    row["raw"] = json!({"writer_protocol":"peer-appends","writers":[{"lanes":[{
        "protocol_requests":{"data_messages":100,"requests":100,
            "max_records":1,"max_inflight_appends":0}
    }]}]});
    save(temp.path(), &row, true);
    let path = temp.path().join("ozzy.jsonl");
    let before = fs::read(&path).unwrap();
    let selected = records::select(temp.path(), &["local-push".into()], None).unwrap();
    assert_eq!(selected["writer_protocols"]["buffered"], "push-records");
    assert_eq!(fs::read(&path).unwrap(), before);

    row["run_id"] = json!("mixed");
    row["raw"]["writers"][0]["lanes"][0]["protocol_requests"]["requests"] = json!(101);
    save(temp.path(), &row, true);
    assert!(records::select(temp.path(), &["mixed".into()], None).is_err());
}

#[test]
fn chart_selection_preserves_push_append_transport() {
    let temp = tempfile::tempdir().unwrap();
    let mut row = saved("ozzy", "push-appends");
    row["raw"] = json!({"writer_protocol":"push-appends"});
    save(temp.path(), &row, true);
    let selected = records::select(temp.path(), &["push-appends".into()], None).unwrap();
    assert_eq!(selected["writer_protocols"]["buffered"], "push-appends");
}

#[test]
fn dsync_is_default_and_cannot_mix_with_old_sync_chart_cohorts() {
    let args = compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "durable"]);
    args.validate(VALIDATION_CPUS).unwrap();
    assert_eq!(
        args.configuration().unwrap()["durable_segment_io"],
        "odsync"
    );
    assert!(compare::Args::try_parse_from(["compare", "--direct-io"]).is_err());
    assert!(compare::Args::try_parse_from(["compare", "--write-padding", "512"]).is_err());
    for case in args.cases() {
        assert!(
            !args
                .command(Path::new("bench"), &case, None)
                .unwrap()
                .iter()
                .any(|arg| arg == "--sync-writes")
        );
    }
    assert!(compare::Args::try_parse_from(["compare", "--sync-writes"]).is_err());

    let temp = tempfile::tempdir().unwrap();
    for (id, mode) in [("explicit", "fdatasync"), ("sync", "odsync")] {
        let mut row = saved("ozzy", id);
        row["configuration"]["durable_segment_io"] = json!(mode);
        save(temp.path(), &row, true);
    }
    assert!(
        records::select(temp.path(), &["explicit".into(), "sync".into()], None)
            .unwrap_err()
            .to_string()
            .contains("incompatible")
    );
}

#[test]
fn tiny_record_queues_keep_full_range_while_append_groups_cap_at_two_k() {
    for records in [8192, 16_384, 65_536] {
        let args = compare::Args::parse_from([
            "compare",
            "--impl",
            "ozzy",
            "--modes",
            "disk-quorum",
            "--sizes",
            "16",
            "--request-records",
            &records.to_string(),
        ]);
        args.validate(VALIDATION_CPUS).unwrap();
        let command = args
            .command(Path::new("bench"), &args.cases()[0], None)
            .unwrap();
        assert!(
            command
                .windows(2)
                .any(|pair| pair == ["--request-records", &records.to_string()])
        );
        assert!(
            command
                .windows(2)
                .any(|pair| pair == ["--writer-batch-records", "2048"])
        );
        assert!(
            command
                .windows(2)
                .any(|pair| pair == ["--writer-inflight-appends", "1"])
        );
        assert!(!command.iter().any(|arg| arg == "--writer-linger-us"));
        assert!(command.iter().any(|arg| arg == "--binary-payload"));
    }
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--sizes",
        "16",
        "--request-records",
        "65537",
    ]);
    assert!(args.validate(VALIDATION_CPUS).is_err());
}

#[test]
fn charts_reject_incomplete_duplicate_and_unaudited_data() {
    let temp = tempfile::tempdir().unwrap();
    for state in [
        "failed",
        "interrupted",
        "missing-repetition",
        "unaudited",
        "resident",
    ] {
        let mut row = saved("ozzy", state);
        match state {
            "missing-repetition" => row["configuration"]["repetitions"] = json!(2),
            "unaudited" => row["process_isolation"] = Value::Null,
            "resident" => {
                row["process_isolation"]["after_shutdown"]["resident_processes"] = json!([123]);
            }
            _ => (),
        }
        save(temp.path(), &row, state != "interrupted");
        if state == "failed" {
            records::append(
                temp.path(),
                "ozzy",
                &json!({"kind":"run-failed","run_id":state}),
            )
            .unwrap();
        }
        assert!(
            records::select(temp.path(), &[state.into()], None).is_err(),
            "{state}"
        );
    }
    save(temp.path(), &saved("ozzy", "first"), true);
    assert!(records::select(temp.path(), &["first".into(), "first".into()], None).is_err());
    let mut second = saved("ozzy", "second");
    second["case"]["mode"] = json!("durable");
    save(temp.path(), &second, true);
    assert!(
        records::select(temp.path(), &["first".into(), "second".into()], None)
            .unwrap_err()
            .to_string()
            .contains("source revisions")
    );
}
