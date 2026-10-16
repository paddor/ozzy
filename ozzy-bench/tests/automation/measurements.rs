//! Measurement validation of comparison rows and run summaries.

use super::*;

#[test]
fn comparison_rejects_legacy_storage_switches() {
    let args = compare::Args::parse_from(["compare", "--impl", "ozzy", "--modes", "disk-quorum"]);
    args.validate(VALIDATION_CPUS).unwrap();
    let command = args
        .command(Path::new("bench"), &args.cases()[0], None)
        .unwrap();
    let flag = "--resident-read-mib";
    assert!(compare::Args::try_parse_from(["compare", flag, "64"]).is_err());
    assert!(!command.iter().any(|arg| arg == flag));
    assert!(
        args.configuration()
            .unwrap()
            .get("native_resident_read_bytes")
            .is_none()
    );
}

#[test]
fn compressed_transports_are_forwarded_recorded_and_refused_inproc() {
    let mut args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "durable",
        "--sizes",
        "1024",
        "--broker-cpus",
        "0,1,2",
        "--client-cpus",
        "3,4,5",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    let case = args.cases()[0].clone();
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert!(!command.iter().any(|arg| arg == "--transport"));
    assert!(args.configuration().unwrap().get("transport").is_none());
    for (flag, scheme) in [("lz4", "lz4+tcp"), ("zstd", "zstd+tcp")] {
        args.transport = flag.into();
        args.validate(VALIDATION_CPUS).unwrap();
        let command = args.command(Path::new("bench"), &case, None).unwrap();
        assert_eq!(option(&command, "--transport"), flag);
        assert_eq!(args.configuration().unwrap()["transport"], scheme);
    }
    args.memory_storage = true;
    args.inproc = true;
    assert!(args.validate(VALIDATION_CPUS).is_err());
    assert!(compare::Args::try_parse_from(["compare", "--transport", "gzip"]).is_err());
}

#[test]
fn shard_count_is_independent_of_cpu_count() {
    let mut args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "durable",
        "--sizes",
        "1024",
        "--shards",
        "4",
        "--broker-cpus",
        "0,1,2",
        "--client-cpus",
        "3,4,5",
        "--partitions",
        "4",
    ]);
    args.validate(VALIDATION_CPUS).unwrap();
    let case = args.cases()[0].clone();
    let command = args.command(Path::new("bench"), &case, None).unwrap();
    assert_eq!(option(&command, "--app-threads"), "4");
    assert!(
        !command
            .iter()
            .any(|arg| arg == "--disk-owner-threads" || arg == "--disk-workers")
    );
    let config = args.configuration().unwrap();
    assert_eq!(config["native_shards"], 4);
    for shards in [0, 5] {
        args.shards = Some(shards);
        assert!(args.validate(VALIDATION_CPUS).is_err());
    }
    args.shards = Some(4);
    args.implementation = "all".into();
    assert!(args.validate(VALIDATION_CPUS).is_err());
    args.implementation = "ozzy".into();
    args.modes = vec!["disk-quorum".into()];
    assert!(args.validate(VALIDATION_CPUS).is_err());
}

#[test]
fn comparison_rejects_warmup_window_cohort_and_count_drift() {
    let args = compare::Args::parse_from(["compare", "--partitions", "4"]);
    let case = external_case(1024, "json");
    let valid = external_row();
    validation::comparison(&case, &valid, &args.configuration().unwrap()).unwrap();
    for (pointer, value) in [
        ("/record_corpus", json!("structured-v1")),
        ("/record_bytes", json!(16)),
        ("/records_per_append", json!(1)),
        ("/reader_records_max", json!(1)),
        ("/reader_payload_bytes_max", json!(1)),
        ("/warmup_seconds", json!(0)),
        ("/measurement_seconds", json!(20)),
        ("/total_verified_records", json!(99)),
        ("/producer_records_per_second", json!(0)),
        ("/consumer_records_per_second", Value::Null),
        ("/producer_ack/samples", json!(0)),
        ("/reader_delivery/samples", json!(0)),
    ] {
        let mut bad = valid.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(
            validation::comparison(&case, &bad, &args.configuration().unwrap()).is_err(),
            "{pointer}"
        );
    }
    for key in ["profiled", "allocation_counted"] {
        let mut bad = valid.clone();
        bad[key] = json!(true);
        assert!(validation::comparison(&case, &bad, &args.configuration().unwrap()).is_err());
    }
}

#[test]
fn native_comparison_accepts_only_production_deployment_rows() {
    let args = compare::Args::parse_from(["compare", "--partitions", "4"]);
    let case = json!({"impl":"ozzy","mode":"durable","size":1024,"pattern":"json"});
    let mut row = external_row();
    row["system"] = json!("single-durable");
    row["native_client_protocol"] = json!(true);
    let error = validation::comparison(&case, &row, &args.configuration().unwrap()).unwrap_err();
    assert!(
        error.to_string().contains("production deployment"),
        "{error}"
    );
}

#[test]
fn event_corpus_requires_binary_at_16_bytes_and_json_at_larger_sizes() {
    let args = compare::Args::parse_from(["compare", "--partitions", "4"]);
    for size in [16, 128, 1024, 8192] {
        let case = external_case(size, "events");
        let mut row = external_row();
        row["record_bytes"] = json!(size);
        let records = 16384;
        row["reader_records_max"] = json!(records);
        row["reader_payload_bytes_max"] = json!(records * size);
        if size == 16 {
            assert!(validation::comparison(&case, &row, &args.configuration().unwrap()).is_err());
            row["record_corpus"] = json!(ozzy_bench::workload::BINARY_EVENT_CORPUS);
        }
        validation::comparison(&case, &row, &args.configuration().unwrap()).unwrap();
        row["record_corpus"] = json!(if size == 16 {
            ozzy_bench::workload::JSON_EVENT_CORPUS
        } else {
            ozzy_bench::workload::BINARY_EVENT_CORPUS
        });
        assert!(validation::comparison(&case, &row, &args.configuration().unwrap()).is_err());
    }
}

#[test]
fn fixed_load_summaries_preserve_scheduled_latency_and_reject_missing_arrivals() {
    let args = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--sizes",
        "1024",
        "--records-per-second",
        "1000",
        "--partitions",
        "4",
    ]);
    let mut iggy_case = args.cases()[0].clone();
    iggy_case["impl"] = json!("iggy");
    iggy_case["mode"] = json!("buffered");
    let case = &iggy_case;
    let mut row = external_row();
    row["reader_payload_bytes_max"] = json!(16384 * 1024);
    row["total_confirmed_records"] = json!(15000);
    row["total_verified_records"] = json!(15000);
    row["submitted_records_per_second"] = json!(1000);
    row["scheduled_load"] = json!({
        "records_per_second":1000,"planned_total_records":15000,"planned_measurement_records":10000,
        "producer_ack":{"samples":10000,"p50_us":11,"p99_us":22},
        "reader_delivery":{"samples":10000,"p50_us":33,"p99_us":44},
        "scheduling_lag":{"samples":10000,"p50_us":5,"p99_us":7}
    });
    let metrics = validation::comparison(case, &row, &args.configuration().unwrap()).unwrap();
    assert_eq!(metrics["scheduled_ack_p99_us"], 22.0);
    assert_eq!(metrics["scheduled_delivery_p50_us"], 33.0);
    assert_eq!(metrics["scheduling_lag_p99_us"], 7.0);
    assert_eq!(metrics["scheduled_samples"], 10000);
    assert_eq!(metrics["ack_p99_us"], 2.0);
    let mut with_p999 = row.clone();
    for field in ["producer_ack", "reader_delivery"] {
        with_p999[field]["p999_us"] = json!(100);
        with_p999[field]["max_us"] = json!(200);
    }
    for field in ["producer_ack", "reader_delivery", "scheduling_lag"] {
        with_p999["scheduled_load"][field]["p999_us"] = json!(100);
        with_p999["scheduled_load"][field]["max_us"] = json!(200);
    }
    let captured =
        validation::comparison(case, &with_p999, &args.configuration().unwrap()).unwrap();
    for key in [
        "ack_p999_us",
        "delivery_p999_us",
        "scheduled_ack_p999_us",
        "scheduled_delivery_p999_us",
        "scheduling_lag_p999_us",
    ] {
        assert_eq!(captured[key], 100.0);
    }
    with_p999["scheduled_load"]["producer_ack"]["p999_us"] = json!(1);
    assert!(validation::comparison(case, &with_p999, &args.configuration().unwrap()).is_err());
    for (pointer, value) in [
        ("/scheduled_load/records_per_second", json!(100)),
        ("/scheduled_load/planned_measurement_records", json!(9999)),
        ("/scheduled_load/planned_total_records", json!(14999)),
        ("/scheduled_load/producer_ack/samples", json!(9999)),
        ("/scheduled_load/reader_delivery/samples", json!(9999)),
        ("/scheduled_load/scheduling_lag/samples", json!(9999)),
        ("/scheduled_load/scheduling_lag/p99_us", Value::Null),
        ("/scheduled_load/producer_ack/p99_us", json!(-1)),
    ] {
        let mut bad = row.clone();
        *bad.pointer_mut(pointer).unwrap() = value.clone();
        assert!(
            validation::comparison(case, &bad, &args.configuration().unwrap()).is_err(),
            "{pointer}"
        );
    }
    let saturation = compare::Args::parse_from(["compare", "--impl", "ozzy", "--sizes", "1024"]);
    let mut saturated = iggy_case.clone();
    saturated["rate"] = Value::Null;
    assert!(
        validation::comparison(&saturated, &row, &saturation.configuration().unwrap()).is_err()
    );
}

#[test]
fn workload_verifies_shards_and_explicit_controls() {
    let case = json!({"profile":"single-durable","partitions":4,"app_threads":4,
        "writer_batch_records":1024,"segment_mib":64});
    let mut valid = external_row();
    valid["system"] = json!("single-durable");
    valid["native_client_protocol"] = json!(true);
    valid["broker_application_shards"] = json!(4);
    valid["writer_batch_records_max"] = json!(1024);
    valid["segment_bytes"] = json!(64 * 1024 * 1024);
    validation::measurements(&case, &valid).unwrap();
    let mut observed = valid.clone();
    observed["logical_stream_count"] = json!(1);
    observed["effective_topic_topology"] = json!({
        "logical_stream_count":1, "topic_count":1,
        "partitions_per_topic":4, "partition_count":4,
    });
    validation::measurements(&case, &observed).unwrap();
    for field in [
        "logical_stream_count",
        "topic_count",
        "partitions_per_topic",
        "partition_count",
    ] {
        let mut mismatched = observed.clone();
        mismatched["effective_topic_topology"][field] = json!(16);
        assert!(
            validation::measurements(&case, &mismatched).is_err(),
            "{field}"
        );
    }
    for (field, value) in [
        ("topic_count", json!(4)),
        ("partition_count", json!(1)),
        ("partitions_per_topic", json!(1)),
        ("broker_application_shards", json!(3)),
        ("writer_batch_records_max", json!(512)),
        ("segment_bytes", json!(1024 * 1024)),
        ("native_client_protocol", json!(false)),
    ] {
        let mut bad = valid.clone();
        bad[field] = value;
        assert!(validation::measurements(&case, &bad).is_err(), "{field}");
    }
    let mut zero = case;
    zero["app_threads"] = json!(0);
    assert!(validation::measurements(&zero, &valid).is_err());
}

#[test]
fn summaries_preserve_overload_in_either_repetition_order() {
    let healthy = json!({"case":{"rate":100_000},"measurements":{"latency":12}});
    let overloaded = json!({
        "case":{"rate":100_000},
        "failure":"scheduled backlog exceeded",
        "measurements":{}
    });
    let other = json!({"case":{"rate":50_000},"measurements":{"latency":4}});
    for rows in [
        vec![healthy.clone(), overloaded.clone(), other.clone()],
        vec![overloaded.clone(), healthy.clone(), other.clone()],
    ] {
        let summary = records::summarize(&rows).unwrap();
        let summary = summary.as_array().unwrap();
        let failed = summary
            .iter()
            .find(|row| row["case"]["rate"] == 100_000)
            .unwrap();
        assert_eq!(failed["failure"], "scheduled backlog exceeded");
        assert_eq!(failed["repetitions"], 2);
        assert_eq!(failed["measurements"], json!({}));
        let measured = summary
            .iter()
            .find(|row| row["case"]["rate"] == 50_000)
            .unwrap();
        assert_eq!(measured["measurements"]["latency"]["median"], 4.0);
    }
}
