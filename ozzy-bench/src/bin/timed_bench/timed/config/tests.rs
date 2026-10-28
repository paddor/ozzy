use super::*;
use clap::Parser;

#[cfg(feature = "comparisons")]
#[test]
fn comparison_native_commands_use_production_settings() {
    use ozzy_bench::automation::compare;

    for mode in ["durable", "disk-quorum"] {
        let options = compare::Args::parse_from([
            "compare", "--impl", "ozzy", "--modes", mode, "--sizes", "128",
        ]);
        let case = &options.cases()[0];
        let command = options
            .command(std::path::Path::new("bench"), case, None)
            .unwrap();
        let args = Args::try_parse_from(command.into_iter().skip(3)).unwrap();
        Config::production(args).unwrap();
    }
}

#[cfg(feature = "comparisons")]
#[test]
fn comparison_delay_cases_drain_past_the_delay() {
    use ozzy_bench::automation::compare;

    let options = compare::Args::parse_from([
        "compare",
        "--impl",
        "ozzy",
        "--modes",
        "durable",
        "--sizes",
        "128",
        "--records-per-second",
        "1000",
        "--delay-millis",
        "60000",
    ]);
    let case = &options.cases()[0];
    let command = options
        .command(std::path::Path::new("bench"), case, None)
        .unwrap();
    let args = Args::try_parse_from(command.into_iter().skip(3)).unwrap();
    assert_eq!(args.drain_timeout_secs, 70);
    assert_eq!(Config::production(args).unwrap().delay_ns(), 60_000_000_000);
    let validate = |implementation| {
        compare::Args::parse_from(["compare", "--impl", implementation, "--delay-millis", "1"])
            .validate(&[0, 1, 2, 3, 4, 5])
    };
    validate("ozzy").unwrap();
    let error = validate("iggy").unwrap_err();
    assert!(error.to_string().contains("--delay-millis"), "{error}");
}

#[cfg(feature = "comparisons")]
#[test]
fn native_and_external_partitions_are_independent_of_writer_count() {
    let args = Args::parse_from([
        "bench",
        "--system",
        "single-durable",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "1",
        "--window",
        "4",
        "--history-mib",
        "512",
        "--partitions",
        "2",
    ]);
    let native = Config::production(args.clone()).unwrap();
    assert_eq!(
        (native.writers, native.partitions(), native.reader_slots()),
        (4, 2, 2)
    );
    for system in [
        super::super::external::System::Iggy,
        super::super::external::System::Redpanda,
    ] {
        let mut args = args.clone();
        args.external_system = Some(system);
        args.external_policy = Some(super::super::external::Policy::Durable);
        args.external_endpoint = Some("127.0.0.1:18090".into());
        let config = Config::new(args).unwrap();
        assert_eq!(
            (config.writers, config.partitions(), config.reader_slots()),
            (4, 2, 2)
        );
        assert_eq!(config.args.reader_workers(), 2);
        let mut default = config.args.clone();
        default.native.partitions = None;
        assert_eq!(Config::new(default).unwrap().partitions(), 16);
    }
}

#[test]
fn binary_layout_requires_16_bytes_and_json_requires_room_for_its_schema() {
    for (size, flag, valid) in [
        ("16", "--binary-payload", true),
        ("128", "--json-payload", true),
        ("16", "--json-payload", false),
        ("128", "--binary-payload", false),
        ("15", "--binary-payload", false),
    ] {
        let args = Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "10",
            "--record-bytes",
            size,
            flag,
        ]);
        assert_eq!(Config::new(args).is_ok(), valid, "{size} {flag}");
    }
}

#[test]
fn memory_reservations_separate_disk_history_from_resident_records() {
    let config = |system, history, decoded, segment| {
        Config::new(Args::parse_from([
            "bench",
            "--network-ingress",
            "--processes",
            "--streaming",
            "--duration",
            "3",
            "--system",
            system,
            "--history-mib",
            history,
            "--segment-decoded-mib",
            decoded,
            "--segment-mib",
            segment,
        ]))
        .unwrap()
    };
    for system in ["single-durable", "disk-quorum"] {
        let small = config(system, "8192", "64", "64");
        let large = config(system, "32768", "64", "64");
        assert_eq!(
            small.memory_reservation_bytes(),
            large.memory_reservation_bytes()
        );
        assert!(
            config(system, "32768", "128", "64").memory_reservation_bytes()
                > large.memory_reservation_bytes()
        );
        assert_eq!(
            large.memory_reservation_bytes(),
            config(system, "32768", "64", "1024").memory_reservation_bytes()
        );
    }
    #[cfg(feature = "comparisons")]
    {
        let mut external = config("single-durable", "32768", "64", "64");
        external.args.external_system = Some(super::super::external::System::Iggy);
        external.args.external_policy = Some(super::super::external::Policy::Durable);
        let before = external.memory_reservation_bytes();
        external.args.segment_decoded_mib = 1024;
        assert_eq!(external.memory_reservation_bytes(), before);
    }
}

#[test]
fn packed_record_reservation_covers_sparse_batches_and_larger_segments() {
    for bytes in [16, 32, 64, 128] {
        let mut args = Args::parse_from([
            "bench",
            "--system",
            "disk-quorum",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
            "--segment-mib",
            "256",
            "--segment-decoded-mib",
            "256",
            "--request-records",
            "2048",
        ]);
        args.record_bytes = bytes;
        let mut config = Config::new(args).unwrap();
        let count = 10_000;
        let sparse_bodies = count * (4 + 76 + 17 + bytes as u64);
        assert!(config.resident_descriptor_reservation(sparse_bodies) >= count * (768 + 48));
        assert!(config.memory_reservation_bytes() < 20 * 1024 * 1024 * 1024);
        if bytes == 16 {
            config.args.segment_decoded_mib = 1024;
            assert!(config.memory_reservation_bytes() > 24 * 1024 * 1024 * 1024);
        }
    }
}

#[test]
fn default_reader_processes_are_capped_by_partitions_and_can_be_overridden() {
    for (partitions, expected) in [("1", 1), ("2", 2), ("4", 4), ("16", 4)] {
        let args = [
            "bench",
            "--system",
            "single-durable",
            "--network-ingress",
            "--processes",
            "--streaming",
            "--duration",
            "3",
            "--window",
            partitions,
        ];
        assert_eq!(
            Config::new(Args::parse_from(args))
                .unwrap()
                .args
                .reader_workers(),
            expected
        );
        for (override_count, valid) in [("1", true), ("0", false), ("33", false)] {
            let config = Config::new(Args::parse_from(
                args.into_iter().chain(["--reader-workers", override_count]),
            ));
            assert_eq!(config.is_ok(), valid);
            if let Ok(config) = config {
                assert_eq!(config.args.reader_workers(), 1);
            }
        }
    }
}

#[test]
fn record_ids_keep_run_entropy_instead_of_discarding_uuid_suffix() {
    let mut bytes = [42; 16];
    let first = GroupId::from_bytes(bytes);
    bytes[12] ^= 0x80;
    let second = GroupId::from_bytes(bytes);
    assert_ne!(message_id(first, 0, 0), message_id(second, 0, 0));
    assert_ne!(message_id(first, 0, 0), message_id(first, 1, 0));
    assert_ne!(message_id(first, 0, 0), message_id(first, 0, 1));
}

#[test]
fn disk_quorum_window_holds_many_full_sdk_batches_beyond_one_storage_group() {
    let config = Config::new(Args::parse_from([
        "bench",
        "--system",
        "disk-quorum",
        "--record-bytes",
        "1024",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "1",
    ]))
    .unwrap();
    let window = config.disk_pipeline();
    // A slow follower pays barriers per window. Two batches per window made
    // saturated 1 KiB catch-up miss its deadline on a 47 ms/barrier disk.
    assert!(window.max_body_bytes >= 8 * SDK_BATCH_TARGET_BYTES);
    assert!(window.max_body_bytes > config.decode_limits().max_group_decoded_body_bytes);
    assert_eq!(
        window.max_operations,
        ozzy_runtime::replica_journal::MAX_APPEND_OPERATIONS
    );
    // Storage grouping is unchanged; only the replication window grew.
    assert_eq!(
        config.decode_limits().max_group_decoded_body_bytes,
        8 * 1024 * 1024
    );
}

#[test]
fn large_sdk_queue_keeps_operation_and_reader_bytes_within_storage_bounds() {
    for bytes in [16, 128, 1024, 8192] {
        let mut args = Args::parse_from([
            "bench",
            "--system",
            "disk-quorum",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
            "--request-records",
            "8192",
        ]);
        args.record_bytes = bytes;
        let config = Config::new(args).unwrap();
        let writer = config.writer_limits();
        assert_eq!(
            writer.max_records,
            ozzy_runtime::replicated::MAX_APPEND_RECORDS
        );
        assert_eq!(
            writer.envelope.max_payload_bytes,
            (ozzy_runtime::replicated::MAX_APPEND_RECORDS * bytes)
                .min(sdk_batch_target(&config.args))
        );
        assert!(config.history.operation.max_payload_bytes >= writer.envelope.max_payload_bytes);
        assert!(config.history.operation.max_payload_bytes <= 4 * 1024 * 1024);
        let decode = config.decode_limits();
        assert!(decode.max_decoded_body_bytes <= decode.max_group_decoded_body_bytes);
        assert!(decode.max_group_decoded_body_bytes <= decode.max_segment_decoded_body_bytes);
        assert!(
            config.reader_limits().envelope.max_payload_bytes
                <= config.disk_pipeline().max_body_bytes
        );
    }
}

#[test]
fn small_segment_respects_sdk_cap_and_physical_bound() {
    let args = Args::parse_from([
        "bench",
        "--system",
        "disk-quorum",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "1",
        "--window",
        "2",
        "--record-bytes",
        "8192",
        "--request-records",
        "256",
        "--writer-batch-records",
        "256",
        "--segment-mib",
        "4",
    ]);
    let config = Config::new(args).unwrap();
    let writer = config.writer_limits();
    assert_eq!(writer.envelope.max_payload_bytes, SDK_BATCH_TARGET_BYTES);
    assert!(writer.envelope.max_payload_bytes < 256 * 8192);
    let usable = 4 * 1024 * 1024 - ozzy_journal_segment::SEGMENT_HEADER_BYTES;
    let physical = usable / ozzy_journal_segment::WRITE_GROUP_ALIGNMENT
        * ozzy_journal_segment::WRITE_GROUP_ALIGNMENT
        - ozzy_journal_segment::ENTRY_HEADER_BYTES
        - ozzy_journal_segment::GROUP_SEAL_BYTES;
    assert!(config.history.operation.max_body_bytes <= physical);
}

#[test]
fn reader_capacity_fits_record_and_payload_bounds_independently_of_writers() {
    for (bytes, records) in [(16, 16384), (128, 16384), (1024, 16384), (8192, 16384)] {
        let mut args = Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
        ]);
        args.record_bytes = bytes;
        args.system = System::SingleDurable;
        let config = Config::new(args.clone()).unwrap();
        assert_eq!(config.reader_records(), records);
        assert_eq!(
            config.reader_limits().envelope.max_payload_bytes,
            records * bytes
        );
        assert_eq!(config.writer_limits().max_records, 1024);
        args.reader_records = 7;
        args.reader_payload_mib = 1;
        assert_eq!(Config::new(args).unwrap().reader_records(), 7);
    }
}
#[test]
fn production_large_batch_byte_caps_reserve_real_sdk_owners_without_io() {
    for (target, records, inflight, fits_vm) in [
        ("832", "2048", "3", true),
        ("1536", "2048", "3", true),
        ("2048", "2048", "3", true),
        ("4096", "512", "2", true),
        ("4096", "2048", "2", false),
        ("4096", "2048", "3", false),
        ("16384", "2048", "3", false),
    ] {
        let args = Args::parse_from([
            "bench",
            "--system",
            "disk-quorum",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
            "--window",
            "8",
            "--partitions",
            "8",
            "--record-bytes",
            "8192",
            "--request-records",
            records,
            "--writer-inflight-appends",
            inflight,
            "--producer-workers",
            "4",
            "--app-threads",
            "1",
            "--reader-workers",
            "8",
            "--writer-batch-target-kib",
            target,
        ]);
        let config = Config::production(args).unwrap();
        let expected = (target.parse::<usize>().unwrap() * 1024).min(8 * 1024 * 1024 - 49_241);
        assert_eq!(config.writer_limits().envelope.max_payload_bytes, expected);
        assert_eq!(
            super::super::native::writer_settings(&config).batch_target_bytes,
            expected
        );
        assert!(config.memory_reservation_bytes() > expected as u64);
        assert_eq!(
            config.memory_reservation_bytes() < 24 * 1024 * 1024 * 1024,
            fits_vm,
            "target={target} records={records} inflight={inflight} bytes={}",
            config.memory_reservation_bytes()
        );
        assert!(config.live_queue_messages() * expected <= 64 * 832 * 1024);
    }
}

#[test]
fn delay_needs_plain_production_readers_and_a_drain_past_it() {
    let args = |extra: &[&str]| {
        Args::try_parse_from(
            [
                "bench",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "1",
            ]
            .into_iter()
            .chain(extra.iter().copied()),
        )
    };
    for (extra, valid) in [
        (&["--delay-millis", "9999"][..], true),
        (&["--delay-millis", "10000"], false),
        (
            &["--delay-millis", "60000", "--drain-timeout-secs", "61"],
            true,
        ),
        (&["--delay-millis", "500", "--groups", "1"], false),
        (
            &[
                "--delay-millis",
                "500",
                "--dedup-window-secs",
                "1",
                "--dedup-max-entries",
                "1000",
                "--dedup-max-bytes",
                "16777216",
            ],
            false,
        ),
    ] {
        let parsed = args(extra).unwrap();
        assert_eq!(Config::production(parsed).is_ok(), valid, "{extra:?}");
    }
    assert!(Config::new(args(&["--delay-millis", "500"]).unwrap()).is_err());
    assert!(args(&["--delay-max-entries", "10"]).is_err());
    assert!(args(&["--delay-millis", "0"]).is_err());
}
