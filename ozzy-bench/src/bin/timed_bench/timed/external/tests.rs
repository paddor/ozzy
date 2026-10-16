use super::*;
use clap::Parser;
use ozzy_proto::GroupId;

#[test]
fn external_topology_has_adapter_bounds_without_native_claims() {
    let config = Config::new(super::super::super::Args::parse_from([
        "bench",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "5",
        "--window",
        "1",
        "--external-policy",
        "disk-quorum",
        "--external-system",
        "redpanda",
        "--external-endpoint",
        "127.0.0.1:18090",
    ]))
    .unwrap();
    validate(&config).unwrap();
    let window = Window {
        start: 1,
        measured: 10,
        end: 20,
    };
    let report = |finished| {
        let mut counts = Counts::new(window);
        counts.submission(12, 1);
        counts.complete(12, finished, 1).unwrap();
        json!({"lanes": [counts.report(0)]})
    };
    let mut row =
        super::super::results::summarize(&config, window, vec![report(15)], vec![report(16)], &[])
            .unwrap();
    describe(&config, "records", &mut row).unwrap();
    assert_eq!(row["request_records"], config.args.request_records);
    assert_eq!(row["reader_records_max"], 1);
    assert_eq!(row["reader_payload_bytes_max"], config.args.record_bytes);
    assert_eq!(row["system"], "redpanda-disk-quorum");
    assert_eq!(row["native_client_protocol"], false);
    for field in [
        "commit_policy",
        "ack_copies",
        "replica_voters",
        "journal_writes",
        "segment_write_mode",
        "broker_io_threads",
        "disk_io_backend",
        "native_reader_api",
    ] {
        assert!(row.get(field).is_none(), "unexpected native field {field}");
    }
}

#[test]
fn external_policy_sets_labels_copies_and_disk_boundary() {
    for (mode, name, brokers, durable) in [
        ("buffered", "buffered", 1, false),
        ("durable", "durable", 1, true),
        ("disk-quorum", "disk-quorum", 3, true),
    ] {
        let config = Config::new(super::super::super::Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "5",
            "--external-policy",
            mode,
            "--external-system",
            "redpanda",
            "--external-endpoint",
            "127.0.0.1:18090",
        ]))
        .unwrap();
        validate(&config).unwrap();
        let mut row = json!({});
        describe(&config, "records", &mut row).unwrap();
        assert_eq!(row["system"], format!("redpanda-{name}"));
        assert_eq!(row["write_caching"], !durable);
        assert_eq!(kafka::replicas(&config).unwrap(), brokers);
        assert_eq!(
            kafka::write_caching(&config).unwrap(),
            Some(if durable { "false" } else { "true" })
        );
    }
}

#[test]
fn external_policies_do_not_silently_weaken_durability() {
    for (system, policy, valid) in [
        ("kafka", "buffered", true),
        ("kafka", "durable", false),
        ("iggy", "buffered", true),
        ("iggy", "durable", true),
        ("iggy", "volatile", false),
        ("iggy", "disk-quorum", true),
    ] {
        let accepted = super::super::super::Args::try_parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "5",
            "--external-policy",
            policy,
            "--external-system",
            system,
            "--external-endpoint",
            "127.0.0.1:18090",
        ])
        .ok()
        .and_then(|args| Config::new(args).ok())
        .is_some_and(|config| validate(&config).is_ok());
        assert_eq!(accepted, valid, "{system} {policy}");
    }
}

#[test]
fn external_comparisons_reject_native_queue_overrides() {
    let config = Config::new(super::super::super::Args::parse_from([
        "bench",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "5",
        "--external-policy",
        "buffered",
        "--external-system",
        "iggy",
        "--external-endpoint",
        "127.0.0.1:18090",
        "--writer-inflight-appends",
        "2",
    ]))
    .unwrap();
    assert!(validate(&config).is_err());
}

#[test]
fn shared_external_verifier_rejects_offsets_identities_and_payload_corruption() {
    let config = Config::new(super::super::super::Args::parse_from([
        "bench",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "5",
        "--external-policy",
        "buffered",
        "--external-system",
        "kafka",
        "--external-endpoint",
        "127.0.0.1:19092",
    ]))
    .unwrap();
    validate(&config).unwrap();
    let group = GroupId::new();
    let bytes = producer::payload(&config, 0, 0, 100).unwrap();
    let id = config::message_id(group, 0, 0);
    let verify = |record: &Record| -> Result<()> {
        let window = Window {
            start: 0,
            measured: 0,
            end: 5_000_000_000,
        };
        let mut oracle = Oracle::new(&config, config.partitions(), &[0], 0, window)?;
        oracle.observe(
            &config,
            group,
            ObservedRecord {
                partition: 0,
                offset: record.offset,
                id: record.id,
                payload: &record.payload,
            },
            200,
        )?;
        let mut writer = Counts::new(window);
        writer.record_bytes(id, &bytes);
        writer.complete(100, 200, 1)?;
        super::super::results::verify_digests(
            &[json!({"lanes":[writer.report(0)]})],
            &[json!({"lanes":oracle.rows()})],
            1,
        )
    };
    assert!(
        verify(&Record {
            offset: 0,
            id,
            payload: bytes.clone()
        },)
        .is_ok()
    );
    assert!(
        verify(&Record {
            offset: 1,
            id,
            payload: bytes.clone()
        },)
        .is_err()
    );
    assert!(
        verify(&Record {
            offset: 0,
            id: config::message_id(group, 1, 0),
            payload: bytes.clone()
        },)
        .is_err()
    );
    // Every original byte is joined through the final writer/reader digest.
    for index in 0..bytes.len() {
        let mut corrupt = bytes.to_vec();
        corrupt[index] ^= 1;
        assert!(
            verify(&Record {
                offset: 0,
                id,
                payload: corrupt.into()
            },)
            .is_err()
        );
    }
}
