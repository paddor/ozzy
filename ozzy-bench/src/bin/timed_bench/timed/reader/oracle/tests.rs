use super::*;
use clap::Parser;

fn configuration() -> Config {
    Config::new(crate::bench::Args::parse_from([
        "bench",
        "--system",
        "single-durable",
        "--processes",
        "--network-ingress",
        "--streaming",
        "--duration",
        "1",
        "--warmup",
        "0",
        "--window",
        "4",
        "--reader-workers",
        "1",
        "--group",
        "00000000-0000-0000-0000-000000000001",
    ]))
    .unwrap()
}

fn window() -> Window {
    Window {
        start: 10_000_000_000,
        measured: 10_000_000_000,
        end: 11_000_000_000,
    }
}

fn payload(config: &Config) -> Vec<u8> {
    let mut bytes = vec![7; config.args.record_bytes];
    bytes[..8].copy_from_slice(&(window().start + 100).to_be_bytes());
    bytes
}

#[test]
fn shared_oracle_verifies_interleaved_writers_and_idle_partitions() {
    let config = configuration();
    let group = config::group(&config.args).unwrap();
    let window = window();
    let mut oracle = Oracle::new(&config, 3, &[0, 1, 2], 0, window).unwrap();
    let mut writers = (0..config.writers)
        .map(|_| Counts::new(window))
        .collect::<Vec<_>>();
    let payload = payload(&config);
    let mut offsets = [0; 3];
    for (lane, sequence) in [(0, 0), (3, 0), (0, 1), (1, 0), (3, 1)] {
        let partition = lane % 3;
        let id = config::message_id(group, lane, sequence);
        writers[lane].record_bytes(id, &payload);
        writers[lane]
            .complete(window.start + 100, window.start + 200, 1)
            .unwrap();
        oracle
            .observe(
                &config,
                group,
                Record {
                    partition: partition as u32,
                    offset: offsets[partition],
                    id,
                    payload: &payload,
                },
                window.start + 200,
            )
            .unwrap();
        offsets[partition] += 1;
    }
    assert!(oracle.complete(&[2, 1, 0, 2]).unwrap());
    assert_eq!(oracle.pending(Some(&[2, 1, 0, 2])).len(), 0);
    assert_eq!(oracle.positions(), [(0, 4), (1, 1), (2, 0)]);
    let writers = vec![json!({"lanes": writers.into_iter().enumerate()
        .map(|(lane, counts)| counts.report(lane)).collect::<Vec<_>>()})];
    let readers = vec![json!({"lanes": oracle.rows()})];
    crate::bench::timed::results::verify_digests(&writers, &readers, 4).unwrap();
    let mut corrupted = readers;
    corrupted[0]["lanes"][3]["digest_xxh3_128"] = json!(vec![0_u8; 16]);
    assert!(crate::bench::timed::results::verify_digests(&writers, &corrupted, 4).is_err());
}

#[test]
fn shared_oracle_rejects_gaps_wrong_writers_and_unassigned_partitions() {
    let config = configuration();
    let group = config::group(&config.args).unwrap();
    let mut oracle = Oracle::new(&config, 2, &[0], 0, window()).unwrap();
    let payload = payload(&config);
    for (partition, offset, lane, sequence) in [
        (0, 1, 0, 0),
        (1, 0, 1, 0),
        (3, 0, 0, 0),
        (0, 0, 0, 1),
        (0, 0, 7, 0),
    ] {
        assert!(
            oracle
                .observe(
                    &config,
                    group,
                    Record {
                        partition,
                        offset,
                        id: config::message_id(group, lane, sequence),
                        payload: &payload,
                    },
                    window().start + 200
                )
                .is_err()
        );
        assert_eq!(oracle.positions(), [(0, 0)]);
    }
    oracle
        .observe(
            &config,
            group,
            Record {
                partition: 0,
                offset: 0,
                id: config::message_id(group, 0, 0),
                payload: &payload,
            },
            window().start + 200,
        )
        .unwrap();
    assert!(
        oracle
            .observe(
                &config,
                group,
                Record {
                    partition: 0,
                    offset: 0,
                    id: config::message_id(group, 0, 0),
                    payload: &payload,
                },
                window().start + 200
            )
            .is_err()
    );
    assert_eq!(oracle.positions(), [(0, 1)]);
    assert!(oracle.complete(&[1, 0, 0, 0]).unwrap());
    assert!(oracle.complete(&[0, 0, 0, 0]).is_err());
}

#[test]
fn untimed_records_verify_at_once_and_share_the_flush_clock() {
    let config = configuration();
    let group = config::group(&config.args).unwrap();
    let mut oracle = Oracle::new(&config, 2, &[0], 0, window()).unwrap();
    let payload = payload(&config);
    for sequence in 0..3 {
        oracle
            .observe_untimed(
                &config,
                group,
                Record {
                    partition: 0,
                    offset: sequence,
                    id: config::message_id(group, 0, sequence),
                    payload: &payload,
                },
            )
            .unwrap();
    }
    // Identity and order checks do not wait for the clock.
    assert!(
        oracle
            .observe_untimed(
                &config,
                group,
                Record {
                    partition: 0,
                    offset: 3,
                    id: config::message_id(group, 0, 4),
                    payload: &payload,
                },
            )
            .is_err()
    );
    assert!(oracle.complete(&[3, 0, 0, 0]).unwrap());
    oracle.flush(window().start + 300).unwrap();
    let rows = oracle.rows();
    assert_eq!(rows[0]["total"], 3);
    assert_eq!(
        rows[0]["latency"]["bins"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_u64)
            .sum::<u64>(),
        3
    );
    assert_eq!(rows[0]["last_ns"], window().start + 300);
}

#[test]
fn held_records_time_from_release_and_never_arrive_before_it() {
    let mut config = configuration();
    config.args.native.delay_millis = Some(2);
    let group = config::group(&config.args).unwrap();
    let payload = payload(&config);
    let record = Record {
        partition: 0,
        offset: 0,
        id: config::message_id(group, 0, 0),
        payload: &payload,
    };
    // Sent at start + 100 ns, so released no sooner than 2 ms later.
    let release = window().start + 100 + 2_000_000;
    let mut early = Oracle::new(&config, 2, &[0], 0, window()).unwrap();
    let error = early
        .observe(&config, group, record, release - 1)
        .unwrap_err();
    assert!(error.to_string().contains("before its due time"), "{error}");
    let mut oracle = Oracle::new(&config, 2, &[0], 0, window()).unwrap();
    oracle
        .observe(&config, group, record, release + 300)
        .unwrap();
    let rows = oracle.rows();
    assert_eq!(rows[0]["latency"]["max"], 300);
    // The throughput window moves with the delay, so this release counts.
    assert_eq!(rows[0]["completed_per_second"], json!([1]));
}
