use super::*;
use clap::Parser;

#[test]
fn native_and_external_inputs_share_exact_binary_and_json_payloads() {
    for (size, flag) in [
        ("16", "--binary-payload"),
        ("128", "--json-payload"),
        ("1024", "--json-payload"),
        ("8192", "--json-payload"),
    ] {
        let config = Config::new(super::super::super::Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "10",
            "--record-bytes",
            size,
            flag,
        ]))
        .unwrap();
        let group = GroupId::new();
        let mut scratch = Vec::with_capacity(RecordInput::INLINE_BYTES);
        for lane in 0..4 {
            for sequence in [0, 129, u64::from(u32::MAX) + 1] {
                let external = payload(&config, lane, sequence, 1234).unwrap();
                let native = Lane {
                    config: &config,
                    group,
                    index: lane,
                }
                .logical_input(sequence, 1234, &mut scratch)
                .unwrap();
                assert_eq!(external.len(), config.args.record_bytes);
                assert_eq!(native.parts().next().unwrap(), external.as_ref());
                assert_eq!(&external[..8], 1234_u64.to_be_bytes());
                if config.args.binary_payload {
                    assert_eq!(external[10], lane as u8);
                } else {
                    // The last event is cut at the record size; all others parse.
                    assert_eq!(external[8], b'{');
                    let text = std::str::from_utf8(&external[8..]).unwrap();
                    let events = text.split_inclusive('\n').filter(|e| e.ends_with('\n'));
                    assert_eq!(events.clone().count() > 0, config.args.record_bytes >= 1024);
                    for event in events {
                        serde_json::from_str::<Value>(event).unwrap();
                    }
                }
            }
        }
    }
}

#[test]
fn bodies_repeat_only_after_the_pool_and_one_request() {
    for (size, flag) in [("16", "--binary-payload"), ("128", "--json-payload")] {
        let config = Config::new(super::super::super::Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "10",
            "--record-bytes",
            size,
            flag,
        ]))
        .unwrap();
        let bodies = payload_bodies(&config);
        let body = config.args.record_bytes as u64 - 8;
        assert!(bodies * body >= PAYLOAD_POOL_BYTES as u64);
        assert!(bodies >= config.args.request_records as u64);
        let first = payload(&config, 1, 7, 1).unwrap();
        assert_eq!(first[8..], payload(&config, 1, 7 + bodies, 2).unwrap()[8..]);
        for other in [8, 7 + bodies - 1, 7 + config.args.request_records as u64] {
            assert_ne!(first[8..], payload(&config, 1, other, 1).unwrap()[8..]);
        }
    }
}
