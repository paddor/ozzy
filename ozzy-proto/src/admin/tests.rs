use super::*;

fn replies() -> Vec<Reply> {
    let node = NodeId::from_bytes([3; 16]);
    let group = GroupId::from_bytes([4; 16]);
    vec![
        Reply::Broker(Broker {
            name: "b1".into(),
            id: node,
            pid: 77,
            uptime_ms: 1234,
            max_clients: 64,
            shards: 2,
            brokers: vec![
                ("b1".into(), node),
                ("b2".into(), NodeId::from_bytes([5; 16])),
            ],
            partitions: vec![PartitionName {
                group,
                topic: "orders".into(),
                partition: 7,
                shard: 1,
            }],
        }),
        Reply::Counters(Counters {
            uptime_ms: 99,
            events: vec![
                ("io_sync".into(), EventKind::Count),
                ("peak".into(), EventKind::Peak),
            ],
            totals: vec![10, u64::MAX],
            threads: vec![
                ("ozzy/shard/0".into(), vec![4, 5]),
                ("ozzy/shard/0".into(), vec![6, 0]),
            ],
        }),
        Reply::Partitions(Partitions {
            uptime_ms: 77,
            missing_shards: 1,
            rows: vec![
                PartitionStatus {
                    group,
                    state: PartitionState::Normal,
                    ready: true,
                    disk_pending: false,
                    view: 3,
                    leader: Some(node),
                    accepted: 10,
                    committed: 9,
                    applied: 8,
                    pending_operations: 2,
                    pending_body_bytes: 4096,
                    validation_cycles: 1,
                    reclaimed_bytes: 1 << 30,
                    written: 10,
                    durable: 9,
                    leader_commit: 9,
                    followers: vec![
                        (NodeId::from_bytes([5; 16]), 9),
                        (NodeId::from_bytes([7; 16]), 4),
                    ],
                    log: Some(PartitionLog {
                        first_offset: 100,
                        next_offset: 250,
                        producers: 3,
                        groups: vec![GroupStatus {
                            id: ConsumerGroupId::from_bytes([8; 16]),
                            name: "billing".into(),
                            revision: 4,
                            members: vec![(ConsumerMemberId::from_bytes([9; 16]), node)],
                        }],
                        cursors: vec![
                            GroupCursor {
                                group: ConsumerGroupId::from_bytes([8; 16]),
                                member: Some(ConsumerMemberId::from_bytes([9; 16])),
                                offset: Some(200),
                            },
                            GroupCursor {
                                group: ConsumerGroupId::from_bytes([10; 16]),
                                member: None,
                                offset: None,
                            },
                        ],
                    }),
                },
                PartitionStatus {
                    group: GroupId::from_bytes([6; 16]),
                    state: PartitionState::Local,
                    ready: false,
                    disk_pending: true,
                    view: 0,
                    leader: None,
                    accepted: 0,
                    committed: 0,
                    applied: 0,
                    pending_operations: 0,
                    pending_body_bytes: 0,
                    validation_cycles: 0,
                    reclaimed_bytes: 0,
                    written: 0,
                    durable: 0,
                    leader_commit: 0,
                    followers: Vec::new(),
                    log: None,
                },
            ],
        }),
        Reply::Connections(Connections {
            brokers: vec![(node, true), (NodeId::from_bytes([5; 16]), false)],
            clients: 12,
        }),
        Reply::Error("unknown request".into()),
    ]
}

#[test]
fn requests_round_trip_and_unknown_kinds_keep_their_id() {
    for request in [
        Request::Broker,
        Request::Counters,
        Request::Partitions,
        Request::Connections,
    ] {
        let bytes = encode_request(42, request);
        assert_eq!(decode_request(&bytes).unwrap(), (42, request));
    }
    let mut unknown = encode_request(43, Request::Broker);
    unknown[0] = 9;
    assert!(matches!(decode_request(&unknown), Err(CodecError::Command)));
    assert_eq!(request_id(&unknown), Some(43));
    assert!(decode_request(&unknown[..8]).is_err());
    assert_eq!(request_id(&unknown[..8]), None);
    unknown.push(0);
    assert!(decode_request(&unknown).is_err());
}

#[test]
fn replies_round_trip_and_reject_every_truncation_and_trailing_byte() {
    for reply in replies() {
        let bytes = encode_reply(u64::MAX, &reply).unwrap();
        assert_eq!(decode_reply(&bytes).unwrap(), (u64::MAX, reply));
        for end in 0..bytes.len() {
            assert!(decode_reply(&bytes[..end]).is_err(), "{end}");
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_reply(&trailing).is_err());
    }
}

#[test]
fn counters_need_one_value_per_event() {
    let Reply::Counters(mut counters) = replies().swap_remove(1) else {
        unreachable!()
    };
    counters.threads[1].1.pop();
    assert!(encode_reply(1, &Reply::Counters(counters)).is_err());
}

#[test]
fn oversized_counts_fail_before_allocating() {
    let mut bytes = encode_reply(1, &replies().swap_remove(2)).unwrap();
    bytes[21..25].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(decode_reply(&bytes), Err(CodecError::Length)));
}

#[test]
fn unknown_flags_and_kinds_are_rejected() {
    let bytes = encode_reply(1, &replies().swap_remove(2)).unwrap();
    // Kind, ID, uptime, missing shards, count, first group, then its state
    // and flags.
    for (offset, value) in [(0, 3), (1, 0x80)] {
        let mut bytes = bytes.clone();
        bytes[9 + 8 + 4 + 4 + 16 + offset] = value;
        assert!(matches!(decode_reply(&bytes), Err(CodecError::Profile)));
    }
    let mut bytes = encode_reply(1, &replies().swap_remove(1)).unwrap();
    // Kind, ID, uptime, count, name length and "io_sync".
    bytes[9 + 8 + 4 + 2 + 7] = 3;
    assert!(matches!(decode_reply(&bytes), Err(CodecError::Profile)));
    let mut bytes = encode_reply(1, &replies().swap_remove(3)).unwrap();
    // Kind, ID, count, first ID, then its link state.
    bytes[9 + 4 + 16] = 2;
    assert!(matches!(decode_reply(&bytes), Err(CodecError::Profile)));
    let mut bytes = encode_reply(1, &replies().swap_remove(4)).unwrap();
    bytes[0] = 9;
    assert!(matches!(decode_reply(&bytes), Err(CodecError::Command)));
}

#[test]
fn unknown_cursor_flags_are_rejected() {
    let Reply::Partitions(mut partitions) = replies().swap_remove(2) else {
        unreachable!()
    };
    // The first row ends with a cursor that has neither member nor offset.
    partitions.rows.truncate(1);
    let mut bytes = encode_reply(1, &Reply::Partitions(partitions)).unwrap();
    let flags = bytes.len() - 1;
    assert_eq!(bytes[flags], 0);
    bytes[flags] = 4;
    assert!(matches!(decode_reply(&bytes), Err(CodecError::Profile)));
}
