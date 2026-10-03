use ozzy_proto::append::{
    Append, AppendKey, Authority, CodecError, DataLimits, Policy, Record, decode_append,
    encode_append, encode_append_metadata,
};
use ozzy_proto::append::{Appended, OpPosition, decode_appended, encode_appended};
use ozzy_proto::{Envelope, EnvelopeLimits, Opcode, Packet, decode_packet};
use ozzy_proto::{
    GroupId, LinkSessionId, MessageId, NodeId, PartitionIncarnation, ProducerId, RequestId,
};

fn envelope() -> Envelope {
    Envelope {
        opcode: Opcode::Append,
        response: false,
        request_id: Some(RequestId::from_bytes([1; 16])),
        sender: NodeId::from_bytes([2; 16]),
        session: Some(LinkSessionId::from_bytes([3; 16])),
    }
}

fn request<'a>(records: &'a [Record<'a>]) -> Append<'a> {
    Append {
        authority: Authority {
            group_id: GroupId::from_bytes([4; 16]),
            config_epoch: 1,
            view: 2,
        },
        partition: PartitionIncarnation::from_bytes([5; 16]),
        owner_epoch: 3,
        key: AppendKey {
            producer_id: ProducerId::from_bytes([6; 16]),
            producer_epoch: 4,
            first_sequence: 5,
        },
        policy: Policy::QuorumDurable,
        records,
    }
}

fn packet<'a>(metadata: &'a [u8], payload: &'a [u8]) -> Packet<'a> {
    Packet {
        envelope: envelope(),
        metadata,
        payload,
    }
}

#[test]
fn append_routing_reads_only_fixed_destination_and_writer_prefix() {
    use ozzy_proto::append::{route, validate_append};
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([7; 16]),
        parts: &[b"abcd"],
    }];
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let expected = request(&records);
    encode_append(envelope(), expected, &mut metadata, &mut payload, limits).unwrap();
    let destination = route(packet(&metadata, &payload), limits.envelope).unwrap();
    assert_eq!(destination.authority, expected.authority);
    assert_eq!(destination.partition, expected.partition);
    assert_eq!(destination.owner_epoch, expected.owner_epoch);
    assert_eq!(destination.key, expected.key);
    for length in 0..89 {
        assert!(route(packet(&metadata[..length], &payload), limits.envelope).is_err());
    }
    // Dispatch cannot establish canonical validity. Invalid payload encoding is
    // rejected by the destination, after the same fixed routing prefix.
    metadata[90] = 255;
    assert_eq!(
        route(packet(&metadata, &payload), limits.envelope).unwrap(),
        destination
    );
    assert!(validate_append(packet(&metadata, &payload), limits).is_err());
    let small = EnvelopeLimits {
        max_payload_bytes: 3,
        ..limits.envelope
    };
    assert!(route(packet(&metadata, &payload), small).is_err());
    for range in [1..17, 17..25, 33..49, 49..57, 57..73, 73..81] {
        let mut invalid = metadata.clone();
        invalid[range].fill(0);
        assert!(route(packet(&invalid, &payload), limits.envelope).is_err());
    }
}

#[test]
fn append_rejects_truncation_trailing_bytes_forged_counts_and_sequence_overflow() {
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([7; 16]),
        parts: &[b"abcd"],
    }];
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    encode_append(
        envelope(),
        request(&records),
        &mut metadata,
        &mut payload,
        limits,
    )
    .unwrap();
    for length in 0..metadata.len() {
        assert!(
            decode_append(packet(&metadata[..length], &payload), limits).is_err(),
            "metadata length {length}"
        );
    }
    let mut extra = metadata.clone();
    extra.push(0);
    assert!(decode_append(packet(&extra, &payload), limits).is_err());
    for bytes in [b"".as_slice(), b"abc", b"abcde"] {
        assert!(decode_append(packet(&metadata, bytes), limits).is_err());
    }
    for offset in [91, 95, 115, 119] {
        let mut bad = metadata.clone();
        bad[offset..offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(
            decode_append(packet(&bad, &payload), limits).is_err(),
            "field {offset}"
        );
    }
    for range in [
        1..17,
        17..25,
        33..49,
        49..57,
        57..73,
        73..81,
        95..99,
        115..119,
    ] {
        let mut bad = metadata.clone();
        bad[range.clone()].fill(0);
        assert!(
            decode_append(packet(&bad, &payload), limits).is_err(),
            "zero field {range:?}"
        );
    }
    for policy in [0, 7, 255] {
        let mut bad = metadata.clone();
        bad[89] = policy;
        assert!(decode_append(packet(&bad, &payload), limits).is_err());
    }
    let mut bad = metadata.clone();
    bad[81..89].copy_from_slice(&u64::MAX.to_be_bytes());
    assert!(decode_append(packet(&bad, &payload), limits).is_err());
}

#[test]
fn append_requires_request_correlation_session_and_directional_limits() {
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([7; 16]),
        parts: &[b"abcd"],
    }];
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    encode_append(
        envelope(),
        request(&records),
        &mut metadata,
        &mut payload,
        limits,
    )
    .unwrap();
    for bad_envelope in [
        Envelope {
            opcode: Opcode::Appended,
            ..envelope()
        },
        Envelope {
            response: true,
            ..envelope()
        },
        Envelope {
            request_id: None,
            ..envelope()
        },
        Envelope {
            session: None,
            ..envelope()
        },
        Envelope {
            sender: NodeId::from_bytes([0; 16]),
            ..envelope()
        },
    ] {
        assert!(
            decode_append(
                Packet {
                    envelope: bad_envelope,
                    metadata: &metadata,
                    payload: &payload
                },
                limits
            )
            .is_err()
        );
    }
    for restricted in [
        DataLimits {
            max_records: 0,
            ..limits
        },
        DataLimits {
            max_parts: 0,
            ..limits
        },
        DataLimits {
            envelope: EnvelopeLimits {
                max_metadata_bytes: metadata.len() - 1,
                ..limits.envelope
            },
            ..limits
        },
        DataLimits {
            envelope: EnvelopeLimits {
                max_payload_bytes: payload.len() - 1,
                ..limits.envelope
            },
            ..limits
        },
    ] {
        assert!(
            decode_append(
                Packet {
                    envelope: envelope(),
                    metadata: &metadata,
                    payload: &payload
                },
                restricted
            )
            .is_err()
        );
    }
}

#[test]
fn encode_reuses_both_arenas_and_rejection_preserves_previous_frames() {
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([7; 16]),
        parts: &[b"abcd"],
    }];
    let append = request(&records);
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(123);
    let mut payload = Vec::with_capacity(4);
    let before = (
        metadata.as_ptr(),
        payload.as_ptr(),
        metadata.capacity(),
        payload.capacity(),
    );
    for _ in 0..32 {
        encode_append(envelope(), append, &mut metadata, &mut payload, limits).unwrap();
    }
    assert_eq!(
        before,
        (
            metadata.as_ptr(),
            payload.as_ptr(),
            metadata.capacity(),
            payload.capacity()
        )
    );
    let old_metadata = metadata.clone();
    let old_payload = payload.clone();
    for invalid in [
        Append {
            records: &[],
            ..append
        },
        Append {
            owner_epoch: 0,
            ..append
        },
        Append {
            key: AppendKey {
                first_sequence: u64::MAX,
                ..append.key
            },
            ..append
        },
    ] {
        assert!(encode_append(envelope(), invalid, &mut metadata, &mut payload, limits).is_err());
        assert_eq!(metadata, old_metadata);
        assert_eq!(payload, old_payload);
    }
    let large = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: records[0].message_id,
        parts: &[b"abcde"],
    }];
    assert_eq!(
        encode_append(
            envelope(),
            request(&large),
            &mut metadata,
            &mut payload,
            limits
        ),
        Err(CodecError::Capacity)
    );
    assert_eq!(metadata, old_metadata);
    assert_eq!(payload, old_payload);
    let no_parts = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: records[0].message_id,
        parts: &[],
    }];
    assert_eq!(
        encode_append(
            envelope(),
            request(&no_parts),
            &mut metadata,
            &mut payload,
            limits
        ),
        Err(CodecError::Parts)
    );
    assert_eq!(metadata, old_metadata);
    assert_eq!(payload, old_payload);
}

#[test]
fn metadata_only_append_preserves_previous_frame_on_every_bound_failure() {
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([7; 16]),
        parts: &[b"ab", b"", b"cd"],
    }];
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(131);
    encode_append_metadata(envelope(), request(&records), &mut metadata, limits).unwrap();
    let original = metadata.clone();
    let pointer = metadata.as_ptr();
    for restricted in [
        DataLimits {
            max_records: 0,
            ..limits
        },
        DataLimits {
            max_parts: 2,
            ..limits
        },
        DataLimits {
            envelope: EnvelopeLimits {
                max_metadata_bytes: 124,
                ..limits.envelope
            },
            ..limits
        },
        DataLimits {
            envelope: EnvelopeLimits {
                max_payload_bytes: 3,
                ..limits.envelope
            },
            ..limits
        },
    ] {
        assert!(
            encode_append_metadata(envelope(), request(&records), &mut metadata, restricted)
                .is_err()
        );
        assert_eq!(metadata, original);
        assert_eq!(metadata.as_ptr(), pointer);
    }
    let no_parts = [Record {
        parts: &[],
        ..records[0]
    }];
    for invalid in [
        request(&[]),
        request(&no_parts),
        Append {
            owner_epoch: 0,
            ..request(&records)
        },
        Append {
            key: AppendKey {
                first_sequence: u64::MAX,
                ..request(&records).key
            },
            ..request(&records)
        },
    ] {
        assert!(encode_append_metadata(envelope(), invalid, &mut metadata, limits).is_err());
        assert_eq!(metadata, original);
    }
    let mut small = vec![99];
    assert_eq!(
        encode_append_metadata(envelope(), request(&records), &mut small, limits),
        Err(CodecError::Capacity)
    );
    assert_eq!(small, [99]);
}

#[test]
fn record_iterators_are_independent_and_preserve_empty_records() {
    let records = [
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([7; 16]),
            parts: &[b"a", b"b"],
        },
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([8; 16]),
            parts: &[b""],
        },
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([9; 16]),
            parts: &[b"c", b"", b"d"],
        },
    ];
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let limits = DataLimits {
        max_records: 3,
        max_parts: 6,
        ..DataLimits::default()
    };
    let header = encode_append(
        envelope(),
        request(&records),
        &mut metadata,
        &mut payload,
        limits,
    )
    .unwrap();
    let packet = decode_packet(&[&header, &metadata, &payload], limits.envelope).unwrap();
    let decoded = decode_append(packet, limits).unwrap();
    assert_eq!(decoded.records.len(), 3);
    let mut iter = decoded.records.iter();
    let mut first = iter.next().unwrap();
    assert_eq!(first.parts.next(), Some(b"a".as_slice()));
    assert_eq!(
        iter.next().unwrap().parts.collect::<Vec<_>>(),
        [b"".as_slice()]
    );
    assert_eq!(
        iter.next().unwrap().parts.collect::<Vec<_>>(),
        [b"c".as_slice(), b"", b"d"]
    );
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
    assert_eq!(first.parts.next(), Some(b"b".as_slice()));
    assert!(first.parts.next().is_none());
    assert!(
        decode_append(
            packet,
            DataLimits {
                max_parts: 5,
                ..limits
            }
        )
        .is_err()
    );
    assert!(
        decode_append(
            packet,
            DataLimits {
                max_records: 2,
                ..limits
            }
        )
        .is_err()
    );
}

fn receipt(ids: &[MessageId]) -> Appended<'_> {
    let request = request(&[]);
    Appended {
        authority: request.authority,
        partition: request.partition,
        owner_epoch: request.owner_epoch,
        key: request.key,
        first_offset: 9,
        policy: request.policy,
        position: OpPosition {
            op: 11,
            digest: [12; 32],
        },
        message_ids: ids,
    }
}

#[test]
fn appended_rejects_inconsistent_counts_positions_payloads_and_overflow() {
    let ids = [MessageId::from_bytes([7; 16])];
    let response = Envelope {
        opcode: Opcode::Appended,
        response: true,
        ..envelope()
    };
    let mut metadata = Vec::with_capacity(1024);
    let limits = DataLimits::default();
    encode_appended(response, receipt(&ids), &mut metadata, limits).unwrap();
    for length in 0..metadata.len() {
        assert!(
            decode_appended(
                Packet {
                    envelope: response,
                    metadata: &metadata[..length],
                    payload: &[]
                },
                limits
            )
            .is_err(),
            "length {length}"
        );
    }
    for (range, byte) in [
        (89..97, 255),
        (81..89, 255),
        (97..101, 0),
        (97..101, 255),
        (142..146, 0),
        (142..146, 255),
        (102..110, 0),
        (102..110, 255),
        (110..142, 0),
        (101..102, 0),
        (101..102, 255),
    ] {
        let mut bad = metadata.clone();
        bad[range.clone()].fill(byte);
        assert!(
            decode_appended(
                Packet {
                    envelope: response,
                    metadata: &bad,
                    payload: &[]
                },
                limits
            )
            .is_err(),
            "field {range:?}"
        );
    }
    let mut extra = metadata.clone();
    extra.push(0);
    assert!(
        decode_appended(
            Packet {
                envelope: response,
                metadata: &extra,
                payload: &[]
            },
            limits
        )
        .is_err()
    );
    assert!(
        decode_appended(
            Packet {
                envelope: response,
                metadata: &metadata,
                payload: &[0]
            },
            limits
        )
        .is_err()
    );
    assert!(
        decode_appended(
            Packet {
                envelope: envelope(),
                metadata: &metadata,
                payload: &[]
            },
            limits
        )
        .is_err()
    );
    assert!(
        decode_appended(
            Packet {
                envelope: response,
                metadata: &metadata,
                payload: &[]
            },
            DataLimits {
                max_records: 0,
                ..limits
            }
        )
        .is_err()
    );
}

#[test]
fn invalid_receipt_encoding_preserves_buffer_without_growing_it() {
    let ids = [MessageId::from_bytes([7; 16])];
    let valid = receipt(&ids);
    let response = Envelope {
        opcode: Opcode::Appended,
        response: true,
        ..envelope()
    };
    let mut metadata = Vec::with_capacity(162);
    let limits = DataLimits::default();
    let before = (metadata.as_ptr(), metadata.capacity());
    for _ in 0..32 {
        encode_appended(response, valid, &mut metadata, limits).unwrap();
    }
    assert_eq!(before, (metadata.as_ptr(), metadata.capacity()));
    let expected = metadata.clone();
    let larger = [ids[0], ids[0]];
    for invalid in [
        Appended {
            message_ids: &[],
            ..valid
        },
        Appended {
            message_ids: &larger,
            ..valid
        },
        Appended {
            first_offset: u64::MAX,
            ..valid
        },
        Appended {
            position: OpPosition {
                op: 0,
                ..valid.position
            },
            ..valid
        },
        Appended {
            position: OpPosition {
                digest: [0; 32],
                ..valid.position
            },
            ..valid
        },
    ] {
        assert!(encode_appended(response, invalid, &mut metadata, limits).is_err());
        assert_eq!(metadata, expected);
        assert_eq!(before, (metadata.as_ptr(), metadata.capacity()));
    }
}
