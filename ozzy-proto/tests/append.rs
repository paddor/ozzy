use ozzy_proto::append::{
    Append, AppendKey, Appended, Authority, DataLimits, OpPosition, PayloadEncoding, Policy,
    Record, decode_append, decode_append_with_scratch, decode_appended, encode_append,
    encode_append_metadata, encode_appended, encode_prepared_append_metadata, validate_append,
};
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

#[test]
fn appended_wire_identifies_exact_offsets_policy_and_log_position() {
    let ids = [
        MessageId::from_bytes([7; 16]),
        MessageId::from_bytes([8; 16]),
    ];
    let receipt = Appended {
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
        first_offset: 9,
        policy: Policy::QuorumDurable,
        position: OpPosition {
            op: 11,
            digest: [12; 32],
        },
        message_ids: &ids,
    };
    let response = Envelope {
        opcode: Opcode::Appended,
        response: true,
        ..envelope()
    };
    let mut metadata = Vec::with_capacity(1024);
    let header = encode_appended(response, receipt, &mut metadata, DataLimits::default()).unwrap();
    assert_eq!(metadata.len(), 178);
    assert_eq!(&metadata[89..97], &9_u64.to_be_bytes());
    assert_eq!(&metadata[97..101], &2_u32.to_be_bytes());
    assert_eq!(metadata[101], 5);
    assert_eq!(&metadata[102..110], &11_u64.to_be_bytes());
    assert_eq!(&metadata[110..142], &[12; 32]);
    assert_eq!(&metadata[142..146], &2_u32.to_be_bytes());
    assert_eq!(&metadata[146..162], ids[0].as_bytes());
    assert_eq!(&metadata[162..178], ids[1].as_bytes());
    let packet = decode_packet(&[&header, &metadata, &[]], EnvelopeLimits::default()).unwrap();
    let decoded = decode_appended(packet, DataLimits::default()).unwrap();
    assert_eq!(decoded.authority, receipt.authority);
    assert_eq!(decoded.key, receipt.key);
    assert_eq!(decoded.partition, receipt.partition);
    assert_eq!(decoded.owner_epoch, receipt.owner_epoch);
    assert_eq!(decoded.first_offset, 9);
    assert_eq!(decoded.policy, Policy::QuorumDurable);
    assert_eq!(decoded.position, receipt.position);
    assert_eq!(decoded.message_ids.collect::<Vec<_>>(), ids);
}

#[test]
fn append_wire_preserves_multipart_identity_without_assigned_offsets() {
    let parts: [&[u8]; 3] = [b"ab", b"", b"cd"];
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([7; 16]),
        parts: &parts,
    }];
    let append = Append {
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
        records: &records,
    };
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let header = encode_append(
        envelope(),
        append,
        &mut metadata,
        &mut payload,
        DataLimits::default(),
    )
    .unwrap();
    // Independently specified fixed fields, then one descriptor with three parts.
    let mut expected = vec![1];
    expected.extend_from_slice(&[4; 16]);
    expected.extend_from_slice(&1_u64.to_be_bytes());
    expected.extend_from_slice(&2_u64.to_be_bytes());
    expected.extend_from_slice(&[5; 16]);
    expected.extend_from_slice(&3_u64.to_be_bytes());
    expected.extend_from_slice(&[6; 16]);
    expected.extend_from_slice(&4_u64.to_be_bytes());
    expected.extend_from_slice(&5_u64.to_be_bytes());
    expected.push(5);
    expected.push(0);
    expected.extend_from_slice(&4_u32.to_be_bytes());
    expected.extend_from_slice(&1_u32.to_be_bytes());
    expected.extend_from_slice(&[7; 16]);
    expected.extend_from_slice(&3_u32.to_be_bytes());
    expected.extend_from_slice(&2_u32.to_be_bytes());
    expected.extend_from_slice(&0_u32.to_be_bytes());
    expected.extend_from_slice(&2_u32.to_be_bytes());
    assert_eq!(metadata, expected);
    assert_eq!(payload, b"abcd");
    assert_eq!(metadata.len(), 131);
    let packet = decode_packet(&[&header, &metadata, &payload], EnvelopeLimits::default()).unwrap();
    let decoded = decode_append(packet, DataLimits::default()).unwrap();
    assert_eq!(decoded.authority, append.authority);
    assert_eq!(decoded.partition, append.partition);
    assert_eq!(decoded.owner_epoch, append.owner_epoch);
    assert_eq!(decoded.key, append.key);
    assert_eq!(decoded.policy, append.policy);
    let record = decoded.records.iter().next().unwrap();
    assert_eq!(record.message_id, records[0].message_id);
    let decoded_parts: Vec<_> = record.parts.collect();
    assert_eq!(decoded_parts, parts);
    assert_eq!(decoded_parts[0].as_ptr(), payload.as_ptr());
    assert_eq!(decoded_parts[2].as_ptr(), payload[2..].as_ptr());
}

#[test]
fn append_metadata_reuses_owned_payload_without_changing_wire_or_capacity() {
    let payload = b"abcdefgh";
    let parts = [&payload[..2], &payload[2..2], &payload[2..]];
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([7; 16]),
        parts: &parts,
    }];
    let append = Append {
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
        policy: Policy::QuorumReplicatedPersisting,
        records: &records,
    };
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(131);
    let pointer = metadata.as_ptr();
    let header = encode_append_metadata(envelope(), append, &mut metadata, limits).unwrap();
    assert_eq!(metadata.as_ptr(), pointer);
    assert_eq!(metadata.capacity(), 131);
    let mut expected = Vec::with_capacity(131);
    let mut copied = Vec::with_capacity(payload.len());
    assert_eq!(
        encode_append(envelope(), append, &mut expected, &mut copied, limits).unwrap(),
        header
    );
    assert_eq!(metadata, expected);
    assert_eq!(copied, payload);
    let packet = decode_packet(&[&header, &metadata, payload], limits.envelope).unwrap();
    let decoded = decode_append(packet, limits).unwrap();
    let record = decoded.records.iter().next().unwrap();
    assert_eq!(record.message_id, records[0].message_id);
    assert_eq!(record.parts.collect::<Vec<_>>(), parts);
}

#[test]
fn compressed_group_validates_once_and_restores_clear_record_boundaries() {
    let large = vec![b'x'; 2048];
    let records = [
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([7; 16]),
            parts: &[b"", &large],
        },
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([8; 16]),
            parts: &[&large, b"end"],
        },
    ];
    let append = Append {
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
        records: &records,
    };
    let decoded = [b"".as_slice(), &large, &large, b"end"].concat();
    let encoded = lz4rip::block::compress(&decoded);
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(1024);
    encode_prepared_append_metadata(
        envelope(),
        append,
        &mut metadata,
        PayloadEncoding::Lz4,
        encoded.len(),
        limits,
    )
    .unwrap();
    assert_eq!(metadata[90], PayloadEncoding::Lz4 as u8);
    assert_eq!(
        u32::from_be_bytes(metadata[91..95].try_into().unwrap()) as usize,
        decoded.len()
    );
    let packet = Packet {
        envelope: envelope(),
        metadata: &metadata,
        payload: &encoded,
    };
    let validated = validate_append(packet, limits).unwrap();
    assert_eq!(validated.payload_encoding, PayloadEncoding::Lz4);
    assert_eq!(validated.encoded_payload, encoded);
    assert_eq!(validated.records.len(), records.len());
    assert_eq!(validated.records.payload_bytes(), decoded.len());
    assert!(decode_append(packet, limits).is_err());
    let mut scratch = Vec::new();
    let decoded_append = decode_append_with_scratch(packet, limits, &mut scratch).unwrap();
    assert_eq!(decoded_append.payload_encoding, PayloadEncoding::Lz4);
    for (actual, expected) in decoded_append.records.iter().zip(records) {
        assert_eq!(actual.message_id, expected.message_id);
        assert!(actual.parts.eq(expected.parts.iter().copied()));
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one malformed compressed APPEND fixture and its boundary mutations"
)]
fn compressed_group_rejects_bad_codec_lengths_syntax_and_trailing_input() {
    let body = vec![7; 4096];
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([7; 16]),
        parts: &[&body],
    }];
    let append = Append {
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
        records: &records,
    };
    let encoded = lz4rip::block::compress(&body);
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(1024);
    encode_prepared_append_metadata(
        envelope(),
        append,
        &mut metadata,
        PayloadEncoding::Lz4,
        encoded.len(),
        limits,
    )
    .unwrap();
    for claimed in [body.len() - 1, body.len() + 1] {
        let mut bad = metadata.clone();
        bad[91..95].copy_from_slice(&(claimed as u32).to_be_bytes());
        assert!(
            validate_append(
                Packet {
                    envelope: envelope(),
                    metadata: &bad,
                    payload: &encoded,
                },
                limits,
            )
            .is_err()
        );
        let mut scratch = Vec::new();
        assert!(
            decode_append_with_scratch(
                Packet {
                    envelope: envelope(),
                    metadata: &bad,
                    payload: &encoded,
                },
                limits,
                &mut scratch,
            )
            .is_err()
        );
    }
    let bounded = DataLimits {
        envelope: EnvelopeLimits {
            max_payload_bytes: body.len() - 1,
            ..limits.envelope
        },
        ..limits
    };
    let mut scratch = Vec::new();
    assert!(
        decode_append_with_scratch(
            Packet {
                envelope: envelope(),
                metadata: &metadata,
                payload: &encoded,
            },
            bounded,
            &mut scratch,
        )
        .is_err()
    );
    assert_eq!(scratch.capacity(), 0);
    let mut unknown = metadata.clone();
    unknown[90] = 2;
    let mut scratch = Vec::new();
    assert!(
        decode_append_with_scratch(
            Packet {
                envelope: envelope(),
                metadata: &unknown,
                payload: &encoded,
            },
            limits,
            &mut scratch,
        )
        .is_err()
    );
    for bad in [
        &encoded[..encoded.len() - 1],
        &[encoded.as_slice(), &[0]].concat(),
    ] {
        assert!(
            validate_append(
                Packet {
                    envelope: envelope(),
                    metadata: &metadata,
                    payload: bad,
                },
                limits,
            )
            .is_err()
        );
        let mut scratch = Vec::new();
        assert!(
            decode_append_with_scratch(
                Packet {
                    envelope: envelope(),
                    metadata: &metadata,
                    payload: bad,
                },
                limits,
                &mut scratch,
            )
            .is_err()
        );
    }
    assert!(
        encode_prepared_append_metadata(
            envelope(),
            append,
            &mut metadata,
            PayloadEncoding::Lz4,
            body.len(),
            limits,
        )
        .is_err()
    );
}
