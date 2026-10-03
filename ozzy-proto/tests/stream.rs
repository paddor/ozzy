use ozzy_proto::append::stream::{self, Confirmed};
use ozzy_proto::append::{AppendKey, Authority, CodecError, Policy};
use ozzy_proto::{
    Envelope, EnvelopeLimits, GroupId, LinkSessionId, NodeId, Opcode, Packet, PartitionIncarnation,
    ProducerId, RequestId, decode_packet,
};

fn envelope() -> Envelope {
    Envelope {
        opcode: Opcode::Appended,
        response: true,
        request_id: Some(RequestId::from_bytes([11; 16])),
        sender: NodeId::from_bytes([12; 16]),
        session: Some(LinkSessionId::from_bytes([13; 16])),
    }
}

fn confirmed() -> Confirmed {
    Confirmed {
        authority: Authority {
            group_id: GroupId::from_bytes([1; 16]),
            config_epoch: 2,
            view: 3,
        },
        partition: PartitionIncarnation::from_bytes([4; 16]),
        owner_epoch: 5,
        key: AppendKey {
            producer_id: ProducerId::from_bytes([6; 16]),
            producer_epoch: 7,
            first_sequence: 8,
        },
        end_sequence: 2008,
        first_offset: 10,
        policy: Policy::QuorumReplicatedPersisting,
    }
}

#[test]
fn confirmed_range_has_fixed_bytes_independent_of_append_boundaries() {
    let receipt = confirmed();
    let limits = EnvelopeLimits::default();
    let mut metadata = Vec::with_capacity(106);
    let header = stream::encode_confirmed(envelope(), receipt, &mut metadata, limits).unwrap();
    let mut golden = vec![1];
    golden.extend([1; 16]);
    golden.extend(2_u64.to_be_bytes());
    golden.extend(3_u64.to_be_bytes());
    golden.extend([4; 16]);
    golden.extend(5_u64.to_be_bytes());
    golden.extend([6; 16]);
    golden.extend(7_u64.to_be_bytes());
    golden.extend(8_u64.to_be_bytes());
    golden.extend(2008_u64.to_be_bytes());
    golden.extend(10_u64.to_be_bytes());
    golden.push(6);
    assert_eq!(metadata, golden);
    assert_eq!(metadata.len(), 106);
    let packet = decode_packet(&[&header, &golden, &[]], limits).unwrap();
    assert_eq!(stream::decode_confirmed(packet, limits).unwrap(), receipt);
}

#[test]
fn invalid_confirmation_ranges_are_rejected_before_replacing_metadata() {
    let limits = EnvelopeLimits::default();
    let valid = confirmed();
    let mut metadata = Vec::with_capacity(106);
    stream::encode_confirmed(envelope(), valid, &mut metadata, limits).unwrap();
    let original = metadata.clone();
    let pointer = metadata.as_ptr();
    for (end_sequence, first_offset) in [
        (valid.key.first_sequence, 0),
        (valid.key.first_sequence - 1, 0),
        (valid.end_sequence, u64::MAX - 1999),
    ] {
        let invalid = Confirmed {
            end_sequence,
            first_offset,
            ..valid
        };
        assert!(stream::encode_confirmed(envelope(), invalid, &mut metadata, limits).is_err());
        assert_eq!(metadata, original);
        assert_eq!(metadata.as_ptr(), pointer);
        let mut malformed = original.clone();
        malformed[89..97].copy_from_slice(&end_sequence.to_be_bytes());
        malformed[97..105].copy_from_slice(&first_offset.to_be_bytes());
        assert!(
            stream::decode_confirmed(
                Packet {
                    envelope: envelope(),
                    metadata: &malformed,
                    payload: &[],
                },
                limits,
            )
            .is_err()
        );
    }
    let maximum = Confirmed {
        key: AppendKey {
            first_sequence: u64::MAX - 1,
            ..valid.key
        },
        end_sequence: u64::MAX,
        first_offset: u64::MAX - 1,
        ..valid
    };
    let header = stream::encode_confirmed(envelope(), maximum, &mut metadata, limits).unwrap();
    let packet = decode_packet(&[&header, &metadata, &[]], limits).unwrap();
    assert_eq!(stream::decode_confirmed(packet, limits).unwrap(), maximum);
    let mut small = vec![99];
    assert_eq!(
        stream::encode_confirmed(envelope(), valid, &mut small, limits),
        Err(CodecError::Capacity)
    );
    assert_eq!(small, [99]);
}

#[test]
fn malformed_confirmations_reject_truncation_trailing_bytes_payload_and_policy() {
    let limits = EnvelopeLimits::default();
    let mut metadata = Vec::with_capacity(106);
    stream::encode_confirmed(envelope(), confirmed(), &mut metadata, limits).unwrap();
    for end in 0..metadata.len() {
        assert!(
            stream::decode_confirmed(
                Packet {
                    envelope: envelope(),
                    metadata: &metadata[..end],
                    payload: &[],
                },
                limits,
            )
            .is_err(),
            "truncation {end}"
        );
    }
    for policy in [0, 4, 7, 255] {
        let mut invalid = metadata.clone();
        invalid[105] = policy;
        assert_eq!(
            stream::decode_confirmed(
                Packet {
                    envelope: envelope(),
                    metadata: &invalid,
                    payload: &[],
                },
                limits,
            ),
            Err(CodecError::Policy)
        );
    }
    for payload in [b"".as_slice(), b"payload".as_slice()] {
        let mut invalid = metadata.clone();
        if payload.is_empty() {
            invalid.push(0);
        }
        assert_eq!(
            stream::decode_confirmed(
                Packet {
                    envelope: envelope(),
                    metadata: &invalid,
                    payload,
                },
                limits,
            ),
            Err(CodecError::Length)
        );
    }
}

#[test]
fn confirmations_require_nonzero_identities_and_correlated_response_sessions() {
    let limits = EnvelopeLimits::default();
    let valid = confirmed();
    let mut metadata = Vec::with_capacity(106);
    stream::encode_confirmed(envelope(), valid, &mut metadata, limits).unwrap();
    let original = metadata.clone();
    let mut invalid = [valid; 6];
    invalid[0].authority.group_id = GroupId::from_bytes([0; 16]);
    invalid[1].authority.config_epoch = 0;
    invalid[2].partition = PartitionIncarnation::from_bytes([0; 16]);
    invalid[3].owner_epoch = 0;
    invalid[4].key.producer_id = ProducerId::from_bytes([0; 16]);
    invalid[5].key.producer_epoch = 0;
    for value in invalid {
        assert_eq!(
            stream::encode_confirmed(envelope(), value, &mut metadata, limits),
            Err(CodecError::Identity)
        );
        assert_eq!(metadata, original);
    }
    for range in [1..17, 17..25, 33..49, 49..57, 57..73, 73..81] {
        let mut invalid = original.clone();
        invalid[range].fill(0);
        assert_eq!(
            stream::decode_confirmed(
                Packet {
                    envelope: envelope(),
                    metadata: &invalid,
                    payload: &[],
                },
                limits,
            ),
            Err(CodecError::Identity)
        );
    }
    for invalid in [
        Envelope {
            opcode: Opcode::Append,
            ..envelope()
        },
        Envelope {
            response: false,
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
            session: Some(LinkSessionId::from_bytes([0; 16])),
            ..envelope()
        },
        Envelope {
            request_id: Some(RequestId::from_bytes([0; 16])),
            ..envelope()
        },
        Envelope {
            sender: NodeId::from_bytes([0; 16]),
            ..envelope()
        },
    ] {
        assert!(stream::encode_confirmed(invalid, valid, &mut metadata, limits).is_err());
        assert_eq!(metadata, original);
        assert!(
            stream::decode_confirmed(
                Packet {
                    envelope: invalid,
                    metadata: &original,
                    payload: &[],
                },
                limits,
            )
            .is_err()
        );
    }
}
