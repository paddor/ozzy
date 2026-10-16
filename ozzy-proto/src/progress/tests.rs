use super::*;
use crate::GroupId;
use crate::{LinkSessionId, NodeId, RequestId, decode_packet, handshake};

#[test]
fn expired_history_detail_has_frozen_bytes_and_rejects_nonexpired_coordinates() {
    let expired = HistoryExpired {
        cursor: Offset::new(1),
        retained_from: Offset::new(3),
    };
    let expected = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3];
    assert_eq!(expired.encode().unwrap(), expected);
    assert_eq!(HistoryExpired::decode(&expected).unwrap(), expired);
    for length in 0..16 {
        assert!(HistoryExpired::decode(&expected[..length]).is_err());
    }
    assert!(HistoryExpired::decode(&[0; 17]).is_err());
    for cursor in [3, u64::MAX] {
        let invalid = HistoryExpired {
            cursor: Offset::new(cursor),
            ..expired
        };
        assert!(invalid.encode().is_err());
        let mut bytes = expected;
        bytes[..8].copy_from_slice(&cursor.to_be_bytes());
        assert!(HistoryExpired::decode(&bytes).is_err());
    }
}

fn envelope(opcode: Opcode) -> Envelope {
    Envelope {
        opcode,
        response: opcode == Opcode::ProgressCommitted,
        request_id: Some(RequestId::from_bytes([10; 16])),
        sender: NodeId::from_bytes([11; 16]),
        session: Some(LinkSessionId::from_bytes([12; 16])),
    }
}

fn request() -> Request {
    Request {
        authority: Authority {
            group_id: GroupId::from_bytes([1; 16]),
            config_epoch: 2,
            view: 3,
        },
        assignment: Assignment {
            partition: PartitionIncarnation::from_bytes([4; 16]),
            group: ConsumerGroupId::from_bytes([5; 16]),
            member: ConsumerMemberId::from_bytes([6; 16]),
            epoch: 7,
        },
        policy: Policy::QuorumDurable,
        cursor: Some(Offset::new(9)),
    }
}

fn packet<'a>(header: &'a [u8; ENVELOPE_BYTES], metadata: &'a [u8]) -> Packet<'a> {
    decode_packet(&[header, metadata, &[]], EnvelopeLimits::default()).unwrap()
}

#[test]
fn progress_request_has_independent_frozen_bytes_and_distinguishes_query_from_zero() {
    let mut metadata = Vec::with_capacity(128);
    let limits = EnvelopeLimits::default();
    let mut request = request();
    let header = encode_request(
        envelope(Opcode::ProgressCommit),
        request,
        &mut metadata,
        limits,
    )
    .unwrap();
    let expected = concat!(
        "01",
        "01010101010101010101010101010101",
        "0000000000000002",
        "0000000000000003",
        "04040404040404040404040404040404",
        "05050505050505050505050505050505",
        "06060606060606060606060606060606",
        "0000000000000007",
        "02",
        "01",
        "0000000000000009"
    );
    assert_eq!(metadata.len(), REQUEST_BYTES);
    assert_eq!(
        metadata.iter().fold(String::new(), |mut hex, byte| {
            use std::fmt::Write;
            write!(hex, "{byte:02x}").unwrap();
            hex
        }),
        expected
    );
    assert_eq!(
        decode_request(packet(&header, &metadata), limits),
        Ok(request)
    );
    for cursor in [None, Some(Offset::ZERO), Some(Offset::new(u64::MAX))] {
        request.cursor = cursor;
        let header = encode_request(
            envelope(Opcode::ProgressCommit),
            request,
            &mut metadata,
            limits,
        )
        .unwrap();
        assert_eq!(
            decode_request(packet(&header, &metadata), limits),
            Ok(request)
        );
    }
}

#[test]
fn confirmed_cursor_can_cover_expired_records_without_claiming_they_can_be_resumed() {
    let request = request();
    let confirmed = Confirmed {
        authority: request.authority,
        assignment: request.assignment,
        policy: request.policy,
        cursor: Offset::new(9),
        retained_from: Offset::new(10),
        end: Offset::new(11),
    };
    let limits = EnvelopeLimits::default();
    let mut metadata = Vec::with_capacity(128);
    let header = encode_confirmed(
        envelope(Opcode::ProgressCommitted),
        confirmed,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), CONFIRMED_BYTES);
    assert_eq!(
        decode_confirmed(packet(&header, &metadata), limits),
        Ok(confirmed)
    );
    assert_eq!(
        &metadata[90..],
        &[
            0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 11
        ]
    );
}

#[test]
fn malformed_progress_shape_direction_identity_and_policy_fail_closed() {
    let limits = EnvelopeLimits::default();
    let mut metadata = Vec::with_capacity(128);
    let header = encode_request(
        envelope(Opcode::ProgressCommit),
        request(),
        &mut metadata,
        limits,
    )
    .unwrap();
    for length in 0..metadata.len() {
        let truncated = Packet {
            envelope: envelope(Opcode::ProgressCommit),
            metadata: &metadata[..length],
            payload: &[],
        };
        assert!(decode_request(truncated, limits).is_err());
    }
    for (index, value) in [(0, 0), (89, 0), (90, 2)] {
        let mut invalid = metadata.clone();
        invalid[index] = value;
        assert!(decode_request(packet(&header, &invalid), limits).is_err());
    }
    let mut invalid = metadata.clone();
    invalid[90] = 0;
    assert!(decode_request(packet(&header, &invalid), limits).is_err());
    for range in [1..17, 17..25, 33..49, 49..65, 65..81, 81..89] {
        let mut invalid = metadata.clone();
        invalid[range].fill(0);
        assert!(decode_request(packet(&header, &invalid), limits).is_err());
    }
    let mut wrong = envelope(Opcode::ProgressCommit);
    wrong.response = true;
    assert!(encode_request(wrong, request(), &mut metadata, limits).is_err());
    let with_payload = Packet {
        envelope: envelope(Opcode::ProgressCommit),
        metadata: &metadata,
        payload: b"unexpected",
    };
    assert!(decode_request(with_payload, limits).is_err());
    let mut small = Vec::with_capacity(REQUEST_BYTES - 1);
    assert_eq!(
        encode_request(
            envelope(Opcode::ProgressCommit),
            request(),
            &mut small,
            limits
        ),
        Err(CodecError::Capacity)
    );
}

#[test]
fn progress_negotiation_requires_record_delivery_and_does_not_downgrade() {
    let processing =
        handshake::Parameters::processing(crate::data::DataLimits::default(), handshake::CONSUMER)
            .unwrap();
    let ordinary =
        handshake::Parameters::reader(crate::data::DataLimits::default(), handshake::OWNER)
            .unwrap();
    assert!(processing.select(ordinary).is_err());
    let mut owner = ordinary;
    owner.capabilities |= handshake::OWNER_PROGRESS;
    assert!(processing.select(owner).is_ok());
    owner.capabilities = handshake::OWNER_PROGRESS;
    owner.required_capabilities = 0;
    assert!(owner.validate().is_err());
}
