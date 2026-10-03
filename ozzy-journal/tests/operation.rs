use std::num::NonZeroU64;

#[path = "operation/borrowed.rs"]
mod borrowed;
#[path = "operation/validation.rs"]
mod validation;

use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Assign, Barrier, CreatePartition, OpenProducer,
    OperationBody, OperationCodecError, OperationKind, OperationLimits, PartitionPolicy,
    ProducerResultFloor, Progress, ProgressOwner, RetentionPolicy, Trim, append_operation_body,
    decode_operation_body, encode_operation_body,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, MessageId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};

const FIXTURES: [&str; 9] = [
    concat!(
        "10101010101010101010101010101010",
        "0000000673747265616d00000005746f706963",
        "000000070000000000000009",
        "01030000000000000000ea6000000000000f4240",
    ),
    concat!(
        "10101010101010101010101010101010",
        "20202020202020202020202020202020",
        "0100000000000000080000000000000009",
        "30303030303030303030303030303030",
    ),
    concat!(
        "00000001",
        "10101010101010101010101010101010",
        "0000000000000009",
        "20202020202020202020202020202020",
        "000000000000000900000000000000040000000000000028",
        "0000018bcfe5687b00000002",
        "4141414141414141414141414141414100000003",
        "000000020000000000000001",
        "424242424242424242424242424242420000000100000003",
        "61626378797a",
    ),
    concat!(
        "02",
        "50505050505050505050505050505050",
        "10101010101010101010101010101010",
        "010000000000000007010000000000000027",
        "0000000000000029",
        "31313131313131313131313131313131",
    ),
    concat!(
        "50505050505050505050505050505050",
        "10101010101010101010101010101010",
        "00000000000000070000000000000008",
        "0160606060606060606060606060606060",
        "32323232323232323232323232323232",
    ),
    concat!(
        "10101010101010101010101010101010",
        "00000000000000020000000000000004",
        "33333333333333333333333333333333",
    ),
    concat!(
        "10101010101010101010101010101010",
        "00000000000000010000000000000002",
        "010100000000000005265c000000000000000000",
        "34343434343434343434343434343434",
    ),
    "35353535353535353535353535353535",
    concat!(
        "10101010101010101010101010101010",
        "20202020202020202020202020202020",
        "000000000000000900000000000000020000000000000004",
        "36363636363636363636363636363636",
    ),
];

fn bodies() -> Vec<OperationBody<'static>> {
    let partition = PartitionIncarnation::from_bytes([0x10; 16]);
    let producer = ProducerId::from_bytes([0x20; 16]);
    vec![
        OperationBody::CreatePartition(CreatePartition {
            partition,
            stream: "stream",
            topic: "topic",
            partition_id: PartitionId::new(7),
            owner_epoch: OwnerEpoch::new(9),
            retention: RetentionPolicy {
                max_age_millis: NonZeroU64::new(60_000),
                max_bytes: NonZeroU64::new(1_000_000),
            },
        }),
        OperationBody::OpenProducer(OpenProducer {
            partition,
            producer_id: producer,
            expected_epoch: Some(ProducerEpoch::new(8)),
            new_epoch: ProducerEpoch::new(9),
            operation_id: OperationId::from_bytes([0x30; 16]),
        }),
        OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition,
                owner_epoch: OwnerEpoch::new(9),
                producer_id: producer,
                producer_epoch: ProducerEpoch::new(9),
                first_sequence: ProducerSequence::new(4),
                first_offset: Offset::new(40),
                append_timestamp_millis: 1_700_000_000_123,
                records: vec![
                    AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([0x41; 16]),
                        parts: vec![b"ab".as_slice(), b"".as_slice(), b"c".as_slice()].into(),
                    },
                    AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([0x42; 16]),
                        parts: vec![b"xyz".as_slice()].into(),
                    },
                ]
                .into(),
            }],
        }),
        OperationBody::Progress(Progress {
            owner: ProgressOwner::ConsumerGroup(ConsumerGroupId::from_bytes([0x50; 16])),
            partition,
            assignment_epoch: Some(7),
            expected_progress: Some(Offset::new(39)),
            new_progress: Offset::new(41),
            operation_id: OperationId::from_bytes([0x31; 16]),
        }),
        OperationBody::Assign(Assign {
            consumer_group_id: ConsumerGroupId::from_bytes([0x50; 16]),
            partition,
            expected_assignment_epoch: 7,
            new_assignment_epoch: 8,
            new_member: Some(ConsumerMemberId::from_bytes([0x60; 16])),
            operation_id: OperationId::from_bytes([0x32; 16]),
        }),
        OperationBody::Trim(Trim {
            partition,
            expected_floor: Offset::new(2),
            new_floor: Offset::new(4),
            operation_id: OperationId::from_bytes([0x33; 16]),
        }),
        OperationBody::PartitionPolicy(PartitionPolicy {
            partition,
            expected_revision: 1,
            new_revision: 2,
            retention: RetentionPolicy {
                max_age_millis: NonZeroU64::new(86_400_000),
                max_bytes: None,
            },
            operation_id: OperationId::from_bytes([0x34; 16]),
        }),
        OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([0x35; 16]),
        }),
        OperationBody::ProducerResultFloor(ProducerResultFloor {
            partition,
            producer_id: producer,
            producer_epoch: ProducerEpoch::new(9),
            expected_floor: ProducerSequence::new(2),
            new_floor: ProducerSequence::new(4),
            operation_id: OperationId::from_bytes([0x36; 16]),
        }),
    ]
}

fn unhex(input: &str) -> Vec<u8> {
    assert!(input.len().is_multiple_of(2));
    input
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).unwrap() as u8;
            let low = (pair[1] as char).to_digit(16).unwrap() as u8;
            high << 4 | low
        })
        .collect()
}

#[test]
fn every_operation_matches_frozen_bytes_and_round_trips() {
    let bodies = bodies();
    for (body, fixture) in bodies.iter().zip(FIXTURES) {
        let fixture = unhex(fixture);
        assert_eq!(
            encode_operation_body(body, OperationLimits::default()).unwrap(),
            fixture,
            "{:?} encoding changed",
            body.kind()
        );
        let decoded =
            decode_operation_body(body.kind(), &fixture, OperationLimits::default()).unwrap();
        assert_eq!(decoded.kind(), body.kind());
        assert_eq!(
            encode_operation_body(&decoded, OperationLimits::default()).unwrap(),
            fixture
        );
        drop(decoded);
        for length in 0..fixture.len() {
            assert!(
                decode_operation_body(body.kind(), &fixture[..length], OperationLimits::default(),)
                    .is_err(),
                "{:?} accepted {length} truncated bytes",
                body.kind()
            );
        }
    }
}

#[test]
fn operation_bodies_append_to_a_reusable_arena() {
    let bodies = bodies();
    let mut arena = Vec::with_capacity(4_096);
    arena.extend_from_slice(b"prefix");

    let first = append_operation_body(&mut arena, &bodies[0], OperationLimits::default()).unwrap();
    let second = append_operation_body(&mut arena, &bodies[1], OperationLimits::default()).unwrap();

    assert_eq!(&arena[..first.start], b"prefix");
    assert_eq!(&arena[first], unhex(FIXTURES[0]));
    assert_eq!(&arena[second], unhex(FIXTURES[1]));

    let stable_len = arena.len();
    let limits = OperationLimits {
        max_records: 1,
        ..OperationLimits::default()
    };
    assert!(append_operation_body(&mut arena, &bodies[2], limits).is_err());
    assert_eq!(arena.len(), stable_len);
}

#[test]
fn append_limits_apply_to_encoding_and_decoding() {
    let body = bodies().remove(2);
    let fixture = unhex(FIXTURES[2]);
    let limits = OperationLimits {
        max_records: 1,
        ..OperationLimits::default()
    };
    assert!(matches!(
        encode_operation_body(&body, limits),
        Err(OperationCodecError::LimitExceeded {
            kind: "append record count",
            ..
        })
    ));
    assert!(matches!(
        decode_operation_body(OperationKind::Append, &fixture, limits),
        Err(OperationCodecError::LimitExceeded {
            kind: "append record count",
            ..
        })
    ));
}

#[test]
fn progress_owner_and_assignment_epoch_must_agree() {
    let progress = OperationBody::Progress(Progress {
        owner: ProgressOwner::Subscription(SubscriptionId::from_bytes([1; 16])),
        partition: PartitionIncarnation::from_bytes([2; 16]),
        assignment_epoch: Some(1),
        expected_progress: None,
        new_progress: Offset::ZERO,
        operation_id: OperationId::from_bytes([3; 16]),
    });
    assert_eq!(
        encode_operation_body(&progress, OperationLimits::default()),
        Err(OperationCodecError::UnexpectedAssignmentEpoch)
    );
}

#[test]
fn body_decoder_rejects_trailing_bytes_and_unknown_kinds() {
    let mut fixture = unhex(FIXTURES[7]);
    fixture.push(0);
    assert_eq!(
        decode_operation_body(OperationKind::Barrier, &fixture, OperationLimits::default()),
        Err(OperationCodecError::TrailingBytes(1))
    );
    assert_eq!(
        OperationKind::try_from(10),
        Err(OperationCodecError::UnsupportedOperationKind(10))
    );
}
