use std::num::NonZeroU64;

use ozzy_core::state::{
    CanonicalState, MemoryIdentityIndex, StateLimits, StateSnapshotError, StateSnapshotLimits,
    canonical_state_schema_digest,
};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Assign, CreatePartition, OpenProducer, OperationBody,
    ProducerResultFloor, Progress, ProgressOwner, RetentionPolicy, Trim,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, MessageId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};

fn apply(
    state: &mut CanonicalState,
    identities: &mut MemoryIdentityIndex,
    body: &OperationBody<'_>,
) {
    let op_number = state.revision() + 1;
    let committed = state.clone();
    let plan = state
        .prepare(op_number, body, identities, &committed)
        .unwrap();
    state.apply(plan, identities).unwrap();
}

#[expect(clippy::too_many_lines, reason = "linear canonical-state fixture")]
fn populated_state() -> CanonicalState {
    let partition = PartitionIncarnation::from_bytes([0x11; 16]);
    let producer = ProducerId::from_bytes([0x12; 16]);
    let consumer_group = ConsumerGroupId::from_bytes([0x13; 16]);
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(128);
    apply(
        &mut state,
        &mut identities,
        &OperationBody::CreatePartition(CreatePartition {
            partition,
            stream: "events",
            topic: "orders",
            partition_id: PartitionId::new(7),
            owner_epoch: OwnerEpoch::INITIAL,
            retention: RetentionPolicy {
                max_age_millis: NonZeroU64::new(3_600_000),
                max_bytes: NonZeroU64::new(1_000_000),
            },
        }),
    );
    apply(
        &mut state,
        &mut identities,
        &OperationBody::OpenProducer(OpenProducer {
            partition,
            producer_id: producer,
            expected_epoch: None,
            new_epoch: ProducerEpoch::INITIAL,
            operation_id: OperationId::from_bytes([0x20; 16]),
        }),
    );
    apply(
        &mut state,
        &mut identities,
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition,
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: producer,
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::ZERO,
                first_offset: Offset::ZERO,
                append_timestamp_millis: 123,
                records: vec![
                    AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([0x30; 16]),
                        parts: vec![b"a".as_slice(), b"bc".as_slice()].into(),
                    },
                    AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([0x31; 16]),
                        parts: vec![b"def".as_slice()].into(),
                    },
                ]
                .into(),
            }],
        }),
    );
    apply(
        &mut state,
        &mut identities,
        &OperationBody::Assign(Assign {
            consumer_group_id: consumer_group,
            partition,
            expected_assignment_epoch: 0,
            new_assignment_epoch: 1,
            new_member: Some(ConsumerMemberId::from_bytes([0x40; 16])),
            operation_id: OperationId::from_bytes([0x41; 16]),
        }),
    );
    apply(
        &mut state,
        &mut identities,
        &OperationBody::Progress(Progress {
            owner: ProgressOwner::Subscription(SubscriptionId::from_bytes([0x50; 16])),
            partition,
            assignment_epoch: None,
            expected_progress: None,
            new_progress: Offset::ZERO,
            operation_id: OperationId::from_bytes([0x51; 16]),
        }),
    );
    apply(
        &mut state,
        &mut identities,
        &OperationBody::Progress(Progress {
            owner: ProgressOwner::ConsumerGroup(consumer_group),
            partition,
            assignment_epoch: Some(1),
            expected_progress: None,
            new_progress: Offset::new(1),
            operation_id: OperationId::from_bytes([0x52; 16]),
        }),
    );
    apply(
        &mut state,
        &mut identities,
        &OperationBody::ProducerResultFloor(ProducerResultFloor {
            partition,
            producer_id: producer,
            producer_epoch: ProducerEpoch::INITIAL,
            expected_floor: ProducerSequence::ZERO,
            new_floor: ProducerSequence::new(1),
            operation_id: OperationId::from_bytes([0x54; 16]),
        }),
    );
    apply(
        &mut state,
        &mut identities,
        &OperationBody::Trim(Trim {
            partition,
            expected_floor: Offset::ZERO,
            new_floor: Offset::new(1),
            operation_id: OperationId::from_bytes([0x53; 16]),
        }),
    );
    state
}

#[test]
fn canonical_state_snapshot_is_deterministic_and_round_trips() {
    let state = populated_state();
    let limits = StateSnapshotLimits::default();
    let first = state.encode_snapshot(limits).unwrap();
    let second = state.encode_snapshot(limits).unwrap();
    assert_eq!(first, second);
    assert_ne!(canonical_state_schema_digest().as_bytes(), &[0_u8; 32]);
    assert_eq!(
        CanonicalState::decode_snapshot(&first, StateLimits::default(), limits).unwrap(),
        state
    );
}

#[test]
fn snapshot_rejects_every_truncation_corruption_and_small_limit() {
    let state = populated_state();
    let encoded = state
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    for length in 0..encoded.len() {
        assert!(
            CanonicalState::decode_snapshot(
                &encoded[..length],
                StateLimits::default(),
                StateSnapshotLimits::default(),
            )
            .is_err()
        );
    }
    let mut corrupt = encoded.clone();
    corrupt[24] ^= 1;
    assert!(matches!(
        CanonicalState::decode_snapshot(
            &corrupt,
            StateLimits::default(),
            StateSnapshotLimits::default(),
        ),
        Err(StateSnapshotError::DigestMismatch)
    ));
    assert!(matches!(
        CanonicalState::decode_snapshot(
            &encoded,
            StateLimits {
                max_partitions: 0,
                ..StateLimits::default()
            },
            StateSnapshotLimits::default(),
        ),
        Err(StateSnapshotError::LimitExceeded {
            kind: "partitions",
            ..
        })
    ));
}
