use std::num::NonZeroU64;

use ozzy_core::state::{
    CanonicalImages, CanonicalImagesError, CanonicalState, IdentityIndexError, MemoryIdentityIndex,
    StateError, StateLimits,
};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Assign, Barrier, CreatePartition, OpenProducer,
    OperationBody, PartitionPolicy, ProducerResultFloor, Progress, ProgressOwner, RetentionPolicy,
    Trim,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, MessageId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};

#[path = "state/shared.rs"]
mod shared;

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([0x10; 16])
}

fn producer() -> ProducerId {
    ProducerId::from_bytes([0x20; 16])
}

fn another_partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([0x11; 16])
}

fn another_producer() -> ProducerId {
    ProducerId::from_bytes([0x21; 16])
}

fn operation(byte: u8) -> OperationId {
    OperationId::from_bytes([byte; 16])
}

fn create() -> OperationBody<'static> {
    OperationBody::CreatePartition(CreatePartition {
        partition: partition(),
        stream: "stream",
        topic: "topic",
        partition_id: PartitionId::new(7),
        owner_epoch: OwnerEpoch::new(9),
        retention: RetentionPolicy::default(),
    })
}

fn open() -> OperationBody<'static> {
    OperationBody::OpenProducer(OpenProducer {
        partition: partition(),
        producer_id: producer(),
        expected_epoch: None,
        new_epoch: ProducerEpoch::new(3),
        operation_id: operation(0x30),
    })
}

fn create_another() -> OperationBody<'static> {
    OperationBody::CreatePartition(CreatePartition {
        partition: another_partition(),
        stream: "stream",
        topic: "topic",
        partition_id: PartitionId::new(8),
        owner_epoch: OwnerEpoch::new(10),
        retention: RetentionPolicy::default(),
    })
}

fn open_another() -> OperationBody<'static> {
    OperationBody::OpenProducer(OpenProducer {
        partition: another_partition(),
        producer_id: another_producer(),
        expected_epoch: None,
        new_epoch: ProducerEpoch::new(4),
        operation_id: operation(0x36),
    })
}

fn append(first_offset: u64, message: u8) -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            first_sequence: ProducerSequence::new(first_offset),
            first_offset: Offset::new(first_offset),
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([message; 16]),
                parts: vec![b"payload".as_slice()].into(),
            }]
            .into(),
        }],
    })
}

#[test]
fn compact_append_admission_matches_materialized_validation_and_state() {
    use ozzy_journal::operation::{
        OperationKind, OperationLimits, decode_append_summary, decode_operation_body,
        encode_operation_body,
    };
    let limits = OperationLimits::default();
    let mut initial = CanonicalImages::new(StateLimits::default(), 64, 2);
    initial.admit(1, &create()).unwrap();
    initial.admit(2, &open()).unwrap();
    initial.commit_through(2).unwrap();
    let bytes = encode_operation_body(&append(0, 1), limits).unwrap();
    let compare = |input: &[u8]| {
        let Ok(body) = decode_operation_body(OperationKind::Append, input, limits) else {
            return;
        };
        let summary = decode_append_summary(input, limits).unwrap();
        let mut materialized = initial.clone();
        let mut compact = initial.clone();
        let expected = materialized.admit(3, &body);
        assert_eq!(compact.admit_append_summary(3, &summary), expected);
        assert_eq!(compact, materialized);
        if expected.is_ok() {
            materialized.commit_through(3).unwrap();
            compact.commit_through(3).unwrap();
            assert_eq!(compact, materialized);
        } else {
            assert_eq!(compact, initial, "failed admission mutated state");
        }
    };
    compare(&bytes);
    for index in 0..bytes.len() {
        for value in [0, 1, 0x7f, 0xff] {
            let mut changed = bytes.clone();
            changed[index] = value;
            compare(&changed);
        }
    }
    // Exercise zero identities, duplicate partitions, and the spilled (>4) path.
    let mut zero_id = bytes.clone();
    zero_id[80..96].fill(0);
    compare(&zero_id);
    for count in [2_u32, 4, 5, 8] {
        let mut duplicate = count.to_be_bytes().to_vec();
        for _ in 0..count {
            duplicate.extend_from_slice(&bytes[4..]);
        }
        compare(&duplicate);
    }
}

#[test]
fn compact_many_partition_append_preserves_set_based_duplicate_checks() {
    use ozzy_journal::operation::{OperationLimits, decode_append_summary, encode_operation_body};
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(64);
    let mut batches = Vec::new();
    for index in 0..6_u8 {
        let id = PartitionIncarnation::from_bytes([index + 1; 16]);
        let OperationBody::CreatePartition(mut create) = create() else {
            unreachable!()
        };
        create.partition = id;
        create.partition_id = PartitionId::new(u32::from(index));
        apply(
            &mut state,
            &mut identities,
            u64::from(index) * 2 + 1,
            &OperationBody::CreatePartition(create),
        );
        let OperationBody::OpenProducer(mut open) = open() else {
            unreachable!()
        };
        open.partition = id;
        open.operation_id = operation(index + 1);
        apply(
            &mut state,
            &mut identities,
            u64::from(index) * 2 + 2,
            &OperationBody::OpenProducer(open),
        );
        let OperationBody::Append(mut value) = append(0, 1) else {
            unreachable!()
        };
        value.batches[0].partition = id;
        batches.push(value.batches.remove(0));
    }
    for duplicate in [false, true] {
        let mut batches = batches.clone();
        if duplicate {
            batches[5].partition = batches[0].partition;
        }
        let body = OperationBody::Append(Append { batches });
        let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
        let summary = decode_append_summary(&bytes, OperationLimits::default()).unwrap();
        let result = state.prepare(13, &body, &identities, &state);
        assert_eq!(state.prepare_append_summary(13, &summary), result);
        if duplicate {
            assert_eq!(result, Err(StateError::DuplicateAppendPartition));
        } else {
            assert!(result.is_ok());
        }
    }
}

fn apply(
    state: &mut CanonicalState,
    index: &mut MemoryIdentityIndex,
    op_number: u64,
    body: &OperationBody<'_>,
) {
    let committed = state.clone();
    let plan = state.prepare(op_number, body, index, &committed).unwrap();
    state.apply(plan, index).unwrap();
}

fn apply_control_operations(
    state: &mut CanonicalState,
    index: &mut MemoryIdentityIndex,
) -> (ConsumerGroupId, RetentionPolicy) {
    let group = ConsumerGroupId::from_bytes([0x50; 16]);
    apply(
        state,
        index,
        4,
        &OperationBody::ProducerResultFloor(ProducerResultFloor {
            partition: partition(),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            expected_floor: ProducerSequence::ZERO,
            new_floor: ProducerSequence::new(1),
            operation_id: operation(0x37),
        }),
    );
    apply(
        state,
        index,
        5,
        &OperationBody::Assign(Assign {
            consumer_group_id: group,
            partition: partition(),
            expected_assignment_epoch: 0,
            new_assignment_epoch: 1,
            new_member: Some(ConsumerMemberId::from_bytes([0x60; 16])),
            operation_id: operation(0x31),
        }),
    );
    apply(
        state,
        index,
        6,
        &OperationBody::Progress(Progress {
            owner: ProgressOwner::ConsumerGroup(group),
            partition: partition(),
            assignment_epoch: Some(1),
            expected_progress: None,
            new_progress: Offset::new(1),
            operation_id: operation(0x32),
        }),
    );
    apply(
        state,
        index,
        7,
        &OperationBody::Trim(Trim {
            partition: partition(),
            expected_floor: Offset::ZERO,
            new_floor: Offset::new(1),
            operation_id: operation(0x33),
        }),
    );
    let retention = RetentionPolicy {
        max_age_millis: NonZeroU64::new(60_000),
        max_bytes: None,
    };
    apply(
        state,
        index,
        8,
        &OperationBody::PartitionPolicy(PartitionPolicy {
            partition: partition(),
            expected_revision: 1,
            new_revision: 2,
            retention,
            operation_id: operation(0x34),
        }),
    );
    apply(
        state,
        index,
        9,
        &OperationBody::Barrier(Barrier {
            operation_id: operation(0x35),
        }),
    );
    (group, retention)
}

#[test]
fn all_canonical_operations_apply_deterministically() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(32);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());

    let append = OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            first_sequence: ProducerSequence::new(0),
            first_offset: Offset::ZERO,
            append_timestamp_millis: 123,
            records: vec![
                AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([0x40; 16]),
                    parts: vec![b"one".as_slice()].into(),
                },
                AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([0x41; 16]),
                    parts: vec![b"two".as_slice(), b"".as_slice()].into(),
                },
            ]
            .into(),
        }],
    });
    apply(&mut state, &mut index, 3, &append);

    let (group, retention) = apply_control_operations(&mut state, &mut index);

    let partition_state = state.partition(partition()).unwrap();
    let writer = partition_state.producer(producer()).unwrap();
    assert_eq!(writer.producer_epoch, ProducerEpoch::new(3));
    assert_eq!(writer.next_producer_sequence, ProducerSequence::new(2));
    assert_eq!(partition_state.next_offset, Offset::new(2));
    assert_eq!(
        writer.result_offset(ProducerSequence::new(1)),
        Some(Offset::new(1))
    );
    assert_eq!(writer.producer_result_floor, ProducerSequence::new(1));
    assert_eq!(
        partition_state.producer_result_offset_floor(),
        Some(Offset::new(1))
    );
    assert_eq!(partition_state.retained_from, Offset::new(1));
    assert_eq!(partition_state.policy_revision, 2);
    assert_eq!(partition_state.retention, retention);
    assert_eq!(state.assignment(group, partition()).unwrap().epoch, 1);
    assert_eq!(
        state.progress(ProgressOwner::ConsumerGroup(group), partition()),
        Some(Offset::new(1))
    );
    assert_eq!(state.revision(), 9);
    assert_eq!(index.len(), 7);
}

#[test]
fn validation_and_identity_capacity_fail_atomically() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(1);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());

    let before = state.clone();
    let index_before = index.clone();
    assert!(matches!(
        state.prepare(3, &append(1, 0x40), &index, &before),
        Err(StateError::ProducerSequenceMismatch)
    ));
    assert_eq!(state, before);
    assert_eq!(index, index_before);

    let plan = state
        .prepare(
            3,
            &OperationBody::Barrier(Barrier {
                operation_id: operation(0x60),
            }),
            &index,
            &before,
        )
        .unwrap();
    assert!(matches!(
        state.apply(plan, &mut index),
        Err(StateError::IdentityIndex(IdentityIndexError::Capacity))
    ));
    assert_eq!(state, before);
    assert_eq!(index, index_before);
}

#[test]
fn producer_result_floor_gates_trim_and_resets_at_session_fence() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(32);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());
    apply(&mut state, &mut index, 3, &append(0, 0x40));

    let trim = OperationBody::Trim(Trim {
        partition: partition(),
        expected_floor: Offset::ZERO,
        new_floor: Offset::new(1),
        operation_id: operation(0x38),
    });
    assert_eq!(
        state.prepare(4, &trim, &index, &state).unwrap_err(),
        StateError::TrimBeyondProducerResults
    );
    apply(
        &mut state,
        &mut index,
        4,
        &OperationBody::ProducerResultFloor(ProducerResultFloor {
            partition: partition(),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            expected_floor: ProducerSequence::ZERO,
            new_floor: ProducerSequence::new(1),
            operation_id: operation(0x39),
        }),
    );
    apply(&mut state, &mut index, 5, &trim);

    apply(
        &mut state,
        &mut index,
        6,
        &OperationBody::OpenProducer(OpenProducer {
            partition: partition(),
            producer_id: producer(),
            expected_epoch: Some(ProducerEpoch::new(3)),
            new_epoch: ProducerEpoch::new(4),
            operation_id: operation(0x3a),
        }),
    );
    let partition = state.partition(partition()).unwrap();
    let writer = partition.producer(producer()).unwrap();
    assert_eq!(writer.result_offset(ProducerSequence::ZERO), None);
    assert_eq!(writer.producer_result_floor, ProducerSequence::ZERO);
    assert_eq!(
        partition.producer_result_offset_floor(),
        Some(Offset::new(1))
    );
}

#[test]
fn stale_plan_and_duplicate_identity_never_mutate_state() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(16);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());
    let committed = state.clone();
    let stale_plan = state
        .prepare(3, &append(0, 0x40), &index, &committed)
        .unwrap();
    apply(
        &mut state,
        &mut index,
        3,
        &OperationBody::Barrier(Barrier {
            operation_id: operation(0x31),
        }),
    );
    let before = state.clone();
    let index_before = index.clone();
    assert_eq!(
        state.apply(stale_plan, &mut index),
        Err(StateError::StalePlan)
    );
    assert_eq!(state, before);
    assert_eq!(index, index_before);

    assert!(matches!(
        state.prepare(
            4,
            &OperationBody::Barrier(Barrier {
                operation_id: operation(0x31),
            }),
            &index,
            &before,
        ),
        Err(StateError::IdentityConflict)
    ));
}

#[test]
fn one_plan_advances_speculative_then_committed_state() {
    let mut committed = CanonicalState::new(StateLimits::default());
    let mut committed_index = MemoryIdentityIndex::new(16);
    apply(&mut committed, &mut committed_index, 1, &create());
    apply(&mut committed, &mut committed_index, 2, &open());
    let mut speculative = committed.clone();
    let mut speculative_index = committed_index.clone();

    let plan = speculative
        .prepare(3, &append(0, 0x40), &speculative_index, &committed)
        .unwrap();
    speculative
        .apply(plan.clone(), &mut speculative_index)
        .unwrap();

    assert_eq!(committed.revision(), 2);
    assert_eq!(speculative.revision(), 3);
    committed.apply(plan, &mut committed_index).unwrap();
    assert_eq!(committed, speculative);
    assert_eq!(committed_index, speculative_index);
}

#[test]
fn canonical_group_preparation_is_atomic_and_revision_bound() {
    let operations = vec![(1, create()), (2, open()), (3, append(0, 0x40))];
    let mut images = CanonicalImages::new(StateLimits::default(), 16, 16);
    let first = images.prepare_group(&operations).unwrap();
    let stale = images.prepare_group(&operations).unwrap();

    assert_eq!(images.committed().revision(), 0);
    assert_eq!(images.speculative().revision(), 0);
    assert_eq!(images.pending_len(), 0);

    images.install_prepared_group(first).unwrap();
    assert_eq!(images.committed().revision(), 0);
    assert_eq!(images.speculative().revision(), 3);
    assert_eq!(images.pending_len(), 3);
    assert_eq!(
        images.install_prepared_group(stale).unwrap_err(),
        CanonicalImagesError::StalePreparedGroup
    );

    images.commit_through(3).unwrap();
    assert_eq!(images.committed(), images.speculative());
    assert_eq!(images.pending_len(), 0);
}

#[test]
fn identity_index_replacement_requires_one_exact_settled_revision() {
    let mut images = CanonicalImages::new(StateLimits::default(), 16, 16);
    images.admit(1, &create()).unwrap();
    let identities = images.speculative_identities().clone();
    let before = images.clone();
    assert_eq!(
        images.replace_settled_identity_index(1, identities.clone()),
        Err(CanonicalImagesError::RecoveredStateMismatch)
    );
    assert_eq!(images, before);
    images.commit_through(1).unwrap();
    let before = images.clone();
    assert_eq!(
        images.replace_settled_identity_index(0, identities.clone()),
        Err(CanonicalImagesError::RecoveredStateMismatch)
    );
    assert_eq!(images, before);
    images
        .replace_settled_identity_index(1, identities)
        .unwrap();
    assert_eq!(images, before);
}

#[test]
fn compact_group_matches_typed_plans_and_rejects_bad_suffix_atomically() {
    use ozzy_journal::operation::{OperationLimits, decode_append_summary, encode_operation_body};
    let mut initial = CanonicalImages::new(StateLimits::default(), 16, 4);
    initial.admit(1, &create()).unwrap();
    initial.admit(2, &open()).unwrap();
    initial.commit_through(2).unwrap();
    for suffix in [append(1, 2), append(3, 2), append(1, 0)] {
        let operations = [(3, append(0, 1)), (4, suffix)];
        let summaries: Vec<_> = operations
            .iter()
            .map(|(number, body)| {
                let bytes = encode_operation_body(body, OperationLimits::default()).unwrap();
                (
                    *number,
                    decode_append_summary(&bytes, OperationLimits::default()).unwrap(),
                )
            })
            .collect();
        let mut compact = initial.clone();
        let mut typed = initial.clone();
        let result = compact.prepare_append_group(&summaries);
        assert_eq!(compact, initial);
        match (result, typed.prepare_group(&operations)) {
            (Ok(plan), Ok(expected)) => {
                let stale = compact.prepare_append_group(&summaries).unwrap();
                compact.install_prepared_group(plan).unwrap();
                typed.install_prepared_group(expected).unwrap();
                assert_eq!(compact, typed);
                assert_eq!(
                    compact.install_prepared_group(stale).unwrap_err(),
                    CanonicalImagesError::StalePreparedGroup
                );
                compact.commit_through(4).unwrap();
                typed.commit_through(4).unwrap();
                assert_eq!(compact, typed);
            }
            (Err(actual), Err(expected)) => assert_eq!(actual, expected),
            _ => panic!("compact and typed admission differ"),
        }
    }
}

#[test]
fn consecutive_singleton_preparation_is_read_only_and_revision_bound() {
    let mut images = CanonicalImages::new(StateLimits::default(), 16, 16);
    let empty = images.prepare_consecutive_group(1, &[]).unwrap();
    images.install_prepared_group(empty).unwrap();
    assert_eq!(images.speculative().revision(), 0);
    let first = images.prepare_consecutive_group(1, &[create()]).unwrap();
    let stale = images.prepare_consecutive_group(1, &[create()]).unwrap();
    assert_eq!(images.speculative().revision(), 0);
    assert_eq!(images.pending_len(), 0);
    images.install_prepared_group(first).unwrap();
    assert_eq!(images.speculative().revision(), 1);
    assert_eq!(images.committed().revision(), 0);
    assert_eq!(
        images.install_prepared_group(stale).unwrap_err(),
        CanonicalImagesError::StalePreparedGroup
    );
    images.commit_through(1).unwrap();
    assert_eq!(images.speculative(), images.committed());
}

#[test]
fn canonical_group_preparation_checks_aggregate_identity_capacity() {
    let operations = vec![
        (1, create()),
        (2, open()),
        (
            3,
            OperationBody::Barrier(Barrier {
                operation_id: operation(0x60),
            }),
        ),
    ];
    let images = CanonicalImages::new(StateLimits::default(), 1, 16);
    assert_eq!(
        images.prepare_group(&operations).unwrap_err(),
        CanonicalImagesError::State(StateError::IdentityIndex(IdentityIndexError::Capacity))
    );
    assert_eq!(images.committed().revision(), 0);
    assert_eq!(images.speculative().revision(), 0);
    assert_eq!(images.pending_len(), 0);
    let bodies = operations
        .into_iter()
        .map(|(_, body)| body)
        .collect::<Vec<_>>();
    assert_eq!(
        images.prepare_consecutive_group(1, &bodies).unwrap_err(),
        CanonicalImagesError::State(StateError::IdentityIndex(IdentityIndexError::Capacity))
    );
    assert_eq!(images.speculative().revision(), 0);
    assert_eq!(images.pending_len(), 0);
}

#[test]
fn message_id_is_metadata_not_the_append_deduplication_key() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(16);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());
    apply(&mut state, &mut index, 3, &append(0, 0x40));

    let next = state.prepare(4, &append(1, 0x40), &index, &state).unwrap();
    assert!(next.identity_claims().is_empty());
}

#[test]
fn repeated_message_ids_are_metadata_even_within_one_batch() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(16);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());

    let message_id = MessageId::from_bytes([0x40; 16]);
    let body = OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            first_sequence: ProducerSequence::ZERO,
            first_offset: Offset::ZERO,
            append_timestamp_millis: 123,
            records: vec![
                AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id,
                    parts: vec![b"one".as_slice()].into(),
                },
                AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id,
                    parts: vec![b"two".as_slice()].into(),
                },
            ]
            .into(),
        }],
    });

    apply(&mut state, &mut index, 3, &body);
    assert_eq!(
        state.partition(partition()).unwrap().next_offset,
        Offset::new(2)
    );
    assert_eq!(index.len(), 1);
}

#[test]
fn multi_partition_append_is_atomic_and_message_ids_are_partition_scoped() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(16);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());
    apply(&mut state, &mut index, 3, &create_another());
    apply(&mut state, &mut index, 4, &open_another());

    let shared_message_id = MessageId::from_bytes([0x40; 16]);
    let batches = vec![
        AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            first_sequence: ProducerSequence::new(0),
            first_offset: Offset::ZERO,
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: shared_message_id,
                parts: vec![b"one".as_slice()].into(),
            }]
            .into(),
        },
        AppendBatch {
            partition: another_partition(),
            owner_epoch: OwnerEpoch::new(10),
            producer_id: another_producer(),
            producer_epoch: ProducerEpoch::new(4),
            first_sequence: ProducerSequence::new(0),
            first_offset: Offset::ZERO,
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: shared_message_id,
                parts: vec![b"two".as_slice()].into(),
            }]
            .into(),
        },
    ];
    let invalid = OperationBody::Append(Append {
        batches: vec![
            batches[0].clone(),
            AppendBatch {
                owner_epoch: OwnerEpoch::new(11),
                ..batches[1].clone()
            },
        ],
    });
    let before = state.clone();
    assert_eq!(
        state.prepare(5, &invalid, &index, &state).unwrap_err(),
        StateError::OwnerEpochMismatch
    );
    assert_eq!(state, before);

    apply(
        &mut state,
        &mut index,
        5,
        &OperationBody::Append(Append { batches }),
    );
    assert_eq!(
        state.partition(partition()).unwrap().next_offset,
        Offset::new(1)
    );
    assert_eq!(
        state.partition(another_partition()).unwrap().next_offset,
        Offset::new(1)
    );
}

#[test]
fn operation_numbers_and_typed_body_shape_are_enforced() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(16);
    assert_eq!(
        state.prepare(2, &create(), &index, &state).unwrap_err(),
        StateError::OperationNumberMismatch
    );

    let malformed_create = OperationBody::CreatePartition(CreatePartition {
        partition: partition(),
        stream: "",
        topic: "topic",
        partition_id: PartitionId::new(7),
        owner_epoch: OwnerEpoch::new(9),
        retention: RetentionPolicy::default(),
    });
    assert!(matches!(
        state.prepare(1, &malformed_create, &index, &state),
        Err(StateError::MalformedBody(_))
    ));

    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());
    let before = state.clone();
    let index_before = index.clone();
    let malformed_append = OperationBody::Append(Append {
        batches: Vec::new(),
    });
    assert!(matches!(
        state.prepare(3, &malformed_append, &index, &state),
        Err(StateError::MalformedBody(_))
    ));
    let malformed_progress = OperationBody::Progress(Progress {
        owner: ProgressOwner::Subscription(SubscriptionId::from_bytes([0x70; 16])),
        partition: partition(),
        assignment_epoch: Some(1),
        expected_progress: None,
        new_progress: Offset::ZERO,
        operation_id: operation(0x31),
    });
    assert!(matches!(
        state.prepare(3, &malformed_progress, &index, &state),
        Err(StateError::MalformedBody(_))
    ));
    assert_eq!(state, before);
    assert_eq!(index, index_before);
}
