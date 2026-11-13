use super::*;
use ozzy_core::state::StateSnapshotLimits;
use ozzy_journal::operation::{OperationLimits, decode_append_summary, encode_operation_body};

/// A producer-less batch of `count` records at `first_offset`.
fn owner_append(first_offset: u64, count: u8) -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: ProducerId::from_bytes([0; 16]),
            producer_epoch: ProducerEpoch::new(0),
            first_sequence: ProducerSequence::ZERO,
            first_offset: Offset::new(first_offset),
            append_timestamp_millis: 77,
            records: (1..=count)
                .map(|message| AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([message; 16]),
                    parts: vec![b"owned".as_slice()].into(),
                })
                .collect::<Vec<_>>()
                .into(),
        }],
    })
}

/// Admit `body` through the materialized and the summary path; both agree.
fn prepare_both(
    state: &CanonicalState,
    identities: &MemoryIdentityIndex,
    op_number: u64,
    body: &OperationBody<'_>,
) -> Result<(), StateError> {
    let limits = OperationLimits::default();
    let bytes = encode_operation_body(body, limits).unwrap();
    let summary = decode_append_summary(&bytes, limits).unwrap();
    let materialized = state.prepare(op_number, body, identities).map(drop);
    let compact = state.prepare_append_summary(op_number, &summary).map(drop);
    assert_eq!(compact, materialized);
    materialized
}

#[test]
fn owner_batches_take_the_next_offsets_without_producer_state() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(64);
    apply(&mut state, &mut identities, 1, &create());
    prepare_both(&state, &identities, 2, &owner_append(0, 3)).unwrap();
    apply(&mut state, &mut identities, 2, &owner_append(0, 3));
    apply(&mut state, &mut identities, 3, &owner_append(3, 2));
    let partition = state.partition(partition()).unwrap();
    assert_eq!(partition.next_offset, Offset::new(5));
    assert_eq!(partition.producers().len(), 0);
    assert_eq!(
        partition.producer_result_offset_floor(),
        Some(Offset::new(5))
    );
    let encoded = state
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let restored = CanonicalState::decode_snapshot(
        &encoded,
        StateLimits::default(),
        StateSnapshotLimits::default(),
    )
    .unwrap();
    assert_eq!(restored, state);
}

#[test]
fn owner_batches_are_fenced_by_offset_epoch_and_ids() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(64);
    apply(&mut state, &mut identities, 1, &create());
    apply(&mut state, &mut identities, 2, &owner_append(0, 2));
    for offset in [0, 1, 3] {
        assert_eq!(
            prepare_both(&state, &identities, 3, &owner_append(offset, 1)),
            Err(StateError::OffsetMismatch)
        );
    }
    let OperationBody::Append(mut epoch) = owner_append(2, 1) else {
        unreachable!()
    };
    epoch.batches[0].owner_epoch = OwnerEpoch::new(10);
    assert_eq!(
        prepare_both(&state, &identities, 3, &OperationBody::Append(epoch)),
        Err(StateError::OwnerEpochMismatch)
    );
    let OperationBody::Append(mut zero) = owner_append(2, 1) else {
        unreachable!()
    };
    zero.batches[0].records = vec![AppendRecord {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([0; 16]),
        parts: vec![b"owned".as_slice()].into(),
    }]
    .into();
    assert_eq!(
        prepare_both(&state, &identities, 3, &OperationBody::Append(zero)),
        Err(StateError::ZeroValue("message"))
    );
    // A batch with any producer field set is a producer batch.
    let OperationBody::Append(mut epoch_only) = owner_append(2, 1) else {
        unreachable!()
    };
    epoch_only.batches[0].producer_epoch = ProducerEpoch::new(1);
    assert_eq!(
        prepare_both(&state, &identities, 3, &OperationBody::Append(epoch_only)),
        Err(StateError::ProducerMismatch)
    );
    prepare_both(&state, &identities, 3, &owner_append(2, 1)).unwrap();
}

#[test]
fn owner_batches_need_a_partition_without_delay_or_deduplication() {
    for (delay, dedup) in [
        (NonZeroU64::new(60_000), None),
        (None, NonZeroU64::new(1000)),
    ] {
        let OperationBody::CreatePartition(mut definition) = create() else {
            unreachable!()
        };
        definition.delay_max_millis = delay;
        definition.dedup_window_millis = dedup;
        let mut state = CanonicalState::new(StateLimits::default());
        let mut identities = MemoryIdentityIndex::new(64);
        apply(
            &mut state,
            &mut identities,
            1,
            &OperationBody::CreatePartition(definition),
        );
        assert_eq!(
            prepare_both(&state, &identities, 2, &owner_append(0, 1)),
            Err(StateError::OwnerAppendDisabled)
        );
    }
}

#[test]
fn owner_trim_moves_the_floor_up_to_the_next_offset() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(64);
    apply(&mut state, &mut identities, 1, &create());
    apply(&mut state, &mut identities, 2, &owner_append(0, 4));
    let trim = |expected: u64, floor: u64, id: u8| {
        OperationBody::Trim(Trim {
            partition: partition(),
            expected_floor: Offset::new(expected),
            new_floor: Offset::new(floor),
            operation_id: operation(id),
            expected_operation_floor: 0,
            new_operation_floor: 0,
        })
    };
    assert_eq!(
        state.prepare(3, &trim(0, 5, 0x50), &identities).map(drop),
        Err(StateError::InvalidTrimFloor)
    );
    apply(&mut state, &mut identities, 3, &trim(0, 4, 0x51));
    assert_eq!(
        state.partition(partition()).unwrap().retained_from,
        Offset::new(4)
    );
}
