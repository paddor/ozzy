use super::*;
use ozzy_core::state::StateSnapshotLimits;

fn initialized(limits: StateLimits) -> (CanonicalState, MemoryIdentityIndex) {
    let mut state = CanonicalState::new(limits);
    let mut index = MemoryIdentityIndex::new(128);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &open());
    let OperationBody::OpenProducer(mut other) = open() else {
        unreachable!()
    };
    other.producer_id = another_producer();
    other.operation_id = operation(0x51);
    apply(
        &mut state,
        &mut index,
        3,
        &OperationBody::OpenProducer(other),
    );
    (state, index)
}

fn assigned(writer: ProducerId, epoch: u64, sequence: u64, offset: u64) -> OperationBody<'static> {
    let OperationBody::Append(mut body) = append(offset, offset as u8 + 1) else {
        unreachable!()
    };
    body.batches[0].producer_id = writer;
    body.batches[0].producer_epoch = ProducerEpoch::new(epoch);
    body.batches[0].first_sequence = ProducerSequence::new(sequence);
    OperationBody::Append(body)
}

fn assign(
    state: &mut CanonicalState,
    index: &mut MemoryIdentityIndex,
    writer: ProducerId,
    sequence: u64,
) {
    let offset = state.partition(partition()).unwrap().next_offset.get();
    let body = assigned(writer, 3, sequence, offset);
    apply(state, index, state.revision() + 1, &body);
}

fn restore(state: &CanonicalState) -> CanonicalState {
    let bytes = state
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    CanonicalState::decode_snapshot(
        &bytes,
        StateLimits::default(),
        StateSnapshotLimits::default(),
    )
    .unwrap()
}

#[test]
fn latest_producer_transition_survives_snapshot_without_historical_indexes() {
    let (state, _) = initialized(StateLimits::default());
    let restored = restore(&state);
    let producer = restored
        .partition(partition())
        .unwrap()
        .producer(producer())
        .unwrap();
    let transition = producer.transition().unwrap();
    assert_eq!(transition.operation_id, operation(0x30));
    assert_eq!(transition.expected_epoch, None);
}

#[test]
fn interleaved_writers_keep_exact_retry_offsets_after_snapshot_restore() {
    let (mut state, mut index) = initialized(StateLimits::default());
    for (writer, sequence) in [
        (producer(), 0),
        (another_producer(), 0),
        (producer(), 1),
        (another_producer(), 1),
        (producer(), 2),
        (producer(), 3),
    ] {
        assign(&mut state, &mut index, writer, sequence);
    }
    for image in [&state, &restore(&state)] {
        let partition = image.partition(partition()).unwrap();
        assert_eq!(partition.next_offset, Offset::new(6));
        for (writer, offsets) in [
            (producer(), vec![0, 2, 4, 5]),
            (another_producer(), vec![1, 3]),
        ] {
            let writer = partition.producer(writer).unwrap();
            for (sequence, offset) in offsets.iter().enumerate() {
                assert_eq!(
                    writer.result_offset(ProducerSequence::new(sequence as u64)),
                    Some(Offset::new(*offset))
                );
            }
            assert_eq!(
                writer.result_offset(ProducerSequence::new(offsets.len() as u64)),
                None
            );
        }
        assert_eq!(
            partition.producer(producer()).unwrap().result_spans().len(),
            3
        );
    }
    assert_eq!(restore(&state), state);
}

#[test]
fn checksum_valid_snapshots_reject_ambiguous_or_missing_writer_results() {
    let (mut state, mut index) = initialized(StateLimits::default());
    assign(&mut state, &mut index, producer(), 0);
    assign(&mut state, &mut index, another_producer(), 0);
    let encoded = state
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let names = usize::from(u16::from_be_bytes(encoded[260..262].try_into().unwrap()))
        + usize::from(u16::from_be_bytes(encoded[262..264].try_into().unwrap()));
    let first_writer = 256 + 96 + names;
    let first_span = first_writer + 72;
    let second_writer = first_span + 24;
    let second_span = second_writer + 72;
    for (field, value) in [
        (first_span, 1),        // Missing sequence zero.
        (first_span + 16, 0),   // Empty range.
        (first_span + 8, 2),    // Past the partition end.
        (second_span + 8, 0),   // Two writers claim the same offset.
        (first_writer + 24, 2), // Unmapped accepted sequence.
        (first_writer + 32, 2), // Result floor past the writer end.
        (88, 3),                // Aggregate writer count disagrees with contents.
        (96, 3),                // Aggregate range count disagrees with contents.
    ] {
        let mut damaged = encoded.clone();
        damaged[field..field + 8].copy_from_slice(&u64::to_be_bytes(value));
        let mut hash =
            ozzy_journal::integrity::IntegrityHasher::new("ozzy canonical state snapshot v1");
        hash.update(&damaged[..56]);
        hash.update(&[0; 32]);
        hash.update(&damaged[88..]);
        damaged[56..88].copy_from_slice(hash.finish().as_bytes());
        let result = CanonicalState::decode_snapshot(
            &damaged,
            StateLimits::default(),
            StateSnapshotLimits::default(),
        );
        assert!(result.is_err(), "accepted field {field} = {value}");
        assert!(!matches!(
            result,
            Err(ozzy_core::state::StateSnapshotError::DigestMismatch)
        ));
    }
}

#[test]
fn fencing_one_writer_preserves_other_sessions_and_partition_offset() {
    let (mut state, mut index) = initialized(StateLimits::default());
    assign(&mut state, &mut index, producer(), 0);
    assign(&mut state, &mut index, another_producer(), 0);
    let other_before = state
        .partition(partition())
        .unwrap()
        .producer(another_producer())
        .unwrap()
        .clone();
    let fence = OperationBody::OpenProducer(OpenProducer {
        partition: partition(),
        producer_id: producer(),
        expected_epoch: Some(ProducerEpoch::new(3)),
        new_epoch: ProducerEpoch::new(4),
        operation_id: operation(0x52),
    });
    apply(&mut state, &mut index, 6, &fence);
    let partition_state = state.partition(partition()).unwrap();
    assert_eq!(partition_state.next_offset, Offset::new(2));
    assert_eq!(
        partition_state.producer(another_producer()).unwrap(),
        &other_before
    );
    assert_eq!(
        partition_state
            .producer(producer())
            .unwrap()
            .result_offset(ProducerSequence::ZERO),
        None
    );
    assert_eq!(
        state.prepare(7, &assigned(producer(), 3, 1, 2), &index, &state),
        Err(StateError::ProducerEpochMismatch)
    );
    apply(&mut state, &mut index, 7, &assigned(producer(), 4, 0, 2));
    assign(&mut state, &mut index, another_producer(), 1);
    let restored = restore(&state);
    assert_eq!(restored, state);
    assert_eq!(
        restored
            .partition(partition())
            .unwrap()
            .producer(another_producer())
            .unwrap()
            .result_offset(ProducerSequence::new(1)),
        Some(Offset::new(3))
    );
}

fn floor(writer: ProducerId, old: u64, new: u64, id: u8) -> OperationBody<'static> {
    OperationBody::ProducerResultFloor(ProducerResultFloor {
        partition: partition(),
        producer_id: writer,
        producer_epoch: ProducerEpoch::new(3),
        expected_floor: ProducerSequence::new(old),
        new_floor: ProducerSequence::new(new),
        operation_id: operation(id),
    })
}

#[test]
fn retention_cannot_destroy_another_writers_retry_evidence() {
    let (mut state, mut index) = initialized(StateLimits::default());
    for (writer, sequence) in [
        (producer(), 0),
        (another_producer(), 0),
        (producer(), 1),
        (another_producer(), 1),
    ] {
        assign(&mut state, &mut index, writer, sequence);
    }
    apply(&mut state, &mut index, 8, &floor(producer(), 0, 1, 0x53));
    let trim = OperationBody::Trim(Trim {
        partition: partition(),
        expected_floor: Offset::ZERO,
        new_floor: Offset::new(2),
        operation_id: operation(0x54),
    });
    assert_eq!(
        state.prepare(9, &trim, &index, &state),
        Err(StateError::TrimBeyondProducerResults)
    );
    apply(
        &mut state,
        &mut index,
        9,
        &floor(another_producer(), 0, 1, 0x55),
    );
    apply(&mut state, &mut index, 10, &trim);
    let restored = restore(&state);
    assert_eq!(restored, state);
    for writer in [producer(), another_producer()] {
        let writer = restored
            .partition(partition())
            .unwrap()
            .producer(writer)
            .unwrap();
        assert_eq!(writer.result_offset(ProducerSequence::ZERO), None);
    }
}

#[test]
fn retry_index_budget_is_aggregate_and_failure_is_atomic() {
    let limits = StateLimits {
        max_retry_spans: 2,
        max_producers: 2,
        ..StateLimits::default()
    };
    let (mut state, mut index) = initialized(limits);
    assign(&mut state, &mut index, producer(), 0);
    assign(&mut state, &mut index, producer(), 1);
    assign(&mut state, &mut index, another_producer(), 0);
    let before = state.clone();
    let next = assigned(producer(), 3, 2, 3);
    assert_eq!(
        state.prepare(7, &next, &index, &state),
        Err(StateError::LimitExceeded("producer retry spans"))
    );
    assert_eq!(state, before);
    let OperationBody::OpenProducer(mut third) = open() else {
        unreachable!()
    };
    third.producer_id = ProducerId::from_bytes([0x66; 16]);
    assert_eq!(
        state.prepare(7, &OperationBody::OpenProducer(third), &index, &state),
        Err(StateError::LimitExceeded("producers"))
    );
    apply(&mut state, &mut index, 7, &floor(producer(), 0, 2, 0x56));
    apply(&mut state, &mut index, 8, &next);
    assert_eq!(
        state
            .partition(partition())
            .unwrap()
            .producer(producer())
            .unwrap()
            .result_offset(ProducerSequence::new(2)),
        Some(Offset::new(3))
    );
}

#[test]
fn another_writer_cannot_create_an_alias_for_the_same_topic_partition() {
    let (state, index) = initialized(StateLimits::default());
    let OperationBody::CreatePartition(mut duplicate) = create() else {
        unreachable!()
    };
    duplicate.partition = another_partition();
    assert_eq!(
        state.prepare(
            4,
            &OperationBody::CreatePartition(duplicate),
            &index,
            &state
        ),
        Err(StateError::PartitionAddressExists)
    );
}
