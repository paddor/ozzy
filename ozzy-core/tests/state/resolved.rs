use super::*;
use ozzy_core::state::CanonicalImages;
use ozzy_core::state::{ProducerResultSource, StateSnapshotLimits};
use ozzy_journal::operation::{
    AppendBatchSummary, AppendResults, AppendSummary, OperationLimits, RetryFingerprint,
};
use ozzy_proto::append::{AppendDisposition, AppendOutcome};

fn setup(limits: StateLimits) -> (CanonicalState, MemoryIdentityIndex) {
    let mut state = CanonicalState::new(limits);
    let mut identities = MemoryIdentityIndex::new(64);
    apply(&mut state, &mut identities, 1, &create());
    apply(&mut state, &mut identities, 2, &open());
    let OperationBody::OpenProducer(mut other) = open() else {
        unreachable!()
    };
    other.producer_id = another_producer();
    other.operation_id = operation(0x50);
    apply(
        &mut state,
        &mut identities,
        3,
        &OperationBody::OpenProducer(other),
    );
    apply(&mut state, &mut identities, 4, &append(0, 1));
    (state, identities)
}

fn summary(state: &CanonicalState, producer: ProducerId, stored: usize) -> AppendBatchSummary {
    let partition = state.partition(partition()).unwrap();
    let session = partition.producer(producer).unwrap();
    AppendBatchSummary {
        partition: super::partition(),
        owner_epoch: partition.owner_epoch,
        producer_id: producer,
        producer_epoch: session.producer_epoch,
        first_sequence: session.next_producer_sequence,
        first_offset: partition.next_offset,
        record_count: stored,
        nonzero_message_ids: true,
    }
}

fn attempt(disposition: AppendDisposition, offset: u64) -> AppendOutcome {
    AppendOutcome {
        disposition,
        offset: Offset::new(offset),
    }
}

fn resolve(
    state: &mut CanonicalState,
    identities: &mut MemoryIdentityIndex,
    producer: ProducerId,
    time: u64,
    rows: &[AppendOutcome],
) {
    let stored = rows
        .iter()
        .filter(|row| row.disposition == AppendDisposition::Stored)
        .count();
    let proofs = vec![RetryFingerprint::from_bytes([0x41; 32]); rows.len() - stored];
    let results =
        AppendResults::classified(time, rows, &proofs, OperationLimits::default()).unwrap();
    let prepared = state
        .prepare_resolved_append_summary(
            state.revision() + 1,
            summary(state, producer, stored),
            results,
        )
        .unwrap();
    state.apply(prepared, identities).unwrap();
}

fn restore(state: &CanonicalState) -> CanonicalState {
    CanonicalState::decode_snapshot(
        &state
            .encode_snapshot(StateSnapshotLimits::default())
            .unwrap(),
        StateLimits::default(),
        StateSnapshotLimits::default(),
    )
    .unwrap()
}

#[test]
fn resolved_groups_are_atomic_and_duplicates_follow_only_their_confirmed_prefix() {
    use AppendDisposition::{Duplicate, Stored};
    let (state, identities) = setup(StateLimits::default());
    let first_rows = [attempt(Stored, 1), attempt(Duplicate, 0)];
    let second_rows = [attempt(Duplicate, 1), attempt(Duplicate, 0)];
    let proofs = [RetryFingerprint::from_bytes([0x22; 32]); 2];
    let limits = OperationLimits::default();
    let first = AppendResults::classified(1, &first_rows, &proofs[..1], limits).unwrap();
    let second = AppendResults::classified(2, &second_rows, &proofs, limits).unwrap();
    let first_batch = summary(&state, producer(), 1);
    let second_batch = AppendBatchSummary {
        first_sequence: ProducerSequence::new(3),
        first_offset: Offset::new(2),
        record_count: 0,
        ..first_batch
    };
    let mut images = CanonicalImages::from_checkpoint(state.clone(), identities, 2);
    let invalid = AppendBatchSummary {
        first_sequence: ProducerSequence::new(4),
        ..second_batch
    };
    assert!(
        images
            .prepare_resolved_append_group(&[(5, first_batch, first), (6, invalid, second)])
            .is_err()
    );
    assert_eq!(images.speculative(), &state);
    assert_eq!(images.pending_len(), 0);
    let prepared = images
        .prepare_resolved_append_group(&[(5, first_batch, first), (6, second_batch, second)])
        .unwrap();
    assert_eq!(images.speculative(), &state);
    images.install_prepared_group(prepared).unwrap();
    assert_eq!(images.committed(), &state);
    assert_eq!(images.pending_len(), 2);
    let accepted = images.speculative().partition(partition()).unwrap();
    assert_eq!(accepted.next_offset, Offset::new(2));
    assert_eq!(
        accepted
            .producer(producer())
            .unwrap()
            .next_producer_sequence,
        ProducerSequence::new(5)
    );
    images.commit_through(5).unwrap();
    let committed = images.committed().partition(partition()).unwrap();
    let session = committed.producer(producer()).unwrap();
    assert_eq!(session.next_producer_sequence, ProducerSequence::new(3));
    assert_eq!(
        session.result_outcome(ProducerSequence::new(2)),
        Some(attempt(Duplicate, 0))
    );
    assert_eq!(session.result_outcome(ProducerSequence::new(3)), None);
    assert_eq!(committed.dedup_time, 1);
    images.discard_suffix();
    assert_eq!(images.speculative(), images.committed());
    assert_eq!(restore(images.committed()), *images.committed());
}

#[test]
fn redundant_and_mixed_attempts_preserve_independent_coordinates_after_checkpoint() {
    use AppendDisposition::{Duplicate, Stored};
    let (mut state, mut identities) = setup(StateLimits::default());
    resolve(
        &mut state,
        &mut identities,
        another_producer(),
        100,
        &[attempt(Duplicate, 0)],
    );
    resolve(
        &mut state,
        &mut identities,
        producer(),
        101,
        &[
            attempt(Stored, 1),
            attempt(Duplicate, 0),
            attempt(Stored, 2),
            attempt(Duplicate, 1),
        ],
    );
    let restored = restore(&state);
    assert_eq!(restored, state);
    for state in [&state, &restored] {
        let partition = state.partition(partition()).unwrap();
        assert_eq!(partition.next_offset, Offset::new(3));
        assert_eq!(partition.dedup_time, 101);
        let first = partition.producer(producer()).unwrap();
        assert_eq!(first.next_producer_sequence, ProducerSequence::new(5));
        for (sequence, disposition, offset) in [
            (0, Stored, 0),
            (1, Stored, 1),
            (2, Duplicate, 0),
            (3, Stored, 2),
            (4, Duplicate, 1),
        ] {
            assert_eq!(
                first.result_outcome(ProducerSequence::new(sequence)),
                Some(attempt(disposition, offset))
            );
        }
        assert_eq!(
            first.result_span(ProducerSequence::new(2)).unwrap().source,
            ProducerResultSource::Duplicate { operation: 6 }
        );
        assert_eq!(first.result_operation_floor(), Some(6));
        let other = partition.producer(another_producer()).unwrap();
        assert_eq!(other.next_producer_sequence, ProducerSequence::new(1));
        assert_eq!(other.result_offset_floor(), None);
        assert_eq!(other.result_operation_floor(), Some(5));
        assert_eq!(
            other.result_outcome(ProducerSequence::ZERO),
            Some(attempt(Duplicate, 0))
        );
    }
}

#[test]
fn duplicate_proof_retirement_does_not_follow_the_original_payload_offset() {
    use AppendDisposition::{Duplicate, Stored};
    let (mut state, mut identities) = setup(StateLimits::default());
    resolve(
        &mut state,
        &mut identities,
        another_producer(),
        10,
        &[attempt(Duplicate, 0)],
    );
    resolve(
        &mut state,
        &mut identities,
        producer(),
        11,
        &[attempt(Duplicate, 0), attempt(Stored, 1)],
    );
    let partition_state = state.partition(partition()).unwrap();
    let first = partition_state.producer(producer()).unwrap();
    assert_eq!(
        first.result_floor_for_offset(Offset::new(2)),
        ProducerSequence::new(1)
    );
    assert_eq!(
        first.result_floor_for_retention(Offset::new(2), 6),
        ProducerSequence::new(1)
    );
    assert_eq!(
        first.result_floor_for_retention(Offset::new(2), 7),
        ProducerSequence::new(3)
    );
    let other = partition_state.producer(another_producer()).unwrap();
    assert_eq!(
        other.result_floor_for_offset(Offset::new(2)),
        ProducerSequence::ZERO
    );
    assert_eq!(
        other.result_floor_for_retention(Offset::new(2), 6),
        ProducerSequence::new(1)
    );
    apply(
        &mut state,
        &mut identities,
        7,
        &OperationBody::ProducerResultFloor(ProducerResultFloor {
            partition: partition(),
            producer_id: producer(),
            producer_epoch: ProducerEpoch::new(3),
            expected_floor: ProducerSequence::ZERO,
            new_floor: ProducerSequence::new(1),
            operation_id: operation(0x61),
        }),
    );
    apply(
        &mut state,
        &mut identities,
        8,
        &OperationBody::Trim(Trim {
            partition: partition(),
            expected_floor: Offset::ZERO,
            new_floor: Offset::new(1),
            operation_id: operation(0x62),
        }),
    );
    let restored = restore(&state);
    let other = restored
        .partition(partition())
        .unwrap()
        .producer(another_producer())
        .unwrap();
    assert_eq!(
        other.result_outcome(ProducerSequence::ZERO),
        Some(attempt(Duplicate, 0))
    );
    assert_eq!(other.result_operation_floor(), Some(5));
}

#[test]
fn capacity_fences_counts_and_clock_errors_leave_the_image_unchanged() {
    let (state, _identities) = setup(StateLimits {
        max_retry_spans: 1,
        ..StateLimits::default()
    });
    let rows = [attempt(AppendDisposition::Duplicate, 0)];
    let proof = [RetryFingerprint::from_bytes([1; 32])];
    let results = AppendResults::classified(10, &rows, &proof, OperationLimits::default()).unwrap();
    let batch = summary(&state, another_producer(), 0);
    let original = state.clone();
    assert_eq!(
        state
            .prepare_resolved_append_summary(5, batch, results)
            .err(),
        Some(StateError::LimitExceeded("producer retry spans"))
    );
    assert_eq!(state, original);
    // Fencing and shape validation precede capacity admission.
    for (bad, error) in [
        (
            AppendBatchSummary {
                owner_epoch: OwnerEpoch::new(10),
                ..batch
            },
            StateError::OwnerEpochMismatch,
        ),
        (
            AppendBatchSummary {
                producer_epoch: ProducerEpoch::new(4),
                ..batch
            },
            StateError::ProducerEpochMismatch,
        ),
        (
            AppendBatchSummary {
                first_sequence: ProducerSequence::new(1),
                ..batch
            },
            StateError::ProducerSequenceMismatch,
        ),
        (
            AppendBatchSummary {
                first_offset: Offset::ZERO,
                ..batch
            },
            StateError::OffsetMismatch,
        ),
        (
            AppendBatchSummary {
                record_count: 1,
                ..batch
            },
            StateError::MalformedBody("stored outcomes do not match records"),
        ),
    ] {
        assert_eq!(
            state.prepare_resolved_append_summary(5, bad, results).err(),
            Some(error)
        );
        assert_eq!(state, original);
    }
    let (mut state, mut identities2) = setup(StateLimits::default());
    resolve(&mut state, &mut identities2, another_producer(), 10, &rows);
    let original = state.clone();
    let results = AppendResults::classified(9, &rows, &proof, OperationLimits::default()).unwrap();
    assert!(
        state
            .prepare_resolved_append_summary(6, summary(&state, another_producer(), 0), results)
            .is_err()
    );
    assert_eq!(state, original);
}

#[test]
fn a_writer_at_the_span_limit_drops_its_oldest_but_keeps_the_span_it_extends() {
    use AppendDisposition::{Duplicate, Stored};
    let (mut state, mut identities) = setup(StateLimits {
        max_retry_spans: 2,
        ..StateLimits::default()
    });
    resolve(
        &mut state,
        &mut identities,
        producer(),
        10,
        &[attempt(Duplicate, 0)],
    );
    resolve(
        &mut state,
        &mut identities,
        producer(),
        11,
        &[attempt(Stored, 1)],
    );
    let writer = |state: &CanonicalState| {
        state
            .partition(partition())
            .unwrap()
            .producer(producer())
            .unwrap()
            .clone()
    };
    let current = writer(&state);
    assert_eq!(current.producer_result_floor, ProducerSequence::new(1));
    assert_eq!(current.result_outcome(ProducerSequence::ZERO), None);
    assert_eq!(
        current.result_outcome(ProducerSequence::new(1)),
        Some(attempt(Duplicate, 0))
    );
    // The first result extends the newest span, so the oldest one goes.
    resolve(
        &mut state,
        &mut identities,
        producer(),
        12,
        &[attempt(Stored, 2), attempt(Duplicate, 0)],
    );
    let current = writer(&state);
    assert_eq!(current.producer_result_floor, ProducerSequence::new(2));
    assert_eq!(current.result_spans().len(), 2);
    assert_eq!(
        current.result_outcome(ProducerSequence::new(3)),
        Some(attempt(Stored, 2))
    );
    // The snapshot counts the same spans.
    assert_eq!(
        restore(&state).partition(partition()),
        state.partition(partition())
    );
}

#[test]
fn empty_legacy_append_cannot_advance_a_revision() {
    let (state, _) = setup(StateLimits::default());
    assert!(
        state
            .prepare_append_summary(5, &AppendSummary::single(summary(&state, producer(), 0)))
            .is_err()
    );
}

#[test]
fn resolved_plan_is_revision_bound_and_retires_partial_duplicate_ranges() {
    let (mut state, mut identities) = setup(StateLimits::default());
    // Another writer adds offset one, so the redundant run can name both records.
    apply(&mut state, &mut identities, 5, &append(1, 2));
    let rows = [
        attempt(AppendDisposition::Duplicate, 0),
        attempt(AppendDisposition::Duplicate, 1),
    ];
    let proofs = [RetryFingerprint::from_bytes([1; 32]); 2];
    let results = AppendResults::classified(3, &rows, &proofs, OperationLimits::default()).unwrap();
    let plan = state
        .prepare_resolved_append_summary(6, summary(&state, another_producer(), 0), results)
        .unwrap();
    let outdated = plan.clone();
    state.apply(plan, &mut identities).unwrap();
    assert_eq!(
        state.apply(outdated, &mut identities),
        Err(StateError::StalePlan)
    );
    apply(
        &mut state,
        &mut identities,
        7,
        &OperationBody::ProducerResultFloor(ProducerResultFloor {
            partition: partition(),
            producer_id: another_producer(),
            producer_epoch: ProducerEpoch::new(3),
            expected_floor: ProducerSequence::ZERO,
            new_floor: ProducerSequence::new(1),
            operation_id: operation(0x64),
        }),
    );
    let restored = restore(&state);
    let producer = restored
        .partition(partition())
        .unwrap()
        .producer(another_producer())
        .unwrap();
    assert_eq!(producer.result_outcome(ProducerSequence::ZERO), None);
    assert_eq!(
        producer.result_outcome(ProducerSequence::new(1)),
        Some(attempt(AppendDisposition::Duplicate, 1))
    );
    assert_eq!(
        producer
            .result_span(ProducerSequence::new(1))
            .unwrap()
            .source,
        ProducerResultSource::Duplicate { operation: 6 }
    );
}
