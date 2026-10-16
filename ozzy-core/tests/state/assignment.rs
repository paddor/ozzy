use super::*;
use ozzy_core::state::StateSnapshotLimits;

fn group() -> ConsumerGroupId {
    ConsumerGroupId::from_bytes([0x60; 16])
}

fn owner() -> ProgressOwner {
    ProgressOwner::ConsumerGroup(group())
}

fn initialized(records: u64) -> CanonicalImages {
    let mut images = CanonicalImages::new(StateLimits::default(), 32, 8);
    images.admit(1, &create()).unwrap();
    images.admit(2, &open()).unwrap();
    for offset in 0..records {
        images
            .admit(offset + 3, &append(offset, offset as u8 + 1))
            .unwrap();
    }
    images.commit_through(records + 2).unwrap();
    images
}

fn assign(epoch: u64, revision: u64, cursor: u64) -> OperationBody<'static> {
    OperationBody::Assign(Assign {
        consumer_group_id: group(),
        partition: partition(),
        expected_assignment_epoch: epoch - 1,
        new_assignment_epoch: epoch,
        plan_revision: revision,
        resume_cursor: Offset::new(cursor),
        new_member: Some(ConsumerMemberId::from_bytes([epoch as u8; 16])),
        operation_id: operation(0x80 + epoch as u8),
    })
}

fn progress(epoch: u64, expected: u64, cursor: u64) -> OperationBody<'static> {
    OperationBody::Progress(Progress {
        owner: owner(),
        partition: partition(),
        assignment: Some(ProgressAssignment {
            epoch,
            member: ConsumerMemberId::from_bytes([epoch as u8; 16]),
        }),
        expected_progress: Some(Offset::new(expected)),
        new_progress: Offset::new(cursor),
        operation_id: operation(0x90 + cursor as u8),
    })
}

#[test]
fn first_assignment_persists_zero_or_an_explicit_confirmed_end_before_any_acknowledgment() {
    for (end, cursor) in [(0, 0), (3, 0), (3, 3)] {
        let mut images = initialized(end);
        let number = end + 3;
        images.admit(number, &assign(1, 7, cursor)).unwrap();
        assert_eq!(images.committed().progress(owner(), partition()), None);
        images.commit_through(number).unwrap();
        let state = images.committed();
        assert_eq!(
            state.progress(owner(), partition()),
            Some(Offset::new(cursor))
        );
        let assignment = state.assignment(group(), partition()).unwrap();
        assert_eq!(
            (
                assignment.epoch,
                assignment.plan_revision,
                assignment.resume_cursor
            ),
            (1, 7, Offset::new(cursor))
        );
        let bytes = state
            .encode_snapshot(StateSnapshotLimits::default())
            .unwrap();
        let restored = CanonicalState::decode_snapshot(
            &bytes,
            StateLimits::default(),
            StateSnapshotLimits::default(),
        )
        .unwrap();
        assert_eq!(&restored, state);
    }
}

#[test]
fn handoff_captures_earlier_accepted_progress_without_resetting_or_skipping_a_hole() {
    let mut images = initialized(3);
    images.admit(6, &assign(1, 7, 0)).unwrap();
    images.commit_through(6).unwrap();
    images.admit(7, &progress(1, 0, 1)).unwrap();
    let before = images.clone();
    for cursor in [0, 2] {
        assert_eq!(
            images.admit(8, &assign(2, 9, cursor)),
            Err(CanonicalImagesError::State(
                StateError::AssignmentProgressMismatch
            ))
        );
        assert_eq!(images, before);
    }
    images.admit(8, &assign(2, 9, 1)).unwrap();
    assert_eq!(
        images.committed().progress(owner(), partition()),
        Some(Offset::ZERO)
    );
    assert_eq!(
        images
            .committed()
            .assignment(group(), partition())
            .unwrap()
            .epoch,
        1
    );
    let after = images.clone();
    assert_eq!(
        images.admit(9, &progress(1, 1, 2)),
        Err(CanonicalImagesError::State(
            StateError::AssignmentEpochMismatch
        ))
    );
    assert_eq!(images, after);
    images.commit_through(8).unwrap();
    let ownership = images.committed().assignment(group(), partition()).unwrap();
    assert_eq!(
        (
            ownership.epoch,
            ownership.plan_revision,
            ownership.resume_cursor
        ),
        (2, 9, Offset::new(1))
    );
    images.admit(9, &progress(2, 1, 2)).unwrap();
    images.commit_through(9).unwrap();
    assert_eq!(
        images.committed().assignment(group(), partition()),
        Some(ownership),
        "later progress does not alter the captured start"
    );
    assert_eq!(
        images.committed().progress(owner(), partition()),
        Some(Offset::new(2))
    );
    let bytes = images
        .committed()
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let restored = CanonicalState::decode_snapshot(
        &bytes,
        StateLimits::default(),
        StateSnapshotLimits::default(),
    )
    .unwrap();
    assert_eq!(restored.assignment(group(), partition()), Some(ownership));
    assert_eq!(
        restored.progress(owner(), partition()),
        Some(Offset::new(2))
    );
    let before = images.clone();
    for revision in [0, 7, 9] {
        assert_eq!(
            images.admit(10, &assign(3, revision, 2)),
            Err(CanonicalImagesError::State(
                StateError::AssignmentPlanNotAdvanced
            ))
        );
        assert_eq!(images, before);
    }
    images.admit(10, &assign(3, 10, 2)).unwrap();
    images.discard_suffix();
    assert_eq!(images, before);
}

#[test]
fn retention_before_first_acknowledgment_preserves_the_persisted_resume_cursor() {
    let mut images = initialized(1);
    images.admit(4, &assign(1, 7, 0)).unwrap();
    images.commit_through(4).unwrap();
    images
        .admit(
            5,
            &OperationBody::ProducerResultFloor(ProducerResultFloor {
                partition: partition(),
                producer_id: producer(),
                producer_epoch: ProducerEpoch::new(3),
                expected_floor: ProducerSequence::ZERO,
                new_floor: ProducerSequence::new(1),
                operation_id: operation(0xa1),
            }),
        )
        .unwrap();
    images
        .admit(
            6,
            &OperationBody::Trim(Trim {
                partition: partition(),
                expected_floor: Offset::ZERO,
                new_floor: Offset::new(1),
                operation_id: operation(0xa2),
            }),
        )
        .unwrap();
    images.commit_through(6).unwrap();
    assert_eq!(
        images.committed().progress(owner(), partition()),
        Some(Offset::ZERO)
    );
    assert_eq!(
        images
            .committed()
            .partition(partition())
            .unwrap()
            .retained_from,
        Offset::new(1)
    );
    let before = images.clone();
    assert_eq!(
        images.admit(7, &assign(2, 9, 1)),
        Err(CanonicalImagesError::State(
            StateError::AssignmentProgressMismatch
        ))
    );
    assert_eq!(images, before);
    images.admit(7, &assign(2, 9, 0)).unwrap();
    images.commit_through(7).unwrap();
    assert_eq!(
        images.committed().progress(owner(), partition()),
        Some(Offset::ZERO)
    );
}

#[test]
fn initial_assignment_selects_only_records_before_it_within_metadata_capacity() {
    let mut images = initialized(0);
    let before = images.clone();
    assert_eq!(
        images.admit(3, &assign(1, 7, 1)),
        Err(CanonicalImagesError::State(
            StateError::InvalidAssignmentCursor
        ))
    );
    assert_eq!(images, before);
    // Replicas validate the same ordered prefix, whatever commit they know.
    images.admit(3, &append(0, 1)).unwrap();
    images.admit(4, &assign(1, 7, 1)).unwrap();
    for (limits, resource) in [
        (
            StateLimits {
                max_progress: 0,
                ..Default::default()
            },
            "progress owners",
        ),
        (
            StateLimits {
                max_assignments: 0,
                ..Default::default()
            },
            "assignments",
        ),
    ] {
        let mut images = CanonicalImages::new(limits, 32, 8);
        images.admit(1, &create()).unwrap();
        images.commit_through(1).unwrap();
        let before = images.clone();
        assert_eq!(
            images.admit(2, &assign(1, 7, 0)),
            Err(CanonicalImagesError::State(StateError::LimitExceeded(
                resource
            )))
        );
        assert_eq!(images, before);
    }
}
