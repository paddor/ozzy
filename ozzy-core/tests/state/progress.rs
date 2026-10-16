use super::*;
use ozzy_core::state::StateSnapshotLimits;

fn subscription() -> ProgressOwner {
    ProgressOwner::Subscription(SubscriptionId::from_bytes([0x70; 16]))
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

fn progress(
    owner: ProgressOwner,
    epoch: Option<u64>,
    expected: Option<u64>,
    next: u64,
) -> OperationBody<'static> {
    OperationBody::Progress(Progress {
        owner,
        partition: partition(),
        assignment: epoch.map(|epoch| ProgressAssignment {
            epoch,
            member: ConsumerMemberId::from_bytes([0x61; 16]),
        }),
        expected_progress: expected.map(Offset::new),
        new_progress: Offset::new(next),
        operation_id: operation(0x71),
    })
}

#[test]
fn exclusive_progress_accepts_empty_and_confirmed_end_and_survives_checkpoint() {
    for end in [0, 1, 3] {
        let mut images = initialized(end);
        let number = images.committed().revision() + 1;
        images
            .admit(number, &progress(subscription(), None, None, end))
            .unwrap();
        images.commit_through(number).unwrap();
        let state = images.committed();
        assert_eq!(
            state.progress(subscription(), partition()),
            Some(Offset::new(end))
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
fn exclusive_progress_covers_only_records_accepted_before_it() {
    let mut images = initialized(0);
    let before = images.clone();
    let body = progress(subscription(), None, None, 1);
    assert_eq!(
        images.admit(3, &body),
        Err(CanonicalImagesError::State(
            StateError::ProgressBeyondRecords
        ))
    );
    assert_eq!(images, before);
    // A replica can accept both operations before it learns either commit;
    // the progress operation then commits only after the record it covers.
    images.admit(3, &append(0, 1)).unwrap();
    images.admit(4, &body).unwrap();
    images.commit_through(3).unwrap();
    assert_eq!(
        images.committed().progress(subscription(), partition()),
        None
    );
    images.commit_through(4).unwrap();
    assert_eq!(
        images.committed().progress(subscription(), partition()),
        Some(Offset::new(1))
    );
}

#[test]
fn exclusive_progress_rejects_overrun_and_regression_without_mutation() {
    let mut images = initialized(2);
    images
        .admit(5, &progress(subscription(), None, None, 2))
        .unwrap();
    images.commit_through(5).unwrap();
    let before = images.clone();
    for (next, error) in [
        (3, StateError::ProgressBeyondRecords),
        (2, StateError::ProgressNotAdvanced),
        (1, StateError::ProgressNotAdvanced),
    ] {
        assert_eq!(
            images.admit(6, &progress(subscription(), None, Some(2), next)),
            Err(CanonicalImagesError::State(error))
        );
        assert_eq!(images, before);
    }
}

#[test]
fn exclusive_group_progress_preserves_assignment_epoch_fencing() {
    let mut images = initialized(1);
    let group = ConsumerGroupId::from_bytes([0x60; 16]);
    images
        .admit(
            4,
            &OperationBody::Assign(Assign {
                consumer_group_id: group,
                partition: partition(),
                expected_assignment_epoch: 0,
                new_assignment_epoch: 1,
                plan_revision: 1,
                resume_cursor: Offset::ZERO,
                new_member: Some(ConsumerMemberId::from_bytes([0x61; 16])),
                operation_id: operation(0x62),
            }),
        )
        .unwrap();
    images.commit_through(4).unwrap();
    let owner = ProgressOwner::ConsumerGroup(group);
    let before = images.clone();
    assert_eq!(
        images.admit(5, &progress(owner, Some(0), None, 1)),
        Err(CanonicalImagesError::State(
            StateError::AssignmentEpochMismatch
        ))
    );
    assert_eq!(images, before);
    images
        .admit(5, &progress(owner, Some(1), Some(0), 1))
        .unwrap();
    images.commit_through(5).unwrap();
    assert_eq!(
        images.committed().progress(owner, partition()),
        Some(Offset::new(1))
    );
}

fn assign(group: ConsumerGroupId, epoch: u64, member: Option<u8>) -> OperationBody<'static> {
    OperationBody::Assign(Assign {
        consumer_group_id: group,
        partition: partition(),
        expected_assignment_epoch: epoch - 1,
        new_assignment_epoch: epoch,
        plan_revision: epoch,
        resume_cursor: Offset::ZERO,
        new_member: member.map(|byte| ConsumerMemberId::from_bytes([byte; 16])),
        operation_id: operation(0x80 + epoch as u8),
    })
}

fn member_progress(group: ConsumerGroupId, epoch: u64, member: u8) -> OperationBody<'static> {
    let mut body = progress(ProgressOwner::ConsumerGroup(group), Some(epoch), Some(0), 1);
    if let OperationBody::Progress(value) = &mut body {
        value.assignment.as_mut().unwrap().member = ConsumerMemberId::from_bytes([member; 16]);
    }
    body
}

#[test]
fn group_progress_rejects_revoked_and_reassigned_members_without_mutation() {
    let mut images = initialized(2);
    let group = ConsumerGroupId::from_bytes([0x60; 16]);
    images.admit(5, &assign(group, 1, Some(0x61))).unwrap();
    images.commit_through(5).unwrap();
    let before = images.clone();
    for member in [0x62, 0x63] {
        assert_eq!(
            images.admit(6, &member_progress(group, 1, member)),
            Err(CanonicalImagesError::State(
                StateError::AssignmentMemberMismatch
            ))
        );
        assert_eq!(images, before);
    }
    images.admit(6, &assign(group, 2, None)).unwrap();
    images.commit_through(6).unwrap();
    let before = images.clone();
    assert_eq!(
        images.admit(7, &member_progress(group, 2, 0x61)),
        Err(CanonicalImagesError::State(
            StateError::AssignmentMemberMismatch
        ))
    );
    assert_eq!(images, before);
    images.admit(7, &assign(group, 3, Some(0x62))).unwrap();
    images.commit_through(7).unwrap();
    let before = images.clone();
    for (epoch, member, error) in [
        (1, 0x61, StateError::AssignmentEpochMismatch),
        (3, 0x61, StateError::AssignmentMemberMismatch),
    ] {
        assert_eq!(
            images.admit(8, &member_progress(group, epoch, member)),
            Err(CanonicalImagesError::State(error))
        );
        assert_eq!(images, before);
    }
    images.admit(8, &member_progress(group, 3, 0x62)).unwrap();
    assert_eq!(
        images
            .committed()
            .progress(ProgressOwner::ConsumerGroup(group), partition()),
        Some(Offset::ZERO)
    );
    images.commit_through(8).unwrap();
    assert_eq!(
        images
            .committed()
            .progress(ProgressOwner::ConsumerGroup(group), partition()),
        Some(Offset::new(1))
    );
}

#[test]
fn discarded_handoff_restores_the_committed_member_fence() {
    let mut images = initialized(2);
    let group = ConsumerGroupId::from_bytes([0x60; 16]);
    images.admit(5, &assign(group, 1, Some(0x61))).unwrap();
    images.commit_through(5).unwrap();
    let before = images.clone();
    images.admit(6, &assign(group, 2, Some(0x62))).unwrap();
    assert_eq!(
        images.admit(7, &member_progress(group, 1, 0x61)),
        Err(CanonicalImagesError::State(
            StateError::AssignmentEpochMismatch
        ))
    );
    images.discard_suffix();
    assert_eq!(images, before);
    images.admit(6, &member_progress(group, 1, 0x61)).unwrap();
    images.commit_through(6).unwrap();
    assert_eq!(
        images
            .committed()
            .progress(ProgressOwner::ConsumerGroup(group), partition()),
        Some(Offset::new(1))
    );
}

#[test]
fn restored_assignment_preserves_member_fencing() {
    let mut images = initialized(2);
    let group = ConsumerGroupId::from_bytes([0x60; 16]);
    images.admit(5, &assign(group, 1, Some(0x62))).unwrap();
    images.commit_through(5).unwrap();
    let bytes = images
        .committed()
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let mut restored = CanonicalState::decode_snapshot(
        &bytes,
        StateLimits::default(),
        StateSnapshotLimits::default(),
    )
    .unwrap();
    let mut identities = MemoryIdentityIndex::new(4);
    let before = restored.clone();
    assert_eq!(
        restored.prepare(6, &member_progress(group, 1, 0x61), &identities),
        Err(StateError::AssignmentMemberMismatch)
    );
    assert_eq!(restored, before);
    let plan = restored
        .prepare(6, &member_progress(group, 1, 0x62), &identities)
        .unwrap();
    restored.apply(plan, &mut identities).unwrap();
    assert_eq!(
        restored.progress(ProgressOwner::ConsumerGroup(group), partition()),
        Some(Offset::new(1))
    );
}
