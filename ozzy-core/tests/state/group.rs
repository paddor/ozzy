use super::*;
use ozzy_core::state::{CoordinatorGroup, StateSnapshotLimits, group_targets};
use ozzy_journal::operation::{GroupDelete, GroupMember, GroupPlan};
use ozzy_proto::{NodeId, group::Start};

fn member(id: u8, join: u8) -> GroupMember {
    GroupMember {
        id: ConsumerMemberId::from_bytes([id; 16]),
        node: NodeId::from_bytes([70; 16]),
        join: operation(join),
    }
}

fn topology(count: usize) -> Vec<PartitionIncarnation> {
    (0..count)
        .map(|i| {
            if i == 0 {
                partition()
            } else {
                PartitionIncarnation::from_bytes([i as u8; 16])
            }
        })
        .collect()
}

fn plan(
    prior: Option<&CoordinatorGroup>,
    members: Vec<GroupMember>,
    count: usize,
) -> OperationBody<'static> {
    let revision = prior.map_or(1, |g| g.revision + 1);
    let targets = group_targets(prior, &members, &topology(count), revision).unwrap();
    OperationBody::GroupPlan(GroupPlan {
        coordinator: partition(),
        group: ConsumerGroupId::from_bytes([61; 16]),
        name: "workers",
        expected_revision: revision - 1,
        revision,
        initial: Start::Earliest,
        members,
        targets,
        operation_id: operation(100 + revision as u8),
    })
}

fn initialized(limits: StateLimits) -> CanonicalImages {
    let mut images = CanonicalImages::new(limits, 32, 8);
    images
        .admit(images.speculative().revision() + 1, &create())
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    images
}

fn image(images: &CanonicalImages) -> &CoordinatorGroup {
    images
        .speculative()
        .coordinator_group(ConsumerGroupId::from_bytes([61; 16]))
        .unwrap()
        .as_ref()
}

#[test]
fn group_membership_and_plan_are_atomic_durable_and_rollback_with_the_suffix() {
    let mut images = initialized(StateLimits::default());
    let first = plan(None, vec![member(2, 2)], 7);
    images
        .admit(images.speculative().revision() + 1, &first)
        .unwrap();
    assert!(
        images
            .committed()
            .coordinator_group(ConsumerGroupId::from_bytes([61; 16]))
            .is_none()
    );
    assert!(
        image(&images)
            .targets
            .iter()
            .all(|t| t.plan_revision == 1 && t.member == Some(member(2, 2)))
    );
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    let second = plan(Some(image(&images)), vec![member(1, 1), member(2, 2)], 7);
    images
        .admit(images.speculative().revision() + 1, &second)
        .unwrap();
    let target = image(&images);
    assert_eq!(
        target
            .targets
            .iter()
            .filter(|t| t.member == Some(member(1, 1)))
            .count(),
        3
    );
    assert_eq!(
        target
            .targets
            .iter()
            .filter(|t| t.member == Some(member(2, 2)))
            .count(),
        4
    );
    assert_eq!(
        target
            .targets
            .iter()
            .filter(|t| t.plan_revision == 1 && t.prior_member.is_none())
            .count(),
        4
    );
    assert!(
        target
            .targets
            .iter()
            .filter(|t| t.plan_revision == 2)
            .all(|t| t.prior_member == Some(member(2, 2)))
    );
    images.discard_suffix();
    assert_eq!(image(&images).revision, 1);
    images
        .admit(images.speculative().revision() + 1, &second)
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    let state = images.committed();
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
    let resumed = restored
        .coordinator_group_by_name(partition(), "workers")
        .unwrap()
        .as_ref();
    assert_eq!(resumed, image(&images));
    let next = plan(Some(resumed), vec![member(1, 1)], 7);
    images
        .admit(images.speculative().revision() + 1, &next)
        .unwrap();
    assert_eq!(
        image(&images)
            .targets
            .iter()
            .filter(|t| t.plan_revision == 3 && t.prior_member == Some(member(2, 2)))
            .count(),
        4
    );
    assert_eq!(
        image(&images)
            .targets
            .iter()
            .filter(|t| t.plan_revision == 2 && t.prior_member == Some(member(2, 2)))
            .count(),
        3
    );
}

#[test]
fn group_restart_with_the_same_member_id_fences_all_of_its_old_sources() {
    let mut images = initialized(StateLimits::default());
    images
        .admit(
            images.speculative().revision() + 1,
            &plan(None, vec![member(1, 1)], 4),
        )
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    let before = image(&images).clone();
    let next = plan(Some(&before), vec![member(1, 2)], 4);
    images
        .admit(images.speculative().revision() + 1, &next)
        .unwrap();
    assert!(
        image(&images)
            .targets
            .iter()
            .all(|target| target.plan_revision == 2
                && target.member == Some(member(1, 2))
                && target.prior_member == Some(member(1, 1)))
    );
}

#[test]
fn group_plans_reject_stale_revisions_changed_topology_unbalanced_and_conflicting_names() {
    let mut images = initialized(StateLimits::default());
    images
        .admit(
            images.speculative().revision() + 1,
            &plan(None, vec![member(1, 1), member(2, 2)], 4),
        )
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    let OperationBody::GroupPlan(next) = plan(
        Some(image(&images)),
        vec![member(1, 1), member(2, 2), member(3, 3)],
        4,
    ) else {
        unreachable!()
    };
    for (bad, expected) in [
        (
            {
                let mut bad = next.clone();
                bad.expected_revision = 0;
                bad.revision = 1;
                bad.targets.iter_mut().for_each(|t| t.plan_revision = 1);
                bad
            },
            StateError::GroupRevisionMismatch,
        ),
        (
            {
                let mut bad = next.clone();
                bad.name = "other";
                bad
            },
            StateError::GroupIdentityMismatch,
        ),
        (
            {
                let mut bad = next.clone();
                bad.targets.swap(0, 1);
                bad
            },
            StateError::GroupIdentityMismatch,
        ),
        (
            {
                let mut bad = next.clone();
                bad.targets
                    .iter_mut()
                    .for_each(|t| t.member = Some(member(1, 1)));
                bad
            },
            StateError::InvalidGroupPlan,
        ),
        (
            {
                let mut bad = next.clone();
                bad.members.swap(0, 1);
                bad
            },
            StateError::InvalidGroupPlan,
        ),
    ] {
        assert!(
            matches!(images.admit(images.speculative().revision() + 1, &OperationBody::GroupPlan(bad)), Err(CanonicalImagesError::State(actual)) if actual == expected)
        );
        assert_eq!(image(&images).revision, 1);
    }
    let OperationBody::GroupPlan(mut duplicate) = plan(None, vec![member(1, 1)], 4) else {
        unreachable!()
    };
    duplicate.group = ConsumerGroupId::from_bytes([62; 16]);
    assert!(matches!(
        images.admit(
            images.speculative().revision() + 1,
            &OperationBody::GroupPlan(duplicate)
        ),
        Err(CanonicalImagesError::State(
            StateError::GroupIdentityMismatch
        ))
    ));
}

#[test]
fn group_metadata_count_limits_are_reusable_after_member_churn() {
    let mut images = initialized(StateLimits {
        max_groups: 1,
        max_group_members: 2,
        max_group_partitions: 4,
        ..Default::default()
    });
    images
        .admit(
            images.speculative().revision() + 1,
            &plan(None, vec![member(1, 1), member(2, 2)], 4),
        )
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    let next = plan(
        Some(image(&images)),
        vec![member(1, 1), member(2, 2), member(3, 3)],
        4,
    );
    assert!(matches!(
        images.admit(images.speculative().revision() + 1, &next),
        Err(CanonicalImagesError::State(StateError::LimitExceeded(
            "group members"
        )))
    ));
    let next = plan(Some(image(&images)), vec![member(1, 1)], 4);
    images
        .admit(images.speculative().revision() + 1, &next)
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    let next = plan(Some(image(&images)), vec![member(1, 1), member(3, 3)], 4);
    images
        .admit(images.speculative().revision() + 1, &next)
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    let next = plan(Some(image(&images)), vec![], 4);
    images
        .admit(images.speculative().revision() + 1, &next)
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    assert!(
        image(&images)
            .targets
            .iter()
            .all(|target| target.member.is_none())
    );
    let next = plan(Some(image(&images)), vec![member(3, 4)], 4);
    images
        .admit(images.speculative().revision() + 1, &next)
        .unwrap();
    assert!(
        image(&images)
            .targets
            .iter()
            .all(|target| target.member == Some(member(3, 4)))
    );
}

#[test]
fn group_plans_supersede_unconfirmed_intents_and_keep_the_creation_selector() {
    let mut images = initialized(StateLimits::default());
    let OperationBody::GroupPlan(mut first) = plan(None, vec![member(1, 1)], 4) else {
        unreachable!()
    };
    first.initial = Start::Latest;
    images
        .admit(2, &OperationBody::GroupPlan(first.clone()))
        .unwrap();
    images.commit_through(2).unwrap();
    let next = plan(Some(image(&images)), vec![member(1, 1), member(2, 2)], 4);
    let OperationBody::GroupPlan(mut changed) = next.clone() else {
        unreachable!()
    };
    changed.initial = Start::Earliest;
    assert!(matches!(
        images.admit(3, &OperationBody::GroupPlan(changed)),
        Err(CanonicalImagesError::State(
            StateError::GroupIdentityMismatch
        ))
    ));
    let OperationBody::GroupPlan(mut next) = next else {
        unreachable!()
    };
    next.initial = Start::Latest;
    images.admit(3, &OperationBody::GroupPlan(next)).unwrap();
    images.commit_through(3).unwrap();
    assert_eq!(image(&images).initial, Start::Latest);
    assert_eq!(
        image(&images)
            .targets
            .iter()
            .filter(|t| t.plan_revision == 2)
            .count(),
        2
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
    assert_eq!(
        restored
            .coordinator_group(image(&images).group)
            .unwrap()
            .as_ref(),
        image(&images)
    );
    assert_eq!(
        restored
            .coordinator_group(image(&images).group)
            .unwrap()
            .target(partition())
            .unwrap()
            .plan_revision,
        1
    );
}

fn deletion(revision: u64, op: u8) -> OperationBody<'static> {
    OperationBody::GroupDelete(GroupDelete {
        coordinator: partition(),
        group: ConsumerGroupId::from_bytes([61; 16]),
        revision,
        operation_id: operation(op),
    })
}

#[test]
fn group_deletion_retires_the_incarnation_and_frees_its_name_and_counts() {
    let mut images = initialized(StateLimits {
        max_groups: 1,
        max_group_members: 2,
        max_group_partitions: 4,
        ..Default::default()
    });
    images
        .admit(
            images.speculative().revision() + 1,
            &plan(None, vec![member(1, 1), member(2, 2)], 4),
        )
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    assert!(matches!(
        images.admit(images.speculative().revision() + 1, &deletion(2, 200)),
        Err(CanonicalImagesError::State(
            StateError::GroupRevisionMismatch
        ))
    ));
    images
        .admit(images.speculative().revision() + 1, &deletion(1, 200))
        .unwrap();
    let retired = ConsumerGroupId::from_bytes([61; 16]);
    assert!(images.speculative().coordinator_group(retired).is_none());
    assert!(images.committed().coordinator_group(retired).is_some());
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    assert!(
        images
            .committed()
            .coordinator_group_by_name(partition(), "workers")
            .is_none()
    );
    assert!(matches!(
        images.admit(images.speculative().revision() + 1, &deletion(1, 201)),
        Err(CanonicalImagesError::State(
            StateError::GroupIdentityMismatch
        ))
    ));
    // The name, the only group slot, and every member and source count are
    // free for a new incarnation.
    let members = vec![member(1, 3), member(2, 4)];
    let recreated = OperationBody::GroupPlan(GroupPlan {
        coordinator: partition(),
        group: ConsumerGroupId::from_bytes([62; 16]),
        name: "workers",
        expected_revision: 0,
        revision: 1,
        initial: Start::Latest,
        targets: group_targets(None, &members, &topology(4), 1).unwrap(),
        members,
        operation_id: operation(202),
    });
    images
        .admit(images.speculative().revision() + 1, &recreated)
        .unwrap();
    images
        .commit_through(images.speculative().revision())
        .unwrap();
    let state = images.committed();
    let group = state
        .coordinator_group_by_name(partition(), "workers")
        .unwrap();
    assert_eq!(group.group, ConsumerGroupId::from_bytes([62; 16]));
    assert_eq!(group.initial, Start::Latest);
    let bytes = state
        .encode_snapshot(StateSnapshotLimits::default())
        .unwrap();
    let restored = CanonicalState::decode_snapshot(
        &bytes,
        StateLimits {
            max_groups: 1,
            max_group_members: 2,
            max_group_partitions: 4,
            ..Default::default()
        },
        StateSnapshotLimits::default(),
    )
    .unwrap();
    assert_eq!(&restored, state);
}
