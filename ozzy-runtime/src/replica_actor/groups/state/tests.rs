use super::*;
use ozzy_core::state::group_targets;
use ozzy_proto::{ConsumerGroupId, append::Authority, group::JoinRequest};

fn member(id: u8) -> GroupMember {
    GroupMember {
        id: ConsumerMemberId::from_bytes([id; 16]),
        node: NodeId::from_bytes([id; 16]),
        join: OperationId::from_bytes([id; 16]),
    }
}

fn moved_plan() -> CoordinatorGroup {
    let partitions = [1, 2].map(|id| PartitionIncarnation::from_bytes([id; 16]));
    let mut image = CoordinatorGroup {
        coordinator: partitions[0],
        group: ConsumerGroupId::from_bytes([3; 16]),
        name: "workers".into(),
        revision: 1,
        initial: Start::Earliest,
        members: vec![member(2)],
        targets: group_targets(None, &[member(2)], &partitions, 1).unwrap(),
        operation: OperationId::from_bytes([4; 16]),
    };
    let members = vec![member(1), member(2)];
    image.targets = group_targets(Some(&image), &members, &partitions, 2).unwrap();
    image.members = members;
    image.revision = 2;
    image
}

fn heartbeat(image: &CoordinatorGroup, id: u8, owned: Vec<Owned>) -> JoinRequest {
    let instance = member(id);
    JoinRequest {
        authority: Authority {
            group_id: ozzy_proto::GroupId::from_bytes([1; 16]),
            config_epoch: 1,
            view: 2,
        },
        coordinator: image.coordinator,
        name: image.name.clone(),
        member: instance.id,
        join: instance.join,
        group: Some(image.group),
        mode: Mode::Heartbeat,
        initial: image.initial,
        owned,
    }
}

fn target(state: &mut GroupState, image: &CoordinatorGroup, now: Duration) -> Target {
    let Disposition::Reply(ozzy_proto::group::Status::Member, targets) = state.observe(
        Some(image),
        heartbeat(image, 1, Vec::new()),
        member(1).node,
        now,
        Duration::from_secs(5),
        &|_| Some(0),
    ) else {
        panic!("committed member must receive its persisted target")
    };
    assert_eq!(targets.len(), 1);
    targets[0]
}

#[test]
fn recovered_plan_waits_for_prior_ownership_report_then_explicit_release() {
    let image = moved_plan();
    let now = Duration::from_secs(20);
    let mut state = GroupState::default();
    state.adopt(&image, now);
    let moved = target(&mut state, &image, now);
    assert_eq!(moved.plan_revision, 2);
    assert_eq!(moved.prior_member, Some(member(2).id));
    assert!(!moved.activatable, "unknown ownership is not a release");
    let owned = Owned {
        partition: moved.partition,
        plan_revision: 1,
        epoch: 7,
    };
    state.observe(
        Some(&image),
        heartbeat(&image, 2, vec![owned]),
        member(2).node,
        now,
        Duration::from_secs(5),
        &|_| Some(0),
    );
    assert!(!target(&mut state, &image, now).activatable);
    state.observe(
        Some(&image),
        heartbeat(&image, 2, Vec::new()),
        member(2).node,
        now,
        Duration::from_secs(5),
        &|_| Some(0),
    );
    assert!(target(&mut state, &image, now).activatable);
}

#[test]
fn recovered_plan_gives_unknown_owner_a_full_drain_deadline() {
    let image = moved_plan();
    let now = Duration::from_secs(20);
    let mut state = GroupState::default();
    state.adopt(&image, now);
    assert!(!target(&mut state, &image, now).activatable);
    assert!(!target(&mut state, &image, now + Duration::from_millis(4999)).activatable);
    assert!(target(&mut state, &image, now + Duration::from_secs(5)).activatable);
}
