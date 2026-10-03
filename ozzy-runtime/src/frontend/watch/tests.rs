use super::*;
use ozzy_proto::PartitionIncarnation;

fn peer(value: u8) -> NodeId {
    NodeId::from_bytes([value; 16])
}

fn session(value: u8) -> LinkSessionId {
    LinkSessionId::from_bytes([value; 16])
}

fn group(value: u8) -> GroupId {
    GroupId::from_bytes([value; 16])
}

fn state(value: u8) -> RouteState {
    RouteState {
        group: group(value),
        config_epoch: 1,
        partition: PartitionIncarnation::from_bytes([value + 10; 16]),
        members: [peer(1), peer(2), peer(3)].into(),
        view: 0,
        leader: None,
    }
}

fn registry(pending: usize) -> WatchRegistry {
    WatchRegistry::new(
        WatchLimits {
            partitions: 3,
            peers: 2,
            registrations: 3,
            interests_per_registration: 3,
            pending_per_registration: pending,
        },
        [state(20), state(21), state(22)],
    )
    .unwrap()
}

fn id(value: u8) -> RequestId {
    RequestId::from_bytes([value; 16])
}

#[test]
fn snapshot_limit_rejection_preserves_watch_slots_and_existing_notifications() {
    let mut registry = registry(2);
    registry.bind(peer(9), session(7)).unwrap();
    assert_eq!(
        registry.register_bounded(peer(9), session(7), id(5), &[group(20)], 1),
        Err(WatchError::Invalid)
    );
    assert!(!registry.unregister(peer(9), session(7), id(5)).unwrap());
    registry
        .register(peer(9), session(7), id(5), &[group(20)])
        .unwrap();
    let mut newer = state(20);
    newer.view = 1;
    newer.leader = Some(peer(2));
    registry.publish(&newer).unwrap();
    assert_eq!(
        registry.register_bounded(peer(9), session(7), id(5), &[group(20)], 1),
        Err(WatchError::Invalid)
    );
    assert_eq!(
        registry.take(peer(9), session(7), id(5), 1).unwrap(),
        WatchUpdates::Updates(vec![newer])
    );
}

#[test]
fn registration_snapshot_and_updates_cannot_miss_an_intervening_view() {
    let mut registry = registry(2);
    registry.bind(peer(9), session(7)).unwrap();
    let snapshot = registry
        .register(peer(9), session(7), id(5), &[group(20), group(21)])
        .unwrap();
    assert_eq!(snapshot.iter().map(|r| r.view).collect::<Vec<_>>(), [0, 0]);
    let mut update = state(20);
    update.view = 1;
    update.leader = Some(peer(1));
    assert!(registry.publish(&update).unwrap());
    // Repeated updates to one partition occupy one pending slot.
    update.view = 2;
    update.leader = Some(peer(2));
    assert!(registry.publish(&update).unwrap());
    assert_eq!(
        registry.take(peer(9), session(7), id(5), 1).unwrap(),
        WatchUpdates::Updates(vec![update.clone()])
    );
    assert_eq!(
        registry.take(peer(9), session(7), id(5), 1).unwrap(),
        WatchUpdates::Updates(vec![])
    );
    // A retry carries the latest state even if the prior snapshot was lost.
    assert_eq!(
        registry
            .register(peer(9), session(7), id(5), &[group(20), group(21)])
            .unwrap()[0],
        update
    );
}

#[test]
fn overflow_requires_resnapshot_and_cannot_look_caught_up() {
    let mut registry = registry(1);
    registry.bind(peer(9), session(7)).unwrap();
    registry
        .register(peer(9), session(7), id(5), &[group(20), group(21)])
        .unwrap();
    for value in [20, 21] {
        let mut update = state(value);
        update.view = 1;
        update.leader = Some(peer(1));
        registry.publish(&update).unwrap();
    }
    assert_eq!(
        registry.take(peer(9), session(7), id(5), 4).unwrap(),
        WatchUpdates::Resync
    );
    assert_eq!(
        registry.take(peer(9), session(7), id(5), 4).unwrap(),
        WatchUpdates::Resync
    );
    let snapshot = registry
        .register(peer(9), session(7), id(5), &[group(20), group(21)])
        .unwrap();
    assert_eq!(snapshot.iter().map(|r| r.view).collect::<Vec<_>>(), [1, 1]);
    assert_eq!(
        registry.take(peer(9), session(7), id(5), 4).unwrap(),
        WatchUpdates::Updates(vec![])
    );
}

#[test]
fn replacement_fences_old_session_without_fencing_another_peer() {
    let mut registry = registry(2);
    registry.bind(peer(9), session(7)).unwrap();
    registry.bind(peer(8), session(6)).unwrap();
    registry
        .register(peer(9), session(7), id(5), &[group(20)])
        .unwrap();
    registry
        .register(peer(8), session(6), id(4), &[group(20)])
        .unwrap();
    registry.bind(peer(9), session(8)).unwrap();
    assert_eq!(
        registry.register(peer(9), session(7), id(5), &[group(20)]),
        Err(WatchError::Session)
    );
    assert_eq!(
        registry.take(peer(9), session(8), id(5), 1),
        Err(WatchError::Unknown)
    );
    assert!(!registry.disconnect(peer(9), session(7)));
    let mut update = state(20);
    update.view = 1;
    update.leader = Some(peer(1));
    registry.publish(&update).unwrap();
    assert_eq!(
        registry.take(peer(8), session(6), id(4), 1).unwrap(),
        WatchUpdates::Updates(vec![update])
    );
}

#[test]
fn identity_and_view_conflicts_never_replace_trusted_state() {
    let mut registry = registry(2);
    let mut current = state(20);
    current.view = 2;
    current.leader = Some(peer(1));
    registry.publish(&current).unwrap();
    let mut stale = current.clone();
    stale.view = 1;
    assert_eq!(registry.publish(&stale), Err(WatchError::Stale));
    let mut conflict = current.clone();
    conflict.leader = Some(peer(2));
    assert_eq!(registry.publish(&conflict), Err(WatchError::Conflict));
    let mut moved = current.clone();
    moved.view = 3;
    moved.members = [peer(2), peer(1), peer(3)].into();
    assert_eq!(registry.publish(&moved), Err(WatchError::Conflict));
    let mut foreign_configuration = current.clone();
    foreign_configuration.config_epoch += 1;
    foreign_configuration.view = u64::MAX;
    assert_eq!(
        registry.publish(&foreign_configuration),
        Err(WatchError::Conflict)
    );
    let mut invalid = current.clone();
    invalid.view = 3;
    invalid.leader = Some(peer(4));
    assert_eq!(registry.publish(&invalid), Err(WatchError::Invalid));
    registry.bind(peer(9), session(7)).unwrap();
    assert_eq!(
        registry
            .register(peer(9), session(7), id(5), &[group(20)])
            .unwrap(),
        vec![current]
    );
}

#[test]
fn election_can_refine_unknown_leader_without_inventing_another_view() {
    let mut registry = registry(1);
    registry.bind(peer(9), session(7)).unwrap();
    registry
        .register(peer(9), session(7), id(5), &[group(20)])
        .unwrap();
    let mut known = state(20);
    known.leader = Some(peer(1));
    assert!(registry.publish(&known).unwrap());
    assert_eq!(
        registry.take(peer(9), session(7), id(5), 1).unwrap(),
        WatchUpdates::Updates(vec![known.clone()])
    );
    assert!(!registry.publish(&state(20)).unwrap());
    let mut conflict = known;
    conflict.leader = Some(peer(2));
    assert_eq!(registry.publish(&conflict), Err(WatchError::Conflict));
}

#[test]
fn bounded_registrations_and_duplicate_ids_do_not_expand_state() {
    let mut registry = registry(1);
    registry.bind(peer(9), session(7)).unwrap();
    for value in [1, 2, 3] {
        registry
            .register(peer(9), session(7), id(value), &[group(20)])
            .unwrap();
    }
    assert_eq!(
        registry.register(peer(9), session(7), id(4), &[group(20)]),
        Err(WatchError::Full)
    );
    assert_eq!(
        registry.register(peer(9), session(7), id(1), &[group(21)]),
        Err(WatchError::Conflict)
    );
    assert!(registry.unregister(peer(9), session(7), id(1)).unwrap());
    registry
        .register(peer(9), session(7), id(4), &[group(21)])
        .unwrap();
    assert_eq!(
        registry.register(peer(9), session(7), id(5), &[group(21), group(21)]),
        Err(WatchError::Invalid)
    );
}

#[test]
fn queued_notice_advances_only_after_successful_outbox_admission() {
    let mut registry = registry(1);
    registry.bind(peer(9), session(7)).unwrap();
    registry
        .register(peer(9), session(7), id(5), &[group(20), group(21)])
        .unwrap();
    let mut update = state(20);
    update.view = 1;
    update.leader = Some(peer(1));
    registry.publish(&update).unwrap();
    let notice = registry.next_notice().unwrap();
    assert_eq!(notice.update, Some(update.clone()));
    // A full outgoing queue keeps the same latest hint without an overflow copy.
    assert_eq!(registry.next_notice(), Some(notice.clone()));
    update.view = 2;
    registry.publish(&update).unwrap();
    assert_eq!(registry.queued(&notice), Err(WatchError::Stale));
    let latest = registry.next_notice().unwrap();
    assert_eq!(latest.update, Some(update));
    registry.queued(&latest).unwrap();
    assert_eq!(registry.next_notice(), None);
}
