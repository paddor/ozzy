use super::*;

fn peer(value: u8) -> NodeId {
    NodeId::from_bytes([value; 16])
}

fn session(value: u8) -> LinkSessionId {
    LinkSessionId::from_bytes([value; 16])
}

fn watch(value: u8) -> RequestId {
    RequestId::from_bytes([value; 16])
}

fn route(view: u64, leader: Option<NodeId>) -> RouteState {
    let partition = &crate::topic_metadata::tests::page(0, 4).partitions[0];
    RouteState {
        group: partition.group,
        config_epoch: partition.config_epoch,
        partition: partition.incarnation,
        members: partition.members.clone().into(),
        view,
        leader,
    }
}

fn cache() -> RouteCache {
    RouteCache::new(
        TopicMetadata::from_pages([crate::topic_metadata::tests::page(0, 4)], 4).unwrap(),
        3,
        4,
    )
    .unwrap()
}

#[test]
fn delayed_snapshot_preserves_newer_update_and_reconnect_fences_old_watch() {
    let mut cache = cache();
    let group = route(0, None).group;
    cache.bind(peer(1), session(4)).unwrap();
    cache
        .register(peer(1), session(4), watch(5), &[group])
        .unwrap();
    cache
        .update(
            peer(1),
            session(4),
            Update {
                watch: watch(5),
                route: route(8, Some(peer(2))),
            },
        )
        .unwrap();
    cache
        .snapshot(
            peer(1),
            session(4),
            Snapshot {
                watch: watch(5),
                routes: vec![route(7, Some(peer(1)))],
            },
        )
        .unwrap();
    assert_eq!(cache.leader(group), Ok(Some(peer(2))));
    cache.bind(peer(1), session(6)).unwrap();
    assert_eq!(cache.leader(group), Ok(None));
    assert_eq!(
        cache.update(
            peer(1),
            session(4),
            Update {
                watch: watch(5),
                route: route(9, Some(peer(3)))
            }
        ),
        Err(RouteCacheError::Stale)
    );
    assert!(!cache.disconnect(peer(1), session(4)));
}

#[test]
fn surviving_broker_supplies_new_leader_without_dead_broker_notice() {
    let mut cache = cache();
    let group = route(0, None).group;
    for broker in [peer(1), peer(2)] {
        cache.bind(broker, session(4)).unwrap();
        cache
            .register(broker, session(4), watch(5), &[group])
            .unwrap();
        cache
            .snapshot(
                broker,
                session(4),
                Snapshot {
                    watch: watch(5),
                    routes: vec![route(7, Some(peer(1)))],
                },
            )
            .unwrap();
    }
    cache.disconnect(peer(1), session(4));
    cache
        .update(
            peer(2),
            session(4),
            Update {
                watch: watch(5),
                route: route(8, Some(peer(2))),
            },
        )
        .unwrap();
    assert_eq!(cache.leader(group), Ok(Some(peer(2))));
    cache
        .resync(peer(2), session(4), Resync { watch: watch(5) })
        .unwrap();
    assert!(cache.needs_snapshot(peer(2), watch(5)));
    assert_eq!(cache.leader(group), Ok(None));
}

#[test]
fn validates_membership_before_view_and_refines_unknown_leader() {
    let mut cache = cache();
    let group = route(0, None).group;
    cache.bind(peer(1), session(4)).unwrap();
    cache
        .register(peer(1), session(4), watch(5), &[group])
        .unwrap();
    cache
        .snapshot(
            peer(1),
            session(4),
            Snapshot {
                watch: watch(5),
                routes: vec![route(7, None)],
            },
        )
        .unwrap();
    cache
        .update(
            peer(1),
            session(4),
            Update {
                watch: watch(5),
                route: route(7, Some(peer(2))),
            },
        )
        .unwrap();
    assert_eq!(cache.leader(group), Ok(Some(peer(2))));
    let mut foreign = route(99, Some(peer(2)));
    foreign.members = [peer(3), peer(2), peer(1)].into();
    assert_eq!(
        cache.update(
            peer(1),
            session(4),
            Update {
                watch: watch(5),
                route: foreign
            }
        ),
        Err(RouteCacheError::Invalid)
    );
    assert_eq!(
        cache.update(
            peer(1),
            session(4),
            Update {
                watch: watch(5),
                route: route(7, Some(peer(3)))
            }
        ),
        Err(RouteCacheError::Conflict)
    );
    assert_eq!(cache.leader(group), Ok(Some(peer(2))));
}

#[test]
fn highest_view_resolves_old_conflicts_and_bad_snapshot_is_atomic() {
    let mut cache = cache();
    let group = route(0, None).group;
    for (broker, view, leader) in [
        (peer(1), 7, peer(1)),
        (peer(2), 7, peer(2)),
        (peer(3), 8, peer(3)),
    ] {
        cache.bind(broker, session(4)).unwrap();
        cache
            .register(broker, session(4), watch(5), &[group])
            .unwrap();
        cache
            .snapshot(
                broker,
                session(4),
                Snapshot {
                    watch: watch(5),
                    routes: vec![route(view, Some(leader))],
                },
            )
            .unwrap();
    }
    assert_eq!(cache.leader(group), Ok(Some(peer(3))));
    assert_eq!(
        cache.snapshot(
            peer(3),
            session(4),
            Snapshot {
                watch: watch(5),
                routes: vec![]
            }
        ),
        Err(RouteCacheError::Conflict)
    );
    assert_eq!(cache.leader(group), Ok(Some(peer(3))));
}

#[test]
fn foreign_configuration_cannot_replace_a_trusted_hint_at_any_view() {
    let mut cache = cache();
    let current = route(7, Some(peer(1)));
    cache.bind(peer(1), session(4)).unwrap();
    cache
        .register(peer(1), session(4), watch(5), &[current.group])
        .unwrap();
    cache
        .snapshot(
            peer(1),
            session(4),
            Snapshot {
                watch: watch(5),
                routes: vec![current.clone()],
            },
        )
        .unwrap();
    let mut foreign = route(u64::MAX, Some(peer(2)));
    foreign.config_epoch += 1;
    assert_eq!(
        cache.update(
            peer(1),
            session(4),
            Update {
                watch: watch(5),
                route: foreign.clone(),
            }
        ),
        Err(RouteCacheError::Invalid)
    );
    assert_eq!(
        cache.snapshot(
            peer(1),
            session(4),
            Snapshot {
                watch: watch(5),
                routes: vec![foreign],
            }
        ),
        Err(RouteCacheError::Invalid)
    );
    assert_eq!(cache.leader(current.group), Ok(current.leader));
    assert!(!cache.needs_snapshot(peer(1), watch(5)));
}

fn install_watch(cache: &mut RouteCache, broker: NodeId, id: RequestId, numbers: &[u32]) {
    let routes: Vec<_> = numbers
        .iter()
        .map(|&number| {
            let partition = cache.topic().partition(number).unwrap();
            RouteState {
                group: partition.group,
                config_epoch: partition.config_epoch,
                partition: partition.incarnation,
                members: partition.members.clone().into(),
                view: 7,
                leader: Some(broker),
            }
        })
        .collect();
    let groups = routes.iter().map(|route| route.group).collect::<Vec<_>>();
    cache.register(broker, session(4), id, &groups).unwrap();
    cache
        .snapshot(broker, session(4), Snapshot { watch: id, routes })
        .unwrap();
}

#[test]
fn overlapping_partition_watches_survive_exact_unregister_resync_and_rebind() {
    let mut cache = cache();
    let groups = (0..4)
        .map(|number| cache.topic().partition(number).unwrap().group)
        .collect::<Vec<_>>();
    cache.bind(peer(1), session(4)).unwrap();
    cache.bind(peer(2), session(4)).unwrap();
    install_watch(&mut cache, peer(1), watch(5), &[0, 1]);
    install_watch(&mut cache, peer(1), watch(6), &[1, 2]);
    install_watch(&mut cache, peer(2), watch(7), &[3]);
    assert_eq!(cache.leader(groups[0]), Ok(Some(peer(1))));
    assert_eq!(cache.leader(groups[1]), Ok(Some(peer(1))));
    assert_eq!(cache.leader(groups[2]), Ok(Some(peer(1))));
    assert_eq!(cache.leader(groups[3]), Ok(Some(peer(2))));

    assert!(cache.unregister(peer(1), session(4), watch(5)).unwrap());
    assert!(!cache.unregister(peer(1), session(4), watch(5)).unwrap());
    assert_eq!(cache.leader(groups[0]), Ok(None));
    assert_eq!(cache.leader(groups[1]), Ok(Some(peer(1))));
    cache
        .resync(peer(1), session(4), Resync { watch: watch(6) })
        .unwrap();
    assert_eq!(cache.leader(groups[1]), Ok(None));
    assert_eq!(cache.leader(groups[2]), Ok(None));
    assert_eq!(cache.leader(groups[3]), Ok(Some(peer(2))));
    install_watch(&mut cache, peer(1), watch(6), &[1, 2]);
    assert_eq!(cache.leader(groups[1]), Ok(Some(peer(1))));

    cache.bind(peer(1), session(8)).unwrap();
    assert_eq!(cache.leader(groups[1]), Ok(None));
    assert_eq!(cache.leader(groups[2]), Ok(None));
    assert_eq!(cache.leader(groups[3]), Ok(Some(peer(2))));
    assert!(!cache.disconnect(peer(1), session(4)));
    assert!(cache.disconnect(peer(2), session(4)));
    assert_eq!(cache.leader(groups[3]), Ok(None));
}
