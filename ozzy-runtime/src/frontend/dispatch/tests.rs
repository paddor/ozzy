use super::*;
use crate::dispatch::{self, Budget, Budgets, Client, Limits, Quota, Receiver};
use crate::frontend::{Placement, test_support::*};
use ozzy_proto::{EnvelopeLimits, LinkSessionId, Opcode};

fn limits() -> Limits {
    Limits {
        capacity: Budgets {
            data: Budget {
                queue_slots: 2,
                retained_messages: 4,
                bytes: 8192,
            },
            control: Budget {
                queue_slots: 1,
                retained_messages: 2,
                bytes: 4096,
            },
        },
        clients: 2,
        grants: 4,
    }
}

pub(in crate::frontend) fn fixture() -> (Dispatcher, [Receiver<Message>; 2], [Placement; 2], Binding)
{
    let placements = [placement(0, 0), placement(1, 17)];
    let table = RoutingTable::new(&[0, 17], &placements, 2, EnvelopeLimits::default()).unwrap();
    let (first, a) = dispatch::channel(limits()).unwrap();
    let (second, b) = dispatch::channel(limits()).unwrap();
    let mut dispatcher = Dispatcher::new(
        NodeId::from_bytes([9; 16]),
        table,
        vec![(0, first), (17, second)],
        DispatcherLimits {
            peers: 2,
            grants_per_class: 2,
            replies: ReplyLimits {
                control: crate::replica_transport::QueueLimits {
                    messages: 2,
                    bytes: 8192,
                    message_bytes: 4096,
                },
                data: crate::replica_transport::QueueLimits {
                    messages: 2,
                    bytes: 8192,
                    message_bytes: 4096,
                },
            },
        },
    )
    .unwrap();
    let binding = binding(Kind::Client);
    dispatcher.bind(binding).unwrap();
    (dispatcher, [a, b], placements, binding)
}

fn install(
    dispatcher: &mut Dispatcher,
    lane: &mut Receiver<Message>,
    binding: Binding,
    placement: Placement,
    class: Class,
) -> Client {
    let client = lane
        .credits()
        .client(binding.session, limits().capacity)
        .unwrap();
    let quota = match class {
        Class::Data => Quota {
            messages: 2,
            bytes: 8192,
        },
        Class::Control => Quota {
            messages: 1,
            bytes: 4096,
        },
    };
    let grant = lane.credits().grant(&client, class, quota).unwrap();
    dispatcher
        .install(
            binding.peer,
            Subject {
                group: placement.group,
                writer: (class == Class::Data).then_some(ProducerId::from_bytes([4; 16])),
            },
            grant,
        )
        .unwrap();
    client
}

#[test]
fn full_shard_keeps_healthy_shard_and_control_progress_without_spill_queue() {
    let (mut dispatcher, mut lanes, placements, binding) = fixture();
    let stalled = install(
        &mut dispatcher,
        &mut lanes[0],
        binding,
        placements[0],
        Class::Data,
    );
    let healthy = install(
        &mut dispatcher,
        &mut lanes[1],
        binding,
        placements[1],
        Class::Data,
    );
    let control_client = install(
        &mut dispatcher,
        &mut lanes[0],
        binding,
        placements[0],
        Class::Control,
    );
    for _ in 0..2 {
        dispatcher
            .dispatch(binding.peer, append(placements[0], binding), 4096)
            .unwrap();
    }
    let unsent = append(placements[0], binding);
    let error = dispatcher
        .dispatch(binding.peer, unsent.clone(), 4096)
        .unwrap_err();
    assert!(matches!(
        error.reason,
        Rejection::Admission(SendFailure::Admission(dispatch::Error::Full))
    ));
    assert_eq!(error.message.part_slice(3), unsent.part_slice(3));
    dispatcher
        .dispatch(
            binding.peer,
            reader(placements[0], binding, Opcode::Subscribe),
            4096,
        )
        .unwrap();
    dispatcher
        .dispatch(binding.peer, append(placements[1], binding), 4096)
        .unwrap();
    let work = lanes[1].try_recv().unwrap().unwrap();
    assert_eq!(work.class, Class::Data);
    assert_eq!(stalled.usage(Class::Data).queue_slots, 2);
    assert_eq!(healthy.usage(Class::Data).queue_slots, 1); // One unused grant remains.
    let held = work.retention.attach(work.value.part_bytes(3).unwrap());
    drop(work.value);
    for class in [Class::Data, Class::Data, Class::Control] {
        assert_eq!(lanes[0].try_recv().unwrap().unwrap().class, class);
    }
    assert_eq!(control_client.usage(Class::Control), Budget::default());
    assert_eq!(stalled.usage(Class::Data), Budget::default());
    assert_eq!(healthy.usage(Class::Data).bytes, 8192);
    drop(held);
    assert_eq!(healthy.usage(Class::Data).bytes, 4096);
}

#[test]
fn revoked_grants_free_subject_slots_without_releasing_admitted_bytes() {
    let (mut dispatcher, mut lanes, placements, binding) = fixture();
    dispatcher.limits.grants_per_class = 1;
    let client = install(
        &mut dispatcher,
        &mut lanes[0],
        binding,
        placements[0],
        Class::Data,
    );
    let old = Subject {
        group: placements[0].group,
        writer: Some(ProducerId::from_bytes([4; 16])),
    };
    let key = dispatcher.peers[&binding.peer].grants[&(old.into(), Class::Data)]
        .grant
        .key();
    dispatcher
        .dispatch(binding.peer, append(placements[0], binding), 4096)
        .unwrap();
    let held = lanes[0].try_recv().unwrap().unwrap();
    lanes[0].credits().revoke_key(&key).unwrap();
    assert_eq!(client.usage(Class::Data).bytes, 4096);
    let fresh = Subject {
        group: old.group,
        writer: Some(ProducerId::from_bytes([5; 16])),
    };
    let grant = lanes[0]
        .credits()
        .grant(
            &client,
            Class::Data,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    dispatcher.install(binding.peer, fresh, grant).unwrap();
    assert_eq!(dispatcher.peers[&binding.peer].grants.len(), 1);
    assert!(matches!(
        dispatcher
            .dispatch(binding.peer, append(placements[0], binding), 4096)
            .unwrap_err()
            .reason,
        Rejection::NoGrant
    ));
    assert_eq!(client.usage(Class::Data).bytes, 8192);
    drop(held);
    assert_eq!(client.usage(Class::Data).bytes, 4096);
}

#[test]
fn shared_control_reservation_covers_partition_count_without_covering_data_or_other_shards() {
    let (mut dispatcher, mut lanes, placements, binding) = fixture();
    let second = placement(2, 0);
    dispatcher.routes = RoutingTable::new(
        &[0, 17],
        &[placements[0], placements[1], second],
        3,
        EnvelopeLimits::default(),
    )
    .unwrap();
    let client = lanes[0]
        .credits()
        .client(binding.session, limits().capacity)
        .unwrap();
    let quota = Quota {
        messages: 1,
        bytes: 4096,
    };
    let grant = lanes[0]
        .credits()
        .grant(&client, Class::Control, quota)
        .unwrap();
    let key = grant.key();
    dispatcher
        .install(binding.peer, GrantTarget::Control(0), grant)
        .unwrap();
    for target in [placements[0], second] {
        dispatcher
            .dispatch(
                binding.peer,
                reader(target, binding, Opcode::Subscribe),
                4096,
            )
            .unwrap();
        assert_eq!(client.usage(Class::Control).queue_slots, 1);
        assert!(matches!(
            dispatcher
                .dispatch(
                    binding.peer,
                    reader(second, binding, Opcode::Subscribe),
                    4096
                )
                .unwrap_err()
                .reason,
            Rejection::Admission(SendFailure::Admission(dispatch::Error::Full))
        ));
        drop(lanes[0].try_recv().unwrap().unwrap());
        lanes[0].credits().extend(&key, quota).unwrap();
    }
    assert!(matches!(
        dispatcher
            .dispatch(
                binding.peer,
                reader(placements[1], binding, Opcode::Subscribe),
                4096
            )
            .unwrap_err()
            .reason,
        Rejection::NoGrant
    ));
    assert!(matches!(
        dispatcher
            .dispatch(binding.peer, append(placements[0], binding), 4096)
            .unwrap_err()
            .reason,
        Rejection::NoGrant
    ));
    let data = lanes[0]
        .credits()
        .grant(&client, Class::Data, quota)
        .unwrap();
    assert_eq!(
        dispatcher
            .install(binding.peer, GrantTarget::Control(0), data)
            .unwrap_err()
            .0,
        SetupError::Binding
    );
    lanes[0].credits().revoke_key(&key).unwrap();
    let grant = lanes[0]
        .credits()
        .grant(&client, Class::Control, quota)
        .unwrap();
    dispatcher
        .install(binding.peer, GrantTarget::Control(0), grant)
        .unwrap();
    dispatcher
        .dispatch(
            binding.peer,
            reader(second, binding, Opcode::Subscribe),
            4096,
        )
        .unwrap();
}

#[test]
fn reconnect_fences_old_grants_and_preserves_already_queued_charges() {
    let (mut dispatcher, mut lanes, placements, old) = fixture();
    let mut client = install(
        &mut dispatcher,
        &mut lanes[0],
        old,
        placements[0],
        Class::Data,
    );
    dispatcher
        .dispatch(old.peer, append(placements[0], old), 4096)
        .unwrap();
    assert!(!dispatcher.bind(old).unwrap());
    let new = Binding {
        session: LinkSessionId::from_bytes([99; 16]),
        ..old
    };
    assert!(dispatcher.bind(new).unwrap());
    assert_eq!(
        client.usage(Class::Data),
        Budget {
            queue_slots: 1,
            retained_messages: 1,
            bytes: 4096
        }
    );
    assert!(matches!(
        dispatcher
            .dispatch(old.peer, append(placements[0], old), 4096)
            .unwrap_err()
            .reason,
        Rejection::Routing(RoutingError::Session)
    ));
    assert!(matches!(
        dispatcher
            .dispatch(new.peer, append(placements[0], new), 4096)
            .unwrap_err()
            .reason,
        Rejection::NoGrant
    ));
    client.replace_session(new.session).unwrap();
    let grant = lanes[0]
        .credits()
        .grant(
            &client,
            Class::Data,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    dispatcher
        .install(
            new.peer,
            Subject {
                group: placements[0].group,
                writer: Some(ProducerId::from_bytes([4; 16])),
            },
            grant,
        )
        .unwrap();
    dispatcher
        .dispatch(new.peer, append(placements[0], new), 4096)
        .unwrap();
    let first = lanes[0].try_recv().unwrap().unwrap();
    let second = lanes[0].try_recv().unwrap().unwrap();
    assert_eq!(first.session, old.session);
    assert_eq!(second.session, new.session);
    dispatcher.disconnect(new.peer);
    assert_eq!(client.usage(Class::Data).bytes, 8192);
    drop((first, second));
    assert_eq!(client.usage(Class::Data), Budget::default());
}

#[test]
fn foreign_or_stale_grants_cannot_be_installed_and_rejections_preserve_tokens() {
    let (mut dispatcher, mut lanes, placements, binding) = fixture();
    let client = lanes[0]
        .credits()
        .client(binding.session, limits().capacity)
        .unwrap();
    let grant = lanes[0]
        .credits()
        .grant(
            &client,
            Class::Data,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    let subject = Subject {
        group: placements[1].group,
        writer: Some(ProducerId::from_bytes([4; 16])),
    };
    let (error, grant) = dispatcher
        .install(binding.peer, subject, grant)
        .unwrap_err();
    assert_eq!(error, SetupError::Destination);
    assert_eq!(grant.remaining().bytes, 4096);
    let subject = Subject {
        group: placements[0].group,
        ..subject
    };
    dispatcher.install(binding.peer, subject, grant).unwrap();
    let duplicate = lanes[0]
        .credits()
        .grant(
            &client,
            Class::Data,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    let (error, duplicate) = dispatcher
        .install(binding.peer, subject, duplicate)
        .unwrap_err();
    assert_eq!(error, SetupError::Duplicate);
    assert!(duplicate.is_live());
    let undercharged = dispatcher
        .dispatch(binding.peer, append(placements[0], binding), 1)
        .unwrap_err();
    assert!(matches!(undercharged.reason, Rejection::Charge));
    assert_eq!(client.usage(Class::Data).queue_slots, 2);
    assert!(lanes[0].try_recv().unwrap().is_none());
}

#[test]
fn dequeued_omq_frames_keep_charges_through_aliases_and_empty_parts() {
    for empty in [false, true] {
        let (mut dispatcher, mut lanes, placements, binding) = fixture();
        let class = if empty { Class::Control } else { Class::Data };
        let client = install(
            &mut dispatcher,
            &mut lanes[0],
            binding,
            placements[0],
            class,
        );
        let message = if empty {
            reader(placements[0], binding, Opcode::Subscribe)
        } else {
            append(placements[0], binding)
        };
        dispatcher.dispatch(binding.peer, message, 4096).unwrap();
        let message = lanes[0]
            .try_recv()
            .unwrap()
            .unwrap()
            .into_retained_message();
        let frame = message.part_bytes(3).unwrap();
        assert_eq!(frame.is_empty(), empty);
        let alias = if empty {
            frame.clone()
        } else {
            frame.slice(..1)
        };
        dispatcher.disconnect(binding.peer);
        drop((message, frame));
        assert_eq!(
            client.usage(class),
            Budget {
                queue_slots: 0,
                retained_messages: 1,
                bytes: 4096
            }
        );
        std::thread::spawn(move || drop(alias)).join().unwrap();
        assert_eq!(client.usage(class), Budget::default());
    }
}

#[test]
fn peer_and_data_token_tables_are_bounded_without_consuming_control_entries() {
    let (mut dispatcher, mut lanes, placements, binding) = fixture();
    dispatcher.limits.grants_per_class = 1;
    let _first = install(
        &mut dispatcher,
        &mut lanes[0],
        binding,
        placements[0],
        Class::Data,
    );
    let client = lanes[1]
        .credits()
        .client(binding.session, limits().capacity)
        .unwrap();
    let grant = lanes[1]
        .credits()
        .grant(
            &client,
            Class::Data,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    let subject = Subject {
        group: placements[1].group,
        writer: Some(ProducerId::from_bytes([4; 16])),
    };
    let (error, grant) = dispatcher
        .install(binding.peer, subject, grant)
        .unwrap_err();
    assert_eq!(error, SetupError::Full);
    drop(grant);
    assert_eq!(client.usage(Class::Data), Budget::default());
    let _control = install(
        &mut dispatcher,
        &mut lanes[0],
        binding,
        placements[0],
        Class::Control,
    );
    dispatcher
        .bind(Binding {
            peer: NodeId::from_bytes([7; 16]),
            ..binding
        })
        .unwrap();
    assert_eq!(
        dispatcher.bind(Binding {
            peer: NodeId::from_bytes([8; 16]),
            ..binding
        }),
        Err(SetupError::Full)
    );
    let unknown = dispatcher
        .dispatch(
            NodeId::from_bytes([8; 16]),
            append(placements[0], binding),
            4096,
        )
        .unwrap_err();
    assert!(matches!(unknown.reason, Rejection::Peer));
    dispatcher
        .dispatch(
            binding.peer,
            reader(placements[0], binding, Opcode::Subscribe),
            4096,
        )
        .unwrap();
    assert_eq!(lanes[0].try_recv().unwrap().unwrap().class, Class::Control);
}
