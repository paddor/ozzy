use super::*;
use crate::{
    dispatch::Budget,
    frontend::{
        Access, Dispatcher, DispatcherLimits, Kind, LinkIds, LinkSessions, ReplyLimits, Service,
        service::tests::{begin, remote},
        test_support,
    },
    memory::{Domain, Limits},
    replica_transport::QueueLimits,
};
use ozzy_proto::{ProducerId, handshake};
use std::{num::NonZeroU64, task::Waker};

mod deferred;

#[test]
fn completed_writer_returns_one_slot_while_other_credit_remains() {
    let mut f = fixture(32768, 8192);
    f.intake.client_window = 3;
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    f.service.receive(f.message(), 1024).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    let slot = received.reservation;
    drop(received);
    assert!(f.intake.replenish_data_slot(slot, &f.links).unwrap());
    // All three slots are available again without reinstalling the grant.
    for _ in 0..3 {
        f.service.receive(f.message(), 1024).unwrap();
    }
    assert!(f.service.receive(f.message(), 1024).is_err());
    for _ in 0..3 {
        drop(f.intake.receive(&f.links, &f.routes).unwrap().unwrap());
    }
    assert!(f.intake.replenish_data_slot(slot, &f.links).unwrap());
    assert_eq!(f.observe_install(), 0);
    f.service.receive(f.message(), 1024).unwrap();
    drop(f.intake.receive(&f.links, &f.routes).unwrap().unwrap());
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
}

#[test]
fn disconnect_after_writer_replenishment_keeps_retained_payload_charged() {
    let mut f = fixture(32768, 8192);
    f.intake.client_window = 3;
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    f.service.receive(f.message(), 1024).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    let slot = received.reservation;
    let alias = received.message.part_bytes(3).unwrap().slice(..1);
    drop(received);
    assert!(f.intake.replenish_data_slot(slot, &f.links).unwrap());
    assert!(f.service.disconnect(f.request.binding));
    f.intake.reconcile(&f.links, 0, 8, |_| Ok(())).unwrap();
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
    assert!(f.data.claimed_capacity().bytes > 0);
    drop(alias);
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
}

#[test]
fn settled_writer_replenishes_installed_grant_without_another_dispatch_command() {
    let mut f = fixture(32768, 8192);
    f.install(4096).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    f.service.receive(f.message(), 1024).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    let slot = received.reservation;
    drop(received);
    assert!(f.intake.replenish_data_slot(slot, &f.links).unwrap());
    assert_eq!(f.observe_install(), 0);
    // The next APPEND may use the original, larger receive allocation class.
    f.service.receive(f.message(), 4096).unwrap();
    drop(f.intake.receive(&f.links, &f.routes).unwrap().unwrap());
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
}

#[test]
fn writer_canonical_allowance_tracks_its_bounded_wire_promise() {
    let mut f = fixture(32768, 8192);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(f.destination.capacity().remaining().bytes, 2048);
    f.install(2048).unwrap();
    assert_eq!(f.destination.capacity().remaining().bytes, 4096);
    f.service.receive(f.message(), 2048).unwrap();
    drop(f.intake.receive(&f.links, &f.routes).unwrap().unwrap());
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
    assert_eq!(f.client.usage(Class::Data), Budget::default());
}

#[test]
fn writer_settlement_preserves_granted_backing_until_release() {
    let mut f = fixture(32768, 8192);
    f.install(4096).unwrap();
    f.service.poll_command().unwrap();
    f.observe_install();
    f.service.receive(f.message(), 1024).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    let slot = received.reservation;
    drop(received);
    let (request, bytes) = f.intake.settle_slot(slot, |_| Ok(())).unwrap().unwrap();
    assert_eq!(bytes, 4096);
    f.intake
        .install(&mut f.port, &f.links, request, None, &f.client, bytes)
        .unwrap();
    assert_eq!(f.destination.capacity().remaining().bytes, 8192);
    assert_eq!(f.client.usage(Class::Data).bytes, 4096);
}

#[test]
fn admitted_writer_keeps_canonical_grant_backing_until_settlement() {
    let mut f = fixture(32768, 8192);
    f.install(4096).unwrap();
    f.service.poll_command().unwrap();
    f.observe_install();
    f.service.receive(f.message(), 4096).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    assert_eq!(f.destination.capacity().remaining().bytes, 8192);
    drop(received);
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
}

#[test]
fn failed_writer_credit_growth_returns_only_its_new_canonical_allowance() {
    let mut f = fixture(32768, 8192);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    f.observe_install();
    let client = f
        .intake
        .client(LinkSessionId::from_bytes([3; 16]), budgets())
        .unwrap();
    let grant = f
        .intake
        .input
        .credits()
        .grant(
            &client,
            Class::Data,
            dispatch::Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    assert!(matches!(
        f.install(8192),
        Err(IntakeError::Admission(dispatch::Error::Full))
    ));
    assert_eq!(f.destination.capacity().remaining().bytes, 2048);
    assert_eq!(f.client.usage(Class::Data).bytes, 1024);
    drop(grant);
    f.install(8192).unwrap();
    assert_eq!(f.destination.capacity().remaining().bytes, 8192);
    f.intake.revoke(f.request, |_| Ok(())).unwrap();
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
    assert_eq!(f.client.usage(Class::Data), Budget::default());
}

struct Fixture {
    intake: ShardIntake,
    service: Service,
    port: Port,
    links: Links,
    routes: RoutingTable,
    client: Client,
    request: GrantRequest,
    destination: Destination,
    data: memory::Owner,
    sessions: LinkSessions,
}

fn budgets() -> Budgets {
    let budget = Budget {
        queue_slots: 4,
        retained_messages: 4,
        bytes: 8192,
    };
    Budgets {
        data: budget,
        control: budget,
    }
}

fn fixture(bytes: usize, canonical: usize) -> Fixture {
    fixture_role(bytes, canonical, Kind::Client)
}

#[expect(clippy::too_many_lines, reason = "bounded frontend and memory setup")]
fn fixture_role(bytes: usize, canonical: usize, kind: Kind) -> Fixture {
    let domain = Domain::new(None, bytes + 16384).unwrap();
    let data = domain
        .owner(Limits {
            bytes,
            buffers: 32,
            cache_bytes: bytes,
        })
        .unwrap();
    let control = domain
        .owner(Limits {
            bytes: 16384,
            buffers: 32,
            cache_bytes: 16384,
        })
        .unwrap();
    let (sender, mut intake) = ShardIntake::new(
        data.clone(),
        &control,
        dispatch::Limits {
            capacity: budgets(),
            clients: 2,
            grants: 4,
        },
        16,
        1,
    )
    .unwrap();
    let placement = test_support::placement(0, 0);
    let placements: Vec<_> = (0..5)
        .map(|index| test_support::placement(index, 0))
        .collect();
    let make_routes =
        || RoutingTable::new(&[0], &placements, 5, ozzy_proto::EnvelopeLimits::default()).unwrap();
    let queue = QueueLimits {
        messages: 4,
        bytes: 8192,
        message_bytes: 4096,
    };
    let dispatcher = Dispatcher::new(
        NodeId::from_bytes([9; 16]),
        make_routes(),
        vec![(0, sender)],
        DispatcherLimits {
            peers: 2,
            grants_per_class: 4,
            replies: ReplyLimits {
                data: queue,
                control: queue,
            },
        },
    )
    .unwrap();
    let parameters = handshake::Parameters::streaming(
        ozzy_proto::data::DataLimits {
            envelope: ozzy_proto::EnvelopeLimits {
                max_metadata_bytes: 1024,
                max_payload_bytes: 1024,
            },
            max_records: 4,
            max_parts: 4,
            max_record_bytes: 1024,
        },
        handshake::OWNER,
        4,
        8192,
    )
    .unwrap();
    let mut service = Service::new(
        dispatcher,
        parameters,
        &[
            Access {
                peer: NodeId::from_bytes([1; 16]),
                kind,
            },
            Access {
                peer: NodeId::from_bytes([2; 16]),
                kind,
            },
        ],
        LinkIds::deterministic(NonZeroU64::new(99).unwrap()),
    )
    .unwrap();
    let client_sessions = remote(
        1,
        if kind == Kind::Client {
            handshake::PRODUCER
        } else {
            handshake::OWNER | 8
        },
    );
    let (binding, _) = begin(&mut service, &client_sessions, NodeId::from_bytes([1; 16]));
    let links = service.links();
    let port = service.port(0, budgets()).unwrap();
    let client = intake.client(binding.session, budgets()).unwrap();
    let request = GrantRequest {
        binding,
        route: crate::frontend::Routed {
            placement,
            class: Class::Data,
            writer: Some(ProducerId::from_bytes([4; 16])),
        },
    };
    let destination = intake
        .destination(
            placement.group,
            Kind::Client,
            Class::Data,
            memory::Quota {
                bytes: canonical,
                buffers: 2,
            },
        )
        .unwrap();
    Fixture {
        intake,
        service,
        port,
        links,
        routes: make_routes(),
        client,
        request,
        destination,
        data,
        sessions: client_sessions,
    }
}

impl Fixture {
    fn install(&mut self, wire_bytes: usize) -> Result<(), IntakeError> {
        self.intake
            .install(
                &mut self.port,
                &self.links,
                self.request,
                None,
                &self.client,
                wire_bytes,
            )
            .map(|_| ())
    }

    fn observe_install(&mut self) -> usize {
        let mut count = 0;
        self.intake
            .poll_installations(
                &mut Context::from_waker(Waker::noop()),
                &self.links,
                0,
                8,
                |_, _| count += 1,
            )
            .unwrap();
        count
    }

    fn message(&self) -> Message {
        test_support::append(self.request.route.placement, self.request.binding)
    }
}

#[test]
fn idle_grants_exclude_pending_spent_and_fenced_dispatch_slots() {
    let mut f = fixture(16384, 256);
    f.install(1024).unwrap();
    assert_eq!(f.intake.idle_grants(&f.links, 0, 8).count(), 0);
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(f.intake.idle_grants(&f.links, 0, 8).count(), 1);
    // Dispatch can spend the token before the shard dequeues its message.
    f.service.receive(f.message(), 1024).unwrap();
    assert_eq!(f.intake.idle_grants(&f.links, 0, 8).count(), 0);
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    drop(received);
    f.intake.settle(&f.destination, |_| Ok(())).unwrap();
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(f.intake.idle_grants(&f.links, 0, 8).count(), 1);
    f.intake.revoke(f.request, |_| Ok(())).unwrap();
    assert_eq!(f.intake.idle_grants(&f.links, 0, 8).count(), 0);
}

#[test]
fn idle_grants_stop_at_disconnect_before_shard_reconciliation() {
    let mut f = fixture(16384, 256);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(f.intake.idle_grants(&f.links, 0, 8).count(), 1);
    assert!(f.service.disconnect(f.request.binding));
    assert_eq!(f.intake.idle_grants(&f.links, 0, 8).count(), 0);
    // The old canonical allowance remains charged until explicit fencing.
    assert_eq!(f.destination.capacity().remaining().bytes, 256);
}

#[test]
fn canonical_and_foreign_admission_fail_without_installing_partial_credit() {
    let mut f = fixture(4096, 256);
    // Destination cannot fit. No receive token or port command was installed.
    let held = f.data.try_lease(4096).unwrap();
    assert!(
        matches!(f.install(1024), Err(IntakeError::Memory(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    assert!(!f.service.poll_command().unwrap());
    assert_eq!(
        f.destination.capacity().remaining(),
        memory::Quota::default()
    );
    drop(held);
    // Canonical reserve fits, but foreign backing would exceed the shared owner.
    assert!(matches!(
        f.install(4096),
        Err(IntakeError::Admission(dispatch::Error::Full))
    ));
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
    assert!(!f.service.poll_command().unwrap());
    f.install(1024).unwrap();
    assert_eq!(f.observe_install(), 0);
    assert!(f.service.poll_command().unwrap());
    assert_eq!(f.observe_install(), 1);
    assert_eq!(
        f.destination.capacity().remaining(),
        memory::Quota {
            bytes: 256,
            buffers: 2
        }
    );
}

#[test]
fn repeated_pending_demand_reserves_once_and_can_grow_backing_credit() {
    let mut f = fixture(16384, 256);
    f.install(1024).unwrap();
    let canonical = f.data.claimed_capacity();
    let staging = f.destination.capacity().remaining();
    let usage = f.client.usage(Class::Data);
    f.install(2048).unwrap();
    assert_eq!(f.destination.capacity().remaining(), staging);
    assert_eq!(f.data.claimed_capacity().bytes, canonical.bytes + 1024);
    assert_eq!(f.data.claimed_capacity().buffers, canonical.buffers);
    let grown = f.client.usage(Class::Data);
    assert_eq!(grown.queue_slots, usage.queue_slots);
    assert_eq!(grown.retained_messages, usage.retained_messages);
    assert_eq!(grown.bytes, usage.bytes + 1024);
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert!(!f.service.poll_command().unwrap());
    f.service.receive(f.message(), 2048).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    assert!(received.current);
    f.install(4096).unwrap();
    assert_eq!(f.client.usage(Class::Data).bytes, 2048);
    drop(received);
    f.intake.settle(&f.destination, |_| Ok(())).unwrap();
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
}

#[test]
fn broker_control_bursts_cross_partitions_and_keep_old_session_input_fenced() {
    let mut f = fixture_role(16384, 256, Kind::Broker);
    f.request.route.class = Class::Control;
    f.request.route.writer = None;
    f.intake
        .destination(
            f.request.route.placement.group,
            Kind::Broker,
            Class::Control,
            memory::Quota::default(),
        )
        .unwrap();
    for index in 1..4 {
        f.intake
            .destination(
                test_support::placement(index, 0).group,
                Kind::Broker,
                Class::Control,
                memory::Quota::default(),
            )
            .unwrap();
    }
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    for index in 0..4 {
        f.service
            .receive(
                test_support::control(test_support::placement(index, 0), f.request.binding),
                1024,
            )
            .unwrap();
    }
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    assert!(received.current);
    let alias = received.message.part_bytes(2).unwrap();
    drop(received);
    f.intake.replenish_controls(0, 8).unwrap();
    assert_eq!(f.client.usage(Class::Control).retained_messages, 4);
    assert_eq!(
        f.intake
            .revoke_idle(Class::Data, 0, 8, |_| panic!(
                "control window is independent"
            ))
            .unwrap(),
        None
    );
    assert!(f.service.disconnect(f.request.binding));
    f.intake.reconcile(&f.links, 0, 8, |_| Ok(())).unwrap();
    for index in 1..4 {
        let stale = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
        assert!(!stale.current);
        assert_eq!(
            stale.request.route.placement,
            test_support::placement(index, 0)
        );
    }
    assert_eq!(f.client.usage(Class::Control).bytes, 1024);
    drop(alias);
    assert_eq!(f.client.usage(Class::Control), Budget::default());
}

#[test]
fn broker_control_revocation_between_link_observation_and_refresh_is_retryable() {
    let mut f = fixture_role(16384, 256, Kind::Broker);
    f.request.route.class = Class::Control;
    f.request.route.writer = None;
    let destination = f
        .intake
        .destination(
            f.request.route.placement.group,
            Kind::Broker,
            Class::Control,
            memory::Quota::default(),
        )
        .unwrap();
    f.install(512).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    f.client
        .replace_session(LinkSessionId::from_bytes([87; 16]))
        .unwrap();
    // Credit can be revoked before this turn observes the new frontend binding.
    f.install(512).unwrap();
    f.intake.reconcile(&f.links, 0, 8, |_| Ok(())).unwrap();
    assert_eq!(destination.capacity().remaining(), memory::Quota::default());
    assert_eq!(f.client.usage(Class::Control), Budget::default());
}

#[test]
fn independent_clients_share_a_destination_with_separately_backed_allowances() {
    let mut f = fixture(16384, 256);
    let second = remote(2, handshake::PRODUCER);
    let (binding, _) = begin(&mut f.service, &second, NodeId::from_bytes([2; 16]));
    let client = f.intake.client(binding.session, budgets()).unwrap();
    let request = GrantRequest {
        binding,
        route: f.request.route,
    };
    f.install(1024).unwrap();
    assert!(
        f.intake
            .install(&mut f.port, &f.links, request, None, &client, 1024)
            .unwrap()
    );
    assert_eq!(
        f.destination.capacity().remaining(),
        memory::Quota {
            bytes: 512,
            buffers: 4
        }
    );
    f.service.poll_command().unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 2);
    f.service.receive(f.message(), 1024).unwrap();
    let first = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    let first_arena = f.destination.capacity().allocator().try_arena(128).unwrap();
    drop(first);
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(
        f.destination.capacity().remaining(),
        memory::Quota {
            bytes: 256,
            buffers: 2
        }
    );
    f.service
        .receive(
            test_support::append(request.route.placement, request.binding),
            1024,
        )
        .unwrap();
    let second = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    assert_eq!(second.request, request);
    let second_arenas = [
        f.destination.capacity().allocator().try_arena(128).unwrap(),
        f.destination.capacity().allocator().try_arena(128).unwrap(),
    ];
    drop(second);
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
    assert_eq!(f.data.allocated_bytes(), 384);
    drop((first_arena, second_arenas));
    f.data.trim_cache();
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
}

#[test]
fn full_installation_port_returns_canonical_and_wire_reservations() {
    let mut f = fixture(16384, 256);
    let mut commands = Vec::new();
    for _ in 0..4 {
        commands.push(
            f.port
                .try_route(ozzy_proto::directory::RouteState {
                    group: f.request.route.placement.group,
                    config_epoch: 1,
                    partition: f.request.route.placement.partition,
                    members: [NodeId::from_bytes([9; 16])].into(),
                    view: 0,
                    leader: Some(NodeId::from_bytes([9; 16])),
                })
                .unwrap(),
        );
    }
    assert!(matches!(
        f.install(1024),
        Err(IntakeError::Port(PortError::Admission(_)))
    ));
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
    assert_eq!(f.client.usage(Class::Data), Budget::default());
}

#[test]
fn dispatcher_disconnect_recovers_unused_destination_capacity_once() {
    let mut f = fixture(16384, 256);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert!(f.service.disconnect(f.request.binding));
    f.intake.reconcile(&f.links, 0, 8, |_| Ok(())).unwrap();
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
    f.intake.reconcile(&f.links, 0, 8, |_| Ok(())).unwrap();
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
}

#[test]
fn queued_and_canonical_aliases_stay_charged_after_session_replacement() {
    let mut f = fixture(16384, 256);
    f.install(4096).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert!(f.service.receive(f.message(), 4096).unwrap().is_some());
    assert!(f.service.disconnect(f.request.binding));
    f.intake.reconcile(&f.links, 0, 8, |_| Ok(())).unwrap();
    assert!(
        !f.intake.settle(&f.destination, |_| Ok(())).unwrap(),
        "queued input cannot settle"
    );
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    assert!(!received.current);
    let alias = received.message.part_bytes(3).unwrap().slice(..1);
    drop(received);
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(
        f.destination.capacity().remaining(),
        memory::Quota::default()
    );
    assert_eq!(f.data.allocated_bytes(), 4096);
    f.sessions.disconnect(f.service.local());
    let (binding, _) = begin(&mut f.service, &f.sessions, f.request.binding.peer);
    f.request.binding = binding;
    f.client.replace_session(binding.session).unwrap();
    f.install(4096).unwrap();
    f.service.poll_command().unwrap();
    f.observe_install();
    f.service.receive(f.message(), 4096).unwrap();
    let received = f.intake.receive(&f.links, &f.routes).unwrap().unwrap();
    assert!(received.current);
    let arena = f.destination.capacity().allocator().try_arena(64).unwrap();
    drop(received);
    assert!(f.intake.settle(&f.destination, |_| Ok(())).unwrap());
    assert_eq!(f.data.allocated_bytes(), 4160);
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
    assert_eq!(f.client.usage(Class::Data).bytes, 4096);
    drop(alias);
    assert_eq!(f.data.allocated_bytes(), 64);
    assert_eq!(f.client.usage(Class::Data), Budget::default());
    drop(arena);
}

#[test]
fn canceling_installation_owner_fences_pending_commands_and_returns_unused_credit() {
    let mut f = fixture(16384, 256);
    f.install(1024).unwrap();
    let message = f.message();
    drop(f.intake);
    assert_eq!(
        f.destination.capacity().remaining(),
        memory::Quota::default()
    );
    assert_eq!(f.data.reserved_capacity(), memory::Quota::default());
    f.service.poll_command().unwrap();
    assert!(f.service.receive(message, 1024).is_err());
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
}

#[test]
fn idle_promise_can_move_without_multiplying_the_shard_budget() {
    let mut f = fixture(16384, 256);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    f.observe_install();
    let data = f.data.clone();
    let capacity = f.destination.capacity().clone();
    assert_eq!(
        f.intake
            .revoke_idle(Class::Data, 0, 8, |request| {
                assert_eq!(request, f.request);
                // Dispatch is already fenced. Canonical allowance remains owned
                // until this callback fences any advertised protocol receive epoch.
                assert_eq!(
                    data.claimed_capacity(),
                    memory::Quota {
                        bytes: 256,
                        buffers: 2
                    }
                );
                assert_eq!(
                    capacity.remaining(),
                    memory::Quota {
                        bytes: 256,
                        buffers: 2
                    }
                );
                Ok(())
            })
            .unwrap(),
        Some(f.request)
    );
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
    f.request.route.writer = Some(ProducerId::from_bytes([5; 16]));
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(
        f.data.claimed_capacity(),
        memory::Quota {
            bytes: 1280,
            buffers: 6
        }
    );
}

#[test]
fn data_exhaustion_preserves_open_admission_and_control_capacity_wakeups() {
    let mut f = fixture(4096, 256);
    let held = f.data.try_lease(4096).unwrap();
    let control = f
        .intake
        .destination(
            f.request.route.placement.group,
            Kind::Client,
            Class::Control,
            memory::Quota {
                bytes: 65,
                buffers: 1,
            },
        )
        .unwrap();
    f.request.route.class = Class::Control;
    f.request.route.writer = None;
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(
        control.capacity().remaining(),
        memory::Quota {
            bytes: 65,
            buffers: 1
        }
    );
    let generation = f.intake.generation();
    let mut ready = Box::pin(f.intake.changed_after(generation));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(ready.as_mut().poll(&mut cx).is_pending());
    f.intake
        .revoke_idle(Class::Control, 0, 8, |_| Ok(()))
        .unwrap();
    assert_eq!(
        f.data.claimed_capacity(),
        memory::Quota {
            bytes: 4096,
            buffers: 1
        }
    );
    assert!(ready.as_mut().poll(&mut cx).is_ready());
    assert_eq!(control.capacity().remaining(), memory::Quota::default());
    drop(held);
}

#[test]
fn data_settlements_cannot_consume_control_reservation_slots() {
    let mut f = fixture(16384, 256);
    for index in 0..4 {
        f.request.route.placement = test_support::placement(index, 0);
        if index != 0 {
            f.intake
                .destination(
                    f.request.route.placement.group,
                    Kind::Client,
                    Class::Data,
                    memory::Quota {
                        bytes: 256,
                        buffers: 2,
                    },
                )
                .unwrap();
        }
        f.install(1024).unwrap();
        f.service.poll_command().unwrap();
        assert_eq!(f.observe_install(), 1);
        f.service.receive(f.message(), 1024).unwrap();
        drop(f.intake.receive(&f.links, &f.routes).unwrap().unwrap());
    }
    f.request.route.placement = test_support::placement(4, 0);
    f.intake
        .destination(
            f.request.route.placement.group,
            Kind::Client,
            Class::Data,
            memory::Quota {
                bytes: 256,
                buffers: 2,
            },
        )
        .unwrap();
    assert!(matches!(
        f.install(1024),
        Err(IntakeError::Admission(dispatch::Error::Full))
    ));
    f.request.route.class = Class::Control;
    f.request.route.writer = None;
    let control = f
        .intake
        .destination(
            f.request.route.placement.group,
            Kind::Client,
            Class::Control,
            memory::Quota {
                bytes: 65,
                buffers: 1,
            },
        )
        .unwrap();
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(control.capacity().remaining().bytes, 65);
}

#[test]
fn reclaiming_idle_control_capacity_preserves_unused_data_promises() {
    let mut f = fixture(16384, 256);
    let data_request = f.request;
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(
        f.intake
            .revoke_idle(Class::Control, 0, 8, |_| panic!("data is independent"))
            .unwrap(),
        None
    );
    assert_eq!(f.destination.capacity().remaining().bytes, 256);
    f.request.route.class = Class::Control;
    f.request.route.writer = None;
    let control = f
        .intake
        .destination(
            f.request.route.placement.group,
            Kind::Client,
            Class::Control,
            memory::Quota {
                bytes: 65,
                buffers: 1,
            },
        )
        .unwrap();
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    assert_eq!(f.observe_install(), 1);
    assert_eq!(
        f.intake.revoke_idle(Class::Data, 0, 8, |_| Ok(())).unwrap(),
        Some(data_request)
    );
    assert_eq!(
        f.destination.capacity().remaining(),
        memory::Quota::default()
    );
    assert_eq!(control.capacity().remaining().bytes, 65);
    assert_eq!(
        f.intake
            .revoke_idle(Class::Control, 0, 8, |_| Ok(()))
            .unwrap(),
        Some(f.request)
    );
    assert_eq!(control.capacity().remaining(), memory::Quota::default());
}

#[test]
fn failed_unused_protocol_fence_retries_before_releasing_allowance() {
    let mut f = fixture(16384, 256);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    f.observe_install();
    assert!(matches!(
        f.intake
            .revoke_idle(Class::Data, 0, 8, |_| Err(IntakeError::Session)),
        Err(IntakeError::Session)
    ));
    assert_eq!(f.destination.capacity().remaining().bytes, 256);
    let mut released = 0;
    f.intake
        .reconcile(&f.links, 0, 8, |request| {
            assert_eq!(request, f.request);
            released += 1;
            Ok(())
        })
        .unwrap();
    f.intake
        .reconcile(&f.links, 0, 8, |_| {
            released += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(released, 1);
    assert_eq!(f.data.claimed_capacity(), memory::Quota::default());
}

#[test]
fn admitted_settlement_fences_residual_protocol_credit_before_reuse() {
    let mut f = fixture(16384, 256);
    f.install(1024).unwrap();
    f.service.poll_command().unwrap();
    f.observe_install();
    f.service.receive(f.message(), 1024).unwrap();
    drop(f.intake.receive(&f.links, &f.routes).unwrap().unwrap());
    assert!(matches!(
        f.intake
            .settle(&f.destination, |_| Err(IntakeError::Session)),
        Err(IntakeError::Session)
    ));
    assert_eq!(f.destination.capacity().remaining().bytes, 256);
    let capacity = f.destination.capacity().clone();
    assert!(
        f.intake
            .settle(&f.destination, |request| {
                assert_eq!(request, f.request);
                assert_eq!(capacity.remaining().bytes, 256);
                Ok(())
            })
            .unwrap()
    );
    assert_eq!(capacity.remaining(), memory::Quota::default());
}
