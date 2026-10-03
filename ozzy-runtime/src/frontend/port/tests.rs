use super::*;
use crate::dispatch::{Budget, Quota, SendFailure};
use crate::frontend::GrantTarget;
use crate::frontend::service::tests::{begin, remote, reply, setup};
use crate::frontend::{Subject, WatchLimits, WatchRegistry};
use ozzy_proto::{Opcode, ProducerId, directory, handshake};

fn capacity() -> Budgets {
    Budgets {
        data: Budget {
            queue_slots: 1,
            retained_messages: 1,
            bytes: 4096,
        },
        control: Budget {
            queue_slots: 2,
            retained_messages: 2,
            bytes: 8192,
        },
    }
}

fn ingress_capacity() -> Budgets {
    let mut limits = capacity();
    limits.control.queue_slots = 1;
    limits.control.bytes = 4096;
    limits
}

#[tokio::test]
async fn reply_capacity_follows_transport_aliases_and_unobserved_completions() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let mut port = service.port(0, capacity()).unwrap();
    let packet = reply(binding, Opcode::Records);
    let pending = port.try_reply(Class::Data, packet.clone(), 2048).unwrap();
    assert!(service.poll_command().unwrap());
    let mut transmitted = Vec::new();
    service
        .flush(|message| {
            transmitted.push(message);
            Ok(())
        })
        .unwrap();
    service.poll_command().unwrap();
    assert!(matches!(
        port.try_reply(Class::Data, packet.clone(), 2048),
        Err((
            PortError::Admission(SendFailure::Admission(dispatch::Error::Full)),
            _
        ))
    ));
    drop(transmitted);
    service.poll_command().unwrap();
    // The reply observer itself is bounded until it consumes the outcome.
    assert!(port.try_reply(Class::Data, packet.clone(), 2048).is_err());
    let generation = port.generation();
    pending.await.unwrap().unwrap();
    let ready = port.changed_after(generation);
    tokio::time::timeout(std::time::Duration::from_secs(1), ready)
        .await
        .unwrap();
    let pending = port.try_reply(Class::Data, packet, 2048).unwrap();
    assert!(service.poll_command().unwrap());
    pending.await.unwrap().unwrap();
    let mut backing = None;
    service
        .flush(|message| {
            backing = message.part_bytes(1);
            Ok(())
        })
        .unwrap();
    service.poll_command().unwrap();
    assert!(
        port.try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
            .is_err()
    );
    drop(backing);
    service.poll_command().unwrap();
    assert!(
        port.try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
            .is_ok()
    );
}

#[tokio::test]
async fn full_port_wakes_when_dispatcher_exits() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let mut port = service.port(0, capacity()).unwrap();
    let packet = reply(binding, Opcode::Records);
    let pending = port.try_reply(Class::Data, packet.clone(), 2048).unwrap();
    let generation = port.generation();
    drop(service);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        port.changed_after(generation),
    )
    .await
    .unwrap();
    assert!(matches!(
        port.try_reply(Class::Data, packet, 2048),
        Err((PortError::Admission(SendFailure::Closed), _))
    ));
    assert_eq!(pending.await, Err(PortError::Closed));
}

#[tokio::test]
async fn full_peer_returns_original_message_and_cannot_deadlock_its_own_retry_credit() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    for _ in 0..2 {
        service
            .try_reply(Class::Data, reply(binding, Opcode::Records))
            .unwrap();
    }
    let mut port = service.port(0, capacity()).unwrap();
    let pending = port
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
        .unwrap();
    service.poll_command().unwrap();
    let (error, message) = pending.await.unwrap().unwrap_err();
    assert_eq!(error, ReplyError::Full);
    service.poll_command().unwrap();
    // Keep returned bytes alive while reusing the released queue reservation.
    let again = port.try_reply(Class::Data, message, 2048).unwrap();
    service.flush(|_| Ok(())).unwrap();
    service.poll_command().unwrap();
    again.await.unwrap().unwrap();
}

#[tokio::test]
async fn grant_installation_uses_reserved_control_and_checks_originating_shard() {
    let (mut service, mut lanes, placements) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let mut port = service.port(0, capacity()).unwrap();
    let data = port
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
        .unwrap();
    let owner = lanes[0]
        .credits()
        .client(binding.session, ingress_capacity())
        .unwrap();
    let grant = lanes[0]
        .credits()
        .grant(
            &owner,
            Class::Data,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    let subject = Subject {
        group: placements[0].group,
        writer: Some(ProducerId::from_bytes([4; 16])),
    };
    let installed = port.try_install(binding.peer, subject, grant).unwrap();
    for _ in 0..2 {
        assert!(service.poll_command().unwrap());
    }
    data.await.unwrap().unwrap();
    installed.await.unwrap().unwrap();
    service
        .receive(
            crate::frontend::test_support::append(placements[0], binding),
            4096,
        )
        .unwrap();
    assert!(lanes[0].try_recv().unwrap().is_some());
    let mut wrong_port = service.port(17, capacity()).unwrap();
    let grant = lanes[0]
        .credits()
        .grant(
            &owner,
            Class::Control,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    let result = wrong_port
        .try_install(
            binding.peer,
            Subject {
                writer: None,
                ..subject
            },
            grant,
        )
        .unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    let (error, grant) = result.await.unwrap().unwrap_err();
    assert_eq!(error, SetupError::Destination);
    let result = wrong_port
        .try_install(binding.peer, GrantTarget::Control(0), grant)
        .unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    let (error, grant) = result.await.unwrap().unwrap_err();
    assert_eq!(error, SetupError::Destination);
    let result = port
        .try_install(binding.peer, GrantTarget::Control(0), grant)
        .unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    result.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancellation_keeps_installed_grant_revocable_and_service_drop_closes_observers() {
    let (mut service, mut lanes, placements) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let mut port = service.port(0, capacity()).unwrap();
    let owner = lanes[0]
        .credits()
        .client(binding.session, ingress_capacity())
        .unwrap();
    let grant = lanes[0]
        .credits()
        .grant(
            &owner,
            Class::Data,
            Quota {
                messages: 1,
                bytes: 4096,
            },
        )
        .unwrap();
    let key = grant.key();
    drop(
        port.try_install(
            binding.peer,
            Subject {
                group: placements[0].group,
                writer: Some(ProducerId::from_bytes([4; 16])),
            },
            grant,
        )
        .unwrap(),
    );
    service.poll_command().unwrap();
    assert_eq!(owner.usage(Class::Data).bytes, 4096);
    lanes[0].credits().revoke_key(&key).unwrap();
    assert_eq!(owner.usage(Class::Data), Budget::default());
    let pending = port
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
        .unwrap();
    drop(service);
    assert_eq!(pending.await, Err(PortError::Closed));
}

#[tokio::test]
async fn route_publication_is_bounded_to_its_partition_owner_shard() {
    let (mut service, _lanes, placements) = setup();
    let mut route = directory::RouteState {
        group: placements[0].group,
        config_epoch: 1,
        partition: placements[0].partition,
        members: [
            service.local(),
            NodeId::from_bytes([8; 16]),
            NodeId::from_bytes([7; 16]),
        ]
        .into(),
        view: 0,
        leader: None,
    };
    service
        .install_watches(
            WatchRegistry::new(
                WatchLimits {
                    partitions: 1,
                    peers: 1,
                    registrations: 1,
                    interests_per_registration: 1,
                    pending_per_registration: 1,
                },
                [route.clone()],
            )
            .unwrap(),
        )
        .unwrap();
    let mut right = service.port(0, capacity()).unwrap();
    let mut wrong = service.port(17, capacity()).unwrap();
    let mut invalid = route.clone();
    invalid.members = [
        service.local(),
        service.local(),
        NodeId::from_bytes([7; 16]),
    ]
    .into();
    assert_eq!(right.try_route(invalid).unwrap_err().0, PortError::Route);
    route.view = 1;
    route.leader = Some(service.local());
    let rejected = wrong.try_route(route.clone()).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(matches!(
        rejected.await.unwrap(),
        Err(RouteError::Destination)
    ));
    let published = right.try_route(route.clone()).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(published.await.unwrap().unwrap());
    let repeated = right.try_route(route).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(!repeated.await.unwrap().unwrap());
}

fn publication(route: &RouteState, local: NodeId) -> Message {
    use ozzy_proto::{Envelope, MessageId, data::DataLimits, reader};
    let source = reader::Source::Group {
        authority: ozzy_proto::data::Authority {
            group_id: route.group,
            config_epoch: route.config_epoch,
            view: route.view,
        },
        partition: route.partition,
        owner_epoch: 1,
    };
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let mut output = reader::RecordsEncoder::publication(
        Envelope {
            opcode: Opcode::RecordsPub,
            response: false,
            request_id: None,
            sender: local,
            session: None,
        },
        reader::PublicationHeader {
            source,
            first_offset: 7,
        },
        &mut metadata,
        &mut payload,
        DataLimits::default(),
    )
    .unwrap();
    output
        .push_raw(MessageId::from_bytes([6; 16]), b"confirmed payload")
        .unwrap();
    let header = output.finish().unwrap();
    crate::native_frames::message(
        &reader::publication_topic(source).unwrap(),
        header,
        &metadata,
        bytes::Bytes::from(payload),
    )
}

#[tokio::test]
async fn reader_publications_fence_sources_and_charge_transport_aliases() {
    use crate::frontend::PublicationError;
    let (mut service, _lanes, placements) = setup();
    let local = service.local();
    let mut route = directory::RouteState {
        group: placements[0].group,
        config_epoch: 1,
        partition: placements[0].partition,
        members: [
            local,
            NodeId::from_bytes([8; 16]),
            NodeId::from_bytes([7; 16]),
        ]
        .into(),
        view: 2,
        leader: Some(local),
    };
    publication_watch(&mut service, route.clone());
    let outgoing = publication_capacity();
    let allowance = Budget {
        queue_slots: 1,
        retained_messages: 1,
        bytes: 4096,
    };
    let mut right = service
        .port_with_publications(0, outgoing, allowance)
        .unwrap();
    let mut wrong = service
        .port_with_publications(17, outgoing, allowance)
        .unwrap();
    let rejected = wrong.try_publish(publication(&route, local), 2048).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(matches!(
        rejected.await.unwrap(),
        Err((PublicationError::Destination, _))
    ));

    let pending = right.try_publish(publication(&route, local), 2048).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    pending.await.unwrap().unwrap();
    let transmitted = service
        .take_publication()
        .expect("current source publication");
    let alias = transmitted.part_bytes(3).unwrap();
    drop(transmitted);
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(right.try_publish(publication(&route, local), 2048).is_err());
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let reply = right
        .try_reply(Class::Data, reply(binding, Opcode::Records), 2048)
        .unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    reply.await.unwrap().unwrap();
    service.flush(|_| Ok(())).unwrap();
    // Control progress stays available while the data alias retains its charge.
    route.view = 3;
    route.leader = Some(NodeId::from_bytes([8; 16]));
    let changed = right.try_route(route.clone()).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(changed.await.unwrap().unwrap());
    drop(alias);
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    let stale = right.try_publish(publication(&route, local), 2048).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    assert!(matches!(
        stale.await.unwrap(),
        Err((PublicationError::Stale, _))
    ));

    route.view = 4;
    route.leader = Some(local);
    service.publish_route(&route).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    let queued = right.try_publish(publication(&route, local), 2048).unwrap();
    for _ in 0..2 {
        service.poll_command().unwrap();
    }
    queued.await.unwrap().unwrap();
    route.view = 5;
    route.leader = None;
    service.publish_route(&route).unwrap();
    assert!(
        service.take_publication().is_none(),
        "queued old source is fenced before PUB"
    );
}

fn publication_watch(service: &mut Service, route: directory::RouteState) {
    service
        .install_watches(
            WatchRegistry::new(
                WatchLimits {
                    partitions: 1,
                    peers: 1,
                    registrations: 1,
                    interests_per_registration: 1,
                    pending_per_registration: 1,
                },
                [route],
            )
            .unwrap(),
        )
        .unwrap();
}

fn publication_capacity() -> Budgets {
    let mut outgoing = capacity();
    outgoing.data.queue_slots += 1;
    outgoing.data.retained_messages += 1;
    outgoing.data.bytes += 4096;
    outgoing
}
