use super::*;
use crate::frontend::{
    DataPressure, Placement, Rejection, data_channel, dispatch::tests::fixture, test_support,
};
use ozzy_proto::{Envelope, RequestId, directory};
use std::num::NonZeroU64;

fn topic_catalog() -> TopicCatalog {
    let node = NodeId::from_bytes([9; 16]);
    TopicCatalog::new(
        [directory::TopicPage {
            id: ozzy_proto::TopicId::from_bytes([5; 16]),
            name: "orders".to_owned(),
            partitioner_seed: 7,
            total: 2,
            policy: ozzy_proto::append::Policy::LocalDurable,
            brokers: vec![directory::BrokerEndpoint {
                node,
                peer: "inproc://peer".to_owned(),
                reader_pub: "inproc://readers".to_owned(),
                follower_pub: None,
            }],
            first: 0,
            partitions: (0..2)
                .map(|number| directory::TopicPartition {
                    number,
                    group: ozzy_proto::GroupId::from_bytes([number as u8 + 20; 16]),
                    config_epoch: 1,
                    incarnation: ozzy_proto::PartitionIncarnation::from_bytes(
                        [number as u8 + 30; 16],
                    ),
                    members: vec![node],
                })
                .collect(),
        }],
        1,
        2,
    )
    .unwrap()
}

#[test]
fn full_backpressured_append_sends_no_admission_refusal() {
    let (mut service, _legacy_lanes, placements) = setup();
    let (sender, mut receiver) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        crate::dispatch::Class::Data,
        1,
        4096,
        8192,
    )
    .unwrap();
    service.dispatcher.install_data_lane(0, sender).unwrap();
    let peer = NodeId::from_bytes([1; 16]);
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, peer);
    assert!(
        service
            .receive(test_support::append(placements[0], binding), 4096)
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        service.receive(test_support::append(placements[0], binding), 4096),
        Err(ReceiveError::Dispatch(Rejection::Data(
            DataPressure::Full { shard: 0, .. }
        )))
    ));
    assert!(!service.dispatcher.has_replies());
    assert!(receiver.try_recv().unwrap().is_some());
    assert!(
        service
            .receive(test_support::append(placements[0], binding), 4096)
            .unwrap()
            .is_some()
    );
}

#[test]
fn publications_require_configured_source_and_group_but_no_dispatch_grant() {
    let (mut service, _legacy_lanes, placements) = setup();
    let (sender, mut receiver) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Broker,
        crate::dispatch::Class::Data,
        1,
        4096,
        8192,
    )
    .unwrap();
    service.dispatcher.install_data_lane(0, sender).unwrap();
    let (writer_lane, _writer_queue) = data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        crate::dispatch::Class::Data,
        1,
        4096,
        8192,
    )
    .unwrap();
    service
        .dispatcher
        .install_data_lane(0, writer_lane)
        .unwrap();
    let client = NodeId::from_bytes([1; 16]);
    let (writer, _) = begin(&mut service, &remote(1, handshake::PRODUCER), client);
    service
        .receive(test_support::append(placements[0], writer), 4096)
        .unwrap();
    let publisher = NodeId::from_bytes([8; 16]);
    let (binding, _) = begin(&mut service, &remote(8, handshake::OWNER | 8), publisher);
    let original = test_support::control(
        placements[0],
        Binding {
            peer: NodeId::from_bytes([1; 16]),
            ..binding
        },
    );
    // Routing checks clear scope only. The partition actor owns canonical
    // payload validation, leader checks, and local retained-body accounting.
    let envelope = Envelope {
        opcode: Opcode::PreparePub,
        response: false,
        request_id: None,
        sender: publisher,
        session: None,
    };
    let mut metadata = original.part_bytes(2).unwrap().to_vec();
    metadata[32..48].copy_from_slice(publisher.as_bytes());
    let header = envelope
        .encode_header(metadata.len(), 0, service.envelope_limits())
        .unwrap();
    let message = Message::multipart([
        Bytes::copy_from_slice(placements[0].group.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ]);
    assert!(
        service
            .receive_publication(NodeId::from_bytes([1; 16]), &message, 4096)
            .is_err()
    );
    let mut wrong_topic = message.clone();
    wrong_topic.pop_front_payload();
    wrong_topic = Message::with_prefix(
        Bytes::copy_from_slice(placements[1].group.as_bytes()),
        wrong_topic,
    );
    assert!(
        service
            .receive_publication(publisher, &wrong_topic, 4096)
            .is_err()
    );
    assert!(
        service
            .receive_publication(publisher, &message, 4096)
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        service.receive_publication(publisher, &message, 4096),
        Err(ReceiveError::Dispatch(Rejection::Data(
            DataPressure::Full { shard: 0, .. }
        )))
    ));
    let queued = receiver.try_recv().unwrap().unwrap();
    assert_eq!(queued.binding, binding);
    assert_eq!(queued.route.placement, placements[0]);
    assert!(
        service.receive(queued.message, 4096).is_err(),
        "ordinary PEER must refuse PUB framing"
    );
    service.disconnect(binding);
    assert!(
        service
            .receive_publication(publisher, &message, 4096)
            .is_err()
    );
}

fn watch_request(
    binding: Binding,
    request: u8,
    watch: u8,
    groups: Vec<ozzy_proto::GroupId>,
) -> Message {
    let mut metadata = Vec::with_capacity(512);
    let header = directory::encode_request(
        Envelope {
            opcode: Opcode::StateSnapshotRequest,
            response: false,
            request_id: Some(RequestId::from_bytes([request; 16])),
            sender: binding.peer,
            session: Some(binding.session),
        },
        &directory::SnapshotRequest {
            watch: RequestId::from_bytes([watch; 16]),
            groups,
        },
        &mut metadata,
        DataLimits::default().envelope,
        directory::Limits::default(),
    )
    .unwrap();
    Message::multipart([
        Bytes::copy_from_slice(binding.peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ])
}

#[test]
#[allow(clippy::too_many_lines)]
fn watch_rejections_are_correlated_and_leave_the_link_usable() {
    let (mut service, _lanes, placements) = setup();
    service
        .install_watches(
            WatchRegistry::new(
                super::super::WatchLimits {
                    partitions: 2,
                    peers: 1,
                    registrations: 1,
                    interests_per_registration: 1,
                    pending_per_registration: 1,
                },
                placements.iter().map(|placement| directory::RouteState {
                    group: placement.group,
                    config_epoch: 1,
                    partition: placement.partition,
                    members: [
                        service.local(),
                        NodeId::from_bytes([8; 16]),
                        NodeId::from_bytes([7; 16]),
                    ]
                    .into(),
                    view: 0,
                    leader: None,
                }),
            )
            .unwrap(),
        )
        .unwrap();
    let peer = NodeId::from_bytes([1; 16]);
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, peer);
    for (request, watch, group, rejection) in [
        (
            10,
            3,
            ozzy_proto::GroupId::from_bytes([99; 16]),
            Some((18, nack::RetryClass::Permanent)),
        ),
        (11, 3, placements[0].group, None),
        (
            12,
            4,
            placements[0].group,
            Some((10, nack::RetryClass::AfterBackoff)),
        ),
        (
            13,
            3,
            placements[1].group,
            Some((1, nack::RetryClass::Permanent)),
        ),
        (14, 3, placements[0].group, None),
    ] {
        service
            .receive(watch_request(binding, request, watch, vec![group]), 4096)
            .unwrap();
        let mut replies = Vec::new();
        service
            .flush(|message| {
                replies.push(message);
                Ok(())
            })
            .unwrap();
        let replies = replies
            .iter()
            .filter(|message| {
                let frames =
                    std::array::from_fn::<_, 3, _>(|index| message.part_slice(index + 1).unwrap());
                decode_packet(&frames, DataLimits::default().envelope)
                    .unwrap()
                    .envelope
                    .request_id
                    == Some(RequestId::from_bytes([request; 16]))
            })
            .collect::<Vec<_>>();
        assert_eq!(replies.len(), 1);
        let frames =
            std::array::from_fn::<_, 3, _>(|index| replies[0].part_slice(index + 1).unwrap());
        let packet = decode_packet(&frames, DataLimits::default().envelope).unwrap();
        assert_eq!(
            packet.envelope.request_id,
            Some(RequestId::from_bytes([request; 16]))
        );
        assert_eq!(packet.envelope.session, Some(binding.session));
        if let Some((code, retry)) = rejection {
            let reply = nack::decode(packet, DataLimits::default().envelope).unwrap();
            assert_eq!((reply.code, reply.retry), (code, retry));
        } else {
            assert_eq!(
                directory::decode_snapshot(
                    packet,
                    DataLimits::default().envelope,
                    directory::Limits::default()
                )
                .unwrap()
                .routes[0]
                    .group,
                group
            );
        }
        assert_eq!(service.links().get(peer).unwrap().binding, binding);
    }
}

#[tokio::test]
async fn topic_lookup_returns_bounded_page_on_established_client_link() {
    let (mut service, _, _) = setup();
    service.install_catalog(topic_catalog()).unwrap();
    let peer = NodeId::from_bytes([1; 16]);
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, peer);
    let request_id = RequestId::from_bytes([6; 16]);
    let mut metadata = Vec::with_capacity(256);
    let header = directory::encode_topic_request(
        Envelope {
            opcode: Opcode::StateSnapshotRequest,
            response: false,
            request_id: Some(request_id),
            sender: peer,
            session: Some(binding.session),
        },
        &directory::TopicRequest {
            name: "orders".to_owned(),
            first: 1,
            maximum: 2,
        },
        &mut metadata,
        DataLimits::default().envelope,
        directory::Limits::default(),
    )
    .unwrap();
    let message = Message::multipart([
        Bytes::copy_from_slice(peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ]);
    service.receive(message.clone(), 4096).unwrap();
    let mut replies = Vec::new();
    for _ in 0..2 {
        service
            .flush(|message| {
                replies.push(message);
                Ok(())
            })
            .unwrap();
    }
    let reply = replies
        .iter()
        .find(|message| message.part_slice(1).unwrap()[5] == Opcode::StateSnapshot as u8)
        .unwrap();
    let frames = std::array::from_fn::<_, 3, _>(|i| reply.part_slice(i + 1).unwrap());
    let packet = decode_packet(&frames, DataLimits::default().envelope).unwrap();
    assert_eq!(packet.envelope.request_id, Some(request_id));
    let page = directory::decode_topic_page(
        packet,
        DataLimits::default().envelope,
        directory::Limits::default(),
    )
    .unwrap();
    assert_eq!(page.name, "orders");
    assert_eq!(page.first, 1);
    assert_eq!(page.partitions.len(), 1);
    assert_eq!(page.partitions[0].number, 1);
    service.start(peer).unwrap();
    assert!(service.receive(message, 4096).is_err());
}

fn ids(seed: u64) -> LinkIds {
    LinkIds::deterministic(NonZeroU64::new(seed).unwrap())
}

fn profile(roles: u32) -> handshake::Parameters {
    let mut parameters = handshake::Parameters::streaming(DataLimits::default(), roles).unwrap();
    parameters.capabilities |= handshake::OWNER_READ | (1 << 3) | (1 << 8);
    parameters.required_capabilities = 0;
    parameters
}

#[test]
fn configured_broker_bulk_replies_preserve_native_client_limits() {
    let (mut dispatcher, _lanes, _, original) = fixture();
    dispatcher.disconnect(original.peer);
    let mut native = profile(handshake::OWNER | 8);
    native.receive.envelope.max_payload_bytes = 1024;
    native.receive.max_record_bytes = 1024;
    let mut bulk = native.receive;
    bulk.envelope.max_payload_bytes = 4096;
    let access = [
        Access {
            peer: original.peer,
            kind: Kind::Client,
        },
        Access {
            peer: NodeId::from_bytes([8; 16]),
            kind: Kind::Broker,
        },
    ];
    let mut service =
        Service::new_with_broker_limits(dispatcher, native, bulk, &access, ids(99)).unwrap();
    let (client, _) = begin(&mut service, &remote(1, handshake::PRODUCER), original.peer);
    let (broker, _) = begin(
        &mut service,
        &remote(8, handshake::OWNER | 8),
        access[1].peer,
    );
    for (binding, opcode, expected) in [
        (client, Opcode::Records, Err(ReplyError::Size)),
        (broker, Opcode::Ops, Ok(())),
    ] {
        let envelope = Envelope {
            opcode,
            response: false,
            request_id: None,
            sender: service.local(),
            session: Some(binding.session),
        };
        let header = envelope.encode_header(0, 2048, bulk.envelope).unwrap();
        let message = Message::multipart([
            Bytes::copy_from_slice(binding.peer.as_bytes()),
            Bytes::copy_from_slice(&header),
            Bytes::new(),
            Bytes::from(vec![7; 2048]),
        ]);
        assert_eq!(
            service
                .try_reply(Class::Data, message)
                .map_err(|(error, _)| error),
            expected
        );
        assert_eq!(service.links.get(binding.peer).unwrap().binding, binding);
    }
    assert_eq!(
        service.links.get(client.peer).unwrap().send.envelope,
        native.receive.envelope
    );
    assert_eq!(
        service.links.get(broker.peer).unwrap().send.envelope,
        bulk.envelope
    );
    let forged = remote(1, handshake::OWNER | 8);
    let hello = Message::with_prefix(
        Bytes::copy_from_slice(original.peer.as_bytes()),
        Message::multipart(forged.start(service.local()).unwrap()),
    );
    assert!(
        service.receive(hello, 4096).is_err(),
        "role claim widened a client profile"
    );
    assert_eq!(service.links.get(client.peer).unwrap().binding, client);
}

#[test]
#[allow(clippy::too_many_lines)]
fn trusted_clients_keep_membership_capacity_and_disconnect_fences_separate() {
    let (mut dispatcher, _lanes, _, original) = fixture();
    dispatcher.disconnect(original.peer);
    let mut service = Service::new(
        dispatcher,
        profile(handshake::OWNER | 8),
        &[Access {
            peer: NodeId::from_bytes([8; 16]),
            kind: Kind::Broker,
        }],
        ids(99),
    )
    .unwrap()
    .with_trusted_clients(1)
    .unwrap();
    let local = service.dispatcher.local;
    let packet = |id: u8, remote: &LinkSessions| {
        Message::with_prefix(
            Bytes::copy_from_slice(NodeId::from_bytes([id; 16]).as_bytes()),
            Message::multipart(remote.start(local).unwrap()),
        )
    };
    let forged = remote(2, handshake::OWNER | 8);
    assert!(matches!(
        service.receive(packet(2, &forged), 4096),
        Err(ReceiveError::Role)
    ));
    assert!(service.links.get(NodeId::from_bytes([2; 16])).is_none());
    let claimant = remote(8, handshake::PRODUCER);
    assert!(matches!(
        service.receive(packet(8, &claimant), 4096),
        Err(ReceiveError::Role)
    ));
    let client = remote(1, handshake::PRODUCER);
    let (binding, old_hello) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    assert_eq!(binding.kind, Kind::Client);
    let broker = remote(8, handshake::OWNER | 8);
    let (broker_binding, _) = begin(&mut service, &broker, NodeId::from_bytes([8; 16]));
    assert_eq!(broker_binding.kind, Kind::Broker);
    let extra = remote(2, handshake::CONSUMER);
    assert!(matches!(
        service.receive(packet(2, &extra), 4096),
        Err(ReceiveError::Peer)
    ));
    assert!(service.disconnect(binding));
    assert!(service.links.get(binding.peer).is_none());
    service.receive(old_hello, 4096).unwrap();
    assert!(
        service.links.get(binding.peer).is_none(),
        "old HELLO resurrected the link"
    );
    // Retain the metadata fence rather than admitting an unbounded identity list.
    assert!(matches!(
        service.receive(packet(2, &extra), 4096),
        Err(ReceiveError::Peer)
    ));
    client.disconnect(local);
    let (current, _) = begin(&mut service, &client, binding.peer);
    assert_ne!(current.session, binding.session);
    assert!(!service.disconnect(binding));
    assert_eq!(service.links.get(binding.peer).unwrap().binding, current);
    assert_eq!(
        service.links.get(broker_binding.peer).unwrap().binding,
        broker_binding
    );
    // Only an independently retired physical source authorizes tombstone removal.
    assert!(!service.retire_transport_client(broker_binding.peer));
    assert!(service.retire_transport_client(current.peer));
    assert!(!service.retire_transport_client(current.peer));
    assert!(service.accepts_transport_peer(NodeId::from_bytes([2; 16])));
    let (replacement, _) = begin(&mut service, &extra, NodeId::from_bytes([2; 16]));
    assert_eq!(replacement.kind, Kind::Client);
    assert_eq!(service.peers.len(), 2);
    assert_eq!(service.sessions.session(current.peer), None);
}

pub(in crate::frontend) fn setup() -> (Service, [crate::frontend::DataReceiver; 2], [Placement; 2])
{
    let (mut dispatcher, lanes, placements, binding) = fixture();
    dispatcher.disconnect(binding.peer);
    let access = [
        Access {
            peer: binding.peer,
            kind: Kind::Client,
        },
        Access {
            peer: NodeId::from_bytes([8; 16]),
            kind: Kind::Broker,
        },
    ];
    let service =
        Service::new(dispatcher, profile(handshake::OWNER | 8), &access, ids(99)).unwrap();
    (service, lanes, placements)
}

pub(in crate::frontend) fn remote(id: u8, roles: u32) -> LinkSessions {
    LinkSessions::with_ids(
        NodeId::from_bytes([id; 16]),
        profile(roles),
        handshake::OWNER,
        1,
        ids(u64::from(id)),
    )
    .unwrap()
}

pub(in crate::frontend) fn begin(
    service: &mut Service,
    remote: &LinkSessions,
    peer: NodeId,
) -> (Binding, Message) {
    let local = service.dispatcher.local;
    let hello = Message::with_prefix(
        Bytes::copy_from_slice(peer.as_bytes()),
        Message::multipart(remote.start(local).unwrap()),
    );
    service.receive(hello.clone(), 4096).unwrap();
    (service.links.get(peer).unwrap().binding, hello)
}

pub(in crate::frontend) fn reply(binding: Binding, opcode: Opcode) -> Message {
    let envelope = Envelope {
        opcode,
        response: false,
        request_id: None,
        sender: NodeId::from_bytes([9; 16]),
        session: Some(binding.session),
    };
    let header = envelope
        .encode_header(0, 0, DataLimits::default().envelope)
        .unwrap();
    Message::multipart([
        Bytes::copy_from_slice(binding.peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::new(),
        Bytes::new(),
    ])
}

#[test]
fn claimed_broker_role_cannot_promote_authorized_client_or_fence_live_link() {
    let (mut service, _lanes, _) = setup();
    let peer = NodeId::from_bytes([1; 16]);
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, peer);
    let forged = remote(1, 8);
    let message = Message::with_prefix(
        Bytes::copy_from_slice(peer.as_bytes()),
        Message::multipart(forged.start(service.dispatcher.local).unwrap()),
    );
    assert!(matches!(
        service.receive(message, 4096),
        Err(ReceiveError::Role)
    ));
    assert_eq!(service.links.get(peer).unwrap().binding, binding);
    assert!(matches!(
        service.receive(Message::multipart([Bytes::from_static(b"unknown")]), 4096),
        Err(ReceiveError::Peer)
    ));
}

#[test]
fn stalled_handshake_and_recurring_hello_leave_other_links_and_data_runnable() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, hello) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    let stalled = NodeId::from_bytes([8; 16]);
    service.start(stalled).unwrap();
    let mut sent = [0; 2];
    for _ in 0..24 {
        service.receive(hello.clone(), 4096).unwrap();
        if service
            .dispatcher
            .queued_replies(binding.peer, Class::Data)
            .unwrap()
            .0
            == 0
        {
            service
                .try_reply(Class::Data, reply(binding, Opcode::Records))
                .unwrap();
        }
        let mut available = true;
        service
            .flush(|message| {
                if message.part_slice(0) == Some(stalled.as_bytes().as_slice()) || !available {
                    return Err(TrySendError::Full(message));
                }
                available = false;
                sent[usize::from(message.part_slice(1).unwrap()[5] == Opcode::Records as u8)] += 1;
                Ok(())
            })
            .unwrap();
    }
    assert!(
        sent[0] > 0 && sent[1] > 0,
        "handshake/data starvation: {sent:?}"
    );
    assert!(service.peers[&stalled].handshake.is_some());
    assert!(service.links.get(stalled).is_none());
    service.retry(binding.peer).unwrap();
    assert_eq!(service.links.get(binding.peer).unwrap().binding, binding);
}

#[test]
fn application_replies_wait_for_the_new_sessions_welcome_submission() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, hello) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    service
        .try_reply(Class::Control, reply(binding, Opcode::Commit))
        .unwrap();
    service.handshake_first = false;
    for _ in 0..4 {
        service
            .flush(|message| {
                assert_eq!(message.part_slice(1).unwrap()[5], Opcode::Welcome as u8);
                Err(TrySendError::Full(message))
            })
            .unwrap();
    }
    let mut sent = Vec::new();
    for _ in 0..4 {
        service
            .flush(|message| {
                sent.push(message.part_slice(1).unwrap()[5]);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(sent, [Opcode::Welcome as u8, Opcode::Commit as u8]);
    // A duplicate HELLO does not close the already opened application path.
    service.receive(hello, 4096).unwrap();
    service
        .try_reply(Class::Control, reply(binding, Opcode::Commit))
        .unwrap();
    service.handshake_first = false;
    service
        .flush(|message| {
            if message.part_slice(1).unwrap()[5] == Opcode::Welcome as u8 {
                return Err(TrySendError::Full(message));
            }
            assert_eq!(message.part_slice(1).unwrap()[5], Opcode::Commit as u8);
            Ok(())
        })
        .unwrap();
    assert!(!service.dispatcher.has_replies());
}

#[tokio::test]
async fn unroutable_welcome_waits_for_retry_without_ready_socket_spinning() {
    let (mut service, _lanes, _) = setup();
    let client = remote(1, handshake::PRODUCER);
    let (binding, hello) = begin(&mut service, &client, NodeId::from_bytes([1; 16]));
    service
        .try_reply(Class::Control, reply(binding, Opcode::Commit))
        .unwrap();
    service
        .flush(|message| {
            assert_eq!(message.part_slice(1).unwrap()[5], Opcode::Welcome as u8);
            Err(TrySendError::Error(omq_tokio::Error::Unroutable))
        })
        .unwrap();
    let context = omq_tokio::Context::new();
    let socket = context.socket(
        omq_tokio::SocketType::Peer,
        omq_tokio::Options::default().router_mandatory(true),
    );
    let socket = socket.identity_routing().unwrap();
    let mut ready = Box::pin(service.flush_ready(&socket));
    assert!(futures::poll!(ready.as_mut()).is_pending());
    drop(ready);
    service.receive(hello, 4096).unwrap();
    let mut sent = Vec::new();
    for _ in 0..4 {
        service
            .flush(|message| {
                sent.push(message.part_slice(1).unwrap()[5]);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(sent, [Opcode::Welcome as u8, Opcode::Commit as u8]);
    socket.into_inner().close().await.unwrap();
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one session drives snapshot, update, and replacement"
)]
fn watch_registration_returns_current_snapshot_and_old_session_cannot_reuse_it() {
    let (mut service, _lanes, placements) = setup();
    let peer = NodeId::from_bytes([1; 16]);
    let route = directory::RouteState {
        group: placements[0].group,
        config_epoch: 1,
        partition: placements[0].partition,
        members: [
            service.local(),
            NodeId::from_bytes([8; 16]),
            NodeId::from_bytes([7; 16]),
        ]
        .into(),
        view: 4,
        leader: Some(service.local()),
    };
    service
        .install_watches(
            WatchRegistry::new(
                super::super::WatchLimits {
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
    let client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &client, peer);
    let watch = RequestId::from_bytes([4; 16]);
    let request_id = RequestId::from_bytes([5; 16]);
    let mut metadata = Vec::with_capacity(64);
    let header = directory::encode_request(
        Envelope {
            opcode: Opcode::StateSnapshotRequest,
            response: false,
            request_id: Some(request_id),
            sender: peer,
            session: Some(binding.session),
        },
        &directory::SnapshotRequest {
            watch,
            groups: vec![route.group],
        },
        &mut metadata,
        DataLimits::default().envelope,
        directory::Limits::default(),
    )
    .unwrap();
    let request = Message::multipart([
        Bytes::copy_from_slice(peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ]);
    assert!(service.receive(request.clone(), 4096).unwrap().is_none());
    let mut sent = Vec::new();
    for _ in 0..2 {
        service
            .flush(|message| {
                sent.push(message);
                Ok(())
            })
            .unwrap();
    }
    let snapshot_message = sent
        .iter()
        .find(|message| message.part_slice(1).unwrap()[5] == Opcode::StateSnapshot as u8)
        .unwrap();
    let frames =
        std::array::from_fn::<_, 3, _>(|index| snapshot_message.part_slice(index + 1).unwrap());
    let packet = decode_packet(&frames, DataLimits::default().envelope).unwrap();
    assert_eq!(packet.envelope.request_id, Some(request_id));
    assert_eq!(
        directory::decode_snapshot(
            packet,
            DataLimits::default().envelope,
            directory::Limits::default()
        )
        .unwrap(),
        directory::Snapshot {
            watch,
            routes: vec![route.clone()]
        }
    );
    let mut newer = route;
    newer.view += 1;
    newer.leader = Some(NodeId::from_bytes([8; 16]));
    assert!(service.publish_route(&newer).unwrap());
    service
        .try_reply(Class::Control, reply(binding, Opcode::Commit))
        .unwrap();
    service
        .try_reply(Class::Control, reply(binding, Opcode::PrepareOk))
        .unwrap();
    assert!(!service.poll_watch().unwrap());
    service.flush(|_| Ok(())).unwrap();
    assert!(service.poll_watch().unwrap());
    let mut notices = Vec::new();
    for _ in 0..4 {
        service
            .flush(|message| {
                notices.push(message);
                Ok(())
            })
            .unwrap();
    }
    let update = notices
        .iter()
        .find(|message| message.part_slice(1).unwrap()[5] == Opcode::StateUpdate as u8)
        .unwrap();
    let frames = std::array::from_fn::<_, 3, _>(|index| update.part_slice(index + 1).unwrap());
    assert_eq!(
        directory::decode_update(
            decode_packet(&frames, DataLimits::default().envelope).unwrap(),
            DataLimits::default().envelope,
            directory::Limits::default()
        )
        .unwrap(),
        directory::Update {
            watch,
            route: newer
        }
    );
    service.start(peer).unwrap();
    assert!(matches!(
        service.receive(request, 4096),
        Err(ReceiveError::Watch(super::super::WatchError::Session))
    ));
}
