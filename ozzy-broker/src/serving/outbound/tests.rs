use super::*;
use bytes::Bytes;
use ozzy_proto::{Envelope, LinkSessionId, RequestId};

fn limits() -> EnvelopeLimits {
    EnvelopeLimits {
        max_metadata_bytes: 64 * 1024,
        max_payload_bytes: 2 * 1024 * 1024,
    }
}

fn frame(opcode: Opcode, bytes: usize) -> Message {
    frame_id(opcode, bytes, 1)
}

fn frame_id(opcode: Opcode, bytes: usize, id: u8) -> Message {
    let envelope = Envelope {
        opcode,
        response: false,
        request_id: (opcode != Opcode::PreparePub).then_some(RequestId::from_bytes([id; 16])),
        sender: NodeId::from_bytes([2; 16]),
        session: (opcode != Opcode::PreparePub).then_some(LinkSessionId::from_bytes([3; 16])),
    };
    let header = envelope.encode_header(0, bytes, limits()).unwrap();
    Message::with_prefix(
        Bytes::copy_from_slice(NodeId::from_bytes([4; 16]).as_bytes()),
        Message::multipart([
            Bytes::copy_from_slice(&header),
            Bytes::new(),
            Bytes::from(vec![0; bytes]),
        ]),
    )
}

#[test]
fn configured_large_replica_and_reader_frames_use_data_capacity() {
    for opcode in [Opcode::PrepareFlow, Opcode::Ops, Opcode::Records] {
        let mut outbound = Outbound::new(1, 2 * 1024 * 1024, limits());
        let message = frame(opcode, 1024 * 1024 + 4096);
        assert!(outbound.send(message).is_ok(), "{opcode:?}");
        assert_eq!(outbound.usage.len(), 1);
        assert_eq!(
            outbound.usage[&(NodeId::from_bytes([4; 16]), Class::Data)].0,
            1
        );
    }
}

/// A dispatcher thread with one broker peer, as production sizes it, whose
/// per-peer reply queues hold `queued` messages.
fn dispatcher(
    queued: usize,
) -> (
    ozzy_runtime::frontend::Service,
    Port,
    ozzy_runtime::frontend::DataReceiver,
) {
    use ozzy_proto::{GroupId, PartitionIncarnation, data::DataLimits, handshake};
    use ozzy_runtime::{
        dispatch::{Budget, Budgets},
        frontend::{
            Access, Dispatcher, DispatcherLimits, Kind, LinkIds, Placement, ReplyLimits,
            RoutingTable, Service,
        },
        replica_transport::QueueLimits,
    };
    let budgets = Budgets {
        data: Budget {
            queue_slots: 8,
            retained_messages: 8,
            bytes: 1024 * 1024,
        },
        control: Budget {
            queue_slots: 16,
            retained_messages: 16,
            bytes: 1024 * 1024,
        },
    };
    let (sender, shard) = ozzy_runtime::frontend::data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        Class::Control,
        ozzy_runtime::frontend::DataLaneLimits {
            slots: 8,
            maximum_retained_bytes: 128 * 1024,
            retained_budget_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let routes = RoutingTable::new(
        &[0],
        &[Placement {
            shard: 0,
            group: GroupId::from_bytes([10; 16]),
            partition: PartitionIncarnation::from_bytes([20; 16]),
        }],
        1,
        limits(),
    )
    .unwrap();
    let queue = QueueLimits {
        messages: queued,
        bytes: 1024 * 1024,
        message_bytes: 128 * 1024,
    };
    let dispatcher = Dispatcher::new(
        NodeId::from_bytes([9; 16]),
        routes,
        vec![(0, sender)],
        DispatcherLimits {
            peers: 2,
            replies: ReplyLimits {
                control: queue,
                data: queue,
            },
        },
    )
    .unwrap();
    let mut parameters =
        handshake::Parameters::streaming(DataLimits::default(), handshake::OWNER | 8).unwrap();
    parameters.capabilities |= handshake::OWNER_READ | (1 << 3) | (1 << 8);
    parameters.required_capabilities = 0;
    let mut service = Service::new(
        dispatcher,
        parameters,
        &[Access {
            peer: NodeId::from_bytes([4; 16]),
            kind: Kind::Broker,
        }],
        LinkIds::random(),
    )
    .unwrap();
    let port = service
        .port(&omq_tokio::Context::new(), 0, budgets)
        .unwrap();
    (service, port, shard)
}

/// Bind peer 4 as a broker link and return its binding.
fn bind(service: &mut ozzy_runtime::frontend::Service) -> ozzy_runtime::frontend::Binding {
    use ozzy_proto::{data::DataLimits, handshake};
    use ozzy_runtime::frontend::{LinkIds, LinkSessions};
    let peer = NodeId::from_bytes([4; 16]);
    let mut parameters =
        handshake::Parameters::streaming(DataLimits::default(), handshake::OWNER | 8).unwrap();
    parameters.capabilities |= handshake::OWNER_READ | (1 << 3) | (1 << 8);
    parameters.required_capabilities = 0;
    let mut remote =
        LinkSessions::with_ids(peer, parameters, handshake::OWNER, 1, LinkIds::random()).unwrap();
    let hello = Message::with_prefix(
        Bytes::copy_from_slice(peer.as_bytes()),
        Message::multipart(remote.start(NodeId::from_bytes([9; 16])).unwrap()),
    );
    service.receive(hello, 4096).unwrap();
    service.links().get(peer).unwrap().binding
}

/// A Commit reply to the bound peer, correlated by `id`.
fn bound_frame(binding: ozzy_runtime::frontend::Binding, id: u8) -> Message {
    let envelope = Envelope {
        opcode: Opcode::Commit,
        response: true,
        request_id: Some(RequestId::from_bytes([id; 16])),
        sender: NodeId::from_bytes([9; 16]),
        session: Some(binding.session),
    };
    let header = envelope.encode_header(0, 0, limits()).unwrap();
    Message::with_prefix(
        Bytes::copy_from_slice(binding.peer.as_bytes()),
        Message::multipart([Bytes::copy_from_slice(&header), Bytes::new(), Bytes::new()]),
    )
}

fn commit_id(message: &Message) -> Option<u8> {
    if message.len() != 4 {
        return None;
    }
    let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
    let packet = ozzy_proto::decode_packet(&frames, limits()).ok()?;
    (packet.envelope.opcode == Opcode::Commit)
        .then(|| packet.envelope.request_id.map(|id| id.as_bytes()[0]))
        .flatten()
}

#[test]
fn one_destination_receives_replies_in_send_order_across_a_refusal() {
    let (mut service, mut port, _shard) = dispatcher(1);
    let binding = bind(&mut service);
    // Production sizing: 32 clients and the two other brokers.
    let mut outbound = Outbound::new(34, 128 * 1024, limits());
    let mut sent = Vec::new();
    let flush = |service: &mut ozzy_runtime::frontend::Service, sent: &mut Vec<u8>| {
        while service
            .flush(|message| {
                sent.extend(commit_id(&message));
                Ok(())
            })
            .unwrap()
        {}
    };
    flush(&mut service, &mut sent);
    // The dispatcher's queue for this peer is full before the shard sends.
    service
        .try_reply(Class::Control, bound_frame(binding, 9))
        .unwrap();
    outbound.send(bound_frame(binding, 1)).unwrap();
    outbound.send(bound_frame(binding, 2)).unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut now = Duration::from_millis(1);
    // Reply 1 is refused for capacity. The queued frame then leaves, so
    // reply 2 would fit, but it must wait behind reply 1's retry.
    outbound.poll(&mut cx, &mut port, now).unwrap();
    assert!(service.poll_command().unwrap());
    flush(&mut service, &mut sent);
    assert_eq!(sent, [9]);
    assert!(service.poll_command().unwrap());
    // The next turn observes the refusal and reports when it retries.
    now += Duration::from_millis(5);
    outbound.poll(&mut cx, &mut port, now).unwrap();
    assert_eq!(
        outbound.next_retry(now),
        Some(now + Duration::from_millis(10))
    );
    for _ in 0..64 {
        now += Duration::from_millis(5);
        outbound.poll(&mut cx, &mut port, now).unwrap();
        while service.poll_command().unwrap() {}
        flush(&mut service, &mut sent);
        if outbound.usage.is_empty() {
            break;
        }
    }
    assert!(outbound.usage.is_empty(), "replies still queued: {sent:?}");
    assert_eq!(sent, [9, 1, 2]);
}

#[test]
fn queued_replies_settle_within_two_turns_of_a_responsive_dispatcher() {
    use ozzy_proto::{GroupId, PartitionIncarnation, data::DataLimits, handshake};
    use ozzy_runtime::{
        dispatch::{Budget, Budgets},
        frontend::{
            Access, Dispatcher, DispatcherLimits, Kind, LinkIds, Placement, ReplyLimits,
            RoutingTable, Service,
        },
        replica_transport::QueueLimits,
    };
    let budgets = Budgets {
        data: Budget {
            queue_slots: 8,
            retained_messages: 8,
            bytes: 1024 * 1024,
        },
        control: Budget {
            queue_slots: 16,
            retained_messages: 16,
            bytes: 1024 * 1024,
        },
    };
    let (sender, _shard) = ozzy_runtime::frontend::data_channel(
        &omq_tokio::Context::new(),
        0,
        Kind::Client,
        Class::Control,
        ozzy_runtime::frontend::DataLaneLimits {
            slots: 8,
            maximum_retained_bytes: 128 * 1024,
            retained_budget_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let routes = RoutingTable::new(
        &[0],
        &[Placement {
            shard: 0,
            group: GroupId::from_bytes([10; 16]),
            partition: PartitionIncarnation::from_bytes([20; 16]),
        }],
        1,
        limits(),
    )
    .unwrap();
    let queue = QueueLimits {
        messages: 8,
        bytes: 1024 * 1024,
        message_bytes: 128 * 1024,
    };
    let dispatcher = Dispatcher::new(
        NodeId::from_bytes([9; 16]),
        routes,
        vec![(0, sender)],
        DispatcherLimits {
            peers: 2,

            replies: ReplyLimits {
                control: queue,
                data: queue,
            },
        },
    )
    .unwrap();
    let mut parameters =
        handshake::Parameters::streaming(DataLimits::default(), handshake::OWNER | 8).unwrap();
    parameters.capabilities |= handshake::OWNER_READ | (1 << 3) | (1 << 8);
    parameters.required_capabilities = 0;
    let mut service = Service::new(
        dispatcher,
        parameters,
        &[Access {
            peer: NodeId::from_bytes([4; 16]),
            kind: Kind::Broker,
        }],
        LinkIds::random(),
    )
    .unwrap();
    let mut port = service
        .port(&omq_tokio::Context::new(), 0, budgets)
        .unwrap();
    // Production sizing: 32 clients and the two other brokers.
    let mut outbound = Outbound::new(34, 128 * 1024, limits());
    for _ in 0..8 {
        outbound.send(frame(Opcode::Commit, 0)).unwrap();
    }
    assert!(matches!(
        outbound.send(frame(Opcode::Commit, 0)),
        Err(TrySendError::Full(_))
    ));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut turns = 0;
    while !outbound.usage.is_empty() {
        turns += 1;
        assert!(
            turns <= 2,
            "queued replies still hold per-peer slots in turn {turns}"
        );
        outbound
            .poll(&mut cx, &mut port, Duration::from_millis(turns))
            .unwrap();
        while service.poll_command().unwrap() {}
    }
    assert!(outbound.send(frame(Opcode::Commit, 0)).is_ok());
}

#[test]
fn a_pending_publication_preserves_other_partitions_without_using_reply_capacity() {
    let mut outbound = Outbound::new(1, 2 * 1024 * 1024, limits());
    let publication = |partition| {
        Message::multipart([
            Bytes::from(vec![partition; 32]),
            Bytes::new(),
            Bytes::new(),
            Bytes::from_static(b"records"),
        ])
    };
    outbound.send(publication(1)).unwrap();
    let Err(TrySendError::Full(second)) = outbound.send(publication(2)) else {
        panic!("another partition's publication must remain available for retry");
    };
    assert_eq!(second.part_slice(0), Some([2; 32].as_slice()));
    assert_eq!(second.part_slice(3), Some(b"records".as_slice()));
    assert!(outbound.usage.is_empty());
    outbound.send(frame(Opcode::Appended, 0)).unwrap();
    let Some(Publication::Ready(first)) = outbound.publications[1].take() else {
        panic!("first publication must remain queued");
    };
    assert_eq!(first.part_slice(0), Some([1; 32].as_slice()));
    outbound.send(second).unwrap();
    assert_eq!(outbound.usage.len(), 1);
}

#[test]
fn follower_publication_pressure_preserves_reader_publication_capacity() {
    let mut outbound = Outbound::new(1, 2 * 1024 * 1024, limits());
    outbound.send(frame(Opcode::PreparePub, 32)).unwrap();
    let reader = Message::multipart([
        Bytes::from(vec![1; 32]),
        Bytes::new(),
        Bytes::new(),
        Bytes::from_static(b"records"),
    ]);
    outbound.send(reader).unwrap();
    assert!(outbound.publications.iter().all(Option::is_some));
    assert!(outbound.usage.is_empty());
    assert!(matches!(
        outbound.send(frame(Opcode::PreparePub, 32)),
        Err(TrySendError::Full(_))
    ));
}

#[test]
fn large_control_frames_cannot_consume_data_capacity() {
    let mut outbound = Outbound::new(1, 2 * 1024 * 1024, limits());
    assert!(matches!(
        outbound.send(frame(Opcode::Commit, 1024 * 1024 + 4096)),
        Err(TrySendError::Full(_))
    ));
    assert!(outbound.usage.is_empty());
}
