//! Topic lookups and route-watch requests on established client links.

use super::*;
use crate::frontend::{WatchError, WatchLimits};
use ozzy_proto::{decode_packet, directory};

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
#[expect(clippy::too_many_lines, reason = "one linear scenario")]
fn watch_rejections_are_correlated_and_leave_the_link_usable() {
    let (mut service, _lanes, placements) = setup();
    service
        .install_watches(
            WatchRegistry::new(
                WatchLimits {
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
    let mut client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &mut client, peer);
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
    let mut client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &mut client, peer);
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
    let mut client = remote(1, handshake::PRODUCER);
    let (binding, _) = begin(&mut service, &mut client, peer);
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
        Err(ReceiveError::Watch(WatchError::Session))
    ));
}
