use super::*;
use crate::frontend::test_support::*;
use bytes::Bytes;
use ozzy_proto::{Envelope, RequestId};

#[test]
fn one_link_routes_multiple_partitions_to_independent_local_placements() {
    let routes = [placement(0, 0), placement(1, 17), placement(2, 17)];
    let first = RoutingTable::new(&[0, 17], &routes, 3, EnvelopeLimits::default()).unwrap();
    let other_routes = routes.map(|route| Placement { shard: 81, ..route });
    let other = RoutingTable::new(&[81], &other_routes, 3, EnvelopeLimits::default()).unwrap();
    let binding = binding(Kind::Client);
    for (index, &target) in routes.iter().enumerate() {
        let message = append(target, binding);
        let before = message.part_slice(3).unwrap().as_ptr();
        let destination = first.route(&message, binding).unwrap();
        assert_eq!(destination.placement, target);
        assert_eq!(destination.class, Class::Data);
        assert_eq!(destination.writer, Some(ProducerId::from_bytes([4; 16])));
        assert_eq!(
            other.route(&message, binding).unwrap().placement,
            other_routes[index]
        );
        assert_eq!(message.part_slice(3).unwrap().as_ptr(), before);
    }
}

#[test]
fn reader_control_uses_same_partition_mapping_without_writer_data_credit() {
    let target = placement(0, 7);
    let table = RoutingTable::new(&[7], &[target], 1, EnvelopeLimits::default()).unwrap();
    let binding = binding(Kind::Client);
    for opcode in [Opcode::Subscribe, Opcode::Ack, Opcode::Unsubscribe] {
        let destination = table
            .route(&reader(target, binding, opcode), binding)
            .unwrap();
        assert_eq!(
            destination,
            Routed {
                placement: target,
                class: Class::Control,
                writer: None
            }
        );
    }
}

#[test]
fn producer_open_uses_partition_control_without_writer_data_credit() {
    let target = placement(0, 7);
    let table = RoutingTable::new(&[7], &[target], 1, EnvelopeLimits::default()).unwrap();
    let binding = binding(Kind::Client);
    let mut metadata = Vec::with_capacity(256);
    let header = producer::encode_open(
        envelope(Opcode::OpenProducer, binding),
        producer::Open {
            authority: authority(target),
            partition: target.partition,
            producer: ProducerId::from_bytes([4; 16]),
            mode: producer::Mode::Resume,
            expected_epoch: None,
            operation: ozzy_proto::OperationId::from_bytes([5; 16]),
        },
        &mut metadata,
        EnvelopeLimits::default(),
    )
    .unwrap();
    let message = Message::multipart([
        Bytes::copy_from_slice(binding.peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ]);
    assert_eq!(
        table.route(&message, binding).unwrap(),
        Routed {
            placement: target,
            class: Class::Control,
            writer: None
        }
    );
    assert!(
        table
            .route(
                &message,
                Binding {
                    kind: Kind::Broker,
                    ..binding
                }
            )
            .is_err()
    );
}

#[test]
fn broker_control_requires_its_own_role_and_does_not_route_by_leader() {
    let routes = [placement(0, 0), placement(1, 7)];
    let table = RoutingTable::new(&[0, 7], &routes, 2, EnvelopeLimits::default()).unwrap();
    let broker = binding(Kind::Broker);
    for target in routes {
        let message = control(target, broker);
        assert_eq!(
            table.route(&message, broker).unwrap(),
            Routed {
                placement: target,
                class: Class::Control,
                writer: None,
            }
        );
        assert!(
            table
                .route(
                    &message,
                    Binding {
                        kind: Kind::Client,
                        ..broker
                    }
                )
                .is_err()
        );
    }
    assert!(table.route(&append(routes[0], broker), broker).is_err());
}

#[test]
fn obsolete_sessions_foreign_incarnations_and_excessive_frames_never_route() {
    let target = placement(0, 0);
    let table = RoutingTable::new(&[0], &[target], 1, EnvelopeLimits::default()).unwrap();
    let binding = binding(Kind::Client);
    let message = append(target, binding);
    assert!(matches!(
        table.route(
            &message,
            Binding {
                session: LinkSessionId::from_bytes([99; 16]),
                ..binding
            }
        ),
        Err(RoutingError::Session)
    ));
    assert!(matches!(
        table.route(
            &message,
            Binding {
                peer: NodeId::from_bytes([99; 16]),
                ..binding
            }
        ),
        Err(RoutingError::Peer)
    ));
    assert!(matches!(
        table.route(&append(placement(1, 0), binding), binding),
        Err(RoutingError::Partition)
    ));
    let foreign = Placement {
        partition: PartitionIncarnation::from_bytes([99; 16]),
        ..target
    };
    assert!(matches!(
        table.route(&append(foreign, binding), binding),
        Err(RoutingError::Partition)
    ));
    let short = Message::multipart(message.iter().take(3));
    assert!(matches!(
        table.route(&short, binding),
        Err(RoutingError::Peer)
    ));
    let bounded = RoutingTable::new(
        &[0],
        &[target],
        1,
        EnvelopeLimits {
            max_payload_bytes: 3,
            ..EnvelopeLimits::default()
        },
    )
    .unwrap();
    assert!(matches!(
        bounded.route(&message, binding),
        Err(RoutingError::Envelope(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_peer_receive_multiplexes_native_partitions_over_one_connection() {
    use omq_tokio::{Context, Options, SocketType};
    let routes = [placement(0, 0), placement(1, 17)];
    let table = RoutingTable::new(&[0, 17], &routes, 2, EnvelopeLimits::default()).unwrap();
    let binding = binding(Kind::Client);
    let context = Context::new();
    let broker = NodeId::from_bytes([9; 16]);
    let options = |node: NodeId| {
        Options::default()
            .identity(Bytes::copy_from_slice(node.as_bytes()))
            .router_mandatory(true)
            .send_hwm(16)
            .recv_hwm(16)
            .max_message_size(4096)
    };
    let server = context.socket(SocketType::Peer, options(broker));
    let sdk = context.socket(SocketType::Peer, options(binding.peer));
    let endpoint = server
        .bind(
            format!("inproc://frontend-routing-{}", RequestId::new())
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
    sdk.connect(endpoint).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        sdk.wait_connected(1, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        server
            .wait_connected(1, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        for target in routes {
            let incoming = append(target, binding);
            let outgoing = Message::multipart(
                std::iter::once(Bytes::copy_from_slice(broker.as_bytes()))
                    .chain((1..4).map(|index| incoming.part_bytes(index).unwrap())),
            );
            sdk.send(outgoing).await.unwrap();
            let received = server.recv().await.unwrap();
            assert_eq!(table.route(&received, binding).unwrap().placement, target);
            assert_eq!(received.part_slice(3), incoming.part_slice(3));
        }
    })
    .await
    .expect("ordinary PEER dispatch stalled");
    sdk.close().await.unwrap();
    server.close().await.unwrap();
}

#[test]
fn route_tables_reject_duplicate_unknown_and_zero_destinations() {
    let first = placement(0, 7);
    let limits = EnvelopeLimits::default();
    assert!(RoutingTable::new(&[], &[first], 1, limits).is_err());
    assert!(RoutingTable::new(&[7, 7], &[first], 1, limits).is_err());
    assert!(RoutingTable::new(&[0], &[first], 1, limits).is_err());
    assert!(RoutingTable::new(&[7], &[first], 0, limits).is_err());
    assert!(RoutingTable::new(&[7], &[first, placement(1, 7)], 1, limits).is_err());
    assert!(RoutingTable::new(&[7], &[first, first], 2, limits).is_err());
    let reused = Placement {
        partition: first.partition,
        ..placement(1, 7)
    };
    assert!(RoutingTable::new(&[7], &[first, reused], 2, limits).is_err());
    let zero = Placement {
        group: GroupId::from_bytes([0; 16]),
        ..first
    };
    assert!(RoutingTable::new(&[7], &[zero], 1, limits).is_err());
}

#[test]
fn routing_class_does_not_validate_broker_payload_or_allow_legacy_prepare() {
    let target = placement(0, 7);
    let table = RoutingTable::new(&[7], &[target], 1, EnvelopeLimits::default()).unwrap();
    let binding = binding(Kind::Broker);
    let original = control(target, binding);
    let metadata = original.part_bytes(2).unwrap();
    let payload = Bytes::from_static(b"invalid canonical payload");
    let mut envelope = envelope(Opcode::PrepareFlow, binding);
    envelope.request_id = None;
    let message = |envelope: Envelope| {
        Message::multipart([
            original.part_bytes(0).unwrap(),
            Bytes::copy_from_slice(
                &envelope
                    .encode_header(metadata.len(), payload.len(), EnvelopeLimits::default())
                    .unwrap(),
            ),
            metadata.clone(),
            payload.clone(),
        ])
    };
    assert_eq!(
        table.route(&message(envelope), binding).unwrap().class,
        Class::Data
    );
    envelope.opcode = Opcode::Prepare;
    assert!(matches!(
        table.route(&message(envelope), binding),
        Err(RoutingError::Command)
    ));
    envelope.opcode = Opcode::Commit;
    assert!(matches!(
        table.route(&message(envelope), binding),
        Err(RoutingError::Broker(wire::WireError::Payload))
    ));
}
