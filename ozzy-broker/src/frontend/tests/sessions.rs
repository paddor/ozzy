use super::*;
use omq_tokio::Message;
use ozzy_proto::{
    LinkSessionId,
    data::DataLimits,
    handshake::{self, Parameters},
};
use ozzy_runtime::frontend::{LinkIds, LinkSessions};
use std::num::NonZeroU64;

const REPLICA: u32 = 8;
const REPLICATION: u16 = (1 << 3) | (1 << 8);

fn profile(roles: u32) -> Parameters {
    let mut parameters = Parameters::streaming(DataLimits::default(), roles).unwrap();
    parameters.capabilities |= handshake::OWNER_READ | REPLICATION;
    parameters.required_capabilities = 0;
    parameters
}

fn source(id: u8, roles: u32) -> LinkSessions {
    LinkSessions::with_ids(
        NodeId::from_bytes([id; 16]),
        profile(roles),
        handshake::OWNER,
        1,
        LinkIds::deterministic(NonZeroU64::new(u64::from(id)).unwrap()),
    )
    .unwrap()
}

async fn negotiate(socket: &Socket, sessions: &LinkSessions) -> LinkSessionId {
    let hello = sessions.start(local()).unwrap();
    socket
        .send(Message::with_prefix(
            Bytes::copy_from_slice(local().as_bytes()),
            Message::multipart(hello),
        ))
        .await
        .unwrap();
    let welcome = socket.recv().await.unwrap();
    assert_eq!(welcome.part_slice(0), Some(local().as_bytes().as_slice()));
    let frames = std::array::from_fn::<_, 3, _>(|i| welcome.part_slice(i + 1).unwrap());
    let packet = ozzy_proto::decode_packet(&frames, DataLimits::default().envelope).unwrap();
    assert!(sessions.receive(local(), packet).unwrap().replaced);
    sessions.session(local()).unwrap()
}

#[tokio::test]
async fn shared_peer_negotiates_client_and_broker_profiles_with_independent_fences() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let context = Context::new();
        let endpoints = endpoints(false);
        let roles = [handshake::PRODUCER, handshake::CONSUMER, REPLICA];
        let (reported, mut reports) = tokio::sync::mpsc::channel(4);
        let frontend = Frontend::start_with_context(
            &plan(),
            local(),
            &endpoints,
            limits(),
            context.clone(),
            move |mut context| async move {
                let sessions = LinkSessions::with_ids(
                    context.local,
                    profile(handshake::OWNER | REPLICA),
                    0,
                    3,
                    LinkIds::deterministic(NonZeroU64::new(9).unwrap()),
                )
                .unwrap();
                context.ready()?;
                loop {
                    let message = tokio::select! {
                        () = context.shutdown.requested() => break,
                        result = context.peer.recv() => result.unwrap(),
                    };
                    // The fixture explicitly trusts these three identities.
                    // A claimed role alone never selects broker authorization.
                    let index = (0..3)
                        .find(|&i| message.part_slice(0) == Some([i as u8 + 1; 16].as_slice()))
                        .expect("unconfigured test peer");
                    let remote = NodeId::from_bytes([index as u8 + 1; 16]);
                    let frames =
                        std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
                    let packet =
                        ozzy_proto::decode_packet(&frames, DataLimits::default().envelope).unwrap();
                    let offered =
                        handshake::decode(packet, DataLimits::default().envelope).unwrap();
                    assert_eq!(offered.parameters.roles, roles[index]);
                    let handled = sessions.receive(remote, packet).unwrap();
                    if let Some(reply) = handled.reply {
                        // Bounded fixture traffic fits the configured socket.
                        // Full in the serving adapter must leave HELLO retryable.
                        context
                            .peer
                            .try_send(Message::with_prefix(
                                Bytes::copy_from_slice(remote.as_bytes()),
                                Message::multipart(reply),
                            ))
                            .unwrap();
                    }
                    if handled.replaced {
                        reported
                            .try_send((remote, sessions.session(remote).unwrap()))
                            .unwrap();
                    }
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        let mut clients = Vec::new();
        for (index, role) in roles.into_iter().enumerate() {
            let id = index as u8 + 1;
            let socket = context.socket(
                SocketType::Peer,
                omq_tokio::Options::default()
                    .identity(Bytes::from(vec![id; 16]))
                    .router_mandatory(true)
                    .linger(Duration::from_millis(5)),
            );
            socket
                .connect(endpoints.peer.parse().unwrap())
                .await
                .unwrap();
            socket
                .wait_connected(1, Duration::from_secs(5))
                .await
                .unwrap();
            let sessions = source(id, role);
            let current = negotiate(&socket, &sessions).await;
            assert_eq!(
                reports.recv().await.unwrap(),
                (NodeId::from_bytes([id; 16]), current)
            );
            clients.push((socket, sessions, current));
        }
        let replacement = negotiate(&clients[0].0, &clients[0].1).await;
        assert_ne!(replacement, clients[0].2);
        assert_eq!(
            reports.recv().await.unwrap(),
            (NodeId::from_bytes([1; 16]), replacement)
        );
        for (_, sessions, previous) in &clients[1..] {
            assert_eq!(sessions.session(local()), Some(*previous));
        }
        for (socket, _, _) in clients {
            socket.close().await.unwrap();
        }
        frontend.shutdown().await.unwrap();
    })
    .await
    .expect("shared link negotiation stalled");
}

fn serving_service(
    trusted: bool,
) -> (
    ozzy_runtime::frontend::Service,
    Vec<ozzy_runtime::frontend::DataReceiver>,
) {
    use ozzy_runtime::{dispatch, frontend, replica_transport::QueueLimits};
    let mut receivers = Vec::new();
    let lanes = [0, 7]
        .into_iter()
        .map(|shard| {
            let (sender, receiver) = frontend::data_channel(
                &omq_tokio::Context::new(),
                shard,
                frontend::Kind::Client,
                dispatch::Class::Control,
                2,
                4096,
                8192,
            )
            .unwrap();
            receivers.push(receiver);
            (shard, sender)
        })
        .collect();
    let routes =
        frontend::RoutingTable::new(&[0, 7], &[], 1, DataLimits::default().envelope).unwrap();
    let queue = QueueLimits {
        messages: 8,
        bytes: 8192,
        message_bytes: 4096,
    };
    let dispatcher = frontend::Dispatcher::new(
        local(),
        routes,
        lanes,
        frontend::DispatcherLimits {
            peers: 1,

            replies: frontend::ReplyLimits {
                control: queue,
                data: queue,
            },
        },
    )
    .unwrap();
    let access = [frontend::Access {
        peer: NodeId::from_bytes([1; 16]),
        kind: frontend::Kind::Client,
    }];
    let service = frontend::Service::new(
        dispatcher,
        profile(handshake::OWNER | REPLICA),
        if trusted { &[] } else { &access },
        LinkIds::deterministic(NonZeroU64::new(9).unwrap()),
    )
    .unwrap();
    let service = if trusted {
        service.with_trusted_clients(1).unwrap()
    } else {
        service
    };
    (service, receivers)
}

#[tokio::test]
async fn serving_rejects_bad_input_and_fences_disconnected_client_sessions() {
    serving_lifecycle(false, false).await;
}

#[tokio::test]
async fn serving_fences_dynamically_admitted_client_disconnects() {
    serving_lifecycle(true, false).await;
}

#[tokio::test]
async fn serving_reclaims_one_client_slot_across_forty_inproc_identities() {
    serving_lifecycle(true, true).await;
}

async fn serving_lifecycle(trusted: bool, distinct: bool) {
    use ozzy_runtime::frontend::{ReceiveBuffers, ReceiveStorage};
    tokio::time::timeout(Duration::from_secs(10), async {
        let omq = Context::new();
        let endpoints = endpoints(false);
        let (reported, report) = oneshot::channel();
        let frontend = Frontend::start_with_context(
            &plan(),
            local(),
            &endpoints,
            limits(),
            omq.clone(),
            move |context| async move {
                let (service, _receivers) = serving_service(trusted);
                reported.send(service.links()).unwrap();
                let buffers = ReceiveBuffers::new(
                    DataLimits::default().envelope,
                    ReceiveStorage::Inproc {
                        payload_backing_bytes: 4096,
                    },
                )
                .unwrap();
                context
                    .serve(
                        service,
                        std::collections::BTreeMap::new(),
                        crate::FollowerRoutes::default(),
                        buffers,
                        Duration::from_millis(10),
                    )
                    .await
            },
        )
        .await
        .unwrap();
        let links = report.await.unwrap();
        let repeated = source(1, handshake::PRODUCER);
        let mut previous = None;
        for wave in 0..if distinct { 40 } else { 2 } {
            let id = if distinct { wave + 32 } else { 1 };
            let remote = NodeId::from_bytes([id; 16]);
            let fresh = source(id, handshake::PRODUCER);
            let sessions = if distinct { &fresh } else { &repeated };
            let socket = omq.socket(
                SocketType::Peer,
                omq_tokio::Options::default()
                    .identity(Bytes::copy_from_slice(remote.as_bytes()))
                    .router_mandatory(true)
                    .linger(Duration::from_millis(5)),
            );
            socket
                .connect(endpoints.peer.parse().unwrap())
                .await
                .unwrap();
            socket
                .wait_connected(1, Duration::from_secs(5))
                .await
                .unwrap();
            socket
                .send(Message::with_prefix(
                    Bytes::copy_from_slice(local().as_bytes()),
                    Message::single("malformed native packet"),
                ))
                .await
                .unwrap();
            let hello = sessions.start(local()).unwrap();
            let current = negotiate(&socket, sessions).await;
            assert_ne!(Some(current), previous);
            assert_eq!(links.get(remote).unwrap().binding.session, current);
            previous = Some(current);
            // Leave an already accepted HELLO retry queued while this identity
            // closes. Reclamation must not let it occupy the one slot again.
            socket
                .send(Message::with_prefix(
                    Bytes::copy_from_slice(local().as_bytes()),
                    Message::multipart(hello),
                ))
                .await
                .unwrap();
            socket.close().await.unwrap();
            loop {
                let generation = links.generation();
                if links.get(remote).is_none() {
                    break;
                }
                links.changed_after(generation).await;
            }
        }
        frontend.shutdown().await.unwrap();
    })
    .await
    .expect("serving session lifecycle stalled");
}

#[tokio::test]
async fn paused_repair_source_preserves_control_other_shards_and_reconnect() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let context = Context::new();
        let endpoint: omq_tokio::Endpoint = endpoints(false).peer.parse().unwrap();
        let bound = context
            .socket(
                SocketType::Peer,
                omq_tokio::Options::default().identity(Bytes::copy_from_slice(local().as_bytes())),
            )
            .identity_routing()
            .unwrap();
        bound.bind(endpoint.clone()).await.unwrap();
        let broker = NodeId::from_bytes([1; 16]);
        let identities = [
            broker,
            crate::FollowerRoutes::identity(broker, local(), 0),
            crate::FollowerRoutes::identity(broker, local(), 7),
        ];
        let mut sockets = Vec::new();
        for identity in identities {
            let socket = context
                .socket(
                    SocketType::Peer,
                    omq_tokio::Options::default()
                        .identity(Bytes::copy_from_slice(identity.as_bytes()))
                        .router_mandatory(true),
                )
                .identity_routing()
                .unwrap();
            socket.connect(endpoint.clone()).await.unwrap();
            socket
                .wait_connected(1, Duration::from_secs(5))
                .await
                .unwrap();
            sockets.push(socket);
        }
        sockets[1]
            .send_to(local().as_bytes(), Message::single("held repair"))
            .await
            .unwrap();
        let (receipt, body) = bound.recv_from_source(None).await.unwrap();
        assert_eq!(
            receipt.identity_bytes().as_deref(),
            Some(identities[1].as_bytes().as_slice())
        );
        let old_source = receipt.source().unwrap().clone();
        bound.unshift(receipt, body).unwrap();
        // An unshifted repair lane must not intercept either eligible source.
        for index in [0, 2] {
            sockets[index]
                .send_to(local().as_bytes(), Message::single("progress"))
                .await
                .unwrap();
            let (receipt, _) = bound.recv_from_source(None).await.unwrap();
            assert_eq!(
                receipt.identity_bytes().as_deref(),
                Some(identities[index].as_bytes().as_slice())
            );
        }
        sockets[1].clone().into_inner().close().await.unwrap();
        let replacement = context
            .socket(
                SocketType::Peer,
                omq_tokio::Options::default()
                    .identity(Bytes::copy_from_slice(identities[1].as_bytes()))
                    .router_mandatory(true),
            )
            .identity_routing()
            .unwrap();
        replacement.connect(endpoint).await.unwrap();
        replacement
            .wait_connected(1, Duration::from_secs(5))
            .await
            .unwrap();
        replacement
            .send_to(local().as_bytes(), Message::single("replacement"))
            .await
            .unwrap();
        let (receipt, body) = bound.recv_from_source(None).await.unwrap();
        assert_eq!(body.part_slice(0), Some(b"replacement".as_slice()));
        assert_ne!(receipt.source().unwrap(), &old_source);
        assert!(matches!(
            bound.try_recv_from_source(Some(&old_source)),
            Err(omq_tokio::Error::Closed)
        ));
        replacement.into_inner().close().await.unwrap();
        for socket in sockets {
            socket.into_inner().close().await.unwrap();
        }
        bound.into_inner().close().await.unwrap();
    })
    .await
    .expect("isolated follower repair stalled");
}
