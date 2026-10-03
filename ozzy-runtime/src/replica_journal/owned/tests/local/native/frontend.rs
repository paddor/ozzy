use super::*;
use crate::{
    dispatch::{Budget, Budgets, Class},
    frontend::{
        Access, DataInput, DataReceiver, Dispatcher, DispatcherLimits, Kind, LinkIds, LinkSessions,
        Placement, Port, ReceiveBuffers, ReceiveStorage, ReplyError, ReplyLimits, RoutingTable,
        Service, data_channel,
    },
    replica_actor::{PartitionActor, PartitionActors},
    replica_transport::QueueLimits,
};
use omq_tokio::{Context as OmqContext, Options, Socket, SocketType};
use std::{num::NonZeroU64, time::Duration};

mod shared;

struct Frontend {
    service: Service,
    input: TestInput,
    routes: RoutingTable,
    _port: Port,
    server: Socket,
    sdk: Socket,
    link: Link,
    local: NodeId,
    _context: OmqContext,
}

fn budgets() -> Budgets {
    let budget = Budget {
        queue_slots: 4,
        retained_messages: 8,
        bytes: 64 * 1024,
    };
    Budgets {
        data: budget,
        control: budget,
    }
}

struct TestInput([DataReceiver; 2]);
impl TestInput {
    fn try_recv(&mut self) -> Result<Option<DataInput>, crate::frontend::DataLaneError> {
        for lane in [1, 0] {
            if let Some(input) = self.0[lane].try_recv()? {
                return Ok(Some(input));
            }
        }
        Ok(None)
    }
}

fn service(local: NodeId, group: GroupId) -> (Service, TestInput) {
    let (data, data_rx) = data_channel(
        &omq_tokio::Context::new(),
        7,
        Kind::Client,
        Class::Data,
        4,
        8192,
        65536,
    )
    .unwrap();
    let (control, control_rx) = data_channel(
        &omq_tokio::Context::new(),
        7,
        Kind::Client,
        Class::Control,
        4,
        8192,
        65536,
    )
    .unwrap();
    let routes = crate::frontend::RoutingTable::new(
        &[7],
        &[Placement {
            group,
            partition: partition(),
            shard: 7,
        }],
        1,
        limits().envelope,
    )
    .unwrap();
    let queue = QueueLimits {
        messages: 8,
        bytes: 16 * 1024,
        message_bytes: 4096,
    };
    let dispatcher = Dispatcher::new(
        local,
        routes,
        vec![(7, data), (7, control)],
        DispatcherLimits {
            peers: 2,

            replies: ReplyLimits {
                data: queue,
                control: queue,
            },
        },
    )
    .unwrap();
    let parameters = handshake::Parameters::streaming(limits(), handshake::OWNER).unwrap();
    let service = Service::new(
        dispatcher,
        parameters,
        &[Access {
            peer: link(70, 80).binding.peer,
            kind: Kind::Client,
        }],
        LinkIds::deterministic(NonZeroU64::new(99).unwrap()),
    )
    .unwrap();
    (service, TestInput([data_rx, control_rx]))
}

impl Frontend {
    async fn new(local: NodeId, group: GroupId) -> Self {
        let (mut service, input) = service(local, group);
        let context = OmqContext::new();
        let options = |node: NodeId| {
            Options::default()
                .identity(Bytes::copy_from_slice(node.as_bytes()))
                .router_mandatory(true)
                .send_hwm(8)
                .recv_hwm(8)
                .max_message_size(4096)
        };
        let server = context.socket(SocketType::Peer, options(local));
        let sdk = context.socket(SocketType::Peer, options(link(70, 80).binding.peer));
        let endpoint = server
            .bind(
                format!("inproc://native-shard-{}", RequestId::new())
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();
        sdk.connect(endpoint).await.unwrap();
        sdk.wait_connected(1, Duration::from_secs(5)).await.unwrap();
        server
            .wait_connected(1, Duration::from_secs(5))
            .await
            .unwrap();
        let remote = LinkSessions::with_ids(
            link(70, 80).binding.peer,
            link(70, 80).remote,
            handshake::OWNER,
            1,
            LinkIds::deterministic(NonZeroU64::new(70).unwrap()),
        )
        .unwrap();
        sdk.send(Message::with_prefix(
            Bytes::copy_from_slice(local.as_bytes()),
            Message::multipart(remote.start(local).unwrap()),
        ))
        .await
        .unwrap();
        service.receive(server.recv().await.unwrap(), 4096).unwrap();
        service.flush(|message| server.try_send(message)).unwrap();
        let welcome = sdk.recv().await.unwrap();
        remote.receive(local, packet(&welcome)).unwrap();
        let link = service.links().get(link(70, 80).binding.peer).unwrap();
        assert_eq!(remote.session(local), Some(link.binding.session));
        let port = service.port(&context, 7, budgets()).unwrap();
        let routes = RoutingTable::new(
            &[7],
            &[Placement {
                group,
                partition: partition(),
                shard: 7,
            }],
            1,
            limits().envelope,
        )
        .unwrap();
        Self {
            service,
            input,
            routes,
            _port: port,
            server,
            sdk,
            link,
            local,
            _context: context,
        }
    }

    async fn deliver(&mut self, actors: &mut PartitionActors, group: GroupId, message: Message) {
        let outgoing = Message::multipart(
            std::iter::once(Bytes::copy_from_slice(self.local.as_bytes()))
                .chain((1..4).map(|index| message.part_bytes(index).unwrap())),
        );
        self.sdk.send(outgoing).await.unwrap();
        let incoming = self.server.recv().await.unwrap();
        let buffers = ReceiveBuffers::new(
            limits().envelope,
            ReceiveStorage::Inproc {
                payload_backing_bytes: 1024,
            },
        )
        .unwrap();
        let (incoming, retained) = buffers.prepare(incoming).unwrap();
        let routed = self.service.receive(incoming, retained).unwrap().unwrap();
        assert_eq!(routed.placement.shard, 7);
        assert_eq!(routed.placement.group, group);
        let received = self.input.try_recv().unwrap().unwrap();
        assert_eq!(
            self.service
                .links()
                .get(received.binding.peer)
                .unwrap()
                .binding,
            received.binding
        );
        assert_eq!(
            self.routes
                .route(&received.message, received.binding)
                .unwrap(),
            received.route
        );
        assert_eq!(
            actors
                .receive_client(group, &received.message, Duration::ZERO)
                .unwrap(),
            NativeReceive::Accepted
        );
    }

    fn progress(&mut self, actors: &mut PartitionActors) {
        assert!(
            actors
                .poll_progress(
                    &mut Context::from_waker(Waker::noop()),
                    Duration::ZERO,
                    |_, message| {
                        self.service.try_reply(Class::Control, message).map_err(
                            |(error, message)| match error {
                                ReplyError::Full => TrySendError::Full(message),
                                error => panic!("frontend rejected native reply: {error:?}"),
                            },
                        )
                    }
                )
                .is_pending()
        );
        self.service
            .flush(|message| self.server.try_send(message))
            .unwrap();
    }

    async fn reply(
        &mut self,
        controller: &mut Controller,
        actors: &mut PartitionActors,
    ) -> Message {
        for _ in 0..10000 {
            self.progress(actors);
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            match self.sdk.try_recv() {
                Ok(message) => return message,
                Err(omq_tokio::Error::WouldBlock) => {}
                Err(error) => panic!("SDK receive failed: {error:?}"),
            }
            tokio::task::yield_now().await;
        }
        panic!("native frontend reply did not arrive");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_peer_frontend_dispatch_opens_writer_and_confirms_shared_shard_append() {
    tokio::time::timeout(Duration::from_secs(5), scenario())
        .await
        .unwrap();
}

async fn scenario() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let hint = actor.authority_hint();
    let group = actor.group();
    let mut front = Frontend::new(hint.primary, group).await;
    let native = intake_access_reserved(
        &mut actor,
        NativeAccess::Writers(vec![ClientAccess {
            node: front.link.binding.peer,
            producer: ProducerId::from_bytes([40; 16]),
        }]),
        partition(),
        None,
    );
    let mut actors =
        PartitionActors::new(vec![PartitionActor::Local(Box::new(actor))], 1, 1).unwrap();
    actors
        .install_native(native, front.service.links())
        .unwrap();
    assert!(!actors.native_has_work(group).unwrap());
    front
        .deliver(
            &mut actors,
            group,
            open(hint.authority, front.link, Mode::Resume, None, 42),
        )
        .await;
    assert!(actors.native_has_work(group).unwrap());
    for _ in 0..100 {
        front.progress(&mut actors);
    }
    assert!(
        matches!(front.sdk.try_recv(), Err(omq_tokio::Error::WouldBlock)),
        "frontend receipt confirmed unobserved disk work"
    );
    let reply = front.reply(&mut controller, &mut actors).await;
    for _ in 0..16 {
        front.progress(&mut actors);
    }
    assert!(!actors.native_has_work(group).unwrap());
    let opened = producer::decode_opened(packet(&reply), limits().envelope).unwrap();
    assert_eq!((opened.epoch, opened.next_sequence), (1, 0));
    front
        .deliver(
            &mut actors,
            group,
            append(hint.authority, front.link, 40, 0, 2),
        )
        .await;
    let reply = front.reply(&mut controller, &mut actors).await;
    let confirmed = append::stream::decode_confirmed(packet(&reply), limits().envelope).unwrap();
    assert_eq!(
        (
            confirmed.key.first_sequence,
            confirmed.end_sequence,
            confirmed.first_offset
        ),
        (0, 2, 0)
    );
    assert_eq!(confirmed.policy, Policy::LocalDurable);
    drive(&mut controller, actors.shutdown()).unwrap();
    front.sdk.close().await.unwrap();
    front.server.close().await.unwrap();
}
