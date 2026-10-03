use super::*;
use crate::{
    dispatch::{self, Budget, Budgets, Class, Client, Quota},
    frontend::{
        Access, Destination, Dispatcher, DispatcherLimits, GrantRequest, GrantTarget, LinkIds,
        LinkSessions, Placement, Port, ReceiveBuffers, ReceiveStorage, ReplyError, ReplyLimits,
        Routed, RoutingTable, Service, ShardIntake, Subject,
    },
    replica_actor::{PartitionActor, PartitionActors},
    replica_transport::QueueLimits,
};
use omq_tokio::{Context as OmqContext, Options, Socket, SocketType};
use std::{num::NonZeroU64, time::Duration};

mod shared;

struct Frontend {
    service: Service,
    input: ShardIntake,
    destinations: [Destination; 2],
    routes: RoutingTable,
    port: Port,
    client: Client,
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

fn service(local: NodeId, group: GroupId) -> (Service, ShardIntake, [Destination; 2]) {
    let domain = crate::memory::Domain::new(None, 256 * 1024).unwrap();
    let memory_limits = crate::memory::Limits {
        bytes: 128 * 1024,
        buffers: 32,
        cache_bytes: 128 * 1024,
    };
    let data = domain.owner(memory_limits).unwrap();
    let control = domain.owner(memory_limits).unwrap();
    let (sender, mut receiver) = ShardIntake::new(
        data,
        &control,
        dispatch::Limits {
            capacity: budgets(),
            clients: 2,
            grants: 3,
        },
        2,
        1,
    )
    .unwrap();
    let canonical = crate::replicated::ClientConfig {
        peers: Vec::new(),
        limits: limits(),
    }
    .prepared_canonical_body_bytes()
    .unwrap();
    let destinations = [
        receiver
            .destination(
                group,
                Kind::Client,
                Class::Control,
                crate::memory::Quota {
                    bytes: 65,
                    buffers: 1,
                },
            )
            .unwrap(),
        receiver
            .destination(
                group,
                Kind::Client,
                Class::Data,
                crate::memory::Quota {
                    bytes: canonical * 2,
                    buffers: 2,
                },
            )
            .unwrap(),
    ];
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
        vec![(7, sender)],
        DispatcherLimits {
            peers: 2,
            grants_per_class: 2,
            replies: ReplyLimits {
                data: queue,
                control: queue,
            },
        },
    )
    .unwrap();
    let parameters = handshake::Parameters::streaming(limits(), handshake::OWNER, 4, 4096).unwrap();
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
    (service, receiver, destinations)
}

impl Frontend {
    async fn new(local: NodeId, group: GroupId) -> Self {
        let (mut service, mut input, destinations) = service(local, group);
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
        let client = input.client(link.binding.session, budgets()).unwrap();
        let port = service.port(7, budgets()).unwrap();
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
            destinations,
            routes,
            port,
            client,
            server,
            sdk,
            link,
            local,
            _context: context,
        }
    }

    async fn deliver(&mut self, actors: &mut PartitionActors, group: GroupId, message: Message) {
        let class = if packet(&message).envelope.opcode == Opcode::Append {
            Class::Data
        } else {
            Class::Control
        };
        let request = GrantRequest {
            binding: self.link.binding,
            route: Routed {
                placement: Placement {
                    group,
                    partition: partition(),
                    shard: 7,
                },
                class,
                writer: (class == Class::Data).then_some(ProducerId::from_bytes([40; 16])),
            },
        };
        self.input
            .install(
                &mut self.port,
                &self.service.links(),
                request,
                None,
                &self.client,
                8192,
            )
            .unwrap();
        assert!(self.service.poll_command().unwrap());
        let mut installed = false;
        self.input
            .poll_installations(
                &mut Context::from_waker(Waker::noop()),
                &self.service.links(),
                0,
                6,
                |_, _| installed = true,
            )
            .unwrap();
        assert!(installed);
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
        let received = self
            .input
            .receive(&self.service.links(), &self.routes)
            .unwrap()
            .unwrap();
        assert!(received.current);
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
        if !actors
            .native_has_work(self.destinations[0].group())
            .unwrap()
        {
            for destination in &self.destinations {
                self.input.settle(destination, |_| Ok(())).unwrap();
            }
        }
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
        Some((
            front.destinations[0].capacity(),
            front.destinations[1].capacity(),
        )),
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
