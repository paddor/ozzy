//! Real shared frontend and native journals. Only workload and bounded retry
//! scheduling live in this fixture; all link, dispatch, and actor state is real.

use super::*;
use ozzy_broker::{Frontend, TransportLimits};
use ozzy_proto::{EnvelopeLimits, Opcode, data::DataLimits, handshake};
use ozzy_runtime::{
    dispatch::{self, Budget, Budgets, Class},
    frontend::{
        Access, Dispatcher, DispatcherLimits, Kind, Links, Pending, Placement, Port, PortError,
        ReceiveBuffers, ReceiveStorage, ReplyError, ReplyLimits, ReplyResult, RoutingTable,
        Service, WatchLimits, WatchRegistry,
    },
    replica_actor::{RoutePublicationError, RoutePublisher},
    replica_transport::QueueLimits,
};
use std::collections::VecDeque;
use tokio::sync::oneshot;

// The inproc fixture transmits only one small canonical creation per partition.
// Its native arenas are explicitly limited to 4 KiB and metadata to 64 entries.
// Include ownership wrappers and alignment. Stream dispatch separately covers
// a full OMQ read chunk behind each small payload slice.
const PAYLOAD_BACKING: usize = 32 * 1024;
const BACKING: usize = 512 * 1024;
const OUTSTANDING: usize = 64;

fn backing(class: Class) -> usize {
    match class {
        Class::Control => 128 * 1024,
        Class::Data => BACKING,
    }
}

fn budgets() -> Budgets {
    Budgets {
        data: Budget {
            queue_slots: 32,
            retained_messages: 64,
            bytes: 16 * 1024 * 1024,
        },
        control: Budget {
            queue_slots: 8,
            retained_messages: 16,
            bytes: 1024 * 1024,
        },
    }
}

fn parameters() -> handshake::Parameters {
    let mut profile =
        handshake::Parameters::streaming(DataLimits::default(), handshake::OWNER | 8).unwrap();
    profile.capabilities |= handshake::OWNER_READ | (1 << 3) | (1 << 8);
    profile.required_capabilities = 0;
    profile
}

type PortSetup = (Links, Port, oneshot::Receiver<()>);
type LaneSetup = (
    u32,
    [ozzy_runtime::frontend::DataSender; 2],
    oneshot::Sender<PortSetup>,
);

struct Network {
    local: NodeId,
    links: Links,
    port: Port,
    input: [ozzy_runtime::frontend::DataReceiver; 2],
    peers: BTreeMap<NodeId, LinkSessionId>,
    replies: FuturesUnordered<Pending<ReplyResult>>,
    retries: VecDeque<Message>,
}

impl Network {
    fn refresh(&mut self, actors: &mut PartitionActors, creations: &[Creation], now: Duration) {
        for (&peer, previous) in &mut self.peers {
            let current = self
                .links
                .get(peer)
                .map_or(LinkSessionId::from_bytes([0; 16]), |link| {
                    link.binding.session
                });
            if current == *previous {
                continue;
            }
            for creation in creations {
                if current.as_bytes() == &[0; 16] {
                    actors
                        .disconnect_session(creation.group, peer, *previous, now)
                        .unwrap();
                } else {
                    actors
                        .replace_session(creation.group, peer, *previous, current, now)
                        .unwrap();
                }
            }
            *previous = current;
        }
    }

    fn send(&mut self, message: Message) -> Result<(), TrySendError> {
        if self.replies.len() + self.retries.len() >= OUTSTANDING {
            return Err(TrySendError::Full(message));
        }
        let class = if message.len() == 2 {
            Class::Control
        } else {
            let opcode = message.part_slice(1).unwrap()[5];
            if [
                Opcode::PrepareFlow as u8,
                Opcode::Ops as u8,
                Opcode::Records as u8,
            ]
            .contains(&opcode)
            {
                Class::Data
            } else {
                Class::Control
            }
        };
        match self.port.try_reply(class, message, backing(class)) {
            Ok(pending) => {
                self.replies.push(pending);
                Ok(())
            }
            Err((
                PortError::Admission(dispatch::SendFailure::Admission(dispatch::Error::Full)),
                message,
            )) => Err(TrySendError::Full(message)),
            Err((
                PortError::Closed
                | PortError::Admission(
                    dispatch::SendFailure::Closed
                    | dispatch::SendFailure::Admission(dispatch::Error::Closed),
                ),
                _,
            )) => Err(TrySendError::Closed),
            Err(error) => panic!("actor reply submission failed: {error:?}"),
        }
    }

    fn receive(
        &mut self,
        actors: &mut PartitionActors,
        now: Duration,
        work: ozzy_runtime::frontend::DataInput,
    ) {
        let message = work.message;
        let peer = NodeId::from_bytes(message.part_slice(0).unwrap().try_into().unwrap());
        let group = if message.len() == 3 {
            ozzy_replication::wire::CompactState::decode(message.part_slice(2).unwrap()).unwrap();
            GroupId::from_bytes(message.part_slice(1).unwrap().try_into().unwrap())
        } else {
            let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
            let packet = ozzy_proto::decode_packet(&frames, EnvelopeLimits::default()).unwrap();
            if self.links.get(peer).map(|link| link.binding.session) != packet.envelope.session {
                return;
            }
            ozzy_replication::wire::route(packet, EnvelopeLimits::default())
                .unwrap()
                .group_id
        };
        actors.receive(group, &message, now).unwrap();
    }
}

struct ShardRun {
    local: NodeId,
    peers: Vec<NodeId>,
    format: bool,
    plans: Vec<PartitionJournal>,
    setup: mpsc::Sender<LaneSetup>,
    completed: mpsc::Sender<(NodeId, u32)>,
}

async fn open(
    context: &ShardContext,
    plans: Vec<PartitionJournal>,
    format: bool,
    local: NodeId,
) -> Result<(PartitionActors, Vec<Creation>, RoutePublisher), StartupError> {
    let mut opening = FuturesUnordered::new();
    for plan in plans {
        let io = context.io.clone();
        opening.push(async move {
            if format {
                Box::pin(plan.format(io, JournalGeneration(1))).await
            } else {
                Box::pin(plan.open(io, JournalGeneration(2))).await
            }
        });
    }
    let mut members = Vec::new();
    let mut creations = Vec::new();
    let mut identities = Vec::new();
    while let Some(opened) = opening.next().await {
        let opened = opened?;
        let configuration = match &opened.authority {
            PartitionAuthority::Local(_) => None,
            PartitionAuthority::Replicated(startup) => Some(startup.configuration()),
        };
        let mut started = start_actor(opened, &context.memory.data, local, &BTreeMap::new())?;
        started.prepare_partition()?;
        identities.push((started.actor.group(), started.incarnation));
        creations.push(Creation {
            group: started.actor.group(),
            members: configuration,
            proposal: started.proposal,
            buffer: Some(started.buffer),
            pending: None,
            confirmed: false,
        });
        members.push(started.actor);
    }
    let actors = PartitionActors::new(members, 64, 2).map_err(failure)?;
    let publisher = RoutePublisher::new(&actors, &identities, 2).map_err(failure)?;
    Ok((actors, creations, publisher))
}

enum ShardEvent {
    Stop,
    Input(ozzy_runtime::frontend::DataInput),
    Links,
    Tick,
    Reply(Result<ReplyResult, PortError>),
    Failed(ozzy_runtime::replica_actor::PartitionError),
    RouteFailed(RoutePublicationError),
}

#[expect(
    clippy::too_many_lines,
    reason = "fixture keeps scheduling and shutdown in one loop"
)]
async fn serve(mut context: ShardContext, run: ShardRun) -> Result<(), StartupError> {
    use ShardEvent as Event;
    let (mut actors, mut creations, mut publisher) =
        open(&context, run.plans, run.format, run.local).await?;
    let (data, data_rx) = ozzy_runtime::frontend::data_channel(
        &omq_tokio::Context::new(),
        context.plan.id,
        Kind::Broker,
        Class::Data,
        32,
        BACKING,
        budgets().data.bytes,
    )
    .unwrap();
    let (control, control_rx) = ozzy_runtime::frontend::data_channel(
        &omq_tokio::Context::new(),
        context.plan.id,
        Kind::Broker,
        Class::Control,
        8,
        128 * 1024,
        budgets().control.bytes,
    )
    .unwrap();
    let (configured, configuration) = oneshot::channel();
    run.setup
        .send((context.plan.id, [data, control], configured))
        .await
        .unwrap();
    context.ready()?;
    let (links, port, frontend_finished) = tokio::select! {
        result = configuration => result.unwrap(),
        () = context.shutdown.requested() => return actors.shutdown().await.map_err(failure),
    };
    let mut network = Network {
        local: run.local,
        links,
        port,
        input: [data_rx, control_rx],
        peers: run
            .peers
            .into_iter()
            .map(|peer| (peer, LinkSessionId::from_bytes([0; 16])))
            .collect(),
        replies: FuturesUnordered::new(),
        retries: VecDeque::new(),
    };
    let clock = std::time::Instant::now();
    let mut now = Duration::ZERO;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut reported = false;
    loop {
        let generation = network.links.generation();
        network.refresh(&mut actors, &creations, now);
        if !reported
            && creations.iter().all(|creation| {
                let state = actors.status(creation.group).unwrap();
                (!creation.leader(network.local, &state) || creation.confirmed)
                    && match state {
                        PartitionStatus::Local(status) => status.applied.op.0 == 1,
                        PartitionStatus::Replicated(status) => {
                            status.application_ready
                                && status.normal.is_some_and(|normal| normal.applied.op.0 == 1)
                        }
                    }
            })
        {
            run.completed
                .send((run.local, context.plan.id))
                .await
                .unwrap();
            reported = true;
        }
        // The fixture retries rejected outbox submissions on an explicit timer.
        // Other partitions and destinations continue while a peer remains full.
        let links = network.links.clone();
        let data_ready = network.input[0].ready();
        let control_ready = network.input[1].ready();
        let event = tokio::select! {
            () = context.shutdown.requested() => Event::Stop,
            input = data_ready => match input {
                Ok(input) => Event::Input(input),
                Err(ozzy_runtime::frontend::DataLaneError::Closed) => Event::Stop,
                Err(error) => panic!("fixture data queue failed: {error}"),
            },
            input = control_ready => match input {
                Ok(input) => Event::Input(input),
                Err(ozzy_runtime::frontend::DataLaneError::Closed) => Event::Stop,
                Err(error) => panic!("fixture control queue failed: {error}"),
            },
            () = links.changed_after(generation) => Event::Links,
            event = std::future::poll_fn(|cx| {
                if let Poll::Ready(Some(reply)) = network.replies.poll_next_unpin(cx) {
                    return Poll::Ready(Event::Reply(reply));
                }
                for creation in &mut creations { creation.poll(cx, run.local, &actors.status(creation.group).unwrap()); }
                match actors.poll_progress(cx, now, |_, message| network.send(message)) {
                    Poll::Ready(Err(error)) => Poll::Ready(Event::Failed(error)),
                    Poll::Ready(Ok(())) => panic!("actors exited before shutdown"),
                    Poll::Pending => publisher
                        .poll_progress(cx, &actors, &mut network.port)
                        .map(|result| Event::RouteFailed(result.unwrap_err())),
                }
            }) => event,
            _ = tick.tick() => Event::Tick,
        };
        // As in production, a pending turn may observe transport closure while
        // another owner requests shutdown. Drain accepted journal work below.
        if context.shutdown.is_requested() {
            break;
        }
        match event {
            Event::Stop => break,
            Event::Failed(error) => return Err(failure(error)),
            Event::RouteFailed(error) => return Err(failure(error)),
            Event::Links => {}
            Event::Input(input) => {
                network.refresh(&mut actors, &creations, now);
                network.receive(&mut actors, now, input);
            }
            Event::Reply(result) => match result.unwrap() {
                Ok(()) | Err((ReplyError::Peer | ReplyError::Session, _)) => {}
                Err((ReplyError::Full, message)) => network.retries.push_back(message),
                Err(error) => panic!("actor reply refused: {error:?}"),
            },
            Event::Tick => {
                now = clock.elapsed();
                if let Some(message) = network.retries.pop_front() {
                    match network.send(message) {
                        Ok(()) => {}
                        Err(TrySendError::Full(message)) => network.retries.push_back(message),
                        Err(TrySendError::Closed) => break,
                        Err(error) => panic!("retry failed: {error:?}"),
                    }
                }
            }
        }
    }
    // Keep ingress alive until its transport owner has stopped dispatching.
    frontend_finished.await.map_err(failure)?;
    actors.shutdown().await.map_err(failure)
}

struct FrontRun {
    peers: Vec<NodeId>,
    remote: BTreeMap<NodeId, ozzy_config::Endpoints>,
    routes: RoutingTable,
    senders: Vec<(u32, ozzy_runtime::frontend::DataSender)>,
    ports: Vec<(u32, oneshot::Sender<PortSetup>)>,
    storage: ReceiveStorage,
    watches: Vec<ozzy_proto::directory::RouteState>,
}

async fn run_frontend(
    context: ozzy_broker::FrontendContext,
    run: FrontRun,
) -> Result<(), StartupError> {
    let FrontRun {
        peers,
        remote,
        routes,
        senders,
        ports,
        storage,
        watches,
    } = run;
    let local = context.local;
    let queue = QueueLimits {
        messages: 32,
        bytes: 2 * 1024 * 1024,
        message_bytes: 256 * 1024,
    };
    let dispatcher = Dispatcher::new(
        local,
        routes,
        senders,
        DispatcherLimits {
            peers: 2,

            replies: ReplyLimits {
                control: queue,
                data: queue,
            },
        },
    )
    .unwrap();
    let access: Vec<_> = peers
        .iter()
        .map(|&peer| Access {
            peer,
            kind: Kind::Broker,
        })
        .collect();
    let mut service = Service::new(
        dispatcher,
        parameters(),
        &access,
        ozzy_runtime::frontend::LinkIds::random(),
    )
    .unwrap();
    service
        .install_watches(
            WatchRegistry::new(
                WatchLimits {
                    partitions: 64,
                    peers: 2,
                    registrations: 8,
                    interests_per_registration: 64,
                    pending_per_registration: 64,
                },
                watches,
            )
            .unwrap(),
        )
        .unwrap();
    let buffers = ReceiveBuffers::new(EnvelopeLimits::default(), storage).unwrap();
    for peer in &peers {
        context
            .data
            .connect(remote[peer].data_peer.parse().unwrap())
            .await
            .unwrap();
    }
    let mut finished = Vec::new();
    for (id, configured) in ports {
        let port = service.port(context.omq_context(), id, budgets()).unwrap();
        let (stopped, stopping) = oneshot::channel();
        finished.push(stopped);
        configured.send((service.links(), port, stopping)).unwrap();
    }
    let brokers = peers
        .into_iter()
        .map(|peer| (peer, remote[&peer].peer.parse().unwrap()))
        .collect();
    let result = context
        .serve(
            service,
            brokers,
            ozzy_broker::FollowerRoutes::default(),
            buffers,
            Duration::from_millis(25),
        )
        .await;
    for stopped in finished {
        let _ = stopped.send(());
    }
    result
}

#[expect(
    clippy::too_many_lines,
    reason = "fixture starts independent brokers and drains every owner"
)]
async fn start(brokers: &[(CheckedConfig, BrokerIdentity, JournalPlan)], format: bool, tcp: bool) {
    let context = omq_tokio::Context::new();
    let namespace = uuid::Uuid::now_v7();
    let mut listeners = BTreeMap::new();
    let all: BTreeMap<_, _> = brokers[0]
        .0
        .identity
        .brokers
        .values()
        .map(|id| {
            let peer = NodeId::from_bytes(*id.as_bytes());
            let endpoints = if tcp {
                let sockets: Vec<_> = (0..3)
                    .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
                    .collect();
                let endpoints = ozzy_config::Endpoints {
                    peer: format!("tcp://{}", sockets[0].local_addr().unwrap()),
                    data_peer: format!("tcp://{}", sockets[2].local_addr().unwrap()),
                    reader_pub: format!("tcp://{}", sockets[1].local_addr().unwrap()),
                    follower_pub: None,
                };
                listeners.insert(peer, sockets);
                endpoints
            } else {
                ozzy_config::Endpoints {
                    peer: format!("inproc://actors-{namespace}-{peer}"),
                    data_peer: format!("inproc://actors-data-{namespace}-{peer}"),
                    reader_pub: format!("inproc://actors-reader-{namespace}-{peer}"),
                    follower_pub: None,
                }
            };
            (peer, endpoints)
        })
        .collect();
    let (completed, mut reports) = mpsc::channel(16);
    let mut running = Vec::new();
    let mut frontends = Vec::new();
    let mut expected = 0;
    for (checked, identity, journals) in brokers {
        let local = NodeId::from_bytes(*identity.broker.as_bytes());
        let (setup, mut setups) = mpsc::channel(16);
        let (devices, lanes) = DevicePools::start(&checked.plan).unwrap();
        let plans = journals.partitions.clone();
        let peers: Vec<_> = all.keys().copied().filter(|peer| *peer != local).collect();
        let shards = ApplicationShards::start(&checked.plan, lanes, {
            let completed = completed.clone();
            let peers = peers.clone();
            move |context| {
                let plans = plans
                    .iter()
                    .filter(|part| part.placement.shard == context.plan.id)
                    .cloned()
                    .collect();
                serve(
                    context,
                    ShardRun {
                        local,
                        peers: peers.clone(),
                        format,
                        plans,
                        setup: setup.clone(),
                        completed: completed.clone(),
                    },
                )
            }
        })
        .await
        .unwrap();
        let mut senders = Vec::new();
        let mut ports = Vec::new();
        for _ in 0..shards.thread_count() {
            let (id, sender, configured) = setups.recv().await.unwrap();
            for lane in sender {
                senders.push((id, lane));
            }
            ports.push((id, configured));
        }
        let placements: Vec<_> = journals
            .partitions
            .iter()
            .map(|part| Placement {
                group: match &part.config {
                    JournalConfig::Local(config) => config.identity.group_id,
                    JournalConfig::Replicated(config) => config.identity.group_id,
                },
                partition: part.incarnation,
                shard: part.placement.shard,
            })
            .collect();
        let shard_ids = checked
            .plan
            .shards
            .iter()
            .map(|shard| shard.id)
            .collect::<Vec<_>>();
        let routes =
            RoutingTable::new(&shard_ids, &placements, 64, EnvelopeLimits::default()).unwrap();
        let endpoints = all[&local].clone();
        let remote = all.clone();
        let watches = checked
            .identity
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| ozzy_proto::directory::RouteState {
                group: GroupId::from_bytes(*partition.group.as_bytes()),
                config_epoch: partition.config_epoch,
                partition: ozzy_proto::PartitionIncarnation::from_bytes(
                    *partition.incarnation.as_bytes(),
                ),
                members: partition
                    .members
                    .iter()
                    .map(|id| NodeId::from_bytes(*id.as_bytes()))
                    .collect(),
                view: 0,
                leader: None,
            })
            .collect();
        listeners.remove(&local); // Release test-only ephemeral port reservations.
        let frontend = Frontend::start_with_context(
            &checked.plan,
            local,
            &endpoints,
            TransportLimits {
                send_messages: 8,
                receive_messages: 8,
                message_bytes: 256 * 1024,
                close_linger: Duration::from_millis(5),
            },
            context.clone(),
            move |context| {
                run_frontend(
                    context,
                    FrontRun {
                        peers,
                        remote,
                        routes,
                        senders,
                        ports,
                        watches,
                        storage: if tcp {
                            ReceiveStorage::Stream {
                                message_bytes: 256 * 1024,
                            }
                        } else {
                            ReceiveStorage::Inproc {
                                payload_backing_bytes: PAYLOAD_BACKING,
                            }
                        },
                    },
                )
            },
        )
        .await
        .unwrap();
        expected += shards.thread_count();
        running.push((shards, devices));
        frontends.push(frontend);
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut closed: FuturesUnordered<_> =
        running.iter().map(|(shards, _)| shards.closed()).collect();
    let mut transport: FuturesUnordered<_> = frontends.iter().map(Frontend::closed).collect();
    while seen.len() < expected {
        tokio::select! {
            Some(result) = closed.next() => panic!("shards stopped: {result:?}"),
            Some(result) = transport.next() => panic!("frontend stopped: {result:?}"),
            Some(report) = reports.recv() => { assert!(seen.insert(report)); },
        }
    }
    drop((closed, transport));
    let stopping = stop(running);
    tokio::pin!(stopping);
    // Exercise shard-first shutdown with transport owners still serving.
    assert!(futures::FutureExt::now_or_never(stopping.as_mut()).is_none());
    tokio::task::yield_now().await;
    let closing = futures::future::join_all(frontends.iter().map(Frontend::shutdown));
    let ((), results) = futures::join!(stopping, closing);
    for result in results {
        result.unwrap();
    }
}

#[tokio::test]
async fn shared_peer_frontend_drives_native_partition_actors_and_restart_elections() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let directory = tempfile::tempdir().unwrap();
        let brokers = fixture(directory.path(), DeploymentMode::Three, policy, 6);
        for (checked, local, _) in &brokers {
            storage(checked, local);
        }
        for format in [true, false] {
            tokio::time::timeout(Duration::from_secs(30), start(&brokers, format, false))
                .await
                .unwrap_or_else(|_| {
                    panic!("shared frontend actor integration stalled: {policy:?}, format={format}")
                });
        }
    }
}

#[tokio::test]
async fn tcp_shared_frontend_runs_native_shards_and_restart_elections() {
    let directory = tempfile::tempdir().unwrap();
    let brokers = fixture(
        directory.path(),
        DeploymentMode::Three,
        Confirmation::DiskQuorum,
        6,
    );
    for (checked, local, _) in &brokers {
        storage(checked, local);
    }
    for format in [true, false] {
        tokio::time::timeout(Duration::from_secs(30), start(&brokers, format, true))
            .await
            .expect("native shared frontend stalled over TCP");
    }
}

#[tokio::test]
async fn shared_peer_startup_does_not_wait_for_unavailable_third_broker() {
    let directory = tempfile::tempdir().unwrap();
    let brokers = fixture(
        directory.path(),
        DeploymentMode::Three,
        Confirmation::DiskQuorum,
        2,
    );
    for (checked, local, _) in &brokers[..2] {
        storage(checked, local);
    }
    for format in [true, false] {
        tokio::time::timeout(Duration::from_secs(30), start(&brokers[..2], format, false))
            .await
            .expect("shared frontend waited for unavailable broker");
    }
}
