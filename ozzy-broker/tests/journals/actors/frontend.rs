//! Real shared frontend and native journals. Only workload and bounded retry
//! scheduling live in this fixture; all link, dispatch, and actor state is real.

use super::*;
use ozzy_broker::{Frontend, TransportLimits};
use ozzy_proto::{EnvelopeLimits, Opcode, data::DataLimits, handshake};
use ozzy_runtime::{
    dispatch::{self, Budget, Budgets, Class, Client, Grant, GrantKey, Quota},
    frontend::{
        Access, Dispatcher, DispatcherLimits, GrantSpec, GrantTarget, Kind, Links, Pending,
        Placement, Port, PortError, ReceiveBuffers, ReceiveStorage, ReplyError, ReplyLimits,
        ReplyResult, RoutingTable, Service, Subject, WatchLimits, WatchRegistry,
    },
    replica_actor::{RoutePublicationError, RoutePublisher},
    replica_transport::QueueLimits,
};
use std::collections::{BTreeSet, VecDeque};
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

fn data_target(group: GroupId) -> GrantTarget {
    Subject {
        group,
        writer: None,
    }
    .into()
}

fn grant_spec(reservation: &Reserved, actors: &PartitionActors) -> Option<GrantSpec> {
    match reservation.target {
        GrantTarget::Control(_) => Some(GrantSpec::Target(reservation.target)),
        GrantTarget::Partition(subject) => actors
            .ops_receive_demand(subject.group)
            .filter(|request| request.source.voter == reservation.peer)
            .map(|request| {
                GrantSpec::replica(ozzy_replication::wire::ReceiveFence::History(request))
            })
            .or_else(|| {
                actors
                    .receive_credit(subject.group)
                    .and_then(|(peer, report)| {
                        (peer == reservation.peer).then(|| {
                            GrantSpec::replica(ozzy_replication::wire::ReceiveFence::Normal(
                                report.channel,
                            ))
                        })
                    })
            }),
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
    let mut profile = handshake::Parameters::streaming(
        DataLimits::default(),
        handshake::OWNER | 8,
        65536,
        1 << 30,
    )
    .unwrap();
    profile.capabilities |= handshake::OWNER_READ | (1 << 3) | (1 << 8);
    profile.required_capabilities = 0;
    profile
}

type PortSetup = (Links, Port);
type LaneSetup = (u32, dispatch::Sender<Message>, oneshot::Sender<PortSetup>);

struct Reserved {
    peer: NodeId,
    session: LinkSessionId,
    target: GrantTarget,
    class: Class,
    key: GrantKey,
    pending: Option<Pending<ozzy_runtime::frontend::InstallResult>>,
    unsent: Option<Grant>,
    installed: bool,
}

struct Network {
    local: NodeId,
    shard: u32,
    links: Links,
    port: Port,
    input: dispatch::Receiver<Message>,
    peers: BTreeMap<NodeId, (LinkSessionId, Option<Client>)>,
    grants: Vec<Reserved>,
    replies: FuturesUnordered<Pending<ReplyResult>>,
    retries: VecDeque<Message>,
    revoked_windows: BTreeSet<GroupId>,
}

impl Network {
    fn refresh(&mut self, actors: &mut PartitionActors, creations: &[Creation], now: Duration) {
        for (&peer, (previous, client)) in &mut self.peers {
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
            self.grants.retain(|grant| grant.peer != peer);
            if current.as_bytes() == &[0; 16] {
                // Dropping the local client fences every old token while admitted
                // aliases continue to count against the destination owner.
                *client = None;
            } else {
                if let Some(client) = client {
                    client.replace_session(current).unwrap();
                } else {
                    *client = Some(self.input.credits().client(current, budgets()).unwrap());
                }
                let targets = std::iter::once((GrantTarget::Control(self.shard), Class::Control))
                    .chain(
                        creations
                            .iter()
                            .map(|creation| (data_target(creation.group), Class::Data)),
                    );
                for (target, class) in targets {
                    let grant = self
                        .input
                        .credits()
                        .grant(
                            client.as_ref().unwrap(),
                            class,
                            Quota {
                                messages: 2,
                                bytes: backing(class) * 2,
                            },
                        )
                        .unwrap();
                    self.grants.push(Reserved {
                        peer,
                        session: current,
                        target,
                        class,
                        key: grant.key(),
                        pending: None,
                        unsent: Some(grant),
                        installed: false,
                    });
                }
            }
            *previous = current;
        }
    }

    fn poll_grants(
        &mut self,
        cx: &mut Context<'_>,
        actors: &mut PartitionActors,
        creations: &[Creation],
    ) {
        for reservation in &mut self.grants {
            if let Some(pending) = &mut reservation.pending
                && let Poll::Ready(result) = Pin::new(pending).poll(cx)
            {
                match result.unwrap() {
                    Ok(()) => reservation.installed = true,
                    Err((ozzy_runtime::frontend::SetupError::Binding, grant)) => {
                        // Dispatcher session installation can trail the link
                        // observer. Retry the same reserved grant.
                        reservation.unsent = Some(grant);
                    }
                    Err(error) => panic!("dispatch grant installation failed: {error:?}"),
                }
                reservation.pending = None;
            }
            let spec = grant_spec(reservation, actors);
            if let Some(spec) = spec
                && let Some(grant) = reservation.unsent.take()
            {
                match self.port.try_install(reservation.peer, spec, grant) {
                    Ok(pending) => {
                        reservation.pending = Some(pending);
                        cx.waker().wake_by_ref();
                    }
                    Err((
                        PortError::Admission(dispatch::SendFailure::Admission(
                            dispatch::Error::Full,
                        )),
                        grant,
                    )) => reservation.unsent = Some(grant),
                    Err(error) => panic!("grant port failed: {error:?}"),
                }
            }
            if reservation.installed {
                let unused = self.input.credits().remaining(&reservation.key).unwrap();
                let quota = Quota {
                    messages: 2 - unused.messages,
                    bytes: backing(reservation.class) * 2 - unused.bytes,
                };
                if quota != Quota::default() {
                    match self.input.credits().extend(&reservation.key, quota) {
                        Ok(()) | Err(dispatch::Error::Full) => {}
                        Err(error) => panic!("grant refill failed: {error:?}"),
                    }
                }
            }
        }
        for creation in creations {
            if let Some((peer, report)) = actors.receive_credit(creation.group)
                && report.operation_limit > 0
                && self.revoked_windows.insert(creation.group)
            {
                // Reallocate a granted window once. Old wire frames and port
                // commands may still be in flight; both admission fences must
                // move before the same unused resources can be granted again.
                actors.revoke_receive(report.channel).unwrap();
                let reservation = self
                    .grants
                    .iter_mut()
                    .find(|grant| {
                        grant.peer == peer
                            && grant.target == data_target(creation.group)
                            && grant.class == Class::Data
                    })
                    .unwrap();
                self.input.credits().revoke_key(&reservation.key).unwrap();
                let client = self.peers[&peer].1.as_ref().unwrap();
                let grant = self
                    .input
                    .credits()
                    .grant(
                        client,
                        Class::Data,
                        Quota {
                            messages: 2,
                            bytes: BACKING * 2,
                        },
                    )
                    .unwrap();
                reservation.key = grant.key();
                reservation.installed = false;
                reservation.pending = None;
                reservation.unsent = Some(grant);
            }
            if let Some((peer, report)) = actors.receive_credit(creation.group)
                && report.operation_limit == 0
                && actors.receive_target(creation.group).is_some()
                && self.grants.iter().any(|grant| {
                    grant.peer == peer
                        && grant.target == data_target(creation.group)
                        && grant.class == Class::Data
                        && grant.installed
                        && self
                            .links
                            .get(peer)
                            .is_some_and(|link| link.binding.session == grant.session)
                })
            {
                // The fixture's one operation has both native memory and an
                // installed dispatcher token before its wire credit is visible.
                actors.grant_receive(report.channel, 1, 512).unwrap();
            }
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
            Err(error) => panic!("actor reply submission failed: {error:?}"),
        }
    }

    fn receive(&mut self, actors: &mut PartitionActors, now: Duration) {
        let Some(work) = self.input.try_recv().unwrap() else {
            return;
        };
        let message = work.into_retained_message();
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
                plan.format(io, JournalGeneration(1)).await
            } else {
                plan.open(io, JournalGeneration(2)).await
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
    Input,
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
    let (sender, input) = dispatch::channel(dispatch::Limits {
        capacity: budgets(),
        clients: 2,
        grants: 32,
    })
    .unwrap();
    let (configured, configuration) = oneshot::channel();
    run.setup
        .send((context.plan.id, sender, configured))
        .await
        .unwrap();
    context.ready()?;
    let (links, port) = tokio::select! {
        result = configuration => result.unwrap(),
        () = context.shutdown.requested() => return actors.shutdown().await.map_err(failure),
    };
    let mut network = Network {
        local: run.local,
        shard: context.plan.id,
        links,
        port,
        input,
        peers: run
            .peers
            .into_iter()
            .map(|peer| (peer, (LinkSessionId::from_bytes([0; 16]), None)))
            .collect(),
        grants: Vec::new(),
        replies: FuturesUnordered::new(),
        retries: VecDeque::new(),
        revoked_windows: BTreeSet::new(),
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
                    && (!run.format
                        || creation.leader(network.local, &state)
                        || network.revoked_windows.contains(&creation.group))
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
        let input_ready = network.input.ready();
        let event = tokio::select! {
            () = context.shutdown.requested() => Event::Stop,
            () = input_ready => Event::Input,
            () = links.changed_after(generation) => Event::Links,
            event = std::future::poll_fn(|cx| {
                if let Poll::Ready(Some(reply)) = network.replies.poll_next_unpin(cx) {
                    return Poll::Ready(Event::Reply(reply));
                }
                network.poll_grants(cx, &mut actors, &creations);
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
        match event {
            Event::Stop => break,
            Event::Failed(error) => return Err(failure(error)),
            Event::RouteFailed(error) => return Err(failure(error)),
            Event::Links => {}
            Event::Input => {
                network.refresh(&mut actors, &creations, now);
                network.receive(&mut actors, now);
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
                        Err(error) => panic!("retry failed: {error:?}"),
                    }
                }
            }
        }
    }
    actors.shutdown().await.map_err(failure)
}

struct FrontRun {
    peers: Vec<NodeId>,
    remote: BTreeMap<NodeId, ozzy_config::Endpoints>,
    routes: RoutingTable,
    senders: Vec<(u32, dispatch::Sender<Message>)>,
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
            grants_per_class: 64,
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
    for (id, configured) in ports {
        let port = service.port(id, budgets()).unwrap();
        configured.send((service.links(), port)).unwrap();
    }
    let brokers = peers
        .into_iter()
        .map(|peer| (peer, remote[&peer].peer.parse().unwrap()))
        .collect();
    context
        .serve(service, brokers, buffers, Duration::from_millis(25))
        .await
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
                let sockets: Vec<_> = (0..2)
                    .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
                    .collect();
                let endpoints = ozzy_config::Endpoints {
                    peer: format!("tcp://{}", sockets[0].local_addr().unwrap()),
                    reader_pub: format!("tcp://{}", sockets[1].local_addr().unwrap()),
                    follower_pub: None,
                };
                listeners.insert(peer, sockets);
                endpoints
            } else {
                ozzy_config::Endpoints {
                    peer: format!("inproc://actors-{namespace}-{peer}"),
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
            senders.push((id, sender));
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
    stop(running).await;
    for frontend in frontends {
        frontend.shutdown().await.unwrap();
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
