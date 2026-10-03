use super::*;
use crate::{
    dispatch::{self, Budget, Budgets},
    frontend::{
        Access, Dispatcher, DispatcherLimits, Kind, LinkIds, LinkSessions, Placement, ReplyLimits,
        RoutingTable, Service, WatchLimits, WatchRegistry,
    },
    replica_actor::{PartitionActor, PartitionActors, PartitionStatus, RoutePublisher},
};
use ozzy_proto::{
    Envelope, EnvelopeLimits, GroupId, Opcode, RequestId, data::DataLimits, directory, handshake,
};
use std::num::NonZeroU64;

fn budgets() -> Budgets {
    let budget = Budget {
        queue_slots: 1,
        retained_messages: 1,
        bytes: 8192,
    };
    Budgets {
        data: budget,
        control: budget,
    }
}

fn service(routes: Vec<directory::RouteState>) -> Service {
    let local = NodeId::from_bytes([2; 16]);
    let placements: Vec<_> = routes
        .iter()
        .map(|route| Placement {
            group: route.group,
            partition: route.partition,
            shard: 7,
        })
        .collect();
    let table = RoutingTable::new(&[7], &placements, 2, EnvelopeLimits::default()).unwrap();
    let (sender, _input) = crate::frontend::data_channel(
        &omq_tokio::Context::new(),
        7,
        Kind::Client,
        dispatch::Class::Control,
        1,
        4096,
        8192,
    )
    .unwrap();
    let queue = QueueLimits {
        messages: 2,
        bytes: 8192,
        message_bytes: 4096,
    };
    let dispatcher = Dispatcher::new(
        local,
        table,
        vec![(7, sender)],
        DispatcherLimits {
            peers: 1,

            replies: ReplyLimits {
                data: queue,
                control: queue,
            },
        },
    )
    .unwrap();
    let mut service = Service::new(
        dispatcher,
        handshake::Parameters::append(DataLimits::default(), handshake::OWNER).unwrap(),
        &[Access {
            peer: NodeId::from_bytes([70; 16]),
            kind: Kind::Client,
        }],
        LinkIds::deterministic(NonZeroU64::new(1).unwrap()),
    )
    .unwrap();
    service
        .install_watches(
            WatchRegistry::new(
                WatchLimits {
                    partitions: 2,
                    peers: 1,
                    registrations: 1,
                    interests_per_registration: 2,
                    pending_per_registration: 2,
                },
                routes,
            )
            .unwrap(),
        )
        .unwrap();
    service
}

fn register(service: &mut Service, groups: Vec<GroupId>) -> Vec<Message> {
    let peer = NodeId::from_bytes([70; 16]);
    let remote = LinkSessions::with_ids(
        peer,
        handshake::Parameters::append(DataLimits::default(), handshake::PRODUCER).unwrap(),
        handshake::OWNER,
        1,
        LinkIds::deterministic(NonZeroU64::new(70).unwrap()),
    )
    .unwrap();
    service
        .receive(
            Message::with_prefix(
                bytes::Bytes::copy_from_slice(peer.as_bytes()),
                Message::multipart(remote.start(service.local()).unwrap()),
            ),
            4096,
        )
        .unwrap();
    let session = service.links().get(peer).unwrap().binding.session;
    let mut metadata = Vec::with_capacity(1024);
    let header = directory::encode_request(
        Envelope {
            opcode: Opcode::StateSnapshotRequest,
            response: false,
            request_id: Some(RequestId::from_bytes([71; 16])),
            sender: peer,
            session: Some(session),
        },
        &directory::SnapshotRequest {
            watch: RequestId::from_bytes([72; 16]),
            groups,
        },
        &mut metadata,
        EnvelopeLimits::default(),
        directory::Limits::default(),
    )
    .unwrap();
    service
        .receive(
            Message::multipart([
                bytes::Bytes::copy_from_slice(peer.as_bytes()),
                bytes::Bytes::copy_from_slice(&header),
                bytes::Bytes::from(metadata),
                bytes::Bytes::new(),
            ]),
            4096,
        )
        .unwrap();
    let mut output = Vec::new();
    for _ in 0..4 {
        service
            .flush(|message| {
                output.push(message);
                Ok(())
            })
            .unwrap();
    }
    output
}

fn step(
    shards: &mut [PartitionActors],
    routes: &mut StepRoutes,
    now: Duration,
    dead: Option<usize>,
) {
    let mut messages = Vec::new();
    for (from, shard) in shards.iter_mut().enumerate() {
        if Some(from) == dead {
            continue;
        }
        let result = shard.poll_progress(
            &mut Context::from_waker(Waker::noop()),
            now,
            |_, mut message| {
                let group = routes.group(from, &message, None);
                let to = usize::from(message.pop_front().unwrap()[0] - 1);
                if Some(to) != dead {
                    messages.push((
                        to,
                        group,
                        Message::with_prefix(bytes::Bytes::from(vec![from as u8 + 1; 16]), message),
                    ));
                }
                Ok(())
            },
        );
        assert!(result.is_pending(), "actor scheduler stopped: {result:?}");
    }
    for (to, group, message) in messages {
        shards[to].receive(group, &message, now).unwrap();
    }
}

fn close(controller: &mut Controller, actors: PartitionActors) {
    let mut closing = Box::pin(actors.shutdown());
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(closing.as_mut()) {
            result.unwrap();
            return;
        }
        settle(controller, &[]);
    }
    panic!("routing fixture did not drain");
}

#[test]
fn surviving_broker_notifies_watch_after_full_port_without_dead_leader() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        scenario(policy);
    }
}

fn actor_sets(
    controller: &mut Controller,
    io: &Local,
    policy: QuorumPolicy,
) -> (
    [PartitionActors; 3],
    Vec<(GroupId, ozzy_proto::PartitionIncarnation)>,
    StepRoutes,
) {
    let mut assigned: [Vec<_>; 3] = std::array::from_fn(|_| Vec::new());
    let mut identities = Vec::new();
    for group in 0..2 {
        let (actors, pending) = cluster(controller, io, group, policy);
        identities.push((
            actors[0].status().scope.group_id,
            ozzy_proto::PartitionIncarnation::from_bytes([91 + group; 16]),
        ));
        drop(pending);
        for (broker, actor) in actors.into_iter().enumerate() {
            assigned[broker].push(PartitionActor::Replicated(actor));
        }
    }
    let mut shards = assigned.map(|actors| PartitionActors::new(actors, 2, 1).unwrap());
    let mut routes = StepRoutes::default();
    for _ in 0..1000 {
        step(&mut shards, &mut routes, Duration::ZERO, None);
        settle(controller, &[]);
        if identities
            .iter()
            .all(|(group, _)| match shards[1].status(*group).unwrap() {
                PartitionStatus::Replicated(status) => {
                    status.application_ready && status.normal.unwrap().applied.op.0 == 1
                }
                PartitionStatus::Local(_) => false,
            })
        {
            break;
        }
    }
    (shards, identities, routes)
}

fn updates(output: &[Message]) -> Vec<directory::RouteState> {
    output
        .iter()
        .filter(|message| opcode(message) == Opcode::StateUpdate)
        .map(|message| {
            let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
            directory::decode_update(
                ozzy_proto::decode_packet(&frames, EnvelopeLimits::default()).unwrap(),
                EnvelopeLimits::default(),
                directory::Limits::default(),
            )
            .unwrap()
            .route
        })
        .collect()
}

fn scenario(policy: QuorumPolicy) {
    let (mut controller, io) = setup();
    let (mut shards, identities, mut routes) = actor_sets(&mut controller, &io, policy);
    let initial_routes = identities
        .iter()
        .map(|&(group, partition)| shards[1].route_state(group, partition).unwrap())
        .collect();
    let mut service = service(initial_routes);
    let mut output = register(
        &mut service,
        identities.iter().map(|identity| identity.0).collect(),
    );
    let mut port = service
        .port(&omq_tokio::Context::new(), 7, budgets())
        .unwrap();
    let mut publisher = RoutePublisher::new(&shards[1], &identities, 1).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    // The sole control slot stays occupied by the older first publication.
    // A second partition then waits for capacity without blocking elections.
    for _ in 0..4 {
        assert!(
            publisher
                .poll_progress(&mut cx, &shards[1], &mut port)
                .is_pending()
        );
    }
    let mut elected = None;
    for tick in 1..10000 {
        let now = Duration::from_millis(1100 + tick / 10);
        step(&mut shards, &mut routes, now, Some(0));
        settle(&mut controller, &[]);
        assert!(
            publisher
                .poll_progress(&mut cx, &shards[1], &mut port)
                .is_pending()
        );
        let route = shards[1]
            .route_state(identities[0].0, identities[0].1)
            .unwrap();
        if route.view > 0
            && route
                .leader
                .is_some_and(|leader| leader != NodeId::from_bytes([1; 16]))
        {
            elected = Some((route.view, route.leader));
            break;
        }
    }
    assert!(
        elected.is_some(),
        "full publication lane stalled the new election: {:?}",
        shards
            .iter()
            .map(|shard| shard.status(identities[0].0))
            .collect::<Vec<_>>()
    );
    for _ in 0..100 {
        assert!(
            publisher
                .poll_progress(&mut cx, &shards[1], &mut port)
                .is_pending()
        );
        service.poll_command().unwrap();
        service.poll_watch().unwrap();
        service
            .flush(|message| {
                output.push(message);
                Ok(())
            })
            .unwrap();
    }
    let updates = updates(&output);
    assert!(
        updates
            .iter()
            .any(|route| route.group == identities[0].0
                && Some((route.view, route.leader)) == elected)
    );
    assert!(
        updates.len() <= 2,
        "intermediate election views were not coalesced"
    );
    for shard in shards {
        close(&mut controller, shard);
    }
}
