//! Native actor/shard composition over a bounded, lossy test network adapter.
//! Shared OMQ frontend qualification remains a separate integration layer.

use super::*;
use futures::{StreamExt, stream::FuturesUnordered};
use omq_tokio::{Message, TrySendError};
use ozzy_broker::{ApplicationShards, DevicePools, PartitionJournal, ShardContext, StartupError};
use ozzy_proto::{GroupId, LinkSessionId, NodeId};
use ozzy_runtime::replica_actor::{
    ActorIds, PartitionActors, PartitionStatus, PendingProposal, ProposalOutcome,
    ProposalSubmitError, ProposalSubmitter,
};
use ozzy_runtime::replica_journal::ProposalBuffer;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::mpsc;

mod frontend;

type Packet = (GroupId, Message);
type Wire = (NodeId, Message);
type Channels = BTreeMap<(NodeId, NodeId, u32), GroupId>;

fn route_wire(
    destination: NodeId,
    from: NodeId,
    mut message: Message,
    channels: &mut Channels,
) -> Packet {
    let group = if message.len() == 1 {
        let compact =
            ozzy_replication::wire::CompactState::decode(message.part_slice(0).unwrap()).unwrap();
        let group = channels[&(destination, from, compact.handle)];
        message = Message::multipart([
            bytes::Bytes::copy_from_slice(group.as_bytes()),
            message.pop_front().unwrap(),
        ]);
        group
    } else {
        let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i).unwrap());
        let limits = ozzy_proto::EnvelopeLimits::default();
        let packet = ozzy_proto::decode_packet(&frames, limits).unwrap();
        let group = ozzy_replication::wire::route(packet, limits)
            .unwrap()
            .group_id;
        if packet.envelope.opcode == ozzy_proto::Opcode::ReplicaState {
            let state = ozzy_replication::wire::flow_state_route(packet, limits).unwrap();
            channels.insert((destination, from, state.handle), group);
        }
        group
    };
    (
        group,
        Message::with_prefix(bytes::Bytes::copy_from_slice(from.as_bytes()), message),
    )
}

struct Creation {
    group: GroupId,
    members: Option<ozzy_replication::Configuration>,
    proposal: ProposalSubmitter,
    buffer: Option<ProposalBuffer>,
    pending: Option<Pin<Box<PendingProposal>>>,
    confirmed: bool,
}

impl Creation {
    fn leader(&self, from: NodeId, status: &PartitionStatus) -> bool {
        match status {
            PartitionStatus::Local(_) => true,
            PartitionStatus::Replicated(status) => {
                self.members.unwrap().primary(status.scope.view) == from
            }
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>, from: NodeId, status: &PartitionStatus) {
        if let Some(pending) = &mut self.pending {
            let Poll::Ready(reply) = pending.as_mut().poll(cx) else {
                return;
            };
            let reply = reply.unwrap();
            match reply.outcome {
                ProposalOutcome::Committed { through, .. } => {
                    assert_eq!(through.op.0, 1, "startup duplicated partition creation");
                    self.confirmed = true;
                }
                ProposalOutcome::NotAdmitted | ProposalOutcome::Unknown => {}
                ProposalOutcome::Invalid(error) => panic!("partition creation rejected: {error}"),
            }
            self.buffer = Some(reply.buffer);
            self.pending = None;
        }
        let ready = match status {
            PartitionStatus::Local(_) => true,
            PartitionStatus::Replicated(status) => status.application_ready,
        };
        if self.confirmed || !ready || !self.leader(from, status) {
            return;
        }
        match self.proposal.try_submit(self.buffer.take().unwrap()) {
            Ok(pending) => {
                self.pending = Some(Box::pin(pending));
                cx.waker().wake_by_ref();
            }
            Err(rejected) => {
                assert_eq!(rejected.reason, ProposalSubmitError::Full);
                self.buffer = Some(rejected.buffer);
            }
        }
    }
}

struct Run {
    from: NodeId,
    format: bool,
    plans: Vec<PartitionJournal>,
    sessions: BTreeMap<NodeId, LinkSessionId>,
    incoming: mpsc::Receiver<Packet>,
    outgoing: mpsc::Sender<Wire>,
    completed: mpsc::Sender<(NodeId, u32)>,
}

fn failure(reason: impl std::fmt::Display) -> StartupError {
    StartupError::Runtime(reason.to_string())
}

fn start_actor(
    opened: ozzy_broker::OpenedPartition,
    memory: &ozzy_runtime::memory::Owner,
    from: NodeId,
    sessions: &BTreeMap<NodeId, LinkSessionId>,
) -> Result<ozzy_broker::StartedPartition, StartupError> {
    let mut started = opened.into_actor(memory, &BTreeMap::new(), ActorIds::random(), || 123)?;
    if let ozzy_runtime::replica_actor::PartitionActor::Replicated(actor) = &mut started.actor {
        for (&peer, &session) in sessions {
            if peer != from {
                actor
                    .replace_session(
                        peer,
                        LinkSessionId::from_bytes([0; 16]),
                        session,
                        Duration::ZERO,
                    )
                    .map_err(failure)?;
            }
        }
    }
    Ok(started)
}

async fn serve(mut context: ShardContext, mut run: Run) -> Result<(), StartupError> {
    let mut opening = FuturesUnordered::new();
    for plan in run.plans {
        let io = context.io.clone();
        let format = run.format;
        opening.push(async move {
            if format {
                Box::pin(plan.format(io, JournalGeneration(1))).await
            } else {
                Box::pin(plan.open(io, JournalGeneration(2))).await
            }
        });
    }
    let mut actors = Vec::new();
    let mut creations = Vec::new();
    while let Some(opened) = opening.next().await {
        let opened = opened?;
        let members = match &opened.authority {
            PartitionAuthority::Local(_) => None,
            PartitionAuthority::Replicated(startup) => {
                assert_eq!(startup.recovered().is_none(), run.format);
                Some(startup.configuration())
            }
        };
        let mut started = start_actor(opened, &context.memory.data, run.from, &run.sessions)?;
        let group = started.actor.group();
        if !run.format
            && let PartitionStatus::Replicated(status) = started.actor.status()
        {
            assert!(!status.application_ready, "restart bypassed election");
        }
        started.prepare_partition()?;
        creations.push(Creation {
            group,
            members,
            proposal: started.proposal,
            buffer: Some(started.buffer),
            pending: None,
            confirmed: false,
        });
        actors.push(started.actor);
    }
    let mut actors = PartitionActors::new(actors, 64, 2).map_err(failure)?;
    // This fixture submits one small control operation per partition. Reserve
    // a conservative fixed share for its incoming packet and canonical copies;
    // do not advertise every partition's full local pipeline simultaneously.
    assert!(creations.len() * 4 <= context.plan.budget.append_slots);
    assert!(creations.len() as u64 * 4096 <= context.plan.budget.resident_bytes);
    context.ready()?;
    let clock = std::time::Instant::now();
    let mut now = Duration::ZERO;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut reported = false;
    loop {
        if !reported
            && creations.iter().all(|creation| {
                let status = actors.status(creation.group).unwrap();
                if creation.leader(run.from, &status) && !creation.confirmed {
                    return false;
                }
                match status {
                    PartitionStatus::Local(status) => status.applied.op.0 == 1,
                    PartitionStatus::Replicated(status) => {
                        status.application_ready
                            && status.normal.is_some_and(|normal| normal.applied.op.0 == 1)
                    }
                }
            })
        {
            run.completed
                .send((run.from, context.plan.id))
                .await
                .unwrap();
            reported = true;
        }
        tokio::select! {
            biased;
            () = context.shutdown.requested() => break,
            Some((group, message)) = run.incoming.recv() => actors.receive(group, &message, now).map_err(failure)?,
            result = std::future::poll_fn(|cx| {
                for creation in &mut creations {
                    creation.poll(cx, run.from, &actors.status(creation.group).unwrap());
                }
                actors.poll_progress(cx, now, |_, message| {
                match run.outgoing.try_send((run.from, message)) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full((_, message))) => Err(TrySendError::Full(message)),
                    Err(mpsc::error::TrySendError::Closed(_)) => Err(TrySendError::Error(omq_tokio::Error::Closed)),
                }
                })
            }) => return result.map_err(failure),
            _ = tick.tick() => now = clock.elapsed(),
        }
    }
    actors.shutdown().await.map_err(failure)?;
    drop(creations);
    Ok(())
}

async fn start(brokers: &[(CheckedConfig, BrokerIdentity, JournalPlan)], format: bool) {
    let mut routes = BTreeMap::new();
    let mut channels = Channels::new();
    let (outgoing, mut network) = mpsc::channel::<Wire>(256);
    let (completed, mut reports) = mpsc::channel(16);
    let sessions: BTreeMap<_, _> = brokers
        .iter()
        .map(|(_, id, _)| {
            (
                NodeId::from_bytes(*id.broker.as_bytes()),
                LinkSessionId::from_bytes([61; 16]),
            )
        })
        .collect();
    let mut running = Vec::new();
    let mut expected = 0;
    for (checked, id, journals) in brokers {
        let from = NodeId::from_bytes(*id.broker.as_bytes());
        let mut incoming = BTreeMap::new();
        for shard in &checked.plan.shards {
            let (sender, receiver) = mpsc::channel(64);
            incoming.insert(shard.id, Some(receiver));
            for part in journals
                .partitions
                .iter()
                .filter(|part| part.placement.shard == shard.id)
            {
                let group = match &part.config {
                    JournalConfig::Local(config) => config.identity.group_id,
                    JournalConfig::Replicated(config) => config.identity.group_id,
                };
                routes.insert((from, group), sender.clone());
            }
        }
        let incoming = Arc::new(Mutex::new(incoming));
        let (devices, lanes) = DevicePools::start(&checked.plan).unwrap();
        let shards = ApplicationShards::start(&checked.plan, lanes, {
            let outgoing = outgoing.clone();
            let completed = completed.clone();
            let sessions = sessions.clone();
            let plans = journals.partitions.clone();
            move |context| {
                let incoming = incoming
                    .lock()
                    .unwrap()
                    .get_mut(&context.plan.id)
                    .unwrap()
                    .take()
                    .unwrap();
                let plans = plans
                    .iter()
                    .filter(|part| part.placement.shard == context.plan.id)
                    .cloned()
                    .collect();
                serve(
                    context,
                    Run {
                        from,
                        format,
                        plans,
                        sessions: sessions.clone(),
                        incoming,
                        outgoing: outgoing.clone(),
                        completed: completed.clone(),
                    },
                )
            }
        })
        .await
        .unwrap();
        assert_eq!(shards.thread_count(), checked.plan.shards.len());
        expected += shards.thread_count();
        running.push((shards, devices));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut closed: FuturesUnordered<_> =
        running.iter().map(|(shards, _)| shards.closed()).collect();
    while seen.len() < expected {
        tokio::select! {
            Some(result) = closed.next() => panic!("application shards exited before convergence: {result:?}"),
            Some(report) = reports.recv() => { assert!(seen.insert(report)); }
            Some((from, mut message)) = network.recv() => {
                let destination = NodeId::from_bytes(message.pop_front().unwrap().as_ref().try_into().unwrap());
                let packet = route_wire(destination, from, message, &mut channels);
                match routes[&(destination, packet.0)].try_send(packet) {
                    Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}, // Bounded test packet loss.
                    Err(mpsc::error::TrySendError::Closed(_)) => panic!("application shard stopped"),
                }
            }
        }
    }
    drop(closed);
    stop(running).await;
}

async fn stop(running: Vec<(ApplicationShards, DevicePools)>) {
    let mut stopping = FuturesUnordered::new();
    for (shards, devices) in running {
        stopping.push(async move {
            shards.shutdown().await.unwrap();
            devices.shutdown().await;
        });
    }
    while stopping.next().await.is_some() {}
}

#[tokio::test]
async fn configured_native_actors_share_symmetric_shards_and_recover_with_required_elections() {
    for (mode, policy) in [
        (DeploymentMode::Single, Confirmation::LocalDurable),
        (DeploymentMode::Three, Confirmation::DiskQuorum),
        (DeploymentMode::Three, Confirmation::ReplicatedPersisting),
    ] {
        let temporary = tempfile::tempdir().unwrap();
        let brokers = fixture(temporary.path(), mode, policy, 6);
        for (checked, local, _) in &brokers {
            storage(checked, local);
        }
        for format in [true, false] {
            tokio::time::timeout(Duration::from_secs(20), start(&brokers, format))
                .await
                .unwrap_or_else(|_| panic!("native actor shard integration stalled: {mode:?}, {policy:?}, format={format}"));
        }
    }
}

#[tokio::test]
async fn configured_startup_and_restart_confirm_with_one_broker_unavailable() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let temporary = tempfile::tempdir().unwrap();
        let brokers = fixture(temporary.path(), DeploymentMode::Three, policy, 2);
        // Persistent membership still has three brokers. Both initial partition
        // leaders are on this available pair; restart must elect again normally.
        for (checked, local, _) in &brokers[..2] {
            storage(checked, local);
        }
        for format in [true, false] {
            tokio::time::timeout(Duration::from_secs(20), start(&brokers[..2], format))
                .await
                .expect("available quorum failed to start without the third link");
        }
    }
}
