//! One simulated broker: a production `PartitionActors` shard hosting one
//! or two replicated groups, each over its own production shard journal on
//! the broker's one device. A turn mirrors the broker's shard turn (`ozzy-broker/src/serving/shard/
//! scheduler.rs`): deliver input first, then visit partitions until the
//! shard waker stays quiet.

use crate::{
    check::leads,
    clock::Clock,
    consumer,
    disk::{Disk, Flag},
    network::{BROKERS, Inbox, Network, node, opcode},
    records,
    seeds::Trace,
};
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_io::Local;
use ozzy_journal::operation::{CreatePartition, RetentionPolicy};
use ozzy_journal_segment::{
    BodyEncoding, CanonicalRecoveryLimits, CheckpointLimits, DecodeLimits, GroupIdentity,
    IndexBuildLimits, JournalLimits, MetadataLimits, PartitionReadLimits, SegmentOptions,
    SegmentWriteMode,
};
use ozzy_proto::{
    EnvelopeLimits, GroupId, LinkSessionId, Opcode, OwnerEpoch, PartitionId, PartitionIncarnation,
    StoreId, VolumeId, append::Policy,
};
use ozzy_replication::{
    ConfigurationRecord, ConfiguredVoter, Digest, JournalGeneration, PipelineLimits,
    driver::Timing,
    flow::{Channel, ProbeTiming},
    wire::{self, CompactState, WireLimits, flow_state_route},
};
use ozzy_runtime::{
    frontend::{Links, ReaderGrants},
    replica_actor::{
        ActorConfig, ActorIds, ActorStatus, NativeAccess, NativeIntake, NativeIntakeConfig,
        NativeReceive, PartitionActor, PartitionActors, PartitionStatus, PendingProposal,
        ProposalOutcome, ProposalSubmitter, ReceiptHandles, ReplicaActor, ScheduledReplica,
        SessionReplacement, SharedReaderConfig, SyncBatchTarget,
    },
    replica_journal::{
        InstallationConfig, JournalStartup, OwnedConfig, OwnedJournal, ProposalBuffer,
        ShardJournalConfig,
    },
    replica_transport::QueueLimits,
};
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

/// The groups a broker can host in its one shard, and their partitions; a
/// run hosts the first `Shape::partitions`. Barrier clients use the first,
/// and scripted faults target its leader.
pub(crate) const GROUPS: [GroupId; 2] =
    [GroupId::from_bytes([7; 16]), GroupId::from_bytes([6; 16])];
pub(crate) const PARTITIONS: [PartitionIncarnation; 2] = [
    PartitionIncarnation::from_bytes([8; 16]),
    PartitionIncarnation::from_bytes([5; 16]),
];
pub(crate) const PIPELINE: PipelineLimits = PipelineLimits {
    max_operations: 8,
    max_body_bytes: 8192,
};
/// Default journal segment size.
pub(crate) const SEGMENT_BYTES: u64 = 256 * 1024;
/// The broker's journal segment limit.
pub(crate) const MAX_SEGMENTS: usize = 256;

/// A broker's shard: the partitions it hosts, and its journals' segment
/// limit and size, and whether they write one operation per physical group.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Shape {
    pub partitions: usize,
    pub max_segments: usize,
    pub segment_bytes: u64,
    pub sparse: bool,
}
/// Partition visits per shard poll and polls per broker per tick.
const TURN_ACTORS: usize = 16;
const POLL_BUDGET: usize = 1000;
/// As `ozzy-broker/src/serving/shard.rs` `SAFETY_SWEEP`.
const SAFETY_SWEEP: Duration = Duration::from_millis(100);
const LIMITS: EnvelopeLimits = EnvelopeLimits {
    max_metadata_bytes: 64 * 1024,
    max_payload_bytes: 1024 * 1024,
};

/// The lane that creates the partition once its broker leads, as the
/// broker's bootstrap does at every startup
/// (`ozzy-broker/src/serving/shard/scheduler.rs` `poll_bootstrap`).
#[derive(Debug)]
struct Bootstrap {
    submitter: ProposalSubmitter,
    buffer: Option<ProposalBuffer>,
    pending: Option<Pin<Box<PendingProposal>>>,
    done: bool,
}

/// One proposal lane of a partition actor with its leased arena.
#[derive(Debug)]
pub(crate) struct Lane {
    pub submitter: ProposalSubmitter,
    pub buffer: Option<ProposalBuffer>,
}

#[derive(Debug)]
pub(crate) struct Broker {
    pub index: usize,
    /// Restarts so far.
    pub incarnation: u64,
    pub shard: PartitionActors,
    pub disk: Disk,
    pub inbox: Inbox,
    woken: Arc<Flag>,
    /// Latest receipt alias per group of each sending broker, as the
    /// receiving frontend binds it (`ozzy-runtime/src/frontend/flow.rs`).
    aliases: [BTreeMap<GroupId, (u32, Channel)>; BROKERS],
    /// Compact receipts without a bound alias and refused alias bindings.
    pub unbound: u64,
    /// One per group.
    bootstrap: Vec<Bootstrap>,
    /// Client links as the dispatcher publishes them.
    links: Links,
    /// Client requests not yet taken, with their partition.
    pub requests: VecDeque<(usize, Message)>,
    /// Replies to clients, in send order.
    pub replies: VecDeque<Message>,
    /// Live reader publications for the reader PUB, in publish order.
    pub publications: VecDeque<Message>,
    /// Readers the live topic admits, as the reader PUB checks them.
    pub grants: ReaderGrants,
    /// Client connection resets so far; each starts fresh client sessions.
    client_resets: u64,
}

impl Broker {
    /// Format, open, and schedule the broker's partitions. Returns one lane
    /// per barrier client, on the first group.
    pub(crate) fn start(
        index: usize,
        clock: &Clock,
        (mut disk, io): (Disk, Local),
        clients: usize,
        shape: Shape,
    ) -> Result<(Self, Vec<Lane>), String> {
        let mut journals = Vec::new();
        for group in 0..shape.partitions {
            journals.push(
                disk.drive(OwnedJournal::format_new(
                    journal_config(index, shape, group),
                    io.clone(),
                    generation(index, 0, group),
                    shape.segment_bytes,
                ))
                .map_err(|error| format!("broker {index}: format group {group}: {error}"))?,
            );
        }
        let sessions = std::array::from_fn(|peer| session(index, peer, 0));
        Self::serve(index, 0, clock, disk, journals, sessions, clients, shape)
    }

    /// Stop, let `loss` turn the disk into what survives, then start again
    /// over it: reopen the journals under fresh writer generations and ID
    /// namespace. The new actor has no links until `connect`.
    pub(crate) fn restart(
        self,
        loss: impl FnOnce(Disk) -> (Disk, Local),
        clock: &Clock,
        clients: usize,
        shape: Shape,
    ) -> Result<(Self, Vec<Lane>), String> {
        let Self {
            index,
            incarnation,
            shard,
            disk,
            ..
        } = self;
        drop(shard);
        let incarnation = incarnation + 1;
        let (mut disk, io) = loss(disk);
        let mut journals = Vec::new();
        for group in 0..shape.partitions {
            journals.push(
                disk.drive(OwnedJournal::open(
                    journal_config(index, shape, group),
                    io.clone(),
                    generation(index, incarnation, group),
                ))
                .map_err(|error| format!("broker {index}: reopen group {group}: {error}"))?,
            );
        }
        let sessions = [LinkSessionId::from_bytes([0; 16]); BROKERS];
        Self::serve(
            index,
            incarnation,
            clock,
            disk,
            journals,
            sessions,
            clients,
            shape,
        )
    }

    fn serve(
        index: usize,
        incarnation: u64,
        clock: &Clock,
        disk: Disk,
        journals: Vec<(OwnedJournal, JournalStartup)>,
        sessions: [LinkSessionId; BROKERS],
        clients: usize,
        shape: Shape,
    ) -> Result<(Self, Vec<Lane>), String> {
        // Every actor of a broker draws receipt aliases from one source.
        let receipts = ReceiptHandles::default();
        let mut actors = Vec::new();
        let mut lanes_out = Vec::new();
        let mut services_out = Vec::new();
        for (group, (owner, startup)) in journals.into_iter().enumerate() {
            let clock = clock.clone();
            let journal = owner
                .into_shard_journal(ShardJournalConfig::default(), move || clock.unix_millis())
                .map_err(|error| format!("broker {index}: journal: {error}"))?;
            // One ID namespace per actor startup.
            let namespace = (incarnation * BROKERS as u64 + index as u64) * GROUPS.len() as u64
                + group as u64
                + 1;
            let ids = ActorIds::deterministic(
                std::num::NonZeroU64::new(namespace).unwrap(),
                receipts.clone(),
            );
            let mut actor = ReplicaActor::new_with_ids(
                journal,
                startup,
                actor_config(sessions, shape.segment_bytes, clients),
                ids,
            )
            .map_err(|error| format!("broker {index}: actor: {error}"))?;
            actor.enable_recovery();
            let mut actor = PartitionActor::Replicated(
                ScheduledReplica::new(actor).map_err(|error| format!("broker {index}: {error}"))?,
            );
            actor.enable_publication();
            if group == 0 {
                lanes_out = lanes(&mut actor, clients)?;
            }
            services_out.push(services(&mut actor, index, group)?);
            actors.push(actor);
        }
        let mut shard = PartitionActors::new(actors, GROUPS.len(), TURN_ACTORS)
            .and_then(|shard| shard.with_safety_sweep(SAFETY_SWEEP))
            .map_err(|error| format!("broker {index}: shard: {error}"))?;
        let links = Links::simulated();
        let grants = ReaderGrants::new([0]);
        let mut bootstrap = Vec::new();
        for (group, (boot, intake)) in services_out.into_iter().enumerate() {
            shard
                .install_native(intake, links.clone())
                .and_then(|()| {
                    shard.install_readers(
                        GROUPS[group],
                        readers(group),
                        links.clone(),
                        grants.shard(0),
                    )
                })
                .map_err(|error| format!("broker {index}: client services: {error}"))?;
            bootstrap.push(boot);
        }
        let lanes = lanes_out;
        let broker = Self {
            index,
            incarnation,
            shard,
            disk,
            inbox: Inbox::default(),
            woken: Arc::new(Flag::default()),
            aliases: std::array::from_fn(|_| BTreeMap::new()),
            unbound: 0,
            bootstrap,
            links,
            requests: VecDeque::new(),
            replies: VecDeque::new(),
            publications: VecDeque::new(),
            grants,
            client_resets: 0,
        };
        broker.link_clients();
        Ok((broker, lanes))
    }

    /// One shard turn. Returns fresh lanes after a recovery handoff replaced
    /// the partition actor, whose old lanes are then closed.
    pub(crate) fn turn(
        &mut self,
        network: &mut Network,
        clock: &Clock,
        trace: &mut Trace,
        clients: usize,
    ) -> Result<Option<Vec<Lane>>, String> {
        let now = clock.now();
        let index = self.index;
        let failure =
            |error: ozzy_runtime::replica_actor::PartitionError| format!("broker {index}: {error}");
        // Input marks its partition; the visits below handle it.
        self.shard.defer_wakes();
        let mut input = false;
        while let Some((from, message)) = self.inbox.control.pop_front() {
            input = true;
            if let Some((message, group)) = self.admit(from, message) {
                self.shard.receive(group, &message, now).map_err(failure)?;
            }
        }
        while let Some((_, message)) = self.inbox.data.front() {
            let Some(group) = group_of(message) else {
                self.inbox.data.pop_front();
                continue;
            };
            let code = opcode(message);
            if code == Some(Opcode::PrepareFlow as u8) || code == Some(Opcode::PreparePub as u8) {
                // False keeps the frame at the lane head, pausing the lane.
                let taken = self
                    .shard
                    .receive_data(group, message, now)
                    .map_err(failure)?;
                if !taken {
                    break;
                }
            } else {
                self.shard.receive(group, message, now).map_err(failure)?;
            }
            self.inbox.data.pop_front();
            input = true;
        }
        let ready: Vec<bool> = (0..self.bootstrap.len())
            .map(|group| self.client_ready(group))
            .collect();
        while let Some((partition, message)) = self.requests.front() {
            let group = GROUPS[*partition];
            let received = if ready[*partition] {
                self.shard.receive_client(group, message, now)
            } else {
                self.shard.defer_client(group, message, now)
            };
            match received.map_err(failure)? {
                // The partition retries a busy request after its next turn.
                NativeReceive::Busy => break,
                NativeReceive::Accepted | NativeReceive::Ignored => {}
            }
            self.requests.pop_front();
            input = true;
        }
        let due = self
            .shard
            .next_deadline()
            .is_some_and(|deadline| now >= deadline);
        if !(input || due || self.woken.is_set()) {
            return Ok(None);
        }
        let waker = Waker::from(self.woken.clone());
        let mut cx = Context::from_waker(&waker);
        let tick = clock.tick();
        let mut quiet = false;
        for _ in 0..POLL_BUDGET {
            self.woken.take();
            self.poll_bootstrap(&mut cx)?;
            let replies = &mut self.replies;
            let publications = &mut self.publications;
            let progress = self.shard.poll_progress(&mut cx, now, |cx, message| {
                if records::producer(&message).is_some() || consumer::consumer(&message).is_some() {
                    replies.push_back(message);
                    return Ok(());
                }
                // A partition's live reader topic: its group and incarnation.
                if message.part_slice(0).is_some_and(|route| route.len() == 32) {
                    publications.push_back(message);
                    return Ok(());
                }
                network.submit(index, tick, cx, message, trace)
            });
            match progress {
                Poll::Pending => {}
                Poll::Ready(Ok(())) => return Err(format!("broker {index}: shard stopped")),
                Poll::Ready(Err(error)) => return Err(failure(error)),
            }
            if !self.woken.is_set() {
                quiet = true;
                break;
            }
        }
        if !quiet {
            return Err(format!(
                "broker {index}: shard woke itself for {POLL_BUDGET} polls in one tick"
            ));
        }
        let mut replaced = None;
        while let Some(group) = self.shard.take_recovered() {
            // As the broker's `install_recovered`: the new actor publishes
            // again and serves through new lanes.
            let actor = self
                .shard
                .startup_actor(group)
                .map_err(failure)?
                .ok_or_else(|| format!("broker {index}: unpublished recovery handoff"))?;
            actor.enable_publication();
            let slot = GROUPS
                .iter()
                .position(|known| *known == group)
                .ok_or_else(|| format!("broker {index}: unknown recovered group"))?;
            let fresh = if slot == 0 {
                Some(lanes(actor, clients)?)
            } else {
                None
            };
            let (bootstrap, intake) = services(actor, index, slot)?;
            self.shard
                .install_native(intake, self.links.clone())
                .map_err(failure)?;
            self.shard
                .install_readers(
                    group,
                    readers(slot),
                    self.links.clone(),
                    self.grants.shard(0),
                )
                .map_err(failure)?;
            self.bootstrap[slot] = bootstrap;
            if fresh.is_some() {
                replaced = fresh;
            }
        }
        Ok(replaced)
    }

    /// Create each partition once this broker leads its group, until that
    /// committed.
    fn poll_bootstrap(&mut self, cx: &mut Context<'_>) -> Result<(), String> {
        for group in 0..self.bootstrap.len() {
            self.poll_bootstrap_group(group, cx)?;
        }
        Ok(())
    }

    fn poll_bootstrap_group(&mut self, group: usize, cx: &mut Context<'_>) -> Result<(), String> {
        let bootstrap = &mut self.bootstrap[group];
        if let Some(pending) = &mut bootstrap.pending {
            let Poll::Ready(reply) = pending.as_mut().poll(cx) else {
                return Ok(());
            };
            bootstrap.pending = None;
            // A stopped actor gave way to recovery, which brings a new lane.
            let Ok(reply) = reply else {
                return Ok(());
            };
            match reply.outcome {
                ProposalOutcome::Committed { .. } => bootstrap.done = true,
                ProposalOutcome::NotAdmitted | ProposalOutcome::Unknown => {
                    bootstrap.buffer = Some(reply.buffer);
                }
                ProposalOutcome::Invalid(error) => {
                    return Err(format!(
                        "broker {}: create partition {group}: {error}",
                        self.index
                    ));
                }
            }
        }
        if bootstrap.done
            || !self
                .shard
                .status(GROUPS[group])
                .is_some_and(|status| match status {
                    PartitionStatus::Replicated(status) => leads(&status),
                    PartitionStatus::Local(_) => false,
                })
        {
            return Ok(());
        }
        let bootstrap = &mut self.bootstrap[group];
        if let Some(buffer) = bootstrap.buffer.take() {
            match bootstrap.submitter.try_submit(buffer) {
                Ok(pending) => {
                    bootstrap.pending = Some(Box::pin(pending));
                    cx.waker().wake_by_ref();
                }
                Err(refused) => bootstrap.buffer = Some(refused.buffer),
            }
        }
        Ok(())
    }

    /// As the broker's `client_ready`: serve a partition's clients once this
    /// broker created it, or while another broker leads it.
    fn client_ready(&self, group: usize) -> bool {
        self.bootstrap[group].done
            || self
                .shard
                .route_state(GROUPS[group], PARTITIONS[group])
                .and_then(|route| route.leader)
                .is_some_and(|leader| leader != node(self.index))
    }

    /// The session of producer `index`'s link to this broker incarnation.
    pub(crate) fn client_session(&self, index: usize) -> LinkSessionId {
        let mut bytes = [0xd0 + index as u8; 16];
        bytes[1] = self.index as u8;
        bytes[2..8].copy_from_slice(&self.client_resets.to_be_bytes()[2..]);
        bytes[8..].copy_from_slice(&self.incarnation.to_be_bytes());
        LinkSessionId::from_bytes(bytes)
    }

    /// The session of consumer `index`'s link to this broker incarnation.
    pub(crate) fn consumer_session(&self, index: usize) -> LinkSessionId {
        let mut bytes = [0xf0 + index as u8; 16];
        bytes[1] = self.index as u8;
        bytes[2..8].copy_from_slice(&self.client_resets.to_be_bytes()[2..]);
        bytes[8..].copy_from_slice(&self.incarnation.to_be_bytes());
        LinkSessionId::from_bytes(bytes)
    }

    /// Every client connection drops; clients reconnect at once under fresh
    /// sessions. Requests on the old ones are fenced.
    pub(crate) fn reset_clients(&mut self) {
        self.client_resets += 1;
        self.link_clients();
    }

    /// Publish every client's link to this broker incarnation.
    pub(crate) fn link_clients(&self) {
        for index in 0..records::PRODUCERS {
            self.links.set(
                records::client(index),
                Some(records::link(index, self.client_session(index))),
            );
        }
        for index in 0..consumer::CONSUMERS {
            self.links.set(
                consumer::client(index),
                Some(consumer::link(index, self.consumer_session(index))),
            );
        }
    }

    /// The receiving frontend's part: bind receipt aliases from full flow
    /// states, one per group of each sender, route compact receipts by their
    /// bound alias, and route everything else by its packet's group.
    fn admit(&mut self, from: usize, message: Message) -> Option<(Message, GroupId)> {
        if message.len() == 2 {
            let state = message.part_slice(1)?;
            let compact = CompactState::decode(state).ok()?;
            let Some((&group, _)) = self.aliases[from]
                .iter()
                .find(|(_, (handle, _))| *handle == compact.handle)
            else {
                self.unbound += 1;
                return None;
            };
            let routed = Message::multipart([
                Bytes::copy_from_slice(node(from).as_bytes()),
                Bytes::copy_from_slice(group.as_bytes()),
                Bytes::copy_from_slice(state),
            ]);
            return Some((routed, group));
        }
        let group = group_of(&message)?;
        if opcode(&message) == Some(Opcode::ReplicaState as u8) {
            let frames: [&[u8]; 3] =
                std::array::from_fn(|index| message.part_slice(index + 1).unwrap_or_default());
            let packet = ozzy_proto::decode_packet(&frames, LIMITS).ok()?;
            let flow = flow_state_route(packet, LIMITS).ok()?;
            let channel_group = flow.report.channel.scope.group_id;
            let aliases = &mut self.aliases[from];
            // An alias bound to another group, a lower alias, or the same
            // alias on another channel is stale.
            let taken = aliases
                .iter()
                .any(|(old, (handle, _))| *handle == flow.handle && *old != channel_group);
            let superseded = aliases
                .get(&channel_group)
                .is_some_and(|(handle, channel)| {
                    *handle > flow.handle
                        || (*handle == flow.handle && *channel != flow.report.channel)
                });
            if taken || superseded {
                self.unbound += 1;
                return None;
            }
            aliases.insert(channel_group, (flow.handle, flow.report.channel));
        }
        Some((message, group))
    }

    /// Bind a newly established link to `peer`. Receipt aliases do not
    /// cross link sessions.
    pub(crate) fn connect(
        &mut self,
        peer: usize,
        session: LinkSessionId,
        now: Duration,
    ) -> Result<(), String> {
        self.aliases[peer].clear();
        let sessions = SessionReplacement {
            expected: LinkSessionId::from_bytes([0; 16]),
            replacement: session,
        };
        for &group in self.groups() {
            self.shard
                .replace_session(group, node(peer), sessions, now)
                .map_err(|error| format!("broker {}: connect {peer}: {error}", self.index))?;
        }
        Ok(())
    }

    /// Fence the link to `peer`, which went down.
    pub(crate) fn disconnect(
        &mut self,
        peer: usize,
        session: LinkSessionId,
        now: Duration,
    ) -> Result<(), String> {
        self.aliases[peer].clear();
        for &group in self.groups() {
            self.shard
                .disconnect_session(group, node(peer), session, now)
                .map_err(|error| format!("broker {}: disconnect {peer}: {error}", self.index))?;
        }
        Ok(())
    }

    /// The groups this broker hosts.
    fn groups(&self) -> &'static [GroupId] {
        &GROUPS[..self.bootstrap.len()]
    }

    /// Group `group`'s status.
    pub(crate) fn status(&self, group: usize) -> ActorStatus {
        match self.shard.status(GROUPS[group]) {
            Some(PartitionStatus::Replicated(status)) => status,
            other => panic!("broker {}: unexpected status {other:?}", self.index),
        }
    }

    /// Group `group`'s partition facts, `None` while busy.
    pub(crate) fn facts(
        &self,
        group: usize,
    ) -> Option<ozzy_runtime::replica_journal::PartitionFacts> {
        self.shard.facts(GROUPS[group], PARTITIONS[group])
    }

    /// Close intake and drain admitted journal work.
    pub(crate) fn shutdown(self) -> Result<(), String> {
        let Self {
            index,
            shard,
            mut disk,
            ..
        } = self;
        let mut closing = std::pin::pin!(shard.shutdown());
        for _ in 0..10_000 {
            if let Poll::Ready(result) = closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            {
                return result.map_err(|error| format!("broker {index}: shutdown: {error}"));
            }
            disk.settle();
        }
        Err(format!("broker {index}: shutdown did not drain"))
    }
}

/// The bootstrap lane with its partition creation prepared, and native intake
/// for the producers, as `ozzy-broker/src/serving/shard/build.rs` builds them.
fn services(
    actor: &mut PartitionActor,
    index: usize,
    group: usize,
) -> Result<(Bootstrap, NativeIntake), String> {
    let failure = |error: &dyn std::fmt::Display| format!("broker {index}: services: {error}");
    let mut buffer = actor
        .lease_proposal_buffer_with_limits(PIPELINE)
        .map_err(|error| failure(&error))?;
    buffer
        .prepare_partition(CreatePartition {
            partition: PARTITIONS[group],
            stream: "sim",
            topic: "orders",
            partition_id: PartitionId::new(group as u32),
            owner_epoch: OwnerEpoch::INITIAL,
            delay_max_millis: None,
            dedup_window_millis: None,
            retention: RetentionPolicy::default(),
        })
        .map_err(|error| failure(&error))?;
    let bootstrap = Bootstrap {
        submitter: actor.take_submitter().ok_or("missing bootstrap lane")?,
        buffer: Some(buffer),
        pending: None,
        done: false,
    };
    let config = NativeIntakeConfig {
        local: node(index),
        group: GROUPS[group],
        partition: PARTITIONS[group],
        policy: Policy::QuorumDurable,
        access: NativeAccess::TrustedClients {
            clients: records::PRODUCERS,
            writers: records::PRODUCERS,
        },
        limits: records::LIMITS,
        reserve_dedup_results: false,
        requests_per_writer: 1,
        progress_requests: 0,
        progress_batch: 1,
        turn_slots: 16,
    };
    let count = config.required_buffers().ok_or("native arena overflow")?;
    let buffers = (0..count)
        .map(|slot| {
            let limits = config.buffer_limits(slot).ok_or("native arena layout")?;
            actor
                .lease_proposal_buffer_with_limits(limits)
                .map_err(|error| failure(&error))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let submitter = actor.take_submitter().ok_or("missing native lane")?;
    let intake = NativeIntake::new(config, submitter, buffers).map_err(|error| failure(&error))?;
    Ok((bootstrap, intake))
}

/// Subscriptions for every consumer, and one more for a replacement.
fn readers(group: usize) -> SharedReaderConfig {
    SharedReaderConfig {
        partition: PARTITIONS[group],
        limits: records::LIMITS,
        subscriptions: consumer::CONSUMERS + 1,
    }
}

fn lanes(actor: &mut PartitionActor, clients: usize) -> Result<Vec<Lane>, String> {
    (0..clients)
        .map(|_| {
            let submitter = actor.take_submitter().ok_or("missing proposal lane")?;
            let buffer = actor
                .lease_proposal_buffer_with_limits(PIPELINE)
                .map_err(|error| format!("proposal arena: {error}"))?;
            Ok(Lane {
                submitter,
                buffer: Some(buffer),
            })
        })
        .collect()
}

/// Both ends of a link use one session, fresh for each `epoch` of that
/// link. Zero means no link, as for the local slot.
pub(crate) fn session(local: usize, peer: usize, epoch: u64) -> LinkSessionId {
    if local == peer {
        return LinkSessionId::from_bytes([0; 16]);
    }
    let pair = local.min(peer) * BROKERS + local.max(peer);
    let mut bytes = [pair as u8 + 1; 16];
    bytes[8..].copy_from_slice(&epoch.to_be_bytes());
    LinkSessionId::from_bytes(bytes)
}

/// A writer generation no other startup in the run uses.
fn generation(index: usize, incarnation: u64, group: usize) -> JournalGeneration {
    let startup = u128::from(incarnation) * BROKERS as u128 + index as u128;
    JournalGeneration(startup * GROUPS.len() as u128 + group as u128 + 1)
}

/// The group a broker frame names in its packet, as the frontend routes it.
fn group_of(message: &Message) -> Option<GroupId> {
    let frames: [&[u8]; 3] =
        std::array::from_fn(|index| message.part_slice(index + 1).unwrap_or_default());
    let packet = ozzy_proto::decode_packet(&frames, LIMITS).ok()?;
    wire::route(packet, LIMITS).ok().map(|scope| scope.group_id)
}

/// Production timing, flow probes, and queue bounds from
/// `ozzy-broker/src/actors.rs`, with a small pipeline.
fn actor_config(
    sessions: [LinkSessionId; BROKERS],
    segment_bytes: u64,
    clients: usize,
) -> ActorConfig {
    let message_bytes = WireLimits::for_transfer(PIPELINE.max_operations, PIPELINE.max_body_bytes)
        .and_then(WireLimits::message_bytes)
        .expect("replica packet bound");
    ActorConfig {
        retention_interval: Some(Duration::from_millis(200)),
        sessions,
        timing: Timing {
            heartbeat: Duration::from_millis(100),
            retransmit: Duration::from_millis(100),
            primary_timeout: Duration::from_secs(1),
            election_timeout: Duration::from_secs(2),
            max_election_timeout: Duration::from_secs(8),
        },
        flow_probe: ProbeTiming {
            initial: Duration::from_millis(10),
            maximum: Duration::from_millis(100),
        },
        pipeline: PIPELINE,
        replay_cache: PIPELINE,
        transfer: PIPELINE,
        sync_batch_target: SyncBatchTarget::half_window(PIPELINE),
        evidence_quiet: Duration::from_millis(20),
        // Barrier clients, the bootstrap, and native intake.
        proposal_lanes: clients + 2,
        proposal_capacity: PIPELINE.max_operations,
        control: QueueLimits {
            messages: 8,
            bytes: 8192,
            message_bytes: 1024,
        },
        data: QueueLimits {
            messages: 4,
            bytes: message_bytes * 4,
            message_bytes,
        },
        installation: InstallationConfig {
            segment_capacity: segment_bytes,
            body_encoding: BodyEncoding::Raw,
            max_staged_bytes: segment_bytes * MAX_SEGMENTS as u64,
            max_orphan_probes: 16,
        },
    }
}

fn journal_config(index: usize, shape: Shape, group: usize) -> OwnedConfig {
    let configuration = ConfigurationRecord::new(
        GROUPS[group],
        1,
        std::array::from_fn(|voter| ConfiguredVoter {
            node_id: node(voter),
            principal: Digest::from_bytes([voter as u8 + 20; 32]),
        }),
    )
    .expect("configuration");
    OwnedConfig {
        root: format!("/group-{group}").into(),
        identity: GroupIdentity {
            group_id: GROUPS[group],
            replica_node_id: node(index),
            volume_id: VolumeId::from_bytes([9; 16]),
            store_id: StoreId::from_bytes([index as u8 + 10; 16]),
            store_generation: 1,
        },
        configuration,
        limits: JournalLimits {
            metadata: MetadataLimits {
                max_manifest_bytes: 256 * 1024,
                max_segments: shape.max_segments,
            },
            decode: DecodeLimits::default(),
            operations: ozzy_journal::operation::OperationLimits::default(),
            checkpoint: CheckpointLimits {
                max_manifest_bytes: 64 * 1024,
                max_chunks: 64,
                max_chunk_bytes: 4096,
                max_state_bytes: 256 * 1024,
            },
            io: SegmentOptions {
                max_segment_bytes: shape.segment_bytes,
                // As the broker: a quarter of the progress share, at most 64 KiB,
                // and at most a segment.
                chunk_bytes: (64 * 1024).min(shape.segment_bytes as usize),
                direct: false,
                write_mode: SegmentWriteMode::DataSync,
            },
            directory_entries: 128,
            directory_name_bytes: 16384,
        },
        recovery: CanonicalRecoveryLimits {
            // As the broker; a small overlay refreshes, and stops intake, often.
            retained_identities: 4096,
            accepted_transitions: PIPELINE.backup().max_operations,
            ..CanonicalRecoveryLimits::default()
        },
        // Native intake arenas, reader leases, and replication.
        append_buffers: 32,
        delay: None,
        deduplication: None,
        append_limits: PIPELINE,
        writeback: PIPELINE,
        // One byte ends each group after its first operation.
        write_group_bytes: if shape.sparse { 1 } else { 8192 },
        reads: PartitionReadLimits {
            index: IndexBuildLimits::default(),
            max_resident_operations: 128,
            // Several sealed indexes, as the broker's resident share allows.
            cached_index_bytes: 256 * 1024,
            cached_indexes: 4,
            concurrent_reads: 2,
        },
        producer_idle: None,
    }
}
