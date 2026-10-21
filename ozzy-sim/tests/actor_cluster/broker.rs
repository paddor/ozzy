//! One simulated broker: a production `PartitionActors` shard hosting one
//! replicated group over production shard journals on its own device. A turn
//! mirrors the broker's shard turn (`ozzy-broker/src/serving/shard/
//! scheduler.rs`): deliver input first, then visit partitions until the
//! shard waker stays quiet.

use crate::{
    clock::Clock,
    disk::{Disk, Flag},
    network::{BROKERS, Inbox, Network, node, opcode},
    seeds::Trace,
};
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_io::Local;
use ozzy_journal_segment::{
    BodyEncoding, CanonicalRecoveryLimits, CheckpointLimits, DecodeLimits, GroupIdentity,
    IndexBuildLimits, JournalLimits, MetadataLimits, PartitionReadLimits, SegmentOptions,
    SegmentWriteMode,
};
use ozzy_proto::{EnvelopeLimits, GroupId, LinkSessionId, Opcode, StoreId, VolumeId};
use ozzy_replication::{
    ConfigurationRecord, ConfiguredVoter, Digest, JournalGeneration, PipelineLimits,
    driver::Timing,
    flow::{Channel, ProbeTiming},
    wire::{CompactState, WireLimits, flow_state_route},
};
use ozzy_runtime::{
    replica_actor::{
        ActorConfig, ActorIds, ActorStatus, PartitionActor, PartitionActors, PartitionStatus,
        ProposalSubmitter, ReceiptHandles, ReplicaActor, ScheduledReplica, SyncBatchTarget,
    },
    replica_journal::{
        InstallationConfig, OwnedConfig, OwnedJournal, ProposalBuffer, ShardJournalConfig,
    },
    replica_transport::QueueLimits,
};
use std::{
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

pub(crate) const GROUP: GroupId = GroupId::from_bytes([7; 16]);
pub(crate) const PIPELINE: PipelineLimits = PipelineLimits {
    max_operations: 8,
    max_body_bytes: 8192,
};
/// Default journal segment size.
pub(crate) const SEGMENT_BYTES: u64 = 256 * 1024;
/// The broker's journal segment limit.
pub(crate) const MAX_SEGMENTS: usize = 256;

/// A broker's journal: segment limit and size, and whether it writes one
/// operation per physical group.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Shape {
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

/// One proposal lane of a partition actor with its leased arena.
#[derive(Debug)]
pub(crate) struct Lane {
    pub submitter: ProposalSubmitter,
    pub buffer: Option<ProposalBuffer>,
}

#[derive(Debug)]
pub(crate) struct Broker {
    pub index: usize,
    pub shard: PartitionActors,
    pub disk: Disk,
    pub inbox: Inbox,
    woken: Arc<Flag>,
    /// Latest receipt alias bound per sending broker, as the receiving
    /// frontend binds it (`ozzy-runtime/src/frontend/flow.rs`).
    aliases: [Option<(u32, Channel)>; BROKERS],
    /// Compact receipts without a bound alias and refused alias bindings.
    pub unbound: u64,
}

impl Broker {
    /// Format, open, and schedule the broker's partition. Returns one lane per
    /// client.
    pub(crate) fn start(
        index: usize,
        clock: &Clock,
        (mut disk, io): (Disk, Local),
        clients: usize,
        shape: Shape,
    ) -> Result<(Self, Vec<Lane>), String> {
        let generation = JournalGeneration(index as u128 + 1);
        let (owner, startup) = disk
            .drive(OwnedJournal::format_new(
                journal_config(index, shape),
                io,
                generation,
                shape.segment_bytes,
            ))
            .map_err(|error| format!("broker {index}: format: {error}"))?;
        let clock = clock.clone();
        let journal = owner
            .into_shard_journal(ShardJournalConfig::default(), move || clock.unix_millis())
            .map_err(|error| format!("broker {index}: journal: {error}"))?;
        // One ID namespace per actor startup; restarts will need fresh ones.
        let ids = ActorIds::deterministic(
            std::num::NonZeroU64::new(index as u64 + 1).unwrap(),
            ReceiptHandles::default(),
        );
        let mut actor = ReplicaActor::new_with_ids(
            journal,
            startup,
            actor_config(index, shape.segment_bytes),
            ids,
        )
        .map_err(|error| format!("broker {index}: actor: {error}"))?;
        actor.enable_recovery();
        let mut actor = PartitionActor::Replicated(
            ScheduledReplica::new(actor).map_err(|error| format!("broker {index}: {error}"))?,
        );
        actor.enable_publication();
        let lanes = lanes(&mut actor, clients)?;
        let shard = PartitionActors::new(vec![actor], 1, TURN_ACTORS)
            .and_then(|shard| shard.with_safety_sweep(SAFETY_SWEEP))
            .map_err(|error| format!("broker {index}: shard: {error}"))?;
        let broker = Self {
            index,
            shard,
            disk,
            inbox: Inbox::default(),
            woken: Arc::new(Flag::default()),
            aliases: [None; BROKERS],
            unbound: 0,
        };
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
            if let Some(message) = self.admit(from, message) {
                self.shard.receive(GROUP, &message, now).map_err(failure)?;
            }
        }
        while let Some((_, message)) = self.inbox.data.front() {
            let code = opcode(message);
            if code == Some(Opcode::PrepareFlow as u8) || code == Some(Opcode::PreparePub as u8) {
                // False keeps the frame at the lane head, pausing the lane.
                let taken = self
                    .shard
                    .receive_data(GROUP, message, now)
                    .map_err(failure)?;
                if !taken {
                    break;
                }
            } else {
                self.shard.receive(GROUP, message, now).map_err(failure)?;
            }
            self.inbox.data.pop_front();
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
            let progress = self.shard.poll_progress(&mut cx, now, |cx, message| {
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
            replaced = Some(lanes(actor, clients)?);
        }
        Ok(replaced)
    }

    /// The receiving frontend's part: bind receipt aliases from full flow
    /// states, and route compact receipts by their bound alias.
    fn admit(&mut self, from: usize, message: Message) -> Option<Message> {
        if message.len() == 2 {
            let state = message.part_slice(1)?;
            let compact = CompactState::decode(state).ok()?;
            if self.aliases[from].map(|(handle, _)| handle) != Some(compact.handle) {
                self.unbound += 1;
                return None;
            }
            return Some(Message::multipart([
                Bytes::copy_from_slice(node(from).as_bytes()),
                Bytes::copy_from_slice(GROUP.as_bytes()),
                Bytes::copy_from_slice(state),
            ]));
        }
        if opcode(&message) == Some(Opcode::ReplicaState as u8) {
            let frames: [&[u8]; 3] =
                std::array::from_fn(|index| message.part_slice(index + 1).unwrap_or_default());
            let packet = ozzy_proto::decode_packet(&frames, LIMITS).ok()?;
            let state = flow_state_route(packet, LIMITS).ok()?;
            // A lower alias, or the same alias on another channel, is stale.
            if self.aliases[from].is_some_and(|(handle, channel)| {
                handle > state.handle || (handle == state.handle && channel != state.report.channel)
            }) {
                self.unbound += 1;
                return None;
            }
            self.aliases[from] = Some((state.handle, state.report.channel));
        }
        Some(message)
    }

    pub(crate) fn status(&self) -> ActorStatus {
        match self.shard.status(GROUP) {
            Some(PartitionStatus::Replicated(status)) => status,
            other => panic!("broker {}: unexpected status {other:?}", self.index),
        }
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

/// Both ends of a link use one session. Zero means no link, as for the
/// local slot.
fn session(local: usize, peer: usize) -> LinkSessionId {
    if local == peer {
        return LinkSessionId::from_bytes([0; 16]);
    }
    let pair = local.min(peer) * BROKERS + local.max(peer);
    LinkSessionId::from_bytes([pair as u8 + 1; 16])
}

/// Production timing, flow probes, and queue bounds from
/// `ozzy-broker/src/actors.rs`, with a small pipeline.
fn actor_config(index: usize, segment_bytes: u64) -> ActorConfig {
    let message_bytes = WireLimits::for_transfer(PIPELINE.max_operations, PIPELINE.max_body_bytes)
        .and_then(WireLimits::message_bytes)
        .expect("replica packet bound");
    ActorConfig {
        retention_interval: Some(Duration::from_millis(200)),
        sessions: std::array::from_fn(|peer| session(index, peer)),
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
        proposal_lanes: 3,
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

fn journal_config(index: usize, shape: Shape) -> OwnedConfig {
    let configuration = ConfigurationRecord::new(
        GROUP,
        1,
        std::array::from_fn(|voter| ConfiguredVoter {
            node_id: node(voter),
            principal: Digest::from_bytes([voter as u8 + 20; 32]),
        }),
    )
    .expect("configuration");
    OwnedConfig {
        root: "/group".into(),
        identity: GroupIdentity {
            group_id: GROUP,
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
        append_buffers: 12,
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
