//! Native actor construction from checked journal and deployment bounds.

use crate::{CheckedConfig, JournalConfig, OpenedPartition, PartitionAuthority, StartupError};
use ozzy_config::PartitionPlacement;
use ozzy_proto::{LinkSessionId, NodeId, PartitionIncarnation};
use ozzy_replication::{PipelineLimits, driver::Timing, flow::ProbeTiming};
use ozzy_runtime::{
    replica_actor::{
        ActorConfig, ActorIds, LocalActor, LocalActorConfig, PartitionActor, ProposalSubmitter,
        ReplicaActor, ScheduledReplica, SyncBatchTarget,
    },
    replica_journal::{InstallationConfig, ProposalBuffer, ShardJournalConfig},
    replica_transport::QueueLimits,
};
use std::{collections::BTreeMap, time::Duration};

/// Native per-partition work bounds. Shared shard memory admission bounds
/// aggregate live work. Replicated session entries are filled
/// from independently established broker links when the actor is constructed.
#[derive(Debug, Clone)]
pub enum ActorSettings {
    /// Single-broker actor settings with a local durability boundary.
    Local(LocalActorConfig),
    /// Fixed-three actor settings preserving their configured quorum policy.
    Replicated {
        /// Exclusive application-shard journal state.
        journal: ShardJournalConfig,
        /// Per-partition replication authority, timing, and work bounds.
        actor: Box<ActorConfig>,
    },
}

/// Shard-local actor and the bounded proposal resources used by native services.
/// Construction starts no task, socket, journal worker, or device worker.
#[derive(Debug)]
pub struct StartedPartition {
    /// Persistent cluster namespace.
    pub cluster: uuid::Uuid,
    /// Exact local journal directory and application owner.
    pub placement: PartitionPlacement,
    /// Partition record namespace, independent of election view.
    pub incarnation: PartitionIncarnation,
    /// Shard-owned partition actor and its policy state.
    pub actor: PartitionActor,
    /// Startup-registered proposal submission lane.
    pub proposal: ProposalSubmitter,
    /// Bounded reusable proposal arena owned by this partition service.
    pub buffer: ProposalBuffer,
}

impl StartedPartition {
    /// Prepare the configured partition in this service's empty proposal lease.
    /// Submit when local authority allows writes, then await the normal proposal
    /// result. Repeating after restart or an uncertain reply does not create a
    /// second operation. Changed retention policy becomes a confirmed policy operation.
    pub fn prepare_partition(&mut self) -> Result<(), StartupError> {
        prepare_partition(
            &mut self.buffer,
            self.cluster,
            &self.placement,
            self.incarnation,
        )
    }
}

pub(crate) fn prepare_partition(
    buffer: &mut ProposalBuffer,
    cluster: uuid::Uuid,
    placement: &PartitionPlacement,
    incarnation: PartitionIncarnation,
) -> Result<(), StartupError> {
    use ozzy_journal::operation::{CreatePartition, RetentionPolicy};
    use ozzy_proto::{OwnerEpoch, PartitionId};
    buffer
        .prepare_partition(CreatePartition {
            partition: incarnation,
            stream: &cluster.hyphenated().to_string(),
            topic: &placement.topic,
            partition_id: PartitionId::new(placement.partition),
            owner_epoch: OwnerEpoch::INITIAL,
            retention: RetentionPolicy {
                max_age_millis: placement
                    .retention
                    .max_age_secs
                    .and_then(|seconds| seconds.checked_mul(1000))
                    .and_then(std::num::NonZeroU64::new),
                max_bytes: placement
                    .retention
                    .max_bytes
                    .and_then(std::num::NonZeroU64::new),
            },
        })
        .map_err(|source| StartupError::Journal {
            path: placement.directory.clone(),
            source,
        })
}

impl ActorSettings {
    pub(crate) fn new(
        checked: &CheckedConfig,
        placement: &PartitionPlacement,
        config: &JournalConfig,
    ) -> Result<Self, StartupError> {
        let journal = ShardJournalConfig::default();
        let retention_interval = (placement.retention.max_age_secs.is_some()
            || placement.retention.max_bytes.is_some())
        .then_some(Duration::from_secs(1));
        let JournalConfig::Replicated(config) = config else {
            let JournalConfig::Local(config) = config else {
                unreachable!()
            };
            return Ok(Self::Local(LocalActorConfig {
                retention_interval,
                journal,
                proposal_lanes: 2,
                proposal_capacity: config.append_limits.max_operations,
                turn_steps: 16,
            }));
        };
        // Every broker reads the same document. A common packet profile must
        // fit even the smallest shard budget, independent of local CPU count or
        // partition placement. Shard-local pipeline sizes can still differ.
        let operations = checked
            .deployment
            .deployment()
            .brokers
            .values()
            .flat_map(|broker| &broker.topology.shards)
            .map(|shard| shard.budget.append_slots)
            .fold(64, usize::min);
        let pipeline = config.append_limits;
        let replay_cache = replay_cache_limits(checked, placement, pipeline)?;
        if operations == 0 || operations > pipeline.max_operations {
            return Err(StartupError::Runtime(
                "common replica transfer exceeds local pipeline".into(),
            ));
        }
        let transfer = PipelineLimits {
            max_operations: operations,
            max_body_bytes: pipeline.max_body_bytes,
        };
        let message_bytes = ozzy_replication::wire::WireLimits::for_transfer(
            transfer.max_operations,
            transfer.max_body_bytes,
        )
        .and_then(ozzy_replication::wire::WireLimits::message_bytes)
        .ok_or_else(|| StartupError::Runtime("replica packet bound overflow".into()))?;
        let queue_bytes = message_bytes
            .checked_mul(4)
            .ok_or_else(|| StartupError::Runtime("replica queue bound overflow".into()))?;
        let segment_capacity = config.limits.io.max_segment_bytes;
        let max_staged_bytes = segment_capacity
            .checked_mul(config.limits.metadata.max_segments as u64)
            .ok_or_else(|| StartupError::Runtime("replica installation bound overflow".into()))?;
        Ok(Self::Replicated {
            journal,
            actor: Box::new(ActorConfig {
                retention_interval,
                sessions: [LinkSessionId::from_bytes([0; 16]); 3],
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
                pipeline,
                replay_cache,
                transfer,
                sync_batch_target: SyncBatchTarget::half_window(pipeline),
                sync_batch_max_age: Duration::from_millis(1),
                proposal_lanes: 2,
                proposal_capacity: pipeline.max_operations,
                control: QueueLimits {
                    messages: 8,
                    bytes: 8192,
                    message_bytes: 1024,
                },
                data: QueueLimits {
                    messages: 4,
                    bytes: queue_bytes,
                    message_bytes,
                },
                installation: InstallationConfig {
                    segment_capacity,
                    body_encoding: ozzy_journal_segment::BodyEncoding::Raw,
                    max_staged_bytes,
                    max_orphan_probes: 16,
                },
            }),
        })
    }
}

fn replay_cache_limits(
    checked: &CheckedConfig,
    placement: &PartitionPlacement,
    pipeline: PipelineLimits,
) -> Result<PipelineLimits, StartupError> {
    let shard = checked
        .plan
        .shards
        .iter()
        .find(|shard| shard.id == placement.shard)
        .ok_or_else(|| StartupError::Runtime("partition has no application shard".into()))?;
    let partitions_on_shard = checked
        .plan
        .partitions
        .iter()
        .filter(|partition| partition.shard == placement.shard)
        .count();
    if partitions_on_shard == 0 {
        return Err(StartupError::Runtime(
            "partition has no shard placement".into(),
        ));
    }
    let cache_share = usize::try_from(shard.budget.resident_bytes / 8 / partitions_on_shard as u64)
        .unwrap_or(usize::MAX);
    // The existing pipeline bound stays intact. Extra replay capacity uses at
    // most one eighth of the shard's resident budget across its partitions,
    // leaving data-owner capacity for intake, persistence, and reader repair.
    let body_bytes = pipeline
        .max_body_bytes
        .saturating_mul(8)
        .min(pipeline.max_body_bytes.max(cache_share))
        .min(u32::MAX as usize);
    Ok(PipelineLimits {
        max_operations: pipeline.max_operations,
        max_body_bytes: body_bytes,
    })
}

impl OpenedPartition {
    /// Construct on the assigned shard after journal startup. Sessions come
    /// from the broker's established link table, never incoming packet claims.
    /// Absent brokers remain unbound; startup does not wait for their links.
    /// Recovered replicated journals still elect a leader before serving writes.
    /// Memory is the shard's shared data owner; empty leases reserve no payload.
    /// Deterministic harnesses inject both IDs and record timestamps.
    /// Normal followers retain within their local bounds. The shard binds
    /// their payload allocator before serving and provides bounded PUB/PEER intake.
    pub fn into_actor(
        mut self,
        memory: &ozzy_runtime::memory::Owner,
        sessions: &BTreeMap<NodeId, LinkSessionId>,
        ids: ActorIds,
        timestamp: impl Fn() -> u64 + 'static,
    ) -> Result<StartedPartition, StartupError> {
        self.journal
            .bind_append_memory(memory)
            .map_err(|source| StartupError::Journal {
                path: self.placement.directory.clone(),
                source,
            })?;
        self.start_actor(sessions, ids, timestamp)
    }

    fn start_actor(
        self,
        sessions: &BTreeMap<NodeId, LinkSessionId>,
        ids: ActorIds,
        timestamp: impl Fn() -> u64 + 'static,
    ) -> Result<StartedPartition, StartupError> {
        let error = |reason: String| {
            StartupError::Runtime(format!(
                "partition {}/{}: {reason}",
                self.placement.topic, self.placement.partition
            ))
        };
        let (actor, proposal, buffer) = match (self.authority, self.actors) {
            (PartitionAuthority::Local(driver), ActorSettings::Local(config)) => {
                let mut actor = LocalActor::new(self.journal, driver, config, timestamp)
                    .map_err(|failure| error(failure.to_string()))?;
                let buffer = actor
                    .lease_proposal_buffer()
                    .map_err(|failure| error(failure.to_string()))?;
                let proposal = actor
                    .take_submitter()
                    .expect("one configured proposal lane");
                (PartitionActor::Local(Box::new(actor)), proposal, buffer)
            }
            (
                PartitionAuthority::Replicated(startup),
                ActorSettings::Replicated { journal, mut actor },
            ) => {
                actor.sessions = [LinkSessionId::from_bytes([0; 16]); 3];
                for (slot, peer) in startup.configuration().voters().iter().enumerate() {
                    if *peer != startup.local()
                        && let Some(session) = sessions.get(peer)
                    {
                        if session.as_bytes() == &[0; 16] {
                            return Err(error("zero established broker session".into()));
                        }
                        actor.sessions[slot] = *session;
                    }
                }
                let journal = self
                    .journal
                    .into_shard_journal(journal, timestamp)
                    .map_err(|failure| error(failure.to_string()))?;
                let mut actor = ReplicaActor::new_with_ids(journal, startup, *actor, ids)
                    .map_err(|failure| error(failure.to_string()))?;
                actor.enable_recovery();
                let buffer = actor
                    .lease_proposal_buffer()
                    .map_err(|failure| error(failure.to_string()))?;
                let proposal = actor
                    .take_submitter()
                    .expect("one configured proposal lane");
                let actor =
                    ScheduledReplica::new(actor).map_err(|failure| error(failure.to_string()))?;
                (PartitionActor::Replicated(actor), proposal, buffer)
            }
            _ => return Err(error("partition mode differs from actor settings".into())),
        };
        Ok(StartedPartition {
            cluster: self.cluster,
            placement: self.placement,
            incarnation: self.incarnation,
            actor,
            proposal,
            buffer,
        })
    }
}
