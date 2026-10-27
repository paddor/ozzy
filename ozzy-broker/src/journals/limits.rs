use crate::StartupError;
use ozzy_config::{
    BrokerPlan, ControllerPlan, IoBackend, PartitionPlacement, Topic, ValidatedDeployment,
};
use ozzy_journal::operation::OperationLimits;
use ozzy_journal_segment::{
    CanonicalRecoveryLimits, CheckpointLimits, DecodeLimits, ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES,
    GroupIdentity, IndexBuildLimits, JournalLimits, MetadataLimits, PartitionReadLimits,
    SEGMENT_HEADER_BYTES, SegmentOptions, SegmentWriteMode, WRITE_GROUP_ALIGNMENT,
};
use ozzy_replication::PipelineLimits;
use ozzy_runtime::replica_journal::{DedupLimits, OwnedConfig};

const CORE_CHECKPOINT_BYTES: usize = 32 * 1024 * 1024;

/// Finite journal work bounds. Shared shard admission bounds aggregate live requests.
pub(super) struct Settings {
    limits: JournalLimits,
    recovery: CanonicalRecoveryLimits,
    pipeline: PipelineLimits,
    reads: PartitionReadLimits,
    clients: usize,
}

impl Settings {
    pub(super) fn new(
        deployment: &ValidatedDeployment,
        plan: &BrokerPlan,
        placement: &PartitionPlacement,
    ) -> Result<Self, StartupError> {
        let error = |reason: &str| {
            StartupError::Runtime(format!(
                "partition {}/{}: {reason}",
                placement.topic, placement.partition,
            ))
        };
        let topic = deployment.deployment().topic(&placement.topic)?;
        let execution = execution(deployment, plan, placement)?;
        let sizes = Sizes::check(topic, &execution).map_err(error)?;
        let pipeline = PipelineLimits {
            max_operations: sizes.operations,
            max_body_bytes: sizes.body,
        };
        let mut snapshot = CanonicalRecoveryLimits::default().snapshot;
        snapshot.max_snapshot_bytes = CORE_CHECKPOINT_BYTES;
        let checkpoint =
            checkpoint_limits(placement, sizes).map_err(|failure| error(&failure.to_string()))?;
        let index = IndexBuildLimits {
            max_entry_buffer_bytes: sizes.chunk,
            max_merge_fan_in: 8,
            max_resident_bytes: 0,
            ..IndexBuildLimits::default()
        };
        let limits = sizes.journal_limits(topic, execution.direct, checkpoint);
        limits
            .validate_backend(execution.backend, execution.lane, &placement.directory)
            .map_err(|failure| error(&failure.to_string()))?;
        let recovery = CanonicalRecoveryLimits {
            retained_identities: 4096,
            accepted_transitions: pipeline.backup().max_operations,
            checkpoint,
            index,
            snapshot,
            ..CanonicalRecoveryLimits::default()
        };
        Ok(Self {
            limits,
            recovery,
            pipeline,
            reads: reader_limits(
                index,
                execution.resident_operations,
                execution.resident_bytes,
            ),
            clients: deployment.deployment().limits.max_clients,
        })
    }

    pub(super) fn config<C>(
        self,
        placement: &PartitionPlacement,
        identity: GroupIdentity,
        configuration: C,
    ) -> OwnedConfig<C> {
        OwnedConfig {
            root: placement.directory.clone(),
            identity,
            configuration,
            limits: self.limits,
            recovery: self.recovery,
            delay: None,
            deduplication: self.deduplication(placement),
            // Native intake arenas, plus the actor's own receive, transfer,
            // and bootstrap leases.
            append_buffers: crate::serving::native_arenas(self.clients) + 32,
            append_limits: self.pipeline,
            writeback: self.pipeline,
            write_group_bytes: self.pipeline.max_body_bytes,
            reads: self.reads,
            producer_idle: Some(std::time::Duration::from_secs(placement.producer_idle_secs)),
        }
    }

    /// The identity index this partition reserves, sized for its pipeline.
    fn deduplication(&self, placement: &PartitionPlacement) -> Option<DedupLimits> {
        placement.deduplication.map(|policy| {
            ozzy_runtime::replica_journal::dedup_limits(
                policy.window_millis,
                policy.max_entries as usize,
                policy.max_bytes as usize,
                self.limits.operations.max_records,
                self.pipeline,
            )
        })
    }
}

/// Derive every local partition's journal bounds as startup would, so a
/// deployment that cannot open fails before provisioning writes anything.
pub fn check_partition_limits(
    deployment: &ValidatedDeployment,
    plan: &BrokerPlan,
) -> Result<(), StartupError> {
    for placement in &plan.partitions {
        let settings = Settings::new(deployment, plan, placement)?;
        let Some(limits) = settings.deduplication(placement) else {
            continue;
        };
        if limits.validate().is_err() {
            let (entries, partition) = (limits.entries, placement.partition);
            let reason = match limits.required_bytes() {
                Some(needed) if needed > limits.bytes => format!(
                    "max_entries {entries} needs at least {needed} bytes on partition \
                     {partition}, not {}",
                    limits.bytes
                ),
                None => format!("max_entries {entries} needs more bytes than fit in memory"),
                Some(_) => format!("partition {partition} cannot size its identity index"),
            };
            return Err(ozzy_config::ConfigError::Invalid {
                path: format!("topics.{}.deduplication.max_bytes", placement.topic),
                reason,
            }
            .into());
        }
    }
    Ok(())
}

/// Checked write path sizes of one partition, from its topic and its share of
/// the device lane.
#[derive(Debug, Clone, Copy)]
struct Sizes {
    operations: usize,
    body: usize,
    segment: usize,
    /// Physical transfer size, also the index entry buffer.
    chunk: usize,
    /// Directory entries one listing may return.
    directory_entries: usize,
    /// Directory name bytes one listing may return.
    directory_name_bytes: usize,
}

impl Sizes {
    fn check(topic: &Topic, execution: &Execution) -> Result<Self, &'static str> {
        let native = |bytes| usize::try_from(bytes).map_err(|_| "byte limit overflow");
        let (data, progress) = share_bytes(execution.backend, execution.lane)
            .map_err(|_| "device lane outside its backend")?;
        let operations = execution.operations;
        let body = native(topic.max_append_bytes)?;
        let segment = native(topic.segment_bytes)?;
        if body < 1024 || operations == 0 {
            return Err("journal needs at least 1 KiB APPEND and one operation");
        }
        check_write(operations, body, segment, data)?;
        let chunk = (progress / 4).min(64 * 1024) / WRITE_GROUP_ALIGNMENT * WRITE_GROUP_ALIGNMENT;
        if chunk < WRITE_GROUP_ALIGNMENT {
            return Err("backend progress share cannot fit a segment transfer");
        }
        Ok(Self {
            operations,
            body,
            segment,
            chunk,
            directory_entries: (progress / 256).min(1024),
            directory_name_bytes: (progress / 4).min(64 * 1024),
        })
    }

    fn journal_limits(
        self,
        topic: &Topic,
        direct: bool,
        checkpoint: CheckpointLimits,
    ) -> JournalLimits {
        JournalLimits {
            metadata: MetadataLimits {
                max_manifest_bytes: 64 * 1024,
                max_segments: ozzy_config::MAX_PARTITION_SEGMENTS,
            },
            decode: DecodeLimits {
                max_groups: self.segment / WRITE_GROUP_ALIGNMENT,
                max_entries: self.operations,
                max_entry_bytes: self.body + ENTRY_HEADER_BYTES + 7,
                max_decoded_body_bytes: self.body,
                max_group_decoded_body_bytes: self.body,
                max_segment_decoded_body_bytes: self.segment,
            },
            operations: OperationLimits {
                max_body_bytes: self.body,
                max_records: 2048,
                max_payload_bytes: self.body,
                ..OperationLimits::default()
            },
            checkpoint,
            io: SegmentOptions {
                max_segment_bytes: topic.segment_bytes,
                chunk_bytes: self.chunk,
                direct,
                write_mode: SegmentWriteMode::DataSync,
            },
            directory_entries: self.directory_entries,
            directory_name_bytes: self.directory_name_bytes,
        }
    }
}

// Three in-flight APPENDs can move writeback ahead of PUB. Keep recent
// written operations in RAM for live reader publication. Share the existing
// read allowance with compact sealed indexes so repair pages avoid rescans.
fn reader_limits(
    index: IndexBuildLimits,
    max_resident_operations: usize,
    max_resident_bytes: usize,
) -> PartitionReadLimits {
    let cached_index_bytes = (max_resident_bytes / 2).min(2 * 1024 * 1024);
    PartitionReadLimits {
        index: IndexBuildLimits {
            max_resident_bytes: max_resident_bytes - cached_index_bytes,
            ..index
        },
        max_resident_operations,
        cached_index_bytes,
        cached_indexes: 4,
        concurrent_reads: 2,
    }
}

struct Execution {
    backend: ozzy_io::Limits,
    lane: usize,
    direct: bool,
    operations: usize,
    resident_operations: usize,
    resident_bytes: usize,
}

fn execution(
    deployment: &ValidatedDeployment,
    plan: &BrokerPlan,
    placement: &PartitionPlacement,
) -> Result<Execution, StartupError> {
    let error = |reason: &str| {
        StartupError::Runtime(format!(
            "partition {}/{}: {reason}",
            placement.topic, placement.partition,
        ))
    };
    let native = |bytes| usize::try_from(bytes).map_err(|_| error("byte limit overflow"));
    let shard = plan
        .shards
        .iter()
        .find(|shard| shard.id == placement.shard)
        .ok_or_else(|| error("missing application shard"))?;
    let (controller, lane) =
        controller_lane(plan, placement.shard).ok_or_else(|| error("missing device controller"))?;
    let backend = backend_limits(controller).map_err(error)?;
    let max_append = deployment
        .deployment()
        .topic(&placement.topic)?
        .max_append_bytes;
    let partitions = plan
        .partitions
        .iter()
        .filter(|partition| partition.shard == shard.id)
        .count()
        .max(1);
    Ok(Execution {
        backend,
        lane,
        direct: controller.workers.backend == IoBackend::Aio,
        operations: shard.budget.append_slots.min(256),
        // Bound retained operations by the shard's physical buffer slots,
        // including when each operation carries only one small record.
        resident_operations: (shard.budget.append_slots / partitions / 8).clamp(1, 128),
        // Cached records retain the original charged APPEND arenas. Reserve
        // room for live proposals and follower replay across all partitions.
        resident_bytes: reader_budget(
            native(max_append)?,
            native(shard.budget.resident_bytes)?,
            partitions,
        ),
    })
}

/// The device controller serving `shard`, and the shard's lane on it.
fn controller_lane(plan: &BrokerPlan, shard: u32) -> Option<(&ControllerPlan, usize)> {
    plan.controllers.iter().find_map(|controller| {
        let lane = controller.shards.iter().position(|id| *id == shard)?;
        Some((controller, lane))
    })
}

/// One controller's shared backend budget, split later across its lanes.
fn backend_limits(controller: &ControllerPlan) -> Result<ozzy_io::Limits, &'static str> {
    let bytes = |value: u64| usize::try_from(value).map_err(|_| "byte limit overflow");
    ozzy_io::Limits {
        shards: controller.shards.len(),
        data: ozzy_io::Quota {
            operations: controller.workers.queued_jobs,
            bytes: bytes(controller.workers.queued_bytes)?,
        },
        progress: ozzy_io::Quota {
            operations: controller.workers.progress_jobs,
            bytes: bytes(controller.workers.progress_bytes)?,
        },
    }
    .validate()
    .map_err(|_| "invalid shared backend budget")
}

/// Data and progress byte shares of one device lane.
fn share_bytes(backend: ozzy_io::Limits, lane: usize) -> std::io::Result<(usize, usize)> {
    let data = backend.share(lane, ozzy_io::Class::Data)?;
    let progress = backend.share(lane, ozzy_io::Class::Progress)?;
    Ok((data.bytes, progress.bytes))
}

fn reader_budget(append: usize, shard_bytes: usize, partitions: usize) -> usize {
    // Keep two complete APPEND arenas plus selector/growth headroom. Compact
    // sealed indexes use at most 2 MiB; all partition readers share at most a
    // third of shard admission. Payload aliases still retain their original charge.
    let desired = append
        .saturating_mul(3)
        .saturating_add(2 * 1024 * 1024)
        .max(4 * 1024 * 1024);
    (shard_bytes / partitions / 3).min(desired)
}

fn check_write(
    operations: usize,
    body: usize,
    segment: usize,
    data: usize,
) -> Result<(), &'static str> {
    // Raw groups retain bodies, framing and a backend direct-I/O staging
    // image. Reserve doubling growth for reused framing and descriptor Vecs.
    // This is conservative across both pool and AIO execution.
    let framing = operations
        .checked_mul(ENTRY_HEADER_BYTES + 7)
        .and_then(|value| value.checked_add(GROUP_SEAL_BYTES + WRITE_GROUP_ALIGNMENT))
        .ok_or("physical framing limit overflow")?;
    let physical = body
        .checked_add(framing)
        .ok_or("physical group limit overflow")?;
    let required = body
        .checked_mul(2)
        .and_then(|value| value.checked_add(framing.checked_mul(3)?))
        .and_then(|value| value.checked_add(operations.checked_mul(256)?))
        .and_then(|value| value.checked_add(16 * 1024))
        .ok_or("backend staging limit overflow")?;
    if required > data || physical > segment.saturating_sub(SEGMENT_HEADER_BYTES) {
        return Err("APPEND framing and backend staging exceed the device or segment budget");
    }
    Ok(())
}

fn checkpoint_limits(
    placement: &PartitionPlacement,
    sizes: Sizes,
) -> Result<CheckpointLimits, ozzy_journal_segment::CheckpointError> {
    CheckpointLimits::for_canonical_image(
        CORE_CHECKPOINT_BYTES,
        placement
            .deduplication
            .map_or(0, |policy| policy.max_entries),
        (sizes.directory_entries, sizes.directory_name_bytes),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_appends_keep_two_complete_backings_within_shared_read_allowance() {
        let append = 8 * 1024 * 1024;
        let share = 512 * 1024 * 1024 / 8 / 3;
        let limits = reader_limits(
            IndexBuildLimits::default(),
            128,
            reader_budget(append, 512 * 1024 * 1024, 8),
        );
        assert!(limits.index.max_resident_bytes >= 2 * append + 512 * 1024);
        assert!(limits.index.max_resident_bytes + limits.cached_index_bytes <= share);
    }
}
