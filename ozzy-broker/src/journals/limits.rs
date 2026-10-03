use crate::{CheckedConfig, StartupError};
use ozzy_config::{Confirmation, IoBackend, PartitionPlacement};
use ozzy_journal::operation::OperationLimits;
use ozzy_journal_segment::{
    AsyncJournalLimits, AsyncPartitionReadLimits, AsyncSegmentOptions, CanonicalRecoveryLimits,
    CheckpointLimits, DecodeLimits, ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, GroupIdentity,
    IndexBuildLimits, MetadataLimits, SEGMENT_HEADER_BYTES, SegmentWriteMode,
    WRITE_GROUP_ALIGNMENT,
};
use ozzy_replication::PipelineLimits;
use ozzy_runtime::replica_journal::OwnedConfig;

/// Finite journal work bounds. Shared shard admission bounds aggregate live requests.
pub(super) struct Settings {
    limits: AsyncJournalLimits,
    recovery: CanonicalRecoveryLimits,
    pipeline: PipelineLimits,
    reads: AsyncPartitionReadLimits,
}

impl Settings {
    pub(super) fn new(
        checked: &CheckedConfig,
        placement: &PartitionPlacement,
    ) -> Result<Self, StartupError> {
        let error = |reason: &str| {
            StartupError::Runtime(format!(
                "partition {}/{}: {reason}",
                placement.topic, placement.partition,
            ))
        };
        let topic = &checked.deployment.deployment().topics[&placement.topic];
        let native = |bytes| usize::try_from(bytes).map_err(|_| error("byte limit overflow"));
        let Execution {
            backend,
            lane,
            direct,
            operations,
            resident_operations,
            resident_bytes,
        } = execution(checked, placement)?;
        let data = backend.share(lane, ozzy_io::Class::Data).bytes;
        let progress = backend.share(lane, ozzy_io::Class::Progress).bytes;
        let body = native(topic.max_append_bytes)?;
        let segment = native(topic.segment_bytes)?;
        if body < 1024 || operations == 0 {
            return Err(error(
                "journal needs at least 1 KiB APPEND and one operation",
            ));
        }
        check_write(operations, body, segment, data).map_err(error)?;
        let chunk = (progress / 4).min(64 * 1024) / WRITE_GROUP_ALIGNMENT * WRITE_GROUP_ALIGNMENT;
        if chunk < WRITE_GROUP_ALIGNMENT {
            return Err(error(
                "backend progress share cannot fit a segment transfer",
            ));
        }
        let pipeline = PipelineLimits {
            max_operations: operations,
            max_body_bytes: body,
        };
        let checkpoint = CheckpointLimits {
            max_manifest_bytes: 64 * 1024,
            max_chunks: 512,
            max_chunk_bytes: chunk,
            max_state_bytes: 32 * 1024 * 1024,
        };
        let index = IndexBuildLimits {
            max_entry_buffer_bytes: chunk,
            max_merge_fan_in: 8,
            max_resident_bytes: 0,
            ..IndexBuildLimits::default()
        };
        let limits = AsyncJournalLimits {
            metadata: MetadataLimits {
                max_manifest_bytes: 64 * 1024,
                max_segments: 256,
            },
            decode: DecodeLimits {
                max_groups: segment / WRITE_GROUP_ALIGNMENT,
                max_entries: segment / ENTRY_HEADER_BYTES,
                max_entry_bytes: body + ENTRY_HEADER_BYTES + 7,
                max_decoded_body_bytes: body,
                max_group_decoded_body_bytes: body,
                max_segment_decoded_body_bytes: segment,
            },
            operations: OperationLimits {
                max_body_bytes: body,
                max_records: 2048,
                max_payload_bytes: body,
                ..OperationLimits::default()
            },
            checkpoint,
            io: AsyncSegmentOptions {
                max_segment_bytes: topic.segment_bytes,
                chunk_bytes: chunk,
                direct,
                write_mode: if topic.confirmation == Confirmation::ReplicatedPersisting {
                    SegmentWriteMode::Buffered
                } else {
                    SegmentWriteMode::DataSync
                },
            },
            directory_entries: (progress / 256).min(1024),
            directory_name_bytes: (progress / 4).min(64 * 1024),
        };
        limits
            .validate_backend(backend, lane, &placement.directory)
            .map_err(|failure| error(&failure.to_string()))?;
        let recovery = CanonicalRecoveryLimits {
            retained_identities: 4096,
            accepted_transitions: operations,
            checkpoint,
            index,
            ..CanonicalRecoveryLimits::default()
        };
        Ok(Self {
            limits,
            recovery,
            pipeline,
            reads: reader_limits(index, resident_operations, resident_bytes),
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
            // Native intake arenas, plus the actor's own receive, transfer,
            // and bootstrap leases.
            append_buffers: crate::serving::NATIVE_ARENAS + 32,
            append_limits: self.pipeline,
            writeback: self.pipeline,
            write_group_bytes: self.pipeline.max_body_bytes,
            reads: self.reads,
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
) -> AsyncPartitionReadLimits {
    let cached_index_bytes = (max_resident_bytes / 2).min(2 * 1024 * 1024);
    AsyncPartitionReadLimits {
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
    checked: &CheckedConfig,
    placement: &PartitionPlacement,
) -> Result<Execution, StartupError> {
    let error = |reason: &str| {
        StartupError::Runtime(format!(
            "partition {}/{}: {reason}",
            placement.topic, placement.partition,
        ))
    };
    let native = |bytes| usize::try_from(bytes).map_err(|_| error("byte limit overflow"));
    let shard = checked
        .plan
        .shards
        .iter()
        .find(|shard| shard.id == placement.shard)
        .ok_or_else(|| error("missing application shard"))?;
    let controller = checked
        .plan
        .controllers
        .iter()
        .find(|controller| controller.shards.contains(&placement.shard))
        .ok_or_else(|| error("missing device controller"))?;
    let lane = controller
        .shards
        .iter()
        .position(|id| *id == placement.shard)
        .expect("selected controller lane");
    let backend = ozzy_io::Limits {
        shards: controller.shards.len(),
        data: ozzy_io::Quota {
            operations: controller.workers.queued_jobs,
            bytes: native(controller.workers.queued_bytes)?,
        },
        progress: ozzy_io::Quota {
            operations: controller.workers.progress_jobs,
            bytes: native(controller.workers.progress_bytes)?,
        },
    }
    .validate()
    .map_err(|_| error("invalid shared backend budget"))?;
    let partitions = checked
        .plan
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
            native(checked.deployment.deployment().topics[&placement.topic].max_append_bytes)?,
            native(shard.budget.resident_bytes)?,
            partitions,
        ),
    })
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
