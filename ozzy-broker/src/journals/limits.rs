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

/// Finite journal work bounds. Shared shard admission remains responsible for
/// aggregate live requests; these limits do not grant any additional credit.
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
            reads: AsyncPartitionReadLimits {
                index,
                cached_index_bytes: 0,
                cached_indexes: 0,
                concurrent_reads: 2,
            },
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
            append_buffers: 128,
            append_limits: self.pipeline,
            writeback: self.pipeline,
            write_group_bytes: self.pipeline.max_body_bytes,
            reads: self.reads,
        }
    }
}

struct Execution {
    backend: ozzy_io::Limits,
    lane: usize,
    direct: bool,
    operations: usize,
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
    Ok(Execution {
        backend,
        lane,
        direct: controller.workers.backend == IoBackend::Aio,
        operations: shard.budget.append_slots.min(256),
    })
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
