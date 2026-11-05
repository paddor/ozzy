//! What a broker builds at each start, as `ozzy-broker` configures it: actor
//! and journal settings, client services, and proposal lanes.

use super::{Bootstrap, GROUPS, Lane, MAX_SEGMENTS, PARTITIONS, PIPELINE, Shape};
use crate::{
    clock::Host,
    consumer,
    network::{BROKERS, node},
    records,
};
use ozzy_journal::operation::{CreatePartition, RetentionPolicy};
use ozzy_journal_segment::{
    BodyEncoding, CanonicalRecoveryLimits, CheckpointLimits, DecodeLimits, GroupIdentity,
    IndexBuildLimits, JournalLimits, MetadataLimits, PartitionReadLimits, SegmentOptions,
    SegmentWriteMode,
};
use ozzy_proto::{LinkSessionId, OwnerEpoch, PartitionId, StoreId, VolumeId, append::Policy};
use ozzy_replication::{
    ConfigurationRecord, ConfiguredVoter, Digest, JournalGeneration, driver::Timing,
    flow::ProbeTiming, wire::WireLimits,
};
use ozzy_runtime::{
    frontend::{ClockNow, ClockTrust},
    replica_actor::{
        ActorConfig, NativeAccess, NativeIntake, NativeIntakeConfig, PartitionActor,
        SharedReaderConfig, SyncBatchTarget,
    },
    replica_journal::{
        ClockReading, DedupLimits, DelayLimits, InstallationConfig, OwnedConfig, dedup_limits,
    },
    replica_transport::QueueLimits,
};
use std::time::Duration;

/// The partitions' maximum delay, and the held records each takes.
const MAX_DELAY: u64 = 10_000;
const HELD: u64 = 1024;
/// The group whose partition deduplicates instead of delaying, as no topic
/// does both. Its producer sessions retire after `PRODUCER_IDLE`.
pub(crate) const DEDUP_GROUP: usize = 1;
/// Its dedup window and producer idle expiry in milliseconds, and the
/// identities its index holds: about four windows of records.
pub(crate) const DEDUP_WINDOW: u64 = 5_000;
pub(crate) const PRODUCER_IDLE: u64 = 3_000;
const DEDUP_ENTRIES: usize = 4096;

/// The bootstrap lane with its partition creation prepared, and native intake
/// for the producers, as `ozzy-broker/src/serving/shard/build.rs` builds them.
pub(super) fn services(
    actor: &mut PartitionActor,
    index: usize,
    group: usize,
    shape: Shape,
) -> Result<(Bootstrap, NativeIntake), String> {
    let failure = |error: &dyn std::fmt::Display| format!("broker {index}: services: {error}");
    let mut buffer = actor
        .lease_proposal_buffer_with_limits(PIPELINE)
        .map_err(|error| failure(&error))?;
    let dedup = group == DEDUP_GROUP && shape.dedup();
    buffer
        .prepare_partition(CreatePartition {
            partition: PARTITIONS[group],
            stream: "sim",
            topic: if dedup { "events" } else { "orders" },
            partition_id: PartitionId::new(group as u32),
            owner_epoch: OwnerEpoch::INITIAL,
            delay_max_millis: std::num::NonZeroU64::new(MAX_DELAY).filter(|_| group != DEDUP_GROUP),
            dedup_window_millis: std::num::NonZeroU64::new(DEDUP_WINDOW).filter(|_| dedup),
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
        reserve_dedup_results: dedup,
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
pub(super) fn readers(group: usize) -> SharedReaderConfig {
    SharedReaderConfig {
        partition: PARTITIONS[group],
        limits: records::LIMITS,
        subscriptions: consumer::CONSUMERS + 1,
    }
}

pub(super) fn lanes(actor: &mut PartitionActor, clients: usize) -> Result<Vec<Lane>, String> {
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

/// As the broker's `journal_clock`: commands stamp with the host's wall time
/// and decide only while the guard, if any, trusts it.
pub(super) fn journal_clock(
    host: &Host,
    trust: Option<&ClockTrust>,
) -> impl Fn() -> ClockReading + 'static {
    let (host, trust) = (host.clone(), trust.cloned());
    move || {
        let (wall_millis, monotonic) = (host.wall_millis(), host.now());
        ClockReading {
            wall_millis,
            trusted: trust.as_ref().is_none_or(|trust| {
                trust.trusted(|| ClockNow {
                    wall_millis,
                    monotonic,
                })
            }),
            monotonic,
        }
    }
}

/// A writer generation no other startup in the run uses.
pub(super) fn generation(index: usize, incarnation: u64, group: usize) -> JournalGeneration {
    let startup = u128::from(incarnation) * BROKERS as u128 + index as u128;
    JournalGeneration(startup * GROUPS.len() as u128 + group as u128 + 1)
}

/// Production timing, flow probes, and queue bounds from
/// `ozzy-broker/src/actors.rs`, with a small pipeline.
pub(super) fn actor_config(
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

pub(super) fn journal_config(index: usize, shape: Shape, group: usize) -> OwnedConfig {
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
        delay: (group != DEDUP_GROUP).then(|| DelayLimits::new(HELD)),
        deduplication: (group == DEDUP_GROUP && shape.dedup()).then(|| {
            let limits = dedup_limits(
                DEDUP_WINDOW,
                DEDUP_ENTRIES,
                usize::MAX,
                records::LIMITS.max_records,
                PIPELINE,
            );
            // The index reserves its whole budget up front.
            DedupLimits {
                bytes: limits.required_bytes().expect("dedup budget fits"),
                ..limits
            }
        }),
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
        producer_idle: (group == DEDUP_GROUP).then(|| shape.idle()),
    }
}
