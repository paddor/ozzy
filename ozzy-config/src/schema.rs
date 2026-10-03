use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One shared deployment document. Each process selects a named broker.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    /// Cluster identity and explicit broker mode.
    pub cluster: Cluster,
    #[serde(default)]
    /// Deployment-wide topic and partition bounds.
    pub limits: ResourceLimits,
    /// Named broker endpoints and local resources.
    pub brokers: BTreeMap<String, Broker>,
    /// Named topic policies and partition counts.
    pub topics: BTreeMap<String, Topic>,
}

/// Deployment authority, independent of local topology.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    /// Optional constraint on the identity generated at explicit initialization.
    pub id: Option<Uuid>,
    /// Single-broker or fixed-three deployment; never inferred from reachability.
    pub mode: DeploymentMode,
}

/// No automatic fallback from three brokers to one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentMode {
    /// One locally durable broker, without failover.
    Single,
    /// Three brokers with per-partition quorum replication.
    Three,
}

/// Explicit bounds checked before allocating per-partition state.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceLimits {
    /// Maximum named topics.
    pub max_topics: usize,
    /// Maximum partitions across all topics.
    pub max_partitions: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_topics: 1024,
            max_partitions: 65_536,
        }
    }
}

/// Broker-local endpoints, devices and execution layout.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Broker {
    /// Optional identity constraint checked during provisioning and restart.
    pub id: Option<Uuid>,
    /// Broker-owned control, data, and publication endpoints.
    pub endpoints: Endpoints,
    /// Named storage roots and controller resources.
    pub devices: BTreeMap<String, Device>,
    #[serde(default)]
    /// Local dispatcher, shard, memory, and partition placement.
    pub topology: Topology,
}

/// Each endpoint binds once. Values must also be usable by connecting clients.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoints {
    /// PEER endpoint for SDK control and confirmations.
    pub peer: String,
    /// PEER endpoint for SDK APPENDs and replay data.
    pub data_peer: String,
    /// PUB endpoint for confirmed live consumer records.
    pub reader_pub: String,
    /// Optional PUB endpoint for live follower replication.
    pub follower_pub: Option<String>,
}

/// Persistent topic attributes and bounded APPEND admission.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topic {
    /// Optional persistent topic identity constraint.
    pub id: Option<Uuid>,
    #[serde(default = "default_partitions")]
    /// Immutable number of partition journals.
    pub partitions: u32,
    #[serde(default)]
    /// Seed shared by SDK keyed partition routing.
    pub partitioner_seed: u64,
    /// Fixed record confirmation policy.
    pub confirmation: Confirmation,
    #[serde(default = "default_segment_bytes")]
    /// Maximum physical bytes per segment file.
    pub segment_bytes: u64,
    #[serde(default = "default_append_bytes")]
    /// Maximum canonical APPEND body, including descriptors and payload bytes.
    /// Deployment validation caps this at 8 MiB.
    pub max_append_bytes: u64,
}

/// Maximum configured broker APPEND message and canonical body size.
pub const MAX_APPEND_BYTES: u64 = 8 * 1024 * 1024;

const fn default_partitions() -> u32 {
    16
}

const fn default_segment_bytes() -> u64 {
    256 * 1024 * 1024
}

const fn default_append_bytes() -> u64 {
    MAX_APPEND_BYTES
}

/// Exact confirmation boundary, never selected per request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Confirmation {
    /// Confirm after the sole broker synchronizes its journal.
    LocalDurable,
    /// Confirm after leader and one follower synchronize matching history.
    DiskQuorum,
    /// Confirm after leader and one follower retain records; persistence proceeds independently.
    ReplicatedPersisting,
}

/// Ozzy-owned CPU affinity. OS identifiers, not hwloc logical indexes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Affinity {
    /// Optional OS CPU ID for this owner thread.
    pub cpu: Option<u32>,
    /// Optional NUMA node constraint for execution and allocation.
    pub numa_node: Option<u32>,
}

/// OMQ worker count only. Unsupported affinity fields are rejected by parsing.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Omq {
    /// OMQ-owned transport threads per broker.
    pub io_threads: usize,
}

impl Default for Omq {
    fn default() -> Self {
        Self { io_threads: 1 }
    }
}

/// Local topology. An omitted shard list becomes one shard on the sole device.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Topology {
    /// OMQ transport worker configuration.
    pub omq: Omq,
    /// Transport-owner thread affinity.
    pub dispatcher: Affinity,
    /// Application owners; omission selects one on the sole device.
    pub shards: Vec<Shard>,
    /// Explicit NUMA allocation domains.
    pub memory_pools: Vec<MemoryPool>,
    /// Optional overrides of deterministic local partition placement.
    pub partitions: Vec<PartitionOverride>,
}

/// A Ozzy allocation domain, first-touched after thread affinity is installed.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryPool {
    /// NUMA node whose pinned owner first touches the allocation.
    pub numa_node: u32,
    /// Allocation-domain byte allowance.
    pub bytes: u64,
}

/// An application shard hosts many independent partition actors.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    /// Broker-local application owner ID, possibly sparse.
    pub id: u32,
    /// Named storage device used by this shard.
    pub device: String,
    #[serde(default)]
    /// Application-owner thread affinity.
    pub affinity: Affinity,
    #[serde(default)]
    /// Independent data and control admission bounds.
    pub budget: QueueBudget,
}

/// Data and control reservations are separate, not overlapping capacities.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueBudget {
    /// Maximum outstanding APPEND reservations on the shard.
    pub append_slots: usize,
    /// Byte allowance for retained data backing.
    pub resident_bytes: u64,
    /// Independent control-message reservations.
    pub control_slots: usize,
    /// Independent retained control-byte allowance.
    pub control_bytes: u64,
}

impl Default for QueueBudget {
    fn default() -> Self {
        Self {
            append_slots: 8192,
            resident_bytes: 256 * 1024 * 1024,
            control_slots: 64,
            control_bytes: 1024 * 1024,
        }
    }
}

/// Optional broker-local placement. No shard numbers enter SDK metadata.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionOverride {
    /// Configured topic name.
    pub topic: String,
    /// Zero-based topic partition number.
    pub partition: u32,
    /// Configured local owner ID.
    pub shard: u32,
}

/// Storage root and shared controller execution. Roots contain topic/partition.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Device {
    /// Storage root containing topic and partition directories.
    pub root: PathBuf,
    /// Shared physical backend owner name within this broker.
    pub controller: String,
    #[serde(default)]
    /// Controller-wide execution and retention bounds.
    pub workers: StorageWorkers,
}

/// Pool sharing is by controller within one broker, not by partition.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageWorkers {
    /// Explicit physical file execution backend.
    pub backend: IoBackend,
    /// Ordinary blocking helper workers per controller.
    pub write_threads: usize,
    /// Empty means no pinning; otherwise one CPU per ordinary blocking worker.
    pub cpus: Vec<u32>,
    /// Optional OS CPU for the reserved progress worker.
    pub progress_cpu: Option<u32>,
    /// Optional OS CPU for the Linux AIO submission and completion owner.
    pub aio_cpu: Option<u32>,
    /// Maximum kernel-outstanding direct writes for the AIO backend.
    pub aio_depth: usize,
    /// Maximum ordinary physical jobs across blocking and direct-write workers.
    pub max_inflight: usize,
    /// Total ordinary retained jobs, including queued, running and unread results.
    pub queued_jobs: usize,
    /// Ordinary retained physical-job byte allowance.
    pub queued_bytes: u64,
    /// Independent retained progress-job reservations.
    pub progress_jobs: usize,
    /// Independent retained progress-job byte allowance.
    pub progress_bytes: u64,
    /// Maximum backend-owned open file and directory handles.
    pub open_handles: usize,
}

impl Default for StorageWorkers {
    fn default() -> Self {
        Self {
            backend: IoBackend::default(),
            write_threads: 2,
            cpus: Vec::new(),
            progress_cpu: None,
            aio_cpu: None,
            aio_depth: 1,
            max_inflight: 64,
            queued_jobs: 256,
            queued_bytes: 128 * 1024 * 1024,
            progress_jobs: 8,
            progress_bytes: 8 * 1024 * 1024,
            open_handles: 4096,
        }
    }
}

/// File execution backend. No implicit fallback or `io_uring` default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IoBackend {
    /// Linux AIO direct writes with bounded blocking file helpers.
    Aio,
    /// Explicit bounded blocking workers for all file operations.
    Pool,
}

impl Default for IoBackend {
    fn default() -> Self {
        if cfg!(target_os = "linux") {
            Self::Aio
        } else {
            Self::Pool
        }
    }
}
