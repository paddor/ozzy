use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One shared deployment document. Each process selects a named broker.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub cluster: Cluster,
    #[serde(default)]
    pub limits: ResourceLimits,
    pub brokers: BTreeMap<String, Broker>,
    pub topics: BTreeMap<String, Topic>,
}

/// Deployment authority, independent of local topology.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    /// Optional constraint on the identity generated at explicit initialization.
    pub id: Option<Uuid>,
    pub mode: DeploymentMode,
}

/// No automatic fallback from three brokers to one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentMode {
    Single,
    Three,
}

/// Explicit bounds checked before allocating per-partition state.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceLimits {
    pub max_topics: usize,
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
    pub id: Option<Uuid>,
    pub endpoints: Endpoints,
    pub devices: BTreeMap<String, Device>,
    #[serde(default)]
    pub topology: Topology,
}

/// Each endpoint binds once. Values must also be usable by connecting clients.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoints {
    pub peer: String,
    pub reader_pub: String,
    pub follower_pub: Option<String>,
}

/// Persistent topic attributes and bounded APPEND admission.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topic {
    pub id: Option<Uuid>,
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    #[serde(default)]
    pub partitioner_seed: u64,
    pub confirmation: Confirmation,
    #[serde(default = "default_segment_bytes")]
    pub segment_bytes: u64,
    #[serde(default = "default_append_bytes")]
    /// Maximum canonical APPEND body, including descriptors and payload bytes.
    pub max_append_bytes: u64,
}

const fn default_partitions() -> u32 {
    16
}

const fn default_segment_bytes() -> u64 {
    256 * 1024 * 1024
}

const fn default_append_bytes() -> u64 {
    8 * 1024 * 1024
}

/// Exact confirmation boundary, never selected per request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Confirmation {
    LocalDurable,
    DiskQuorum,
    ReplicatedPersisting,
}

/// Ozzy-owned CPU affinity. OS identifiers, not hwloc logical indexes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Affinity {
    pub cpu: Option<u32>,
    pub numa_node: Option<u32>,
}

/// OMQ worker count only. Unsupported affinity fields are rejected by parsing.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Omq {
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
    pub omq: Omq,
    pub dispatcher: Affinity,
    pub shards: Vec<Shard>,
    pub memory_pools: Vec<MemoryPool>,
    pub partitions: Vec<PartitionOverride>,
}

/// A Ozzy allocation domain, first-touched after thread affinity is installed.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryPool {
    pub numa_node: u32,
    pub bytes: u64,
}

/// An application shard hosts many independent partition actors.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Shard {
    pub id: u32,
    pub device: String,
    #[serde(default)]
    pub affinity: Affinity,
    #[serde(default)]
    pub budget: QueueBudget,
}

/// Data and control reservations are separate, not overlapping capacities.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueBudget {
    pub append_slots: usize,
    pub resident_bytes: u64,
    pub control_slots: usize,
    pub control_bytes: u64,
}

impl Default for QueueBudget {
    fn default() -> Self {
        Self {
            append_slots: 1024,
            resident_bytes: 64 * 1024 * 1024,
            control_slots: 64,
            control_bytes: 1024 * 1024,
        }
    }
}

/// Optional broker-local placement. No shard numbers enter SDK metadata.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionOverride {
    pub topic: String,
    pub partition: u32,
    pub shard: u32,
}

/// Storage root and shared controller execution. Roots contain topic/partition.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Device {
    pub root: PathBuf,
    pub controller: String,
    #[serde(default)]
    pub workers: StorageWorkers,
}

/// Pool sharing is by controller within one broker, not by partition.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageWorkers {
    pub backend: IoBackend,
    pub write_threads: usize,
    /// Empty means no pinning; otherwise one CPU per ordinary blocking worker.
    pub cpus: Vec<u32>,
    pub progress_cpu: Option<u32>,
    pub aio_cpu: Option<u32>,
    pub aio_depth: usize,
    /// Maximum ordinary physical jobs across blocking and direct-write workers.
    pub max_inflight: usize,
    /// Total ordinary retained jobs, including queued, running and unread results.
    pub queued_jobs: usize,
    pub queued_bytes: u64,
    pub progress_jobs: usize,
    pub progress_bytes: u64,
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
    Aio,
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
