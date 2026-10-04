//! Broker provisioning, startup checks and shared device execution.
//!
//! Offline filesystem checks run before workers start. Device pools and
//! application shards follow validated placement. Shard factories initialize
//! partition actors and publish readiness on their application threads.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod actors;
mod devices;
mod format;
mod frontend;
mod host;
mod journals;
mod placement;
mod provisioning;
mod serving;
mod shards;
mod volumes;

pub use actors::{ActorSettings, StartedPartition};
pub use devices::{DevicePools, ShardIo};
pub use format::format_partition_journals;
pub use frontend::{FollowerRoutes, Frontend, FrontendContext, TransportLimits};
pub use host::host_resources;
pub use journals::{
    JournalConfig, JournalPlan, OpenedPartition, PartitionAuthority, PartitionJournal,
    RecoveryIntent, RecoverySelection,
};
pub use provisioning::{
    CheckedConfig, check_config, initialize_broker_identity, initialize_identity,
    load_broker_identity, load_deployment,
};
pub use serving::{Broker, ServingContext, StorageOwner};
pub use shards::{ApplicationShards, ShardContext, ShardMemory, Shutdown};
pub use volumes::{check_volumes, initialize_volumes};

use std::path::PathBuf;

/// Configuration or offline filesystem failure. Never triggers implicit bootstrap.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error(transparent)]
    /// Deployment or persistent identity validation failed.
    Config(#[from] ozzy_config::ConfigError),
    #[error("{path}: {source}")]
    /// An offline filesystem operation failed.
    Io {
        /// Exact affected journal or filesystem path.
        path: PathBuf,
        #[source]
        /// Underlying file, backend, or journal failure.
        source: std::io::Error,
    },
    #[error("cannot discover effective host resources: {0}")]
    /// Effective host resources could not be discovered.
    Host(String),
    #[error("storage controller {controller}: {source}")]
    /// A shared physical storage backend could not start.
    Device {
        /// Configured shared physical backend owner name.
        controller: String,
        #[source]
        /// Underlying file, backend, or journal failure.
        source: std::io::Error,
    },
    #[error("application shard {shard}: {reason}")]
    /// Application owner startup or execution failed.
    Shard {
        /// Broker-local application owner ID.
        shard: u32,
        /// Owner startup or execution failure.
        reason: String,
    },
    #[error("application runtime: {0}")]
    /// The application runtime could not start or complete its work.
    Runtime(String),
    #[error("broker frontend: {0}")]
    /// The transport owner could not bind or start.
    Frontend(String),
    #[error("partition journal {path}: {source}")]
    /// A partition journal could not open or recover.
    Journal {
        /// Exact affected journal or filesystem path.
        path: PathBuf,
        #[source]
        /// Underlying file, backend, or journal failure.
        source: ozzy_runtime::replica_journal::JournalError,
    },
}

pub(crate) fn io_error(path: impl Into<PathBuf>, source: std::io::Error) -> StartupError {
    StartupError::Io {
        path: path.into(),
        source,
    }
}
