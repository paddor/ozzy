//! Broker provisioning, startup checks and shared device execution.
//!
//! Offline filesystem checks run before workers start. Device pools and
//! application shards follow validated placement. Shard factories initialize
//! partition actors and publish readiness on their application threads.
#![forbid(unsafe_code)]

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
pub use frontend::{Frontend, FrontendContext, TransportLimits};
pub use host::host_resources;
pub use journals::{
    JournalConfig, JournalPlan, OpenedPartition, PartitionAuthority, PartitionJournal,
    RecoveryIntent, RecoverySelection,
};
pub use provisioning::{
    CheckedConfig, check_config, initialize_broker_identity, initialize_identity,
    load_broker_identity, load_deployment,
};
pub use serving::Broker;
pub use shards::{ApplicationShards, ShardContext, ShardMemory, Shutdown};
pub use volumes::{check_volumes, initialize_volumes};

use std::path::PathBuf;

/// Configuration or offline filesystem failure. Never triggers implicit bootstrap.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error(transparent)]
    Config(#[from] ozzy_config::ConfigError),
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot discover effective host resources: {0}")]
    Host(String),
    #[error("storage controller {controller}: {source}")]
    Device {
        controller: String,
        #[source]
        source: std::io::Error,
    },
    #[error("application shard {shard}: {reason}")]
    Shard { shard: u32, reason: String },
    #[error("application runtime: {0}")]
    Runtime(String),
    #[error("broker frontend: {0}")]
    Frontend(String),
    #[error("partition journal {path}: {source}")]
    Journal {
        path: PathBuf,
        #[source]
        source: ozzy_runtime::replica_journal::JournalError,
    },
}

pub(crate) fn io_error(path: impl Into<PathBuf>, source: std::io::Error) -> StartupError {
    StartupError::Io {
        path: path.into(),
        source,
    }
}
