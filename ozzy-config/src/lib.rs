//! Deployment configuration without runtime, transport or filesystem side effects.
//!
//! Parsing does not provision storage. Explicit initialization supplies an identity
//! source; restart validates its persisted result. Local CPU/shard placement never
//! contributes to partition identity or replicated membership.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod broker_identity;
mod identity;
mod placement;
mod record;
mod schema;
mod validate;

pub use broker_identity::{BrokerIdentity, PartitionStore, VolumeIdentity};
pub use identity::{DeploymentIdentity, PartitionIdentity, TopicIdentity};
pub use placement::{BrokerPlan, ControllerPlan, HostResources, PartitionPlacement, ShardPlan};
pub use schema::{
    Affinity, Broker, Cluster, Confirmation, Deployment, DeploymentMode, Device, Endpoints,
    IoBackend, MAX_APPEND_BYTES, MemoryPool, Omq, PartitionOverride, QueueBudget, ResourceLimits,
    Shard, StorageWorkers, Topic, Topology,
};
pub use validate::ValidatedDeployment;

/// Invalid input, unsupported settings, or a mismatch with persisted identity.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Malformed TOML or an unknown setting.
    #[error("invalid TOML: {0}")]
    Parse(#[from] toml::de::Error),
    /// A value violates a deployment invariant.
    #[error("{path}: {reason}")]
    Invalid {
        /// Dotted configuration location.
        path: String,
        /// Human-readable rejection reason.
        reason: String,
    },
    /// Identity serialization failed.
    #[error("cannot encode deployment identity: {0}")]
    Encode(#[from] toml::ser::Error),
}

pub(crate) fn invalid(path: impl Into<String>, reason: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        path: path.into(),
        reason: reason.into(),
    }
}
