//! A shared identity every broker computes alone from the cluster ID.

use uuid::Uuid;

use crate::identity::Purpose;
use crate::{ConfigError, DeploymentIdentity, ValidatedDeployment, invalid};

/// Separates these IDs from every other use of the hash.
const CONTEXT: &str = "ozzy-config 2026-11 derived deployment identity";

impl ValidatedDeployment {
    /// The shared identity that follows from `cluster.id`, the broker names
    /// and the topics. Brokers that hold the same configuration derive the
    /// same record, so none is copied between them. IDs the configuration
    /// fixes are kept.
    ///
    /// The cluster ID stands for one cluster and its history. Stores that
    /// were emptied need a new cluster ID, or old and new records share
    /// their partition namespaces.
    ///
    /// # Errors
    ///
    /// Fails without a `cluster.id`.
    pub fn derive_identity(&self) -> Result<DeploymentIdentity, ConfigError> {
        let cluster = self
            .deployment
            .cluster
            .id
            .ok_or_else(|| invalid("cluster.id", "a derived identity needs a cluster ID"))?;
        self.identify(cluster, &mut |purpose| derive(cluster, purpose))
    }
}

/// The ID of `purpose` in `cluster`: a version 8 UUID of hashed bytes.
fn derive(cluster: Uuid, purpose: Purpose<'_>) -> Uuid {
    let (kind, name, number) = match purpose {
        Purpose::Broker(name) => (1_u8, name, 0),
        Purpose::Principal(name, half) => (2, name, half),
        Purpose::Topic(name) => (3, name, 0),
        Purpose::Group(name, partition) => (4, name, partition),
        Purpose::Incarnation(name, partition) => (5, name, partition),
    };
    let mut hasher = blake3::Hasher::new_derive_key(CONTEXT);
    hasher.update(cluster.as_bytes());
    hasher.update(&[kind]);
    hasher.update(&number.to_le_bytes());
    // The name comes last and nothing follows it, so it needs no length.
    hasher.update(name.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    uuid::Builder::from_custom_bytes(bytes).into_uuid()
}
