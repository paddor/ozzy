use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BrokerIdentity, ConfigError};

/// Separates store origins from every other use of the hash.
const CONTEXT: &str = "ozzy-config 2026-11 broker store origin";

/// One broker's durable record of the stores behind its peers. A peer's
/// origin is recorded before the first link to it and never replaced, so a
/// peer that lost its stores is told from the one that held them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerRecord {
    /// Persistent cluster namespace.
    pub cluster: Uuid,
    /// Persistent local broker ID.
    pub broker: Uuid,
    /// The store origin of each peer broker met so far, by broker ID.
    pub origins: BTreeMap<Uuid, Uuid>,
}

impl PeerRecord {
    /// Encode a checksummed record.
    pub fn encode(&self) -> Result<String, ConfigError> {
        crate::record::encode("peers", self)
    }

    /// Check integrity and parse a record.
    pub fn decode(input: &str) -> Result<Self, ConfigError> {
        crate::record::decode("peers", input)
    }
}

impl BrokerIdentity {
    /// What names this broker's stores to its peers: the same for as long
    /// as these volume bindings exist, and another once they are created
    /// again. Never zero.
    pub fn origin(&self) -> Uuid {
        let mut hasher = blake3::Hasher::new_derive_key(CONTEXT);
        hasher.update(self.cluster.as_bytes());
        hasher.update(self.broker.as_bytes());
        for (device, volume) in &self.volumes {
            hasher.update(volume.as_bytes());
            hasher.update(&(device.len() as u64).to_le_bytes());
            hasher.update(device.as_bytes());
        }
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        // The version bits make it nonzero.
        uuid::Builder::from_custom_bytes(bytes).into_uuid()
    }
}
