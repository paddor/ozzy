use crate::{
    ConfigError, DeploymentIdentity, ValidatedDeployment, invalid,
    validate::{optional_id, require},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Broker-local storage bindings. Provision once and retain separately from the
/// shared deployment identity. Normal startup must not discover replacement IDs
/// from whichever directory happens to be present at the configured location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerIdentity {
    pub cluster: Uuid,
    pub broker: Uuid,
    pub volumes: BTreeMap<String, Uuid>,
    pub topics: BTreeMap<String, Vec<PartitionStore>>,
}

/// Expected local copy of a shared partition. Moving it between application
/// shards keeps these IDs. Moving storage requires explicit verified relocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionStore {
    pub partition: u32,
    pub group: Uuid,
    pub volume: Uuid,
    pub store: Uuid,
    pub generation: u64,
}

/// Identity placed on the provisioned device itself. Checking this independently
/// of partition files prevents a missing mount from becoming an empty store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeIdentity {
    pub cluster: Uuid,
    pub broker: Uuid,
    pub volume: Uuid,
}

impl VolumeIdentity {
    pub fn encode(&self) -> Result<String, ConfigError> {
        crate::record::encode("volume", self)
    }

    pub fn decode(input: &str) -> Result<Self, ConfigError> {
        crate::record::decode("volume", input)
    }
}

impl BrokerIdentity {
    /// Expected device marker, derived from established local bindings.
    pub fn volume(&self, device: &str) -> Option<VolumeIdentity> {
        self.volumes.get(device).map(|volume| VolumeIdentity {
            cluster: self.cluster,
            broker: self.broker,
            volume: *volume,
        })
    }

    /// Integrity detects corruption, not an administrator changing authority.
    pub fn encode(&self) -> Result<String, ConfigError> {
        crate::record::encode("broker", self)
    }
    /// Validate the decoded record against deployment and shared identity next.
    pub fn decode(input: &str) -> Result<Self, ConfigError> {
        crate::record::decode("broker", input)
    }
}

impl ValidatedDeployment {
    /// Explicit storage identity provisioning, before formatting any journals.
    /// A failed initialization never authorizes a second ID source on restart.
    pub fn initialize_broker_identity(
        &self,
        shared: &DeploymentIdentity,
        broker: &str,
        mut next_id: impl FnMut() -> Uuid,
    ) -> Result<BrokerIdentity, ConfigError> {
        self.check_identity(shared)?;
        let configured = self
            .deployment
            .brokers
            .get(broker)
            .ok_or_else(|| invalid("broker", "unknown broker"))?;
        let volumes: BTreeMap<_, _> = configured
            .devices
            .keys()
            .map(|name| (name.clone(), next_id()))
            .collect();
        let mut topics = BTreeMap::<String, Vec<PartitionStore>>::new();
        for placement in self.partition_placements(broker) {
            let group =
                shared.topics[&placement.topic].partitions[placement.partition as usize].group;
            topics
                .entry(placement.topic)
                .or_default()
                .push(PartitionStore {
                    partition: placement.partition,
                    group,
                    volume: volumes[&placement.device],
                    store: next_id(),
                    generation: 1,
                });
        }
        let identity = BrokerIdentity {
            cluster: shared.cluster,
            broker: shared.brokers[broker],
            volumes,
            topics,
        };
        self.check_broker_identity(shared, broker, &identity)?;
        Ok(identity)
    }

    /// Validate bindings without touching storage. Reject missing volumes,
    /// changed group/partition mapping and another broker's local identities.
    pub fn check_broker_identity(
        &self,
        shared: &DeploymentIdentity,
        broker: &str,
        identity: &BrokerIdentity,
    ) -> Result<(), ConfigError> {
        self.check_identity(shared)?;
        let configured = self
            .deployment
            .brokers
            .get(broker)
            .ok_or_else(|| invalid("broker", "unknown broker"))?;
        let path = "broker_identity";
        require(
            identity.cluster == shared.cluster
                && Some(&identity.broker) == shared.brokers.get(broker)
                && identity.volumes.len() == configured.devices.len()
                && identity.topics.len() == shared.topics.len(),
            path,
            "wrong broker, cluster or storage metadata set",
        )?;
        let mut used = shared_ids(shared);
        for name in configured.devices.keys() {
            let id = identity
                .volumes
                .get(name)
                .ok_or_else(|| invalid(path, "missing device volume identity"))?;
            optional_id(Some(*id), &mut used, path)?;
        }
        for (name, topic) in &shared.topics {
            let stores = identity
                .topics
                .get(name)
                .ok_or_else(|| invalid(path, "missing topic storage bindings"))?;
            require(
                stores.len() == topic.partitions.len(),
                path,
                "partition storage binding count changed",
            )?;
            for (store, partition) in stores.iter().zip(&topic.partitions) {
                require(
                    store.partition == partition.partition
                        && store.group == partition.group
                        && store.generation > 0,
                    path,
                    "wrong partition storage binding or zero store generation",
                )?;
                optional_id(Some(store.store), &mut used, path)?;
            }
        }
        for placement in self.partition_placements(broker) {
            let stored = &identity.topics[&placement.topic][placement.partition as usize];
            require(
                stored.volume == identity.volumes[&placement.device],
                path,
                "partition moved to another volume without explicit storage relocation",
            )?;
        }
        Ok(())
    }
}

fn shared_ids(identity: &DeploymentIdentity) -> BTreeSet<Uuid> {
    let mut used = BTreeSet::from([identity.cluster]);
    used.extend(identity.brokers.values().copied());
    for topic in identity.topics.values() {
        used.insert(topic.id);
        for partition in &topic.partitions {
            used.extend([partition.group, partition.incarnation]);
        }
    }
    used
}
