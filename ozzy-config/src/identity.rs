use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::validate::{optional_id, require};
use crate::{ConfigError, Confirmation, DeploymentMode, ValidatedDeployment};

/// Shared provisioning output. Persist exactly once and distribute unchanged to
/// all brokers. Decoding alone does not validate it against a deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentIdentity {
    /// Persistent cluster namespace.
    pub cluster: Uuid,
    /// Provisioned single-broker or fixed-three mode.
    pub mode: DeploymentMode,
    /// Persistent broker IDs by configured name.
    pub brokers: BTreeMap<String, Uuid>,
    /// Independently generated persistent bindings for a trusted broker domain.
    /// These identify principals, but do not authenticate transport connections.
    pub principals: BTreeMap<String, [u8; 32]>,
    /// Persistent topic and partition identities by name.
    pub topics: BTreeMap<String, TopicIdentity>,
}

/// Persistent topic metadata, excluding local execution topology and endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicIdentity {
    /// Persistent topic ID.
    pub id: Uuid,
    /// Fixed keyed partition algorithm name.
    pub partitioner: String,
    /// Persistent seed used by SDK keyed routing.
    pub partitioner_seed: u64,
    /// Persistent topic confirmation policy.
    pub confirmation: Confirmation,
    /// Numeric partition order, never sorted by random group IDs.
    pub partitions: Vec<PartitionIdentity>,
}

/// One partition's persistent identity and ordered broker membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionIdentity {
    /// Zero-based topic partition number.
    pub partition: u32,
    /// Persistent partition replication-group ID.
    pub group: Uuid,
    /// Provisioned configuration epoch. Online membership changes are absent.
    pub config_epoch: u64,
    /// Partition record namespace, independent of election view.
    pub incarnation: Uuid,
    /// Initial leader is first. Later leaders follow the election state machine.
    pub members: Vec<Uuid>,
}

impl DeploymentIdentity {
    /// Encode a checksummed TOML record. Integrity detects corruption, not hostile edits.
    pub fn encode(&self) -> Result<String, ConfigError> {
        crate::record::encode("deployment", self)
    }

    /// Check integrity and parse persisted bytes. Validate against deployment next.
    pub fn decode(input: &str) -> Result<Self, ConfigError> {
        crate::record::decode("deployment", input)
    }
}

impl ValidatedDeployment {
    /// Create identities only during explicit provisioning, never normal startup.
    /// Injected identity generation makes initialization deterministic in tests.
    /// The returned document is shared by every broker, not generated per broker.
    pub fn initialize(
        &self,
        mut next_id: impl FnMut() -> Uuid,
    ) -> Result<DeploymentIdentity, ConfigError> {
        let config = &self.deployment;
        let cluster = config.cluster.id.unwrap_or_else(&mut next_id);
        let brokers: BTreeMap<_, _> = config
            .brokers
            .iter()
            .map(|(name, broker)| (name.clone(), broker.id.unwrap_or_else(&mut next_id)))
            .collect();
        let members: Vec<_> = brokers.values().copied().collect();
        let mut topics = BTreeMap::new();
        let mut ordinal = 0;
        for (name, topic) in &config.topics {
            let id = topic.id.unwrap_or_else(&mut next_id);
            let mut partitions = Vec::with_capacity(topic.partitions as usize);
            for partition in 0..topic.partitions {
                let mut ordered = members.clone();
                ordered.rotate_left(ordinal % members.len());
                partitions.push(PartitionIdentity {
                    partition,
                    group: next_id(),
                    config_epoch: 1,
                    incarnation: next_id(),
                    members: ordered,
                });
                ordinal += 1;
            }
            topics.insert(
                name.clone(),
                TopicIdentity {
                    id,
                    partitioner: "xxh3-64".to_owned(),
                    partitioner_seed: topic.partitioner_seed,
                    confirmation: topic.confirmation,
                    partitions,
                },
            );
        }
        let principals = brokers
            .keys()
            .map(|name| {
                let mut binding = [0; 32];
                binding[..16].copy_from_slice(next_id().as_bytes());
                binding[16..].copy_from_slice(next_id().as_bytes());
                (name.clone(), binding)
            })
            .collect();
        let identity = DeploymentIdentity {
            cluster,
            mode: config.cluster.mode,
            brokers,
            principals,
            topics,
        };
        self.check_identity(&identity)?;
        Ok(identity)
    }

    /// Fail closed on changed identity, membership, hash parameters or policy.
    /// Local shard counts, CPUs, roots and worker budgets are deliberately absent.
    pub fn check_identity(&self, identity: &DeploymentIdentity) -> Result<(), ConfigError> {
        let config = &self.deployment;
        let mut used = BTreeSet::new();
        optional_id(Some(identity.cluster), &mut used, "identity.cluster")?;
        require(
            identity.mode == config.cluster.mode
                && config.cluster.id.is_none_or(|id| id == identity.cluster)
                && identity.brokers.len() == config.brokers.len()
                && identity.principals.len() == config.brokers.len()
                && identity.topics.len() == config.topics.len(),
            "identity",
            "cluster identity, mode or metadata set changed",
        )?;
        let mut principals = BTreeSet::new();
        for (name, broker) in &config.brokers {
            let id = identity.brokers.get(name);
            require(
                id.is_some() && broker.id.is_none_or(|expected| Some(&expected) == id),
                "identity.brokers",
                "broker identity or name changed",
            )?;
            optional_id(id.copied(), &mut used, "identity.brokers")?;
            require(
                identity.principals.get(name).is_some_and(|principal| {
                    principal != &[0; 32] && principals.insert(*principal)
                }),
                "identity.principals",
                "broker principal binding is missing, zero or reused",
            )?;
        }
        let members: Vec<_> = identity.brokers.values().copied().collect();
        let mut ordinal = 0;
        for (name, topic) in &config.topics {
            let stored = identity
                .topics
                .get(name)
                .ok_or_else(|| crate::invalid("identity.topics", "topic set changed"))?;
            optional_id(Some(stored.id), &mut used, "identity.topics")?;
            require(
                topic.id.is_none_or(|id| id == stored.id)
                    && stored.partitioner == "xxh3-64"
                    && stored.partitioner_seed == topic.partitioner_seed
                    && stored.confirmation == topic.confirmation
                    && stored.partitions.len() == topic.partitions as usize,
                "identity.topics",
                "topic identity, hash parameters, partition count or policy changed",
            )?;
            for (index, partition) in stored.partitions.iter().enumerate() {
                let mut ordered = members.clone();
                ordered.rotate_left(ordinal % members.len());
                require(
                    partition.partition as usize == index
                        && partition.config_epoch == 1
                        && partition.members == ordered,
                    "identity.partitions",
                    "partition order, configuration epoch or persisted membership changed",
                )?;
                optional_id(
                    Some(partition.group),
                    &mut used,
                    "identity.partitions.group",
                )?;
                optional_id(
                    Some(partition.incarnation),
                    &mut used,
                    "identity.partitions.incarnation",
                )?;
                ordinal += 1;
            }
        }
        Ok(())
    }
}
