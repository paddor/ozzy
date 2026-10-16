//! Native partition journal construction from persisted deployment bindings.

mod limits;
mod recovery;
pub use recovery::{RecoveryIntent, RecoverySelection};

use ozzy_config::{BrokerIdentity, Confirmation, PartitionIdentity, PartitionPlacement};
use ozzy_io::Local;
use ozzy_journal_segment::GroupIdentity;
use ozzy_proto::{GroupId, NodeId, PartitionIncarnation, StoreId, VolumeId};
use ozzy_replication::{
    ConfigurationRecord, ConfiguredVoter, Digest, JournalGeneration, QuorumPolicy, local,
};
use ozzy_runtime::replica_journal::{JournalStartup, OwnedConfig, OwnedJournal};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::{ActorSettings, CheckedConfig, StartupError};

/// Fully checked construction inputs. No files, worker threads or runtime state.
#[derive(Debug, Clone)]
pub struct JournalPlan {
    /// Broker-local partitions in the validated construction plan.
    pub partitions: Vec<PartitionJournal>,
    pub(crate) recovery: BTreeMap<GroupId, RecoveryIntent>,
}

/// One broker-local journal, independent of its actor's leadership role.
#[derive(Debug, Clone)]
pub struct PartitionJournal {
    /// Persistent cluster namespace used by the canonical partition address.
    pub cluster: Uuid,
    /// Exact local journal directory and application owner.
    pub placement: PartitionPlacement,
    /// Partition record namespace, independent of election view.
    pub incarnation: PartitionIncarnation,
    /// Explicit local-durable or replicated journal configuration.
    pub config: JournalConfig,
    /// Partition actor timing, confirmation, and work bounds.
    pub actors: ActorSettings,
}

/// Explicit mode. A failed replicated open never tries the local variant.
#[derive(Debug, Clone)]
pub enum JournalConfig {
    /// Explicit single-broker locally durable journal.
    Local(OwnedConfig<local::Configuration>),
    /// Explicit fixed-three quorum journal configuration.
    Replicated(OwnedConfig),
}

/// Authority returned by the native segment runtime. Replicated restart still
/// requires election; an opened journal is not permission to serve APPENDs.
#[derive(Debug)]
pub enum PartitionAuthority {
    /// Recovered locally durable driver; no election or failover.
    Local(local::Driver),
    /// Recovered replicated startup; election is required before leadership.
    Replicated(JournalStartup),
}

#[derive(Debug)]
/// Opened journal and exact recovered authority awaiting actor construction.
pub struct OpenedPartition {
    /// Persistent cluster namespace.
    pub cluster: Uuid,
    /// Exact local journal directory and application owner.
    pub placement: PartitionPlacement,
    /// Partition record namespace, independent of election view.
    pub incarnation: PartitionIncarnation,
    /// Exclusive application-shard journal state.
    pub journal: OwnedJournal,
    /// Recovered local driver or fenced replicated startup evidence.
    pub authority: PartitionAuthority,
    /// Partition actor timing, confirmation, and work bounds.
    pub actors: ActorSettings,
}

impl JournalPlan {
    /// Use the separately provisioned bindings in an explicitly trusted broker
    /// transport domain. No principal is derived from a routing node ID or
    /// endpoint. The caller establishes transport trust independently.
    pub fn from_trusted_deployment(
        checked: &CheckedConfig,
        local: &BrokerIdentity,
    ) -> Result<Self, StartupError> {
        checked.deployment.check_identity(&checked.identity)?;
        let principals = checked
            .identity
            .brokers
            .iter()
            .map(|(name, id)| Ok((*id, Digest::from_bytes(checked.identity.principal(name)?))))
            .collect::<Result<_, StartupError>>()?;
        Self::new(checked, local, &principals)
    }

    /// Bind journals to the adapter's exact principal fingerprints. These must
    /// come from its established identity mapping, never endpoint hashes or
    /// freshly generated restart IDs. This does not authenticate any connection.
    pub fn new(
        checked: &CheckedConfig,
        local: &BrokerIdentity,
        principals: &BTreeMap<Uuid, Digest>,
    ) -> Result<Self, StartupError> {
        checked
            .deployment
            .check_broker_identity(&checked.identity, &checked.plan.name, local)?;
        check_principals(checked, principals)?;
        let mut partitions = Vec::with_capacity(checked.plan.partitions.len());
        for placement in &checked.plan.partitions {
            let binding = PartitionSetup {
                placement,
                principals,
            };
            partitions.push(binding.journal(checked, local)?);
        }
        Ok(Self {
            partitions,
            recovery: BTreeMap::new(),
        })
    }
}

/// Every broker has exactly one nonzero principal, and no two share one.
fn check_principals(
    checked: &CheckedConfig,
    principals: &BTreeMap<Uuid, Digest>,
) -> Result<(), StartupError> {
    if principals.len() != checked.identity.brokers.len()
        || checked.identity.brokers.values().any(|id| {
            principals
                .get(id)
                .is_none_or(|principal| *principal == Digest::ZERO)
        })
    {
        return Err(StartupError::Runtime(
            "missing broker principal binding".into(),
        ));
    }
    let mut distinct = std::collections::BTreeSet::new();
    if principals
        .values()
        .any(|value| !distinct.insert(*value.as_bytes()))
    {
        return Err(StartupError::Runtime(
            "duplicate broker principal binding".into(),
        ));
    }
    Ok(())
}

/// One placed partition and the principals its configuration binds.
struct PartitionSetup<'a> {
    placement: &'a PartitionPlacement,
    principals: &'a BTreeMap<Uuid, Digest>,
}

impl PartitionSetup<'_> {
    /// This broker's journal for the partition, in its topic's mode.
    fn journal(
        &self,
        checked: &CheckedConfig,
        local: &BrokerIdentity,
    ) -> Result<PartitionJournal, StartupError> {
        let placement = self.placement;
        let partition = checked
            .identity
            .partition(&placement.topic, placement.partition)?;
        let store = local.store(&placement.topic, placement.partition)?;
        let identity = GroupIdentity {
            group_id: GroupId::from_bytes(*partition.group.as_bytes()),
            replica_node_id: NodeId::from_bytes(*local.broker.as_bytes()),
            volume_id: VolumeId::from_bytes(*store.volume.as_bytes()),
            store_id: StoreId::from_bytes(*store.store.as_bytes()),
            store_generation: store.generation,
        };
        let settings = limits::Settings::new(checked, placement)?;
        let config = match checked.identity.topic(&placement.topic)?.confirmation {
            Confirmation::LocalDurable => {
                let configuration = local::Configuration::new(
                    identity.group_id,
                    partition.config_epoch,
                    identity.replica_node_id,
                    self.principal(&local.broker)?,
                )
                .map_err(|failure| self.error(&failure.to_string()))?;
                JournalConfig::Local(settings.config(placement, identity, configuration))
            }
            policy => {
                let configuration = self.replicated(identity.group_id, partition, policy)?;
                JournalConfig::Replicated(settings.config(placement, identity, configuration))
            }
        };
        Ok(PartitionJournal {
            cluster: checked.identity.cluster,
            actors: ActorSettings::new(checked, placement, &config)?,
            placement: placement.clone(),
            incarnation: PartitionIncarnation::from_bytes(*partition.incarnation.as_bytes()),
            config,
        })
    }

    /// The fixed-three voter configuration under the topic's quorum policy.
    fn replicated(
        &self,
        group: GroupId,
        partition: &PartitionIdentity,
        policy: Confirmation,
    ) -> Result<ConfigurationRecord, StartupError> {
        let members: [Uuid; 3] = partition
            .members
            .clone()
            .try_into()
            .map_err(|_| self.error("expected three broker members"))?;
        let [first, second, third] = members.map(|id| {
            self.principal(&id).map(|principal| ConfiguredVoter {
                node_id: NodeId::from_bytes(*id.as_bytes()),
                principal,
            })
        });
        ConfigurationRecord::with_policy(
            group,
            partition.config_epoch,
            [first?, second?, third?],
            if policy == Confirmation::DiskQuorum {
                QuorumPolicy::Durable
            } else {
                QuorumPolicy::Replicated
            },
        )
        .map_err(|failure| self.error(&failure.to_string()))
    }

    fn principal(&self, id: &Uuid) -> Result<Digest, StartupError> {
        self.principals
            .get(id)
            .copied()
            .ok_or_else(|| self.error("missing broker principal"))
    }

    fn error(&self, reason: &str) -> StartupError {
        StartupError::Runtime(format!(
            "partition {}/{}: {reason}",
            self.placement.topic, self.placement.partition,
        ))
    }
}

impl PartitionJournal {
    /// Explicit formatting only, below an existing topic directory. A partial
    /// or established store is an error, never removed or replaced here.
    pub async fn format(
        self,
        io: Local,
        generation: JournalGeneration,
    ) -> Result<OpenedPartition, StartupError> {
        self.start(io, generation, true).await
    }

    /// Normal startup never creates missing directories or fresh authority.
    pub async fn open(
        self,
        io: Local,
        generation: JournalGeneration,
    ) -> Result<OpenedPartition, StartupError> {
        self.start(io, generation, false).await
    }

    async fn start(
        self,
        io: Local,
        generation: JournalGeneration,
        format: bool,
    ) -> Result<OpenedPartition, StartupError> {
        let error = |source| StartupError::Journal {
            path: self.placement.directory.clone(),
            source,
        };
        let (journal, authority) = match self.config {
            JournalConfig::Local(config) => {
                let capacity = config.limits.io.max_segment_bytes;
                let (journal, driver) = if format {
                    OwnedJournal::format_local(config, io, generation, capacity).await
                } else {
                    OwnedJournal::open_local(config, io, generation).await
                }
                .map_err(error)?;
                (journal, PartitionAuthority::Local(driver))
            }
            JournalConfig::Replicated(config) => {
                let capacity = config.limits.io.max_segment_bytes;
                let (journal, startup) = if format {
                    OwnedJournal::format_new(config, io, generation, capacity).await
                } else {
                    OwnedJournal::open(config, io, generation).await
                }
                .map_err(error)?;
                (journal, PartitionAuthority::Replicated(startup))
            }
        };
        Ok(OpenedPartition {
            cluster: self.cluster,
            placement: self.placement,
            incarnation: self.incarnation,
            journal,
            authority,
            actors: self.actors,
        })
    }
}
