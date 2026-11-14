//! What a broker needs to turn a log spec into a record and a journal.

use super::{CreateError, LogSpec};
use crate::journals::limits::{Settings, Shape};
use crate::{ActorSettings, CheckedConfig, JournalConfig, PartitionJournal, StartupError};
use ozzy_config::{BrokerIdentity, DeploymentMode, LogClass, LogRecord, LogState};
use ozzy_journal_segment::GroupIdentity;
use ozzy_proto::{GroupId, NodeId, PartitionIncarnation, StoreId, VolumeId};
use ozzy_replication::{ConfigurationRecord, ConfiguredVoter, Digest, local};
use ozzy_runtime::frontend::{Placement, RuntimeRoute};
use std::{collections::BTreeMap, path::PathBuf};
use uuid::Uuid;

/// Checked deployment facts shared by every shard of one broker.
#[derive(Debug, Clone)]
pub(crate) struct LogSetup {
    checked: CheckedConfig,
    broker: Uuid,
    volumes: BTreeMap<String, Uuid>,
    principals: BTreeMap<Uuid, Digest>,
    pub(crate) class: LogClass,
}

fn failure(reason: impl std::fmt::Display) -> StartupError {
    StartupError::Runtime(reason.to_string())
}

impl LogSetup {
    /// `None` when the deployment configures no log class.
    pub(crate) fn new(
        checked: &CheckedConfig,
        local: &BrokerIdentity,
        principals: &BTreeMap<Uuid, Digest>,
    ) -> Option<Self> {
        Some(Self {
            class: checked.deployment.deployment().logs?,
            checked: checked.clone(),
            broker: local.broker,
            volumes: local.volumes.clone(),
            principals: principals.clone(),
        })
    }

    /// The shard of this broker that owns log `id`.
    pub(crate) fn shard(&self, id: GroupId) -> Result<u32, StartupError> {
        Ok(self.placement(&self.checked.plan.name, id)?.shard)
    }

    fn placement(
        &self,
        broker: &str,
        id: GroupId,
    ) -> Result<ozzy_config::PartitionPlacement, StartupError> {
        Ok(self
            .checked
            .deployment
            .log_placement(broker, Uuid::from_bytes(*id.as_bytes()))?)
    }

    /// The directory of a device that holds log records and log stores.
    pub(crate) fn directory(&self, device: &str) -> Result<PathBuf, StartupError> {
        Ok(self
            .checked
            .local_broker()?
            .device(device)?
            .root
            .join("logs"))
    }

    /// The `creating` record of `spec` with a fresh store, once the spec
    /// names valid members for this broker.
    pub(crate) fn record(&self, spec: &LogSpec, store: Uuid) -> Result<LogRecord, CreateError> {
        let invalid = CreateError::Invalid;
        let zero = [0; 16];
        if spec.id.as_bytes() == &zero || spec.incarnation.as_bytes() == &zero {
            return Err(invalid("zero log ID or incarnation"));
        }
        let members: Vec<Uuid> = spec
            .members
            .iter()
            .map(|node| Uuid::from_bytes(*node.as_bytes()))
            .collect();
        let brokers = &self.checked.identity.brokers;
        let known = |member: &Uuid| brokers.values().any(|id| id == member);
        let replicated = self.checked.identity.mode == DeploymentMode::Three;
        match members.as_slice() {
            [one] if *one == self.broker => {}
            [_] => return Err(invalid("the only member is another broker")),
            [first, second, third]
                if replicated
                    && members.contains(&self.broker)
                    && members.iter().all(known)
                    && first != second
                    && first != third
                    && second != third => {}
            _ => return Err(invalid("members must be this broker or the three brokers")),
        }
        let placement = self
            .placement(&self.checked.plan.name, spec.id)
            .map_err(|error| CreateError::Failed(error.to_string()))?;
        let volume = self
            .volumes
            .get(&placement.device)
            .ok_or_else(|| CreateError::Failed("the log's device has no volume".into()))?;
        Ok(LogRecord {
            cluster: self.checked.identity.cluster,
            broker: self.broker,
            log: Uuid::from_bytes(*spec.id.as_bytes()),
            incarnation: Uuid::from_bytes(*spec.incarnation.as_bytes()),
            members,
            volume: *volume,
            store,
            generation: 1,
            segment_bytes: self.class.segment_bytes,
            max_append_bytes: self.class.max_append_bytes,
            state: LogState::Creating,
        })
    }

    /// Whether `record` is one of this broker's, on the volume its
    /// placement names, within the bounds the transport was sized for.
    pub(crate) fn check(&self, record: &LogRecord) -> Result<(), StartupError> {
        let id = GroupId::from_bytes(*record.log.as_bytes());
        let placement = self.placement(&self.checked.plan.name, id)?;
        if record.cluster != self.checked.identity.cluster
            || record.broker != self.broker
            || self.volumes.get(&placement.device) != Some(&record.volume)
            || record.max_append_bytes > self.checked.deployment.deployment().max_append_bytes()
            || !matches!(record.members.len(), 1 | 3)
        {
            return Err(failure(format!(
                "log {} does not belong to this broker and class",
                record.log
            )));
        }
        Ok(())
    }

    /// This broker's journal of the recorded log: local for one member,
    /// replicated for three.
    pub(crate) fn journal(&self, record: &LogRecord) -> Result<PartitionJournal, StartupError> {
        self.check(record)?;
        let checked = &self.checked;
        let group = GroupId::from_bytes(*record.log.as_bytes());
        let placement = self.placement(&checked.plan.name, group)?;
        let shape = Shape {
            max_append_bytes: record.max_append_bytes,
            segment_bytes: record.segment_bytes,
            ..Shape::log(&checked.deployment, &checked.plan, &placement)?
        };
        let settings = Settings::new(&checked.deployment, &checked.plan, &placement, shape)?;
        let identity = GroupIdentity {
            group_id: group,
            replica_node_id: NodeId::from_bytes(*self.broker.as_bytes()),
            volume_id: VolumeId::from_bytes(*record.volume.as_bytes()),
            store_id: StoreId::from_bytes(*record.store.as_bytes()),
            store_generation: record.generation,
        };
        let voter = |id: &Uuid| {
            let principal = self.principals.get(id).copied();
            principal
                .map(|principal| ConfiguredVoter {
                    node_id: NodeId::from_bytes(*id.as_bytes()),
                    principal,
                })
                .ok_or_else(|| failure("log member without a principal"))
        };
        let config = if let [first, second, third] = record.members.as_slice() {
            let voters = [voter(first)?, voter(second)?, voter(third)?];
            let configuration = ConfigurationRecord::new(group, 1, voters).map_err(failure)?;
            JournalConfig::Replicated(settings.config(&placement, identity, configuration))
        } else {
            let own = voter(&self.broker)?;
            let configuration =
                local::Configuration::new(group, 1, own.node_id, own.principal).map_err(failure)?;
            JournalConfig::Local(settings.config(&placement, identity, configuration))
        };
        Ok(PartitionJournal {
            cluster: checked.identity.cluster,
            actors: ActorSettings::new(checked, &placement, &config, shape.sharers)?,
            placement,
            incarnation: PartitionIncarnation::from_bytes(*record.incarnation.as_bytes()),
            config,
        })
    }

    /// The broker route of a replicated log: this broker's copy and the
    /// shard of each other member's. `None` for a local log, which takes
    /// no broker traffic.
    pub(crate) fn route(&self, record: &LogRecord) -> Result<Option<RuntimeRoute>, StartupError> {
        if record.members.len() != 3 {
            return Ok(None);
        }
        let group = GroupId::from_bytes(*record.log.as_bytes());
        let mut remote = Vec::with_capacity(2);
        for (name, id) in &self.checked.identity.brokers {
            if *id != self.broker && record.members.contains(id) {
                let shard = self.placement(name, group)?.shard;
                remote.push((NodeId::from_bytes(*id.as_bytes()), shard));
            }
        }
        let remote: [(NodeId, u32); 2] = remote
            .try_into()
            .map_err(|_| failure("replicated log without two other members"))?;
        Ok(Some(RuntimeRoute {
            placement: Placement {
                group,
                partition: PartitionIncarnation::from_bytes(*record.incarnation.as_bytes()),
                shard: self.shard(group)?,
            },
            remote,
        }))
    }
}
