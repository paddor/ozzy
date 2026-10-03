use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use ozzy_config::{
    BrokerIdentity, BrokerPlan, Confirmation, Deployment, DeploymentIdentity, HostResources,
    ValidatedDeployment,
};
use ozzy_proto::{GroupId, NodeId, PartitionIncarnation, TopicId, append::Policy, directory};
use ozzy_runtime::frontend::TopicCatalog;
use uuid::Uuid;

use crate::{StartupError, io_error};

const MAX_CONFIG_BYTES: u64 = 16 * 1024 * 1024;

/// Validated metadata and local topology, not an opened/recovered partition store.
#[derive(Debug)]
pub struct CheckedConfig {
    /// Validated shared deployment settings.
    pub deployment: ValidatedDeployment,
    /// Exact persistent cluster and broker bindings.
    pub identity: DeploymentIdentity,
    /// Validated broker-local resources and partition placement.
    pub plan: BrokerPlan,
}

impl CheckedConfig {
    /// SDK-visible immutable topic identity from the checked deployment record.
    /// Neither local shard placement nor runtime leader guesses enter this table.
    pub fn topic_catalog(&self) -> Result<TopicCatalog, StartupError> {
        let deployment = self.deployment.deployment();
        let brokers: Vec<_> = self
            .identity
            .brokers
            .iter()
            .map(|(name, id)| {
                let endpoints = &deployment.brokers[name].endpoints;
                directory::BrokerEndpoint {
                    node: NodeId::from_bytes(*id.as_bytes()),
                    peer: endpoints.peer.clone(),
                    reader_pub: endpoints.reader_pub.clone(),
                    follower_pub: endpoints.follower_pub.clone(),
                }
            })
            .collect();
        let mut pages = Vec::new();
        for (name, topic) in &self.identity.topics {
            let policy = match topic.confirmation {
                Confirmation::LocalDurable => Policy::LocalDurable,
                Confirmation::DiskQuorum => Policy::QuorumDurable,
                Confirmation::ReplicatedPersisting => Policy::QuorumReplicatedPersisting,
            };
            for chunk in topic
                .partitions
                .chunks(directory::Limits::default().partitions)
            {
                let partitions = chunk
                    .iter()
                    .map(|partition| directory::TopicPartition {
                        number: partition.partition,
                        group: GroupId::from_bytes(*partition.group.as_bytes()),
                        config_epoch: partition.config_epoch,
                        incarnation: PartitionIncarnation::from_bytes(
                            *partition.incarnation.as_bytes(),
                        ),
                        members: partition
                            .members
                            .iter()
                            .map(|id| NodeId::from_bytes(*id.as_bytes()))
                            .collect(),
                    })
                    .collect();
                pages.push(directory::TopicPage {
                    id: TopicId::from_bytes(*topic.id.as_bytes()),
                    name: name.clone(),
                    partitioner_seed: topic.partitioner_seed,
                    total: topic.partitions.len() as u32,
                    policy,
                    brokers: brokers.clone(),
                    first: chunk[0].partition,
                    partitions,
                });
            }
        }
        TopicCatalog::new(
            pages,
            deployment.limits.max_topics,
            deployment.limits.max_partitions,
        )
        .map_err(|error| StartupError::Runtime(error.to_string()))
    }
}

/// Load a bounded config document without formatting or opening any stores.
pub fn load_deployment(path: &Path) -> Result<ValidatedDeployment, StartupError> {
    Ok(Deployment::parse(&read_bounded(path, MAX_CONFIG_BYTES)?)?.validate()?)
}

/// Publish shared identity once. Existing files, including damaged ones, are never
/// replaced. Generate on one host, then distribute the exact record to all brokers.
/// The parent directory must already exist; no storage roots are created here.
pub fn initialize_identity(
    deployment: &ValidatedDeployment,
    path: &Path,
    next_id: impl FnMut() -> Uuid,
) -> Result<DeploymentIdentity, StartupError> {
    let identity = deployment.initialize(next_id)?;
    publish_identity(path, &identity.encode()?)?;
    Ok(identity)
}

/// Persist expected local volume/store IDs before explicit segment formatting.
/// This never discovers IDs from data directories or opens partition stores.
pub fn initialize_broker_identity(
    checked: &CheckedConfig,
    path: &Path,
    next_id: impl FnMut() -> Uuid,
) -> Result<BrokerIdentity, StartupError> {
    let identity = checked.deployment.initialize_broker_identity(
        &checked.identity,
        &checked.plan.name,
        next_id,
    )?;
    publish_identity(path, &identity.encode()?)?;
    Ok(identity)
}

/// Load exact local bindings for startup. Missing or damaged identity is fatal.
pub fn load_broker_identity(
    checked: &CheckedConfig,
    path: &Path,
) -> Result<BrokerIdentity, StartupError> {
    let bytes = read_bounded(path, identity_byte_limit(&checked.deployment)?)?;
    let identity = BrokerIdentity::decode(&bytes)?;
    checked
        .deployment
        .check_broker_identity(&checked.identity, &checked.plan.name, &identity)?;
    Ok(identity)
}

pub(crate) fn publish_identity(path: &Path, encoded: &str) -> Result<(), StartupError> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let directory = File::open(parent).map_err(|error| io_error(parent, error))?;
    let mut staging =
        tempfile::NamedTempFile::new_in(parent).map_err(|error| io_error(parent, error))?;
    staging
        .write_all(encoded.as_bytes())
        .map_err(|error| io_error(staging.path(), error))?;
    staging
        .as_file()
        .sync_all()
        .map_err(|error| io_error(staging.path(), error))?;
    // persist_noclobber atomically refuses an existing destination. A preflight
    // exists() check would race another initializer and could replace authority.
    let published = staging
        .persist_noclobber(path)
        .map_err(|error| io_error(path, error.error))?;
    published
        .sync_all()
        .map_err(|error| io_error(path, error))?;
    directory
        .sync_all()
        .map_err(|error| io_error(parent, error))?;
    Ok(())
}

/// Read established identity and validate selected placement. Missing, corrupt or
/// incompatible identity is an error, never an instruction to create a new one.
pub fn check_config(
    deployment: ValidatedDeployment,
    identity_path: &Path,
    broker: &str,
    host: &HostResources,
) -> Result<CheckedConfig, StartupError> {
    let maximum = identity_byte_limit(&deployment)?;
    let identity = DeploymentIdentity::decode(&read_bounded(identity_path, maximum)?)?;
    deployment.check_identity(&identity)?;
    let plan = deployment.broker_plan(broker, host)?;
    Ok(CheckedConfig {
        deployment,
        identity,
        plan,
    })
}

fn identity_byte_limit(deployment: &ValidatedDeployment) -> Result<u64, StartupError> {
    let config = deployment.deployment();
    let partitions: u64 = config
        .topics
        .values()
        .map(|topic| u64::from(topic.partitions))
        .sum();
    partitions
        .checked_mul(1024)
        .and_then(|size| size.checked_add(MAX_CONFIG_BYTES))
        .ok_or_else(|| StartupError::Host("identity size limit overflow".to_owned()))
}

pub(crate) fn read_bounded(path: &Path, maximum: u64) -> Result<String, StartupError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Do not follow a final symlink or block on a FIFO disguised as config.
        options.custom_flags(
            (rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK)
                .bits()
                .cast_signed(),
        );
    }
    let file = options.open(path).map_err(|error| io_error(path, error))?;
    let metadata = file.metadata().map_err(|error| io_error(path, error))?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(io_error(
            path,
            io::Error::new(io::ErrorKind::InvalidData, "expected bounded regular file"),
        ));
    }
    let mut contents = String::new();
    file.take(maximum + 1)
        .read_to_string(&mut contents)
        .map_err(|error| io_error(path, error))?;
    if contents.len() as u64 > maximum {
        return Err(io_error(
            path,
            io::Error::new(
                io::ErrorKind::InvalidData,
                "file grew beyond configured bound",
            ),
        ));
    }
    Ok(contents)
}
