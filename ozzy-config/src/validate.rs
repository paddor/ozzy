use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;

use uuid::Uuid;

use crate::{
    Broker, ConfigError, Confirmation, Deployment, DeploymentMode, QueueBudget, Shard, invalid,
    schema::MAX_APPEND_BYTES,
};

/// Structurally valid configuration. Host checks are performed for the selected
/// broker separately; another broker may have different CPUs and storage paths.
#[derive(Debug, Clone)]
pub struct ValidatedDeployment {
    pub(crate) deployment: Deployment,
    pub(crate) shards: BTreeMap<String, Vec<Shard>>,
}

impl Deployment {
    /// Parse a document, rejecting typos instead of silently ignoring settings.
    pub fn parse(input: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(input)?)
    }

    /// Check deployment-wide contracts without touching storage or starting workers.
    pub fn validate(self) -> Result<ValidatedDeployment, ConfigError> {
        let expected = match self.cluster.mode {
            DeploymentMode::Single => 1,
            DeploymentMode::Three => 3,
        };
        require(
            self.brokers.len() == expected,
            "brokers",
            "membership must match the explicit single or three mode",
        )?;
        require(
            self.limits.max_topics > 0 && self.limits.max_partitions > 0,
            "limits",
            "resource limits must be positive",
        )?;
        require(
            !self.topics.is_empty() && self.topics.len() <= self.limits.max_topics,
            "topics",
            "topic count must be nonzero and fit max_topics",
        )?;
        let mut ids = BTreeSet::new();
        optional_id(self.cluster.id, &mut ids, "cluster.id")?;
        validate_topics(&self, &mut ids)?;
        let mut endpoints = BTreeSet::new();
        let mut shards = BTreeMap::new();
        for (name, broker) in &self.brokers {
            safe_name(name, "brokers")?;
            let path = format!("brokers.{name}");
            optional_id(broker.id, &mut ids, &path)?;
            for endpoint in [
                Some(&broker.endpoints.peer),
                Some(&broker.endpoints.data_peer),
                Some(&broker.endpoints.reader_pub),
                broker.endpoints.follower_pub.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                validate_endpoint(endpoint, &path)?;
                require(
                    endpoints.insert(endpoint.clone()),
                    &path,
                    "endpoint reused within deployment",
                )?;
            }
            let local = validate_broker(broker, &path)?;
            let mut overrides = BTreeSet::new();
            for placement in &broker.topology.partitions {
                let topic = self
                    .topics
                    .get(&placement.topic)
                    .ok_or_else(|| invalid(&path, "placement refers to an unknown topic"))?;
                require(
                    placement.partition < topic.partitions
                        && local.iter().any(|shard| shard.id == placement.shard)
                        && overrides.insert((&placement.topic, placement.partition)),
                    &path,
                    "invalid or duplicate partition placement",
                )?;
            }
            // One accepted APPEND needs receive backing and two canonical
            // buffers while its destination admits work. Leave one
            // further body for framing and in-flight settlement.
            let max_append = self
                .topics
                .values()
                .map(|t| t.max_append_bytes)
                .max()
                .unwrap();
            for shard in &local {
                let device = &broker.devices[&shard.device];
                require(
                    max_append
                        .checked_mul(4)
                        .is_some_and(|bytes| shard.budget.resident_bytes >= bytes),
                    &path,
                    "shard byte budget must cover APPEND receive and preparation backing",
                )?;
                require(
                    device.workers.queued_bytes >= max_append,
                    &path,
                    "device byte budget must hold a maximum APPEND",
                )?;
            }
            shards.insert(name.clone(), local);
        }
        Ok(ValidatedDeployment {
            deployment: self,
            shards,
        })
    }
}

impl ValidatedDeployment {
    /// Immutable source settings. Changes require validation again.
    pub fn deployment(&self) -> &Deployment {
        &self.deployment
    }

    /// Stable keyed partition selection in numeric partition order.
    pub fn partition_for_key(&self, topic: &str, key: &[u8]) -> Result<u32, ConfigError> {
        let config = self
            .deployment
            .topics
            .get(topic)
            .ok_or_else(|| invalid("topic", "unknown topic"))?;
        Ok(
            (xxhash_rust::xxh3::xxh3_64_with_seed(key, config.partitioner_seed)
                % u64::from(config.partitions)) as u32,
        )
    }
}

fn validate_topics(config: &Deployment, ids: &mut BTreeSet<Uuid>) -> Result<(), ConfigError> {
    let mut partitions = 0_usize;
    for (name, topic) in &config.topics {
        safe_name(name, "topics")?;
        let path = format!("topics.{name}");
        optional_id(topic.id, ids, &path)?;
        partitions = partitions
            .checked_add(topic.partitions as usize)
            .ok_or_else(|| invalid(&path, "partition count overflow"))?;
        require(
            topic.partitions > 0 && partitions <= config.limits.max_partitions,
            &path,
            "partition count must be nonzero and fit aggregate max_partitions",
        )?;
        require(
            matches!(
                (config.cluster.mode, topic.confirmation),
                (DeploymentMode::Single, Confirmation::LocalDurable)
                    | (
                        DeploymentMode::Three,
                        Confirmation::DiskQuorum | Confirmation::ReplicatedPersisting
                    )
            ),
            &path,
            "confirmation policy is incompatible with deployment mode",
        )?;
        require(
            topic.max_append_bytes > 0 && topic.max_append_bytes <= MAX_APPEND_BYTES,
            &path,
            "max_append_bytes must be at most 8 MiB",
        )?;
        require(
            topic.segment_bytes >= 1024 * 1024
                && topic.segment_bytes % 4096 == 0
                && topic.max_append_bytes <= topic.segment_bytes / 2,
            &path,
            "segment must be aligned and leave framing space beyond max_append_bytes",
        )?;
        require(
            topic
                .retention
                .max_age_secs
                .is_none_or(|age| age > 0 && age.checked_mul(1000).is_some()),
            &format!("{path}.retention.max_age_secs"),
            "age must be positive and fit milliseconds",
        )?;
        require(
            topic
                .retention
                .max_bytes
                .is_none_or(|bytes| bytes >= topic.segment_bytes),
            &format!("{path}.retention.max_bytes"),
            "byte target must fit at least one segment",
        )?;
    }
    Ok(())
}

fn validate_devices(broker: &Broker, path: &str) -> Result<(), ConfigError> {
    require(
        !broker.devices.is_empty(),
        path,
        "at least one device required",
    )?;
    let mut roots: Vec<&Path> = Vec::new();
    let mut controllers = BTreeMap::new();
    for (name, device) in &broker.devices {
        safe_name(name, path)?;
        safe_name(&device.controller, path)?;
        let device_path = format!("{path}.devices.{name}");
        safe_root(&device.root, &device_path)?;
        require(
            roots
                .iter()
                .all(|root| !root.starts_with(&device.root) && !device.root.starts_with(root)),
            &device_path,
            "device roots overlap",
        )?;
        roots.push(&device.root);
        let workers = &device.workers;
        require(
            (1..=64).contains(&workers.write_threads)
                && (workers.cpus.is_empty() || workers.cpus.len() == workers.write_threads)
                && workers.cpus.iter().collect::<BTreeSet<_>>().len() == workers.cpus.len()
                && (workers.backend == crate::IoBackend::Aio || workers.aio_cpu.is_none())
                && (1..=64).contains(&workers.aio_depth)
                && workers.max_inflight >= workers.aio_depth
                && workers.max_inflight <= workers.queued_jobs
                && workers.queued_jobs > 0
                && workers.queued_bytes > 0
                && workers.progress_jobs > 0
                && workers.progress_bytes > 0
                && workers.open_handles > 0
                && workers
                    .queued_jobs
                    .checked_add(workers.progress_jobs)
                    .is_some()
                && workers
                    .queued_bytes
                    .checked_add(workers.progress_bytes)
                    .is_some(),
            &device_path,
            "invalid storage worker count, affinity or admission budget",
        )?;
        if let Some(previous) = controllers.insert(&device.controller, workers) {
            require(
                previous == workers,
                &device_path,
                "devices sharing a controller must share worker settings",
            )?;
        }
    }
    Ok(())
}

fn validate_broker(broker: &Broker, path: &str) -> Result<Vec<Shard>, ConfigError> {
    validate_devices(broker, path)?;
    require(
        (1..=256).contains(&broker.topology.omq.io_threads),
        path,
        "OMQ io_threads must be between 1 and 256",
    )?;
    let mut shards = broker.topology.shards.clone();
    if shards.is_empty() {
        require(
            broker.devices.len() == 1,
            path,
            "multiple devices require explicit shard-to-device assignments",
        )?;
        shards.push(Shard {
            id: 0,
            device: broker.devices.keys().next().unwrap().clone(),
            affinity: crate::Affinity::default(),
            budget: QueueBudget::default(),
        });
    }
    let mut ids = BTreeSet::new();
    for shard in &shards {
        require(
            ids.insert(shard.id) && broker.devices.contains_key(&shard.device),
            path,
            "duplicate shard ID or unknown shard device",
        )?;
        let budget = &shard.budget;
        require(
            budget.append_slots > 0
                && budget.control_slots > 0
                && budget.resident_bytes > 0
                && budget.control_bytes > 0
                && budget
                    .append_slots
                    .checked_add(budget.control_slots)
                    .is_some()
                && budget
                    .resident_bytes
                    .checked_add(budget.control_bytes)
                    .is_some(),
            path,
            "invalid shard data/control budget",
        )?;
        require(
            shard.affinity.numa_node.is_none() || shard.affinity.cpu.is_some(),
            path,
            "NUMA placement requires an explicit CPU",
        )?;
    }
    shards.sort_by_key(|shard| shard.id);
    let mut pools = BTreeMap::new();
    for pool in &broker.topology.memory_pools {
        require(
            pool.bytes > 0 && pools.insert(pool.numa_node, pool.bytes).is_none(),
            path,
            "duplicate or empty NUMA memory pool",
        )?;
    }
    let mut reserved = BTreeMap::<u32, u64>::new();
    for shard in &shards {
        if let Some(node) = shard.affinity.numa_node {
            let total = reserved.entry(node).or_default();
            *total = total
                .checked_add(shard.budget.resident_bytes + shard.budget.control_bytes)
                .ok_or_else(|| invalid(path, "NUMA memory reservation overflow"))?;
            require(
                pools.get(&node).is_some_and(|bytes| bytes >= total),
                path,
                "NUMA memory pool does not cover shard reservations",
            )?;
        }
    }
    Ok(shards)
}

fn validate_endpoint(endpoint: &str, path: &str) -> Result<(), ConfigError> {
    require(
        endpoint.len() <= 1024,
        path,
        "endpoint exceeds protocol limit",
    )?;
    require(
        !endpoint
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control()),
        path,
        "endpoint contains whitespace or control bytes",
    )?;
    if let Some(address) = endpoint.strip_prefix("tcp://") {
        if let Ok(address) = address.parse::<SocketAddr>() {
            return require(
                address.port() > 0
                    && !address.ip().is_unspecified()
                    && !address.ip().is_multicast(),
                path,
                "TCP endpoint must be connectable and use a fixed nonzero port",
            );
        }
        let (host, port) = address
            .rsplit_once(':')
            .ok_or_else(|| invalid(path, "TCP endpoint requires host:port"))?;
        return require(
            !host.is_empty()
                && host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
                && port.parse::<u16>().is_ok_and(|port| port > 0),
            path,
            "invalid TCP host or port",
        );
    }
    if let Some(name) = endpoint.strip_prefix("inproc://") {
        return require(!name.is_empty(), path, "empty inproc endpoint");
    }
    if let Some(name) = endpoint.strip_prefix("ipc://") {
        return require(
            name.starts_with('/') || (name.starts_with('@') && name.len() > 1),
            path,
            "IPC endpoint requires an absolute path or abstract name",
        );
    }
    Err(invalid(path, "unsupported endpoint transport"))
}

fn safe_root(root: &Path, path: &str) -> Result<(), ConfigError> {
    let value = root
        .to_str()
        .ok_or_else(|| invalid(path, "storage root must be UTF-8"))?;
    require(
        root.is_absolute()
            && value != "/"
            && !value
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_control())
            && value[1..]
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != ".."),
        path,
        "storage root must be an absolute normalized path below filesystem root",
    )
}

pub(crate) fn safe_name(name: &str, path: &str) -> Result<(), ConfigError> {
    require(
        (1..=128).contains(&name.len())
            && name.as_bytes()[0].is_ascii_alphanumeric()
            && name.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
            }),
        path,
        "names must be lowercase ASCII path components, 1..128 bytes, starting with a letter or digit",
    )
}

pub(crate) fn optional_id(
    id: Option<Uuid>,
    used: &mut BTreeSet<Uuid>,
    path: &str,
) -> Result<(), ConfigError> {
    if let Some(id) = id {
        require(
            !id.is_nil() && used.insert(id),
            path,
            "zero or reused persistent identity",
        )?;
    }
    Ok(())
}

pub(crate) fn require(condition: bool, path: &str, reason: &str) -> Result<(), ConfigError> {
    if condition {
        Ok(())
    } else {
        Err(invalid(path, reason))
    }
}
