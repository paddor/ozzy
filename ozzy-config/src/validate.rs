mod broker;

use ozzy_proto::directory::EndpointScheme;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use uuid::Uuid;

use crate::{
    Broker, ConfigError, Deployment, DeploymentMode, Shard, invalid,
    schema::{MAX_APPEND_BYTES, MAX_CLIENTS, MAX_DELAY_ENTRIES, MAX_PARTITION_SEGMENTS},
};
use broker::validate_broker;

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
        self.check_counts()?;
        let mut ids = BTreeSet::new();
        optional_id(self.cluster.id, &mut ids, "cluster.id")?;
        validate_topics(&self, &mut ids)?;
        let max_append = self
            .topics
            .values()
            .map(|t| t.max_append_bytes)
            .max()
            .unwrap_or(0);
        let mut endpoints = BTreeSet::new();
        let mut shards = BTreeMap::new();
        for (name, broker) in &self.brokers {
            safe_name(name, "brokers")?;
            let path = format!("brokers.{name}");
            optional_id(broker.id, &mut ids, &path)?;
            validate_endpoints(broker, &mut endpoints, &path)?;
            let local = validate_broker(broker, &path)?;
            self.validate_overrides(broker, &local, &path)?;
            check_append_backing(broker, &local, max_append, &path)?;
            shards.insert(name.clone(), local);
        }
        // Each broker receives every client and broker socket through one
        // buffer profile. The plain admin endpoint has its own.
        let compressed = endpoints
            .iter()
            .filter(|endpoint| {
                EndpointScheme::parse(endpoint).is_some_and(|(scheme, _)| scheme.compressed())
            })
            .count();
        let admins = self
            .brokers
            .values()
            .filter(|broker| broker.endpoints.admin.is_some())
            .count();
        require(
            compressed == 0 || compressed + admins == endpoints.len(),
            "brokers",
            "compressed endpoints cannot mix with plain transports",
        )?;
        Ok(ValidatedDeployment {
            deployment: self,
            shards,
        })
    }

    /// Broker membership, resource limits and topic count.
    fn check_counts(&self) -> Result<(), ConfigError> {
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
            self.limits.max_topics > 0
                && self.limits.max_partitions > 0
                && (1..=MAX_CLIENTS).contains(&self.limits.max_clients),
            "limits",
            "resource limits must be positive; max_clients at most 4096",
        )?;
        require(
            !self.topics.is_empty() && self.topics.len() <= self.limits.max_topics,
            "topics",
            "topic count must be nonzero and fit max_topics",
        )
    }

    fn validate_overrides(
        &self,
        broker: &Broker,
        local: &[Shard],
        path: &str,
    ) -> Result<(), ConfigError> {
        let mut overrides = BTreeSet::new();
        for placement in &broker.topology.partitions {
            let topic = self
                .topics
                .get(&placement.topic)
                .ok_or_else(|| invalid(path, "placement refers to an unknown topic"))?;
            require(
                placement.partition < topic.partitions
                    && local.iter().any(|shard| shard.id == placement.shard)
                    && overrides.insert((&placement.topic, placement.partition)),
                path,
                "invalid or duplicate partition placement",
            )?;
        }
        Ok(())
    }
}

/// Each endpoint is valid and used once across the deployment.
fn validate_endpoints(
    broker: &Broker,
    endpoints: &mut BTreeSet<String>,
    path: &str,
) -> Result<(), ConfigError> {
    for endpoint in [
        Some(&broker.endpoints.peer),
        Some(&broker.endpoints.data_peer),
        Some(&broker.endpoints.reader_pub),
        broker.endpoints.follower_pub.as_ref(),
        broker.endpoints.admin.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        validate_endpoint(endpoint, path)?;
        require(
            endpoints.insert(endpoint.clone()),
            path,
            "endpoint reused within deployment",
        )?;
    }
    // TODO(admin-auth): accept TCP once admins authenticate with CURVE keys.
    require(
        broker.endpoints.admin.as_deref().is_none_or(|endpoint| {
            matches!(
                EndpointScheme::parse(endpoint),
                Some((EndpointScheme::Ipc | EndpointScheme::Inproc, _))
            )
        }),
        path,
        "admin endpoint must use ipc:// or inproc://",
    )
}

/// One accepted APPEND needs receive backing and two canonical buffers while
/// its destination admits work. Leave one further body for framing and
/// in-flight settlement.
fn check_append_backing(
    broker: &Broker,
    local: &[Shard],
    max_append: u64,
    path: &str,
) -> Result<(), ConfigError> {
    for shard in local {
        let device = broker.device(&shard.device)?;
        require(
            max_append
                .checked_mul(4)
                .is_some_and(|bytes| shard.budget.resident_bytes >= bytes),
            path,
            "shard byte budget must cover APPEND receive and preparation backing",
        )?;
        require(
            device.workers.queued_bytes >= max_append,
            path,
            "device byte budget must hold a maximum APPEND",
        )?;
    }
    Ok(())
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
            topic.retention.max_bytes.is_none_or(|bytes| {
                bytes >= topic.segment_bytes
                    && bytes
                        <= topic
                            .segment_bytes
                            .saturating_mul(MAX_PARTITION_SEGMENTS as u64 / 8 * 7)
            }),
            &format!("{path}.retention.max_bytes"),
            "byte target must fit one segment and at most seven eighths of the segment limit",
        )?;
        require(
            topic.producer_idle_secs > 0 && topic.producer_idle_secs.checked_mul(1000).is_some(),
            &format!("{path}.producer_idle_secs"),
            "idle expiry must be positive and fit milliseconds",
        )?;
        if let Some(policy) = topic.deduplication {
            validate_deduplication(policy, &path)?;
        }
        if let Some(delay) = topic.delay {
            validate_delay(topic, delay, &path)?;
        }
    }
    Ok(())
}

fn validate_delay(
    topic: &crate::Topic,
    delay: crate::TopicDelay,
    path: &str,
) -> Result<(), ConfigError> {
    let path = format!("{path}.delay");
    require(
        topic.deduplication.is_none(),
        &path,
        "delay cannot be combined with deduplication",
    )?;
    require(
        delay.max_millis().is_some(),
        &format!("{path}.max_secs"),
        "maximum delay must be positive and fit milliseconds",
    )?;
    require(
        delay.max_entries > 0 && delay.max_entries <= MAX_DELAY_ENTRIES,
        &format!("{path}.max_entries"),
        "held record limit must be positive and at most 4294967296",
    )
}

fn validate_deduplication(
    policy: crate::TopicDeduplication,
    path: &str,
) -> Result<(), ConfigError> {
    require(
        policy.window_millis > 0,
        &format!("{path}.deduplication.window_millis"),
        "window must be positive",
    )?;
    require(
        policy.max_entries > 0
            && usize::try_from(policy.max_entries).is_ok()
            && isize::try_from(policy.max_bytes).is_ok()
            && policy
                .max_entries
                .checked_mul(32)
                .is_some_and(|minimum| minimum <= policy.max_bytes),
        &format!("{path}.deduplication"),
        "positive entry and byte bounds must fit the native index and its encoded identity rows",
    )
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
    match EndpointScheme::parse(endpoint) {
        Some((scheme, address)) if scheme.is_tcp() => {
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
            require(
                !host.is_empty()
                    && host
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-')
                    && port.parse::<u16>().is_ok_and(|port| port > 0),
                path,
                "invalid TCP host or port",
            )
        }
        Some((EndpointScheme::Inproc, name)) => {
            require(!name.is_empty(), path, "empty inproc endpoint")
        }
        Some((EndpointScheme::Ipc, name)) => require(
            name.starts_with('/') || (name.starts_with('@') && name.len() > 1),
            path,
            "IPC endpoint requires an absolute path or abstract name",
        ),
        _ => Err(invalid(path, "unsupported endpoint transport")),
    }
}

pub(crate) fn safe_name(name: &str, path: &str) -> Result<(), ConfigError> {
    require(
        (1..=128).contains(&name.len())
            && name
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
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
