//! Owner configuration: broker addresses, shared-link bounds, and their
//! validation before any socket opens.

use super::{AppendLinkLimits, BrokerLinkError, ReaderLinkLimits, SdkClock, readers};
use omq_tokio::Endpoint;
use ozzy_proto::{NodeId, handshake};
use std::{collections::BTreeSet, time::Duration};

/// One configured broker address. Shard placement never enters the SDK table.
#[derive(Clone, Debug)]
pub struct BrokerAddress {
    /// Persistent broker identity, independently trusted or authenticated.
    pub node: NodeId,
    /// Shared native PEER endpoint.
    pub endpoint: Endpoint,
    /// Independent APPEND and replay data endpoint.
    pub data_endpoint: Endpoint,
}

/// Fixed shared-link and control-resource bounds for one SDK owner.
#[derive(Clone, Debug)]
pub struct BrokerLinksConfig {
    /// SDK process identity used across its broker connections.
    pub local: NodeId,
    /// One or three brokers, each exactly once.
    pub brokers: Vec<BrokerAddress>,
    /// Shared negotiation profile for writer and reader commands.
    pub parameters: handshake::Parameters,
    /// Aggregate control-frame slots, including unread raw reply aliases.
    /// Decoded reader/processing completions use their owner reservation instead.
    pub requests: usize,
    /// Aggregate control storage, including unused slots and encoding scratch.
    pub control_bytes: usize,
    /// Aggregate cached topic identity, watch registrations, and hints. Zero
    /// disables routing watches. Idle cached interests stay bounded here.
    pub routing_bytes: usize,
    /// Aggregate APPEND transport capacity across logical writers and brokers.
    /// Absent for control-only owners. Writer intake has separate bounds.
    pub append: Option<AppendLinkLimits>,
    /// Bounded shared reader inboxes. Absent for writer-only owners.
    pub reader: Option<ReaderLinkLimits>,
    /// Per-broker partition bound for topic discovery. One lookup attempts
    /// each of the one or three configured brokers within shared admission.
    pub maximum_partitions: usize,
    /// Includes link negotiation and waiting for local control admission.
    pub request_timeout: Duration,
    /// HELLO retry interval while a broker is unavailable.
    pub retry_interval: Duration,
    /// Production monotonic clock or an injected deterministic clock.
    pub clock: SdkClock,
}

impl BrokerLinksConfig {
    /// Storage for every declared control slot and each broker's receive and
    /// encoding scratch. This includes unused capacity. Arithmetic overflow
    /// returns `None`; it does not validate identities or negotiation settings.
    pub fn control_reservation_bytes(&self) -> Option<usize> {
        let (scratch, slot) = control_sizes(self)?;
        self.requests.checked_mul(slot)?.checked_add(scratch)
    }

    /// Validate fixed admission and negotiation without starting an SDK owner
    /// or creating sockets. Smaller byte budgets may admit fewer request slots.
    pub fn validate(&self) -> Result<(), BrokerLinkError> {
        validate(self).map(|_| ())
    }
}

pub(super) fn validate(config: &BrokerLinksConfig) -> Result<usize, BrokerLinkError> {
    config.parameters.validate()?;
    if let Some(append) = config.append {
        append.validate()?;
        if config.parameters.capabilities & handshake::OWNER_STREAM == 0
            || config.parameters.roles & handshake::PRODUCER == 0
        {
            return Err(BrokerLinkError::Configuration);
        }
    }
    let metadata = config.parameters.receive.envelope.max_metadata_bytes;
    let (scratch, slot_bytes) = control_sizes(config).ok_or(BrokerLinkError::Configuration)?;
    let slots = config
        .requests
        .min(config.control_bytes.saturating_sub(scratch) / slot_bytes);
    let mut unique = BTreeSet::new();
    if config.local.as_bytes() == &[0; 16]
        || !matches!(config.brokers.len(), 1 | 3)
        || config.requests > 4096
        || slots < config.brokers.len()
        || !(90..=65536).contains(&metadata)
        || !(1..=65536).contains(&config.maximum_partitions)
        || config.request_timeout.is_zero()
        || config.request_timeout > Duration::from_hours(24)
        || config.retry_interval.is_zero()
        || config.retry_interval > config.request_timeout
        || config.parameters.roles == 0
        || config.parameters.roles & !(handshake::PRODUCER | handshake::CONSUMER) != 0
        || config.brokers.iter().any(|broker| {
            broker.node == config.local
                || broker.node.as_bytes() == &[0; 16]
                || !unique.insert(broker.node)
                || !supported(&broker.endpoint)
                || broker.data_endpoint == broker.endpoint
                || !supported(&broker.data_endpoint)
        })
    {
        return Err(BrokerLinkError::Configuration);
    }
    readers::Admission::capacity(config)?;
    Ok(slots)
}

fn supported(endpoint: &Endpoint) -> bool {
    match endpoint {
        Endpoint::Tcp { .. } | Endpoint::Ipc(_) | Endpoint::Inproc { .. } => true,
        #[cfg(feature = "lz4-transport")]
        Endpoint::Lz4Tcp { .. } => true,
        #[cfg(feature = "zstd-transport")]
        Endpoint::ZstdTcp { .. } => true,
        _ => false,
    }
}

fn control_sizes(config: &BrokerLinksConfig) -> Option<(usize, usize)> {
    let envelope = config.parameters.receive.envelope;
    let scratch = envelope
        .max_metadata_bytes
        .checked_add(envelope.max_payload_bytes)?
        .checked_mul(4)?
        .checked_add(8192)?
        .checked_mul(config.brokers.len())?;
    let slot = envelope
        .max_metadata_bytes
        .checked_mul(2)?
        .checked_add(envelope.max_payload_bytes)?
        .checked_add(8192)?;
    Some((scratch, slot))
}
