//! Paged topic metadata. No broker-local shard route appears on this wire.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use crate::{
    ENVELOPE_BYTES, Envelope, EnvelopeLimits, GroupId, NodeId, Opcode, Packet,
    PartitionIncarnation, TopicId,
    append::Policy,
    data::{CodecError, Cursor},
};

use super::{Limits, control, prepare};

const TOPIC_TAG: u8 = 0;
const MAX_NAME: usize = 128;
const MAX_ENDPOINT: usize = 1024;
const PARTITION_BASE: usize = 45;

/// Fetch one bounded page. `first` is the numeric partition, not a group ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicRequest {
    /// Configured topic name.
    pub name: String,
    /// First numeric partition requested.
    pub first: u32,
    /// Maximum entries in one reply, capped again by local codec limits.
    pub maximum: u16,
}

/// Explicit broker addresses, independent of each broker's shard topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerEndpoint {
    /// Configured broker identity.
    pub node: NodeId,
    /// Shared PEER endpoint for writer, reader control, and broker traffic.
    pub peer: String,
    /// Live reader PUB endpoint.
    pub reader_pub: String,
    /// Optional follower publication endpoint.
    pub follower_pub: Option<String>,
}

/// One immutable partition identity in numeric order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPartition {
    /// Numeric topic partition, shared by every writer.
    pub number: u32,
    /// Fixed group identity.
    pub group: GroupId,
    /// Persistent configuration epoch for this ordered membership.
    pub config_epoch: u64,
    /// Fixed partition incarnation.
    pub incarnation: PartitionIncarnation,
    /// Ordered broker membership. The initial leader is first; subsequent
    /// leaders follow election state, never this static placement hint.
    pub members: Vec<NodeId>,
}

/// One page of immutable topic metadata. A client compares topic ID, hash
/// parameters, policy, broker endpoints, total, and page range across replies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPage {
    /// Persistent topic identity.
    pub id: TopicId,
    /// Configured topic name.
    pub name: String,
    /// Seed for fixed XXH3-64 modulo partition count in numeric order.
    pub partitioner_seed: u64,
    /// Fixed count. More broker CPUs do not change this value.
    pub total: u32,
    /// Confirmation boundary shared by this topic's partitions.
    pub policy: Policy,
    /// Current configured broker endpoints, each exactly once.
    pub brokers: Vec<BrokerEndpoint>,
    /// First partition number in this page.
    pub first: u32,
    /// Nonempty contiguous page.
    pub partitions: Vec<TopicPartition>,
}

impl TopicPage {
    /// Check complete identity, placement, and endpoint structure.
    pub fn valid(&self, limits: Limits) -> bool {
        if !limits.valid()
            || self.id.as_bytes() == &[0; 16]
            || !name_valid(&self.name)
            || self.total == 0
            || self.partitions.is_empty()
            || self.partitions.len() > limits.partitions
            || !matches!(self.brokers.len(), 1 | 3)
            || self.brokers.len() > limits.members
            || self
                .first
                .checked_add(u32::try_from(self.partitions.len()).unwrap_or(u32::MAX))
                .is_none_or(|end| end > self.total)
            || !matches!(
                (self.brokers.len(), self.policy),
                (1, Policy::LocalDurable)
                    | (
                        3,
                        Policy::QuorumDurable | Policy::QuorumReplicatedPersisting
                    )
            )
        {
            return false;
        }
        let mut brokers = BTreeSet::new();
        for broker in &self.brokers {
            if broker.node.as_bytes() == &[0; 16]
                || !brokers.insert(broker.node)
                || !endpoint_valid(&broker.peer)
                || !endpoint_valid(&broker.reader_pub)
                || broker
                    .follower_pub
                    .as_ref()
                    .is_some_and(|value| !endpoint_valid(value))
            {
                return false;
            }
        }
        let mut groups = BTreeSet::new();
        let mut incarnations = BTreeSet::new();
        for (index, partition) in self.partitions.iter().enumerate() {
            if partition.number != self.first + index as u32
                || partition.group.as_bytes() == &[0; 16]
                || partition.config_epoch == 0
                || partition.incarnation.as_bytes() == &[0; 16]
                || !groups.insert(partition.group)
                || !incarnations.insert(partition.incarnation)
                || partition.members.len() != brokers.len()
                || partition.members.iter().copied().collect::<BTreeSet<_>>() != brokers
            {
                return false;
            }
        }
        true
    }

    /// Exact metadata bytes before caller allocation. Encoding still validates
    /// all identities and the directional envelope limit.
    pub fn metadata_bytes(&self) -> Result<usize, CodecError> {
        let mut size = 39_usize
            .checked_add(self.name.len())
            .ok_or(CodecError::Length)?;
        for broker in &self.brokers {
            size = size
                .checked_add(16 + 2 + broker.peer.len() + 2 + broker.reader_pub.len() + 2)
                .and_then(|value| {
                    value.checked_add(broker.follower_pub.as_ref().map_or(0, String::len))
                })
                .ok_or(CodecError::Length)?;
        }
        for partition in &self.partitions {
            size = size
                .checked_add(
                    partition
                        .members
                        .len()
                        .checked_mul(16)
                        .and_then(|members| PARTITION_BASE.checked_add(members))
                        .ok_or(CodecError::Length)?,
                )
                .ok_or(CodecError::Length)?;
        }
        Ok(size)
    }
}

/// Encode a topic page request into caller-reserved metadata.
pub fn encode_topic_request(
    envelope: Envelope,
    request: &TopicRequest,
    out: &mut Vec<u8>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    if !limits.valid()
        || !name_valid(&request.name)
        || request.maximum == 0
        || usize::from(request.maximum) > limits.partitions
    {
        return Err(CodecError::Profile);
    }
    let header = prepare(
        envelope,
        Opcode::StateSnapshotRequest,
        true,
        8 + request.name.len(),
        out,
        frame,
    )?;
    out.push(TOPIC_TAG);
    write_name(out, &request.name);
    out.extend_from_slice(&request.first.to_be_bytes());
    out.extend_from_slice(&request.maximum.to_be_bytes());
    Ok(header)
}

/// Decode a topic page request with fixed name and page bounds.
pub fn decode_topic_request(
    packet: Packet<'_>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<TopicRequest, CodecError> {
    let mut cursor = control(packet, Opcode::StateSnapshotRequest, true, frame, limits)?;
    if cursor.byte()? != TOPIC_TAG {
        return Err(CodecError::Profile);
    }
    let name = read_name(&mut cursor)?;
    let first = cursor.u32()?;
    let maximum = u16::from_be_bytes(cursor.array()?);
    if !cursor.0.is_empty() || maximum == 0 || usize::from(maximum) > limits.partitions {
        return Err(CodecError::Profile);
    }
    Ok(TopicRequest {
        name,
        first,
        maximum,
    })
}

/// Encode a current topic page. One reply contains no partial partition entry.
pub fn encode_topic_page(
    envelope: Envelope,
    page: &TopicPage,
    out: &mut Vec<u8>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    if !page.valid(limits) {
        return Err(CodecError::Profile);
    }
    let header = prepare(
        envelope,
        Opcode::StateSnapshot,
        true,
        page.metadata_bytes()?,
        out,
        frame,
    )?;
    out.push(TOPIC_TAG);
    out.extend_from_slice(page.id.as_bytes());
    write_name(out, &page.name);
    out.push(1); // XXH3-64, numeric order, modulo fixed partition count.
    out.extend_from_slice(&page.partitioner_seed.to_be_bytes());
    out.extend_from_slice(&page.total.to_be_bytes());
    out.extend_from_slice(&page.first.to_be_bytes());
    out.push(page.policy as u8);
    out.push(page.brokers.len() as u8);
    for broker in &page.brokers {
        out.extend_from_slice(broker.node.as_bytes());
        write_endpoint(out, &broker.peer);
        write_endpoint(out, &broker.reader_pub);
        write_endpoint(out, broker.follower_pub.as_deref().unwrap_or(""));
    }
    out.extend_from_slice(&(page.partitions.len() as u16).to_be_bytes());
    for partition in &page.partitions {
        out.extend_from_slice(&partition.number.to_be_bytes());
        out.extend_from_slice(partition.group.as_bytes());
        out.extend_from_slice(&partition.config_epoch.to_be_bytes());
        out.extend_from_slice(partition.incarnation.as_bytes());
        out.push(partition.members.len() as u8);
        for member in &partition.members {
            out.extend_from_slice(member.as_bytes());
        }
    }
    Ok(header)
}

/// Decode and validate one page before exposing its broker endpoints or group
/// membership to the SDK. A caller must compare repeated page metadata.
pub fn decode_topic_page(
    packet: Packet<'_>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<TopicPage, CodecError> {
    let mut cursor = control(packet, Opcode::StateSnapshot, true, frame, limits)?;
    if cursor.byte()? != TOPIC_TAG {
        return Err(CodecError::Profile);
    }
    let id = TopicId::from_bytes(cursor.array()?);
    let name = read_name(&mut cursor)?;
    if cursor.byte()? != 1 {
        return Err(CodecError::Profile);
    }
    let partitioner_seed = cursor.u64()?;
    let total = cursor.u32()?;
    let first = cursor.u32()?;
    let policy = Policy::try_from(cursor.byte()?)?;
    let broker_count = usize::from(cursor.byte()?);
    if !matches!(broker_count, 1 | 3) || broker_count > limits.members {
        return Err(CodecError::Limit);
    }
    let mut brokers = Vec::with_capacity(broker_count);
    for _ in 0..broker_count {
        let node = NodeId::from_bytes(cursor.array()?);
        let peer = read_endpoint(&mut cursor)?.ok_or(CodecError::Profile)?;
        let reader_pub = read_endpoint(&mut cursor)?.ok_or(CodecError::Profile)?;
        let follower_pub = read_endpoint(&mut cursor)?;
        brokers.push(BrokerEndpoint {
            node,
            peer,
            reader_pub,
            follower_pub,
        });
    }
    let count = usize::from(u16::from_be_bytes(cursor.array()?));
    if count == 0 || count > limits.partitions || cursor.0.len() < PARTITION_BASE * count {
        return Err(CodecError::Limit);
    }
    let mut partitions = Vec::with_capacity(count);
    for _ in 0..count {
        let number = cursor.u32()?;
        let group = GroupId::from_bytes(cursor.array()?);
        let config_epoch = cursor.u64()?;
        let incarnation = PartitionIncarnation::from_bytes(cursor.array()?);
        let members = usize::from(cursor.byte()?);
        if members != broker_count {
            return Err(CodecError::Profile);
        }
        let mut ordered = Vec::with_capacity(members);
        for _ in 0..members {
            ordered.push(NodeId::from_bytes(cursor.array()?));
        }
        partitions.push(TopicPartition {
            number,
            group,
            config_epoch,
            incarnation,
            members: ordered,
        });
    }
    if !cursor.0.is_empty() {
        return Err(CodecError::Length);
    }
    let page = TopicPage {
        id,
        name,
        partitioner_seed,
        total,
        policy,
        brokers,
        first,
        partitions,
    };
    if !page.valid(limits) {
        return Err(CodecError::Profile);
    }
    Ok(page)
}

fn name_valid(name: &str) -> bool {
    (1..=MAX_NAME).contains(&name.len())
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
        })
}

fn endpoint_valid(endpoint: &str) -> bool {
    if !(1..=MAX_ENDPOINT).contains(&endpoint.len())
        || endpoint
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return false;
    }
    if let Some(address) = endpoint.strip_prefix("tcp://") {
        if let Ok(address) = address.parse::<SocketAddr>() {
            return address.port() > 0
                && !address.ip().is_unspecified()
                && !address.ip().is_multicast();
        }
        return address.rsplit_once(':').is_some_and(|(host, port)| {
            !host.is_empty()
                && host
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
                && port.parse::<u16>().is_ok_and(|port| port > 0)
        });
    }
    if let Some(name) = endpoint.strip_prefix("ipc://") {
        return name.starts_with('/') || (name.starts_with('@') && name.len() > 1);
    }
    endpoint
        .strip_prefix("inproc://")
        .is_some_and(|name| !name.is_empty())
}

fn write_name(out: &mut Vec<u8>, name: &str) {
    out.push(name.len() as u8);
    out.extend_from_slice(name.as_bytes());
}

fn read_name(cursor: &mut Cursor<'_>) -> Result<String, CodecError> {
    let count = usize::from(cursor.byte()?);
    if count == 0 || count > MAX_NAME {
        return Err(CodecError::Profile);
    }
    let name = std::str::from_utf8(cursor.take(count)?).map_err(|_| CodecError::Profile)?;
    if !name_valid(name) {
        return Err(CodecError::Profile);
    }
    Ok(name.to_owned())
}

fn write_endpoint(out: &mut Vec<u8>, endpoint: &str) {
    out.extend_from_slice(&(endpoint.len() as u16).to_be_bytes());
    out.extend_from_slice(endpoint.as_bytes());
}

fn read_endpoint(cursor: &mut Cursor<'_>) -> Result<Option<String>, CodecError> {
    let count = usize::from(u16::from_be_bytes(cursor.array()?));
    if count == 0 {
        return Ok(None);
    }
    if count > MAX_ENDPOINT {
        return Err(CodecError::Limit);
    }
    let endpoint = std::str::from_utf8(cursor.take(count)?).map_err(|_| CodecError::Profile)?;
    if !endpoint_valid(endpoint) {
        return Err(CodecError::Profile);
    }
    Ok(Some(endpoint.to_owned()))
}

#[cfg(test)]
mod tests;
