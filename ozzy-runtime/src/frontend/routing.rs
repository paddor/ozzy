use std::collections::{BTreeMap, BTreeSet};

use omq_tokio::Message;
use ozzy_proto::{
    EnvelopeLimits, GroupId, LinkSessionId, NodeId, Opcode, PartitionIncarnation, ProducerId,
    append, decode_packet, producer, reader,
};
use ozzy_replication::wire;

use crate::dispatch::Class;

/// Local placement selected by deployment configuration, never by a client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Placement {
    /// Stable partition replication/local-durability group.
    pub group: GroupId,
    /// Exact partition incarnation, shared by all writers.
    pub partition: PartitionIncarnation,
    /// Broker-local application shard; has no meaning on another broker.
    pub shard: u32,
}

/// Role of an independently authenticated or explicitly trusted connection.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Kind {
    /// SDK writer/reader multiplexed over one broker connection.
    Client,
    /// Configured remote broker. Actors independently check group membership.
    Broker,
}

/// Trusted link state supplied by the session owner. Never construct this from
/// claimed sender or routing bytes alone. A routing ID is not authentication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Binding {
    /// Verified peer identity associated with this link.
    pub peer: NodeId,
    /// Current established session; replacement fences every prior session.
    pub session: LinkSessionId,
    /// Role authorized by the connection owner.
    pub kind: Kind,
}

/// Metadata-only dispatch result. It neither consumes credit nor accepts work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Routed {
    /// Configured destination of this partition on the receiving broker.
    pub placement: Placement,
    /// Queue class that must back admission before enqueue.
    pub class: Class,
    /// APPEND's logical writer for independent per-writer admission limits.
    pub writer: Option<ProducerId>,
}

/// Immutable per-broker placement. Connections can target every entry; no
/// accept-time shard affinity or special handling for shard zero exists here.
#[derive(Debug)]
pub struct RoutingTable {
    pub(super) partitions: BTreeMap<GroupId, Placement>,
    pub(super) shards: BTreeSet<u32>,
    pub(super) limits: EnvelopeLimits,
}

impl RoutingTable {
    /// Build from bounded deployment metadata. Duplicate groups/incarnations,
    /// unknown shards, and zero identities are refused before traffic starts.
    pub fn new(
        shards: &[u32],
        partitions: &[Placement],
        maximum: usize,
        limits: EnvelopeLimits,
    ) -> Result<Self, RoutingError> {
        let shard_set: BTreeSet<_> = shards.iter().copied().collect();
        if maximum == 0
            || partitions.len() > maximum
            || shards.is_empty()
            || shard_set.len() != shards.len()
            || limits.max_metadata_bytes == 0
            || limits.max_payload_bytes == 0
        {
            return Err(RoutingError::Configuration);
        }
        let mut table = BTreeMap::new();
        let mut incarnations = BTreeSet::new();
        for &placement in partitions {
            if placement.group.as_bytes() == &[0; 16]
                || placement.partition.as_bytes() == &[0; 16]
                || !shard_set.contains(&placement.shard)
                || table.insert(placement.group, placement).is_some()
                || !incarnations.insert(placement.partition)
            {
                return Err(RoutingError::Configuration);
            }
        }
        Ok(Self {
            partitions: table,
            shards: shard_set,
            limits,
        })
    }

    /// Inspect the ordinary PEER routing frame and three native frames without
    /// copying or scanning record payloads. Link negotiation runs before this
    /// established-session path. The original message remains with its caller.
    pub fn route(&self, message: &Message, binding: Binding) -> Result<Routed, RoutingError> {
        if message.len() == 3
            && binding.kind == Kind::Broker
            && message.part_slice(0) == Some(binding.peer.as_bytes().as_slice())
        {
            let group = GroupId::from_bytes(
                message
                    .part_slice(1)
                    .and_then(|p| p.try_into().ok())
                    .ok_or(RoutingError::Partition)?,
            );
            wire::CompactState::decode(message.part_slice(2).unwrap_or_default())?;
            let placement = *self.partitions.get(&group).ok_or(RoutingError::Partition)?;
            return Ok(Routed {
                placement,
                class: Class::Control,
                writer: None,
            });
        }
        if message.len() != 4 || message.part_slice(0) != Some(binding.peer.as_bytes().as_slice()) {
            return Err(RoutingError::Peer);
        }
        let frames = std::array::from_fn::<_, 3, _>(|index| {
            message.part_slice(index + 1).expect("checked frame count")
        });
        let packet = decode_packet(&frames, self.limits)?;
        if packet.envelope.sender != binding.peer
            || packet.envelope.session != Some(binding.session)
        {
            return Err(RoutingError::Session);
        }
        let (group, partition, class, writer) = match binding.kind {
            Kind::Broker => {
                // Credited prepares replace the old, uncredited PREPARE path.
                if packet.envelope.opcode == Opcode::Prepare {
                    return Err(RoutingError::Command);
                }
                let scope = wire::route(packet, self.limits)?;
                let class = if matches!(packet.envelope.opcode, Opcode::PrepareFlow | Opcode::Ops) {
                    Class::Data
                } else {
                    Class::Control
                };
                (scope.group_id, None, class, None)
            }
            Kind::Client if packet.envelope.opcode == Opcode::Append => {
                let target = append::route(packet, self.limits)?;
                (
                    target.authority.group_id,
                    Some(target.partition),
                    Class::Data,
                    Some(target.key.producer_id),
                )
            }
            Kind::Client if packet.envelope.opcode == Opcode::OpenProducer => {
                let target = producer::decode_open(packet, self.limits)?;
                (
                    target.authority.group_id,
                    Some(target.partition),
                    Class::Control,
                    None,
                )
            }
            Kind::Client => {
                if packet.envelope.response
                    || !matches!(
                        packet.envelope.opcode,
                        Opcode::Subscribe | Opcode::Credit | Opcode::Ack | Opcode::Unsubscribe
                    )
                {
                    return Err(RoutingError::Command);
                }
                let reader::Source::Group {
                    authority,
                    partition,
                    ..
                } = reader::route(packet, self.limits)?
                else {
                    return Err(RoutingError::Command);
                };
                (authority.group_id, Some(partition), Class::Control, None)
            }
        };
        let placement = *self.partitions.get(&group).ok_or(RoutingError::Partition)?;
        if partition.is_some_and(|id| id != placement.partition) {
            return Err(RoutingError::Partition);
        }
        Ok(Routed {
            placement,
            class,
            writer,
        })
    }
}

/// Routing rejected before consuming a grant or reaching an application actor.
#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    /// Invalid deployment route table or configured framing limits.
    #[error("invalid frontend routing configuration")]
    Configuration,
    /// Routing metadata does not name the supplied trusted peer binding.
    #[error("frontend peer binding mismatch")]
    Peer,
    /// Claimed sender or link session is obsolete or belongs to another peer.
    #[error("frontend link session mismatch")]
    Session,
    /// Command is not supported for this connection role/direction.
    #[error("unsupported frontend command")]
    Command,
    /// Unknown group or another partition incarnation.
    #[error("unknown frontend partition")]
    Partition,
    /// Invalid or excessive native framing.
    #[error(transparent)]
    Envelope(#[from] ozzy_proto::EnvelopeError),
    /// Invalid fixed SDK routing metadata.
    #[error(transparent)]
    Client(#[from] ozzy_proto::data::CodecError),
    /// Invalid fixed broker routing metadata.
    #[error(transparent)]
    Broker(#[from] wire::WireError),
}

#[cfg(test)]
mod tests;
