//! Admin console requests and replies on a broker's admin endpoint.
//!
//! A client sends one request at a time and matches the reply by its ID.
//! Replies are observations, never authority. Fields are big-endian; a string
//! is a `u16` byte length plus UTF-8, a list a `u32` count plus its items.

use crate::{ConsumerGroupId, ConsumerMemberId, GroupId, NodeId, data::CodecError, layout::Reader};

mod partitions;

use partitions::{decode_status, encode_status};

const ERROR: u8 = 0xff;

/// What a client asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Request {
    /// Static broker facts: names, IDs, partition directory.
    Broker = 1,
    /// Event counters: totals and each thread's values.
    Counters = 2,
    /// Each local partition's replication status and committed state.
    Partitions = 3,
    /// Broker links and connected clients.
    Connections = 4,
}

/// What a broker answers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    /// Answers [`Request::Broker`].
    Broker(Broker),
    /// Answers [`Request::Counters`].
    Counters(Counters),
    /// Answers [`Request::Partitions`].
    Partitions(Partitions),
    /// Answers [`Request::Connections`].
    Connections(Connections),
    /// The broker could not answer, for example an unknown request.
    Error(String),
}

/// Static facts about the answering broker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Broker {
    /// Configured name.
    pub name: String,
    /// Persistent broker ID.
    pub id: NodeId,
    /// Operating system process ID.
    pub pid: u32,
    /// Milliseconds since the broker started.
    pub uptime_ms: u64,
    /// `limits.max_clients`.
    pub max_clients: u32,
    /// Application shard threads.
    pub shards: u32,
    /// Configured brokers in persistent order.
    pub brokers: Vec<(String, NodeId)>,
    /// Partitions this broker stores.
    pub partitions: Vec<PartitionName>,
}

/// Where one partition lives and what it is called.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionName {
    /// Replication group, the key of [`PartitionStatus`].
    pub group: GroupId,
    /// Topic name.
    pub topic: String,
    /// Partition number within the topic.
    pub partition: u32,
    /// Local application shard.
    pub shard: u32,
}

/// How one event's per-thread values combine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum EventKind {
    /// Totals add.
    Count = 0,
    /// Totals keep the largest.
    Peak = 1,
    /// A level; totals add with wrapping.
    Gauge = 2,
}

/// Event counter values at one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counters {
    /// Milliseconds since the broker started, for rates.
    pub uptime_ms: u64,
    /// Event names and kinds. Every value list follows this order.
    pub events: Vec<(String, EventKind)>,
    /// Every thread's values combined, including exited threads.
    pub totals: Vec<u64>,
    /// Each live thread's name and values. Names can repeat.
    pub threads: Vec<(String, Vec<u64>)>,
}

/// Status of the local partitions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Partitions {
    /// Milliseconds since the broker started, when the shards were asked.
    pub uptime_ms: u64,
    /// Shards that did not answer in time; their partitions are missing.
    pub missing_shards: u32,
    /// One row per answered partition.
    pub rows: Vec<PartitionStatus>,
}

/// One partition's status on the answering broker. Positions are canonical
/// operation numbers; only [`PartitionLog`] has record offsets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionStatus {
    /// Replication group.
    pub group: GroupId,
    /// Replication mode and phase.
    pub state: PartitionState,
    /// The partition serves clients.
    pub ready: bool,
    /// A journal action is outstanding.
    pub disk_pending: bool,
    /// Election view.
    pub view: u64,
    /// Best-known leader.
    pub leader: Option<NodeId>,
    /// Highest validated contiguous operation.
    pub accepted: u64,
    /// Highest confirmed operation.
    pub committed: u64,
    /// Highest operation applied to partition state.
    pub applied: u64,
    /// Accepted operations awaiting application.
    pub pending_operations: u64,
    /// Body bytes those operations retain.
    pub pending_body_bytes: u64,
    /// Completed checks of every segment.
    pub validation_cycles: u64,
    /// File bytes removed by cleanup.
    pub reclaimed_bytes: u64,
    /// Highest operation in a fully written journal group.
    pub written: u64,
    /// Highest operation a journal sync covers.
    pub durable: u64,
    /// Highest commit the leader announced, as this broker knows it.
    pub leader_commit: u64,
    /// Leader only: each follower and the highest operation it acknowledged.
    pub followers: Vec<(NodeId, u64)>,
    /// Committed state; absent while the partition's journal is busy.
    pub log: Option<PartitionLog>,
}

/// A partition's committed records and consumer groups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionLog {
    /// Earliest retained record offset.
    pub first_offset: u64,
    /// Offset the next record gets.
    pub next_offset: u64,
    /// Open producer sessions.
    pub producers: u32,
    /// Consumer groups this partition coordinates.
    pub groups: Vec<GroupStatus>,
    /// Consumer groups reading this partition.
    pub cursors: Vec<GroupCursor>,
}

/// A consumer group's committed plan on its coordinator partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupStatus {
    /// Group incarnation; a recreated name gets a new one.
    pub id: ConsumerGroupId,
    /// Group name, unique within the topic.
    pub name: String,
    /// Committed plan revision.
    pub revision: u64,
    /// Members and the client each runs on.
    pub members: Vec<(ConsumerMemberId, NodeId)>,
}

/// A consumer group's position on one partition it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupCursor {
    /// Group incarnation.
    pub group: ConsumerGroupId,
    /// Active owner; absent when revoked or never assigned.
    pub member: Option<ConsumerMemberId>,
    /// Exclusive committed offset; absent before the first progress.
    pub offset: Option<u64>,
}

/// Established links of the answering broker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connections {
    /// Every other configured broker and whether a session is established.
    pub brokers: Vec<(NodeId, bool)>,
    /// Clients with an established session.
    pub clients: u32,
}

/// A partition's replication mode and phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PartitionState {
    /// Single-broker partition.
    Local = 0,
    /// Three-broker partition in normal replication.
    Normal = 1,
    /// Three-broker partition in a view change, installation, or recovery.
    Electing = 2,
}

/// Encode request `id`.
pub fn encode_request(id: u64, request: Request) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(request as u8);
    out.extend_from_slice(&id.to_be_bytes());
    out
}

/// Decode a request. An unknown kind fails with [`CodecError::Command`] and
/// still returns the ID through [`request_id`].
pub fn decode_request(input: &[u8]) -> Result<(u64, Request), CodecError> {
    let mut reader = Reader::new(input);
    let kind = reader.u8()?;
    let id = reader.u64()?;
    reader.finish()?;
    let request = match kind {
        1 => Request::Broker,
        2 => Request::Counters,
        3 => Request::Partitions,
        4 => Request::Connections,
        _ => return Err(CodecError::Command),
    };
    Ok((id, request))
}

/// The ID of a request whose kind may be unknown.
pub fn request_id(input: &[u8]) -> Option<u64> {
    let mut reader = Reader::new(input);
    reader.u8().ok()?;
    reader.u64().ok()
}

/// Encode reply `id`. Fails when a string or list exceeds its length field.
pub fn encode_reply(id: u64, reply: &Reply) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    let kind = match reply {
        Reply::Broker(_) => Request::Broker as u8,
        Reply::Counters(_) => Request::Counters as u8,
        Reply::Partitions(_) => Request::Partitions as u8,
        Reply::Connections(_) => Request::Connections as u8,
        Reply::Error(_) => ERROR,
    };
    out.push(kind);
    out.extend_from_slice(&id.to_be_bytes());
    match reply {
        Reply::Broker(broker) => encode_broker(&mut out, broker)?,
        Reply::Counters(counters) => encode_counters(&mut out, counters)?,
        Reply::Partitions(partitions) => {
            out.extend_from_slice(&partitions.uptime_ms.to_be_bytes());
            out.extend_from_slice(&partitions.missing_shards.to_be_bytes());
            count(&mut out, partitions.rows.len())?;
            for row in &partitions.rows {
                encode_status(&mut out, row)?;
            }
        }
        Reply::Connections(connections) => {
            count(&mut out, connections.brokers.len())?;
            for (id, up) in &connections.brokers {
                out.extend_from_slice(id.as_bytes());
                out.push(u8::from(*up));
            }
            out.extend_from_slice(&connections.clients.to_be_bytes());
        }
        Reply::Error(message) => text(&mut out, message)?,
    }
    Ok(out)
}

/// Decode a reply and its request ID.
pub fn decode_reply(input: &[u8]) -> Result<(u64, Reply), CodecError> {
    let mut reader = Reader::new(input);
    let kind = reader.u8()?;
    let id = reader.u64()?;
    let reply = match kind {
        1 => Reply::Broker(decode_broker(&mut reader)?),
        2 => Reply::Counters(decode_counters(&mut reader)?),
        3 => {
            let uptime_ms = reader.u64()?;
            let missing_shards = reader.u32()?;
            let rows = list(&mut reader, decode_status)?;
            Reply::Partitions(Partitions {
                uptime_ms,
                missing_shards,
                rows,
            })
        }
        4 => Reply::Connections(Connections {
            brokers: list(&mut reader, |reader| {
                let id = NodeId::from_bytes(reader.array()?);
                let up = match reader.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(CodecError::Profile),
                };
                Ok((id, up))
            })?,
            clients: reader.u32()?,
        }),
        ERROR => Reply::Error(read_text(&mut reader)?),
        _ => return Err(CodecError::Command),
    };
    reader.finish()?;
    Ok((id, reply))
}

fn encode_broker(out: &mut Vec<u8>, broker: &Broker) -> Result<(), CodecError> {
    text(out, &broker.name)?;
    out.extend_from_slice(broker.id.as_bytes());
    out.extend_from_slice(&broker.pid.to_be_bytes());
    out.extend_from_slice(&broker.uptime_ms.to_be_bytes());
    out.extend_from_slice(&broker.max_clients.to_be_bytes());
    out.extend_from_slice(&broker.shards.to_be_bytes());
    count(out, broker.brokers.len())?;
    for (name, id) in &broker.brokers {
        text(out, name)?;
        out.extend_from_slice(id.as_bytes());
    }
    count(out, broker.partitions.len())?;
    for partition in &broker.partitions {
        out.extend_from_slice(partition.group.as_bytes());
        text(out, &partition.topic)?;
        out.extend_from_slice(&partition.partition.to_be_bytes());
        out.extend_from_slice(&partition.shard.to_be_bytes());
    }
    Ok(())
}

fn decode_broker(reader: &mut Reader<'_>) -> Result<Broker, CodecError> {
    Ok(Broker {
        name: read_text(reader)?,
        id: NodeId::from_bytes(reader.array()?),
        pid: reader.u32()?,
        uptime_ms: reader.u64()?,
        max_clients: reader.u32()?,
        shards: reader.u32()?,
        brokers: list(reader, |reader| {
            Ok((read_text(reader)?, NodeId::from_bytes(reader.array()?)))
        })?,
        partitions: list(reader, |reader| {
            Ok(PartitionName {
                group: GroupId::from_bytes(reader.array()?),
                topic: read_text(reader)?,
                partition: reader.u32()?,
                shard: reader.u32()?,
            })
        })?,
    })
}

fn encode_counters(out: &mut Vec<u8>, counters: &Counters) -> Result<(), CodecError> {
    let events = counters.events.len();
    if counters.totals.len() != events
        || counters
            .threads
            .iter()
            .any(|(_, values)| values.len() != events)
    {
        return Err(CodecError::Length);
    }
    out.extend_from_slice(&counters.uptime_ms.to_be_bytes());
    count(out, events)?;
    for (name, kind) in &counters.events {
        text(out, name)?;
        out.push(*kind as u8);
    }
    values(out, &counters.totals);
    count(out, counters.threads.len())?;
    for (name, thread) in &counters.threads {
        text(out, name)?;
        values(out, thread);
    }
    Ok(())
}

fn decode_counters(reader: &mut Reader<'_>) -> Result<Counters, CodecError> {
    let uptime_ms = reader.u64()?;
    let events = list(reader, |reader| {
        let name = read_text(reader)?;
        let kind = match reader.u8()? {
            0 => EventKind::Count,
            1 => EventKind::Peak,
            2 => EventKind::Gauge,
            _ => return Err(CodecError::Profile),
        };
        Ok((name, kind))
    })?;
    let read_values = |reader: &mut Reader<'_>| {
        if reader.rest().len() / 8 < events.len() {
            return Err(CodecError::Length);
        }
        (0..events.len())
            .map(|_| Ok(reader.u64()?))
            .collect::<Result<Vec<_>, CodecError>>()
    };
    let totals = read_values(reader)?;
    let threads = list(reader, |reader| {
        Ok((read_text(reader)?, read_values(reader)?))
    })?;
    Ok(Counters {
        uptime_ms,
        events,
        totals,
        threads,
    })
}

fn count(out: &mut Vec<u8>, length: usize) -> Result<(), CodecError> {
    let length = u32::try_from(length).map_err(|_| CodecError::Length)?;
    out.extend_from_slice(&length.to_be_bytes());
    Ok(())
}

/// A counted list. Each item takes at least one byte, so a count beyond the
/// remaining input fails before allocating.
fn list<T>(
    reader: &mut Reader<'_>,
    mut item: impl FnMut(&mut Reader<'_>) -> Result<T, CodecError>,
) -> Result<Vec<T>, CodecError> {
    let length = reader.u32()? as usize;
    if length > reader.rest().len() {
        return Err(CodecError::Length);
    }
    (0..length).map(|_| item(reader)).collect()
}

fn values(out: &mut Vec<u8>, values: &[u64]) {
    for value in values {
        out.extend_from_slice(&value.to_be_bytes());
    }
}

fn text(out: &mut Vec<u8>, value: &str) -> Result<(), CodecError> {
    let length = u16::try_from(value.len()).map_err(|_| CodecError::Length)?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_text(reader: &mut Reader<'_>) -> Result<String, CodecError> {
    let length = reader.u16()? as usize;
    std::str::from_utf8(reader.take(length)?)
        .map(str::to_owned)
        .map_err(|_| CodecError::Profile)
}

#[cfg(test)]
mod tests;
