//! Admin console requests and replies on a broker's admin endpoint.
//!
//! A client matches each reply to its request by ID. Replies are
//! observations, never authority; [`Request::DeleteGroup`] is the one
//! command. Fields are big-endian; a string
//! is a `u16` byte length plus UTF-8, a list a `u32` count plus its items.

use crate::{ConsumerGroupId, ConsumerMemberId, GroupId, NodeId, data::CodecError, layout::Reader};
use std::num::NonZeroU64;

mod broker;
mod counters;
mod logs;
mod partitions;

use broker::{decode_broker, encode_broker};
use counters::{decode_counters, encode_counters};
use logs::{decode_logs, encode_logs};
use partitions::{decode_status, encode_status};

const DELETE_GROUP: u8 = 5;
const LOGS: u8 = 6;
const ERROR: u8 = 0xff;

/// What a client asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// Static broker facts: names, IDs, partition directory.
    Broker,
    /// Event counters: totals and each thread's values.
    Counters,
    /// Each local partition's replication status and committed state.
    Partitions,
    /// Broker links and connected clients.
    Connections,
    /// The broker process's recent diagnostics.
    Logs,
    /// Delete consumer group `name` on `topic`: fence its members on every
    /// partition, then free the name.
    DeleteGroup {
        /// Topic the group reads.
        topic: String,
        /// Group name.
        name: String,
    },
}

impl Request {
    /// The wire kind, shared by the request and its reply.
    const fn kind(&self) -> u8 {
        match self {
            Self::Broker => 1,
            Self::Counters => 2,
            Self::Partitions => 3,
            Self::Connections => 4,
            Self::DeleteGroup { .. } => DELETE_GROUP,
            Self::Logs => LOGS,
        }
    }
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
    /// Answers [`Request::Logs`], oldest first.
    Logs(Vec<LogEntry>),
    /// Answers [`Request::DeleteGroup`]: the deleted group incarnation, or
    /// `None` when no group has that name.
    GroupDeleted(Option<ConsumerGroupId>),
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
    /// Retention age limit in milliseconds; `None` keeps records of any age.
    pub retention_age_millis: Option<NonZeroU64>,
    /// Retention limit on summed segment capacities; `None` keeps any size.
    pub retention_bytes: Option<NonZeroU64>,
    /// Retention policy revision, 1 at creation.
    pub policy_revision: u64,
    /// Journal segments; 0 while the journal is being installed.
    pub segments: u32,
    /// Summed segment capacities, the size `retention_bytes` limits.
    pub segment_bytes: u64,
    /// Retained deduplication identities.
    pub dedup_entries: u64,
    /// Deduplication identity limit; `None` without deduplication.
    pub dedup_limit: Option<NonZeroU64>,
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

/// A diagnostic the broker process logged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    /// Milliseconds after the broker started; 0 for earlier entries.
    pub uptime_ms: u64,
    /// Severity.
    pub level: LogLevel,
    /// The logging thread's name.
    pub thread: String,
    /// The message.
    pub message: String,
}

/// Severity of a [`LogEntry`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LogLevel {
    /// A notable state transition.
    Info = 0,
    /// Recoverable pressure or degraded execution.
    Warn = 1,
    /// Execution failure.
    Error = 2,
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

/// Encode request `id`. Fails when a string exceeds its length field.
pub fn encode_request(id: u64, request: &Request) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(9);
    out.push(request.kind());
    out.extend_from_slice(&id.to_be_bytes());
    if let Request::DeleteGroup { topic, name } = request {
        text(&mut out, topic)?;
        text(&mut out, name)?;
    }
    Ok(out)
}

/// Decode a request. An unknown kind fails with [`CodecError::Command`] and
/// still returns the ID through [`request_id`].
pub fn decode_request(input: &[u8]) -> Result<(u64, Request), CodecError> {
    let mut reader = Reader::new(input);
    let kind = reader.u8()?;
    let id = reader.u64()?;
    let request = match kind {
        1 => Request::Broker,
        2 => Request::Counters,
        3 => Request::Partitions,
        4 => Request::Connections,
        LOGS => Request::Logs,
        DELETE_GROUP => Request::DeleteGroup {
            topic: read_text(&mut reader)?,
            name: read_text(&mut reader)?,
        },
        _ => return Err(CodecError::Command),
    };
    reader.finish()?;
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
        Reply::Broker(_) => Request::Broker.kind(),
        Reply::Counters(_) => Request::Counters.kind(),
        Reply::Partitions(_) => Request::Partitions.kind(),
        Reply::Connections(_) => Request::Connections.kind(),
        Reply::Logs(_) => LOGS,
        Reply::GroupDeleted(_) => DELETE_GROUP,
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
        Reply::Logs(entries) => encode_logs(&mut out, entries)?,
        Reply::GroupDeleted(group) => {
            out.push(u8::from(group.is_some()));
            if let Some(group) = group {
                out.extend_from_slice(group.as_bytes());
            }
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
        LOGS => Reply::Logs(decode_logs(&mut reader)?),
        DELETE_GROUP => Reply::GroupDeleted(match reader.u8()? {
            0 => None,
            1 => Some(ConsumerGroupId::from_bytes(reader.array()?)),
            _ => return Err(CodecError::Profile),
        }),
        ERROR => Reply::Error(read_text(&mut reader)?),
        _ => return Err(CodecError::Command),
    };
    reader.finish()?;
    Ok((id, reply))
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
