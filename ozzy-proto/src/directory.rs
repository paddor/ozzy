//! Session-scoped partition routing interests over ordinary broker PEER.
//! Snapshots and updates are hints, never proof of leader authority.

use std::{collections::BTreeSet, sync::Arc};

use crate::{
    ENVELOPE_BYTES, Envelope, EnvelopeLimits, GroupId, NodeId, Opcode, Packet,
    PartitionIncarnation, RequestId,
    data::{self, CodecError, Cursor},
};

const WATCH_TAG: u8 = 1;
const ROUTE_BASE: usize = 65;

mod topic;
pub use topic::{
    BrokerEndpoint, TopicPage, TopicPartition, TopicRequest, decode_topic_page,
    decode_topic_request, encode_topic_page, encode_topic_request,
};

/// Structural limits in addition to the envelope metadata byte ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Group interests or route entries in one command.
    pub partitions: usize,
    /// Current formats allow one local broker, three replicated brokers, or a
    /// future six-broker configuration. This field cannot change quorum rules.
    pub members: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            partitions: 256,
            members: 6,
        }
    }
}

impl Limits {
    fn valid(self) -> bool {
        (1..=u16::MAX as usize).contains(&self.partitions) && (1..=6).contains(&self.members)
    }

    /// Conservative metadata bound for one registration snapshot. The caller
    /// adds its PEER routing and envelope frames to bound an outgoing queue.
    pub fn maximum_snapshot_bytes(self, interests: usize) -> Option<usize> {
        if !self.valid() || interests == 0 || interests > self.partitions {
            return None;
        }
        interests
            .checked_mul(ROUTE_BASE.checked_add(self.members.checked_mul(16)?)?)?
            .checked_add(19)
    }
}

/// Stable group identity and one broker's current leader observation.
/// Member order is persistent configuration, not a routing preference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteState {
    /// Group for one topic partition.
    pub group: GroupId,
    /// Persistent configuration epoch. Views compare only within this epoch.
    pub config_epoch: u64,
    /// Immutable partition incarnation.
    pub partition: PartitionIncarnation,
    /// Configured brokers in persistent order. One broker is local-durable;
    /// three brokers use replication. Six is reserved for a future policy.
    pub members: Arc<[NodeId]>,
    /// Election view within this exact group identity and configuration.
    pub view: u64,
    /// Best-known leader, if election has completed on this broker.
    pub leader: Option<NodeId>,
}

impl RouteState {
    /// Check structural identity only. A caller must independently compare this
    /// with trusted topic metadata before treating it as a routing hint.
    pub fn valid(&self) -> bool {
        if !matches!(self.members.len(), 1 | 3 | 6) {
            return false;
        }
        let unique: BTreeSet<_> = self.members.iter().copied().collect();
        self.group.as_bytes() != &[0; 16]
            && self.config_epoch != 0
            && self.partition.as_bytes() != &[0; 16]
            && self.members.len() == unique.len()
            && self.members.iter().all(|id| id.as_bytes() != &[0; 16])
            && self.leader.is_none_or(|id| unique.contains(&id))
    }

    /// View numbers can be compared only for this complete identity and member
    /// order. A re-created partition or changed configuration needs a snapshot.
    pub fn same_identity(&self, other: &Self) -> bool {
        self.group == other.group
            && self.config_epoch == other.config_epoch
            && self.partition == other.partition
            && self.members == other.members
    }

    fn size(&self) -> Result<usize, CodecError> {
        ROUTE_BASE
            .checked_add(
                self.members
                    .len()
                    .checked_mul(16)
                    .ok_or(CodecError::Length)?,
            )
            .ok_or(CodecError::Length)
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.group.as_bytes());
        out.extend_from_slice(&self.config_epoch.to_be_bytes());
        out.extend_from_slice(self.partition.as_bytes());
        out.push(self.members.len() as u8);
        for member in self.members.iter() {
            out.extend_from_slice(member.as_bytes());
        }
        out.extend_from_slice(&self.view.to_be_bytes());
        if let Some(leader) = &self.leader {
            out.extend_from_slice(leader.as_bytes());
        } else {
            out.extend_from_slice(&[0; 16]);
        }
    }

    fn decode(cursor: &mut Cursor<'_>, limits: Limits) -> Result<Self, CodecError> {
        let group = GroupId::from_bytes(cursor.array()?);
        let config_epoch = cursor.u64()?;
        let partition = PartitionIncarnation::from_bytes(cursor.array()?);
        let count = usize::from(cursor.byte()?);
        if !matches!(count, 1 | 3 | 6) || count > limits.members {
            return Err(CodecError::Limit);
        }
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            members.push(NodeId::from_bytes(cursor.array()?));
        }
        let view = cursor.u64()?;
        let leader = cursor.array::<16>()?;
        let route = Self {
            group,
            config_epoch,
            partition,
            members: members.into(),
            view,
            leader: (leader != [0; 16]).then(|| NodeId::from_bytes(leader)),
        };
        if !route.valid() {
            return Err(CodecError::Profile);
        }
        Ok(route)
    }
}

/// Register exact group interests. Topic lookup supplies these group IDs first.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRequest {
    /// Session-scoped registration, independent of one request correlation ID.
    pub watch: RequestId,
    /// No duplicate group or zero identity.
    pub groups: Vec<GroupId>,
}

/// Atomic registration snapshot from one broker's best-known observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    /// Matching registration.
    pub watch: RequestId,
    /// Exactly the requested groups, sorted only by the sender's local choice.
    pub routes: Vec<RouteState>,
}

impl Snapshot {
    /// Exact metadata bytes before caller allocation. Encoding still validates
    /// identity, count, and negotiated envelope limits.
    pub fn metadata_bytes(&self) -> Result<usize, CodecError> {
        self.routes.iter().try_fold(19_usize, |total, route| {
            total.checked_add(route.size()?).ok_or(CodecError::Length)
        })
    }
}

/// Coalesced hint after a registration snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Update {
    /// Matching registration.
    pub watch: RequestId,
    /// One partition's newer observation.
    pub route: RouteState,
}

impl Update {
    /// Exact metadata bytes before caller allocation.
    pub fn metadata_bytes(&self) -> Result<usize, CodecError> {
        17_usize
            .checked_add(self.route.size()?)
            .ok_or(CodecError::Length)
    }
}

/// A bounded watcher overflowed and must request a fresh snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Resync {
    /// Matching registration.
    pub watch: RequestId,
}

/// Encode a registration request into a caller-owned metadata buffer.
pub fn encode_request(
    envelope: Envelope,
    request: &SnapshotRequest,
    out: &mut Vec<u8>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    let groups = &request.groups;
    if !limits.valid()
        || !watch_valid(request.watch)
        || groups.is_empty()
        || groups.len() > limits.partitions
        || groups.iter().any(|group| group.as_bytes() == &[0; 16])
        || groups.iter().copied().collect::<BTreeSet<_>>().len() != groups.len()
    {
        return Err(CodecError::Profile);
    }
    let size = 19_usize
        .checked_add(groups.len().checked_mul(16).ok_or(CodecError::Length)?)
        .ok_or(CodecError::Length)?;
    let header = prepare(
        envelope,
        Opcode::StateSnapshotRequest,
        true,
        size,
        out,
        frame,
    )?;
    prefix(out, request.watch);
    out.extend_from_slice(&(groups.len() as u16).to_be_bytes());
    for group in groups {
        out.extend_from_slice(group.as_bytes());
    }
    Ok(header)
}

/// Decode a registration request before allocating its bounded group list.
pub fn decode_request(
    packet: Packet<'_>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<SnapshotRequest, CodecError> {
    let mut cursor = control(packet, Opcode::StateSnapshotRequest, true, frame, limits)?;
    let watch = read_prefix(&mut cursor)?;
    let count = usize::from(u16::from_be_bytes(cursor.array()?));
    if count == 0 || count > limits.partitions || cursor.0.len() != 16 * count {
        return Err(CodecError::Length);
    }
    let mut groups = Vec::with_capacity(count);
    let mut unique = BTreeSet::new();
    for _ in 0..count {
        let group = GroupId::from_bytes(cursor.array()?);
        if group.as_bytes() == &[0; 16] || !unique.insert(group) {
            return Err(CodecError::Profile);
        }
        groups.push(group);
    }
    Ok(SnapshotRequest { watch, groups })
}

/// Encode a bounded registration snapshot.
pub fn encode_snapshot(
    envelope: Envelope,
    snapshot: &Snapshot,
    out: &mut Vec<u8>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    if !limits.valid()
        || !watch_valid(snapshot.watch)
        || snapshot.routes.is_empty()
        || snapshot.routes.len() > limits.partitions
    {
        return Err(CodecError::Profile);
    }
    let mut groups = BTreeSet::new();
    let mut partitions = BTreeSet::new();
    let mut size = 19_usize;
    for route in &snapshot.routes {
        if !route.valid()
            || route.members.len() > limits.members
            || !groups.insert(route.group)
            || !partitions.insert(route.partition)
        {
            return Err(CodecError::Profile);
        }
        size = size.checked_add(route.size()?).ok_or(CodecError::Length)?;
    }
    let header = prepare(envelope, Opcode::StateSnapshot, true, size, out, frame)?;
    prefix(out, snapshot.watch);
    out.extend_from_slice(&(snapshot.routes.len() as u16).to_be_bytes());
    for route in &snapshot.routes {
        route.encode(out);
    }
    Ok(header)
}

/// Decode a snapshot with no payload frame or unbounded route allocation.
pub fn decode_snapshot(
    packet: Packet<'_>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<Snapshot, CodecError> {
    let mut cursor = control(packet, Opcode::StateSnapshot, true, frame, limits)?;
    let watch = read_prefix(&mut cursor)?;
    let count = usize::from(u16::from_be_bytes(cursor.array()?));
    if count == 0 || count > limits.partitions || cursor.0.len() < ROUTE_BASE * count {
        return Err(CodecError::Length);
    }
    let mut routes = Vec::with_capacity(count);
    let mut groups = BTreeSet::new();
    let mut partitions = BTreeSet::new();
    for _ in 0..count {
        let route = RouteState::decode(&mut cursor, limits)?;
        if !groups.insert(route.group) || !partitions.insert(route.partition) {
            return Err(CodecError::Profile);
        }
        routes.push(route);
    }
    if !cursor.0.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(Snapshot { watch, routes })
}

/// Encode one uncorrelated, session-fenced route update.
pub fn encode_update(
    envelope: Envelope,
    update: &Update,
    out: &mut Vec<u8>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    if !limits.valid()
        || !watch_valid(update.watch)
        || !update.route.valid()
        || update.route.members.len() > limits.members
    {
        return Err(CodecError::Profile);
    }
    let size = 17_usize
        .checked_add(update.route.size()?)
        .ok_or(CodecError::Length)?;
    let header = prepare(envelope, Opcode::StateUpdate, false, size, out, frame)?;
    prefix(out, update.watch);
    update.route.encode(out);
    Ok(header)
}

/// Decode one route hint. SDKs compare identity and view with trusted metadata.
pub fn decode_update(
    packet: Packet<'_>,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<Update, CodecError> {
    let mut cursor = control(packet, Opcode::StateUpdate, false, frame, limits)?;
    let watch = read_prefix(&mut cursor)?;
    let route = RouteState::decode(&mut cursor, limits)?;
    if !cursor.0.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(Update { watch, route })
}

/// Encode a notice that the receiver must register for a new snapshot.
pub fn encode_resync(
    envelope: Envelope,
    resync: Resync,
    out: &mut Vec<u8>,
    frame: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    if !watch_valid(resync.watch) {
        return Err(CodecError::Profile);
    }
    let header = prepare(envelope, Opcode::StateResync, false, 17, out, frame)?;
    prefix(out, resync.watch);
    Ok(header)
}

/// Decode a resync notice without changing topic or writer authority.
pub fn decode_resync(packet: Packet<'_>, frame: EnvelopeLimits) -> Result<Resync, CodecError> {
    let mut cursor = control(packet, Opcode::StateResync, false, frame, Limits::default())?;
    let watch = read_prefix(&mut cursor)?;
    if !cursor.0.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(Resync { watch })
}

fn watch_valid(id: RequestId) -> bool {
    id.as_bytes() != &[0; 16]
}

fn prefix(out: &mut Vec<u8>, watch: RequestId) {
    out.push(WATCH_TAG);
    out.extend_from_slice(watch.as_bytes());
}

fn read_prefix(cursor: &mut Cursor<'_>) -> Result<RequestId, CodecError> {
    if cursor.byte()? != WATCH_TAG {
        return Err(CodecError::Profile);
    }
    let watch = RequestId::from_bytes(cursor.array()?);
    if !watch_valid(watch) {
        return Err(CodecError::Profile);
    }
    Ok(watch)
}

fn prepare(
    envelope: Envelope,
    opcode: Opcode,
    correlated: bool,
    size: usize,
    out: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, opcode, correlated)?;
    let header = envelope.encode_header(size, 0, limits)?;
    data::capacity(out, size)?;
    out.clear();
    Ok(header)
}

fn control(
    packet: Packet<'_>,
    opcode: Opcode,
    correlated: bool,
    frame: EnvelopeLimits,
    limits: Limits,
) -> Result<Cursor<'_>, CodecError> {
    if !limits.valid() {
        return Err(CodecError::Limit);
    }
    command(packet.envelope, opcode, correlated)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), frame)?;
    if !packet.payload.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(Cursor(packet.metadata))
}

fn command(envelope: Envelope, opcode: Opcode, correlated: bool) -> Result<(), CodecError> {
    if envelope.opcode != opcode
        || envelope.response != (opcode == Opcode::StateSnapshot)
        || envelope.request_id.is_some() != correlated
        || envelope.session.is_none()
    {
        return Err(CodecError::Command);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
