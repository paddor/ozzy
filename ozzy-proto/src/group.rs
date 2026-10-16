//! Consumer-group membership, coordinator plans, and source activation on the
//! addressed control plane. Membership replies describe committed intent;
//! sources confirm activation separately.

use crate::{
    ConsumerGroupId, ConsumerMemberId, ENVELOPE_BYTES, Envelope, EnvelopeLimits, GroupId, Offset,
    Opcode, OperationId, Packet, PartitionIncarnation,
    append::{Authority, Policy},
    data::{CodecError, Cursor},
};

/// Absolute member bound of one group.
pub const MAX_MEMBERS: usize = 256;
/// Absolute source bound of one group. Every reply that lists one member's
/// sources, and every owned-source report, must fit one bounded control frame.
pub const MAX_PARTITIONS: usize = 1024;
/// Maximum topic-scoped group name bytes.
pub const MAX_NAME_BYTES: usize = 255;
/// Fixed join metadata before the name and owned-source entries.
pub const JOIN_FIXED_BYTES: usize = 112;
/// Bytes per owned-source entry in a join request.
pub const OWNED_BYTES: usize = 32;
/// Fixed membership reply metadata before the target entries.
pub const JOINED_FIXED_BYTES: usize = 78;
/// Bytes per target entry in a membership reply.
pub const TARGET_BYTES: usize = 45;
/// Fixed activation request metadata.
pub const ASSIGN_BYTES: usize = 99;
/// Fixed activation reply metadata.
pub const ASSIGNED_BYTES: usize = 122;
const SOURCE_TAG: u8 = 1;

/// First position of a group that has no persisted cursor for a source yet.
/// Existing cursors always win; this selector never resets progress.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Start {
    /// Earliest retained record of each source when it is first assigned.
    #[default]
    Earliest,
    /// Confirmed end of each source when it is first assigned.
    Latest,
    /// Exact offset applied to every source; it must be retained and confirmed.
    Offset(Offset),
}

impl Start {
    /// Fixed nine-byte encoding: tag, then the offset or zero.
    pub const BYTES: usize = 9;

    fn encode(self, output: &mut Vec<u8>) {
        let (tag, value) = match self {
            Self::Earliest => (0, 0),
            Self::Latest => (1, 0),
            Self::Offset(offset) => (2, offset.get()),
        };
        output.push(tag);
        output.extend_from_slice(&value.to_be_bytes());
    }

    fn decode(cursor: &mut Cursor<'_>) -> Result<Self, CodecError> {
        let tag = cursor.byte()?;
        let value = cursor.u64()?;
        match (tag, value) {
            (0, 0) => Ok(Self::Earliest),
            (1, 0) => Ok(Self::Latest),
            (2, value) => Ok(Self::Offset(Offset::new(value))),
            _ => Err(CodecError::Profile),
        }
    }
}

/// Membership intent carried by one request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Mode {
    /// Keep an existing membership alive and report owned sources.
    Heartbeat = 0,
    /// Join with a fresh join identity, creating the group if absent.
    Join = 1,
    /// Leave cooperatively; owned sources move in the next plan.
    Leave = 2,
}

/// One source this instance currently processes, as activated by its owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Owned {
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Plan revision that activated this ownership.
    pub plan_revision: u64,
    /// Epoch allocated by the source.
    pub epoch: u64,
}

/// Join, heartbeat, or leave, addressed to the group's coordinator leader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinRequest {
    /// Expected coordinator leader generation.
    pub authority: Authority,
    /// Deterministic coordinator partition of this topic and group name.
    pub coordinator: PartitionIncarnation,
    /// Topic-scoped group name.
    pub name: String,
    /// Application member identity, stable across restarts.
    pub member: ConsumerMemberId,
    /// Fresh identity of this processing instance.
    pub join: OperationId,
    /// Incarnation this instance belongs to, once known. A request naming a
    /// retired incarnation is told so instead of creating a new one.
    pub group: Option<ConsumerGroupId>,
    /// Membership intent.
    pub mode: Mode,
    /// Creation selector, used only when this request creates the group.
    pub initial: Start,
    /// Sources this instance currently processes or drains.
    pub owned: Vec<Owned>,
}

/// Membership status in a reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Status {
    /// The committed plan contains this join; targets follow.
    Member = 0,
    /// The group or this join is not committed yet; keep heartbeating.
    Joining = 1,
    /// No such group; a join request creates it.
    Unknown = 2,
    /// The requested incarnation was deleted; this instance must stop.
    Deleted = 3,
}

/// Committed intent for one of the requesting member's sources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Target {
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Numeric topic partition.
    pub number: u32,
    /// Plan revision to relay to the source.
    pub plan_revision: u64,
    /// The prior owner drained, left, or exceeded its deadline.
    pub activatable: bool,
    /// Instance intended by the previous plan, if any.
    pub prior_member: Option<ConsumerMemberId>,
}

/// Committed membership view for one instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinReply {
    /// Coordinator leader generation that served this reply.
    pub authority: Authority,
    /// Coordinator partition incarnation.
    pub coordinator: PartitionIncarnation,
    /// Group incarnation; zero only while the group is still being created.
    pub group: ConsumerGroupId,
    /// Committed coordinator revision; zero without a committed group.
    pub revision: u64,
    /// Membership status of the requesting join.
    pub status: Status,
    /// Sources intended for this instance, in numeric partition order.
    pub targets: Vec<Target>,
}

/// Relay one committed intent to its source leader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignRequest {
    /// Expected source leader generation.
    pub authority: Authority,
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Group incarnation.
    pub group: ConsumerGroupId,
    /// Activating member, or none to revoke the source. Deletion revokes at
    /// the maximum revision, fencing every plan of the incarnation.
    pub member: Option<ConsumerMemberId>,
    /// Committed plan revision.
    pub plan_revision: u64,
    /// Group creation selector, resolved only on a source without progress.
    pub initial: Start,
    /// Required immutable source confirmation boundary.
    pub policy: Policy,
}

/// Activation coordinates after the source's confirmation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignReply {
    /// Confirming source leader generation.
    pub authority: Authority,
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Group incarnation.
    pub group: ConsumerGroupId,
    /// Activated member, or none after a revocation.
    pub member: Option<ConsumerMemberId>,
    /// Epoch allocated by the source for this ownership.
    pub epoch: u64,
    /// Plan revision confirmed at the source.
    pub plan_revision: u64,
    /// Exclusive cursor captured with the assignment.
    pub cursor: Offset,
    /// Earliest retained record at validation.
    pub retained_from: Offset,
    /// Exclusive confirmed record end at validation.
    pub end: Offset,
    /// Configured confirmation policy.
    pub policy: Policy,
}

/// Native code-7 detail for a creation offset outside confirmed retained history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitialPosition {
    /// Requested first cursor.
    pub cursor: Offset,
    /// Confirmed earliest retained offset.
    pub retained_from: Offset,
    /// Confirmed exclusive record end.
    pub end: Offset,
}

impl InitialPosition {
    /// Encode cursor, retained floor, and end, in that order.
    pub fn encode(self) -> Result<[u8; 24], CodecError> {
        self.validate()?;
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&self.cursor.get().to_be_bytes());
        bytes[8..16].copy_from_slice(&self.retained_from.get().to_be_bytes());
        bytes[16..].copy_from_slice(&self.end.get().to_be_bytes());
        Ok(bytes)
    }

    /// Decode a strict detail; unrelated NACKs use other details.
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() != 24 {
            return Err(CodecError::Length);
        }
        let mut cursor = Cursor(bytes);
        let detail = Self {
            cursor: Offset::new(cursor.u64()?),
            retained_from: Offset::new(cursor.u64()?),
            end: Offset::new(cursor.u64()?),
        };
        detail.validate()?;
        Ok(detail)
    }

    fn validate(self) -> Result<(), CodecError> {
        if self.retained_from > self.end || (self.retained_from..=self.end).contains(&self.cursor) {
            return Err(CodecError::Profile);
        }
        Ok(())
    }
}

/// Exact join request metadata bytes.
pub fn join_bytes(name: &str, owned: usize) -> Result<usize, CodecError> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || owned > MAX_PARTITIONS {
        return Err(CodecError::Limit);
    }
    Ok(JOIN_FIXED_BYTES + name.len() + owned * OWNED_BYTES)
}

/// Exact membership reply metadata bytes.
pub fn joined_bytes(targets: usize) -> Result<usize, CodecError> {
    if targets > MAX_PARTITIONS {
        return Err(CodecError::Limit);
    }
    Ok(JOINED_FIXED_BYTES + targets * TARGET_BYTES)
}

/// Encode one correlated, payload-free membership request.
pub fn encode_join(
    envelope: Envelope,
    request: &JoinRequest,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::GroupJoin, false)?;
    validate_scope(request.authority, request.coordinator)?;
    nonzero(request.member.as_bytes())?;
    nonzero(request.join.as_bytes())?;
    let length = join_bytes(&request.name, request.owned.len())?;
    let header = envelope.encode_header(length, 0, limits)?;
    reserve(metadata, length)?;
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, request.authority);
    metadata.extend_from_slice(request.coordinator.as_bytes());
    metadata.push(request.name.len() as u8);
    metadata.extend_from_slice(request.name.as_bytes());
    metadata.extend_from_slice(request.member.as_bytes());
    metadata.extend_from_slice(request.join.as_bytes());
    metadata.extend_from_slice(&request.group.map_or([0; 16], |group| *group.as_bytes()));
    metadata.push(request.mode as u8);
    request.initial.encode(metadata);
    metadata.extend_from_slice(&(request.owned.len() as u32).to_be_bytes());
    for owned in &request.owned {
        nonzero(owned.partition.as_bytes())?;
        if owned.plan_revision == 0 || owned.epoch == 0 {
            return Err(CodecError::Identity);
        }
        metadata.extend_from_slice(owned.partition.as_bytes());
        metadata.extend_from_slice(&owned.plan_revision.to_be_bytes());
        metadata.extend_from_slice(&owned.epoch.to_be_bytes());
    }
    debug_assert_eq!(metadata.len(), length);
    Ok(header)
}

/// Decode shape and identities. Link trust and membership remain owner checks.
pub fn decode_join(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<JoinRequest, CodecError> {
    check(packet, Opcode::GroupJoin, false, limits)?;
    let mut cursor = Cursor(packet.metadata);
    if cursor.byte()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let authority = decode_authority(&mut cursor)?;
    let coordinator = PartitionIncarnation::from_bytes(cursor.array()?);
    validate_scope(authority, coordinator)?;
    let name = cursor.byte()? as usize;
    if name == 0 {
        return Err(CodecError::Limit);
    }
    let name = std::str::from_utf8(cursor.take(name)?)
        .map_err(|_| CodecError::Profile)?
        .to_owned();
    let member = ConsumerMemberId::from_bytes(cursor.array()?);
    let join = OperationId::from_bytes(cursor.array()?);
    nonzero(member.as_bytes())?;
    nonzero(join.as_bytes())?;
    let group = cursor.array::<16>()?;
    let group = (group != [0; 16]).then(|| ConsumerGroupId::from_bytes(group));
    let mode = match cursor.byte()? {
        0 => Mode::Heartbeat,
        1 => Mode::Join,
        2 => Mode::Leave,
        _ => return Err(CodecError::Profile),
    };
    let initial = Start::decode(&mut cursor)?;
    let count = cursor.u32()? as usize;
    if count > MAX_PARTITIONS || cursor.0.len() != count * OWNED_BYTES {
        return Err(CodecError::Length);
    }
    let mut owned = Vec::with_capacity(count);
    for _ in 0..count {
        let entry = Owned {
            partition: PartitionIncarnation::from_bytes(cursor.array()?),
            plan_revision: cursor.u64()?,
            epoch: cursor.u64()?,
        };
        nonzero(entry.partition.as_bytes())?;
        if entry.plan_revision == 0 || entry.epoch == 0 {
            return Err(CodecError::Identity);
        }
        owned.push(entry);
    }
    Ok(JoinRequest {
        authority,
        coordinator,
        name,
        member,
        join,
        group,
        mode,
        initial,
        owned,
    })
}

/// Encode a committed membership view, never a transport receipt.
pub fn encode_joined(
    envelope: Envelope,
    reply: &JoinReply,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::GroupJoined, true)?;
    validate_scope(reply.authority, reply.coordinator)?;
    validate_joined(reply)?;
    let length = joined_bytes(reply.targets.len())?;
    let header = envelope.encode_header(length, 0, limits)?;
    reserve(metadata, length)?;
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, reply.authority);
    metadata.extend_from_slice(reply.coordinator.as_bytes());
    metadata.extend_from_slice(reply.group.as_bytes());
    metadata.extend_from_slice(&reply.revision.to_be_bytes());
    metadata.push(reply.status as u8);
    metadata.extend_from_slice(&(reply.targets.len() as u32).to_be_bytes());
    for target in &reply.targets {
        metadata.extend_from_slice(target.partition.as_bytes());
        metadata.extend_from_slice(&target.number.to_be_bytes());
        metadata.extend_from_slice(&target.plan_revision.to_be_bytes());
        metadata.push(u8::from(target.activatable));
        metadata.extend_from_slice(&target.prior_member.map_or([0; 16], |m| *m.as_bytes()));
    }
    debug_assert_eq!(metadata.len(), length);
    Ok(header)
}

/// Decode a membership view before checking its request and link binding.
pub fn decode_joined(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<JoinReply, CodecError> {
    check(packet, Opcode::GroupJoined, true, limits)?;
    let mut cursor = Cursor(packet.metadata);
    if cursor.byte()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let authority = decode_authority(&mut cursor)?;
    let coordinator = PartitionIncarnation::from_bytes(cursor.array()?);
    validate_scope(authority, coordinator)?;
    let group = ConsumerGroupId::from_bytes(cursor.array()?);
    let revision = cursor.u64()?;
    let status = match cursor.byte()? {
        0 => Status::Member,
        1 => Status::Joining,
        2 => Status::Unknown,
        3 => Status::Deleted,
        _ => return Err(CodecError::Profile),
    };
    let count = cursor.u32()? as usize;
    if count > MAX_PARTITIONS || cursor.0.len() != count * TARGET_BYTES {
        return Err(CodecError::Length);
    }
    let mut targets = Vec::with_capacity(count);
    for _ in 0..count {
        let partition = PartitionIncarnation::from_bytes(cursor.array()?);
        let number = cursor.u32()?;
        let plan_revision = cursor.u64()?;
        let activatable = match cursor.byte()? {
            0 => false,
            1 => true,
            _ => return Err(CodecError::Profile),
        };
        let prior = cursor.array::<16>()?;
        targets.push(Target {
            partition,
            number,
            plan_revision,
            activatable,
            prior_member: (prior != [0; 16]).then(|| ConsumerMemberId::from_bytes(prior)),
        });
    }
    let reply = JoinReply {
        authority,
        coordinator,
        group,
        revision,
        status,
        targets,
    };
    validate_joined(&reply)?;
    Ok(reply)
}

/// Encode one correlated, payload-free activation request.
pub fn encode_assign(
    envelope: Envelope,
    request: AssignRequest,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::GroupAssign, false)?;
    validate_assign(request)?;
    let header = envelope.encode_header(ASSIGN_BYTES, 0, limits)?;
    reserve(metadata, ASSIGN_BYTES)?;
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, request.authority);
    metadata.extend_from_slice(request.partition.as_bytes());
    metadata.extend_from_slice(request.group.as_bytes());
    metadata.extend_from_slice(&member_bytes(request.member));
    metadata.extend_from_slice(&request.plan_revision.to_be_bytes());
    request.initial.encode(metadata);
    metadata.push(request.policy as u8);
    debug_assert_eq!(metadata.len(), ASSIGN_BYTES);
    Ok(header)
}

/// Decode shape and identities. The source validates plan revision and policy.
pub fn decode_assign(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<AssignRequest, CodecError> {
    check(packet, Opcode::GroupAssign, false, limits)?;
    if packet.metadata.len() != ASSIGN_BYTES {
        return Err(CodecError::Length);
    }
    let mut cursor = Cursor(packet.metadata);
    if cursor.byte()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let request = AssignRequest {
        authority: decode_authority(&mut cursor)?,
        partition: PartitionIncarnation::from_bytes(cursor.array()?),
        group: ConsumerGroupId::from_bytes(cursor.array()?),
        member: decode_member(&mut cursor)?,
        plan_revision: cursor.u64()?,
        initial: Start::decode(&mut cursor)?,
        policy: Policy::try_from(cursor.byte()?)?,
    };
    validate_assign(request)?;
    Ok(request)
}

/// Encode confirmed activation coordinates.
pub fn encode_assigned(
    envelope: Envelope,
    reply: AssignReply,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::GroupAssigned, true)?;
    validate_assigned(reply)?;
    let header = envelope.encode_header(ASSIGNED_BYTES, 0, limits)?;
    reserve(metadata, ASSIGNED_BYTES)?;
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, reply.authority);
    metadata.extend_from_slice(reply.partition.as_bytes());
    metadata.extend_from_slice(reply.group.as_bytes());
    metadata.extend_from_slice(&member_bytes(reply.member));
    for value in [
        reply.epoch,
        reply.plan_revision,
        reply.cursor.get(),
        reply.retained_from.get(),
        reply.end.get(),
    ] {
        metadata.extend_from_slice(&value.to_be_bytes());
    }
    metadata.push(reply.policy as u8);
    debug_assert_eq!(metadata.len(), ASSIGNED_BYTES);
    Ok(header)
}

/// Decode activation coordinates before checking the request and link binding.
pub fn decode_assigned(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<AssignReply, CodecError> {
    check(packet, Opcode::GroupAssigned, true, limits)?;
    if packet.metadata.len() != ASSIGNED_BYTES {
        return Err(CodecError::Length);
    }
    let mut cursor = Cursor(packet.metadata);
    if cursor.byte()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let reply = AssignReply {
        authority: decode_authority(&mut cursor)?,
        partition: PartitionIncarnation::from_bytes(cursor.array()?),
        group: ConsumerGroupId::from_bytes(cursor.array()?),
        member: decode_member(&mut cursor)?,
        epoch: cursor.u64()?,
        plan_revision: cursor.u64()?,
        cursor: Offset::new(cursor.u64()?),
        retained_from: Offset::new(cursor.u64()?),
        end: Offset::new(cursor.u64()?),
        policy: Policy::try_from(cursor.byte()?)?,
    };
    validate_assigned(reply)?;
    Ok(reply)
}

fn command(envelope: Envelope, opcode: Opcode, response: bool) -> Result<(), CodecError> {
    if envelope.opcode != opcode
        || envelope.response != response
        || envelope.request_id.is_none()
        || envelope.session.is_none()
    {
        return Err(CodecError::Profile);
    }
    Ok(())
}

fn check(
    packet: Packet<'_>,
    opcode: Opcode,
    response: bool,
    limits: EnvelopeLimits,
) -> Result<(), CodecError> {
    command(packet.envelope, opcode, response)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if !packet.payload.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(())
}

fn reserve(metadata: &mut Vec<u8>, length: usize) -> Result<(), CodecError> {
    if metadata.capacity() < length {
        return Err(CodecError::Capacity);
    }
    metadata.clear();
    Ok(())
}

fn encode_authority(metadata: &mut Vec<u8>, authority: Authority) {
    metadata.extend_from_slice(authority.group_id.as_bytes());
    metadata.extend_from_slice(&authority.config_epoch.to_be_bytes());
    metadata.extend_from_slice(&authority.view.to_be_bytes());
}

fn decode_authority(cursor: &mut Cursor<'_>) -> Result<Authority, CodecError> {
    Ok(Authority {
        group_id: GroupId::from_bytes(cursor.array()?),
        config_epoch: cursor.u64()?,
        view: cursor.u64()?,
    })
}

fn member_bytes(member: Option<ConsumerMemberId>) -> [u8; 16] {
    member.map_or([0; 16], |member| *member.as_bytes())
}

fn decode_member(cursor: &mut Cursor<'_>) -> Result<Option<ConsumerMemberId>, CodecError> {
    let member = cursor.array::<16>()?;
    Ok((member != [0; 16]).then(|| ConsumerMemberId::from_bytes(member)))
}

fn nonzero(bytes: &[u8; 16]) -> Result<(), CodecError> {
    if bytes == &[0; 16] {
        return Err(CodecError::Identity);
    }
    Ok(())
}

fn validate_scope(authority: Authority, partition: PartitionIncarnation) -> Result<(), CodecError> {
    nonzero(authority.group_id.as_bytes())?;
    nonzero(partition.as_bytes())?;
    if authority.config_epoch == 0 {
        return Err(CodecError::Identity);
    }
    Ok(())
}

fn validate_joined(reply: &JoinReply) -> Result<(), CodecError> {
    let created = reply.group.as_bytes() != &[0; 16];
    if created != (reply.revision != 0)
        || (reply.status == Status::Member && !created)
        || (reply.status != Status::Member && !reply.targets.is_empty())
        || reply.targets.len() > MAX_PARTITIONS
    {
        return Err(CodecError::Profile);
    }
    for target in &reply.targets {
        nonzero(target.partition.as_bytes())?;
        if target.plan_revision == 0 || target.plan_revision > reply.revision {
            return Err(CodecError::Position);
        }
    }
    Ok(())
}

fn validate_assign(request: AssignRequest) -> Result<(), CodecError> {
    validate_scope(request.authority, request.partition)?;
    nonzero(request.group.as_bytes())?;
    if request.plan_revision == 0 {
        return Err(CodecError::Identity);
    }
    Ok(())
}

fn validate_assigned(reply: AssignReply) -> Result<(), CodecError> {
    validate_scope(reply.authority, reply.partition)?;
    nonzero(reply.group.as_bytes())?;
    if reply.plan_revision == 0 || reply.epoch == 0 {
        return Err(CodecError::Identity);
    }
    if reply.retained_from > reply.end || reply.cursor > reply.end {
        return Err(CodecError::Profile);
    }
    Ok(())
}

mod admin;
pub use admin::*;

#[cfg(test)]
mod tests;
