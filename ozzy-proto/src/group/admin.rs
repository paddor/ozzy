//! Group inspection and deletion on the coordinator. Replies follow the
//! coordinator confirmation boundary; deletion fences sources separately.

use super::{
    MAX_MEMBERS, MAX_NAME_BYTES, MAX_PARTITIONS, Reader, SOURCE_TAG, Start, check, command,
    decode_authority, decode_member, encode_authority, member_bytes, nonzero, reserve,
    validate_scope,
};
use crate::{
    ConsumerGroupId, ConsumerMemberId, ENVELOPE_BYTES, Envelope, EnvelopeLimits, NodeId, Opcode,
    OperationId, Packet, PartitionIncarnation, append::Authority, data::CodecError,
};

/// Fixed inspection request metadata before the name.
pub const INSPECT_FIXED_BYTES: usize = 50;
/// Fixed inspection reply metadata before members and targets.
pub const INSPECTED_FIXED_BYTES: usize = 90;
/// Bytes per member in an inspection reply.
pub const INSPECTED_MEMBER_BYTES: usize = 48;
/// Bytes per target in an inspection reply.
pub const INSPECTED_TARGET_BYTES: usize = 44;
/// Fixed deletion request metadata before the name.
pub const DELETE_FIXED_BYTES: usize = 66;
/// Deletion reply metadata.
pub const DELETED_BYTES: usize = 66;

/// Read the committed coordinator image of one topic-scoped group name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectRequest {
    /// Expected coordinator leader generation.
    pub authority: Authority,
    /// Deterministic coordinator partition of this topic and group name.
    pub coordinator: PartitionIncarnation,
    /// Topic-scoped group name.
    pub name: String,
}

/// One processing instance in a committed coordinator image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InspectedMember {
    /// Application member identity.
    pub id: ConsumerMemberId,
    /// Physical client identity.
    pub node: NodeId,
    /// Join identity of this processing instance.
    pub join: OperationId,
}

/// Committed intent for one source; sources confirm activation separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InspectedTarget {
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Numeric topic partition.
    pub number: u32,
    /// Last revision that changed this source's owner.
    pub plan_revision: u64,
    /// Intended owner, if any.
    pub member: Option<ConsumerMemberId>,
}

/// Committed coordinator image. Without a group of that name, the group is
/// absent, the revision is zero, and no members or targets follow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Inspection {
    /// Coordinator leader generation that served this reply.
    pub authority: Authority,
    /// Coordinator partition incarnation.
    pub coordinator: PartitionIncarnation,
    /// Group incarnation currently holding the name.
    pub group: Option<ConsumerGroupId>,
    /// Committed coordinator revision.
    pub revision: u64,
    /// First position of sources without a cursor.
    pub initial: Start,
    /// Processing instances in member ID order.
    pub members: Vec<InspectedMember>,
    /// Intended ownership in numeric partition order.
    pub targets: Vec<InspectedTarget>,
}

/// Retire one group incarnation if the name still holds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRequest {
    /// Expected coordinator leader generation.
    pub authority: Authority,
    /// Deterministic coordinator partition of this topic and group name.
    pub coordinator: PartitionIncarnation,
    /// Incarnation to retire.
    pub group: ConsumerGroupId,
    /// Topic-scoped group name.
    pub name: String,
}

/// Deletion outcome after the coordinator confirmation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteReply {
    /// Confirming coordinator leader generation.
    pub authority: Authority,
    /// Coordinator partition incarnation.
    pub coordinator: PartitionIncarnation,
    /// Requested incarnation.
    pub group: ConsumerGroupId,
    /// This request retired the incarnation; false means it was already gone.
    pub deleted: bool,
}

/// Exact inspection request metadata bytes.
pub fn inspect_bytes(name: &str) -> Result<usize, CodecError> {
    name_bytes(name).and_then(|name| crate::data::add(INSPECT_FIXED_BYTES, name))
}

/// Exact inspection reply metadata bytes.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "both counts are capped first, far below overflow"
)]
pub fn inspected_bytes(members: usize, targets: usize) -> Result<usize, CodecError> {
    if members > MAX_MEMBERS || targets > MAX_PARTITIONS {
        return Err(CodecError::Limit);
    }
    Ok(INSPECTED_FIXED_BYTES + members * INSPECTED_MEMBER_BYTES + targets * INSPECTED_TARGET_BYTES)
}

/// Exact deletion request metadata bytes.
pub fn delete_bytes(name: &str) -> Result<usize, CodecError> {
    name_bytes(name).and_then(|name| crate::data::add(DELETE_FIXED_BYTES, name))
}

/// Encode one correlated, payload-free inspection request.
pub fn encode_inspect(
    envelope: Envelope,
    request: &InspectRequest,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::GroupInspect, false)?;
    validate_scope(request.authority, request.coordinator)?;
    let length = inspect_bytes(&request.name)?;
    let header = envelope.encode_header(length, 0, limits)?;
    reserve(metadata, length)?;
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, request.authority);
    metadata.extend_from_slice(request.coordinator.as_bytes());
    encode_name(metadata, &request.name);
    debug_assert_eq!(metadata.len(), length);
    Ok(header)
}

/// Decode shape and identities. Link trust and placement remain owner checks.
pub fn decode_inspect(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<InspectRequest, CodecError> {
    check(packet, Opcode::GroupInspect, false, limits)?;
    let mut cursor = Reader::new(packet.metadata);
    let (authority, coordinator) = decode_scope(&mut cursor)?;
    let name = decode_name(&mut cursor)?;
    if !cursor.rest().is_empty() {
        return Err(CodecError::Length);
    }
    Ok(InspectRequest {
        authority,
        coordinator,
        name,
    })
}

/// Encode a committed coordinator image.
pub fn encode_inspected(
    envelope: Envelope,
    reply: &Inspection,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::GroupInspected, true)?;
    validate_scope(reply.authority, reply.coordinator)?;
    validate_inspection(reply)?;
    let length = inspected_bytes(reply.members.len(), reply.targets.len())?;
    let header = envelope.encode_header(length, 0, limits)?;
    reserve(metadata, length)?;
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, reply.authority);
    metadata.extend_from_slice(reply.coordinator.as_bytes());
    metadata.extend_from_slice(&reply.group.map_or([0; 16], |group| *group.as_bytes()));
    metadata.extend_from_slice(&reply.revision.to_be_bytes());
    reply.initial.encode(metadata);
    metadata.extend_from_slice(&(reply.members.len() as u32).to_be_bytes());
    for member in &reply.members {
        metadata.extend_from_slice(member.id.as_bytes());
        metadata.extend_from_slice(member.node.as_bytes());
        metadata.extend_from_slice(member.join.as_bytes());
    }
    metadata.extend_from_slice(&(reply.targets.len() as u32).to_be_bytes());
    for target in &reply.targets {
        metadata.extend_from_slice(target.partition.as_bytes());
        metadata.extend_from_slice(&target.number.to_be_bytes());
        metadata.extend_from_slice(&target.plan_revision.to_be_bytes());
        metadata.extend_from_slice(&member_bytes(target.member));
    }
    debug_assert_eq!(metadata.len(), length);
    Ok(header)
}

/// Decode a coordinator image before checking its request and link binding.
pub fn decode_inspected(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<Inspection, CodecError> {
    check(packet, Opcode::GroupInspected, true, limits)?;
    let mut cursor = Reader::new(packet.metadata);
    let (authority, coordinator) = decode_scope(&mut cursor)?;
    let group = cursor.array::<16>()?;
    let revision = cursor.u64()?;
    let initial = Start::decode(&mut cursor)?;
    let count = cursor.u32()? as usize;
    if count > MAX_MEMBERS || cursor.rest().len() < count * INSPECTED_MEMBER_BYTES {
        return Err(CodecError::Length);
    }
    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        members.push(InspectedMember {
            id: ConsumerMemberId::from_bytes(cursor.array()?),
            node: NodeId::from_bytes(cursor.array()?),
            join: OperationId::from_bytes(cursor.array()?),
        });
    }
    let count = cursor.u32()? as usize;
    if count > MAX_PARTITIONS || cursor.rest().len() != count * INSPECTED_TARGET_BYTES {
        return Err(CodecError::Length);
    }
    let mut targets = Vec::with_capacity(count);
    for _ in 0..count {
        targets.push(InspectedTarget {
            partition: PartitionIncarnation::from_bytes(cursor.array()?),
            number: cursor.u32()?,
            plan_revision: cursor.u64()?,
            member: decode_member(&mut cursor)?,
        });
    }
    let reply = Inspection {
        authority,
        coordinator,
        group: (group != [0; 16]).then(|| ConsumerGroupId::from_bytes(group)),
        revision,
        initial,
        members,
        targets,
    };
    validate_inspection(&reply)?;
    Ok(reply)
}

/// Encode one correlated, payload-free deletion request.
pub fn encode_delete(
    envelope: Envelope,
    request: &DeleteRequest,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::GroupDelete, false)?;
    validate_scope(request.authority, request.coordinator)?;
    nonzero(request.group.as_bytes())?;
    let length = delete_bytes(&request.name)?;
    let header = envelope.encode_header(length, 0, limits)?;
    reserve(metadata, length)?;
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, request.authority);
    metadata.extend_from_slice(request.coordinator.as_bytes());
    metadata.extend_from_slice(request.group.as_bytes());
    encode_name(metadata, &request.name);
    debug_assert_eq!(metadata.len(), length);
    Ok(header)
}

/// Decode shape and identities. The coordinator checks the incarnation.
pub fn decode_delete(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<DeleteRequest, CodecError> {
    check(packet, Opcode::GroupDelete, false, limits)?;
    let mut cursor = Reader::new(packet.metadata);
    let (authority, coordinator) = decode_scope(&mut cursor)?;
    let group = ConsumerGroupId::from_bytes(cursor.array()?);
    nonzero(group.as_bytes())?;
    let name = decode_name(&mut cursor)?;
    if !cursor.rest().is_empty() {
        return Err(CodecError::Length);
    }
    Ok(DeleteRequest {
        authority,
        coordinator,
        group,
        name,
    })
}

/// Encode a confirmed deletion outcome.
pub fn encode_deleted(
    envelope: Envelope,
    reply: DeleteReply,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::GroupDeleted, true)?;
    validate_scope(reply.authority, reply.coordinator)?;
    nonzero(reply.group.as_bytes())?;
    let header = envelope.encode_header(DELETED_BYTES, 0, limits)?;
    reserve(metadata, DELETED_BYTES)?;
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, reply.authority);
    metadata.extend_from_slice(reply.coordinator.as_bytes());
    metadata.extend_from_slice(reply.group.as_bytes());
    metadata.push(u8::from(!reply.deleted));
    debug_assert_eq!(metadata.len(), DELETED_BYTES);
    Ok(header)
}

/// Decode a deletion outcome before checking its request and link binding.
pub fn decode_deleted(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<DeleteReply, CodecError> {
    check(packet, Opcode::GroupDeleted, true, limits)?;
    if packet.metadata.len() != DELETED_BYTES {
        return Err(CodecError::Length);
    }
    let mut cursor = Reader::new(packet.metadata);
    let (authority, coordinator) = decode_scope(&mut cursor)?;
    let group = ConsumerGroupId::from_bytes(cursor.array()?);
    nonzero(group.as_bytes())?;
    let deleted = match cursor.u8()? {
        0 => true,
        1 => false,
        _ => return Err(CodecError::Profile),
    };
    Ok(DeleteReply {
        authority,
        coordinator,
        group,
        deleted,
    })
}

fn name_bytes(name: &str) -> Result<usize, CodecError> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return Err(CodecError::Limit);
    }
    Ok(name.len())
}

fn encode_name(metadata: &mut Vec<u8>, name: &str) {
    metadata.push(name.len() as u8);
    metadata.extend_from_slice(name.as_bytes());
}

fn decode_name(cursor: &mut Reader<'_>) -> Result<String, CodecError> {
    let length = cursor.u8()? as usize;
    if length == 0 {
        return Err(CodecError::Limit);
    }
    std::str::from_utf8(cursor.take(length)?)
        .map(str::to_owned)
        .map_err(|_| CodecError::Profile)
}

fn decode_scope(cursor: &mut Reader<'_>) -> Result<(Authority, PartitionIncarnation), CodecError> {
    if cursor.u8()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let authority = decode_authority(cursor)?;
    let coordinator = PartitionIncarnation::from_bytes(cursor.array()?);
    validate_scope(authority, coordinator)?;
    Ok((authority, coordinator))
}

fn validate_inspection(reply: &Inspection) -> Result<(), CodecError> {
    if reply.group.is_some() != (reply.revision != 0)
        || (reply.group.is_none() && (!reply.members.is_empty() || !reply.targets.is_empty()))
        || reply.members.len() > MAX_MEMBERS
        || reply.targets.len() > MAX_PARTITIONS
    {
        return Err(CodecError::Profile);
    }
    for member in &reply.members {
        nonzero(member.id.as_bytes())?;
        nonzero(member.node.as_bytes())?;
        nonzero(member.join.as_bytes())?;
    }
    for target in &reply.targets {
        nonzero(target.partition.as_bytes())?;
        if target.plan_revision == 0 || target.plan_revision > reply.revision {
            return Err(CodecError::Position);
        }
    }
    Ok(())
}
