//! Join, heartbeat, and leave requests and their membership replies.

use super::{
    CodecError, ConsumerGroupId, ConsumerMemberId, ENVELOPE_BYTES, Envelope, EnvelopeLimits,
    JOIN_FIXED_BYTES, JOINED_FIXED_BYTES, JoinReply, JoinRequest, MAX_NAME_BYTES, MAX_PARTITIONS,
    Mode, OWNED_BYTES, Opcode, OperationId, Owned, Packet, PartitionIncarnation, Reader,
    SOURCE_TAG, Start, Status, TARGET_BYTES, Target, check, command, decode_authority,
    encode_authority, nonzero, reserve, validate_scope,
};

/// Exact join request metadata bytes.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the name and count are capped first, far below overflow"
)]
pub fn join_bytes(name: &str, owned: usize) -> Result<usize, CodecError> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || owned > MAX_PARTITIONS {
        return Err(CodecError::Limit);
    }
    Ok(JOIN_FIXED_BYTES + name.len() + owned * OWNED_BYTES)
}

/// Exact membership reply metadata bytes.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the count is capped first, far below overflow"
)]
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
    let mut cursor = Reader::new(packet.metadata);
    if cursor.u8()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let authority = decode_authority(&mut cursor)?;
    let coordinator = PartitionIncarnation::from_bytes(cursor.array()?);
    validate_scope(authority, coordinator)?;
    let name = cursor.u8()? as usize;
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
    let mode = match cursor.u8()? {
        0 => Mode::Heartbeat,
        1 => Mode::Join,
        2 => Mode::Leave,
        _ => return Err(CodecError::Profile),
    };
    let initial = Start::decode(&mut cursor)?;
    let count = cursor.u32()? as usize;
    if count > MAX_PARTITIONS || cursor.rest().len() != count * OWNED_BYTES {
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
    let mut cursor = Reader::new(packet.metadata);
    if cursor.u8()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let authority = decode_authority(&mut cursor)?;
    let coordinator = PartitionIncarnation::from_bytes(cursor.array()?);
    validate_scope(authority, coordinator)?;
    let group = ConsumerGroupId::from_bytes(cursor.array()?);
    let revision = cursor.u64()?;
    let status = match cursor.u8()? {
        0 => Status::Member,
        1 => Status::Joining,
        2 => Status::Unknown,
        3 => Status::Deleted,
        _ => return Err(CodecError::Profile),
    };
    let count = cursor.u32()? as usize;
    if count > MAX_PARTITIONS || cursor.rest().len() != count * TARGET_BYTES {
        return Err(CodecError::Length);
    }
    let mut targets = Vec::with_capacity(count);
    for _ in 0..count {
        let partition = PartitionIncarnation::from_bytes(cursor.array()?);
        let number = cursor.u32()?;
        let plan_revision = cursor.u64()?;
        let activatable = match cursor.u8()? {
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
