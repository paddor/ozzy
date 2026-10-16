//! Activation of a committed target on its source partition.

use super::{
    ASSIGN_BYTES, ASSIGNED_BYTES, AssignReply, AssignRequest, CodecError, ConsumerGroupId,
    ENVELOPE_BYTES, Envelope, EnvelopeLimits, Offset, Opcode, Packet, PartitionIncarnation, Policy,
    Reader, SOURCE_TAG, Start, check, command, decode_authority, decode_member, encode_authority,
    member_bytes, nonzero, reserve, validate_scope,
};

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
    let mut cursor = Reader::new(packet.metadata);
    if cursor.u8()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let request = AssignRequest {
        authority: decode_authority(&mut cursor)?,
        partition: PartitionIncarnation::from_bytes(cursor.array()?),
        group: ConsumerGroupId::from_bytes(cursor.array()?),
        member: decode_member(&mut cursor)?,
        plan_revision: cursor.u64()?,
        initial: Start::decode(&mut cursor)?,
        policy: Policy::try_from(cursor.u8()?)?,
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
    let mut cursor = Reader::new(packet.metadata);
    if cursor.u8()? != SOURCE_TAG {
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
        policy: Policy::try_from(cursor.u8()?)?,
    };
    validate_assigned(reply)?;
    Ok(reply)
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
