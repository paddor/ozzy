//! Cumulative group processing cursors on the addressed control plane.

use crate::{
    ConsumerGroupId, ConsumerMemberId, ENVELOPE_BYTES, Envelope, EnvelopeLimits, Offset, Opcode,
    Packet, PartitionIncarnation,
    append::{Authority, Policy},
    data::CodecError,
    layout::Reader,
    wire::{check_exact, command, decode_authority, encode_authority, reserve},
};

/// Fixed request metadata; queries encode an absent cursor as zero tag/value.
pub const REQUEST_BYTES: usize = 99;
/// Fixed confirmed metadata, including retained floor and exclusive record end.
pub const CONFIRMED_BYTES: usize = 114;
const SOURCE_TAG: u8 = 1;

/// Native code-9 detail for processing progress whose history was retained away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryExpired {
    /// Unavailable exclusive processing cursor, preserved without resetting it.
    pub cursor: Offset,
    /// Confirmed earliest retained record.
    pub retained_from: Offset,
}

impl HistoryExpired {
    /// Encode the exact cursor and retained floor, in that order.
    pub fn encode(self) -> Result<[u8; 16], CodecError> {
        self.validate()?;
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&self.cursor.get().to_be_bytes());
        bytes[8..].copy_from_slice(&self.retained_from.get().to_be_bytes());
        Ok(bytes)
    }

    /// Decode a strict expired-history proof; unrelated NACKs use other details.
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() != 16 {
            return Err(CodecError::Length);
        }
        let mut cursor = Reader::new(bytes);
        let expired = Self {
            cursor: Offset::new(cursor.u64()?),
            retained_from: Offset::new(cursor.u64()?),
        };
        expired.validate()?;
        Ok(expired)
    }

    fn validate(self) -> Result<(), CodecError> {
        if self.cursor >= self.retained_from {
            return Err(CodecError::Profile);
        }
        Ok(())
    }
}

/// Exact source assignment, independent of transport sessions and leader views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Assignment {
    /// Persistent consumer-group incarnation.
    pub group: ConsumerGroupId,
    /// Persistent source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Current processing member.
    pub member: ConsumerMemberId,
    /// Nonzero source assignment epoch.
    pub epoch: u64,
}

/// Query confirmed progress or request a cumulative exclusive cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request {
    /// Expected source leader generation.
    pub authority: Authority,
    /// Source ownership under which these records were processed.
    pub assignment: Assignment,
    /// Required immutable source confirmation boundary.
    pub policy: Policy,
    /// Absent queries confirmed state; present advances cumulative progress.
    pub cursor: Option<Offset>,
}

/// Source coordinates after the configured journal confirmation boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Confirmed {
    /// Confirming source leader generation.
    pub authority: Authority,
    /// Exact assignment checked by the source owner.
    pub assignment: Assignment,
    /// Configured confirmation policy.
    pub policy: Policy,
    /// Confirmed cumulative exclusive processing offset.
    pub cursor: Offset,
    /// Earliest retained record at owner validation.
    pub retained_from: Offset,
    /// Exclusive confirmed record end at owner validation.
    pub end: Offset,
}

/// Encode one correlated, payload-free processing request into reserved metadata.
pub fn encode_request(
    envelope: Envelope,
    request: Request,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::ProgressCommit, false)?;
    validate(request.authority, request.assignment)?;
    let header = envelope.encode_header(REQUEST_BYTES, 0, limits)?;
    reserve(metadata, REQUEST_BYTES)?;
    encode_common(
        metadata,
        request.authority,
        request.assignment,
        request.policy,
    );
    metadata.push(u8::from(request.cursor.is_some()));
    metadata.extend_from_slice(&request.cursor.unwrap_or(Offset::ZERO).get().to_be_bytes());
    Ok(header)
}

/// Decode shape and identities. Link trust and assignment remain owner checks.
pub fn decode_request(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<Request, CodecError> {
    check_exact(packet, Opcode::ProgressCommit, false, REQUEST_BYTES, limits)?;
    let mut cursor = Reader::new(packet.metadata);
    let (authority, assignment, policy) = decode_common(&mut cursor)?;
    let tag = cursor.u8()?;
    let value = cursor.u64()?;
    let offset = match (tag, value) {
        (0, 0) => None,
        (1, value) => Some(Offset::new(value)),
        _ => return Err(CodecError::Profile),
    };
    Ok(Request {
        authority,
        assignment,
        policy,
        cursor: offset,
    })
}

/// Encode confirmed processing coordinates, never a transport receipt.
pub fn encode_confirmed(
    envelope: Envelope,
    confirmed: Confirmed,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::ProgressCommitted, true)?;
    validate_confirmed(confirmed)?;
    let header = envelope.encode_header(CONFIRMED_BYTES, 0, limits)?;
    reserve(metadata, CONFIRMED_BYTES)?;
    encode_common(
        metadata,
        confirmed.authority,
        confirmed.assignment,
        confirmed.policy,
    );
    for offset in [confirmed.cursor, confirmed.retained_from, confirmed.end] {
        metadata.extend_from_slice(&offset.get().to_be_bytes());
    }
    Ok(header)
}

/// Decode confirmed coordinates before checking their request and link binding.
pub fn decode_confirmed(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<Confirmed, CodecError> {
    check_exact(
        packet,
        Opcode::ProgressCommitted,
        true,
        CONFIRMED_BYTES,
        limits,
    )?;
    let mut cursor = Reader::new(packet.metadata);
    let (authority, assignment, policy) = decode_common(&mut cursor)?;
    let confirmed = Confirmed {
        authority,
        assignment,
        policy,
        cursor: Offset::new(cursor.u64()?),
        retained_from: Offset::new(cursor.u64()?),
        end: Offset::new(cursor.u64()?),
    };
    validate_confirmed(confirmed)?;
    Ok(confirmed)
}

fn encode_common(
    metadata: &mut Vec<u8>,
    authority: Authority,
    assignment: Assignment,
    policy: Policy,
) {
    metadata.push(SOURCE_TAG);
    encode_authority(metadata, authority);
    metadata.extend_from_slice(assignment.partition.as_bytes());
    metadata.extend_from_slice(assignment.group.as_bytes());
    metadata.extend_from_slice(assignment.member.as_bytes());
    metadata.extend_from_slice(&assignment.epoch.to_be_bytes());
    metadata.push(policy as u8);
}

fn decode_common(cursor: &mut Reader<'_>) -> Result<(Authority, Assignment, Policy), CodecError> {
    if cursor.u8()? != SOURCE_TAG {
        return Err(CodecError::Profile);
    }
    let authority = decode_authority(cursor)?;
    let assignment = Assignment {
        partition: PartitionIncarnation::from_bytes(cursor.array()?),
        group: ConsumerGroupId::from_bytes(cursor.array()?),
        member: ConsumerMemberId::from_bytes(cursor.array()?),
        epoch: cursor.u64()?,
    };
    let policy = Policy::try_from(cursor.u8()?)?;
    validate(authority, assignment)?;
    Ok((authority, assignment, policy))
}

fn validate(authority: Authority, assignment: Assignment) -> Result<(), CodecError> {
    if authority.group_id.as_bytes() == &[0; 16]
        || authority.config_epoch == 0
        || assignment.group.as_bytes() == &[0; 16]
        || assignment.partition.as_bytes() == &[0; 16]
        || assignment.member.as_bytes() == &[0; 16]
        || assignment.epoch == 0
    {
        return Err(CodecError::Identity);
    }
    Ok(())
}

fn validate_confirmed(confirmed: Confirmed) -> Result<(), CodecError> {
    validate(confirmed.authority, confirmed.assignment)?;
    if confirmed.retained_from > confirmed.end || confirmed.cursor > confirmed.end {
        return Err(CodecError::Profile);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
