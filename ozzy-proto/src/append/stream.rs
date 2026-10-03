//! Compact per-writer confirmations selected by the owner-stream capability.
//!
//! Receive batches, replicated operations, and these ranges are independent.
//! Session, request, capability, authority, and confirmation validation remain
//! runtime obligations; this codec establishes the structure only.

use super::{
    AppendKey, Authority, CodecError, Cursor, ENVELOPE_BYTES, Envelope, Opcode, Packet,
    PartitionIncarnation, Policy, capacity, command, decode_common, encode_common, identities,
};
use crate::EnvelopeLimits;

/// Contiguous record range that reached the configured confirmation boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Confirmed {
    /// Serving group and leader generation, not independently verified evidence.
    pub authority: Authority,
    /// Immutable partition incarnation.
    pub partition: PartitionIncarnation,
    /// Nonzero partition ownership fence.
    pub owner_epoch: u64,
    /// Writer identity, epoch, and first confirmed sequence.
    pub key: AppendKey,
    /// Exclusive confirmed sequence end; strictly greater than the first.
    pub end_sequence: u64,
    /// Offset assigned to the first confirmed sequence.
    pub first_offset: u64,
    /// Exact achieved policy, requiring comparison with the writer's request.
    pub policy: Policy,
}

/// Encode a fixed-size range into reserved metadata. Payload must be empty.
/// Rejection leaves the buffer unchanged; encoding never grows it.
pub fn encode_confirmed(
    envelope: Envelope,
    confirmed: Confirmed,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::Appended, true)?;
    identities(
        confirmed.authority,
        confirmed.partition,
        confirmed.owner_epoch,
        confirmed.key,
    )?;
    range(confirmed)?;
    let header = envelope.encode_header(106, 0, limits)?;
    capacity(metadata, 106)?;
    metadata.clear();
    encode_common(
        metadata,
        confirmed.authority,
        confirmed.partition,
        confirmed.owner_epoch,
        confirmed.key,
    );
    metadata.extend_from_slice(&confirmed.end_sequence.to_be_bytes());
    metadata.extend_from_slice(&confirmed.first_offset.to_be_bytes());
    metadata.push(confirmed.policy as u8);
    Ok(header)
}

/// Decode one range without allocating or inferring application confirmation.
pub fn decode_confirmed(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<Confirmed, CodecError> {
    command(packet.envelope, Opcode::Appended, true)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if !packet.payload.is_empty() || packet.metadata.len() != 106 {
        return Err(CodecError::Length);
    }
    let mut cursor = Cursor(packet.metadata);
    let (authority, partition, owner_epoch, key) = decode_common(&mut cursor)?;
    let confirmed = Confirmed {
        authority,
        partition,
        owner_epoch,
        key,
        end_sequence: cursor.u64()?,
        first_offset: cursor.u64()?,
        policy: Policy::try_from(cursor.byte()?)?,
    };
    range(confirmed)?;
    Ok(confirmed)
}

fn range(confirmed: Confirmed) -> Result<(), CodecError> {
    let count = confirmed
        .end_sequence
        .checked_sub(confirmed.key.first_sequence)
        .filter(|&count| count != 0)
        .ok_or(CodecError::Length)?;
    confirmed
        .first_offset
        .checked_add(count)
        .ok_or(CodecError::Length)?;
    Ok(())
}
