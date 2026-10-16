//! Compact per-writer confirmations selected by the owner-stream capability.
//!
//! Receive batches, replicated operations, and these ranges are independent.
//! Session, request, capability, authority, and confirmation validation remain
//! runtime obligations; this codec establishes the structure only.

use super::{
    AppendDisposition, AppendKey, Authority, CodecError, ENVELOPE_BYTES, Envelope, Opcode, Packet,
    PartitionIncarnation, Policy, Reader, capacity, command, decode_common, encode_common,
    identities,
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
    /// Whether these attempts stored records or reused original offsets.
    pub disposition: AppendDisposition,
    /// Exact achieved policy, requiring comparison with the writer's request.
    pub policy: Policy,
}

impl Confirmed {
    /// Shared outcome representation for this consecutive producer sequence run.
    pub fn outcomes(self) -> Result<super::OutcomeRun, CodecError> {
        let records = self
            .end_sequence
            .checked_sub(self.key.first_sequence)
            .and_then(|count| u32::try_from(count).ok())
            .ok_or(CodecError::Length)?;
        let run = super::OutcomeRun {
            disposition: self.disposition,
            first_offset: crate::Offset::new(self.first_offset),
            records,
        };
        run.encode()?;
        Ok(run)
    }
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
    let header = envelope.encode_header(107, 0, limits)?;
    capacity(metadata, 107)?;
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
    metadata.push(confirmed.disposition as u8);
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
    if !packet.payload.is_empty() || packet.metadata.len() != 107 {
        return Err(CodecError::Length);
    }
    let mut cursor = Reader::new(packet.metadata);
    let (authority, partition, owner_epoch, key) = decode_common(&mut cursor)?;
    let confirmed = Confirmed {
        authority,
        partition,
        owner_epoch,
        key,
        end_sequence: cursor.u64()?,
        first_offset: cursor.u64()?,
        disposition: AppendDisposition::try_from(cursor.u8()?)?,
        policy: Policy::try_from(cursor.u8()?)?,
    };
    range(confirmed)?;
    Ok(confirmed)
}

fn range(confirmed: Confirmed) -> Result<(), CodecError> {
    confirmed.outcomes().map(|_| ())
}
