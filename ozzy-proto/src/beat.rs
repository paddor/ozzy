//! The sources of a broker's live shards and their epochs, sent to a peer
//! on an established link. A shard reports several sources, each for a
//! part of its groups, under numbers of its own.

use crate::{ENVELOPE_BYTES, Envelope, EnvelopeLimits, Opcode, Packet, data::CodecError};

/// Metadata bytes of one source: its number and its epoch.
pub const SHARD_BYTES: usize = 12;

/// Most sources one beat names.
pub const MAX_SHARDS: usize = 4096;

/// One source of the sender and the epoch it reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardEpoch {
    /// The sender's own number of the source. It means nothing on another
    /// broker.
    pub shard: u32,
    /// Changes whenever the source withdraws what was said under the last
    /// one.
    pub epoch: u64,
}

impl ShardEpoch {
    /// The wire form: the number, then the epoch, both big-endian.
    pub const fn to_bytes(self) -> [u8; SHARD_BYTES] {
        let mut bytes = [0; SHARD_BYTES];
        let (shard, epoch) = bytes.split_at_mut(4);
        shard.copy_from_slice(&self.shard.to_be_bytes());
        epoch.copy_from_slice(&self.epoch.to_be_bytes());
        bytes
    }

    /// Read the wire form back.
    pub const fn from_bytes([a, b, c, d, epoch @ ..]: [u8; SHARD_BYTES]) -> Self {
        Self {
            shard: u32::from_be_bytes([a, b, c, d]),
            epoch: u64::from_be_bytes(epoch),
        }
    }
}

/// Encode one uncorrelated, payload-free beat into `metadata`, which is
/// cleared first.
pub fn encode(
    envelope: Envelope,
    shards: &[ShardEpoch],
    limits: EnvelopeLimits,
    metadata: &mut Vec<u8>,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    check(envelope)?;
    if shards.len() > MAX_SHARDS {
        return Err(CodecError::Length);
    }
    let header = envelope.encode_header(shards.len().saturating_mul(SHARD_BYTES), 0, limits)?;
    metadata.clear();
    for shard in shards {
        metadata.extend_from_slice(&shard.to_bytes());
    }
    Ok(header)
}

/// Decode shape only. Link trust remains the receiver's check.
pub fn decode(
    packet: Packet<'_>,
    limits: EnvelopeLimits,
) -> Result<impl Iterator<Item = ShardEpoch> + '_, CodecError> {
    check(packet.envelope)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if !packet.payload.is_empty()
        || !packet.metadata.len().is_multiple_of(SHARD_BYTES)
        || packet.metadata.len() > MAX_SHARDS.saturating_mul(SHARD_BYTES)
    {
        return Err(CodecError::Length);
    }
    let (shards, _) = packet.metadata.as_chunks::<SHARD_BYTES>();
    Ok(shards.iter().copied().map(ShardEpoch::from_bytes))
}

fn check(envelope: Envelope) -> Result<(), CodecError> {
    if envelope.opcode != Opcode::Beat
        || envelope.response
        || envelope.request_id.is_some()
        || envelope.session.is_none()
    {
        return Err(CodecError::Profile);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LinkSessionId, NodeId, decode_packet};

    fn envelope() -> Envelope {
        Envelope {
            opcode: Opcode::Beat,
            response: false,
            request_id: None,
            sender: NodeId::from_bytes([1; 16]),
            session: Some(LinkSessionId::from_bytes([2; 16])),
        }
    }

    #[test]
    fn a_beat_round_trips_its_shards_in_order() {
        let limits = EnvelopeLimits::default();
        let shards = [
            ShardEpoch { shard: 3, epoch: 7 },
            ShardEpoch {
                shard: 0,
                epoch: u64::MAX,
            },
        ];
        let mut metadata = vec![9];
        let header = encode(envelope(), &shards, limits, &mut metadata).unwrap();
        assert_eq!(metadata.len(), 2 * SHARD_BYTES);
        let packet = decode_packet(&[&header, &metadata, &[]], limits).unwrap();
        assert_eq!(decode(packet, limits).unwrap().collect::<Vec<_>>(), shards);
        let header = encode(envelope(), &[], limits, &mut metadata).unwrap();
        let packet = decode_packet(&[&header, &metadata, &[]], limits).unwrap();
        assert_eq!(decode(packet, limits).unwrap().count(), 0);
    }

    #[test]
    fn a_wrong_shape_is_refused() {
        let limits = EnvelopeLimits::default();
        let mut metadata = Vec::new();
        let wrong = Envelope {
            opcode: Opcode::ClockSample,
            ..envelope()
        };
        assert!(encode(wrong, &[], limits, &mut metadata).is_err());
        let many = vec![ShardEpoch { shard: 0, epoch: 0 }; MAX_SHARDS + 1];
        assert!(encode(envelope(), &many, limits, &mut metadata).is_err());
        let ragged = envelope().encode_header(5, 0, limits).unwrap();
        let packet = decode_packet(&[&ragged, &[0; 5], &[]], limits).unwrap();
        assert!(decode(packet, limits).is_err());
        let padded = envelope().encode_header(0, 1, limits).unwrap();
        let packet = decode_packet(&[&padded, &[], &[0]], limits).unwrap();
        assert!(decode(packet, limits).is_err());
    }
}
