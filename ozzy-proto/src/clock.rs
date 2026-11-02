//! A broker's wall time, sent to a peer on an established link.

use crate::{ENVELOPE_BYTES, Envelope, EnvelopeLimits, Opcode, Packet, data::CodecError};

/// Fixed metadata: wall time in Unix milliseconds.
pub const SAMPLE_BYTES: usize = 8;

/// Encode one uncorrelated, payload-free sample.
pub fn encode_sample(
    envelope: Envelope,
    wall_millis: u64,
    limits: EnvelopeLimits,
) -> Result<([u8; ENVELOPE_BYTES], [u8; SAMPLE_BYTES]), CodecError> {
    check(envelope)?;
    let header = envelope.encode_header(SAMPLE_BYTES, 0, limits)?;
    Ok((header, wall_millis.to_be_bytes()))
}

/// Decode shape only. Link trust remains the receiver's check.
pub fn decode_sample(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<u64, CodecError> {
    check(packet.envelope)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if !packet.payload.is_empty() {
        return Err(CodecError::Length);
    }
    let wall: [u8; SAMPLE_BYTES] = packet.metadata.try_into().map_err(|_| CodecError::Length)?;
    Ok(u64::from_be_bytes(wall))
}

fn check(envelope: Envelope) -> Result<(), CodecError> {
    if envelope.opcode != Opcode::ClockSample
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
    use crate::{LinkSessionId, NodeId, RequestId, decode_packet};

    fn envelope() -> Envelope {
        Envelope {
            opcode: Opcode::ClockSample,
            response: false,
            request_id: None,
            sender: NodeId::from_bytes([1; 16]),
            session: Some(LinkSessionId::from_bytes([2; 16])),
        }
    }

    #[test]
    fn a_sample_round_trips_in_network_order() {
        let limits = EnvelopeLimits::default();
        let (header, metadata) = encode_sample(envelope(), 0x0102_0304_0506_0708, limits).unwrap();
        assert_eq!(metadata, [1, 2, 3, 4, 5, 6, 7, 8]);
        let packet = decode_packet(&[&header, &metadata, &[]], limits).unwrap();
        assert_eq!(packet.envelope, envelope());
        assert_eq!(
            decode_sample(packet, limits).unwrap(),
            0x0102_0304_0506_0708
        );
    }

    #[test]
    fn a_wrong_shape_is_refused() {
        let limits = EnvelopeLimits::default();
        for wrong in [
            Envelope {
                opcode: Opcode::Commit,
                ..envelope()
            },
            Envelope {
                request_id: Some(RequestId::from_bytes([3; 16])),
                ..envelope()
            },
            Envelope {
                response: true,
                request_id: Some(RequestId::from_bytes([3; 16])),
                ..envelope()
            },
        ] {
            assert!(encode_sample(wrong, 1, limits).is_err());
        }
        let (header, metadata) = encode_sample(envelope(), 1, limits).unwrap();
        let short = envelope().encode_header(4, 0, limits).unwrap();
        let packet = decode_packet(&[&short, &metadata[..4], &[]], limits).unwrap();
        assert!(decode_sample(packet, limits).is_err());
        let padded = envelope().encode_header(8, 1, limits).unwrap();
        let packet = decode_packet(&[&padded, &metadata, &[0]], limits).unwrap();
        assert!(decode_sample(packet, limits).is_err());
        assert!(decode_packet(&[&header, &metadata, &[]], limits).is_ok());
    }
}
