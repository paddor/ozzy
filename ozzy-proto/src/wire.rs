//! Field and frame checks shared by the correlated control codecs.

use crate::{
    Envelope, EnvelopeLimits, GroupId, Opcode, Packet, append::Authority, data::CodecError,
    layout::Reader,
};

/// Require a correlated request or reply with this opcode.
pub(crate) fn command(
    envelope: Envelope,
    opcode: Opcode,
    response: bool,
) -> Result<(), CodecError> {
    if envelope.opcode != opcode
        || envelope.response != response
        || envelope.request_id.is_none()
        || envelope.session.is_none()
    {
        return Err(CodecError::Profile);
    }
    Ok(())
}

/// Check a metadata-only packet's command and frame bounds.
pub(crate) fn check(
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

/// [`check`] a packet whose metadata has a fixed `length`.
pub(crate) fn check_exact(
    packet: Packet<'_>,
    opcode: Opcode,
    response: bool,
    length: usize,
    limits: EnvelopeLimits,
) -> Result<(), CodecError> {
    check(packet, opcode, response, limits)?;
    if packet.metadata.len() != length {
        return Err(CodecError::Length);
    }
    Ok(())
}

/// Clear `metadata` for an encoding of `length` bytes it can hold.
pub(crate) fn reserve(metadata: &mut Vec<u8>, length: usize) -> Result<(), CodecError> {
    if metadata.capacity() < length {
        return Err(CodecError::Capacity);
    }
    metadata.clear();
    Ok(())
}

pub(crate) fn encode_authority(metadata: &mut Vec<u8>, authority: Authority) {
    metadata.extend_from_slice(authority.group_id.as_bytes());
    metadata.extend_from_slice(&authority.config_epoch.to_be_bytes());
    metadata.extend_from_slice(&authority.view.to_be_bytes());
}

pub(crate) fn decode_authority(cursor: &mut Reader<'_>) -> Result<Authority, CodecError> {
    Ok(Authority {
        group_id: GroupId::from_bytes(cursor.array()?),
        config_epoch: cursor.u64()?,
        view: cursor.u64()?,
    })
}

/// Reject an all-zero identity.
pub(crate) fn nonzero(bytes: &[u8; 16]) -> Result<(), CodecError> {
    if bytes == &[0; 16] {
        return Err(CodecError::Identity);
    }
    Ok(())
}
