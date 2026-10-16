//! One shared PREPARE on the group topic; receiver admission remains local.

use super::{
    PeerBinding, PrepareBatch, ReadFields, Reader, WireError, WireLimits, WriteFields, Writer,
    prepare,
};
use crate::{Prefix, Scope};
use ozzy_proto::{GroupId, NodeId, Opcode};

/// One group ID is the complete subscription prefix.
pub const PUBLICATION_TOPIC_BYTES: usize = 16;
/// Publication prefix size; no recipient masks or per-follower credit fields.
pub const PUBLICATION_BYTES: usize = PUBLICATION_TOPIC_BYTES;

/// Common topic for every follower of this group.
pub fn publication_topic(group: GroupId) -> [u8; PUBLICATION_TOPIC_BYTES] {
    *group.as_bytes()
}

/// Reframe an actor-owned PREPARE template without copying or hashing its body.
/// The caller retains the original, already validated payload for publication.
pub fn encode_publication(
    sender: NodeId,
    template: &[&[u8]],
    committed: Prefix,
    metadata: &mut [u8],
    limits: WireLimits,
) -> Result<super::PrepareEncoding, WireError> {
    let (packet, skip) = publication_template(sender, template, limits)?;
    super::validate_prefix(committed)?;
    let length = packet
        .metadata
        .len()
        .checked_sub(skip)
        .ok_or(WireError::Peer)?;
    let header = ozzy_proto::Envelope {
        opcode: Opcode::PreparePub,
        response: false,
        request_id: None,
        sender,
        session: None,
    }
    .encode_header(length, packet.payload.len(), limits.envelope)?;
    let (common, tail) = template_fields(packet.metadata, skip)?;
    let mut writer = Writer::with_len(metadata, length)?;
    write_head(&mut writer, common, tail, committed)?;
    writer.bytes(tail.get(80..).ok_or(WireError::Length)?)?;
    Ok(super::PrepareEncoding {
        header,
        metadata_bytes: length,
        payload_bytes: packet.payload.len(),
    })
}

/// Split template metadata into its 80-byte scope and the fields after the
/// `skip` bytes of a flow epoch: predecessor (40), commit (40), count (4), and
/// operation descriptors.
fn template_fields(metadata: &[u8], skip: usize) -> Result<(&[u8], &[u8]), WireError> {
    let (common, rest) = metadata.split_at_checked(80).ok_or(WireError::Length)?;
    Ok((common, rest.get(skip..).ok_or(WireError::Length)?))
}

/// Write the scope and predecessor of `tail` with a new commit prefix.
fn write_head(
    writer: &mut Writer<'_>,
    common: &[u8],
    tail: &[u8],
    committed: Prefix,
) -> Result<(), WireError> {
    writer.bytes(common)?;
    writer.bytes(tail.get(..40).ok_or(WireError::Length)?)?;
    writer.prefix(committed)
}

// Outgoing actor-owned templates may precede any PEER session. Their metadata
// was encoded after journal validation. Wire decoding still requires a complete
// envelope, established binding, and canonical integrity validation.
fn publication_template<'a>(
    sender: NodeId,
    frames: &[&'a [u8]],
    limits: WireLimits,
) -> Result<(ozzy_proto::Packet<'a>, usize), WireError> {
    let packet = if let [header, metadata, payload] = frames
        && header.is_empty()
    {
        let envelope = ozzy_proto::Envelope {
            opcode: Opcode::PreparePub,
            response: false,
            request_id: None,
            sender,
            session: None,
        };
        envelope.validate_frames(metadata.len(), payload.len(), limits.envelope)?;
        ozzy_proto::Packet {
            envelope,
            metadata,
            payload,
        }
    } else {
        ozzy_proto::decode_packet(frames, limits.envelope)?
    };
    let skip = match packet.envelope.opcode {
        Opcode::Prepare => 0,
        Opcode::PreparePub if frames.first().is_some_and(|header| header.is_empty()) => 0,
        Opcode::PrepareFlow => 16,
        _ => return Err(WireError::Peer),
    };
    if packet.envelope.sender != sender
        || packet
            .metadata
            .get(skip..)
            .is_none_or(|body| body.len() < 164)
        || packet.metadata.get(32..48) != Some(sender.as_bytes())
    {
        return Err(WireError::Peer);
    }
    Ok((packet, skip))
}

/// Decode only from a separately bound publisher socket. Reject wrong leader,
/// scope or group before hashing any payload. PUB receipt is
/// neither admission nor a vote; ordinary receive and core checks still apply.
pub fn decode_publication<'a>(
    frames: &[&'a [u8]],
    binding: PeerBinding,
    local: NodeId,
    scope: Scope,
    limits: WireLimits,
) -> Result<PrepareBatch<'a>, WireError> {
    let [topic, frames @ ..] = frames else {
        return Err(WireError::Peer);
    };
    if frames.len() != 3
        || topic.len() != PUBLICATION_BYTES
        || binding.peer != binding.configuration.primary(scope.view)
        || binding.peer == local
        || *topic != scope.group_id.as_bytes()
    {
        return Err(WireError::Peer);
    }
    let packet = ozzy_proto::decode_packet(frames, limits.envelope)?;
    if packet.envelope.sender != binding.peer
        || packet.envelope.session.is_some()
        || packet.envelope.opcode != Opcode::PreparePub
        || packet.envelope.response
        || packet.envelope.request_id.is_some()
    {
        return Err(WireError::Peer);
    }
    let mut reader = Reader::new(packet.metadata);
    if reader.scope(binding)? != scope {
        return Err(WireError::Peer);
    }
    prepare::decode_prepare(scope, reader, packet.payload, limits)
}
