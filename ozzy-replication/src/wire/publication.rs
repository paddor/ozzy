//! One shared PREPARE on the group topic; receiver admission remains local.

use super::{PeerBinding, PrepareBatch, Reader, WireError, WireLimits, prepare};
use crate::{Prefix, Scope};
use ozzy_proto::{GroupId, NodeId, Opcode};

/// One already encoded, consecutive PREPARE retained by its actor. An empty
/// header denotes actor-owned unbound metadata, never an accepted wire packet.
#[derive(Debug, Clone, Copy)]
pub struct PublicationPart<'a> {
    pub frames: [&'a [u8]; 3],
    pub predecessor: Prefix,
    pub end: Prefix,
}

struct ValidatedPublicationPart<'a> {
    metadata: &'a [u8],
    skip: usize,
    payload: &'a [u8],
    count: usize,
    scope: &'a [u8],
}

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
    let length = packet.metadata.len() - skip;
    let header = ozzy_proto::Envelope {
        opcode: Opcode::PreparePub,
        response: false,
        request_id: None,
        sender,
        session: None,
    }
    .encode_header(length, packet.payload.len(), limits.envelope)?;
    if metadata.len() < length {
        return Err(WireError::Capacity);
    }
    metadata[..80].copy_from_slice(&packet.metadata[..80]);
    metadata[80..length].copy_from_slice(&packet.metadata[80 + skip..]);
    metadata[120..128].copy_from_slice(&committed.op.0.to_be_bytes());
    metadata[128..160].copy_from_slice(committed.digest.as_bytes());
    Ok(super::PrepareEncoding {
        header,
        metadata_bytes: length,
        payload_bytes: packet.payload.len(),
    })
}

/// Combine consecutive actor-owned PREPARE templates into one publication.
///
/// Bodies are copied once into `payload`; canonical bodies and digests are not
/// decoded, recompressed, or rehashed. Input templates were produced only after
/// journal validation. This function rechecks framing, bounds, and chain joins.
pub fn encode_publication_group(
    sender: NodeId,
    parts: &[PublicationPart<'_>],
    committed: Prefix,
    metadata: &mut [u8],
    payload: &mut Vec<u8>,
    limits: WireLimits,
) -> Result<super::PrepareEncoding, WireError> {
    if parts.is_empty() {
        return Err(WireError::Limit);
    }
    super::validate_prefix(committed)?;
    let mut operation_count = 0usize;
    let mut payload_bytes = 0usize;
    let mut normalized = Vec::with_capacity(parts.len());
    let mut previous = parts[0].predecessor;
    let mut scope = None;
    for part in parts {
        let validated = validate_publication_part(sender, part, previous, scope, limits)?;
        scope.get_or_insert(validated.scope);
        operation_count = operation_count
            .checked_add(validated.count)
            .ok_or(WireError::Limit)?;
        payload_bytes = payload_bytes
            .checked_add(validated.payload.len())
            .ok_or(WireError::Limit)?;
        if payload_bytes > limits.envelope.max_payload_bytes {
            return Err(WireError::Limit);
        }
        normalized.push((validated.metadata, validated.skip, validated.payload));
        previous = part.end;
    }
    let metadata_bytes = prepare::metadata_length(operation_count, 164, limits)?;
    if metadata.len() < metadata_bytes || u32::try_from(operation_count).is_err() {
        return Err(WireError::Capacity);
    }
    let (first, first_skip, _) = normalized[0];
    metadata[..80].copy_from_slice(&first[..80]);
    metadata[80..160].copy_from_slice(&first[80 + first_skip..160 + first_skip]);
    metadata[120..128].copy_from_slice(&committed.op.0.to_be_bytes());
    metadata[128..160].copy_from_slice(committed.digest.as_bytes());
    metadata[160..164].copy_from_slice(&(operation_count as u32).to_be_bytes());
    let mut cursor = 164;
    payload.clear();
    payload.reserve(payload_bytes);
    for (batch, skip, body) in normalized {
        let descriptors = &batch[164 + skip..];
        metadata[cursor..cursor + descriptors.len()].copy_from_slice(descriptors);
        cursor += descriptors.len();
        payload.extend_from_slice(body);
    }
    let header = ozzy_proto::Envelope {
        opcode: Opcode::PreparePub,
        response: false,
        request_id: None,
        sender,
        session: None,
    }
    .encode_header(metadata_bytes, payload_bytes, limits.envelope)?;
    Ok(super::PrepareEncoding {
        header,
        metadata_bytes,
        payload_bytes,
    })
}

fn validate_publication_part<'a>(
    sender: NodeId,
    part: &PublicationPart<'a>,
    previous: Prefix,
    scope: Option<&[u8]>,
    limits: WireLimits,
) -> Result<ValidatedPublicationPart<'a>, WireError> {
    if part.predecessor != previous {
        return Err(WireError::Chain);
    }
    let count = part
        .end
        .op
        .0
        .checked_sub(part.predecessor.op.0)
        .and_then(|count| usize::try_from(count).ok())
        .filter(|count| *count != 0)
        .ok_or(WireError::Chain)?;
    let (packet, skip) = publication_template(sender, &part.frames, limits)?;
    let common = &packet.metadata[..80];
    if scope.is_some_and(|expected| expected != common) {
        return Err(WireError::Scope);
    }
    let tail = &packet.metadata[80 + skip..];
    let first = u64::from_be_bytes(tail[..8].try_into().unwrap());
    let predecessor = Prefix {
        op: crate::OpNumber(first.checked_sub(1).ok_or(WireError::Chain)?),
        digest: crate::Digest::from_bytes(tail[8..40].try_into().unwrap()),
    };
    let encoded_count = u32::from_be_bytes(tail[80..84].try_into().unwrap()) as usize;
    let expected = encoded_count
        .checked_mul(86)
        .and_then(|bytes| bytes.checked_add(84))
        .ok_or(WireError::Limit)?;
    if predecessor != part.predecessor {
        return Err(WireError::Chain);
    }
    if encoded_count != count || tail.len() != expected {
        return Err(WireError::Length);
    }
    let descriptors = &tail[84..];
    let body_bytes = descriptors
        .as_chunks::<86>()
        .0
        .iter()
        .try_fold(0usize, |sum, descriptor| {
            sum.checked_add(u32::from_be_bytes(descriptor[18..22].try_into().unwrap()) as usize)
        })
        .ok_or(WireError::Limit)?;
    let last = descriptors
        .rchunks_exact(86)
        .next()
        .expect("nonempty batch");
    if body_bytes != packet.payload.len() || last[54..86] != *part.end.digest.as_bytes() {
        return Err(WireError::Chain);
    }
    Ok(ValidatedPublicationPart {
        metadata: packet.metadata,
        skip,
        payload: packet.payload,
        count,
        scope: common,
    })
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
        Opcode::PreparePub if frames[0].is_empty() => 0,
        Opcode::PrepareFlow => 16,
        _ => return Err(WireError::Peer),
    };
    if packet.envelope.sender != sender
        || packet.metadata.len() < 164 + skip
        || packet.metadata[32..48] != *sender.as_bytes()
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
    if frames.len() != 4
        || frames[0].len() != PUBLICATION_BYTES
        || binding.peer != binding.configuration.primary(scope.view)
        || binding.peer == local
        || frames[0][..16] != *scope.group_id.as_bytes()
    {
        return Err(WireError::Peer);
    }
    let packet = ozzy_proto::decode_packet(&frames[1..], limits.envelope)?;
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
