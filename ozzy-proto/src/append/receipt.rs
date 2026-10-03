use super::{
    AppendKey, Authority, CodecError, Cursor, DataLimits, ENVELOPE_BYTES, Envelope, MessageId,
    Opcode, Packet, PartitionIncarnation, Policy, add, capacity, command, count, decode_common,
    encode_common, identities, record_count,
};

/// Exact non-genesis group operation and logical digest named by an append receipt.
/// Bytes alone are not evidence of commit or of the responder's authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpPosition {
    /// Group-wide operation number, distinct from partition offsets.
    pub op: u64,
    /// Canonical logical operation digest, independent of journal compression.
    pub digest: [u8; 32],
}

/// APPENDED response issued only after the configured owner-completion boundary.
#[derive(Debug, Clone, Copy)]
pub struct Appended<'a> {
    /// Authority serving this response, possibly newer than the original attempt.
    pub authority: Authority,
    /// Immutable partition incarnation.
    pub partition: PartitionIncarnation,
    /// Partition ownership fence associated with the append.
    pub owner_epoch: u64,
    /// Stable request identity, unchanged by retry routing.
    pub key: AppendKey,
    /// Assigned first partition offset; count is the message-ID list length.
    pub first_offset: u64,
    /// Exact achieved policy. Clients must compare it with their requested policy.
    pub policy: Policy,
    /// Exact operation covering the final record, including coalesced requests.
    pub position: OpPosition,
    /// Ordered IDs of every record; the nonempty list determines both wire counts.
    pub message_ids: &'a [MessageId],
}

/// Structurally valid response, still requiring session and request validation.
#[derive(Debug, Clone)]
pub struct DecodedAppended<'a> {
    /// Responder's claimed group and primary view.
    pub authority: Authority,
    /// Immutable partition incarnation.
    pub partition: PartitionIncarnation,
    /// Claimed partition ownership fence.
    pub owner_epoch: u64,
    /// Stable retry identity, distinct from envelope correlation.
    pub key: AppendKey,
    /// First assigned partition offset.
    pub first_offset: u64,
    /// Claimed completion boundary, not established by decoding.
    pub policy: Policy,
    /// Claimed operation covering this request's final record.
    pub position: OpPosition,
    /// Ordered message IDs; iterator length is the validated record count.
    pub message_ids: MessageIds<'a>,
}

/// Iterator over an already bounded, fixed-width message-ID list.
#[derive(Debug, Clone)]
pub struct MessageIds<'a>(std::slice::Iter<'a, [u8; 16]>);

impl Iterator for MessageIds<'_> {
    type Item = MessageId;

    fn next(&mut self) -> Option<Self::Item> {
        Some(MessageId::from_bytes(*self.0.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for MessageIds<'_> {}
impl std::iter::FusedIterator for MessageIds<'_> {}

/// Encode a receipt into reserved metadata storage. Payload frame must be empty.
/// Rejection leaves the buffer unchanged; successful encoding never grows it.
pub fn encode_appended(
    envelope: Envelope,
    receipt: Appended<'_>,
    metadata: &mut Vec<u8>,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::Appended, true)?;
    identities(
        receipt.authority,
        receipt.partition,
        receipt.owner_epoch,
        receipt.key,
    )?;
    record_count(
        receipt.message_ids.len(),
        receipt.key.first_sequence,
        limits,
    )?;
    record_count(receipt.message_ids.len(), receipt.first_offset, limits)?;
    position(receipt.position)?;
    let size = add(
        146,
        receipt
            .message_ids
            .len()
            .checked_mul(16)
            .ok_or(CodecError::Length)?,
    )?;
    let header = envelope.encode_header(size, 0, limits.envelope)?;
    capacity(metadata, size)?;
    metadata.clear();
    encode_common(
        metadata,
        receipt.authority,
        receipt.partition,
        receipt.owner_epoch,
        receipt.key,
    );
    metadata.extend_from_slice(&receipt.first_offset.to_be_bytes());
    let records = count(receipt.message_ids.len())?.to_be_bytes();
    metadata.extend_from_slice(&records);
    metadata.push(receipt.policy as u8);
    metadata.extend_from_slice(&receipt.position.op.to_be_bytes());
    metadata.extend_from_slice(&receipt.position.digest);
    metadata.extend_from_slice(&records);
    for id in receipt.message_ids {
        metadata.extend_from_slice(id.as_bytes());
    }
    Ok(header)
}

/// Decode one bounded receipt. This does not prove its policy, digest, or authority.
pub fn decode_appended(
    packet: Packet<'_>,
    limits: DataLimits,
) -> Result<DecodedAppended<'_>, CodecError> {
    command(packet.envelope, Opcode::Appended, true)?;
    packet.envelope.validate_frames(
        packet.metadata.len(),
        packet.payload.len(),
        limits.envelope,
    )?;
    if !packet.payload.is_empty() {
        return Err(CodecError::Length);
    }
    let mut cursor = Cursor(packet.metadata);
    let (authority, partition, owner_epoch, key) = decode_common(&mut cursor)?;
    let first_offset = cursor.u64()?;
    let records = cursor.u32()? as usize;
    record_count(records, key.first_sequence, limits)?;
    record_count(records, first_offset, limits)?;
    let policy = Policy::try_from(cursor.byte()?)?;
    let through = OpPosition {
        op: cursor.u64()?,
        digest: cursor.array()?,
    };
    position(through)?;
    if cursor.u32()? as usize != records {
        return Err(CodecError::Length);
    }
    let ids = cursor.take(records.checked_mul(16).ok_or(CodecError::Length)?)?;
    if !cursor.0.is_empty() {
        return Err(CodecError::Length);
    }
    Ok(DecodedAppended {
        authority,
        partition,
        owner_epoch,
        key,
        first_offset,
        policy,
        position: through,
        message_ids: MessageIds(ids.as_chunks::<16>().0.iter()),
    })
}

fn position(position: OpPosition) -> Result<(), CodecError> {
    if position.op == 0 || position.op == u64::MAX || position.digest == [0; 32] {
        return Err(CodecError::Position);
    }
    Ok(())
}
