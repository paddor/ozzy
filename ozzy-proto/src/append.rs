//! Native producer requests and receipts for the native protocol.
//!
//! Encoding uses caller-reserved frame buffers. Decoding borrows validated
//! descriptors and payloads, including empty multipart boundaries. These codecs
//! establish structure only: authenticated sessions, negotiated capabilities,
//! producer fences, authority, and commit evidence remain runtime obligations.

pub use super::data::{
    Authority, CodecError, DataLimits, DecodedRecord, Parts, Record, RecordIter, Records,
};
use super::data::{Cursor, add, capacity, command, count, record_count};
use super::{ENVELOPE_BYTES, Envelope, Opcode, Packet};
use crate::{GroupId, MessageId, PartitionIncarnation, ProducerId};

mod receipt;
pub mod stream;
pub use receipt::{
    Appended, DecodedAppended, MessageIds, OpPosition, decode_appended, encode_appended,
};

/// Bytes in the common owner-routed identity header, before policy and descriptors.
pub const IDENTITY_METADATA_BYTES: usize = 89;

/// Encoding of one complete APPEND payload frame.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadEncoding {
    /// Concatenated original record parts.
    #[default]
    Raw = 0,
    /// One raw LZ4 block over all concatenated record parts.
    Lz4 = 1,
}

/// Raw APPEND payload size where SDKs and brokers begin adaptive LZ4 packing.
/// Sparse writers are never delayed to reach this threshold.
pub const ADAPTIVE_LZ4_THRESHOLD: usize = 2 * 1024;

impl TryFrom<u8> for PayloadEncoding {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Lz4),
            _ => Err(CodecError::Profile),
        }
    }
}

/// Stable append identity, independent of link correlation and current primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendKey {
    /// Producer identity retained across retries.
    pub producer_id: ProducerId,
    /// Nonzero producer-session fencing epoch.
    pub producer_epoch: u64,
    /// Sequence of the first record in this request, independent of retry grouping.
    pub first_sequence: u64,
}

/// Claimed APPEND destination and writer identity for bounded metadata routing.
/// These fields establish no writer authority, payload validity, or acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route {
    /// Claimed group configuration and view.
    pub authority: Authority,
    /// Immutable destination partition incarnation.
    pub partition: PartitionIncarnation,
    /// Claimed partition ownership fence.
    pub owner_epoch: u64,
    /// Writer identity and retry position; sequence is not assigned by dispatch.
    pub key: AppendKey,
}

/// Read the fixed routing prefix without scanning record descriptors, hashing,
/// decompressing, or retaining payload bytes. The destination must still call
/// `validate_append` and enforce its session, writer, policy, and authority rules.
pub fn route(packet: Packet<'_>, limits: crate::EnvelopeLimits) -> Result<Route, CodecError> {
    command(packet.envelope, Opcode::Append, false)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    let (authority, partition, owner_epoch, key) = decode_common(&mut Cursor(packet.metadata))?;
    Ok(Route {
        authority,
        partition,
        owner_epoch,
        key,
    })
}

/// Explicit owner-completion policy. Recognition does not advertise support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Policy {
    /// Synchronized local journal.
    LocalDurable = 3,
    /// Matching synchronized prefix on the configured disk quorum.
    QuorumDurable = 5,
    /// Retained RAM copies on the group; bounded disk persistence runs independently.
    QuorumReplicatedPersisting = 6,
}

impl TryFrom<u8> for Policy {
    type Error = CodecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            3 => Ok(Self::LocalDurable),
            5 => Ok(Self::QuorumDurable),
            6 => Ok(Self::QuorumReplicatedPersisting),
            _ => Err(CodecError::Policy),
        }
    }
}

/// One producer request. The owner assigns offsets and timestamps. Streaming
/// requests may receive partial confirmations; they are not application transactions.
#[derive(Debug, Clone, Copy)]
pub struct Append<'a> {
    /// Expected group and primary generation.
    pub authority: Authority,
    /// Immutable partition incarnation.
    pub partition: PartitionIncarnation,
    /// Expected nonzero partition ownership fence.
    pub owner_epoch: u64,
    /// Stable session/sequence identity.
    pub key: AppendKey,
    /// Must match the configured group policy; no downgrade is permitted.
    pub policy: Policy,
    /// Nonempty, ordered records.
    pub records: &'a [Record<'a>],
}

/// Structurally valid append borrowing the received frame storage.
#[derive(Debug, Clone, Copy)]
pub struct DecodedAppend<'a> {
    /// Claimed authority, still requiring runtime validation.
    pub authority: Authority,
    /// Claimed partition incarnation.
    pub partition: PartitionIncarnation,
    /// Expected partition ownership fence.
    pub owner_epoch: u64,
    /// Stable retry identity, not a link request ID.
    pub key: AppendKey,
    /// Requested completion boundary.
    pub policy: Policy,
    /// Encoding used by the received group before bounded validation.
    pub payload_encoding: PayloadEncoding,
    /// Exact payload frame received from the producer.
    ///
    /// For LZ4 this remains the compressed block while `records` borrows the
    /// validated materialization in caller-owned scratch.
    pub encoded_payload: &'a [u8],
    /// Fully length-checked descriptors and opaque payload.
    pub records: Records<'a>,
}

/// Structurally validated APPEND retaining its exact payload representation.
/// Compressed payloads are syntax-checked without allocating decoded storage.
#[derive(Debug, Clone, Copy)]
pub struct ValidatedAppend<'a> {
    /// Claimed authority, still requiring runtime validation.
    pub authority: Authority,
    /// Claimed partition incarnation.
    pub partition: PartitionIncarnation,
    /// Expected partition ownership fence.
    pub owner_epoch: u64,
    /// Stable retry identity.
    pub key: AppendKey,
    /// Requested confirmation boundary.
    pub policy: Policy,
    /// Encoding of the complete payload frame.
    pub payload_encoding: PayloadEncoding,
    /// Exact payload frame received from the writer.
    pub encoded_payload: &'a [u8],
    /// Validated identities and part lengths for the decoded records.
    pub records: super::data::RecordDescriptors<'a>,
}

/// Encode APPEND into caller-reserved buffers, replacing their previous contents.
///
/// Rejection leaves both buffers unchanged. Insufficient capacity is an explicit
/// error; this function never grows buffers or assigns offsets/timestamps.
pub fn encode_append(
    envelope: Envelope,
    append: Append<'_>,
    metadata: &mut Vec<u8>,
    payload: &mut Vec<u8>,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_frames(
        envelope,
        append,
        metadata,
        Some(payload),
        PayloadEncoding::Raw,
        None,
        limits,
    )
}

/// Encode APPEND metadata without copying already packed payload bytes.
///
/// The caller sends the unchanged concatenation of the supplied record parts as
/// the payload frame. All normal identity, length, and receive bounds apply.
/// Rejection leaves metadata unchanged; encoding never grows its buffer.
pub fn encode_append_metadata(
    envelope: Envelope,
    append: Append<'_>,
    metadata: &mut Vec<u8>,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_frames(
        envelope,
        append,
        metadata,
        None,
        PayloadEncoding::Raw,
        None,
        limits,
    )
}

/// Encode APPEND metadata for an already packed raw or compressed payload.
///
/// Record parts describe the original uncompressed bytes. The caller sends one
/// payload frame containing either their exact concatenation or one LZ4 block.
pub fn encode_prepared_append_metadata(
    envelope: Envelope,
    append: Append<'_>,
    metadata: &mut Vec<u8>,
    encoding: PayloadEncoding,
    encoded_payload_bytes: usize,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_frames(
        envelope,
        append,
        metadata,
        None,
        encoding,
        Some(encoded_payload_bytes),
        limits,
    )
}

fn encode_frames(
    envelope: Envelope,
    append: Append<'_>,
    metadata: &mut Vec<u8>,
    mut payload: Option<&mut Vec<u8>>,
    encoding: PayloadEncoding,
    encoded_payload_bytes: Option<usize>,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    command(envelope, Opcode::Append, false)?;
    identities(
        append.authority,
        append.partition,
        append.owner_epoch,
        append.key,
    )?;
    if append
        .records
        .iter()
        .any(|record| record.encoding != crate::data::Encoding::Raw)
    {
        return Err(CodecError::Profile);
    }
    let (descriptors, decoded_bytes) =
        super::data::records_size(append.records, append.key.first_sequence, limits)?;
    let payload_bytes = encoded_payload_bytes.unwrap_or(decoded_bytes);
    if encoding == PayloadEncoding::Lz4 && payload_bytes >= decoded_bytes {
        return Err(CodecError::Length);
    }
    validate_payload_encoding(encoding, decoded_bytes, payload_bytes, limits)?;
    let metadata_bytes = add(IDENTITY_METADATA_BYTES + 6, descriptors)?;
    let header = envelope.encode_header(metadata_bytes, payload_bytes, limits.envelope)?;
    capacity(metadata, metadata_bytes)?;
    if let Some(payload) = payload.as_mut() {
        capacity(payload, payload_bytes)?;
        payload.clear();
    }
    metadata.clear();
    encode_common(
        metadata,
        append.authority,
        append.partition,
        append.owner_epoch,
        append.key,
    );
    metadata.push(append.policy as u8);
    encode_payload_encoding(metadata, encoding, decoded_bytes)?;
    super::data::encode_records(append.records, metadata, payload);
    Ok(header)
}

/// Decode a raw APPEND without allocation.
pub fn decode_append(
    packet: Packet<'_>,
    limits: DataLimits,
) -> Result<DecodedAppend<'_>, CodecError> {
    command(packet.envelope, Opcode::Append, false)?;
    packet.envelope.validate_frames(
        packet.metadata.len(),
        packet.payload.len(),
        limits.envelope,
    )?;
    let mut cursor = Cursor(packet.metadata);
    let (authority, partition, owner_epoch, key) = decode_common(&mut cursor)?;
    let policy = Policy::try_from(cursor.byte()?)?;
    let payload_encoding = PayloadEncoding::try_from(cursor.byte()?)?;
    let decoded_bytes = cursor.u32()? as usize;
    validate_payload_encoding(
        payload_encoding,
        decoded_bytes,
        packet.payload.len(),
        limits,
    )?;
    if payload_encoding != PayloadEncoding::Raw {
        return Err(CodecError::Profile);
    }
    let records =
        super::data::decode_records(cursor.0, packet.payload, key.first_sequence, limits)?;
    Ok(DecodedAppend {
        authority,
        partition,
        owner_epoch,
        key,
        policy,
        payload_encoding,
        encoded_payload: packet.payload,
        records,
    })
}

/// Decode APPEND, materializing compressed bytes into caller-owned scratch.
pub fn decode_append_with_scratch<'a>(
    packet: Packet<'a>,
    limits: DataLimits,
    scratch: &'a mut Vec<u8>,
) -> Result<DecodedAppend<'a>, CodecError> {
    command(packet.envelope, Opcode::Append, false)?;
    packet.envelope.validate_frames(
        packet.metadata.len(),
        packet.payload.len(),
        limits.envelope,
    )?;
    let mut cursor = Cursor(packet.metadata);
    let (authority, partition, owner_epoch, key) = decode_common(&mut cursor)?;
    let policy = Policy::try_from(cursor.byte()?)?;
    let (payload_encoding, payload) =
        decode_payload_encoding(&mut cursor, packet.payload, scratch, limits)?;
    let records = super::data::decode_records(cursor.0, payload, key.first_sequence, limits)?;
    Ok(DecodedAppend {
        authority,
        partition,
        owner_epoch,
        key,
        policy,
        payload_encoding,
        encoded_payload: packet.payload,
        records,
    })
}

/// Decode and validate APPEND without materializing whole-payload compression.
pub fn validate_append(
    packet: Packet<'_>,
    limits: DataLimits,
) -> Result<ValidatedAppend<'_>, CodecError> {
    command(packet.envelope, Opcode::Append, false)?;
    packet.envelope.validate_frames(
        packet.metadata.len(),
        packet.payload.len(),
        limits.envelope,
    )?;
    let mut cursor = Cursor(packet.metadata);
    let (authority, partition, owner_epoch, key) = decode_common(&mut cursor)?;
    let policy = Policy::try_from(cursor.byte()?)?;
    let payload_encoding = PayloadEncoding::try_from(cursor.byte()?)?;
    let decoded_bytes = cursor.u32()? as usize;
    validate_payload_encoding(
        payload_encoding,
        decoded_bytes,
        packet.payload.len(),
        limits,
    )?;
    let records = super::data::decode_record_descriptors(
        cursor.0,
        decoded_bytes,
        key.first_sequence,
        limits,
    )?;
    if payload_encoding == PayloadEncoding::Lz4
        && !lz4_block_is_valid(packet.payload, decoded_bytes)
    {
        return Err(CodecError::Length);
    }
    Ok(ValidatedAppend {
        authority,
        partition,
        owner_epoch,
        key,
        policy,
        payload_encoding,
        encoded_payload: packet.payload,
        records,
    })
}

/// Check LZ4 block syntax and exact decoded length without materializing bytes.
/// The fast path validates the common block layout. The reference validator
/// handles any valid form the fast path cannot recognize.
pub fn lz4_block_is_valid(input: &[u8], expected: usize) -> bool {
    fast_lz4_block_is_valid(input, expected)
        || lz4rip::block::validate_block(input, expected).is_ok()
}

fn fast_lz4_block_is_valid(input: &[u8], expected: usize) -> bool {
    let mut at = 0;
    let mut produced = 0;
    loop {
        let Some(&token) = input.get(at) else {
            return false;
        };
        at += 1;
        let mut literals = (token >> 4) as usize;
        if literals == 15 {
            loop {
                let Some(&extra) = input.get(at) else {
                    return false;
                };
                at += 1;
                let Some(sum) = literals.checked_add(extra as usize) else {
                    return false;
                };
                literals = sum;
                if extra != 255 {
                    break;
                }
            }
        }
        if literals > input.len() - at || literals > expected - produced {
            return false;
        }
        at += literals;
        produced += literals;
        if at == input.len() {
            return produced == expected;
        }
        if input.len() - at < 2 {
            return false;
        }
        let offset = u16::from_le_bytes([input[at], input[at + 1]]) as usize;
        at += 2;
        if offset == 0 || offset > produced {
            return false;
        }
        let mut matched = 4 + (token & 15) as usize;
        if matched == 19 {
            loop {
                let Some(&extra) = input.get(at) else {
                    return false;
                };
                at += 1;
                let Some(sum) = matched.checked_add(extra as usize) else {
                    return false;
                };
                matched = sum;
                if extra != 255 {
                    break;
                }
            }
        }
        if matched > expected - produced {
            return false;
        }
        produced += matched;
    }
}

pub(crate) fn encode_payload_encoding(
    output: &mut Vec<u8>,
    encoding: PayloadEncoding,
    decoded_bytes: usize,
) -> Result<(), CodecError> {
    output.push(encoding as u8);
    output.extend_from_slice(
        &u32::try_from(decoded_bytes)
            .map_err(|_| CodecError::Limit)?
            .to_be_bytes(),
    );
    Ok(())
}

pub(crate) fn validate_payload_encoding(
    encoding: PayloadEncoding,
    decoded_bytes: usize,
    encoded_bytes: usize,
    limits: DataLimits,
) -> Result<(), CodecError> {
    if decoded_bytes > limits.envelope.max_payload_bytes {
        return Err(CodecError::Limit);
    }
    match encoding {
        PayloadEncoding::Raw if encoded_bytes == decoded_bytes => Ok(()),
        PayloadEncoding::Lz4 if encoded_bytes != 0 => Ok(()),
        _ => Err(CodecError::Length),
    }
}

pub(crate) fn decode_payload_encoding<'a>(
    cursor: &mut Cursor<'a>,
    encoded: &'a [u8],
    scratch: &'a mut Vec<u8>,
    limits: DataLimits,
) -> Result<(PayloadEncoding, &'a [u8]), CodecError> {
    let encoding = PayloadEncoding::try_from(cursor.byte()?)?;
    let decoded_bytes = cursor.u32()? as usize;
    validate_payload_encoding(encoding, decoded_bytes, encoded.len(), limits)?;
    if encoding == PayloadEncoding::Raw {
        return Ok((encoding, encoded));
    }
    scratch.clear();
    scratch
        .try_reserve(decoded_bytes)
        .map_err(|_| CodecError::Limit)?;
    scratch.resize(decoded_bytes, 0);
    let actual =
        lz4rip::block::decompress_into(encoded, scratch).map_err(|_| CodecError::Length)?;
    if actual != decoded_bytes {
        return Err(CodecError::Length);
    }
    Ok((encoding, scratch))
}

/// Owned payload of a raw or whole-group LZ4 frame: the received `frame`, or
/// its decompressed bytes split off `buffer`. Once every earlier split-off
/// payload is released, `buffer` reuses its allocation.
pub(crate) fn decode_owned_payload(
    cursor: &mut Cursor<'_>,
    frame: &bytes::Bytes,
    buffer: &mut bytes::BytesMut,
    limits: DataLimits,
) -> Result<(PayloadEncoding, bytes::Bytes), CodecError> {
    let encoding = PayloadEncoding::try_from(cursor.byte()?)?;
    let decoded_bytes = cursor.u32()? as usize;
    validate_payload_encoding(encoding, decoded_bytes, frame.len(), limits)?;
    if encoding == PayloadEncoding::Raw {
        return Ok((encoding, frame.clone()));
    }
    buffer.clear();
    buffer.resize(decoded_bytes, 0);
    let actual = lz4rip::block::decompress_into(frame, buffer).map_err(|_| CodecError::Length)?;
    if actual != decoded_bytes {
        return Err(CodecError::Length);
    }
    Ok((encoding, buffer.split().freeze()))
}

fn identities(
    authority: Authority,
    partition: PartitionIncarnation,
    owner_epoch: u64,
    key: AppendKey,
) -> Result<(), CodecError> {
    if authority.group_id.as_bytes() == &[0; 16]
        || authority.config_epoch == 0
        || partition.as_bytes() == &[0; 16]
        || owner_epoch == 0
        || key.producer_id.as_bytes() == &[0; 16]
        || key.producer_epoch == 0
    {
        return Err(CodecError::Identity);
    }
    Ok(())
}

fn encode_common(
    output: &mut Vec<u8>,
    authority: Authority,
    partition: PartitionIncarnation,
    owner_epoch: u64,
    key: AppendKey,
) {
    output.push(1);
    output.extend_from_slice(authority.group_id.as_bytes());
    output.extend_from_slice(&authority.config_epoch.to_be_bytes());
    output.extend_from_slice(&authority.view.to_be_bytes());
    output.extend_from_slice(partition.as_bytes());
    output.extend_from_slice(&owner_epoch.to_be_bytes());
    output.extend_from_slice(key.producer_id.as_bytes());
    output.extend_from_slice(&key.producer_epoch.to_be_bytes());
    output.extend_from_slice(&key.first_sequence.to_be_bytes());
}

fn decode_common(
    cursor: &mut Cursor<'_>,
) -> Result<(Authority, PartitionIncarnation, u64, AppendKey), CodecError> {
    if cursor.byte()? != 1 {
        return Err(CodecError::Profile);
    }
    let authority = Authority {
        group_id: GroupId::from_bytes(cursor.array()?),
        config_epoch: cursor.u64()?,
        view: cursor.u64()?,
    };
    let partition = PartitionIncarnation::from_bytes(cursor.array()?);
    let owner_epoch = cursor.u64()?;
    let key = AppendKey {
        producer_id: ProducerId::from_bytes(cursor.array()?),
        producer_epoch: cursor.u64()?,
        first_sequence: cursor.u64()?,
    };
    identities(authority, partition, owner_epoch, key)?;
    Ok((authority, partition, owner_epoch, key))
}

#[cfg(test)]
mod owned_payload_tests {
    use super::*;

    fn decode(
        body: &[u8],
        buffer: &mut bytes::BytesMut,
    ) -> Result<(PayloadEncoding, bytes::Bytes), CodecError> {
        let mut header = Vec::new();
        encode_payload_encoding(&mut header, PayloadEncoding::Lz4, body.len())?;
        let frame = bytes::Bytes::from(lz4rip::block::compress(body));
        decode_owned_payload(&mut Cursor(&header), &frame, buffer, DataLimits::default())
    }

    #[test]
    fn released_payload_reuses_buffer_and_retained_payload_stays_intact() {
        let mut buffer = bytes::BytesMut::new();
        let (encoding, first) = decode(&[1; 4096], &mut buffer).unwrap();
        assert_eq!(encoding, PayloadEncoding::Lz4);
        let address = first.as_ptr();
        drop(first);
        let (_, second) = decode(&[2; 4096], &mut buffer).unwrap();
        assert_eq!(
            second.as_ptr(),
            address,
            "released allocation was not reused"
        );
        let (_, third) = decode(&[3; 4096], &mut buffer).unwrap();
        assert_ne!(third.as_ptr(), second.as_ptr());
        assert!(second.iter().all(|&byte| byte == 2));
        assert!(third.iter().all(|&byte| byte == 3));
        let mut header = Vec::new();
        encode_payload_encoding(&mut header, PayloadEncoding::Lz4, 4097).unwrap();
        let frame = bytes::Bytes::from(lz4rip::block::compress(&[4; 4096]));
        assert!(
            decode_owned_payload(
                &mut Cursor(&header),
                &frame,
                &mut buffer,
                DataLimits::default()
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod lz4_validation_tests {
    use super::*;

    fn equivalent(input: &[u8], decoded_bytes: usize) {
        let reference = lz4rip::block::validate_block(input, decoded_bytes).is_ok();
        assert_eq!(fast_lz4_block_is_valid(input, decoded_bytes), reference);
        assert_eq!(lz4_block_is_valid(input, decoded_bytes), reference);
    }

    #[test]
    fn fast_validator_agrees_on_blocks_and_mutations() {
        let mut state = 0x85a3_08d3_2f6e_5c41_u64;
        for decoded_bytes in [0, 1, 15, 16, 256, 4096, 851_968] {
            let mut raw = Vec::with_capacity(decoded_bytes);
            for _ in 0..decoded_bytes {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                raw.push(b'a' + (state % 26) as u8);
            }
            let block = lz4rip::block::compress(&raw);
            equivalent(&block, decoded_bytes);
            equivalent(&block, decoded_bytes.saturating_add(1));
            if decoded_bytes > 0 {
                equivalent(&block, decoded_bytes - 1);
            }
            for _ in 0..100 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let mut changed = block.clone();
                let index = state as usize % changed.len();
                changed[index] ^= ((state >> 32) as u8) | 1;
                equivalent(&changed, decoded_bytes);
                equivalent(&changed, decoded_bytes.saturating_add(1));
            }
        }
    }

    #[test]
    fn fast_validator_agrees_on_arbitrary_input() {
        let mut state = 0x636e_38a2_9d71_4b52_u64;
        let mut bytes = [0_u8; 96];
        for _ in 0..10_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let length = state as usize % (bytes.len() + 1);
            for byte in &mut bytes[..length] {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            equivalent(&bytes[..length], (state >> 32) as usize % 256);
        }
        equivalent(&[0x10, b'a', 1, 0, 0], usize::MAX);
    }

    #[test]
    fn fast_validator_agrees_on_short_blocks_and_long_matches() {
        for first in 0..=u8::MAX {
            for second in 0..=u8::MAX {
                let input = [first, second];
                for expected in [0, 1, 4, 19, 20, 255, 256, usize::MAX] {
                    equivalent(&input, expected);
                }
            }
        }
        let block = lz4rip::block::compress(&vec![b'x'; 851_968]);
        equivalent(&block, 851_968);
        for cut in (0..block.len()).step_by(17) {
            equivalent(&block[..cut], 851_968);
        }

        let mut literals = vec![0xf0, 255, 255, 255, 244];
        literals.extend_from_slice(&[b'x'; 1024]);
        equivalent(&literals, 1024);
        equivalent(&literals[..literals.len() - 1], 1024);
        equivalent(&literals, 1025);
    }
}
