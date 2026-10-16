//! Native producer requests and receipts for the native protocol.
//!
//! Encoding uses caller-reserved frame buffers. Decoding borrows validated
//! descriptors and payloads, including empty multipart boundaries. These codecs
//! establish structure only: authenticated sessions, negotiated capabilities,
//! producer fences, authority, and commit evidence remain runtime obligations.

pub use super::data::{
    Authority, CodecError, DataLimits, DecodedRecord, Parts, Record, RecordIter, Records,
};
use super::data::{add, capacity, command, count, record_count};
use super::{ENVELOPE_BYTES, Envelope, Opcode, Packet};
use crate::layout::Reader;
use crate::{GroupId, MessageId, PartitionIncarnation, ProducerId};

mod outcome;
mod payload;
mod receipt;
pub use outcome::{AppendDisposition, AppendOutcome, OUTCOME_RUN_BYTES, OutcomeRun, OutcomeRuns};
pub use payload::{ADAPTIVE_LZ4_THRESHOLD, PayloadEncoding, lz4_block_is_valid};
pub(crate) use payload::{
    decode_owned_payload, decode_payload_encoding, encode_payload_encoding, read_payload_encoding,
    validate_payload_encoding,
};
pub mod stream;
pub use receipt::{
    Appended, DecodedAppended, MessageIds, OpPosition, decode_appended, encode_appended,
};

/// Bytes in the common owner-routed identity header, before policy and descriptors.
pub const IDENTITY_METADATA_BYTES: usize = 89;

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
    let (authority, partition, owner_epoch, key) =
        decode_common(&mut Reader::new(packet.metadata))?;
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

/// An already packed APPEND payload frame that the caller sends unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedPayload {
    /// Exact concatenation of the record parts, or one LZ4 block of it.
    pub encoding: PayloadEncoding,
    /// Byte length of the frame as sent.
    pub encoded_bytes: usize,
}

/// Source of an encoded APPEND's payload frame.
enum PayloadFrame<'a> {
    /// Copy the record parts into this caller-reserved buffer.
    Copied(&'a mut Vec<u8>),
    /// The caller sends the unchanged concatenation of the record parts.
    Parts,
    /// The caller sends this packed frame.
    Prepared(PreparedPayload),
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
        PayloadFrame::Copied(payload),
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
    encode_frames(envelope, append, metadata, PayloadFrame::Parts, limits)
}

/// Encode APPEND metadata for an already packed raw or compressed payload.
///
/// Record parts describe the original uncompressed bytes. The caller sends one
/// payload frame containing either their exact concatenation or one LZ4 block.
pub fn encode_prepared_append_metadata(
    envelope: Envelope,
    append: Append<'_>,
    metadata: &mut Vec<u8>,
    payload: PreparedPayload,
    limits: DataLimits,
) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
    encode_frames(
        envelope,
        append,
        metadata,
        PayloadFrame::Prepared(payload),
        limits,
    )
}

fn encode_frames(
    envelope: Envelope,
    append: Append<'_>,
    metadata: &mut Vec<u8>,
    payload: PayloadFrame<'_>,
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
    let (encoding, payload_bytes) = match payload {
        PayloadFrame::Prepared(prepared) => (prepared.encoding, prepared.encoded_bytes),
        PayloadFrame::Copied(_) | PayloadFrame::Parts => (PayloadEncoding::Raw, decoded_bytes),
    };
    if encoding == PayloadEncoding::Lz4 && payload_bytes >= decoded_bytes {
        return Err(CodecError::Length);
    }
    validate_payload_encoding(encoding, decoded_bytes, payload_bytes, limits)?;
    let metadata_bytes = add(IDENTITY_METADATA_BYTES + 6, descriptors)?;
    let header = envelope.encode_header(metadata_bytes, payload_bytes, limits.envelope)?;
    capacity(metadata, metadata_bytes)?;
    let mut payload = match payload {
        PayloadFrame::Copied(payload) => Some(payload),
        PayloadFrame::Parts | PayloadFrame::Prepared(_) => None,
    };
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
    let (header, mut cursor) = decode_header(packet, limits)?;
    let (payload_encoding, _) = read_payload_encoding(&mut cursor, packet.payload.len(), limits)?;
    if payload_encoding != PayloadEncoding::Raw {
        return Err(CodecError::Profile);
    }
    let records = super::data::decode_records(
        cursor.rest(),
        packet.payload,
        header.key.first_sequence,
        limits,
    )?;
    Ok(DecodedAppend {
        authority: header.authority,
        partition: header.partition,
        owner_epoch: header.owner_epoch,
        key: header.key,
        policy: header.policy,
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
    let (header, mut cursor) = decode_header(packet, limits)?;
    let (payload_encoding, payload) =
        decode_payload_encoding(&mut cursor, packet.payload, scratch, limits)?;
    let records =
        super::data::decode_records(cursor.rest(), payload, header.key.first_sequence, limits)?;
    Ok(DecodedAppend {
        authority: header.authority,
        partition: header.partition,
        owner_epoch: header.owner_epoch,
        key: header.key,
        policy: header.policy,
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
    let (header, mut cursor) = decode_header(packet, limits)?;
    let (payload_encoding, decoded_bytes) =
        read_payload_encoding(&mut cursor, packet.payload.len(), limits)?;
    let records = super::data::decode_record_descriptors(
        cursor.rest(),
        decoded_bytes,
        header.key.first_sequence,
        limits,
    )?;
    if payload_encoding == PayloadEncoding::Lz4
        && !lz4_block_is_valid(packet.payload, decoded_bytes)
    {
        return Err(CodecError::Length);
    }
    Ok(ValidatedAppend {
        authority: header.authority,
        partition: header.partition,
        owner_epoch: header.owner_epoch,
        key: header.key,
        policy: header.policy,
        payload_encoding,
        encoded_payload: packet.payload,
        records,
    })
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

/// APPEND fields before the payload encoding.
struct Header {
    authority: Authority,
    partition: PartitionIncarnation,
    owner_epoch: u64,
    key: AppendKey,
    policy: Policy,
}

/// Check the APPEND command and frame bounds, then read its header fields.
fn decode_header(
    packet: Packet<'_>,
    limits: DataLimits,
) -> Result<(Header, Reader<'_>), CodecError> {
    command(packet.envelope, Opcode::Append, false)?;
    packet.envelope.validate_frames(
        packet.metadata.len(),
        packet.payload.len(),
        limits.envelope,
    )?;
    let mut cursor = Reader::new(packet.metadata);
    let (authority, partition, owner_epoch, key) = decode_common(&mut cursor)?;
    let policy = Policy::try_from(cursor.u8()?)?;
    let header = Header {
        authority,
        partition,
        owner_epoch,
        key,
        policy,
    };
    Ok((header, cursor))
}

fn decode_common(
    cursor: &mut Reader<'_>,
) -> Result<(Authority, PartitionIncarnation, u64, AppendKey), CodecError> {
    if cursor.u8()? != 1 {
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
