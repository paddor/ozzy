//! Shared bounded data-command fields and multipart records.

use super::{Envelope, EnvelopeError, EnvelopeLimits, Opcode};
use crate::{
    GroupId, MessageId,
    layout::{LayoutError, Reader},
};

mod batch;
mod descriptors;
mod encoding;
mod records;
pub use encoding::Encoding;
mod buffer;
pub use batch::{BatchIter, BatchRecord, PayloadRef, RecordBatch, TinyRecords};
mod indexed;
mod owned;
pub use buffer::RecordBuffer;
pub(super) use descriptors::validate_raw_record_entries;
pub use descriptors::{
    RecordDescriptor, RecordDescriptorIter, RecordDescriptors, decode_record_descriptors,
};
pub use indexed::{EncodedParts, IndexedRecords};
pub use owned::{OwnedRecord, OwnedRecordIter, OwnedRecords};
pub(super) use records::validate_record_entries;
pub use records::{DecodedRecord, Parts, RecordIter, Records, decode_records};

/// Group membership and primary generation, not proof of serving authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Authority {
    /// Immutable group identity.
    pub group_id: GroupId,
    /// Nonzero membership generation.
    pub config_epoch: u64,
    /// Primary view; zero is the initial view.
    pub view: u64,
}

/// Hard directional bounds, enforced before iterating or copying record bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataLimits {
    /// Maximum complete metadata and payload frame sizes.
    pub envelope: EnvelopeLimits,
    /// Maximum records in one data packet.
    pub max_records: usize,
    /// Maximum total payload parts, including empty parts.
    pub max_parts: usize,
    /// Maximum payload bytes in one record, summed across all parts.
    pub max_record_bytes: usize,
}

impl DataLimits {
    /// Select the smaller bound in each field for a directional data transfer.
    #[must_use]
    pub fn intersection(self, other: Self) -> Self {
        Self {
            envelope: EnvelopeLimits {
                max_metadata_bytes: self
                    .envelope
                    .max_metadata_bytes
                    .min(other.envelope.max_metadata_bytes),
                max_payload_bytes: self
                    .envelope
                    .max_payload_bytes
                    .min(other.envelope.max_payload_bytes),
            },
            max_records: self.max_records.min(other.max_records),
            max_parts: self.max_parts.min(other.max_parts),
            max_record_bytes: self.max_record_bytes.min(other.max_record_bytes),
        }
    }
}

#[cfg(test)]
mod record_limit_tests {
    use super::*;

    #[test]
    fn record_ceiling_is_independent_of_packet_capacity() {
        let limits = DataLimits {
            envelope: EnvelopeLimits {
                max_payload_bytes: 4 * 1024 * 1024,
                ..EnvelopeLimits::default()
            },
            ..DataLimits::default()
        };
        let payload = vec![7; 600 * 1024];
        let parts = [payload.as_slice()];
        let records = [Record {
            encoding: crate::data::Encoding::Raw,
            message_id: MessageId::new(),
            parts: &parts,
        }; 2];
        assert!(records_size(&records, 0, limits).is_ok());
        let large_parts = [payload.as_slice(), payload.as_slice()];
        let large = [Record {
            encoding: crate::data::Encoding::Raw,
            message_id: MessageId::new(),
            parts: &large_parts,
        }];
        assert!(matches!(
            records_size(&large, 0, limits),
            Err(CodecError::Limit)
        ));
        let mut metadata = Vec::new();
        let mut wire_payload = Vec::new();
        encode_records(&large, &mut metadata, Some(&mut wire_payload));
        assert!(matches!(
            decode_records(&metadata, &wire_payload, 0, limits),
            Err(CodecError::Limit)
        ));
    }
}

impl Default for DataLimits {
    fn default() -> Self {
        Self {
            envelope: EnvelopeLimits {
                max_payload_bytes: 16 * 1024 * 1024,
                ..EnvelopeLimits::default()
            },
            max_records: 1000,
            max_parts: 4000,
            max_record_bytes: 1024 * 1024,
        }
    }
}

/// Borrowed application record supplied to the encoder.
#[derive(Debug, Clone, Copy)]
pub struct Record<'a> {
    /// Payload representation; brokers preserve it without decoding.
    pub encoding: Encoding,
    /// Stable record identity; retries retain it along with multipart bytes.
    pub message_id: MessageId,
    /// Ordered parts. At least one part is required; a part may be empty.
    pub parts: &'a [&'a [u8]],
}

pub(super) fn records_size(
    records: &[Record<'_>],
    first: u64,
    limits: DataLimits,
) -> Result<(usize, usize), CodecError> {
    record_sizes(
        records
            .iter()
            .map(|r| (r.message_id, r.encoding, r.parts.iter().copied())),
        first,
        limits,
    )
}

pub(super) fn record_sizes<'a, I, P>(
    records: I,
    first: u64,
    limits: DataLimits,
) -> Result<(usize, usize), CodecError>
where
    I: ExactSizeIterator<Item = (MessageId, Encoding, P)>,
    P: Iterator<Item = &'a [u8]>,
{
    record_count(records.len(), first, limits)?;
    let mut metadata = 4_usize;
    let mut payload = 0_usize;
    let mut parts = 0_usize;
    for (_, encoding, record) in records {
        metadata = add(metadata, encoding.head_bytes())?;
        let mut record_parts = 0_usize;
        let mut record_bytes = 0_usize;
        for part in record {
            if encoding
                .frame_offset(part, limits.max_parts, limits.max_record_bytes)
                .is_none()
            {
                return Err(CodecError::Length);
            }
            parts = add(parts, 1)?;
            record_parts = add(record_parts, 1)?;
            count(parts)?;
            if parts > limits.max_parts {
                return Err(CodecError::Limit);
            }
            metadata = add(metadata, 4)?;
            count(part.len())?;
            payload = add(payload, part.len())?;
            record_bytes = add(record_bytes, part.len())?;
            if record_bytes > limits.max_record_bytes {
                return Err(CodecError::Limit);
            }
        }
        if !encoding.validate(record_parts, limits.max_parts, limits.max_record_bytes) {
            return Err(CodecError::Parts);
        }
    }
    Ok((metadata, payload))
}

/// Caller validated sizes and reserved both complete output frames.
pub(super) fn encode_records(
    records: &[Record<'_>],
    metadata: &mut Vec<u8>,
    payload: Option<&mut Vec<u8>>,
) {
    write_record_frames(
        records
            .iter()
            .map(|r| (r.message_id, r.encoding, r.parts.iter().copied())),
        metadata,
        payload,
    );
}

/// Same descriptor writer for borrowed slices and owned-runtime iterators.
pub(super) fn write_records<'a, I, P>(records: I, metadata: &mut Vec<u8>, payload: &mut Vec<u8>)
where
    I: ExactSizeIterator<Item = (MessageId, Encoding, P)>,
    P: Iterator<Item = &'a [u8]>,
{
    write_record_frames(records, metadata, Some(payload));
}

fn write_record_frames<'a, I, P>(records: I, metadata: &mut Vec<u8>, payload: Option<&mut Vec<u8>>)
where
    I: ExactSizeIterator<Item = (MessageId, Encoding, P)>,
    P: Iterator<Item = &'a [u8]>,
{
    metadata.extend_from_slice(&(records.len() as u32).to_be_bytes());
    write_record_entries(records, metadata, payload);
}

/// Append validated entries without a new collection count.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "callers validated each part count against max_parts, a u32"
)]
pub(super) fn write_record_entries<'a, I, P>(
    records: I,
    metadata: &mut Vec<u8>,
    mut payload: Option<&mut Vec<u8>>,
) where
    I: Iterator<Item = (MessageId, Encoding, P)>,
    P: Iterator<Item = &'a [u8]>,
{
    for (id, encoding, parts) in records {
        metadata.extend_from_slice(id.as_bytes());
        let count_at = metadata.len();
        metadata.extend_from_slice(&[0; 4]);
        if let Encoding::Lz4 { decoded_bytes } = encoding {
            metadata.extend_from_slice(&decoded_bytes.to_be_bytes());
        }
        let mut count = 0_u32;
        for part in parts {
            count += 1;
            metadata.extend_from_slice(&(part.len() as u32).to_be_bytes());
            if let Some(payload) = payload.as_mut() {
                payload.extend_from_slice(part);
            }
        }
        // The placeholder above is in bounds, so this cannot fail.
        let _ = crate::layout::patch(metadata, count_at, (count | encoding.tag()).to_be_bytes());
    }
}

pub(super) fn command(
    envelope: Envelope,
    opcode: Opcode,
    response: bool,
) -> Result<(), CodecError> {
    if envelope.opcode != opcode || envelope.response != response || envelope.request_id.is_none() {
        return Err(CodecError::Command);
    }
    Ok(())
}

pub(super) fn record_count(
    records: usize,
    first: u64,
    limits: DataLimits,
) -> Result<(), CodecError> {
    if records == 0 || records > limits.max_records {
        return Err(CodecError::Limit);
    }
    first
        .checked_add(u64::from(count(records)?))
        .ok_or(CodecError::Length)?;
    Ok(())
}

pub(super) fn add(left: usize, right: usize) -> Result<usize, CodecError> {
    left.checked_add(right).ok_or(CodecError::Length)
}
/// Checked total of encoded field sizes.
pub(super) fn sum(terms: &[usize]) -> Result<usize, CodecError> {
    terms.iter().try_fold(0, |total, &term| add(total, term))
}
pub(super) fn count(value: usize) -> Result<u32, CodecError> {
    u32::try_from(value).map_err(|_| CodecError::Length)
}
pub(super) fn capacity(buffer: &Vec<u8>, length: usize) -> Result<(), CodecError> {
    if buffer.capacity() < length {
        return Err(CodecError::Capacity);
    }
    Ok(())
}

/// Structural rejection. No error establishes whether an earlier retry committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// Envelope, bounds, correlation, or link-session shape is invalid.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// Wrong opcode, direction, or correlation shape.
    #[error("invalid data command envelope")]
    Command,
    /// Zero identity or required fencing epoch.
    #[error("invalid data identity or epoch")]
    Identity,
    /// Unknown policy byte.
    #[error("unknown completion policy")]
    Policy,
    /// Empty batch or exceeded record/part bounds.
    #[error("data command exceeds record or part limits")]
    Limit,
    /// A record must contain at least one part, which may itself be empty.
    #[error("record has no payload parts")]
    Parts,
    /// Truncated/trailing bytes, inconsistent packed lengths, or arithmetic overflow.
    #[error("invalid data command length or sequence range")]
    Length,
    /// Caller must reserve both output arenas before encoding.
    #[error("data output buffer capacity exhausted")]
    Capacity,
    /// Invalid confirmation position or progress beyond the received prefix.
    #[error("invalid data confirmation position")]
    Position,
    /// Unsupported reader mode, delivery requirement, or progress evidence.
    #[error("unsupported reader profile")]
    Profile,
}

impl From<LayoutError> for CodecError {
    fn from(_: LayoutError) -> Self {
        Self::Length
    }
}

fn descriptor(cursor: &mut Reader<'_>) -> Result<(usize, Encoding), CodecError> {
    let tagged = cursor.u32()?;
    let encoding = match tagged >> 24 {
        0 => Encoding::Raw,
        1 => Encoding::Lz4 {
            decoded_bytes: cursor.u32()?,
        },
        _ => return Err(CodecError::Profile),
    };
    Ok(((tagged & 0x00ff_ffff) as usize, encoding))
}
