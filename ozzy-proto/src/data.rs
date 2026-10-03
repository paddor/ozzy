//! Shared bounded data-command fields and multipart records.

use super::{Envelope, EnvelopeError, EnvelopeLimits, Opcode};
use crate::{GroupId, MessageId};

mod batch;
mod encoding;
pub use encoding::Encoding;
mod buffer;
pub use batch::{BatchIter, BatchRecord, PayloadRef, RecordBatch, TinyRecords};
mod indexed;
mod owned;
pub use buffer::RecordBuffer;
pub use indexed::{EncodedParts, IndexedRecords};
pub use owned::{OwnedRecord, OwnedRecordIter, OwnedRecords};

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

/// Validated record collection with no allocated descriptor table.
#[derive(Debug, Clone, Copy)]
pub struct Records<'a> {
    descriptors: &'a [u8],
    payload: &'a [u8],
    count: usize,
    /// Totals the validating walk already computed, when this view came
    /// straight from it. Derived views compute them on demand.
    summary: Option<RecordSummary>,
}

#[derive(Debug, Clone, Copy)]
struct RecordSummary {
    parts: usize,
    nonzero_message_ids: bool,
}

/// Validated raw-record descriptors without materializing their payload.
#[derive(Debug, Clone, Copy)]
pub struct RecordDescriptors<'a> {
    framed: &'a [u8],
    descriptors: &'a [u8],
    count: usize,
    payload_bytes: usize,
    parts: usize,
    nonzero_message_ids: bool,
}

/// One raw record descriptor and its original multipart lengths.
#[derive(Debug, Clone, Copy)]
pub struct RecordDescriptor<'a> {
    /// Stable record identity.
    pub message_id: MessageId,
    /// Original multipart payload lengths in network byte order.
    pub part_lengths: &'a [[u8; 4]],
}

/// Iterator over validated raw record descriptors.
#[derive(Debug, Clone)]
pub struct RecordDescriptorIter<'a> {
    descriptors: Cursor<'a>,
    remaining: usize,
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
        metadata = add(metadata, 20 + encoding.metadata_bytes())?;
        let before = parts;
        let payload_before = payload;
        for part in record {
            if encoding
                .frame_offset(part, limits.max_parts, limits.max_record_bytes)
                .is_none()
            {
                return Err(CodecError::Length);
            }
            parts = add(parts, 1)?;
            count(parts)?;
            if parts > limits.max_parts {
                return Err(CodecError::Limit);
            }
            metadata = add(metadata, 4)?;
            count(part.len())?;
            payload = add(payload, part.len())?;
            if payload - payload_before > limits.max_record_bytes {
                return Err(CodecError::Limit);
            }
        }
        if !encoding.validate(parts - before, limits.max_parts, limits.max_record_bytes) {
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
        metadata[count_at..count_at + 4].copy_from_slice(&(count | encoding.tag()).to_be_bytes());
    }
}

/// Validate a counted record descriptor table against the exact payload it
/// covers. The first sequence bounds the collection's position range.
pub fn decode_records<'a>(
    metadata: &'a [u8],
    payload: &'a [u8],
    first: u64,
    limits: DataLimits,
) -> Result<Records<'a>, CodecError> {
    let mut cursor = Cursor(metadata);
    let count = cursor.u32()? as usize;
    let (parts, nonzero_message_ids) =
        validate_record_entries(cursor.0, payload, count, first, limits)?;
    Ok(Records {
        descriptors: cursor.0,
        payload,
        count,
        summary: Some(RecordSummary {
            parts,
            nonzero_message_ids,
        }),
    })
}

/// Validate APPEND record metadata against its declared decoded payload length.
/// Every record must use raw per-record encoding; whole-APPEND compression is
/// represented separately by the command payload encoding.
pub fn decode_record_descriptors(
    metadata: &[u8],
    payload_bytes: usize,
    first: u64,
    limits: DataLimits,
) -> Result<RecordDescriptors<'_>, CodecError> {
    let mut cursor = Cursor(metadata);
    let count = cursor.u32()? as usize;
    let (parts, descriptors, nonzero_message_ids) =
        validate_raw_record_entries(cursor.0, count, payload_bytes, first, limits)?;
    Ok(RecordDescriptors {
        framed: metadata,
        descriptors,
        count,
        payload_bytes,
        parts,
        nonzero_message_ids,
    })
}

pub(super) fn validate_raw_record_entries(
    descriptors: &[u8],
    count: usize,
    payload_bytes: usize,
    first: u64,
    limits: DataLimits,
) -> Result<(usize, &[u8], bool), CodecError> {
    record_count(count, first, limits)?;
    if payload_bytes > limits.envelope.max_payload_bytes {
        return Err(CodecError::Limit);
    }
    let mut cursor = Cursor(descriptors);
    let mut decoded = 0_usize;
    let mut total_parts = 0_usize;
    let mut nonzero_message_ids = true;
    for _ in 0..count {
        let before = decoded;
        nonzero_message_ids &= cursor.take(16)? != [0; 16];
        let (parts, encoding) = descriptor(&mut cursor)?;
        if encoding != Encoding::Raw {
            return Err(CodecError::Profile);
        }
        if parts == 0 {
            return Err(CodecError::Parts);
        }
        total_parts = add(total_parts, parts)?;
        if total_parts > limits.max_parts {
            return Err(CodecError::Limit);
        }
        let lengths = cursor.take(parts.checked_mul(4).ok_or(CodecError::Length)?)?;
        for length in lengths.as_chunks::<4>().0 {
            decoded = add(decoded, u32::from_be_bytes(*length) as usize)?;
            if decoded > payload_bytes {
                return Err(CodecError::Length);
            }
            if decoded - before > limits.max_record_bytes {
                return Err(CodecError::Limit);
            }
        }
    }
    if !cursor.0.is_empty() || decoded != payload_bytes {
        return Err(CodecError::Length);
    }
    Ok((total_parts, descriptors, nonzero_message_ids))
}

impl<'a> RecordDescriptors<'a> {
    /// Collection count followed by exact record descriptor bytes.
    pub const fn framed(self) -> &'a [u8] {
        self.framed
    }

    /// Exact descriptor bytes without the collection count.
    pub const fn encoded(self) -> &'a [u8] {
        self.descriptors
    }

    /// Number of records.
    pub const fn len(self) -> usize {
        self.count
    }

    /// Whether the collection has no records.
    pub const fn is_empty(self) -> bool {
        self.count == 0
    }

    /// Total decoded application payload bytes.
    pub const fn payload_bytes(self) -> usize {
        self.payload_bytes
    }

    /// Total number of multipart payload parts.
    pub const fn parts(self) -> usize {
        self.parts
    }

    /// Whether every record carries a nonzero retry identity.
    pub const fn nonzero_message_ids(self) -> bool {
        self.nonzero_message_ids
    }

    /// Iterate over record identities and multipart lengths.
    pub fn iter(self) -> RecordDescriptorIter<'a> {
        RecordDescriptorIter {
            descriptors: Cursor(self.descriptors),
            remaining: self.count,
        }
    }
}

impl<'a> Iterator for RecordDescriptorIter<'a> {
    type Item = RecordDescriptor<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let message_id = MessageId::from_bytes(
            self.descriptors
                .array()
                .expect("validated record descriptor"),
        );
        let (parts, encoding) =
            descriptor(&mut self.descriptors).expect("validated record descriptor");
        debug_assert_eq!(encoding, Encoding::Raw);
        let lengths = self
            .descriptors
            .take(parts * 4)
            .expect("validated record lengths")
            .as_chunks::<4>()
            .0;
        self.remaining -= 1;
        Some(RecordDescriptor {
            message_id,
            part_lengths: lengths,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for RecordDescriptorIter<'_> {}
impl std::iter::FusedIterator for RecordDescriptorIter<'_> {}

/// Validate a descriptor table without its collection count. Returns the
/// total part count and whether every record identity is nonzero.
pub(super) fn validate_record_entries(
    descriptors: &[u8],
    payload: &[u8],
    count: usize,
    first: u64,
    limits: DataLimits,
) -> Result<(usize, bool), CodecError> {
    record_count(count, first, limits)?;
    let mut cursor = Cursor(descriptors);
    let mut payload_bytes = 0_usize;
    let mut total_parts = 0_usize;
    let mut nonzero_message_ids = true;
    for _ in 0..count {
        let payload_before = payload_bytes;
        nonzero_message_ids &= cursor.take(16)? != [0; 16];
        let (parts, encoding) = descriptor(&mut cursor)?;
        if !encoding.validate(parts, limits.max_parts, limits.max_record_bytes) {
            return Err(CodecError::Limit);
        }
        if parts == 0 {
            return Err(CodecError::Parts);
        }
        total_parts = add(total_parts, parts)?;
        if total_parts > limits.max_parts {
            return Err(CodecError::Limit);
        }
        let lengths = cursor.take(parts.checked_mul(4).ok_or(CodecError::Length)?)?;
        for length in lengths.as_chunks::<4>().0 {
            payload_bytes = add(payload_bytes, u32::from_be_bytes(*length) as usize)?;
            if payload_bytes > payload.len() {
                return Err(CodecError::Length);
            }
            if payload_bytes - payload_before > limits.max_record_bytes {
                return Err(CodecError::Limit);
            }
        }
        if encoding
            .frame_offset(
                &payload[payload_before..payload_bytes],
                limits.max_parts,
                limits.max_record_bytes,
            )
            .is_none()
        {
            return Err(CodecError::Length);
        }
    }
    if !cursor.0.is_empty() || payload_bytes != payload.len() {
        return Err(CodecError::Length);
    }
    Ok((total_parts, nonzero_message_ids))
}

impl<'a> Records<'a> {
    /// Validated descriptor and payload spans, without the collection count.
    pub const fn encoded(self) -> (&'a [u8], &'a [u8]) {
        (self.descriptors, self.payload)
    }

    /// Total multipart parts across every record, including empty parts.
    pub fn parts(self) -> usize {
        match self.summary {
            Some(summary) => summary.parts,
            None => self.iter().map(|record| record.parts.len()).sum(),
        }
    }

    /// Whether every record carries a nonzero retry identity.
    pub fn nonzero_message_ids(self) -> bool {
        match self.summary {
            Some(summary) => summary.nonzero_message_ids,
            None => self
                .iter()
                .all(|record| record.message_id.as_bytes() != &[0; 16]),
        }
    }
    /// Exact validated descriptor bytes, excluding the collection count.
    pub const fn descriptor_bytes(self) -> usize {
        self.descriptors.len()
    }

    /// Total opaque application bytes, excluding metadata and routing.
    pub const fn payload_bytes(self) -> usize {
        self.payload.len()
    }

    /// Number of records, checked against the receive limit.
    pub const fn len(self) -> usize {
        self.count
    }

    /// Whether this collection contains no records.
    pub const fn is_empty(self) -> bool {
        self.count == 0
    }

    /// The records after the first `count`, without copying or revalidation.
    /// A reader uses this when part of a shared batch was already delivered.
    #[must_use]
    pub fn skip(self, count: usize) -> Self {
        let mut iter = self.iter();
        let skipped = iter.by_ref().take(count).count();
        Self {
            descriptors: iter.descriptors.0,
            payload: iter.payload,
            count: self.count - skipped,
            summary: None,
        }
    }

    /// Iterate over validated records without copying payloads or allocating.
    pub fn iter(self) -> RecordIter<'a> {
        RecordIter {
            descriptors: Cursor(self.descriptors),
            payload: self.payload,
            remaining: self.count,
        }
    }
}

/// One decoded record with an independent iterator over its borrowed parts.
#[derive(Debug, Clone)]
pub struct DecodedRecord<'a> {
    /// Payload representation.
    pub encoding: Encoding,
    /// Stable record identity.
    pub message_id: MessageId,
    /// Ordered parts, including empty ones.
    pub parts: Parts<'a>,
}

/// Iterator over a previously validated collection.
#[derive(Debug, Clone)]
pub struct RecordIter<'a> {
    descriptors: Cursor<'a>,
    payload: &'a [u8],
    remaining: usize,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = DecodedRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let message_id =
            MessageId::from_bytes(self.descriptors.array().expect("validated record ID"));
        let (count, encoding) = descriptor(&mut self.descriptors).expect("validated descriptor");
        let lengths = self
            .descriptors
            .take(count * 4)
            .expect("validated part lengths");
        let bytes = lengths
            .as_chunks::<4>()
            .0
            .iter()
            .map(|part| u32::from_be_bytes(*part) as usize)
            .sum();
        let (payload, rest) = self.payload.split_at(bytes);
        self.payload = rest;
        self.remaining -= 1;
        Some(DecodedRecord {
            encoding,
            message_id,
            parts: Parts {
                lengths: lengths.as_chunks::<4>().0.iter(),
                payload,
            },
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for RecordIter<'_> {}
impl std::iter::FusedIterator for RecordIter<'_> {}

/// Borrowed multipart iterator. Dropping it cannot disturb the next record.
#[derive(Debug, Clone)]
pub struct Parts<'a> {
    lengths: std::slice::Iter<'a, [u8; 4]>,
    payload: &'a [u8],
}

impl<'a> Iterator for Parts<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let length = u32::from_be_bytes(*self.lengths.next()?) as usize;
        let (part, rest) = self.payload.split_at(length);
        self.payload = rest;
        Some(part)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.lengths.size_hint()
    }
}

impl ExactSizeIterator for Parts<'_> {}
impl std::iter::FusedIterator for Parts<'_> {}

impl DoubleEndedIterator for Parts<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let length = u32::from_be_bytes(*self.lengths.next_back()?) as usize;
        let (rest, part) = self.payload.split_at(self.payload.len() - length);
        self.payload = rest;
        Some(part)
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
pub(super) fn count(value: usize) -> Result<u32, CodecError> {
    u32::try_from(value).map_err(|_| CodecError::Length)
}
pub(super) fn capacity(buffer: &Vec<u8>, length: usize) -> Result<(), CodecError> {
    if buffer.capacity() < length {
        return Err(CodecError::Capacity);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Cursor<'a>(pub(super) &'a [u8]);

impl<'a> Cursor<'a> {
    pub(super) fn take(&mut self, count: usize) -> Result<&'a [u8], CodecError> {
        let (bytes, rest) = self.0.split_at_checked(count).ok_or(CodecError::Length)?;
        self.0 = rest;
        Ok(bytes)
    }
    pub(super) fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        Ok(self.take(N)?.try_into().expect("checked field"))
    }
    pub(super) fn byte(&mut self) -> Result<u8, CodecError> {
        Ok(self.array::<1>()?[0])
    }
    pub(super) fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    pub(super) fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
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

fn descriptor(cursor: &mut Cursor<'_>) -> Result<(usize, Encoding), CodecError> {
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
