//! Raw APPEND record descriptors validated against a declared decoded
//! payload length, without the payload itself.

use super::{CodecError, DataLimits, Encoding, add, descriptor, record_count};
use crate::{MessageId, layout::Reader};

/// Validated raw-record descriptors without materializing their payload.
#[derive(Debug, Clone, Copy)]
pub struct RecordDescriptors<'a> {
    framed: &'a [u8],
    descriptors: &'a [u8],
    count: usize,
    payload_bytes: usize,
    parts: usize,
    nonzero_message_ids: bool,
    uniform_part_bytes: Option<u32>,
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
    descriptors: Reader<'a>,
    remaining: usize,
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
    let mut cursor = Reader::new(metadata);
    let count = cursor.u32()? as usize;
    let entries = validate_raw_record_entries(cursor.rest(), count, payload_bytes, first, limits)?;
    Ok(RecordDescriptors {
        framed: metadata,
        descriptors: entries.descriptors,
        count,
        payload_bytes,
        parts: entries.parts,
        nonzero_message_ids: entries.nonzero_message_ids,
        uniform_part_bytes: entries.uniform_part_bytes,
    })
}

pub(crate) struct RawRecordEntries<'a> {
    pub(crate) parts: usize,
    pub(crate) descriptors: &'a [u8],
    pub(crate) nonzero_message_ids: bool,
    pub(crate) uniform_part_bytes: Option<u32>,
}

pub(crate) fn validate_raw_record_entries(
    descriptors: &[u8],
    count: usize,
    payload_bytes: usize,
    first: u64,
    limits: DataLimits,
) -> Result<RawRecordEntries<'_>, CodecError> {
    record_count(count, first, limits)?;
    if payload_bytes > limits.envelope.max_payload_bytes {
        return Err(CodecError::Limit);
    }
    let mut cursor = Reader::new(descriptors);
    let mut decoded = 0_usize;
    let mut total_parts = 0_usize;
    let mut nonzero_message_ids = true;
    let mut uniform_part_bytes = None;
    for index in 0..count {
        let mut record_bytes = 0_usize;
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
        // Only a single part has exactly four length bytes.
        let single = <[u8; 4]>::try_from(lengths).ok().map(u32::from_be_bytes);
        if index == 0 {
            uniform_part_bytes = single;
        } else if uniform_part_bytes != single {
            uniform_part_bytes = None;
        }
        for length in lengths.as_chunks::<4>().0 {
            let length = u32::from_be_bytes(*length) as usize;
            decoded = add(decoded, length)?;
            record_bytes = add(record_bytes, length)?;
            if decoded > payload_bytes {
                return Err(CodecError::Length);
            }
            if record_bytes > limits.max_record_bytes {
                return Err(CodecError::Limit);
            }
        }
    }
    if !cursor.rest().is_empty() || decoded != payload_bytes {
        return Err(CodecError::Length);
    }
    Ok(RawRecordEntries {
        parts: total_parts,
        descriptors,
        nonzero_message_ids,
        uniform_part_bytes,
    })
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

    /// Part length when every record has exactly one part of that length.
    /// Computed during validation, including zero-length parts.
    pub const fn uniform_part_bytes(self) -> Option<u32> {
        self.uniform_part_bytes
    }

    /// Iterate over record identities and multipart lengths.
    pub fn iter(self) -> RecordDescriptorIter<'a> {
        RecordDescriptorIter {
            descriptors: Reader::new(self.descriptors),
            remaining: self.count,
        }
    }
}

impl<'a> Iterator for RecordDescriptorIter<'a> {
    type Item = RecordDescriptor<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.remaining.checked_sub(1)?;
        // The table was validated, so no read below fails.
        let message_id = MessageId::from_bytes(self.descriptors.array().ok()?);
        let (parts, encoding) = descriptor(&mut self.descriptors).ok()?;
        debug_assert_eq!(encoding, Encoding::Raw);
        let lengths = self
            .descriptors
            .take(parts.checked_mul(4)?)
            .ok()?
            .as_chunks::<4>()
            .0;
        self.remaining = remaining;
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
