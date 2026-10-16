//! Validated record collections: decoding against their exact payload and
//! borrowed iteration over records and parts.

use super::{CodecError, DataLimits, Encoding, add, descriptor, record_count};
use crate::{MessageId, layout::Reader};

/// Validated record collection with no allocated descriptor table.
#[derive(Debug, Clone, Copy)]
pub struct Records<'a> {
    pub(super) descriptors: &'a [u8],
    pub(super) payload: &'a [u8],
    pub(super) count: usize,
    /// Totals the validating walk already computed, when this view came
    /// straight from it. Derived views compute them on demand.
    pub(super) summary: Option<RecordSummary>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RecordSummary {
    parts: usize,
    nonzero_message_ids: bool,
}

/// Validate a counted record descriptor table against the exact payload it
/// covers. The first sequence bounds the collection's position range.
pub fn decode_records<'a>(
    metadata: &'a [u8],
    payload: &'a [u8],
    first: u64,
    limits: DataLimits,
) -> Result<Records<'a>, CodecError> {
    let mut cursor = Reader::new(metadata);
    let count = cursor.u32()? as usize;
    let (parts, nonzero_message_ids) =
        validate_record_entries(cursor.rest(), payload, count, first, limits)?;
    Ok(Records {
        descriptors: cursor.rest(),
        payload,
        count,
        summary: Some(RecordSummary {
            parts,
            nonzero_message_ids,
        }),
    })
}

/// Validate a descriptor table without its collection count. Returns the
/// total part count and whether every record identity is nonzero.
pub(crate) fn validate_record_entries(
    descriptors: &[u8],
    payload: &[u8],
    count: usize,
    first: u64,
    limits: DataLimits,
) -> Result<(usize, bool), CodecError> {
    record_count(count, first, limits)?;
    let mut cursor = Reader::new(descriptors);
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
        let mut record_bytes = 0_usize;
        for length in lengths.as_chunks::<4>().0 {
            let length = u32::from_be_bytes(*length) as usize;
            payload_bytes = add(payload_bytes, length)?;
            record_bytes = add(record_bytes, length)?;
            if payload_bytes > payload.len() {
                return Err(CodecError::Length);
            }
            if record_bytes > limits.max_record_bytes {
                return Err(CodecError::Limit);
            }
        }
        if encoding
            .frame_offset(
                payload
                    .get(payload_before..payload_bytes)
                    .ok_or(CodecError::Length)?,
                limits.max_parts,
                limits.max_record_bytes,
            )
            .is_none()
        {
            return Err(CodecError::Length);
        }
    }
    if !cursor.rest().is_empty() || payload_bytes != payload.len() {
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
        iter.by_ref().take(count).for_each(drop);
        Self {
            descriptors: iter.descriptors.rest(),
            payload: iter.payload,
            count: iter.remaining,
            summary: None,
        }
    }

    /// Iterate over validated records without copying payloads or allocating.
    pub fn iter(self) -> RecordIter<'a> {
        RecordIter {
            descriptors: Reader::new(self.descriptors),
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
    pub(super) descriptors: Reader<'a>,
    pub(super) payload: &'a [u8],
    pub(super) remaining: usize,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = DecodedRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.remaining.checked_sub(1)?;
        // The collection was validated, so no read below fails.
        let message_id = MessageId::from_bytes(self.descriptors.array().ok()?);
        let (count, encoding) = descriptor(&mut self.descriptors).ok()?;
        let lengths = self.descriptors.take(count.checked_mul(4)?).ok()?;
        let bytes = lengths
            .as_chunks::<4>()
            .0
            .iter()
            .map(|part| u32::from_be_bytes(*part) as usize)
            .sum();
        let (payload, rest) = self.payload.split_at_checked(bytes)?;
        self.payload = rest;
        self.remaining = remaining;
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
    pub(super) lengths: std::slice::Iter<'a, [u8; 4]>,
    pub(super) payload: &'a [u8],
}

impl<'a> Iterator for Parts<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let length = u32::from_be_bytes(*self.lengths.next()?) as usize;
        let (part, rest) = self.payload.split_at_checked(length)?;
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
        let at = self.payload.len().checked_sub(length)?;
        let (rest, part) = self.payload.split_at_checked(at)?;
        self.payload = rest;
        Some(part)
    }
}
