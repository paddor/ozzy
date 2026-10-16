//! Share one contiguous canonical payload; mixed backings use the reserved buffer.

use super::{CodecError, RecordsEncoder, data, frame_capacity};
use crate::layout;
use bytes::{Buf, Bytes};
use std::ops::Range;

#[derive(Debug)]
pub(super) struct Payload {
    body: Bytes,
    range: Range<usize>,
}

impl Payload {
    pub(super) fn finish(mut self) -> Bytes {
        self.body.truncate(self.range.end);
        self.body.advance(self.range.start);
        self.body
    }
}

/// One canonical producer LZ4 block within an immutable body.
#[derive(Clone, Debug)]
pub struct PreparedBlock<'a> {
    /// Descriptor table of the uncompressed records.
    pub descriptors: &'a [u8],
    /// Total uncompressed payload bytes the descriptors declare.
    pub decoded_bytes: usize,
    /// Body that holds the block.
    pub body: &'a Bytes,
    /// Exact block range within `body`.
    pub encoded: Range<usize>,
}

impl RecordsEncoder<'_> {
    /// Adopt one complete canonical producer LZ4 block into an empty response.
    /// Descriptors are checked against the declared decoded byte count; the
    /// transmitted payload remains the exact producer bytes.
    pub fn extend_shared_prepared_lz4(
        &mut self,
        block: PreparedBlock<'_>,
        count: usize,
    ) -> Result<bool, CodecError> {
        let PreparedBlock {
            descriptors,
            decoded_bytes,
            body,
            encoded,
        } = block;
        if !self.shared_enabled || !self.is_empty() || self.shared.is_some() {
            return Ok(false);
        }
        let encoded_payload = body.get(encoded.clone()).ok_or(CodecError::Length)?;
        if encoded_payload.is_empty() {
            return Err(CodecError::Length);
        }
        data::record_count(count, self.first_offset, self.limits)?;
        frame_capacity(
            self.metadata,
            data::add(self.metadata.len(), descriptors.len())?,
            self.limits.envelope.max_metadata_bytes,
        )?;
        frame_capacity(
            self.payload,
            encoded_payload.len(),
            self.limits.envelope.max_payload_bytes,
        )?;
        let parts = data::validate_raw_record_entries(
            descriptors,
            count,
            decoded_bytes,
            self.first_offset,
            self.remaining(),
        )?
        .parts;
        self.metadata.extend_from_slice(descriptors);
        self.share(body, encoded);
        self.records = count;
        self.parts = parts;
        self.decoded_payload_bytes = decoded_bytes;
        layout::patch(self.metadata, self.count_at, (count as u32).to_be_bytes())?;
        self.set_payload_encoding(crate::append::PayloadEncoding::Lz4, decoded_bytes)?;
        Ok(true)
    }

    /// Append an entire general descriptor table and share its contiguous payload.
    /// Validate all input and remaining output capacity before changing output. Returns
    /// false when sharing would require copying a previous or nonadjacent span.
    pub fn extend_shared_packed(
        &mut self,
        descriptors: &[u8],
        body: &Bytes,
        range: Range<usize>,
        count: usize,
    ) -> Result<bool, CodecError> {
        if self.has_encoded_payload()
            || !self.shared_enabled
            || (self.shared.is_none() && !self.is_empty())
        {
            return Ok(false);
        }
        let payload = body.get(range.clone()).ok_or(CodecError::Length)?;
        if !self.continues_shared(body, &range) {
            return Ok(false);
        }
        let first = self
            .first_offset
            .checked_add(self.records as u64)
            .ok_or(CodecError::Length)?;
        let records = data::add(self.records, count)?;
        data::count(records)?;
        frame_capacity(
            self.metadata,
            data::add(self.metadata.len(), descriptors.len())?,
            self.limits.envelope.max_metadata_bytes,
        )?;
        frame_capacity(
            self.payload,
            data::add(self.payload_bytes(), payload.len())?,
            self.limits.envelope.max_payload_bytes,
        )?;
        let parts =
            data::validate_record_entries(descriptors, payload, count, first, self.remaining())?.0;
        let parts = data::add(self.parts, parts)?;
        self.metadata.extend_from_slice(descriptors);
        self.share(body, range);
        self.records = records;
        self.parts = parts;
        layout::patch(self.metadata, self.count_at, (records as u32).to_be_bytes())?;
        self.set_raw_payload_bytes()?;
        Ok(true)
    }

    /// Permit shared payloads. Call `finish_with_payload` to retain their owner.
    /// The caller must bound retained backing memory and outstanding replies.
    pub fn allow_shared_payload(&mut self) {
        self.shared_enabled = true;
    }

    /// Bound additional retained source memory by the reserved fallback buffer.
    pub fn shared_backing_limit(&self) -> usize {
        if self.shared_enabled {
            self.payload.capacity()
        } else {
            0
        }
    }

    /// Logical payload size, whether shared or copied.
    pub fn payload_bytes(&self) -> usize {
        self.shared
            .as_ref()
            .map_or(self.payload.len(), |p| p.range.len())
    }

    /// Append a record from immutable canonical backing without copying payload.
    /// Returns false for disabled sharing, mixed owners, or noncontiguous parts;
    /// the caller can then use ordinary encoding. Rejection changes no bytes.
    /// Cloned part iterators must yield identical ranges.
    pub fn push_shared(
        &mut self,
        id: crate::MessageId,
        encoding: data::Encoding,
        body: &Bytes,
        parts: impl ExactSizeIterator<Item = Range<usize>> + Clone,
    ) -> Result<bool, CodecError> {
        if self.has_encoded_payload()
            || !self.shared_enabled
            || (self.shared.is_none() && !self.is_empty())
        {
            return Ok(false);
        }
        let count = parts.len();
        if !encoding.validate(count, self.limits.max_parts, self.limits.max_record_bytes) {
            return Err(CodecError::Parts);
        }
        let Some(range) = contiguous(body.len(), parts.clone())? else {
            return Ok(false);
        };
        if !self.continues_shared(body, &range) {
            return Ok(false);
        }
        let bytes = data::add(self.payload_bytes(), range.len())?;
        let total_parts = data::add(self.parts, count)?;
        if range.len() > self.limits.max_record_bytes || total_parts > self.limits.max_parts {
            return Err(CodecError::Limit);
        }
        if encoding
            .frame_offset(
                body.get(range.clone()).ok_or(CodecError::Length)?,
                self.limits.max_parts,
                self.limits.max_record_bytes,
            )
            .is_none()
        {
            return Err(CodecError::Length);
        }
        let records = data::add(self.records, 1)?;
        data::record_count(records, self.first_offset, self.limits)?;
        let descriptor = count
            .checked_mul(4)
            .and_then(|n| n.checked_add(encoding.head_bytes()))
            .ok_or(CodecError::Length)?;
        frame_capacity(
            self.metadata,
            data::add(self.metadata.len(), descriptor)?,
            self.limits.envelope.max_metadata_bytes,
        )?;
        // Keep fallback capacity reserved even while sending shared bytes.
        frame_capacity(self.payload, bytes, self.limits.envelope.max_payload_bytes)?;
        write_descriptor(self.metadata, id, encoding, parts)?;
        self.share(body, range);
        self.records = records;
        self.parts = total_parts;
        layout::patch(self.metadata, self.count_at, (records as u32).to_be_bytes())?;
        self.set_raw_payload_bytes()?;
        Ok(true)
    }

    /// Whether `range` of `body` directly follows the shared payload, or
    /// would start it.
    fn continues_shared(&self, body: &Bytes, range: &Range<usize>) -> bool {
        self.shared.as_ref().is_none_or(|previous| {
            previous.body.as_ptr() == body.as_ptr()
                && previous.body.len() == body.len()
                && previous.range.end == range.start
        })
    }

    fn share(&mut self, body: &Bytes, range: Range<usize>) {
        match &mut self.shared {
            Some(previous) => previous.range.end = range.end,
            None => {
                self.shared = Some(Payload {
                    body: body.clone(),
                    range,
                });
            }
        }
    }

    pub(super) fn materialize(&mut self) -> Result<(), CodecError> {
        debug_assert!(!self.has_encoded_payload());
        if let Some(shared) = self.shared.take() {
            debug_assert_eq!(self.payload.len(), 0);
            debug_assert!(self.payload.capacity() >= shared.range.len());
            let body = shared.body.get(shared.range).ok_or(CodecError::Length)?;
            self.payload.extend_from_slice(body);
        }
        Ok(())
    }
}

/// The span of `parts` when each starts where the previous one ends, or
/// `None` at the first gap.
fn contiguous(
    body_len: usize,
    parts: impl Iterator<Item = Range<usize>> + Clone,
) -> Result<Option<Range<usize>>, CodecError> {
    let first = parts.clone().next().ok_or(CodecError::Parts)?;
    let mut end = first.start;
    for part in parts {
        if part.start != end {
            return Ok(None);
        }
        if part.end < part.start || part.end > body_len {
            return Err(CodecError::Length);
        }
        data::count(part.len())?;
        end = part.end;
    }
    Ok(Some(first.start..end))
}

fn write_descriptor(
    metadata: &mut Vec<u8>,
    id: crate::MessageId,
    encoding: data::Encoding,
    parts: impl ExactSizeIterator<Item = Range<usize>>,
) -> Result<(), CodecError> {
    metadata.extend_from_slice(id.as_bytes());
    metadata.extend_from_slice(&(data::count(parts.len())? | encoding.tag()).to_be_bytes());
    if let data::Encoding::Lz4 { decoded_bytes } = encoding {
        metadata.extend_from_slice(&decoded_bytes.to_be_bytes());
    }
    for part in parts {
        metadata.extend_from_slice(&(part.len() as u32).to_be_bytes());
    }
    Ok(())
}
