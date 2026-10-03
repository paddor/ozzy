//! Share one contiguous canonical payload; mixed backings use the reserved buffer.

use super::{CodecError, RecordsEncoder, data, frame_capacity};
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

impl RecordsEncoder<'_> {
    /// Adopt one complete canonical producer LZ4 block into an empty response.
    /// Descriptors are checked against the declared decoded byte count; the
    /// transmitted payload remains the exact producer bytes.
    pub fn extend_shared_prepared_lz4(
        &mut self,
        descriptors: &[u8],
        decoded_bytes: usize,
        body: &Bytes,
        encoded: Range<usize>,
        count: usize,
    ) -> Result<bool, CodecError> {
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
        self.shared = Some(Payload {
            body: body.clone(),
            range: encoded,
        });
        self.records = count;
        self.parts = parts;
        self.decoded_payload_bytes = decoded_bytes;
        self.metadata[self.count_at..self.count_at + 4]
            .copy_from_slice(&(count as u32).to_be_bytes());
        self.metadata[self.encoding_at] = crate::append::PayloadEncoding::Lz4 as u8;
        self.metadata[self.encoding_at + 1..self.encoding_at + 5]
            .copy_from_slice(&(decoded_bytes as u32).to_be_bytes());
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
        if let Some(previous) = &self.shared
            && (previous.body.as_ptr() != body.as_ptr()
                || previous.body.len() != body.len()
                || previous.range.end != range.start)
        {
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
        self.metadata.extend_from_slice(descriptors);
        match &mut self.shared {
            Some(previous) => previous.range.end = range.end,
            None => {
                self.shared = Some(Payload {
                    body: body.clone(),
                    range,
                });
            }
        }
        self.records = records;
        self.parts += parts;
        self.metadata[self.count_at..self.count_at + 4]
            .copy_from_slice(&(records as u32).to_be_bytes());
        self.set_raw_payload_bytes();
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
        let first = parts.clone().next().ok_or(CodecError::Parts)?;
        let mut end = first.start;
        for part in parts.clone() {
            if part.start != end {
                return Ok(false);
            }
            if part.end < part.start || part.end > body.len() {
                return Err(CodecError::Length);
            }
            data::count(part.len())?;
            end = part.end;
        }
        let range = first.start..end;
        if let Some(previous) = &self.shared
            && (previous.body.as_ptr() != body.as_ptr()
                || previous.body.len() != body.len()
                || previous.range.end != range.start)
        {
            return Ok(false);
        }
        let bytes = data::add(self.payload_bytes(), range.len())?;
        if range.len() > self.limits.max_record_bytes || count > self.limits.max_parts - self.parts
        {
            return Err(CodecError::Limit);
        }
        if encoding
            .frame_offset(
                &body[range.clone()],
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
            .and_then(|n| n.checked_add(20 + encoding.metadata_bytes()))
            .ok_or(CodecError::Length)?;
        frame_capacity(
            self.metadata,
            data::add(self.metadata.len(), descriptor)?,
            self.limits.envelope.max_metadata_bytes,
        )?;
        // Keep fallback capacity reserved even while sending shared bytes.
        frame_capacity(self.payload, bytes, self.limits.envelope.max_payload_bytes)?;
        self.metadata.extend_from_slice(id.as_bytes());
        self.metadata
            .extend_from_slice(&(data::count(count)? | encoding.tag()).to_be_bytes());
        if let data::Encoding::Lz4 { decoded_bytes } = encoding {
            self.metadata
                .extend_from_slice(&decoded_bytes.to_be_bytes());
        }
        for part in parts {
            self.metadata
                .extend_from_slice(&(part.len() as u32).to_be_bytes());
        }
        match &mut self.shared {
            Some(previous) => previous.range.end = end,
            None => {
                self.shared = Some(Payload {
                    body: body.clone(),
                    range,
                });
            }
        }
        self.records = records;
        self.parts += count;
        self.metadata[self.count_at..self.count_at + 4]
            .copy_from_slice(&(records as u32).to_be_bytes());
        self.set_raw_payload_bytes();
        Ok(true)
    }

    pub(super) fn materialize(&mut self) {
        debug_assert!(!self.has_encoded_payload());
        if let Some(shared) = self.shared.take() {
            debug_assert!(self.payload.is_empty());
            debug_assert!(self.payload.capacity() >= shared.range.len());
            self.payload.extend_from_slice(&shared.body[shared.range]);
        }
    }
}
