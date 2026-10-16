//! Incremental canonical-reader output, independent of stored batch boundaries.

use super::{
    CodecError, DataLimits, ENVELOPE_BYTES, Envelope, PublicationHeader, RecordHeader, data,
};

use crate::layout;

mod shared;
pub use shared::PreparedBlock;

/// Encode available records into one bounded reader message. This controls the
/// application record table, not OMQ transport framing or write coalescing.
#[derive(Debug)]
pub struct RecordsEncoder<'a> {
    source: super::Source,
    first_offset: u64,
    envelope: Envelope,
    metadata: &'a mut Vec<u8>,
    payload: &'a mut Vec<u8>,
    limits: DataLimits,
    records: usize,
    parts: usize,
    decoded_payload_bytes: usize,
    encoding_at: usize,
    count_at: usize,
    shared_enabled: bool,
    shared: Option<shared::Payload>,
}

impl<'a> RecordsEncoder<'a> {
    /// Begin a response using caller-reserved storage. No allocation growth.
    pub fn new(
        envelope: Envelope,
        header: RecordHeader,
        metadata: &'a mut Vec<u8>,
        payload: &'a mut Vec<u8>,
        limits: DataLimits,
    ) -> Result<Self, CodecError> {
        super::command(envelope, super::Opcode::Records)?;
        header.subscription.validate()?;
        header.source.validate()?;
        let count_at = data::add(45, header.source.size())?;
        let header_bytes = data::add(count_at, 4)?;
        envelope.validate_frames(header_bytes, 0, limits.envelope)?;
        data::capacity(metadata, header_bytes)?;
        metadata.clear();
        header.subscription.encode(metadata);
        Ok(Self::begin(
            envelope,
            header.source,
            header.first_offset,
            (metadata, payload),
            limits,
        ))
    }

    /// Begin one message shared by every live reader of a group partition. It
    /// names no link, request, or subscription; receivers fence it by source.
    pub fn publication(
        envelope: Envelope,
        header: PublicationHeader,
        metadata: &'a mut Vec<u8>,
        payload: &'a mut Vec<u8>,
        limits: DataLimits,
    ) -> Result<Self, CodecError> {
        super::command(envelope, super::Opcode::RecordsPub)?;
        super::publication_topic(header.source)?;
        let count_at = data::add(13, header.source.size())?;
        let header_bytes = data::add(count_at, 4)?;
        envelope.validate_frames(header_bytes, 0, limits.envelope)?;
        data::capacity(metadata, header_bytes)?;
        metadata.clear();
        Ok(Self::begin(
            envelope,
            header.source,
            header.first_offset,
            (metadata, payload),
            limits,
        ))
    }

    /// Append the fields common to both messages after any subscription identity.
    fn begin(
        envelope: Envelope,
        source: super::Source,
        first_offset: u64,
        (metadata, payload): (&'a mut Vec<u8>, &'a mut Vec<u8>),
        limits: DataLimits,
    ) -> Self {
        payload.clear();
        source.encode(metadata);
        metadata.extend_from_slice(&first_offset.to_be_bytes());
        let encoding_at = metadata.len();
        metadata.push(crate::append::PayloadEncoding::Raw as u8);
        metadata.extend_from_slice(&[0; 4]);
        let count_at = metadata.len();
        metadata.extend_from_slice(&[0; 4]);
        Self {
            source,
            first_offset,
            envelope,
            metadata,
            payload,
            limits,
            records: 0,
            parts: 0,
            decoded_payload_bytes: 0,
            encoding_at,
            count_at,
            shared_enabled: false,
            shared: None,
        }
    }

    /// The log whose records this message carries.
    pub const fn source(&self) -> super::Source {
        self.source
    }

    /// The bounds this message was started with.
    pub const fn limits(&self) -> DataLimits {
        self.limits
    }

    /// Remaining record, part and payload capacity for a storage read.
    pub fn remaining(&self) -> DataLimits {
        let encoded = self.has_encoded_payload();
        DataLimits {
            max_record_bytes: self.limits.max_record_bytes,
            max_records: if encoded {
                0
            } else {
                self.limits.max_records.saturating_sub(self.records)
            },
            max_parts: if encoded {
                0
            } else {
                self.limits.max_parts.saturating_sub(self.parts)
            },
            envelope: super::EnvelopeLimits {
                max_metadata_bytes: self
                    .limits
                    .envelope
                    .max_metadata_bytes
                    .saturating_sub(self.metadata.len()),
                max_payload_bytes: if encoded {
                    0
                } else {
                    self.limits
                        .envelope
                        .max_payload_bytes
                        .saturating_sub(self.payload_bytes())
                },
            },
        }
    }

    /// Number of records already encoded.
    pub const fn len(&self) -> usize {
        self.records
    }

    /// Whether a nonempty response is available.
    pub const fn is_empty(&self) -> bool {
        self.records == 0
    }

    /// Decoded record bytes charged against local output limits.
    pub const fn decoded_payload_bytes(&self) -> usize {
        self.decoded_payload_bytes
    }

    /// Append one raw, single-part record. Check all bounds before copying its
    /// descriptor and payload; failure preserves the previous response.
    pub fn push_raw(&mut self, id: crate::MessageId, payload: &[u8]) -> Result<(), CodecError> {
        if self.has_encoded_payload() {
            return Err(CodecError::Limit);
        }
        self.materialize()?;
        let records = data::add(self.records, 1)?;
        data::record_count(records, self.first_offset, self.limits)?;
        let parts = data::add(self.parts, 1)?;
        if parts > self.limits.max_parts || payload.len() > self.limits.max_record_bytes {
            return Err(CodecError::Limit);
        }
        let length = data::count(payload.len())?;
        frame_capacity(
            self.metadata,
            data::add(self.metadata.len(), 24)?,
            self.limits.envelope.max_metadata_bytes,
        )?;
        frame_capacity(
            self.payload,
            data::add(self.payload.len(), payload.len())?,
            self.limits.envelope.max_payload_bytes,
        )?;
        let mut descriptor = [0; 24];
        descriptor[..16].copy_from_slice(id.as_bytes());
        descriptor[19] = 1;
        descriptor[20..].copy_from_slice(&length.to_be_bytes());
        self.metadata.extend_from_slice(&descriptor);
        self.payload.extend_from_slice(payload);
        self.records = records;
        self.parts = parts;
        layout::patch(self.metadata, self.count_at, (records as u32).to_be_bytes())?;
        self.set_raw_payload_bytes()?;
        Ok(())
    }

    /// Append an encoded descriptor table (without a collection count) and its
    /// exact payload. Validate every descriptor and all destination limits before
    /// copying either span. Failure leaves output and capacity unchanged.
    pub fn extend_packed(
        &mut self,
        descriptors: &[u8],
        payload: &[u8],
        count: usize,
    ) -> Result<(), CodecError> {
        if self.has_encoded_payload() {
            return Err(CodecError::Limit);
        }
        self.materialize()?;
        let first = self
            .first_offset
            .checked_add(self.records as u64)
            .ok_or(CodecError::Length)?;
        let records = data::add(self.records, count)?;
        data::count(records)?;
        let size = data::add(self.metadata.len(), descriptors.len())?;
        let bytes = data::add(self.payload.len(), payload.len())?;
        self.envelope
            .validate_frames(size, bytes, self.limits.envelope)?;
        data::capacity(self.metadata, size)?;
        data::capacity(self.payload, bytes)?;
        let parts =
            data::validate_record_entries(descriptors, payload, count, first, self.remaining())?.0;
        let parts = data::add(self.parts, parts)?;
        self.metadata.extend_from_slice(descriptors);
        self.payload.extend_from_slice(payload);
        self.records = records;
        self.parts = parts;
        layout::patch(self.metadata, self.count_at, (records as u32).to_be_bytes())?;
        self.set_raw_payload_bytes()?;
        Ok(())
    }

    /// Validate and append a chunk in one pass, without allocation growth.
    /// On failure, this chunk leaves the output unchanged.
    pub fn extend<'b, I, P>(&mut self, records: I) -> Result<(), CodecError>
    where
        I: ExactSizeIterator<Item = (crate::MessageId, data::Encoding, P)>,
        P: Iterator<Item = &'b [u8]>,
    {
        if self.has_encoded_payload() {
            return Err(CodecError::Limit);
        }
        self.materialize()?;
        let remaining = self.remaining();
        let first = self
            .first_offset
            .checked_add(self.records as u64)
            .ok_or(CodecError::Length)?;
        let count = records.len();
        data::record_count(count, first, remaining)?;
        let total = data::add(self.records, count)?;
        data::count(total)?;
        let before = (self.metadata.len(), self.payload.len());
        let written = self
            .write_entries(records)
            .and_then(|parts| data::add(self.parts, parts));
        let parts = match written {
            Ok(parts) => parts,
            Err(error) => {
                self.metadata.truncate(before.0);
                self.payload.truncate(before.1);
                return Err(error);
            }
        };
        self.records = total;
        self.parts = parts;
        layout::patch(self.metadata, self.count_at, (total as u32).to_be_bytes())?;
        self.set_raw_payload_bytes()?;
        Ok(())
    }

    fn write_entries<'b, P>(
        &mut self,
        records: impl Iterator<Item = (crate::MessageId, data::Encoding, P)>,
    ) -> Result<usize, CodecError>
    where
        P: Iterator<Item = &'b [u8]>,
    {
        let mut total_parts = 0;
        for (id, encoding, parts) in records {
            let count_at = self.begin_entry(id, encoding)?;
            let mut record_bytes = 0_usize;
            let mut count = 0_u32;
            for part in parts {
                if encoding
                    .frame_offset(part, self.limits.max_parts, self.limits.max_record_bytes)
                    .is_none()
                {
                    return Err(CodecError::Length);
                }
                total_parts = data::add(total_parts, 1)?;
                data::count(total_parts)?;
                if data::add(self.parts, total_parts)? > self.limits.max_parts {
                    return Err(CodecError::Limit);
                }
                self.push_part(part, &mut record_bytes)?;
                count = count.checked_add(1).ok_or(CodecError::Length)?;
            }
            if !encoding.validate(
                count as usize,
                self.limits.max_parts,
                self.limits.max_record_bytes,
            ) {
                return Err(CodecError::Parts);
            }
            layout::patch(
                self.metadata,
                count_at,
                (count | encoding.tag()).to_be_bytes(),
            )?;
        }
        Ok(total_parts)
    }

    /// Write one descriptor head within the frame limit. Returns where its
    /// part count goes once the parts are known.
    fn begin_entry(
        &mut self,
        id: crate::MessageId,
        encoding: data::Encoding,
    ) -> Result<usize, CodecError> {
        let header_end = data::add(self.metadata.len(), encoding.head_bytes())?;
        frame_capacity(
            self.metadata,
            header_end,
            self.limits.envelope.max_metadata_bytes,
        )?;
        self.metadata.extend_from_slice(id.as_bytes());
        let count_at = self.metadata.len();
        self.metadata.extend_from_slice(&[0; 4]);
        if let data::Encoding::Lz4 { decoded_bytes } = encoding {
            self.metadata
                .extend_from_slice(&decoded_bytes.to_be_bytes());
        }
        Ok(count_at)
    }

    /// Append one part's length and bytes, charging `record_bytes` against
    /// the record limit and both frames against theirs.
    fn push_part(&mut self, part: &[u8], record_bytes: &mut usize) -> Result<(), CodecError> {
        let part_bytes = data::count(part.len())?;
        let metadata_end = data::add(self.metadata.len(), 4)?;
        let payload_end = data::add(self.payload.len(), part.len())?;
        *record_bytes = data::add(*record_bytes, part.len())?;
        if *record_bytes > self.limits.max_record_bytes {
            return Err(CodecError::Limit);
        }
        frame_capacity(
            self.metadata,
            metadata_end,
            self.limits.envelope.max_metadata_bytes,
        )?;
        frame_capacity(
            self.payload,
            payload_end,
            self.limits.envelope.max_payload_bytes,
        )?;
        self.metadata.extend_from_slice(&part_bytes.to_be_bytes());
        self.payload.extend_from_slice(part);
        Ok(())
    }

    /// Adopt a complete packed batch into an empty response. Validate all bounds
    /// before swapping equal-capacity payload allocations; no payload copy or
    /// allocation growth. The emptied batch receives the previous output buffer.
    pub fn take_buffer(&mut self, buffer: &mut data::RecordBuffer) -> Result<(), CodecError> {
        if !self.is_empty() || buffer.is_empty() {
            return Err(CodecError::Limit);
        }
        // RecordBuffer's private table was validated on insertion. Only the
        // destination's aggregate limits and offset range need checking here.
        data::record_count(buffer.count, self.first_offset, self.limits)?;
        if buffer.parts > self.limits.max_parts {
            return Err(CodecError::Limit);
        }
        let payload = buffer.payload.len();
        let size = data::add(self.metadata.len(), buffer.metadata.len())?;
        self.envelope
            .validate_frames(size, payload, self.limits.envelope)?;
        data::capacity(self.metadata, size)?;
        // Swapping must not grow the transport slot's backing-memory bound.
        if buffer.payload.capacity() != self.payload.capacity() {
            return Err(CodecError::Capacity);
        }
        self.metadata.extend_from_slice(&buffer.metadata);
        std::mem::swap(self.payload, &mut buffer.payload);
        self.records = buffer.count;
        self.parts = buffer.parts;
        layout::patch(
            self.metadata,
            self.count_at,
            (self.records as u32).to_be_bytes(),
        )?;
        self.set_raw_payload_bytes()?;
        buffer.clear();
        Ok(())
    }

    /// Finish a nonempty response and return its envelope.
    pub fn finish(self) -> Result<[u8; ENVELOPE_BYTES], CodecError> {
        if self.shared.is_some() {
            return Err(CodecError::Parts);
        }
        self.finish_with_payload().map(|(header, _)| header)
    }

    /// Finish and return any shared payload owner. Otherwise the caller's
    /// original payload buffer holds the encoded bytes.
    pub fn finish_with_payload(
        self,
    ) -> Result<([u8; ENVELOPE_BYTES], Option<bytes::Bytes>), CodecError> {
        if self.is_empty() {
            return Err(CodecError::Limit);
        }
        let header = self.envelope.encode_header(
            self.metadata.len(),
            self.payload_bytes(),
            self.limits.envelope,
        )?;
        Ok((header, self.shared.map(shared::Payload::finish)))
    }

    fn set_raw_payload_bytes(&mut self) -> Result<(), CodecError> {
        let payload_bytes = self.payload_bytes();
        self.set_payload_encoding(crate::append::PayloadEncoding::Raw, payload_bytes)?;
        self.decoded_payload_bytes = payload_bytes;
        Ok(())
    }

    fn set_payload_encoding(
        &mut self,
        encoding: crate::append::PayloadEncoding,
        decoded_bytes: usize,
    ) -> Result<(), CodecError> {
        let [a, b, c, d] = (decoded_bytes as u32).to_be_bytes();
        layout::patch(
            self.metadata,
            self.encoding_at,
            [encoding as u8, a, b, c, d],
        )?;
        Ok(())
    }

    fn has_encoded_payload(&self) -> bool {
        self.metadata
            .get(self.encoding_at)
            .is_some_and(|&tag| tag != crate::append::PayloadEncoding::Raw as u8)
    }
}

/// Check before each write so even a late error cannot grow either allocation.
fn frame_capacity(buffer: &Vec<u8>, end: usize, limit: usize) -> Result<(), CodecError> {
    if end > limit {
        return Err(crate::EnvelopeError::Limit.into());
    }
    u32::try_from(end).map_err(|_| crate::EnvelopeError::Length)?;
    data::capacity(buffer, end)
}
