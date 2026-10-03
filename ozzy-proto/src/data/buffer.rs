//! Reusable packed records without per-record payload ownership.

use super::{CodecError, DataLimits, Records, add, capacity, count, write_record_entries};
use crate::MessageId;

/// One bounded record table and payload allocation. Records borrow this buffer;
/// no record creates a reference-counted payload owner. No source storage is pinned.
#[derive(Debug)]
pub struct RecordBuffer {
    pub(crate) metadata: Vec<u8>,
    pub(crate) payload: Vec<u8>,
    pub(crate) count: usize,
    pub(crate) parts: usize,
}

impl RecordBuffer {
    /// Reserve both allocations once. Subsequent writes never grow capacity.
    pub fn new(limits: DataLimits) -> Self {
        Self {
            metadata: Vec::with_capacity(limits.envelope.max_metadata_bytes),
            payload: Vec::with_capacity(limits.envelope.max_payload_bytes),
            count: 0,
            parts: 0,
        }
    }

    /// Release contents, preserving both allocations.
    pub fn clear(&mut self) {
        self.metadata.clear();
        self.payload.clear();
        self.count = 0;
        self.parts = 0;
    }

    /// Number of complete records.
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Whether no records have been written, including zero-byte records.
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Selected opaque bytes; empty multipart descriptors still consume metadata.
    pub fn payload_bytes(&self) -> usize {
        self.payload.len()
    }

    /// Borrow the validated record table and exact payload, without allocating.
    pub fn records(&self) -> Records<'_> {
        Records {
            descriptors: &self.metadata,
            payload: &self.payload,
            count: self.count,
            summary: None,
        }
    }

    /// Append one whole record within current count/part/byte credit. Failure
    /// leaves contents and capacities unchanged. Parts must repeat when cloned.
    pub fn push<'a>(
        &mut self,
        id: MessageId,
        encoding: crate::data::Encoding,
        parts: impl Iterator<Item = &'a [u8]> + Clone,
        limits: DataLimits,
    ) -> Result<(), CodecError> {
        if self.count >= limits.max_records {
            return Err(CodecError::Limit);
        }
        let remaining_parts = limits.max_parts.saturating_sub(self.parts);
        let (metadata, payload, part_count, single) = record_size(parts.clone(), remaining_parts)?;
        if payload > limits.max_record_bytes
            || !encoding.validate(part_count, limits.max_parts, limits.max_record_bytes)
        {
            return Err(CodecError::Limit);
        }
        if encoding
            .frame_offset(single, limits.max_parts, limits.max_record_bytes)
            .is_none()
        {
            return Err(CodecError::Length);
        }
        let metadata = add(self.metadata.len(), metadata + encoding.metadata_bytes())?;
        let payload = add(self.payload.len(), payload)?;
        if metadata > limits.envelope.max_metadata_bytes
            || payload > limits.envelope.max_payload_bytes
        {
            return Err(CodecError::Limit);
        }
        capacity(&self.metadata, metadata)?;
        capacity(&self.payload, payload)?;
        if part_count == 1 && encoding == crate::data::Encoding::Raw {
            // The sizing pass already retained this one borrowed part. Pack the
            // complete descriptor once, without another iterator or backpatch.
            let mut descriptor = [0; 24];
            descriptor[..16].copy_from_slice(id.as_bytes());
            descriptor[19] = 1;
            descriptor[20..].copy_from_slice(&(single.len() as u32).to_be_bytes());
            self.metadata.extend_from_slice(&descriptor);
            self.payload.extend_from_slice(single);
        } else {
            write_record_entries(
                std::iter::once((id, encoding, parts)),
                &mut self.metadata,
                Some(&mut self.payload),
            );
        }
        self.count += 1;
        self.parts += part_count;
        Ok(())
    }
}

fn record_size<'a>(
    parts: impl Iterator<Item = &'a [u8]>,
    max_parts: usize,
) -> Result<(usize, usize, usize, &'a [u8]), CodecError> {
    let mut metadata = 20;
    let mut payload = 0;
    let mut part_count = 0;
    let mut single = &[][..];
    for part in parts {
        part_count = add(part_count, 1)?;
        count(part_count)?;
        if part_count > max_parts {
            return Err(CodecError::Limit);
        }
        metadata = add(metadata, 4)?;
        count(part.len())?;
        payload = add(payload, part.len())?;
        single = part;
    }
    if part_count == 0 {
        return Err(CodecError::Parts);
    }
    Ok((metadata, payload, part_count, single))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EnvelopeLimits;

    #[test]
    fn single_part_reply_visits_payload_once() {
        let limits = DataLimits::default();
        let mut buffer = RecordBuffer::new(limits);
        let visits = std::cell::Cell::new(0);
        let parts = [b"payload".as_slice()].into_iter().inspect(|_| {
            visits.set(visits.get() + 1);
        });
        let id = MessageId::new();
        buffer
            .push(id, crate::data::Encoding::Raw, parts, limits)
            .unwrap();
        assert_eq!(visits.get(), 1);
        let record = buffer.records().iter().next().unwrap();
        assert_eq!(record.message_id, id);
        assert_eq!(record.parts.collect::<Vec<_>>(), [b"payload".as_slice()]);
    }

    #[test]
    fn packed_reply_matches_generic_encoder_for_varied_records() {
        let limits = DataLimits::default();
        let mut buffer = RecordBuffer::new(limits);
        let payload = [7; 1024];
        for part_count in [1, 2, 7] {
            for shift in 0..4 {
                buffer.clear();
                let parts = (0..part_count)
                    .map(|index| &payload[..[0, 1, 128, 1024][(index + shift) % 4]])
                    .collect::<Vec<_>>();
                let id = MessageId::new();
                buffer
                    .push(
                        id,
                        crate::data::Encoding::Raw,
                        parts.iter().copied(),
                        limits,
                    )
                    .unwrap();
                let mut metadata = Vec::new();
                let mut encoded_payload = Vec::new();
                write_record_entries(
                    std::iter::once((id, crate::data::Encoding::Raw, parts.iter().copied())),
                    &mut metadata,
                    Some(&mut encoded_payload),
                );
                assert_eq!(buffer.metadata, metadata);
                assert_eq!(buffer.payload, encoded_payload);
                assert_eq!(buffer.parts, part_count);
            }
        }
        buffer.clear();
        assert_eq!(
            buffer.push(
                MessageId::new(),
                crate::data::Encoding::Raw,
                std::iter::empty(),
                limits
            ),
            Err(CodecError::Parts)
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn rejected_reply_keeps_existing_records_and_allocations() {
        let capacity = DataLimits {
            max_record_bytes: 1024 * 1024,
            max_records: 4,
            max_parts: 8,
            envelope: EnvelopeLimits {
                max_metadata_bytes: 128,
                max_payload_bytes: 32,
            },
        };
        for multipart in [false, true] {
            for limit in 0..6 {
                let mut buffer = RecordBuffer::new(capacity);
                buffer
                    .push(
                        MessageId::new(),
                        crate::data::Encoding::Raw,
                        [b"kept".as_slice()].into_iter(),
                        capacity,
                    )
                    .unwrap();
                let original = (
                    buffer.metadata.clone(),
                    buffer.payload.clone(),
                    buffer.count,
                    buffer.parts,
                );
                let pointers = (buffer.metadata.as_ptr(), buffer.payload.as_ptr());
                let mut limits = capacity;
                match limit {
                    0 => limits.max_records = 1,
                    1 => limits.max_parts = 1,
                    2 => limits.envelope.max_metadata_bytes = 24,
                    3 => limits.envelope.max_payload_bytes = 4,
                    // Larger caller limits do not authorize allocation growth.
                    4 => limits.envelope.max_metadata_bytes = 4096,
                    _ => limits.envelope.max_payload_bytes = 4096,
                }
                let wide = vec![b"".as_slice(); 28];
                let large = [0; 64];
                let parts: &[&[u8]] = match limit {
                    4 => &wide,
                    5 => &[large.as_slice()],
                    _ if multipart => &[b"new", b"", b"data"],
                    _ => &[b"new"],
                };
                if limit == 4 {
                    limits.max_parts = 64;
                }
                assert!(
                    buffer
                        .push(
                            MessageId::new(),
                            crate::data::Encoding::Raw,
                            parts.iter().copied(),
                            limits
                        )
                        .is_err()
                );
                assert_eq!(
                    (
                        &buffer.metadata,
                        &buffer.payload,
                        buffer.count,
                        buffer.parts
                    ),
                    (&original.0, &original.1, original.2, original.3)
                );
                assert_eq!(
                    (buffer.metadata.as_ptr(), buffer.payload.as_ptr()),
                    pointers
                );
            }
        }
    }

    #[test]
    fn packed_records_keep_empty_parts_and_reuse_allocations() {
        let limits = DataLimits {
            max_record_bytes: 1024 * 1024,
            max_records: 2,
            max_parts: 4,
            envelope: EnvelopeLimits {
                max_metadata_bytes: 64,
                max_payload_bytes: 8,
            },
        };
        let mut buffer = RecordBuffer::new(limits);
        let pointers = (buffer.metadata.as_ptr(), buffer.payload.as_ptr());
        for _ in 0..3 {
            buffer
                .push(
                    MessageId::new(),
                    crate::data::Encoding::Raw,
                    [b"".as_slice(), b"abc", b""].into_iter(),
                    limits,
                )
                .unwrap();
            buffer
                .push(
                    MessageId::new(),
                    crate::data::Encoding::Raw,
                    [b"de".as_slice()].into_iter(),
                    limits,
                )
                .unwrap();
            assert_eq!(buffer.payload_bytes(), 5);
            assert_eq!(
                buffer
                    .records()
                    .iter()
                    .map(|r| r.parts.collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
                vec![vec![b"".as_slice(), b"abc", b""], vec![b"de".as_slice()]]
            );
            assert!(
                buffer
                    .push(
                        MessageId::new(),
                        crate::data::Encoding::Raw,
                        [b"x".as_slice()].into_iter(),
                        limits
                    )
                    .is_err()
            );
            assert_eq!(buffer.len(), 2);
            assert_eq!(buffer.payload_bytes(), 5);
            buffer.clear();
            assert_eq!(
                pointers,
                (buffer.metadata.as_ptr(), buffer.payload.as_ptr())
            );
        }
    }
}
