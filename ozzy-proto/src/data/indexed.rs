//! Shared received frames with bounded, constant-time record-range selection.

use std::ops::Range;

use super::{BatchRecord, OwnedRecords, Parts, PayloadRef, Records};

/// Immutable received records. One position table replaces per-record payload
/// owners; ranges retain their original descriptor and payload layout.
#[derive(Debug, Clone)]
pub struct IndexedRecords {
    records: OwnedRecords,
    positions: Vec<(usize, usize)>,
}

impl IndexedRecords {
    /// Index already validated frames. Neither frame is copied.
    pub fn new(records: OwnedRecords) -> Self {
        let view = records.as_records();
        let mut cursor = view.iter();
        let mut positions = Vec::with_capacity(view.len() + 1);
        positions.push((0, 0));
        while cursor.next().is_some() {
            positions.push((
                view.descriptor_bytes() - cursor.descriptors.0.len(),
                view.payload_bytes() - cursor.payload.len(),
            ));
        }
        Self { records, positions }
    }

    /// Number of indexed records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether this frame collection has no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Borrow a validated range without scanning earlier records.
    pub fn range(&self, range: Range<usize>) -> Option<Records<'_>> {
        if range.start > range.end || range.end > self.len() {
            return None;
        }
        let (descriptors, payload) = self.records.as_records().encoded();
        let start = self.positions[range.start];
        let end = self.positions[range.end];
        Some(Records {
            descriptors: &descriptors[start.0..end.0],
            payload: &payload[start.1..end.1],
            count: range.len(),
            summary: None,
        })
    }

    pub(super) fn get(&self, index: usize) -> Option<BatchRecord<'_>> {
        let (metadata, payload) = self.range(index..index.checked_add(1)?)?.encoded();
        let mut cursor = super::Cursor(metadata);
        let message_id = crate::MessageId::from_bytes(cursor.array().expect("validated ID"));
        let (_, encoding) = super::descriptor(&mut cursor).expect("validated descriptor");
        Some(BatchRecord {
            message_id,
            encoding,
            payload: PayloadRef::Encoded(EncodedParts {
                lengths: cursor.0.as_chunks::<4>().0,
                payload,
            }),
        })
    }

    pub(super) fn into_owned(self) -> Vec<super::OwnedRecord> {
        self.records.into_records().collect()
    }
}

/// Multipart view into a validated frame. No per-part owner or allocation.
#[derive(Debug, Clone, Copy)]
pub struct EncodedParts<'a> {
    lengths: &'a [[u8; 4]],
    payload: &'a [u8],
}

impl<'a> EncodedParts<'a> {
    /// Total payload bytes, already established by the position table.
    pub fn payload_bytes(self) -> usize {
        self.payload.len()
    }
    /// Number of parts, including empty parts.
    pub fn len(self) -> usize {
        self.lengths.len()
    }

    /// Whether there are no parts.
    pub fn is_empty(self) -> bool {
        self.lengths.is_empty()
    }

    /// Walk parts in either direction without rescanning lengths.
    pub fn iter(self) -> Parts<'a> {
        Parts {
            lengths: self.lengths.iter(),
            payload: self.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MessageId;
    use crate::data::{DataLimits, Encoding, Record, RecordBatch, decode_records, encode_records};
    use bytes::Bytes;

    #[test]
    fn indexed_ranges_share_frames_and_preserve_bidirectional_multipart_iteration() {
        let large = vec![7; 1024];
        let encoded = [0, 0, 0, 1, 0, 0, 4, 0, 0xff];
        let shapes: &[&[&[u8]]] = &[&[b""], &[b"small"], &[b"", &large, b""], &[&encoded]];
        let records: Vec<_> = (0..97)
            .map(|i| Record {
                message_id: MessageId::from_bytes([i + 1; 16]),
                encoding: if i % 4 == 3 {
                    Encoding::Lz4 {
                        decoded_bytes: 1024,
                    }
                } else {
                    Encoding::Raw
                },
                parts: shapes[i as usize % 4],
            })
            .collect();
        let mut metadata = Vec::new();
        let mut payload = Vec::new();
        encode_records(&records, &mut metadata, Some(&mut payload));
        let metadata = Bytes::from(metadata);
        let payload = Bytes::from(payload);
        let decoded = decode_records(&metadata, &payload, 0, DataLimits::default()).unwrap();
        let owned = decoded.to_owned(&metadata, &payload);
        let expected = RecordBatch::General(owned.clone().into_records().collect());
        let indexed = IndexedRecords::new(owned);
        let full = indexed.range(0..97).unwrap().encoded();
        assert_eq!(full.0.as_ptr(), metadata[4..].as_ptr());
        assert_eq!(full.1.as_ptr(), payload.as_ptr());
        for start in 0..=97 {
            for end in start..=97 {
                let slice = indexed.range(start..end).unwrap();
                assert_eq!(slice.len(), end - start);
                for (record, expected) in slice.iter().zip(&records[start..end]) {
                    assert_eq!(record.message_id, expected.message_id);
                    assert_eq!(record.encoding, expected.encoding);
                    assert!(record.parts.clone().eq(expected.parts.iter().copied()));
                    assert!(record.parts.rev().eq(expected.parts.iter().copied().rev()));
                }
            }
        }
        assert!(indexed.range(98..98).is_none());
        assert!(indexed.range(Range { start: 1, end: 0 }).is_none());
        let batch = RecordBatch::Encoded(indexed);
        assert_eq!(batch, expected);
        assert!(batch.iter().rev().eq(expected.iter().rev()));
        let mut a = batch.iter();
        let mut b = expected.iter();
        while a.len() != 0 {
            assert_eq!(a.next(), b.next());
            assert_eq!(a.next_back(), b.next_back());
        }
        assert_eq!(a.next(), None);
        assert_eq!(a.next_back(), None);
        drop((metadata, payload));
        assert_eq!(batch.clone().into_owned(), expected.into_owned());
        assert!(batch.get(97).is_none());
        assert!(batch.get(usize::MAX).is_none());
    }
}
