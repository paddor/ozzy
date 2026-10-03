//! Group-owned records. Tiny single-part payloads need no per-record owners.

use bytes::Bytes;
use smallvec::SmallVec;

use super::OwnedRecord;
use crate::MessageId;

/// Mutable packed single-part records. Lengths cover 0 through 255 bytes.
/// Every 32 records share one payload-position checkpoint for bounded seeking.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TinyRecords {
    ids: Vec<MessageId>,
    lengths: Vec<u8>,
    payload: Vec<u8>,
    checkpoints: Vec<usize>,
}

impl TinyRecords {
    /// Pack a validated raw, single-part collection with one contiguous payload
    /// copy. Other record shapes return `None` without allocating.
    pub fn from_records(records: super::Records<'_>) -> Option<Self> {
        let (descriptors, remainder) = records.descriptors.as_chunks::<24>();
        if !remainder.is_empty()
            || descriptors.len() != records.count
            || descriptors
                .iter()
                .any(|entry| entry[16..23] != [0, 0, 0, 1, 0, 0, 0])
        {
            return None;
        }
        let mut packed = Self {
            ids: Vec::with_capacity(records.count),
            lengths: Vec::with_capacity(records.count),
            payload: records.payload.to_vec(),
            checkpoints: Vec::with_capacity(records.count.div_ceil(32)),
        };
        let mut offset = 0;
        for (index, entry) in descriptors.iter().enumerate() {
            if index.is_multiple_of(32) {
                packed.checkpoints.push(offset);
            }
            packed
                .ids
                .push(MessageId::from_bytes(entry[..16].try_into().unwrap()));
            packed.lengths.push(entry[23]);
            offset += usize::from(entry[23]);
        }
        debug_assert_eq!(offset, packed.payload.len());
        Some(packed)
    }

    /// Empty group with reserved record and payload capacity.
    pub fn with_capacity(records: usize, bytes: usize) -> Self {
        Self {
            ids: Vec::with_capacity(records),
            lengths: Vec::with_capacity(records),
            payload: Vec::with_capacity(bytes),
            checkpoints: Vec::with_capacity(records.div_ceil(32)),
        }
    }

    /// Append an exact ID and payload. Reject oversize input without mutation.
    pub fn push(&mut self, id: MessageId, payload: &[u8]) -> bool {
        let Ok(len) = u8::try_from(payload.len()) else {
            return false;
        };
        if self.ids.len().is_multiple_of(32) {
            self.checkpoints.push(self.payload.len());
        }
        self.ids.push(id);
        self.lengths.push(len);
        self.payload.extend_from_slice(payload);
        true
    }

    /// Number of packed records.
    pub fn len(&self) -> usize {
        self.ids.len()
    }
    /// Whether no records have been appended.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
    /// Release logical contents while keeping capacities.
    pub fn clear(&mut self) {
        self.ids.clear();
        self.lengths.clear();
        self.payload.clear();
        self.checkpoints.clear();
    }
    /// Borrow a record, scanning at most 31 preceding length bytes.
    pub fn get(&self, index: usize) -> Option<BatchRecord<'_>> {
        let id = *self.ids.get(index)?;
        let block = index / 32;
        let start = self.checkpoints[block]
            + self.lengths[block * 32..index]
                .iter()
                .map(|&n| n as usize)
                .sum::<usize>();
        Some(BatchRecord {
            encoding: super::Encoding::Raw,
            message_id: id,
            payload: PayloadRef::Single(&self.payload[start..start + self.lengths[index] as usize]),
        })
    }
    /// Contiguous record IDs in arrival order.
    pub fn ids(&self) -> &[MessageId] {
        &self.ids
    }
    /// One byte per payload length, including zero for an empty part.
    pub fn lengths(&self) -> &[u8] {
        &self.lengths
    }
    /// Concatenated payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Borrow a contiguous record range without walking each record.
    pub fn payload_range(&self, range: std::ops::Range<usize>) -> Option<&[u8]> {
        if range.start > range.end || range.end > self.len() {
            return None;
        }
        let offset = |index: usize| {
            if index == self.len() {
                self.payload.len()
            } else {
                let block = index / 32;
                self.checkpoints[block]
                    + self.lengths[block * 32..index]
                        .iter()
                        .map(|&n| n as usize)
                        .sum::<usize>()
            }
        };
        Some(&self.payload[offset(range.start)..offset(range.end)])
    }
}

/// Payloads borrowed from either ordinary records or a packed group.
#[derive(Debug, Clone, Copy)]
pub enum PayloadRef<'a> {
    /// Existing multipart owners.
    Parts(&'a [Bytes]),
    /// One part inside a group-owned arena.
    Single(&'a [u8]),
    /// Descriptors and payload borrowed directly from a received frame.
    Encoded(super::EncodedParts<'a>),
}
impl<'a> PayloadRef<'a> {
    /// Opaque payload bytes. Encoded groups use their existing range bounds.
    pub fn payload_bytes(self) -> usize {
        match self {
            Self::Parts(p) => p.iter().map(Bytes::len).sum(),
            Self::Single(p) => p.len(),
            Self::Encoded(p) => p.payload_bytes(),
        }
    }
    /// Exact part count; empty payloads still count as one part.
    pub fn len(self) -> usize {
        match self {
            Self::Parts(p) => p.len(),
            Self::Single(_) => 1,
            Self::Encoded(p) => p.len(),
        }
    }
    /// Whether the record has no parts.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
    /// Borrow each part without constructing shared owners.
    pub fn iter(self) -> impl ExactSizeIterator<Item = &'a [u8]> + DoubleEndedIterator + Clone {
        match self {
            Self::Parts(p) => PayloadIter::Parts(p.iter()),
            Self::Single(p) => PayloadIter::Single(Some(p)),
            Self::Encoded(p) => PayloadIter::Encoded(p.iter()),
        }
    }
    /// Materialize at an API that requires independently owned records.
    pub fn to_owned(self) -> SmallVec<[Bytes; 2]> {
        match self {
            Self::Parts(p) => p.iter().cloned().collect(),
            Self::Single(p) => smallvec::smallvec![Bytes::copy_from_slice(p)],
            Self::Encoded(p) => p.iter().map(Bytes::copy_from_slice).collect(),
        }
    }
}
impl PartialEq for PayloadRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}
impl Eq for PayloadRef<'_> {}

#[derive(Clone)]
enum PayloadIter<'a> {
    Parts(std::slice::Iter<'a, Bytes>),
    Single(Option<&'a [u8]>),
    Encoded(super::Parts<'a>),
}

impl<'a> Iterator for PayloadIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Parts(p) => p.next().map(Bytes::as_ref),
            Self::Single(p) => p.take(),
            Self::Encoded(p) => p.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = match self {
            Self::Parts(p) => p.len(),
            Self::Single(p) => usize::from(p.is_some()),
            Self::Encoded(p) => p.len(),
        };
        (n, Some(n))
    }
}
impl DoubleEndedIterator for PayloadIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Parts(p) => p.next_back().map(Bytes::as_ref),
            Self::Single(p) => p.take(),
            Self::Encoded(p) => p.next_back(),
        }
    }
}
impl ExactSizeIterator for PayloadIter<'_> {}

/// One borrowed record with no reference count or allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchRecord<'a> {
    /// Payload representation.
    pub encoding: super::Encoding,
    /// Exact application record identity.
    pub message_id: MessageId,
    /// Opaque parts, preserving empty boundaries.
    pub payload: PayloadRef<'a>,
}
impl BatchRecord<'_> {
    /// Materialize only at an independently owned record API boundary.
    pub fn to_owned(self) -> OwnedRecord {
        OwnedRecord {
            encoding: self.encoding,
            message_id: self.message_id,
            payload: self.payload.to_owned(),
        }
    }
}

/// Records owned once per append, with a compact path for tiny payloads.
#[derive(Debug, Clone)]
pub enum RecordBatch {
    /// General multipart records, retaining their original payload owners.
    General(Vec<OwnedRecord>),
    /// Packed single-part records.
    Tiny(TinyRecords),
    /// Indexed received frames, shared once for the complete batch.
    Encoded(super::IndexedRecords),
}
impl Default for RecordBatch {
    fn default() -> Self {
        Self::General(Vec::new())
    }
}
impl From<Vec<OwnedRecord>> for RecordBatch {
    fn from(records: Vec<OwnedRecord>) -> Self {
        Self::General(records)
    }
}
impl RecordBatch {
    /// General records when this group is not packed; never materializes a table.
    pub fn general(&self) -> Option<&[OwnedRecord]> {
        match self {
            Self::General(r) => Some(r),
            Self::Tiny(_) | Self::Encoded(_) => None,
        }
    }

    /// Number of records.
    pub fn len(&self) -> usize {
        match self {
            Self::General(r) => r.len(),
            Self::Tiny(r) => r.len(),
            Self::Encoded(r) => r.len(),
        }
    }
    /// Whether this group is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Borrow one record without materializing a payload owner.
    pub fn get(&self, index: usize) -> Option<BatchRecord<'_>> {
        match self {
            Self::General(r) => r.get(index).map(|r| BatchRecord {
                encoding: r.encoding,
                message_id: r.message_id,
                payload: PayloadRef::Parts(&r.payload),
            }),
            Self::Tiny(r) => r.get(index),
            Self::Encoded(r) => r.get(index),
        }
    }
    /// Borrow records in order. The packed cursor advances payload positions once.
    pub fn iter(&self) -> BatchIter<'_> {
        BatchIter {
            records: self,
            front: 0,
            back: self.len(),
            front_byte: 0,
            back_byte: match self {
                Self::Tiny(r) => r.payload.len(),
                Self::General(_) | Self::Encoded(_) => 0,
            },
        }
    }
    /// Consume general records unchanged, expanding packed records only on fallback.
    pub fn into_owned(self) -> Vec<OwnedRecord> {
        match self {
            Self::General(r) => r,
            Self::Tiny(_) => self.iter().map(BatchRecord::to_owned).collect(),
            Self::Encoded(r) => r.into_owned(),
        }
    }
}
impl PartialEq for RecordBatch {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}
impl Eq for RecordBatch {}
impl<'a> IntoIterator for &'a RecordBatch {
    type Item = BatchRecord<'a>;
    type IntoIter = BatchIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Linear, allocation-free group cursor.
#[derive(Debug, Clone)]
pub struct BatchIter<'a> {
    records: &'a RecordBatch,
    front: usize,
    back: usize,
    front_byte: usize,
    back_byte: usize,
}
impl<'a> Iterator for BatchIter<'a> {
    type Item = BatchRecord<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        let index = self.front;
        self.front += 1;
        match self.records {
            RecordBatch::General(_) | RecordBatch::Encoded(_) => self.records.get(index),
            RecordBatch::Tiny(r) => {
                let start = self.front_byte;
                self.front_byte += r.lengths[index] as usize;
                Some(BatchRecord {
                    encoding: super::Encoding::Raw,
                    message_id: r.ids[index],
                    payload: PayloadRef::Single(&r.payload[start..self.front_byte]),
                })
            }
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.back - self.front;
        (n, Some(n))
    }
}
impl DoubleEndedIterator for BatchIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        match self.records {
            RecordBatch::General(_) | RecordBatch::Encoded(_) => self.records.get(self.back),
            RecordBatch::Tiny(r) => {
                let end = self.back_byte;
                self.back_byte -= r.lengths[self.back] as usize;
                Some(BatchRecord {
                    encoding: super::Encoding::Raw,
                    message_id: r.ids[self.back],
                    payload: PayloadRef::Single(&r.payload[self.back_byte..end]),
                })
            }
        }
    }
}
impl ExactSizeIterator for BatchIter<'_> {}
impl std::iter::FusedIterator for BatchIter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_packing_preserves_seek_boundaries_and_rejects_other_shapes() {
        use crate::data::{DataLimits, Encoding, Record, decode_records, encode_records};

        let payloads: Vec<_> = (0..257)
            .map(|index| vec![index as u8; [0, 1, 16, 127, 128, 254, 255][index % 7]])
            .collect();
        let parts: Vec<_> = payloads.iter().map(|p| [p.as_slice()]).collect();
        let mut records: Vec<_> = parts
            .iter()
            .enumerate()
            .map(|(index, parts)| Record {
                encoding: Encoding::Raw,
                message_id: MessageId::from_bytes((index as u128 + 1).to_le_bytes()),
                parts,
            })
            .collect();
        let mut metadata = Vec::new();
        let mut payload = Vec::new();
        encode_records(&records, &mut metadata, Some(&mut payload));
        let decoded = decode_records(&metadata, &payload, 0, DataLimits::default()).unwrap();
        let packed = TinyRecords::from_records(decoded).unwrap();
        for (index, expected) in records.iter().enumerate() {
            let record = packed.get(index).unwrap();
            assert_eq!(record.message_id, expected.message_id);
            assert!(record.payload.iter().eq(expected.parts.iter().copied()));
        }
        for parts in [&[b"", b"".as_slice()][..], &[&[7; 256][..]][..]] {
            records[128].parts = parts;
            metadata.clear();
            payload.clear();
            encode_records(&records, &mut metadata, Some(&mut payload));
            let decoded = decode_records(&metadata, &payload, 0, DataLimits::default()).unwrap();
            assert!(TinyRecords::from_records(decoded).is_none());
        }
    }

    #[test]
    fn packed_seek_iteration_and_fallback_preserve_identity_and_empty_payloads() {
        let mut packed = TinyRecords::default();
        let mut general = Vec::new();
        for index in 0..257_u128 {
            let id = MessageId::from_bytes((index + 1).to_le_bytes());
            let payload = vec![index as u8; [0, 1, 16, 127, 128, 254, 255][index as usize % 7]];
            assert!(packed.push(id, &payload));
            general.push(OwnedRecord {
                encoding: crate::data::Encoding::Raw,
                message_id: id,
                payload: smallvec::smallvec![Bytes::from(payload)],
            });
        }
        let before = packed.clone();
        assert!(!packed.push(MessageId::from_bytes([9; 16]), &[7; 256]));
        assert_eq!(packed, before);
        for start in [0, 1, 31, 32, 33, 255, 257] {
            for end in start..=257 {
                let expected: Vec<u8> = general[start..end]
                    .iter()
                    .flat_map(|r| r.payload[0].iter().copied())
                    .collect();
                assert_eq!(packed.payload_range(start..end), Some(expected.as_slice()));
            }
        }
        assert_eq!(packed.payload_range(258..258), None);
        let packed = RecordBatch::Tiny(packed);
        let general = RecordBatch::General(general);
        assert_eq!(packed, general);
        for index in 0..257 {
            assert_eq!(packed.get(index), general.get(index));
        }
        assert_eq!(packed.get(257), None);
        assert!(packed.iter().rev().eq(general.iter().rev()));
        let mut a = packed.iter();
        let mut b = general.iter();
        while a.len() != 0 {
            assert_eq!(a.next(), b.next());
            assert_eq!(a.next_back(), b.next_back());
        }
        assert_eq!(a.next(), None);
        assert_eq!(a.next_back(), None);
        assert_eq!(packed.into_owned(), general.into_owned());
    }
}
