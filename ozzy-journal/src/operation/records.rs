//! Borrow record descriptors directly from admitted native batches.

use bytes::Bytes;
use ozzy_proto::MessageId;
use ozzy_proto::data::{BatchIter, OwnedRecord, PayloadRef, RecordBatch};
use smallvec::SmallVec;
use std::ops::Range;

use super::{AppendRecord, OperationCodecError};

/// Canonical append records, either described individually or borrowed from
/// native input slices. Both representations have identical canonical bytes.
#[derive(Debug, Clone)]
pub struct AppendRecordList<'a>(pub(super) Storage<'a>);

#[derive(Debug, Clone)]
pub(super) enum Storage<'a> {
    Batches {
        slices: SmallVec<[BatchSlice<'a>; 2]>,
        len: usize,
    },
    Described(Vec<AppendRecord<'a>>),
    Borrowed {
        slices: SmallVec<[&'a [OwnedRecord]; 2]>,
        len: usize,
    },
    Owned(Vec<OwnedRecord>),
}

#[derive(Debug, Clone)]
pub(super) struct BatchSlice<'a> {
    batch: &'a RecordBatch,
    range: Range<usize>,
}
type SliceIter<'a> = std::iter::Take<std::iter::Skip<BatchIter<'a>>>;
impl<'a> BatchSlice<'a> {
    fn iter(&self) -> SliceIter<'a> {
        self.batch
            .iter()
            .skip(self.range.start)
            .take(self.range.len())
    }
}
impl<'a> AppendRecordList<'a> {
    /// Original descriptor and payload runs when every input uses shared wire
    /// storage. Partial selections use the input's constant-time position table.
    pub(super) fn encoded_slices(
        &self,
    ) -> Option<impl Iterator<Item = ozzy_proto::data::Records<'_>> + Clone> {
        let Storage::Batches { slices, .. } = &self.0 else {
            return None;
        };
        if !slices
            .iter()
            .all(|s| matches!(s.batch, RecordBatch::Encoded(_)))
        {
            return None;
        }
        Some(slices.iter().map(|slice| {
            let RecordBatch::Encoded(records) = slice.batch else {
                unreachable!("checked encoded record list")
            };
            records.range(slice.range.clone()).expect("validated range")
        }))
    }

    pub(super) fn is_packed(&self) -> bool {
        matches!(&self.0, Storage::Batches { slices, .. }
            if slices.iter().all(|s| matches!(s.batch, RecordBatch::Tiny(_))))
    }

    /// Borrow contiguous IDs, one-byte lengths, and payload runs when all ranges
    /// are packed. General/multipart lists return `None` without materialization.
    pub fn packed_slices(&self) -> Option<impl Iterator<Item = (&[MessageId], &[u8], &[u8])>> {
        if !self.is_packed() {
            return None;
        }
        let Storage::Batches { slices, .. } = &self.0 else {
            unreachable!("checked packed record list")
        };
        Some(slices.iter().map(|slice| {
            let RecordBatch::Tiny(records) = slice.batch else {
                unreachable!("checked packed record list")
            };
            (
                &records.ids()[slice.range.clone()],
                &records.lengths()[slice.range.clone()],
                records
                    .payload_range(slice.range.clone())
                    .expect("validated range"),
            )
        }))
    }

    /// Borrow a range of group-owned records, including packed tiny payloads.
    pub fn batch(batch: &'a RecordBatch, range: Range<usize>) -> Self {
        assert!(range.start <= range.end && range.end <= batch.len());
        Self(Storage::Batches {
            len: range.len(),
            slices: smallvec::smallvec![BatchSlice { batch, range }],
        })
    }
    /// Extend a list of group-owned ranges without making record owners.
    pub fn extend_batch(
        &mut self,
        batch: &'a RecordBatch,
        range: Range<usize>,
    ) -> Result<(), OperationCodecError> {
        assert!(range.start <= range.end && range.end <= batch.len());
        let Storage::Batches { slices, len } = &mut self.0 else {
            panic!("group-owned record list required")
        };
        *len = len
            .checked_add(range.len())
            .ok_or(OperationCodecError::LengthOverflow)?;
        slices.push(BatchSlice { batch, range });
        Ok(())
    }

    /// Borrow an existing input vector without allocating record descriptors.
    pub fn borrowed(records: &'a [OwnedRecord]) -> Self {
        Self(Storage::Borrowed {
            slices: if records.is_empty() {
                SmallVec::new()
            } else {
                smallvec::smallvec![records]
            },
            len: records.len(),
        })
    }

    /// Add another slice from the same contiguous partition range.
    ///
    /// Panics if this list was constructed from individual descriptors.
    pub fn extend_borrowed(
        &mut self,
        records: &'a [OwnedRecord],
    ) -> Result<(), OperationCodecError> {
        let Storage::Borrowed { slices, len } = &mut self.0 else {
            panic!("extend_borrowed requires a borrowed record list");
        };
        let next = len
            .checked_add(records.len())
            .ok_or(OperationCodecError::LengthOverflow)?;
        if !records.is_empty() {
            slices.push(records);
        }
        *len = next;
        Ok(())
    }

    /// Number of logical records in this list.
    pub fn len(&self) -> usize {
        match &self.0 {
            Storage::Described(records) => records.len(),
            Storage::Owned(records) => records.len(),
            Storage::Borrowed { len, .. } | Storage::Batches { len, .. } => *len,
        }
    }

    /// Whether the logical record list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Access explicitly described records, without silently materializing
    /// borrowed input. Intended for callers constructing canonical operations.
    pub fn described(&self) -> Option<&[AppendRecord<'a>]> {
        match &self.0 {
            Storage::Described(records) => Some(records),
            Storage::Borrowed { .. } | Storage::Batches { .. } | Storage::Owned(_) => None,
        }
    }

    /// Mutate explicitly described records, without materializing borrowed input.
    pub fn described_mut(&mut self) -> Option<&mut Vec<AppendRecord<'a>>> {
        match &mut self.0 {
            Storage::Described(records) => Some(records),
            Storage::Borrowed { .. } | Storage::Batches { .. } | Storage::Owned(_) => None,
        }
    }

    /// Iterate borrowed records without materializing a new descriptor table.
    pub fn iter(&self) -> RecordIter<'_, 'a> {
        match &self.0 {
            Storage::Batches { slices, len } => RecordIter {
                remaining: *len,
                inner: Iter::Batches {
                    slices: slices.iter(),
                    current: None,
                },
            },
            Storage::Described(records) => RecordIter {
                remaining: records.len(),
                inner: Iter::Described(records.iter()),
            },
            Storage::Borrowed { slices, len } => RecordIter {
                remaining: *len,
                inner: Iter::Borrowed(slices.iter().copied().flatten()),
            },
            Storage::Owned(records) => RecordIter {
                remaining: records.len(),
                inner: Iter::Owned(records.iter()),
            },
        }
    }

    /// Borrow the record at this zero-based list position.
    pub fn get(&self, mut index: usize) -> Option<RecordRef<'_>> {
        match &self.0 {
            Storage::Batches { slices, .. } => {
                for slice in slices {
                    if index < slice.range.len() {
                        return slice
                            .batch
                            .get(slice.range.start + index)
                            .map(RecordRef::batch);
                    }
                    index -= slice.range.len();
                }
                None
            }
            Storage::Described(records) => records.get(index).map(RecordRef::described),
            Storage::Borrowed { slices, .. } => {
                for slice in slices {
                    if let Some(record) = slice.get(index) {
                        return Some(RecordRef::owned(record));
                    }
                    index = index.checked_sub(slice.len())?;
                }
                None
            }
            Storage::Owned(records) => records.get(index).map(RecordRef::owned),
        }
    }

    pub(super) fn owned(records: Vec<OwnedRecord>) -> Self {
        Self(Storage::Owned(records))
    }
}

impl<'a> From<Vec<AppendRecord<'a>>> for AppendRecordList<'a> {
    fn from(records: Vec<AppendRecord<'a>>) -> Self {
        Self(Storage::Described(records))
    }
}

impl<'a> FromIterator<AppendRecord<'a>> for AppendRecordList<'a> {
    fn from_iter<T: IntoIterator<Item = AppendRecord<'a>>>(records: T) -> Self {
        Self::from(records.into_iter().collect::<Vec<_>>())
    }
}

impl PartialEq for AppendRecordList<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self.iter().zip(other.iter()).all(|(a, b)| {
                a.encoding == b.encoding
                    && a.message_id == b.message_id
                    && a.parts.iter().eq(b.parts.iter())
            })
    }
}

impl Eq for AppendRecordList<'_> {}

impl<'a, 'data> IntoIterator for &'a AppendRecordList<'data> {
    type Item = RecordRef<'a>;
    type IntoIter = RecordIter<'a, 'data>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A record view without payload ownership or per-record scratch storage.
#[derive(Debug, Clone, Copy)]
pub struct RecordRef<'a> {
    /// Payload representation, preserved through persistence and replay.
    pub encoding: ozzy_proto::data::Encoding,
    /// Application record identity preserved through retry and replay.
    pub message_id: MessageId,
    /// Opaque payload parts in record order.
    pub parts: RecordParts<'a>,
}

impl<'a> RecordRef<'a> {
    fn batch(record: ozzy_proto::data::BatchRecord<'a>) -> Self {
        Self {
            encoding: record.encoding,
            message_id: record.message_id,
            parts: match record.payload {
                PayloadRef::Parts(p) => RecordParts::Owned(p),
                PayloadRef::Single(p) => RecordParts::Single(p),
                PayloadRef::Encoded(p) => RecordParts::Encoded(p),
            },
        }
    }

    #[inline]
    fn described(record: &'a AppendRecord<'_>) -> Self {
        Self {
            encoding: record.encoding,
            message_id: record.message_id,
            parts: RecordParts::Described(&record.parts),
        }
    }

    #[inline]
    fn owned(record: &'a OwnedRecord) -> Self {
        Self {
            encoding: record.encoding,
            message_id: record.message_id,
            parts: RecordParts::Owned(&record.payload),
        }
    }
}

/// Borrowed multipart descriptors; no allocation even for many parts.
#[derive(Debug, Clone, Copy)]
pub enum RecordParts<'a> {
    /// Borrowed slices from an explicitly described record.
    Described(&'a [&'a [u8]]),
    /// Borrowed views of reference-counted owned payload parts.
    Owned(&'a [Bytes]),
    /// One contiguous payload part.
    Single(&'a [u8]),
    /// Multipart views decoded lazily from a validated wire descriptor.
    Encoded(ozzy_proto::data::EncodedParts<'a>),
}

impl<'a> RecordParts<'a> {
    /// Number of logical parts in this payload view.
    pub fn len(self) -> usize {
        match self {
            Self::Described(parts) => parts.len(),
            Self::Owned(parts) => parts.len(),
            Self::Single(_) => 1,
            Self::Encoded(parts) => parts.len(),
        }
    }

    /// Whether this payload view has no parts.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Iterate borrowed payload parts in record order.
    pub fn iter(self) -> impl ExactSizeIterator<Item = &'a [u8]> + Clone {
        match self {
            Self::Described(p) => PartIter::Described(p.iter()),
            Self::Owned(p) => PartIter::Owned(p.iter()),
            Self::Single(p) => PartIter::Single(Some(p)),
            Self::Encoded(p) => PartIter::Encoded(p.iter()),
        }
    }
}

#[derive(Clone)]
enum PartIter<'a> {
    Described(std::slice::Iter<'a, &'a [u8]>),
    Owned(std::slice::Iter<'a, Bytes>),
    Single(Option<&'a [u8]>),
    Encoded(ozzy_proto::data::Parts<'a>),
}

impl<'a> Iterator for PartIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Described(p) => p.next().copied(),
            Self::Owned(p) => p.next().map(Bytes::as_ref),
            Self::Single(p) => p.take(),
            Self::Encoded(p) => p.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = match self {
            Self::Described(p) => p.len(),
            Self::Owned(p) => p.len(),
            Self::Single(p) => usize::from(p.is_some()),
            Self::Encoded(p) => p.len(),
        };
        (n, Some(n))
    }
}
impl ExactSizeIterator for PartIter<'_> {}

/// Linear iteration across all borrowed slices, without repeated prefix scans.
#[derive(Debug, Clone)]
pub struct RecordIter<'a, 'data> {
    remaining: usize,
    inner: Iter<'a, 'data>,
}

#[derive(Debug, Clone)]
enum Iter<'a, 'data> {
    Batches {
        slices: std::slice::Iter<'a, BatchSlice<'data>>,
        current: Option<SliceIter<'data>>,
    },
    Described(std::slice::Iter<'a, AppendRecord<'data>>),
    Borrowed(std::iter::Flatten<std::iter::Copied<std::slice::Iter<'a, &'data [OwnedRecord]>>>),
    Owned(std::slice::Iter<'a, OwnedRecord>),
}

impl<'a, 'data: 'a> Iterator for RecordIter<'a, 'data> {
    type Item = RecordRef<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let record = match &mut self.inner {
            Iter::Batches { slices, current } => loop {
                if let Some(record) = current.as_mut().and_then(Iterator::next) {
                    break Some(RecordRef::batch(record));
                }
                let Some(slice) = slices.next() else {
                    break None;
                };
                *current = Some(slice.iter());
            },
            Iter::Described(records) => records.next().map(RecordRef::described),
            Iter::Borrowed(records) => records.next().map(RecordRef::owned),
            Iter::Owned(records) => records.next().map(RecordRef::owned),
        };
        if record.is_some() {
            self.remaining -= 1;
        }
        record
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for RecordIter<'_, '_> {}
impl std::iter::FusedIterator for RecordIter<'_, '_> {}
