//! Owned frame cursor over validated immutable record descriptors.

use bytes::{Buf, Bytes};
use smallvec::SmallVec;

use super::{RecordIter, Records};
use crate::MessageId;

/// One owned record; multipart bytes may share immutable input or received frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedRecord {
    /// Payload representation.
    pub encoding: super::Encoding,
    /// Stable application identity.
    pub message_id: MessageId,
    /// Opaque payload parts, including empty parts.
    pub payload: SmallVec<[Bytes; 2]>,
}

/// Validated records with ownership of their exact metadata/payload frame slices.
#[derive(Debug, Clone)]
pub struct OwnedRecords {
    descriptors: Bytes,
    payload: Bytes,
    count: usize,
}

impl Records<'_> {
    /// Retain validated slices without copying. Both inputs must own the frames
    /// supplied to the decoder; unrelated frame storage is a caller error.
    pub fn to_owned(self, metadata: &Bytes, payload: &Bytes) -> OwnedRecords {
        OwnedRecords {
            descriptors: metadata.slice_ref(self.descriptors),
            payload: payload.slice_ref(self.payload),
            count: self.count,
        }
    }
}

impl OwnedRecords {
    /// The records after the first `count`, sharing the same frames.
    #[must_use]
    pub fn skip(&self, count: usize) -> Self {
        let rest = self.as_records().skip(count);
        Self {
            descriptors: self.descriptors.slice_ref(rest.descriptors),
            payload: self.payload.slice_ref(rest.payload),
            count: rest.count,
        }
    }

    /// Borrow the already validated immutable collection. No descriptor rescan.
    pub fn as_records(&self) -> Records<'_> {
        Records {
            descriptors: &self.descriptors,
            payload: &self.payload,
            count: self.count,
            summary: None,
        }
    }

    /// Exact validated record count.
    pub const fn len(&self) -> usize {
        self.count
    }

    /// Valid decoded batches are nonempty.
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Payload bytes charged after skipping an already received prefix.
    pub fn payload_bytes_after(&self, skip: usize) -> usize {
        let mut records = self.borrowed();
        for _ in 0..skip.min(self.count) {
            let _ = records.next();
        }
        records.payload.len()
    }

    /// Consume the batch into a lazy cursor; no payload copy or record table.
    pub fn into_records(self) -> OwnedRecordIter {
        OwnedRecordIter(self)
    }

    fn borrowed(&self) -> RecordIter<'_> {
        self.as_records().iter()
    }
}

/// Lazy owned cursor retaining only the remaining validated frame slices.
#[derive(Debug)]
pub struct OwnedRecordIter(OwnedRecords);

impl Iterator for OwnedRecordIter {
    type Item = OwnedRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.count == 0 {
            return None;
        }
        if self.0.descriptors[16..20] == [0, 0, 0, 1] {
            // Raw, single-part records have one fixed 24-byte descriptor.
            // The collection was validated before ownership was retained.
            let message_id = MessageId::from_bytes(self.0.descriptors[..16].try_into().unwrap());
            let bytes = u32::from_be_bytes(self.0.descriptors[20..24].try_into().unwrap()) as usize;
            let payload = self.0.payload.slice(..bytes);
            let metadata_left = self.0.descriptors.len() - 24;
            let payload_left = self.0.payload.len() - bytes;
            retain_tail(&mut self.0.descriptors, metadata_left);
            retain_tail(&mut self.0.payload, payload_left);
            self.0.count -= 1;
            return Some(OwnedRecord {
                encoding: super::Encoding::Raw,
                message_id,
                payload: smallvec::smallvec![payload],
            });
        }
        let mut cursor = self.0.borrowed();
        let record = cursor.next()?;
        let record = OwnedRecord {
            encoding: record.encoding,
            message_id: record.message_id,
            payload: record
                .parts
                .map(|part| self.0.payload.slice_ref(part))
                .collect(),
        };
        let remaining = (
            cursor.descriptors.0.len(),
            cursor.payload.len(),
            cursor.remaining,
        );
        // Returned parts retain their original owners. Advancing the cursor
        // itself needs no new owners or atomic reference-count round trips.
        retain_tail(&mut self.0.descriptors, remaining.0);
        retain_tail(&mut self.0.payload, remaining.1);
        self.0.count = remaining.2;
        Some(record)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0.count, Some(self.0.count))
    }
}

impl ExactSizeIterator for OwnedRecordIter {}

fn retain_tail(bytes: &mut Bytes, remaining: usize) {
    if remaining == 0 {
        // clear()/advance(len) can retain backing storage. Match slice_ref's
        // empty-tail release, even before trailing empty records are consumed.
        *bytes = Bytes::new();
    } else {
        bytes.advance(bytes.len() - remaining);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{DataLimits, Record, decode_records, encode_records};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Frame(Vec<u8>, Arc<AtomicUsize>);

    impl AsRef<[u8]> for Frame {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    impl Drop for Frame {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn tracked(bytes: Vec<u8>) -> (Bytes, Arc<AtomicUsize>) {
        let drops = Arc::new(AtomicUsize::new(0));
        (Bytes::from_owner(Frame(bytes, drops.clone())), drops)
    }

    fn input(bytes: &[u8]) -> (OwnedRecords, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let records = [
            Record {
                encoding: crate::data::Encoding::Raw,
                message_id: MessageId::from_bytes([1; 16]),
                parts: &[b"", bytes, b""],
            },
            Record {
                encoding: crate::data::Encoding::Raw,
                message_id: MessageId::from_bytes([2; 16]),
                parts: &[b""],
            },
            Record {
                encoding: crate::data::Encoding::Raw,
                message_id: MessageId::from_bytes([3; 16]),
                parts: &[b"", b""],
            },
        ];
        let mut metadata = Vec::new();
        let mut payload = Vec::new();
        encode_records(&records, &mut metadata, Some(&mut payload));
        let (metadata, metadata_drops) = tracked(metadata);
        let (payload, payload_drops) = tracked(payload);
        let records = decode_records(&metadata, &payload, 0, DataLimits::default())
            .unwrap()
            .to_owned(&metadata, &payload);
        (records, metadata_drops, payload_drops)
    }

    #[test]
    fn mixed_descriptors_keep_owned_and_borrowed_iteration_identical() {
        let encoded = [0, 0, 0, 1, 0, 0, 0, 4, 0x40, 1, 2, 3, 4];
        let records = [
            Record {
                encoding: super::super::Encoding::Raw,
                message_id: MessageId::from_bytes([1; 16]),
                parts: &[b""],
            },
            Record {
                encoding: super::super::Encoding::Raw,
                message_id: MessageId::from_bytes([2; 16]),
                parts: &[b"first"],
            },
            Record {
                encoding: super::super::Encoding::Lz4 { decoded_bytes: 4 },
                message_id: MessageId::from_bytes([3; 16]),
                parts: &[&encoded],
            },
            Record {
                encoding: super::super::Encoding::Raw,
                message_id: MessageId::from_bytes([4; 16]),
                parts: &[b"", b"middle", b""],
            },
            Record {
                encoding: super::super::Encoding::Raw,
                message_id: MessageId::from_bytes([5; 16]),
                parts: &[b"last"],
            },
        ];
        let mut metadata = Vec::new();
        let mut payload = Vec::new();
        encode_records(&records, &mut metadata, Some(&mut payload));
        let metadata = Bytes::from(metadata);
        let payload = Bytes::from(payload);
        let borrowed = decode_records(&metadata, &payload, 0, DataLimits::default()).unwrap();
        let mut owned = borrowed.to_owned(&metadata, &payload).into_records();
        for expected in borrowed.iter() {
            let actual = owned.next().unwrap();
            assert_eq!(actual.encoding, expected.encoding);
            assert_eq!(actual.message_id, expected.message_id);
            assert!(actual.payload.iter().map(Bytes::as_ref).eq(expected.parts));
        }
        assert_eq!(owned.len(), 0);
        assert!(owned.next().is_none());
    }

    #[test]
    fn record_clones_share_parts_until_the_last_owner_is_dropped() {
        for count in [0, 1, 2, 3, 7] {
            for size in [0, 128, 1024] {
                let (body, drops) = tracked(vec![7; size]);
                let record = OwnedRecord {
                    encoding: crate::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([9; 16]),
                    payload: (0..count).map(|_| body.slice(..)).collect(),
                };
                let copy = record.clone();
                assert_eq!(record, copy);
                for (original, cloned) in record.payload.iter().zip(&copy.payload) {
                    assert_eq!(original.as_ptr(), cloned.as_ptr());
                }
                drop(body);
                drop(record);
                assert_eq!(
                    drops.load(Ordering::Relaxed),
                    usize::from(count == 0 || size == 0)
                );
                for part in &copy.payload {
                    assert_eq!(part.as_ref(), vec![7; size]);
                }
                drop(copy);
                assert_eq!(drops.load(Ordering::Relaxed), 1);
            }
        }
    }

    #[test]
    fn single_and_multipart_cursors_preserve_bytes_and_last_owner_release() {
        for size in [0, 1, 128, 1024] {
            for count in [1, 2, 3, 7] {
                let body = vec![7; size];
                let parts = (0..count)
                    .map(|index| {
                        if index % 2 == 0 {
                            body.as_slice()
                        } else {
                            b"".as_slice()
                        }
                    })
                    .collect::<Vec<_>>();
                let id = MessageId::from_bytes([9; 16]);
                let mut metadata = Vec::new();
                let mut payload = Vec::new();
                encode_records(
                    &[Record {
                        encoding: crate::data::Encoding::Raw,
                        message_id: id,
                        parts: &parts,
                    }],
                    &mut metadata,
                    Some(&mut payload),
                );
                let (metadata, metadata_drops) = tracked(metadata);
                let (payload, payload_drops) = tracked(payload);
                let records = decode_records(&metadata, &payload, 0, DataLimits::default())
                    .unwrap()
                    .to_owned(&metadata, &payload);
                drop((metadata, payload));
                let mut records = records.into_records();
                let record = records.next().unwrap();
                assert_eq!(record.message_id, id);
                assert!(
                    record
                        .payload
                        .iter()
                        .map(Bytes::as_ref)
                        .eq(parts.iter().copied())
                );
                assert_eq!(records.len(), 0);
                assert!(records.next().is_none());
                assert_eq!(metadata_drops.load(Ordering::Relaxed), 1);
                assert_eq!(
                    payload_drops.load(Ordering::Relaxed),
                    usize::from(size == 0)
                );
                drop(records);
                drop(record);
                assert_eq!(payload_drops.load(Ordering::Relaxed), 1);
            }
        }
    }

    #[test]
    fn exhausted_frames_release_before_cursor_with_empty_records_is_dropped() {
        for size in [1, 128, 1024] {
            let bytes = vec![7; size];
            let (records, metadata, payload) = input(&bytes);
            let mut cursor = records.into_records();
            let record = cursor.next().unwrap();
            assert_eq!(record.payload[1], bytes);
            assert!(record.payload[0].is_empty() && record.payload[2].is_empty());
            assert_eq!(cursor.len(), 2);
            assert_eq!(payload.load(Ordering::Relaxed), 0);
            drop(record);
            assert_eq!(payload.load(Ordering::Relaxed), 1);
            assert_eq!(metadata.load(Ordering::Relaxed), 0);
            for id in [2, 3] {
                let record = cursor.next().unwrap();
                assert_eq!(record.message_id, MessageId::from_bytes([id; 16]));
                assert!(record.payload.iter().all(Bytes::is_empty));
            }
            assert_eq!(metadata.load(Ordering::Relaxed), 1);
            assert_eq!(cursor.len(), 0);
            assert!(cursor.next().is_none());
            assert!(cursor.next().is_none());
        }
    }

    #[test]
    fn abandoning_a_cursor_preserves_cloned_input_and_yielded_parts() {
        let (records, metadata, payload) = input(b"body");
        let clone = records.clone();
        let mut cursor = records.into_records();
        let first = cursor.next().unwrap();
        drop(cursor);
        assert_eq!(metadata.load(Ordering::Relaxed), 0);
        let mut other = clone.into_records();
        assert_eq!(other.next().unwrap(), first);
        drop(other);
        assert_eq!(metadata.load(Ordering::Relaxed), 1);
        assert_eq!(payload.load(Ordering::Relaxed), 0);
        assert_eq!(first.payload[1], b"body".as_slice());
        drop(first);
        assert_eq!(payload.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn empty_payloads_keep_metadata_progress_without_retaining_a_frame() {
        let (records, metadata, payload) = input(b"");
        assert_eq!(payload.load(Ordering::Relaxed), 1);
        let mut cursor = records.into_records();
        for id in [1, 2, 3] {
            assert_eq!(cursor.len(), usize::from(4 - id));
            let record = cursor.next().unwrap();
            assert_eq!(record.message_id, MessageId::from_bytes([id; 16]));
            assert!(record.payload.iter().all(Bytes::is_empty));
            assert_eq!(metadata.load(Ordering::Relaxed), usize::from(id == 3));
        }
        assert!(cursor.next().is_none());
    }
}
