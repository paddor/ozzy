//! Constant-time selection from immutable descriptor bytes. Only byte positions
//! are retained; identities, encodings and part lengths stay in the body.

use super::{AppendBatchView, IndexedReadError, MessageId, Range};
use ozzy_proto::data::Encoding;

#[derive(Debug, Clone, Copy)]
pub(super) struct RecordStart {
    pub(super) descriptor: usize,
    pub(super) payload: usize,
}

#[derive(Debug)]
pub(super) enum CachedRecords {
    Uniform {
        count: usize,
        descriptor: usize,
        payload: usize,
        record_bytes: usize,
    },
    General {
        starts: Vec<RecordStart>,
        parts: usize,
    },
    Packed {
        ids: Vec<MessageId>,
        offsets: Vec<usize>,
    },
}

impl CachedRecords {
    pub(super) fn general(
        batch: &AppendBatchView<'_>,
        descriptor: usize,
        payload: usize,
    ) -> Result<Self, IndexedReadError> {
        if let Some(record_bytes) = batch.uniform_raw_record_bytes() {
            return Ok(Self::Uniform {
                count: batch.summary.record_count,
                descriptor,
                payload,
                record_bytes,
            });
        }
        let mut next = RecordStart {
            descriptor,
            payload,
        };
        let mut starts = Vec::with_capacity(batch.summary.record_count + 1);
        let mut parts = 0;
        for record in batch.descriptors() {
            starts.push(next);
            let count = record.part_lengths.len();
            next.descriptor += 20 + record.encoding.metadata_bytes() + count * 4;
            parts += count;
            for length in record.part_lengths {
                next.payload = next
                    .payload
                    .checked_add(length)
                    .ok_or(IndexedReadError::InvalidSelector)?;
            }
        }
        starts.push(next);
        Ok(Self::General { starts, parts })
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Uniform { count, .. } => *count,
            Self::General { starts, .. } => starts.len() - 1,
            Self::Packed { ids, .. } => ids.len(),
        }
    }

    pub(super) fn parts(&self) -> usize {
        match self {
            Self::Uniform { count, .. } => *count,
            Self::General { parts, .. } => *parts,
            Self::Packed { ids, .. } => ids.len(),
        }
    }

    pub(super) fn payload_range(&self, range: Range<usize>) -> Range<usize> {
        match self {
            Self::Uniform {
                payload,
                record_bytes,
                ..
            } => payload + range.start * record_bytes..payload + range.end * record_bytes,
            Self::General { starts, .. } => starts[range.start].payload..starts[range.end].payload,
            Self::Packed { offsets, .. } => offsets[range.start]..offsets[range.end],
        }
    }

    pub(super) fn descriptor_range(&self, range: Range<usize>) -> Option<Range<usize>> {
        match self {
            Self::Uniform { descriptor, .. } => {
                Some(descriptor + range.start * 24..descriptor + range.end * 24)
            }
            Self::General { starts, .. } => {
                Some(starts[range.start].descriptor..starts[range.end].descriptor)
            }
            Self::Packed { .. } => None,
        }
    }

    #[inline]
    pub(super) fn get<'a>(&'a self, index: usize, body: &'a [u8]) -> Option<RecordRef<'a>> {
        match self {
            Self::Uniform {
                count,
                descriptor,
                payload,
                record_bytes,
            } => {
                if index >= *count {
                    return None;
                }
                let descriptor = descriptor + index * 24;
                let payload = payload + index * record_bytes;
                Some(RecordRef {
                    encoding: Encoding::Raw,
                    message_id: MessageId::from_bytes(
                        body[descriptor..descriptor + 16].try_into().unwrap(),
                    ),
                    parts: Parts::Single(payload..payload + record_bytes),
                })
            }
            Self::General { starts, .. } => {
                let start = starts.get(index)?;
                let end = starts.get(index + 1)?;
                let descriptor = &body[start.descriptor..end.descriptor];
                let encoding = match u32_at(descriptor, 16) >> 24 {
                    0 => Encoding::Raw,
                    1 => Encoding::Lz4 {
                        decoded_bytes: u32_at(descriptor, 20),
                    },
                    _ => unreachable!("validated encoding"),
                };
                Some(RecordRef {
                    encoding,
                    message_id: MessageId::from_bytes(descriptor[..16].try_into().unwrap()),
                    parts: Parts::Encoded {
                        lengths: descriptor[20 + encoding.metadata_bytes()..]
                            .as_chunks::<4>()
                            .0,
                        payload: start.payload..end.payload,
                    },
                })
            }
            Self::Packed { ids, offsets } => Some(RecordRef {
                encoding: Encoding::Raw,
                message_id: *ids.get(index)?,
                parts: Parts::Single(offsets[index]..offsets[index + 1]),
            }),
        }
    }

    pub(super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Uniform { .. } => 0,
            Self::Packed { ids, offsets } => {
                ids.capacity() * size_of::<MessageId>() + offsets.capacity() * size_of::<usize>()
            }
            Self::General { starts, .. } => starts.capacity() * size_of::<RecordStart>(),
        }
    }
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap())
}

#[derive(Debug)]
pub(super) struct RecordRef<'a> {
    pub(super) encoding: Encoding,
    pub(super) message_id: MessageId,
    pub(super) parts: Parts<'a>,
}

#[derive(Debug)]
pub(super) enum Parts<'a> {
    Single(Range<usize>),
    Encoded {
        lengths: &'a [[u8; 4]],
        payload: Range<usize>,
    },
}

impl Parts<'_> {
    pub(super) fn bytes(&self) -> usize {
        match self {
            Self::Single(range) => range.len(),
            Self::Encoded { payload, .. } => payload.len(),
        }
    }

    #[inline]
    pub(super) fn iter(&self) -> impl ExactSizeIterator<Item = Range<usize>> + Clone {
        let (single, lengths, start) = match self {
            Self::Single(range) => (Some(range.clone()), [].iter(), 0),
            Self::Encoded { lengths, payload } => (None, lengths.iter(), payload.start),
        };
        PartRanges {
            single,
            lengths,
            start,
        }
    }
}

#[derive(Clone)]
struct PartRanges<'a> {
    single: Option<Range<usize>>,
    lengths: std::slice::Iter<'a, [u8; 4]>,
    start: usize,
}

impl Iterator for PartRanges<'_> {
    type Item = Range<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        self.single.take().or_else(|| {
            let end = self.start + u32::from_be_bytes(*self.lengths.next()?) as usize;
            let range = self.start..end;
            self.start = end;
            Some(range)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.lengths.len() + usize::from(self.single.is_some());
        (len, Some(len))
    }
}

impl ExactSizeIterator for PartRanges<'_> {}
