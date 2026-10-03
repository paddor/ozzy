//! Packed single-part selectors. One end offset per record supports constant-time
//! random reads; general records retain exact multipart boundaries.

use super::{CachedRecord, MessageId, Range};

#[derive(Debug)]
pub(super) enum CachedRecords {
    General(Vec<CachedRecord>),
    Packed {
        ids: Vec<MessageId>,
        offsets: Vec<usize>,
    },
}

impl CachedRecords {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::General(records) => records.len(),
            Self::Packed { ids, .. } => ids.len(),
        }
    }

    #[inline]
    pub(super) fn get(&self, index: usize) -> Option<RecordRef<'_>> {
        match self {
            Self::General(records) => records.get(index).map(|record| RecordRef {
                encoding: record.encoding,
                message_id: record.message_id,
                parts: Parts::Many(&record.parts),
            }),
            Self::Packed { ids, offsets } => Some(RecordRef {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: *ids.get(index)?,
                parts: Parts::Single(offsets[index]..offsets[index + 1]),
            }),
        }
    }

    pub(super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Packed { ids, offsets } => {
                ids.capacity() * size_of::<MessageId>() + offsets.capacity() * size_of::<usize>()
            }
            Self::General(records) => {
                records.capacity() * size_of::<CachedRecord>()
                    + records
                        .iter()
                        .filter(|r| r.parts.spilled())
                        .map(|r| r.parts.capacity() * size_of::<Range<usize>>())
                        .sum::<usize>()
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct RecordRef<'a> {
    pub(super) encoding: ozzy_proto::data::Encoding,
    pub(super) message_id: MessageId,
    pub(super) parts: Parts<'a>,
}

#[derive(Debug)]
pub(super) enum Parts<'a> {
    Single(Range<usize>),
    Many(&'a [Range<usize>]),
}

impl Parts<'_> {
    #[inline]
    pub(super) fn iter(&self) -> impl ExactSizeIterator<Item = Range<usize>> + Clone {
        let len = match self {
            Self::Single(_) => 1,
            Self::Many(parts) => parts.len(),
        };
        (0..len).map(move |i| match self {
            Self::Single(range) => range.clone(),
            Self::Many(parts) => parts[i].clone(),
        })
    }
}
