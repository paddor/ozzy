//! Metadata-only lookup over a captured visible partition range.

use crate::OffsetIndexEntry;
use ozzy_core::reader::seek::Selection;
use ozzy_proto::{Offset, PartitionIncarnation, reader::Start};

/// Owner-captured bounds. A storage lookup supplies no visibility authority.
#[derive(Clone, Copy, Debug)]
pub struct SeekQuery {
    /// Exact selected partition incarnation.
    pub partition: PartitionIncarnation,
    /// Initial selector, used once before offset delivery.
    pub start: Start,
    /// Earliest retained record in the committed image.
    pub retained_from: Offset,
    /// Exclusive confirmed partition record end.
    pub confirmed_end: Offset,
    /// Upper visible canonical operation number.
    pub through: u64,
}

impl SeekQuery {
    pub(crate) fn observe_index(self, index: crate::SegmentIndexView<'_>, result: &mut Selection) {
        match self.start {
            Start::RecordId { id, .. } => {
                if let Some((first, last)) = index.message_offsets(
                    self.partition,
                    id,
                    self.retained_from,
                    self.confirmed_end,
                ) {
                    for offset in [first, last] {
                        if index
                            .find_offset(self.partition, offset)
                            .is_some_and(|entry| entry.location.operation.op_number <= self.through)
                        {
                            result.observe_match(offset);
                        }
                    }
                }
            }
            Start::Timestamp(timestamp) => {
                let find = |partition, offset| index.find_offset(partition, offset);
                if let Some(offset) = self.timestamp_offset(timestamp, index.offsets(), find) {
                    result.observe_match(offset);
                }
            }
            _ => {}
        }
    }

    /// Sparse APPEND timestamps live on batch-head offset rows. Payloads remain
    /// unopened; nonmonotonic clocks are resolved in record-offset order. A head
    /// below the retained floor selects the floor when the floor row, looked up
    /// through `find_offset`, lies in the same batch.
    pub(crate) fn timestamp_offset(
        self,
        timestamp: u64,
        offsets: impl IntoIterator<Item = OffsetIndexEntry>,
        find_offset: impl FnOnce(PartitionIncarnation, Offset) -> Option<OffsetIndexEntry>,
    ) -> Option<Offset> {
        let at_floor = find_offset(self.partition, self.retained_from);
        offsets
            .into_iter()
            .filter(|entry| {
                entry.partition == self.partition
                    && entry.location.record_index == 0
                    && entry.append_timestamp_millis >= timestamp
                    && entry.location.operation.op_number <= self.through
                    && entry.offset < self.confirmed_end
            })
            .find_map(|head| {
                if head.offset >= self.retained_from {
                    return Some(head.offset);
                }
                at_floor
                    .filter(|entry| {
                        entry.location.operation == head.location.operation
                            && entry.location.batch_index == head.location.batch_index
                    })
                    .map(|_| self.retained_from)
            })
    }
}
