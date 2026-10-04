//! Metadata-only lookup over a captured visible partition range.

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
                if let Some(offset) = index.timestamp_offset(
                    self.partition,
                    timestamp,
                    self.retained_from,
                    self.confirmed_end,
                    self.through,
                ) {
                    result.observe_match(offset);
                }
            }
            _ => {}
        }
    }
}
