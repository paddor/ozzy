//! A borrowed view of one encoded index file: lookups by offset, message,
//! timestamp, and operation.

use super::{
    IndexLayout, IndexSource, MESSAGE_INDEX_ENTRY_BYTES, OFFSET_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES,
    order::{binary_search_section, compare_message_key, compare_offset_key, lower_bound_section},
    rows::{
        decode_message_entry, decode_offset_entry, decode_operation_entry,
        decode_operation_location,
    },
    sections::Sections,
};
use crate::{MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry, OperationLocation};
use ozzy_proto::layout::Fixed;
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use std::cmp::Ordering;

/// Validated zero-copy view over one complete immutable index file.
#[derive(Debug, Clone, Copy)]
pub struct SegmentIndexView<'a> {
    pub(super) source: IndexSource,
    build_memory_limit: u64,
    pub(super) offsets: &'a [u8],
    pub(super) messages: &'a [u8],
    pub(super) operations: &'a [u8],
    pub(super) locations: &'a [u8],
}

impl SegmentIndexView<'_> {
    /// Exact immutable segment prefix from which this index was derived.
    pub const fn source(&self) -> IndexSource {
        self.source
    }

    /// Encoded build-time memory allowance recorded in the index header.
    pub const fn build_memory_limit(&self) -> u64 {
        self.build_memory_limit
    }

    /// Number of record-offset selectors.
    pub fn offset_count(&self) -> usize {
        self.offsets.len() / OFFSET_INDEX_ENTRY_BYTES
    }

    /// Number of message-identity selectors.
    pub fn message_count(&self) -> usize {
        self.messages.len() / MESSAGE_INDEX_ENTRY_BYTES
    }

    /// Number of canonical operation-identity selectors.
    pub fn operation_count(&self) -> usize {
        self.operations.len() / OPERATION_INDEX_ENTRY_BYTES
    }

    /// Number of numeric selectors, covering the complete source prefix.
    pub fn location_count(&self) -> usize {
        self.locations.len() / OPERATION_LOCATION_BYTES
    }

    /// Locate an exact canonical operation without scanning segment payloads.
    pub fn find_operation_number(&self, number: u64) -> Option<OperationLocation> {
        let at = usize::try_from(number.checked_sub(self.source.first_op_number)?).ok()?;
        let entry = self
            .locations
            .as_chunks::<OPERATION_LOCATION_BYTES>()
            .0
            .get(at)?;
        Some(decode_operation_location(entry, self.source.segment_id))
    }

    /// Look up an exact partition incarnation and global record offset.
    pub fn find_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Option<OffsetIndexEntry> {
        binary_search_section(self.offsets, |entry| {
            compare_offset_key(entry, partition, offset)
        })
        .map(|entry| decode_offset_entry(entry, self.source.segment_id))
    }

    /// Look up an exact partition incarnation and record identity.
    pub fn find_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Option<MessageIndexEntry> {
        lower_bound_section(self.messages, |entry| {
            compare_message_key(entry, partition, message_id) == Ordering::Less
        })
        .filter(|entry| compare_message_key(entry, partition, message_id) == Ordering::Equal)
        .map(decode_message_entry)
    }

    /// Oldest/newest matches inside one retained partition range. Binary search
    /// includes the offset, so even millions of duplicate IDs keep bounded work.
    pub fn message_offsets(
        &self,
        partition: PartitionIncarnation,
        id: MessageId,
        floor: Offset,
        end: Offset,
    ) -> Option<(Offset, Offset)> {
        let rows = self.messages.as_chunks::<MESSAGE_INDEX_ENTRY_BYTES>().0;
        let before = |row: &[u8; MESSAGE_INDEX_ENTRY_BYTES], offset: Offset| {
            compare_message_key(row, partition, id)
                .then_with(|| row.u64_at::<32>().cmp(&offset.get()))
                .is_lt()
        };
        let first = rows.partition_point(|row| before(row, floor));
        let after = rows.partition_point(|row| before(row, end));
        let matching = rows.get(first..after)?.split_first()?;
        if compare_message_key(matching.0, partition, id) != Ordering::Equal {
            return None;
        }
        Some((
            Offset::new(matching.0.u64_at::<32>()),
            Offset::new(matching.1.last().unwrap_or(matching.0).u64_at::<32>()),
        ))
    }

    /// Look up an exact canonical control-operation identity.
    pub fn find_operation(&self, operation_id: OperationId) -> Option<OperationIndexEntry> {
        binary_search_section(
            self.operations,
            |entry: &[u8; OPERATION_INDEX_ENTRY_BYTES]| {
                entry.array::<0, 16>().cmp(operation_id.as_bytes())
            },
        )
        .map(|entry| decode_operation_entry(entry, self.source.segment_id))
    }
}

impl<'a> SegmentIndexView<'a> {
    pub(crate) fn offsets(self) -> impl ExactSizeIterator<Item = OffsetIndexEntry> + 'a {
        self.offsets
            .as_chunks::<OFFSET_INDEX_ENTRY_BYTES>()
            .0
            .iter()
            .map(move |entry| decode_offset_entry(entry, self.source.segment_id))
    }

    /// Control-operation identities in sorted order.
    pub(crate) fn operation_ids(self) -> impl ExactSizeIterator<Item = OperationId> + 'a {
        self.operations
            .as_chunks::<OPERATION_INDEX_ENTRY_BYTES>()
            .0
            .iter()
            .map(|entry| OperationId::from_bytes(entry.array::<0, 16>()))
    }

    pub(crate) fn from_validated(
        bytes: &'a [u8],
        source: IndexSource,
        build_memory_limit: u64,
        layout: IndexLayout,
    ) -> Self {
        // The builder validated these bytes against this layout; a mismatch
        // would only leave the view empty.
        let sections = layout.split(bytes).map(|(_, sections)| sections);
        debug_assert!(sections.is_some(), "built index matches its layout");
        Self::new(source, build_memory_limit, sections.unwrap_or_default())
    }

    pub(super) fn new(
        source: IndexSource,
        build_memory_limit: u64,
        sections: Sections<&'a [u8]>,
    ) -> Self {
        Self {
            source,
            build_memory_limit,
            offsets: sections.offsets,
            messages: sections.messages,
            operations: sections.operations,
            locations: sections.locations,
        }
    }
}
