//! A borrowed view of one encoded index file: lookups by offset, message,
//! timestamp, and operation.

use super::{
    BATCH_INDEX_ENTRY_BYTES, IndexLayout, IndexSource, MESSAGE_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES, SCHEDULE_INDEX_ENTRY_BYTES,
    order::{binary_search_section, compare_schedule_key, lower_bound_section},
    rows::{
        RowBase, decode_batch_entry, decode_message_row, decode_operation_entry,
        decode_operation_location, decode_row_base, decode_schedule_entry,
    },
    sections::Sections,
};
use crate::{
    BatchIndexEntry, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry, OperationLocation,
    ScheduleIndexEntry,
};
use ozzy_journal::operation::ReleaseKey;
use ozzy_proto::layout::Fixed;
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};

/// Validated zero-copy view over one complete immutable index file.
#[derive(Debug, Clone, Copy)]
pub struct SegmentIndexView<'a> {
    pub(super) source: IndexSource,
    build_memory_limit: u64,
    pub(super) base: RowBase,
    pub(super) batches: &'a [u8],
    pub(super) messages: &'a [u8],
    pub(super) operations: &'a [u8],
    pub(super) locations: &'a [u8],
    pub(super) schedules: &'a [u8],
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

    /// Partition of the indexed records; `None` without records.
    pub const fn partition(&self) -> Option<PartitionIncarnation> {
        self.base.partition
    }

    /// Number of batch rows.
    pub fn batch_count(&self) -> usize {
        self.batches.len() / BATCH_INDEX_ENTRY_BYTES
    }

    /// Number of message rows, one per record.
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

    /// Number of scheduled-record rows.
    pub fn schedule_count(&self) -> usize {
        self.schedules.len() / SCHEDULE_INDEX_ENTRY_BYTES
    }

    /// Release key of the partition's last scheduled record.
    pub fn last_schedule(&self, partition: PartitionIncarnation) -> Option<ReleaseKey> {
        let rows = self.schedules.as_chunks::<SCHEDULE_INDEX_ENTRY_BYTES>().0;
        let end = rows.partition_point(|row| row.array::<0, 16>() <= *partition.as_bytes());
        let row = rows.get(end.checked_sub(1)?)?;
        (row.array::<0, 16>() == *partition.as_bytes()).then(|| decode_schedule_entry(row).key)
    }

    /// Release key of the last scheduled record of each partition, in
    /// partition order.
    pub fn last_schedules(&self) -> Vec<(PartitionIncarnation, ReleaseKey)> {
        let mut rows = self.schedules.as_chunks::<SCHEDULE_INDEX_ENTRY_BYTES>().0;
        let mut last = Vec::new();
        while let Some(first) = rows.first() {
            let partition = first.array::<0, 16>();
            let (group, rest) =
                rows.split_at(rows.partition_point(|row| row.array::<0, 16>() == partition));
            if let Some(row) = group.last() {
                let entry = decode_schedule_entry(row);
                last.push((entry.partition, entry.key));
            }
            rows = rest;
        }
        last
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

    /// The batch holding `offset`.
    pub(super) fn batch_holding(&self, offset: Offset) -> Option<BatchIndexEntry> {
        let rows = self.batches.as_chunks::<BATCH_INDEX_ENTRY_BYTES>().0;
        let at = rows
            .partition_point(|row| row.u64_at::<0>() <= offset.get())
            .checked_sub(1)?;
        let batch = decode_batch_entry(rows.get(at)?, self.source.first_op_number);
        batch.record_index(offset).map(|_| batch)
    }

    /// Look up an exact partition incarnation and global record offset.
    pub fn find_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Option<OffsetIndexEntry> {
        if self.base.partition != Some(partition) {
            return None;
        }
        let batch = self.batch_holding(offset)?;
        Some(batch.entry(
            partition,
            self.find_operation_number(batch.op_number)?,
            batch.record_index(offset)?,
        ))
    }

    /// Look up an exact partition incarnation and record identity.
    pub fn find_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Option<MessageIndexEntry> {
        if self.base.partition != Some(partition) {
            return None;
        }
        let row = lower_bound_section(self.messages, |row: &[u8; MESSAGE_INDEX_ENTRY_BYTES]| {
            row.array::<0, 16>() < *message_id.as_bytes()
        })
        .filter(|row| row.array::<0, 16>() == *message_id.as_bytes())?;
        let offset = decode_message_row(row, self.base.first_offset).offset;
        Some(MessageIndexEntry {
            partition,
            message_id,
            offset,
            operation_digest: self
                .find_offset(partition, offset)?
                .location
                .operation
                .operation_digest,
        })
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
        if self.base.partition != Some(partition) {
            return None;
        }
        let first_offset = self.base.first_offset;
        let rows = self.messages.as_chunks::<MESSAGE_INDEX_ENTRY_BYTES>().0;
        let before = |row: &[u8; MESSAGE_INDEX_ENTRY_BYTES], offset: Offset| {
            row.array::<0, 16>()
                .cmp(id.as_bytes())
                .then_with(|| decode_message_row(row, first_offset).offset.cmp(&offset))
                .is_lt()
        };
        let first = rows.partition_point(|row| before(row, floor));
        let after = rows.partition_point(|row| before(row, end));
        let matching = rows.get(first..after)?.split_first()?;
        if matching.0.array::<0, 16>() != *id.as_bytes() {
            return None;
        }
        Some((
            decode_message_row(matching.0, first_offset).offset,
            decode_message_row(matching.1.last().unwrap_or(matching.0), first_offset).offset,
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
    /// The partition's scheduled records after release key `after`, or all
    /// of them, in release order.
    pub fn schedules_after(
        self,
        partition: PartitionIncarnation,
        after: Option<ReleaseKey>,
    ) -> impl Iterator<Item = ScheduleIndexEntry> + 'a {
        let rows = self.schedules.as_chunks::<SCHEDULE_INDEX_ENTRY_BYTES>().0;
        let first = match after {
            Some(after) => {
                rows.partition_point(|row| compare_schedule_key(row, partition, after).is_le())
            }
            None => rows.partition_point(|row| row.array::<0, 16>() < *partition.as_bytes()),
        };
        rows.get(first..)
            .unwrap_or_default()
            .iter()
            .take_while(move |row| row.array::<0, 16>() == *partition.as_bytes())
            .map(decode_schedule_entry)
    }

    pub(crate) fn batches(self) -> impl ExactSizeIterator<Item = BatchIndexEntry> + 'a {
        let first_op_number = self.source.first_op_number;
        self.batches
            .as_chunks::<BATCH_INDEX_ENTRY_BYTES>()
            .0
            .iter()
            .map(move |entry| decode_batch_entry(entry, first_op_number))
    }

    /// The first record of each batch, in offset order. Only these carry an
    /// append time.
    pub(crate) fn batch_heads(self) -> impl Iterator<Item = OffsetIndexEntry> + 'a {
        self.batches().filter_map(move |batch| {
            Some(batch.entry(
                self.base.partition?,
                self.find_operation_number(batch.op_number)?,
                0,
            ))
        })
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
        let split = layout.split(bytes);
        debug_assert!(split.is_some(), "built index matches its layout");
        let base = split.map_or(
            RowBase {
                partition: None,
                first_offset: 0,
                first_op_number: source.first_op_number,
            },
            |(header, _)| decode_row_base(header, source.first_op_number),
        );
        Self::new(
            source,
            build_memory_limit,
            base,
            split.map(|(_, sections)| sections).unwrap_or_default(),
        )
    }

    pub(super) fn new(
        source: IndexSource,
        build_memory_limit: u64,
        base: RowBase,
        sections: Sections<&'a [u8]>,
    ) -> Self {
        Self {
            source,
            build_memory_limit,
            base,
            batches: sections.batches,
            messages: sections.messages,
            operations: sections.operations,
            locations: sections.locations,
            schedules: sections.schedules,
        }
    }
}
