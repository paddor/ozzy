//! Row order: sort keys, comparisons, section searches, and order checks.

use super::{
    IndexFileError, MESSAGE_INDEX_ENTRY_BYTES, OFFSET_INDEX_ENTRY_BYTES, SCHEDULE_INDEX_ENTRY_BYTES,
};
use crate::{
    MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry, OperationLocation, ScheduleIndexEntry,
};
use ozzy_journal::operation::ReleaseKey;
use ozzy_proto::layout::Fixed;
use ozzy_proto::{MessageId, Offset, PartitionIncarnation};
use std::cmp::Ordering;

pub(crate) fn compare_locations(left: &OperationLocation, right: &OperationLocation) -> Ordering {
    left.op_number.cmp(&right.op_number)
}

pub(crate) fn compare_offset_entries(
    left: &OffsetIndexEntry,
    right: &OffsetIndexEntry,
) -> Ordering {
    compare_partition_offset(left.partition, left.offset, right.partition, right.offset)
}

pub(super) fn compare_partition_offset(
    left_partition: PartitionIncarnation,
    left_offset: Offset,
    right_partition: PartitionIncarnation,
    right_offset: Offset,
) -> Ordering {
    left_partition
        .as_bytes()
        .cmp(right_partition.as_bytes())
        .then_with(|| left_offset.get().cmp(&right_offset.get()))
}

pub(crate) fn compare_message_entries(
    left: &MessageIndexEntry,
    right: &MessageIndexEntry,
) -> Ordering {
    left.partition
        .as_bytes()
        .cmp(right.partition.as_bytes())
        .then_with(|| left.message_id.as_bytes().cmp(right.message_id.as_bytes()))
        .then_with(|| left.offset.cmp(&right.offset))
}

pub(crate) fn compare_operation_entries(
    left: &OperationIndexEntry,
    right: &OperationIndexEntry,
) -> Ordering {
    left.operation_id
        .as_bytes()
        .cmp(right.operation_id.as_bytes())
}

pub(crate) fn compare_schedule_entries(
    left: &ScheduleIndexEntry,
    right: &ScheduleIndexEntry,
) -> Ordering {
    left.partition
        .as_bytes()
        .cmp(right.partition.as_bytes())
        .then_with(|| left.key.cmp(&right.key))
        .then_with(|| {
            (left.record.operation, left.record.index)
                .cmp(&(right.record.operation, right.record.index))
        })
}

/// Order of a schedule row against a partition and release key.
pub(super) fn compare_schedule_key(
    entry: &[u8; SCHEDULE_INDEX_ENTRY_BYTES],
    partition: PartitionIncarnation,
    key: ReleaseKey,
) -> Ordering {
    entry
        .array::<0, 16>()
        .cmp(partition.as_bytes())
        .then_with(|| entry.u64_at::<16>().cmp(&key.millis))
        .then_with(|| entry.u64_at::<24>().cmp(&key.schedule))
}

pub(super) fn compare_offset_key(
    entry: &[u8; OFFSET_INDEX_ENTRY_BYTES],
    partition: PartitionIncarnation,
    offset: Offset,
) -> Ordering {
    entry
        .array::<0, 16>()
        .cmp(partition.as_bytes())
        .then_with(|| entry.u64_at::<16>().cmp(&offset.get()))
}

pub(super) fn compare_message_key(
    entry: &[u8; MESSAGE_INDEX_ENTRY_BYTES],
    partition: PartitionIncarnation,
    message_id: MessageId,
) -> Ordering {
    entry
        .array::<0, 16>()
        .cmp(partition.as_bytes())
        .then_with(|| entry.array::<16, 16>().cmp(message_id.as_bytes()))
}

pub(super) fn binary_search_section<const W: usize>(
    section: &[u8],
    compare: impl Fn(&[u8; W]) -> Ordering,
) -> Option<&[u8; W]> {
    let rows = section.as_chunks::<W>().0;
    rows.get(rows.binary_search_by(compare).ok()?)
}

pub(super) fn lower_bound_section<const W: usize>(
    section: &[u8],
    is_less: impl Fn(&[u8; W]) -> bool,
) -> Option<&[u8; W]> {
    let rows = section.as_chunks::<W>().0;
    rows.get(rows.partition_point(|row| is_less(row)))
}

pub(super) fn sorted_entry_checks<'a, T>(
    entries: &'a [T],
    compare: fn(&T, &T) -> Ordering,
    kind: &'static str,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + 'a {
    entries.windows(2).map(move |pair| match pair {
        [first, second] if compare(first, second) == Ordering::Less => Ok(size_of::<T>()),
        _ => Err(IndexFileError::UnsortedOrDuplicate(kind)),
    })
}

pub(super) fn raw_order_checks<'a, const W: usize>(
    section: &'a [u8],
    key_bytes: usize,
    kind: &'static str,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + 'a {
    let rows = section.as_chunks::<W>().0;
    rows.iter()
        .zip(rows.iter().skip(1))
        .map(move |(previous, entry)| {
            let previous = previous.iter().take(key_bytes);
            if previous.cmp(entry.iter().take(key_bytes)) == Ordering::Less {
                Ok(W)
            } else {
                Err(IndexFileError::UnsortedOrDuplicate(kind))
            }
        })
}
