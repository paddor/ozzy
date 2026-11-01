//! Fixed-width index rows: the file header, entry rows, and operation
//! locations.

use super::{
    BATCH_INDEX_ENTRY_BYTES, INDEX_HEADER_BYTES, INDEX_MAGIC, INDEX_VERSION, IndexCounts,
    IndexFileError, IndexLayout, IndexSource, MESSAGE_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES, SCHEDULE_INDEX_ENTRY_BYTES,
    u64_from_usize,
};
use crate::{
    BatchIndexEntry, MessageRow, OperationIndexEntry, OperationLocation, ScheduleIndexEntry,
};
use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::{Digest, ReleaseKey};
use ozzy_proto::layout::Fixed;
use ozzy_proto::{GroupId, MessageId, Offset, OperationId, PartitionIncarnation};

/// What batch and message rows count from, so they need not repeat it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowBase {
    /// Partition of every record; `None` without records.
    pub partition: Option<PartitionIncarnation>,
    /// Offset of the first record, which message rows count from; zero without
    /// records.
    pub first_offset: u64,
    /// First operation of the source, which batch rows count from.
    pub first_op_number: u64,
}

pub(crate) fn encode_index_header(
    source: IndexSource,
    build_memory_limit: u64,
    base: RowBase,
    counts: IndexCounts,
    layout: IndexLayout,
) -> Result<[u8; INDEX_HEADER_BYTES], IndexFileError> {
    let mut header = [0_u8; INDEX_HEADER_BYTES];
    header[..8].copy_from_slice(INDEX_MAGIC);
    header.put_u16::<8>(INDEX_VERSION);
    header.put_u16::<10>(INDEX_HEADER_BYTES as u16);
    header.put::<16, 16>(*source.group_id.as_bytes());
    header.put_u64::<32>(source.segment_id);
    header.put_u64::<40>(source.valid_bytes);
    header.put::<48, 32>(*source.segment_digest.as_bytes());
    header.put_u64::<80>(source.first_op_number);
    header.put_u64::<88>(source.last_op_number);
    header.put::<96, 32>(*source.last_operation_digest.as_bytes());
    header.put_u64::<128>(u64_from_usize(counts.batches)?);
    header.put_u64::<136>(INDEX_HEADER_BYTES as u64);
    header.put_u32::<144>(BATCH_INDEX_ENTRY_BYTES as u32);
    header.put_u64::<152>(u64_from_usize(counts.messages)?);
    header.put_u64::<160>(u64_from_usize(layout.message_start)?);
    header.put_u32::<168>(MESSAGE_INDEX_ENTRY_BYTES as u32);
    header.put_u64::<176>(u64_from_usize(counts.operations)?);
    header.put_u64::<184>(u64_from_usize(layout.operation_start)?);
    header.put_u32::<192>(OPERATION_INDEX_ENTRY_BYTES as u32);
    header.put_u64::<200>(u64_from_usize(layout.file_bytes)?);
    header.put_u64::<208>(build_memory_limit);
    header.put_u64::<216>(u64_from_usize(counts.locations)?);
    header.put_u64::<224>(u64_from_usize(layout.location_start)?);
    header.put_u32::<232>(OPERATION_LOCATION_BYTES as u32);
    header.put_u64::<288>(u64_from_usize(counts.schedules)?);
    header.put_u64::<296>(u64_from_usize(layout.schedule_start)?);
    header.put_u32::<304>(SCHEDULE_INDEX_ENTRY_BYTES as u32);
    header.put_u64::<312>(base.first_offset);
    if let Some(partition) = base.partition {
        header.put::<320, 16>(*partition.as_bytes());
    }
    Ok(header)
}

pub(super) fn decode_row_base(header: &[u8; INDEX_HEADER_BYTES], first_op_number: u64) -> RowBase {
    let partition = header.array::<320, 16>();
    RowBase {
        partition: (partition != [0; 16]).then(|| PartitionIncarnation::from_bytes(partition)),
        first_offset: header.u64_at::<312>(),
        first_op_number,
    }
}

pub(super) fn decode_source(header: &[u8; INDEX_HEADER_BYTES]) -> IndexSource {
    IndexSource {
        group_id: GroupId::from_bytes(header.array::<16, 16>()),
        segment_id: header.u64_at::<32>(),
        valid_bytes: header.u64_at::<40>(),
        segment_digest: Digest::from_bytes(header.array::<48, 32>()),
        first_op_number: header.u64_at::<80>(),
        last_op_number: header.u64_at::<88>(),
        last_operation_digest: Digest::from_bytes(header.array::<96, 32>()),
    }
}

pub(crate) fn encode_batch_entry(
    entry: &BatchIndexEntry,
    base: RowBase,
    output: &mut [u8; BATCH_INDEX_ENTRY_BYTES],
) {
    output.put_u64::<0>(entry.first_offset.get());
    output.put_u64::<8>(entry.append_timestamp_millis);
    output.put_u32::<16>(entry.op_number.wrapping_sub(base.first_op_number) as u32);
    output.put_u32::<20>(entry.batch_index);
    output.put_u32::<24>(entry.records);
}

pub(super) fn decode_batch_entry(
    input: &[u8; BATCH_INDEX_ENTRY_BYTES],
    first_op_number: u64,
) -> BatchIndexEntry {
    BatchIndexEntry {
        first_offset: Offset::new(input.u64_at::<0>()),
        append_timestamp_millis: input.u64_at::<8>(),
        op_number: first_op_number.saturating_add(u64::from(input.u32_at::<16>())),
        batch_index: input.u32_at::<20>(),
        records: input.u32_at::<24>(),
    }
}

pub(crate) fn encode_message_row(
    entry: &MessageRow,
    base: RowBase,
    output: &mut [u8; MESSAGE_INDEX_ENTRY_BYTES],
) {
    output[..16].copy_from_slice(entry.message_id.as_bytes());
    output.put_u32::<16>(entry.offset.get().wrapping_sub(base.first_offset) as u32);
}

pub(super) fn decode_message_row(
    input: &[u8; MESSAGE_INDEX_ENTRY_BYTES],
    first_offset: u64,
) -> MessageRow {
    MessageRow {
        message_id: MessageId::from_bytes(input.array::<0, 16>()),
        offset: Offset::new(first_offset.saturating_add(u64::from(input.u32_at::<16>()))),
    }
}

pub(crate) fn encode_operation_entry(
    entry: &OperationIndexEntry,
    output: &mut [u8; OPERATION_INDEX_ENTRY_BYTES],
) {
    output[..16].copy_from_slice(entry.operation_id.as_bytes());
    output.put_u64::<16>(entry.location.op_number);
    output.put_u64::<24>(entry.location.entry_offset);
    output.put_u64::<32>(entry.location.entry_bytes);
    output.put::<40, 32>(*entry.location.operation_digest.as_bytes());
}

pub(crate) fn encode_operation_location(
    location: &OperationLocation,
    output: &mut [u8; OPERATION_LOCATION_BYTES],
) {
    output.put_u64::<0>(location.op_number);
    output.put_u64::<8>(location.entry_offset);
    output.put_u64::<16>(location.entry_bytes);
    output.put::<24, 32>(*location.operation_digest.as_bytes());
}

pub(super) fn decode_operation_location(
    input: &[u8; OPERATION_LOCATION_BYTES],
    segment_id: u64,
) -> OperationLocation {
    OperationLocation {
        segment_id,
        op_number: input.u64_at::<0>(),
        entry_offset: input.u64_at::<8>(),
        entry_bytes: input.u64_at::<16>(),
        operation_digest: Digest::from_bytes(input.array::<24, 32>()),
    }
}

pub(super) fn decode_operation_entry(
    input: &[u8; OPERATION_INDEX_ENTRY_BYTES],
    segment_id: u64,
) -> OperationIndexEntry {
    OperationIndexEntry {
        operation_id: OperationId::from_bytes(input.array::<0, 16>()),
        location: OperationLocation {
            segment_id,
            op_number: input.u64_at::<16>(),
            entry_offset: input.u64_at::<24>(),
            entry_bytes: input.u64_at::<32>(),
            operation_digest: Digest::from_bytes(input.array::<40, 32>()),
        },
    }
}

pub(crate) fn encode_schedule_entry(
    entry: &ScheduleIndexEntry,
    output: &mut [u8; SCHEDULE_INDEX_ENTRY_BYTES],
) {
    output[..16].copy_from_slice(entry.partition.as_bytes());
    output.put_u64::<16>(entry.key.millis);
    output.put_u64::<24>(entry.key.schedule);
    output.put_u64::<32>(entry.record.operation);
    output.put_u32::<40>(entry.record.index);
    output.put_u32::<44>(entry.record.bytes);
    output.put::<48, 16>(*entry.record.message_id.as_bytes());
}

pub(super) fn decode_schedule_entry(
    input: &[u8; SCHEDULE_INDEX_ENTRY_BYTES],
) -> ScheduleIndexEntry {
    ScheduleIndexEntry {
        partition: PartitionIncarnation::from_bytes(input.array::<0, 16>()),
        key: ReleaseKey {
            millis: input.u64_at::<16>(),
            schedule: input.u64_at::<24>(),
        },
        record: PendingSchedule {
            operation: input.u64_at::<32>(),
            index: input.u32_at::<40>(),
            bytes: input.u32_at::<44>(),
            message_id: MessageId::from_bytes(input.array::<48, 16>()),
        },
    }
}
