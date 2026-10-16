//! Fixed-width index rows: the file header, entry rows, and operation
//! locations.

use super::{
    INDEX_HEADER_BYTES, INDEX_MAGIC, INDEX_VERSION, IndexCounts, IndexFileError, IndexLayout,
    IndexSource, MESSAGE_INDEX_ENTRY_BYTES, OFFSET_INDEX_ENTRY_BYTES, OPERATION_INDEX_ENTRY_BYTES,
    OPERATION_LOCATION_BYTES, u64_from_usize,
};
use crate::{
    MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry, OperationLocation, RecordLocation,
};
use ozzy_journal::operation::Digest;
use ozzy_proto::layout::Fixed;
use ozzy_proto::{GroupId, MessageId, Offset, OperationId, PartitionIncarnation};

pub(crate) fn encode_index_header(
    source: IndexSource,
    build_memory_limit: u64,
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
    header.put_u64::<128>(u64_from_usize(counts.offsets)?);
    header.put_u64::<136>(INDEX_HEADER_BYTES as u64);
    header.put_u32::<144>(OFFSET_INDEX_ENTRY_BYTES as u32);
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
    Ok(header)
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

pub(crate) fn encode_offset_entry(
    entry: &OffsetIndexEntry,
    output: &mut [u8; OFFSET_INDEX_ENTRY_BYTES],
) {
    output[..16].copy_from_slice(entry.partition.as_bytes());
    output.put_u64::<16>(entry.offset.get());
    output.put::<24, 56>(location_bytes(entry.location.operation));
    output.put_u32::<80>(entry.location.batch_index);
    output.put_u32::<84>(entry.location.record_index);
    output.put_u64::<88>(entry.append_timestamp_millis);
}

pub(super) fn decode_offset_entry(
    input: &[u8; OFFSET_INDEX_ENTRY_BYTES],
    segment_id: u64,
) -> OffsetIndexEntry {
    OffsetIndexEntry {
        partition: PartitionIncarnation::from_bytes(input.array::<0, 16>()),
        offset: Offset::new(input.u64_at::<16>()),
        append_timestamp_millis: input.u64_at::<88>(),
        location: RecordLocation {
            operation: read_location(input.array::<24, 56>(), segment_id),
            batch_index: input.u32_at::<80>(),
            record_index: input.u32_at::<84>(),
        },
    }
}

pub(crate) fn encode_message_entry(
    entry: &MessageIndexEntry,
    output: &mut [u8; MESSAGE_INDEX_ENTRY_BYTES],
) {
    output[..16].copy_from_slice(entry.partition.as_bytes());
    output.put::<16, 16>(*entry.message_id.as_bytes());
    output.put_u64::<32>(entry.offset.get());
    output.put::<40, 32>(*entry.operation_digest.as_bytes());
}

pub(super) fn decode_message_entry(input: &[u8; MESSAGE_INDEX_ENTRY_BYTES]) -> MessageIndexEntry {
    MessageIndexEntry {
        partition: PartitionIncarnation::from_bytes(input.array::<0, 16>()),
        message_id: MessageId::from_bytes(input.array::<16, 16>()),
        offset: Offset::new(input.u64_at::<32>()),
        operation_digest: Digest::from_bytes(input.array::<40, 32>()),
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

/// A record's operation location as stored in its offset entry.
pub(super) fn location_bytes(location: OperationLocation) -> [u8; 56] {
    let mut bytes = [0; 56];
    bytes.put_u64::<0>(location.entry_offset);
    bytes.put_u64::<8>(location.entry_bytes);
    bytes.put_u64::<16>(location.op_number);
    bytes.put::<24, 32>(*location.operation_digest.as_bytes());
    bytes
}

fn read_location(bytes: [u8; 56], segment_id: u64) -> OperationLocation {
    OperationLocation {
        segment_id,
        entry_offset: bytes.u64_at::<0>(),
        entry_bytes: bytes.u64_at::<8>(),
        op_number: bytes.u64_at::<16>(),
        operation_digest: Digest::from_bytes(bytes.array::<24, 32>()),
    }
}
