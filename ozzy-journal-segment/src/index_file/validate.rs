//! Index image validation: header, source, sections, rows, and operation
//! locations.

use super::{
    IndexFileError, IndexSource, MESSAGE_INDEX_ENTRY_BYTES, OFFSET_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES, SegmentIndexImage, SegmentIndexView,
    compare_message_entries, compare_offset_entries, compare_operation_entries,
    order::{
        binary_search_section, compare_offset_key, compare_partition_offset, raw_order_checks,
        sorted_entry_checks,
    },
    require_zero,
    rows::{
        decode_message_entry, decode_offset_entry, decode_operation_entry,
        decode_operation_location,
    },
};
use crate::{ENTRY_HEADER_BYTES, OperationLocation, SEGMENT_HEADER_BYTES};
use ozzy_journal::operation::Digest;
use ozzy_proto::layout::Fixed;

pub(super) fn validate_image(image: &SegmentIndexImage) -> Result<(), IndexFileError> {
    validate_image_header(image)?;
    for check in image_checks(image) {
        check?;
    }
    Ok(())
}

pub(super) fn validate_image_header(image: &SegmentIndexImage) -> Result<(), IndexFileError> {
    validate_source(image.source)?;
    validate_location_count(image.source, image.locations().len())?;
    if image.offsets().len() != image.messages().len() {
        return Err(IndexFileError::RecordIndexCountMismatch);
    }
    Ok(())
}

pub(super) fn image_checks(
    image: &SegmentIndexImage,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + '_ {
    sorted_entry_checks(image.offsets(), compare_offset_entries, "offset")
        .chain(sorted_entry_checks(
            image.messages(),
            compare_message_entries,
            "message",
        ))
        .chain(sorted_entry_checks(
            image.operations(),
            compare_operation_entries,
            "operation",
        ))
        .chain(image.offsets().iter().map(|entry| {
            if entry.location.record_index != 0 && entry.append_timestamp_millis != 0 {
                return Err(IndexFileError::NonZeroReserved);
            }
            validate_location(image.source, entry.location.operation)?;
            validate_indexed_location(entry.location.operation, |number| {
                image.find_operation_number(number)
            })?;
            Ok(OFFSET_INDEX_ENTRY_BYTES)
        }))
        .chain(image.operations().iter().map(|entry| {
            validate_location(image.source, entry.location)?;
            validate_indexed_location(entry.location, |number| {
                image.find_operation_number(number)
            })?;
            Ok(OPERATION_INDEX_ENTRY_BYTES)
        }))
        .chain(image.locations().iter().enumerate().map(|(at, &location)| {
            validate_numeric_location(image.source, at, location)?;
            if let Some(&previous) = at.checked_sub(1).and_then(|at| image.locations().get(at)) {
                validate_extent_order(previous, location)?;
            }
            Ok(OPERATION_LOCATION_BYTES)
        }))
        .chain(image.messages().iter().map(|message| {
            if image
                .offsets()
                .binary_search_by(|entry| {
                    compare_partition_offset(
                        entry.partition,
                        entry.offset,
                        message.partition,
                        message.offset,
                    )
                })
                .is_err()
            {
                return Err(IndexFileError::MissingOffsetEntry);
            }
            Ok(MESSAGE_INDEX_ENTRY_BYTES)
        }))
}

pub(super) fn validate_source(source: IndexSource) -> Result<(), IndexFileError> {
    if source.group_id.as_bytes().iter().all(|byte| *byte == 0)
        || source.segment_id == 0
        || source.valid_bytes < SEGMENT_HEADER_BYTES as u64
        || source.segment_digest == Digest::ZERO
        || source.first_op_number == 0
        || source.last_op_number < source.first_op_number
        || source.last_operation_digest == Digest::ZERO
    {
        return Err(IndexFileError::InvalidSource);
    }
    Ok(())
}

/// Check ordering first so later binary searches only see validated ordering.
/// The iterator retains no entry allocation and each step checks at most one
/// entry (including a logarithmic offset lookup for message entries).
pub(super) fn section_checks(
    view: SegmentIndexView<'_>,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + '_ {
    raw_order_checks::<OFFSET_INDEX_ENTRY_BYTES>(view.offsets, 24, "offset")
        .chain(raw_order_checks::<MESSAGE_INDEX_ENTRY_BYTES>(
            view.messages,
            40,
            "message",
        ))
        .chain(raw_order_checks::<OPERATION_INDEX_ENTRY_BYTES>(
            view.operations,
            16,
            "operation",
        ))
        .chain(offset_row_checks(view))
        .chain(message_row_checks(view))
        .chain(operation_row_checks(view))
        .chain(location_row_checks(view))
}

/// Reserved bytes and an indexed, in-source operation for each offset row.
fn offset_row_checks(
    view: SegmentIndexView<'_>,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + '_ {
    let source = view.source;
    let rows = view.offsets.as_chunks::<OFFSET_INDEX_ENTRY_BYTES>().0;
    rows.iter().map(move |entry| {
        if entry.u32_at::<84>() != 0 {
            require_zero(entry, 88..96)?;
        }
        let operation = decode_offset_entry(entry, source.segment_id)
            .location
            .operation;
        validate_location(source, operation)?;
        validate_indexed_location(operation, |number| view.find_operation_number(number))?;
        Ok(OFFSET_INDEX_ENTRY_BYTES)
    })
}

/// Reserved bytes and a matching offset row for each message row.
fn message_row_checks(
    view: SegmentIndexView<'_>,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + '_ {
    let rows = view.messages.as_chunks::<MESSAGE_INDEX_ENTRY_BYTES>().0;
    rows.iter().map(move |entry| {
        require_zero(entry, 72..80)?;
        let message = decode_message_entry(entry);
        if binary_search_section(view.offsets, |entry| {
            compare_offset_key(entry, message.partition, message.offset)
        })
        .is_none()
        {
            return Err(IndexFileError::MissingOffsetEntry);
        }
        Ok(MESSAGE_INDEX_ENTRY_BYTES)
    })
}

/// Reserved bytes and an indexed, in-source location for each operation row.
fn operation_row_checks(
    view: SegmentIndexView<'_>,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + '_ {
    let source = view.source;
    let rows = view.operations.as_chunks::<OPERATION_INDEX_ENTRY_BYTES>().0;
    rows.iter().map(move |entry| {
        require_zero(entry, 72..80)?;
        let location = decode_operation_entry(entry, source.segment_id).location;
        validate_location(source, location)?;
        validate_indexed_location(location, |number| view.find_operation_number(number))?;
        Ok(OPERATION_INDEX_ENTRY_BYTES)
    })
}

/// Dense numbering and nonoverlapping extents across the location rows.
fn location_row_checks(
    view: SegmentIndexView<'_>,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + '_ {
    let source = view.source;
    let rows = view.locations.as_chunks::<OPERATION_LOCATION_BYTES>().0;
    rows.iter().enumerate().map(move |(at, entry)| {
        validate_numeric_location(
            source,
            at,
            decode_operation_location(entry, source.segment_id),
        )?;
        if let Some(previous) = at.checked_sub(1).and_then(|at| rows.get(at)) {
            validate_extent_order(
                decode_operation_location(previous, source.segment_id),
                decode_operation_location(entry, source.segment_id),
            )?;
        }
        Ok(OPERATION_LOCATION_BYTES)
    })
}

pub(super) fn validate_location_count(
    source: IndexSource,
    count: usize,
) -> Result<(), IndexFileError> {
    if u64::try_from(count).ok()
        != source
            .last_op_number
            .checked_sub(source.first_op_number)
            .and_then(|n| n.checked_add(1))
    {
        return Err(IndexFileError::InvalidLocation);
    }
    Ok(())
}

fn validate_numeric_location(
    source: IndexSource,
    at: usize,
    location: OperationLocation,
) -> Result<(), IndexFileError> {
    if source.first_op_number.checked_add(at as u64) != Some(location.op_number) {
        return Err(IndexFileError::InvalidLocation);
    }
    validate_location(source, location)
}

fn validate_indexed_location(
    location: OperationLocation,
    find: impl FnOnce(u64) -> Option<OperationLocation>,
) -> Result<(), IndexFileError> {
    if find(location.op_number) != Some(location) {
        return Err(IndexFileError::InvalidLocation);
    }
    Ok(())
}

fn validate_extent_order(
    previous: OperationLocation,
    next: OperationLocation,
) -> Result<(), IndexFileError> {
    let shared =
        previous.entry_offset == next.entry_offset && previous.entry_bytes == next.entry_bytes;
    if !shared
        && previous
            .entry_offset
            .checked_add(previous.entry_bytes)
            .is_none_or(|end| end > next.entry_offset)
    {
        return Err(IndexFileError::InvalidLocation);
    }
    Ok(())
}

fn validate_location(
    source: IndexSource,
    location: OperationLocation,
) -> Result<(), IndexFileError> {
    let end = location
        .entry_offset
        .checked_add(location.entry_bytes)
        .ok_or(IndexFileError::LengthOverflow)?;
    if location.segment_id != source.segment_id
        || location.operation_digest == Digest::ZERO
        || location.entry_offset < SEGMENT_HEADER_BYTES as u64
        || !location.entry_offset.is_multiple_of(8)
        || location.entry_bytes < ENTRY_HEADER_BYTES as u64
        || !location.entry_bytes.is_multiple_of(8)
        || end > source.valid_bytes
        || location.op_number < source.first_op_number
        || location.op_number > source.last_op_number
        || (location.op_number == source.last_op_number
            && location.operation_digest != source.last_operation_digest)
    {
        return Err(IndexFileError::InvalidLocation);
    }
    Ok(())
}
