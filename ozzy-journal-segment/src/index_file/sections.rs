//! Index file sections: counts, byte offsets, and total size.

use super::{
    INDEX_HEADER_BYTES, IndexFileError, IndexLimits, MESSAGE_INDEX_ENTRY_BYTES,
    OFFSET_INDEX_ENTRY_BYTES, OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES,
    SCHEDULE_INDEX_ENTRY_BYTES, enforce_count, validate_limits,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexCounts {
    pub offsets: usize,
    pub messages: usize,
    pub operations: usize,
    pub locations: usize,
    pub schedules: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexLayout {
    pub message_start: usize,
    pub operation_start: usize,
    pub location_start: usize,
    pub schedule_start: usize,
    pub file_bytes: usize,
}

/// An index file's sections after its header, in file order.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Sections<T> {
    pub(super) offsets: T,
    pub(super) messages: T,
    pub(super) operations: T,
    pub(super) locations: T,
    pub(super) schedules: T,
}

impl IndexLayout {
    /// Section lengths after the header, in file order.
    pub(crate) fn section_lengths(&self) -> Option<[usize; 5]> {
        Some([
            self.message_start.checked_sub(INDEX_HEADER_BYTES)?,
            self.operation_start.checked_sub(self.message_start)?,
            self.location_start.checked_sub(self.operation_start)?,
            self.schedule_start.checked_sub(self.location_start)?,
            self.file_bytes.checked_sub(self.schedule_start)?,
        ])
    }

    /// Split a whole index file into its header and sections. `None` unless
    /// `bytes` has exactly this layout.
    pub(super) fn split<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Option<(&'a [u8; INDEX_HEADER_BYTES], Sections<&'a [u8]>)> {
        let [
            offset_bytes,
            message_bytes,
            operation_bytes,
            location_bytes,
            schedule_bytes,
        ] = self.section_lengths()?;
        let (header, rest) = bytes.split_first_chunk::<INDEX_HEADER_BYTES>()?;
        let (offsets, rest) = rest.split_at_checked(offset_bytes)?;
        let (messages, rest) = rest.split_at_checked(message_bytes)?;
        let (operations, rest) = rest.split_at_checked(operation_bytes)?;
        let (locations, schedules) = rest.split_at_checked(location_bytes)?;
        (schedules.len() == schedule_bytes).then_some((
            header,
            Sections {
                offsets,
                messages,
                operations,
                locations,
                schedules,
            },
        ))
    }

    /// Mutable form of [`Self::split`].
    pub(super) fn split_mut<'a>(
        &self,
        bytes: &'a mut [u8],
    ) -> Option<(&'a mut [u8; INDEX_HEADER_BYTES], Sections<&'a mut [u8]>)> {
        let [
            offset_bytes,
            message_bytes,
            operation_bytes,
            location_bytes,
            schedule_bytes,
        ] = self.section_lengths()?;
        let (header, rest) = bytes.split_first_chunk_mut::<INDEX_HEADER_BYTES>()?;
        let (offsets, rest) = rest.split_at_mut_checked(offset_bytes)?;
        let (messages, rest) = rest.split_at_mut_checked(message_bytes)?;
        let (operations, rest) = rest.split_at_mut_checked(operation_bytes)?;
        let (locations, schedules) = rest.split_at_mut_checked(location_bytes)?;
        (schedules.len() == schedule_bytes).then_some((
            header,
            Sections {
                offsets,
                messages,
                operations,
                locations,
                schedules,
            },
        ))
    }
}

fn section_bytes(count: usize, width: usize) -> Result<usize, IndexFileError> {
    count
        .checked_mul(width)
        .ok_or(IndexFileError::LengthOverflow)
}

pub(crate) fn calculate_index_layout(
    counts: IndexCounts,
    limits: IndexLimits,
) -> Result<IndexLayout, IndexFileError> {
    validate_limits(limits)?;
    enforce_count("offset entries", counts.offsets, limits.max_offset_entries)?;
    enforce_count(
        "message entries",
        counts.messages,
        limits.max_message_entries,
    )?;
    enforce_count(
        "operation entries",
        counts.operations,
        limits.max_operation_entries,
    )?;
    if counts.offsets != counts.messages {
        return Err(IndexFileError::RecordIndexCountMismatch);
    }
    enforce_count(
        "operation locations",
        counts.locations,
        limits.max_operation_entries,
    )?;
    enforce_count(
        "schedule entries",
        counts.schedules,
        limits.max_schedule_entries,
    )?;
    let message_start = INDEX_HEADER_BYTES
        .checked_add(section_bytes(counts.offsets, OFFSET_INDEX_ENTRY_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    let operation_start = message_start
        .checked_add(section_bytes(counts.messages, MESSAGE_INDEX_ENTRY_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    let location_start = operation_start
        .checked_add(section_bytes(
            counts.operations,
            OPERATION_INDEX_ENTRY_BYTES,
        )?)
        .ok_or(IndexFileError::LengthOverflow)?;
    let schedule_start = location_start
        .checked_add(section_bytes(counts.locations, OPERATION_LOCATION_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    let file_bytes = schedule_start
        .checked_add(section_bytes(counts.schedules, SCHEDULE_INDEX_ENTRY_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    enforce_count("index file bytes", file_bytes, limits.max_file_bytes)?;
    Ok(IndexLayout {
        message_start,
        operation_start,
        location_start,
        schedule_start,
        file_bytes,
    })
}
