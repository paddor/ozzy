//! Authoritative record reads through validated derived index locations.

pub(crate) mod asynchronous;

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use bytes::Bytes;
use ozzy_journal::operation::{
    OperationBody, OperationCodecError, OperationLimits, decode_append_batches,
    decode_operation_body,
};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};
use smallvec::SmallVec;
use thiserror::Error;

use crate::{
    CodecError, DecodeLimits, ENTRY_HEADER_BYTES, IndexSource, MessageIndexEntry, OffsetIndexEntry,
    OperationLocation, SEGMENT_HEADER_BYTES, SegmentHeader, decode_indexed_operation,
    decode_segment_header,
};

mod cache;
pub(crate) use cache::{CachedOperation, ResidentOperations};
pub use cache::{
    DEFAULT_RESIDENT_BYTES, DecodedBatches, PreparedOperationRecords, RecordSpan, RecordView,
};

/// One canonical Append record copied from its authoritative segment entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedRecord {
    /// Payload representation.
    pub encoding: ozzy_proto::data::Encoding,
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Partition ownership fence at canonical admission.
    pub owner_epoch: OwnerEpoch,
    /// Producer identity scoped to this partition.
    pub producer_id: ProducerId,
    /// Producer-session fence at canonical admission.
    pub producer_epoch: ProducerEpoch,
    /// Producer-local record sequence.
    pub producer_sequence: ProducerSequence,
    /// Partition-global record offset.
    pub offset: Offset,
    /// Primary-resolved Unix timestamp in milliseconds.
    pub append_timestamp_millis: u64,
    /// Application record identity preserved through retry and replay.
    pub message_id: MessageId,
    /// Reference-counted opaque payload parts in record order.
    pub parts: SmallVec<[Bytes; 2]>,
}

impl IndexedRecord {
    /// Combined payload bytes in this selected record or span.
    pub fn payload_bytes(&self) -> usize {
        self.parts.iter().map(Bytes::len).sum()
    }
}

/// Read one offset location, revalidating authoritative entry bytes.
pub fn read_indexed_record(
    segment_path: impl AsRef<Path>,
    source: IndexSource,
    entry: OffsetIndexEntry,
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
) -> Result<IndexedRecord, IndexedReadError> {
    let mut records = read_indexed_records(
        segment_path,
        source,
        std::slice::from_ref(&entry),
        decode_limits,
        operation_limits,
    )?;
    let mut record = records.pop().ok_or(IndexedReadError::InvalidSelector)?;
    for part in &mut record.parts {
        *part = Bytes::copy_from_slice(part);
    }
    Ok(record)
}

/// Read selectors from one canonical operation, validating its bytes once.
pub(crate) fn read_indexed_records(
    segment_path: impl AsRef<Path>,
    source: IndexSource,
    entries: &[OffsetIndexEntry],
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
) -> Result<Vec<IndexedRecord>, IndexedReadError> {
    let first = *entries.first().ok_or(IndexedReadError::InvalidSelector)?;
    if entries
        .iter()
        .any(|entry| entry.location.operation != first.location.operation)
    {
        return Err(IndexedReadError::InvalidLocation);
    }
    with_indexed_operation(
        segment_path.as_ref(),
        source,
        first,
        decode_limits,
        operation_limits,
        |append, body, prepared_batches| {
            entries
                .iter()
                .map(|entry| select_indexed_record(append, body, prepared_batches, *entry))
                .collect()
        },
    )
}

fn with_indexed_operation<T>(
    segment_path: &Path,
    source: IndexSource,
    first: OffsetIndexEntry,
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
    inspect: impl FnOnce(
        &ozzy_journal::operation::Append<'_>,
        &Bytes,
        &[bool],
    ) -> Result<T, IndexedReadError>,
) -> Result<T, IndexedReadError> {
    with_indexed_append_body(segment_path, source, first, decode_limits, |body| {
        inspect_append_body(body, operation_limits, inspect)
    })
}

fn inspect_append_body<T>(
    body: &Bytes,
    operation_limits: OperationLimits,
    inspect: impl FnOnce(
        &ozzy_journal::operation::Append<'_>,
        &Bytes,
        &[bool],
    ) -> Result<T, IndexedReadError>,
) -> Result<T, IndexedReadError> {
    let prepared = decode_append_batches(body, operation_limits)?
        .iter()
        .map(|batch| batch.prepared_payload.is_some())
        .collect::<SmallVec<[_; 4]>>();
    let OperationBody::Append(append) = decode_operation_body(
        ozzy_journal::operation::OperationKind::Append,
        body,
        operation_limits,
    )?
    else {
        return Err(IndexedReadError::NotAppend);
    };
    inspect(&append, body, &prepared)
}

/// Validate the physical entry and its identity before decoding append records.
/// Callers must still validate the canonical append schema against their limits.
fn with_indexed_append_body<T>(
    segment_path: &Path,
    source: IndexSource,
    first: OffsetIndexEntry,
    decode_limits: DecodeLimits,
    inspect: impl FnOnce(&Bytes) -> Result<T, IndexedReadError>,
) -> Result<T, IndexedReadError> {
    let location = validate_location(source, first, decode_limits)?;
    let path = segment_path;
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(IndexedReadError::NotRegularFile);
    }
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.len() < source.valid_bytes {
        return Err(IndexedReadError::SourceMismatch);
    }
    let mut header_bytes = [0_u8; SEGMENT_HEADER_BYTES];
    file.read_exact(&mut header_bytes)?;
    let header = decode_segment_header(&header_bytes)?;
    validate_source_header(&header, source, metadata.len())?;
    let length =
        usize::try_from(location.entry_bytes).map_err(|_| IndexedReadError::InvalidLocation)?;
    let mut operation_bytes = vec![0_u8; length];
    file.seek(SeekFrom::Start(location.entry_offset))?;
    file.read_exact(&mut operation_bytes)?;
    let body = decode_body(
        &header,
        location,
        &Bytes::from(operation_bytes),
        decode_limits,
    )?;
    inspect(&body)
}

fn validate_location(
    source: IndexSource,
    first: OffsetIndexEntry,
    decode_limits: DecodeLimits,
) -> Result<OperationLocation, IndexedReadError> {
    validate_operation_location(source, first.location.operation, decode_limits)
}

fn validate_operation_location(
    source: IndexSource,
    location: OperationLocation,
    decode_limits: DecodeLimits,
) -> Result<OperationLocation, IndexedReadError> {
    let entry_end = location
        .entry_offset
        .checked_add(location.entry_bytes)
        .ok_or(IndexedReadError::InvalidLocation)?;
    if location.segment_id != source.segment_id
        || location.entry_offset < SEGMENT_HEADER_BYTES as u64
        || !location.entry_offset.is_multiple_of(8)
        || location.entry_bytes < ENTRY_HEADER_BYTES as u64
        || entry_end > source.valid_bytes
        || location.entry_bytes > decode_limits.max_indexed_entry_bytes() as u64
    {
        return Err(IndexedReadError::InvalidLocation);
    }
    Ok(location)
}

fn validate_source_header(
    header: &SegmentHeader,
    source: IndexSource,
    length: u64,
) -> Result<(), IndexedReadError> {
    if header.group_id() != source.group_id
        || header.segment_id() != source.segment_id
        || length > header.capacity()
        || length < source.valid_bytes
    {
        return Err(IndexedReadError::SourceMismatch);
    }
    Ok(())
}

fn decode_body(
    header: &SegmentHeader,
    location: OperationLocation,
    operation_bytes: &Bytes,
    decode_limits: DecodeLimits,
) -> Result<Bytes, IndexedReadError> {
    let operation = decode_indexed_operation(
        header,
        location.entry_offset,
        operation_bytes,
        location.op_number,
        decode_limits,
    )?;
    if operation.entry_bytes != location.entry_bytes
        || operation.op_number != location.op_number
        || operation.digest != location.operation_digest
    {
        return Err(IndexedReadError::LocationMismatch);
    }
    if operation.kind != ozzy_journal::operation::OperationKind::Append {
        return Err(IndexedReadError::NotAppend);
    }
    let body = match operation.body {
        std::borrow::Cow::Borrowed(body) => shared_subslice(operation_bytes, body)?,
        std::borrow::Cow::Owned(body) => Bytes::from(body),
    };
    Ok(body)
}

fn select_indexed_record(
    append: &ozzy_journal::operation::Append<'_>,
    body: &Bytes,
    prepared_batches: &[bool],
    entry: OffsetIndexEntry,
) -> Result<IndexedRecord, IndexedReadError> {
    let batch_index = entry.location.batch_index as usize;
    let batch = append
        .batches
        .get(batch_index)
        .ok_or(IndexedReadError::InvalidSelector)?;
    let prepared_payload = *prepared_batches
        .get(batch_index)
        .ok_or(IndexedReadError::InvalidSelector)?;
    let record_index = entry.location.record_index as usize;
    let record = batch
        .records
        .get(record_index)
        .ok_or(IndexedReadError::InvalidSelector)?;
    let delta = u64::try_from(record_index).map_err(|_| IndexedReadError::InvalidSelector)?;
    let offset = batch
        .first_offset
        .get()
        .checked_add(delta)
        .map(Offset::new)
        .ok_or(IndexedReadError::InvalidSelector)?;
    let producer_sequence = batch
        .first_sequence
        .get()
        .checked_add(delta)
        .map(ProducerSequence::new)
        .ok_or(IndexedReadError::InvalidSelector)?;
    if batch.partition != entry.partition || offset != entry.offset {
        return Err(IndexedReadError::RecordMismatch);
    }
    Ok(IndexedRecord {
        encoding: record.encoding,
        partition: batch.partition,
        owner_epoch: batch.owner_epoch,
        producer_id: batch.producer_id,
        producer_epoch: batch.producer_epoch,
        producer_sequence,
        offset,
        append_timestamp_millis: batch.append_timestamp_millis,
        message_id: record.message_id,
        parts: record
            .parts
            .iter()
            .map(|part| {
                if prepared_payload {
                    Ok(Bytes::copy_from_slice(part))
                } else {
                    shared_subslice(body, part)
                }
            })
            .collect::<Result<_, _>>()?,
    })
}

fn shared_subslice(bytes: &Bytes, slice: &[u8]) -> Result<Bytes, IndexedReadError> {
    Ok(bytes.slice(shared_range(bytes, slice)?))
}

fn shared_range(bytes: &Bytes, slice: &[u8]) -> Result<std::ops::Range<usize>, IndexedReadError> {
    let base = bytes.as_ptr() as usize;
    let start = (slice.as_ptr() as usize)
        .checked_sub(base)
        .ok_or(IndexedReadError::InvalidSelector)?;
    let end = start
        .checked_add(slice.len())
        .ok_or(IndexedReadError::InvalidSelector)?;
    if end > bytes.len() {
        return Err(IndexedReadError::InvalidSelector);
    }
    Ok(start..end)
}

/// Read one message-ID result after verifying both index sections agree.
pub fn read_indexed_message(
    segment_path: impl AsRef<Path>,
    source: IndexSource,
    offset_entry: OffsetIndexEntry,
    message_entry: MessageIndexEntry,
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
) -> Result<IndexedRecord, IndexedReadError> {
    if message_entry.partition != offset_entry.partition
        || message_entry.offset != offset_entry.offset
    {
        return Err(IndexedReadError::RecordMismatch);
    }
    let record = read_indexed_record(
        segment_path,
        source,
        offset_entry,
        decode_limits,
        operation_limits,
    )?;
    if record.message_id != message_entry.message_id
        || offset_entry.location.operation.operation_digest != message_entry.operation_digest
    {
        return Err(IndexedReadError::RecordMismatch);
    }
    Ok(record)
}

/// Indexed source, pointer, or authoritative entry failure.
#[derive(Debug, Error)]
pub enum IndexedReadError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error(transparent)]
    /// Physical segment framing or integrity validation failed.
    Codec(#[from] CodecError),
    #[error(transparent)]
    /// Canonical operation-body validation failed.
    Operation(#[from] OperationCodecError),
    #[error("segment path is not a regular file")]
    /// The named artifact is not a regular file.
    NotRegularFile,
    #[error("segment header or length does not match index source")]
    /// Segment header or length does not match index source.
    SourceMismatch,
    #[error("indexed physical location is outside source or configured limits")]
    /// Indexed physical location is outside source or configured limits.
    InvalidLocation,
    #[error("decoded operation identity does not match index location")]
    /// Decoded operation identity does not match index location.
    LocationMismatch,
    #[error("record index points to a non-Append operation")]
    /// Record index points to a non-Append operation.
    NotAppend,
    #[error("record selector is outside indexed Append operation")]
    /// Record selector is outside indexed Append operation.
    InvalidSelector,
    #[error("record identity or coordinates disagree with index")]
    /// Record identity or coordinates disagree with index.
    RecordMismatch,
}
