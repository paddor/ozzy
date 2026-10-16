//! Authoritative record reads through validated derived index locations.

pub(crate) mod asynchronous;

use std::io::{self};

use bytes::Bytes;
use ozzy_journal::operation::{OperationCodecError, OperationKind, OperationLimits};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};
use smallvec::SmallVec;
use thiserror::Error;

use crate::{
    CodecError, DecodeLimits, ENTRY_HEADER_BYTES, IndexSource, OffsetIndexEntry, OperationLocation,
    SEGMENT_HEADER_BYTES, SegmentHeader, decode_indexed_operation, decode_segment_header,
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
) -> Result<(OperationKind, Bytes), IndexedReadError> {
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
    if !matches!(
        operation.kind,
        OperationKind::Append | OperationKind::ResolvedAppend
    ) {
        return Err(IndexedReadError::NotAppend);
    }
    let body = match operation.body {
        std::borrow::Cow::Borrowed(body) => shared_subslice(operation_bytes, body)?,
        std::borrow::Cow::Owned(body) => Bytes::from(body),
    };
    Ok((operation.kind, body))
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
