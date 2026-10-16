use super::{
    Bytes, DecodeLimits, IndexSource, IndexedReadError, IndexedRecord, OffsetIndexEntry,
    OperationLimits, SEGMENT_HEADER_BYTES, decode_body, decode_segment_header, validate_location,
    validate_operation_location, validate_source_header,
};
use crate::async_files::Access;
use crate::{
    DecodedOperation, OperationIndexEntry, OperationLocation, SegmentHeader,
    decode_indexed_operation,
};
use ozzy_journal::operation::decode_operation_body;
use std::path::PathBuf;

/// Revalidate a control-operation retry from exact indexed bytes. Returned body
/// owns only its bounded decoded operation, not the surrounding file extent.
pub(crate) async fn read_control(
    access: &Access,
    path: PathBuf,
    source: IndexSource,
    entry: OperationIndexEntry,
    decode: DecodeLimits,
    operations: OperationLimits,
    chunk: usize,
) -> Result<DecodedOperation<'static>, IndexedReadError> {
    let operation = read_operation(access, path, source, entry.location, decode, chunk).await?;
    if decode_operation_body(operation.kind, operation.body.as_ref(), operations)?.operation_id()
        != Some(entry.operation_id)
    {
        return Err(IndexedReadError::LocationMismatch);
    }
    Ok(operation)
}

/// Verify one numeric selector against authoritative framing and logical digest.
/// The caller validates the returned operation body for its own purpose.
pub(crate) async fn read_operation(
    access: &Access,
    path: PathBuf,
    source: IndexSource,
    location: OperationLocation,
    decode: DecodeLimits,
    chunk: usize,
) -> Result<DecodedOperation<'static>, IndexedReadError> {
    let location = validate_operation_location(source, location, decode)?;
    let (header, bytes) = extent(access, path, source, location, chunk).await?;
    let operation = decode_indexed_operation(
        &header,
        location.entry_offset,
        &bytes,
        location.op_number,
        decode,
    )?;
    if operation.entry_bytes != location.entry_bytes
        || operation.op_number != location.op_number
        || operation.digest != location.operation_digest
    {
        return Err(IndexedReadError::LocationMismatch);
    }
    Ok(DecodedOperation {
        body: std::borrow::Cow::Owned(operation.body.into_owned()),
        ..operation
    })
}

pub(crate) async fn load(
    access: &Access,
    path: PathBuf,
    source: IndexSource,
    entry: OffsetIndexEntry,
    decode: DecodeLimits,
    operations: OperationLimits,
    chunk: usize,
) -> Result<super::CachedOperation, IndexedReadError> {
    let location = validate_location(source, entry, decode)?;
    let (header, bytes) = extent(access, path, source, location, chunk).await?;
    let (kind, body) = decode_body(&header, location, &Bytes::from(bytes), decode)?;
    super::CachedOperation::from_body(entry, kind, body, operations)
}

pub(crate) async fn read_record(
    access: &Access,
    path: PathBuf,
    source: IndexSource,
    entry: OffsetIndexEntry,
    decode: DecodeLimits,
    operations: OperationLimits,
    chunk: usize,
) -> Result<IndexedRecord, IndexedReadError> {
    read_records(access, path, source, &[entry], decode, operations, chunk)
        .await?
        .pop()
        .ok_or(IndexedReadError::InvalidSelector)
}

/// Read an enclosing operation once for all requested selectors. Returned
/// parts own only their payload bytes, even when a byte-limited caller trims
/// this result to a small prefix.
pub(crate) async fn read_records(
    access: &Access,
    path: PathBuf,
    source: IndexSource,
    entries: &[OffsetIndexEntry],
    decode: DecodeLimits,
    operations: OperationLimits,
    chunk: usize,
) -> Result<Vec<IndexedRecord>, IndexedReadError> {
    let entry = *entries.first().ok_or(IndexedReadError::InvalidSelector)?;
    if entries
        .iter()
        .any(|other| other.location.operation != entry.location.operation)
    {
        return Err(IndexedReadError::InvalidLocation);
    }
    let location = validate_location(source, entry, decode)?;
    let (header, bytes) = extent(access, path, source, location, chunk).await?;
    let (kind, body) = decode_body(&header, location, &Bytes::from(bytes), decode)?;
    super::CachedOperation::from_body(entry, kind, body, operations)?.materialize(entries, true)
}

/// Load one exact indexed extent after its caller validated the location.
async fn extent(
    access: &Access,
    path: PathBuf,
    source: IndexSource,
    location: OperationLocation,
    chunk: usize,
) -> Result<(SegmentHeader, Vec<u8>), IndexedReadError> {
    let file = access.read_handle(path, source).await?;
    let length = access.length(&file).await?;
    let header_bytes = access
        .read_range(&file, 0, SEGMENT_HEADER_BYTES, chunk)
        .await?;
    let header = decode_segment_header(&header_bytes)?;
    validate_source_header(&header, source, length)?;
    let count =
        usize::try_from(location.entry_bytes).map_err(|_| IndexedReadError::InvalidLocation)?;
    let bytes = access
        .read_range(&file, location.entry_offset, count, chunk)
        .await?;
    Ok((header, bytes))
}
