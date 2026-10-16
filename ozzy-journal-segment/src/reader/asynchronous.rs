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

/// One protected segment file and the bounds for decoding its extents.
pub(crate) struct ExtentReader<'a> {
    pub(crate) access: &'a Access,
    pub(crate) path: PathBuf,
    pub(crate) source: IndexSource,
    pub(crate) decode: DecodeLimits,
    pub(crate) operations: OperationLimits,
    pub(crate) chunk: usize,
}

impl ExtentReader<'_> {
    /// Revalidate a control-operation retry from exact indexed bytes. Returned body
    /// owns only its bounded decoded operation, not the surrounding file extent.
    pub(crate) async fn control(
        self,
        entry: OperationIndexEntry,
    ) -> Result<DecodedOperation<'static>, IndexedReadError> {
        let operations = self.operations;
        let operation = self.operation(entry.location).await?;
        if decode_operation_body(operation.kind, operation.body.as_ref(), operations)?
            .operation_id()
            != Some(entry.operation_id)
        {
            return Err(IndexedReadError::LocationMismatch);
        }
        Ok(operation)
    }

    /// Verify one numeric selector against authoritative framing and logical digest.
    /// The caller validates the returned operation body for its own purpose.
    pub(crate) async fn operation(
        self,
        location: OperationLocation,
    ) -> Result<DecodedOperation<'static>, IndexedReadError> {
        let location = validate_operation_location(self.source, location, self.decode)?;
        let (header, bytes) = self.extent(location).await?;
        let operation = decode_indexed_operation(
            &header,
            location.entry_offset,
            &bytes,
            location.op_number,
            self.decode,
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
        self,
        entry: OffsetIndexEntry,
    ) -> Result<super::CachedOperation, IndexedReadError> {
        let location = validate_location(self.source, entry, self.decode)?;
        let (header, bytes) = self.extent(location).await?;
        let (kind, body) = decode_body(&header, location, &Bytes::from(bytes), self.decode)?;
        super::CachedOperation::from_body(entry, kind, body, self.operations)
    }

    pub(crate) async fn record(
        self,
        entry: OffsetIndexEntry,
    ) -> Result<IndexedRecord, IndexedReadError> {
        self.records(&[entry])
            .await?
            .pop()
            .ok_or(IndexedReadError::InvalidSelector)
    }

    /// Read an enclosing operation once for all requested selectors. Returned
    /// parts own only their payload bytes, even when a byte-limited caller trims
    /// this result to a small prefix.
    pub(crate) async fn records(
        self,
        entries: &[OffsetIndexEntry],
    ) -> Result<Vec<IndexedRecord>, IndexedReadError> {
        let entry = *entries.first().ok_or(IndexedReadError::InvalidSelector)?;
        if entries
            .iter()
            .any(|other| other.location.operation != entry.location.operation)
        {
            return Err(IndexedReadError::InvalidLocation);
        }
        self.load(entry).await?.materialize(entries, true)
    }

    /// Load one exact indexed extent after its caller validated the location.
    async fn extent(
        &self,
        location: OperationLocation,
    ) -> Result<(SegmentHeader, Vec<u8>), IndexedReadError> {
        let file = self
            .access
            .read_handle(self.path.clone(), self.source)
            .await?;
        let length = self.access.length(&file).await?;
        let header_bytes = self
            .access
            .read_range(&file, 0, SEGMENT_HEADER_BYTES, self.chunk)
            .await?;
        let header = decode_segment_header(&header_bytes)?;
        validate_source_header(&header, self.source, length)?;
        let count =
            usize::try_from(location.entry_bytes).map_err(|_| IndexedReadError::InvalidLocation)?;
        let bytes = self
            .access
            .read_range(&file, location.entry_offset, count, self.chunk)
            .await?;
        Ok((header, bytes))
    }
}
