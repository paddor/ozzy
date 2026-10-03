//! Exact derived-index entries extracted from validated segment operations.

use ozzy_journal::operation::{
    Digest, OperationBody, OperationCodecError, OperationLimits, decode_operation_body,
};
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::{DecodedOperation, SegmentHeader};

/// Physical extent and logical identity of one canonical operation. Operations
/// in a shared compressed group use the same extent and distinct numbers/digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationLocation {
    pub segment_id: u64,
    pub entry_offset: u64,
    pub entry_bytes: u64,
    pub op_number: u64,
    pub operation_digest: Digest,
}

/// Physical operation plus record selectors within its decoded Append body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordLocation {
    pub operation: OperationLocation,
    pub batch_index: u32,
    pub record_index: u32,
}

/// Exact partition-offset lookup entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetIndexEntry {
    pub partition: PartitionIncarnation,
    pub offset: Offset,
    pub location: RecordLocation,
}

/// Exact idempotency lookup entry. Resolve its offset through the offset index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageIndexEntry {
    pub partition: PartitionIncarnation,
    pub message_id: MessageId,
    pub offset: Offset,
    pub operation_digest: Digest,
}

/// Exact control-operation retry lookup entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationIndexEntry {
    pub operation_id: OperationId,
    pub location: OperationLocation,
}

/// Entries derived from one canonical operation without retaining its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedIndexEntries {
    pub offsets: Vec<OffsetIndexEntry>,
    pub messages: Vec<MessageIndexEntry>,
    pub operation: Option<OperationIndexEntry>,
}

/// Decode one bounded operation and derive exact disposable index entries.
pub fn derive_index_entries(
    segment: &SegmentHeader,
    operation: &DecodedOperation<'_>,
    limits: OperationLimits,
) -> Result<DerivedIndexEntries, IndexError> {
    if segment.group_id() != operation.group_id {
        return Err(IndexError::SourceMismatch);
    }
    let body = decode_operation_body(operation.kind, operation.body.as_ref(), limits)?;
    let location = OperationLocation {
        segment_id: segment.segment_id(),
        entry_offset: operation.entry_offset,
        entry_bytes: operation.entry_bytes,
        op_number: operation.op_number,
        operation_digest: operation.digest,
    };
    let operation_entry = operation_id(&body).map(|operation_id| OperationIndexEntry {
        operation_id,
        location,
    });
    let OperationBody::Append(append) = body else {
        return Ok(DerivedIndexEntries {
            offsets: Vec::new(),
            messages: Vec::new(),
            operation: operation_entry,
        });
    };

    let record_count = append
        .batches
        .iter()
        .try_fold(0_usize, |count, batch| {
            count.checked_add(batch.records.len())
        })
        .ok_or(IndexError::PositionOverflow)?;
    let mut offsets = Vec::with_capacity(record_count);
    let mut messages = Vec::with_capacity(record_count);
    for (batch_index, batch) in append.batches.iter().enumerate() {
        let batch_index = u32::try_from(batch_index).map_err(|_| IndexError::PositionOverflow)?;
        for (record_index, record) in batch.records.iter().enumerate() {
            let delta = u64::try_from(record_index).map_err(|_| IndexError::PositionOverflow)?;
            let offset = batch
                .first_offset
                .get()
                .checked_add(delta)
                .map(Offset::new)
                .ok_or(IndexError::PositionOverflow)?;
            let record_index =
                u32::try_from(record_index).map_err(|_| IndexError::PositionOverflow)?;
            offsets.push(OffsetIndexEntry {
                partition: batch.partition,
                offset,
                location: RecordLocation {
                    operation: location,
                    batch_index,
                    record_index,
                },
            });
            messages.push(MessageIndexEntry {
                partition: batch.partition,
                message_id: record.message_id,
                offset,
                operation_digest: operation.digest,
            });
        }
    }
    Ok(DerivedIndexEntries {
        offsets,
        messages,
        operation: None,
    })
}

pub(crate) fn operation_id(body: &OperationBody<'_>) -> Option<OperationId> {
    body.operation_id()
}

/// Derived-index extraction failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IndexError {
    #[error(transparent)]
    Operation(#[from] OperationCodecError),
    #[error("decoded operation does not belong to the source segment")]
    SourceMismatch,
    #[error("derived index position overflow")]
    PositionOverflow,
}
