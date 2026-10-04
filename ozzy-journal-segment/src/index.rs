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
    /// Physical segment identity.
    pub segment_id: u64,
    /// First physical byte offset of this canonical operation entry.
    pub entry_offset: u64,
    /// Encoded physical bytes occupied by this operation extent.
    pub entry_bytes: u64,
    /// Partition-local canonical operation number.
    pub op_number: u64,
    /// Canonical logical digest of the selected operation.
    pub operation_digest: Digest,
}

/// Physical operation plus record selectors within its decoded Append body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordLocation {
    /// Canonical operation selected for replay or indexing.
    pub operation: OperationLocation,
    /// Zero-based addressed batch within the canonical APPEND.
    pub batch_index: u32,
    /// Zero-based record within that APPEND batch.
    pub record_index: u32,
}

/// Exact partition-offset lookup entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetIndexEntry {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Partition-global record offset.
    pub offset: Offset,
    /// Broker append time on the first record of a batch; zero on other rows.
    /// Uses the offset table's existing eight reserved bytes.
    pub append_timestamp_millis: u64,
    /// Exact physical operation and record coordinates.
    pub location: RecordLocation,
}

/// Exact idempotency lookup entry. Resolve its offset through the offset index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageIndexEntry {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Application record identity preserved through retry and replay.
    pub message_id: MessageId,
    /// Partition-global record offset.
    pub offset: Offset,
    /// Canonical logical digest of the selected operation.
    pub operation_digest: Digest,
}

/// Exact control-operation retry lookup entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationIndexEntry {
    /// Exact canonical control-operation retry identity.
    pub operation_id: OperationId,
    /// Exact physical operation and record coordinates.
    pub location: OperationLocation,
}

/// Entries derived from one canonical operation without retaining its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedIndexEntries {
    /// Record-offset selectors in canonical sorted order.
    pub offsets: Vec<OffsetIndexEntry>,
    /// Message-identity selectors in canonical sorted order.
    pub messages: Vec<MessageIndexEntry>,
    /// Optional canonical control-operation identity selector.
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
                append_timestamp_millis: if record_index == 0 {
                    batch.append_timestamp_millis
                } else {
                    0
                },
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
    /// Canonical operation-body validation failed.
    Operation(#[from] OperationCodecError),
    #[error("decoded operation does not belong to the source segment")]
    /// Decoded operation does not belong to the source segment.
    SourceMismatch,
    #[error("derived index position overflow")]
    /// Derived index position overflow.
    PositionOverflow,
}
