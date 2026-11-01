//! Exact derived-index entries extracted from validated segment operations.

use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::{
    AppendBatch, Digest, OperationBody, OperationCodecError, OperationLimits, ReleaseKey,
    decode_operation_body,
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

/// One batch's record range. Its records share everything here, so an index
/// holds one of these per batch, not per record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchIndexEntry {
    /// Offset of the batch's first record.
    pub first_offset: Offset,
    /// Broker append time of the batch.
    pub append_timestamp_millis: u64,
    /// Canonical operation holding the batch.
    pub op_number: u64,
    /// Zero-based addressed batch within the canonical APPEND.
    pub batch_index: u32,
    /// Records in the batch, at least one.
    pub records: u32,
}

impl BatchIndexEntry {
    /// The zero-based record at `offset`, if this batch holds it.
    pub(crate) fn record_index(self, offset: Offset) -> Option<u32> {
        let delta = offset.get().checked_sub(self.first_offset.get())?;
        u32::try_from(delta)
            .ok()
            .filter(|&index| index < self.records)
    }

    /// One past the batch's last offset.
    pub(crate) fn end_offset(self) -> Option<u64> {
        self.first_offset.get().checked_add(u64::from(self.records))
    }

    /// The offset entry of record `record_index`, which the batch holds, given
    /// where its operation is.
    pub(crate) fn entry(
        self,
        partition: PartitionIncarnation,
        operation: OperationLocation,
        record_index: u32,
    ) -> OffsetIndexEntry {
        OffsetIndexEntry {
            partition,
            offset: Offset::new(self.first_offset.get() + u64::from(record_index)),
            append_timestamp_millis: if record_index == 0 {
                self.append_timestamp_millis
            } else {
                0
            },
            location: RecordLocation {
                operation,
                batch_index: self.batch_index,
                record_index,
            },
        }
    }
}

/// One record's identity, sorted by identity then offset. Its batch row gives
/// the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageRow {
    /// Application record identity preserved through retry and replay.
    pub message_id: MessageId,
    /// Partition-global record offset.
    pub offset: Offset,
}

/// Exact partition-offset lookup result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetIndexEntry {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Partition-global record offset.
    pub offset: Offset,
    /// Broker append time on the first record of a batch; zero on other records.
    pub append_timestamp_millis: u64,
    /// Exact physical operation and record coordinates.
    pub location: RecordLocation,
}

/// Exact idempotency lookup result. Resolve its offset through the batch rows.
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

/// One scheduled record of a `Schedule` operation, by release key. It is
/// held until a `Release` passes its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduleIndexEntry {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// `(max(due, acceptance), schedule sequence)`.
    pub key: ReleaseKey,
    /// The `Schedule` operation, record index, encoded bytes, and identity.
    pub record: PendingSchedule,
}

/// Entries derived from one canonical operation without retaining its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedIndexEntries {
    /// Numeric selector for every operation, including payload-free appends.
    pub location: OperationLocation,
    /// Partition of the records below; `None` without records.
    pub partition: Option<PartitionIncarnation>,
    /// One row per batch with records, in batch order.
    pub batches: Vec<BatchIndexEntry>,
    /// One row per record, in record order.
    pub messages: Vec<MessageRow>,
    /// Optional canonical control-operation identity selector.
    pub operation: Option<OperationIndexEntry>,
    /// Scheduled records in batch order.
    pub schedules: Vec<ScheduleIndexEntry>,
}

/// Held-record rows of a `Schedule` or `Carry`; `None` for other operations.
fn held_rows(
    op_number: u64,
    body: &OperationBody<'_>,
) -> Result<Option<Vec<ScheduleIndexEntry>>, IndexError> {
    let (partition, entries) = match body {
        OperationBody::Schedule(schedule) => (
            schedule.batch.partition,
            ozzy_core::state::schedule_entries(op_number, schedule),
        ),
        OperationBody::Carry(carry) => (
            carry.batch.partition,
            ozzy_core::state::carry_entries(op_number, carry),
        ),
        _ => return Ok(None),
    };
    Ok(Some(
        entries
            .map_err(|_| IndexError::PositionOverflow)?
            .into_iter()
            .map(|(key, record)| ScheduleIndexEntry {
                partition,
                key,
                record,
            })
            .collect(),
    ))
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
    // Held records have no offsets; indexes list them by release key.
    if let Some(schedules) = held_rows(operation.op_number, &body)? {
        return Ok(DerivedIndexEntries {
            location,
            partition: None,
            batches: Vec::new(),
            messages: Vec::new(),
            operation: operation_entry,
            schedules,
        });
    }
    let batches = match &body {
        OperationBody::Append(append) => append.batches.as_slice(),
        OperationBody::ResolvedAppend(append) => std::slice::from_ref(&append.batch),
        OperationBody::Release(release) => std::slice::from_ref(&release.batch),
        _ => {
            return Ok(DerivedIndexEntries {
                location,
                partition: None,
                batches: Vec::new(),
                messages: Vec::new(),
                operation: operation_entry,
                schedules: Vec::new(),
            });
        }
    };
    record_entries(batches, location)
}

/// Entries of every record of one APPEND's batches. An index holds one partition,
/// as a journal does.
fn record_entries(
    batches: &[AppendBatch<'_>],
    location: OperationLocation,
) -> Result<DerivedIndexEntries, IndexError> {
    let record_count = batches
        .iter()
        .try_fold(0_usize, |count, batch| {
            count.checked_add(batch.records.len())
        })
        .ok_or(IndexError::PositionOverflow)?;
    let mut partition = None;
    let mut rows = Vec::with_capacity(batches.len());
    let mut messages = Vec::with_capacity(record_count);
    for (batch_index, batch) in batches.iter().enumerate() {
        if batch.records.is_empty() {
            continue;
        }
        if *partition.get_or_insert(batch.partition) != batch.partition {
            return Err(IndexError::MixedPartitions);
        }
        rows.push(BatchIndexEntry {
            first_offset: batch.first_offset,
            append_timestamp_millis: batch.append_timestamp_millis,
            op_number: location.op_number,
            batch_index: u32::try_from(batch_index).map_err(|_| IndexError::PositionOverflow)?,
            records: u32::try_from(batch.records.len())
                .map_err(|_| IndexError::PositionOverflow)?,
        });
        for (record_index, record) in batch.records.iter().enumerate() {
            let delta = u64::try_from(record_index).map_err(|_| IndexError::PositionOverflow)?;
            let offset = batch
                .first_offset
                .get()
                .checked_add(delta)
                .map(Offset::new)
                .ok_or(IndexError::PositionOverflow)?;
            messages.push(MessageRow {
                message_id: record.message_id,
                offset,
            });
        }
    }
    Ok(DerivedIndexEntries {
        location,
        partition,
        batches: rows,
        messages,
        operation: None,
        schedules: Vec::new(),
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
    #[error("one operation holds records of more than one partition")]
    /// One operation holds records of more than one partition.
    MixedPartitions,
}
