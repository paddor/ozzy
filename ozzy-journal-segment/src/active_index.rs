//! Bounded exact in-memory index for one observed active-segment prefix.

use ozzy_journal::operation::OperationLimits;
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::{
    INDEX_HEADER_BYTES, IndexFileError, IndexLimits, IndexSource, MESSAGE_INDEX_ENTRY_BYTES,
    MessageIndexEntry, OFFSET_INDEX_ENTRY_BYTES, OPERATION_INDEX_ENTRY_BYTES, OffsetIndexEntry,
    OperationIndexEntry, SegmentIndexImage, SegmentScan,
};

mod building;
use building::Entries;
#[cfg(test)]
mod tests;

/// Exact accepted active-segment entries bounded by encoded index footprint.
#[derive(Debug, Clone)]
pub struct ActiveSegmentIndex {
    image: SegmentIndexImage,
}

impl ActiveSegmentIndex {
    /// Build through one exact operation, ignoring later complete physical groups.
    pub fn build(
        scan: &SegmentScan<'_>,
        through_op: u64,
        operation_limits: OperationLimits,
        limits: IndexLimits,
    ) -> Result<Option<Self>, ActiveIndexError> {
        let mut entries = Entries::new(limits)?;
        for operation in scan.groups.iter().flat_map(|group| &group.operations) {
            if operation.op_number > through_op {
                break;
            }
            entries.push(&scan.header, operation, operation_limits)?;
        }
        let Some(source) = entries.source(scan, through_op)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            image: entries.finish(source)?,
        }))
    }

    pub(crate) async fn build_async(
        scan: &SegmentScan<'_>,
        through_op: u64,
        operation_limits: OperationLimits,
        limits: IndexLimits,
    ) -> Result<Option<Self>, ActiveIndexError> {
        let mut entries = Entries::new(limits)?;
        let mut budget = crate::cooperative::Budget::default();
        for operation in scan.groups.iter().flat_map(|group| &group.operations) {
            if operation.op_number > through_op {
                break;
            }
            entries.push(&scan.header, operation, operation_limits)?;
            budget.charge(operation.body.len()).await;
        }
        let Some(source) = entries.source(scan, through_op)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            image: entries.finish_async(source).await?,
        }))
    }

    pub const fn source(&self) -> IndexSource {
        self.image.source()
    }

    pub fn offset_count(&self) -> usize {
        self.image.offsets().len()
    }

    pub fn message_count(&self) -> usize {
        self.image.messages().len()
    }

    pub fn operation_count(&self) -> usize {
        self.image.operations().len()
    }

    pub(crate) fn offsets(&self) -> &[OffsetIndexEntry] {
        self.image.offsets()
    }

    pub fn find_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Option<OffsetIndexEntry> {
        self.image
            .offsets()
            .binary_search_by(|entry| {
                entry
                    .partition
                    .as_bytes()
                    .cmp(partition.as_bytes())
                    .then_with(|| entry.offset.cmp(&offset))
            })
            .ok()
            .map(|index| self.image.offsets()[index])
    }

    pub fn find_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Option<MessageIndexEntry> {
        let messages = self.image.messages();
        let index = messages.partition_point(|entry| {
            entry
                .partition
                .as_bytes()
                .cmp(partition.as_bytes())
                .then_with(|| entry.message_id.as_bytes().cmp(message_id.as_bytes()))
                .is_lt()
        });
        messages
            .get(index)
            .copied()
            .filter(|entry| entry.partition == partition && entry.message_id == message_id)
    }

    pub fn find_operation(&self, operation_id: OperationId) -> Option<OperationIndexEntry> {
        self.image
            .operations()
            .binary_search_by(|entry| entry.operation_id.as_bytes().cmp(operation_id.as_bytes()))
            .ok()
            .map(|index| self.image.operations()[index])
    }
}

fn extend_bounded<T>(
    target: &mut Vec<T>,
    values: impl IntoIterator<Item = T>,
    limit: usize,
    kind: &'static str,
) -> Result<(), ActiveIndexError> {
    for value in values {
        let actual = target
            .len()
            .checked_add(1)
            .ok_or(ActiveIndexError::LengthOverflow)?;
        if actual > limit {
            return Err(ActiveIndexError::LimitExceeded {
                kind,
                actual,
                limit,
            });
        }
        target.push(value);
    }
    Ok(())
}

fn enforce_footprint(
    offsets: usize,
    messages: usize,
    operations: usize,
    limits: IndexLimits,
) -> Result<(), ActiveIndexError> {
    let bytes = offsets
        .checked_mul(OFFSET_INDEX_ENTRY_BYTES)
        .and_then(|value| {
            messages
                .checked_mul(MESSAGE_INDEX_ENTRY_BYTES)
                .and_then(|section| value.checked_add(section))
        })
        .and_then(|value| {
            operations
                .checked_mul(OPERATION_INDEX_ENTRY_BYTES)
                .and_then(|section| value.checked_add(section))
        })
        .and_then(|value| value.checked_add(INDEX_HEADER_BYTES))
        .ok_or(ActiveIndexError::LengthOverflow)?;
    if bytes > limits.max_file_bytes {
        return Err(ActiveIndexError::LimitExceeded {
            kind: "active index footprint",
            actual: bytes,
            limit: limits.max_file_bytes,
        });
    }
    Ok(())
}

fn validate_limits(limits: IndexLimits) -> Result<(), ActiveIndexError> {
    if limits.max_file_bytes < INDEX_HEADER_BYTES
        || limits.max_offset_entries == 0
        || limits.max_message_entries == 0
        || limits.max_operation_entries == 0
    {
        Err(ActiveIndexError::InvalidLimits)
    } else {
        Ok(())
    }
}

/// Active-prefix derivation, bound, or continuity failure.
#[derive(Debug, Error)]
pub enum ActiveIndexError {
    #[error(transparent)]
    Index(#[from] crate::IndexError),
    #[error(transparent)]
    File(#[from] IndexFileError),
    #[error("active index resource limits are invalid")]
    InvalidLimits,
    #[error("active scan ends at operation {last}, before requested operation {requested}")]
    PositionNotCovered { requested: u64, last: u64 },
    #[error("active index integer or length overflow")]
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    LimitExceeded {
        kind: &'static str,
        actual: usize,
        limit: usize,
    },
}
