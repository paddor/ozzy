//! Bounded exact in-memory index for one observed active-segment prefix.

use ozzy_journal::operation::OperationLimits;
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::{
    INDEX_HEADER_BYTES, IndexFileError, IndexLimits, IndexSource, MESSAGE_INDEX_ENTRY_BYTES,
    MessageIndexEntry, OFFSET_INDEX_ENTRY_BYTES, OPERATION_INDEX_ENTRY_BYTES,
    OPERATION_LOCATION_BYTES, OffsetIndexEntry, OperationIndexEntry, OperationLocation,
    SegmentIndexImage, SegmentScan,
};

mod building;
use building::Entries;
#[cfg(test)]
mod tests;

/// Exact accepted active-segment entries bounded by encoded index footprint.
#[derive(Debug, Clone)]
pub(crate) struct ActiveSegmentIndex {
    image: SegmentIndexImage,
}

/// The validated index of an active-segment prefix with its reader state. The
/// next build of the same segment sorts and merges only appended operations.
pub(crate) struct ActivePrefix {
    resume: crate::directory::asynchronous::groups::Resume,
    index: Option<std::rc::Rc<ActiveSegmentIndex>>,
    limits: IndexLimits,
}

impl ActivePrefix {
    /// Reusable only with identical limits and a boundary covering every entry.
    pub(crate) fn reusable(&self, through_op: u64, limits: IndexLimits) -> bool {
        self.limits == limits
            && self
                .index
                .as_ref()
                .is_none_or(|index| index.image.source().last_op_number <= through_op)
    }

    pub(crate) fn resume(&self) -> crate::directory::asynchronous::groups::Resume {
        self.resume.clone()
    }
}

impl std::fmt::Debug for ActivePrefix {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActivePrefix")
            .field("source", &self.index.as_ref().map(|index| index.source()))
            .finish_non_exhaustive()
    }
}

impl ActiveSegmentIndex {
    pub(crate) fn seek(
        &self,
        query: crate::SeekQuery,
        result: &mut ozzy_core::reader::seek::Selection,
    ) {
        // In-memory active rows use the same validated image order as disk.
        match query.start {
            ozzy_proto::reader::Start::RecordId { id, .. } => {
                let messages = self.image.messages();
                let key =
                    |entry: &MessageIndexEntry| (entry.partition, entry.message_id, entry.offset);
                let first = messages.partition_point(|entry| {
                    key(entry) < (query.partition, id, query.retained_from)
                });
                let after = messages.partition_point(|entry| {
                    key(entry) < (query.partition, id, query.confirmed_end)
                });
                if let Some(matching) = messages.get(first..after)
                    && let (Some(first), Some(last)) = (matching.first(), matching.last())
                {
                    for entry in [first, last] {
                        if entry.partition == query.partition
                            && entry.message_id == id
                            && self.find_offset(query.partition, entry.offset).is_some_and(
                                |offset| offset.location.operation.op_number <= query.through,
                            )
                        {
                            result.observe_match(entry.offset);
                        }
                    }
                }
            }
            ozzy_proto::reader::Start::Timestamp(timestamp) => {
                let at_floor = self.find_offset(query.partition, query.retained_from);
                for head in self.image.offsets().iter().filter(|entry| {
                    entry.partition == query.partition
                        && entry.location.record_index == 0
                        && entry.append_timestamp_millis >= timestamp
                        && entry.location.operation.op_number <= query.through
                        && entry.offset < query.confirmed_end
                }) {
                    let offset = if head.offset >= query.retained_from {
                        Some(head.offset)
                    } else {
                        at_floor
                            .filter(|entry| {
                                entry.location.operation == head.location.operation
                                    && entry.location.batch_index == head.location.batch_index
                            })
                            .map(|_| query.retained_from)
                    };
                    if let Some(offset) = offset {
                        result.observe_match(offset);
                        break;
                    }
                }
            }
            _ => {}
        }
    }

    /// Build through one exact operation, ignoring later complete physical groups.
    pub(crate) fn build(
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
        let Some(source) = entries.source(
            &scan.header,
            scan.valid_bytes,
            scan.digest,
            through_op,
            None,
        )?
        else {
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
        let Some(source) = entries.source(
            &scan.header,
            scan.valid_bytes,
            scan.digest,
            through_op,
            None,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            image: entries.finish_async(source).await?,
        }))
    }

    /// Index groups through `through_op`, continuing `prefix` when the reader
    /// resumed after it. With `keep`, a scan that indexed every operation also
    /// returns the prefix for the next build of this segment.
    pub(crate) async fn build_groups(
        mut groups: Box<crate::directory::asynchronous::groups::Groups>,
        prefix: Option<ActivePrefix>,
        keep: bool,
        through_op: u64,
        operations: OperationLimits,
        limits: IndexLimits,
    ) -> Result<(Option<std::rc::Rc<Self>>, Option<ActivePrefix>), crate::JournalIndexError> {
        let header = groups.header().clone();
        let (valid_bytes, digest) = groups.boundary();
        let base = prefix
            .filter(|_| groups.resumed())
            .and_then(|prefix| prefix.index);
        let mut entries = Entries::new(limits)?;
        let mut complete = true;
        let mut budget = crate::cooperative::Budget::default();
        while let Some(group) = groups.next().await? {
            for operation in &group.operations {
                if operation.op_number <= through_op {
                    entries.push(&header, operation, operations)?;
                    budget.charge(operation.body.len()).await;
                } else {
                    complete = false;
                }
            }
        }
        let resume = groups.resume().filter(|_| keep && complete);
        groups.finish().await?;
        let base_image = base.as_deref().map(|base| &base.image);
        let index = match entries.source(&header, valid_bytes, digest, through_op, base_image)? {
            None => None,
            Some(source)
                if entries.is_empty()
                    && base_image.map(SegmentIndexImage::source) == Some(source) =>
            {
                base
            }
            Some(source) => Some(std::rc::Rc::new(Self {
                image: match base_image {
                    Some(base) => entries.extend_async(base, source).await?,
                    None => entries.finish_async(source).await?,
                },
            })),
        };
        let prefix = resume.map(|resume| ActivePrefix {
            resume,
            index: index.clone(),
            limits,
        });
        Ok((index, prefix))
    }

    pub(crate) const fn source(&self) -> IndexSource {
        self.image.source()
    }

    pub(crate) fn operation_count(&self) -> usize {
        self.image.operations().len()
    }

    pub(crate) fn location_count(&self) -> usize {
        self.image.locations().len()
    }

    pub(crate) fn find_operation_number(&self, number: u64) -> Option<OperationLocation> {
        self.image.find_operation_number(number)
    }

    pub(crate) fn offsets(&self) -> &[OffsetIndexEntry] {
        self.image.offsets()
    }

    pub(crate) fn find_offset(
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

    pub(crate) fn find_message(
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

    pub(crate) fn find_operation(&self, operation_id: OperationId) -> Option<OperationIndexEntry> {
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
    locations: usize,
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
        .and_then(|value| {
            locations
                .checked_mul(OPERATION_LOCATION_BYTES)
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
    /// Derived index construction or validation failed.
    Index(#[from] crate::IndexError),
    #[error(transparent)]
    /// The encoded index representation is invalid.
    File(#[from] IndexFileError),
    #[error("active index resource limits are invalid")]
    /// Active index resource limits are invalid.
    InvalidLimits,
    #[error("active scan ends at operation {last}, before requested operation {requested}")]
    /// Active scan ends at operation, before requested operation.
    PositionNotCovered {
        #[doc = "Requested canonical operation number."]
        requested: u64,
        #[doc = "Last canonical operation covered by this source."]
        last: u64,
    },
    #[error("active index integer or length overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    /// The named resource exceeds its configured bound.
    LimitExceeded {
        /// Resource bound that rejected the operation.
        kind: &'static str,
        /// Observed size, count, or fenced field value.
        actual: usize,
        /// Configured maximum for the reported resource.
        limit: usize,
    },
}
