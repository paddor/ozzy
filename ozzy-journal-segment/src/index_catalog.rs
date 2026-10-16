//! Bounded exact lookup across immutable sealed-segment indexes.

pub(crate) mod asynchronous;

use std::collections::VecDeque;

use ozzy_proto::{MessageId, Offset, PartitionIncarnation};
use thiserror::Error;

use crate::{
    IndexBuildError, IndexSource, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation,
};

const HOT_INDEX_CAPACITY: usize = 4;
const OFFSET_HINT_CAPACITY: usize = 64;

/// Message identity plus corresponding authoritative record location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedMessageLocation {
    /// Exact immutable segment prefix binding this selector.
    pub source: IndexSource,
    /// Message-identity selector for this exact record.
    pub message: MessageIndexEntry,
    /// Offset selector for the same selected record.
    pub offset: OffsetIndexEntry,
}

/// Offset result bound to its source segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedOffsetLocation {
    /// Exact immutable segment prefix binding this selector.
    pub source: IndexSource,
    /// Validated record or operation selector bound to the exact source.
    pub entry: OffsetIndexEntry,
}

/// Operation-ID result bound to its source segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedOperationLocation {
    /// Exact immutable segment prefix binding this selector.
    pub source: IndexSource,
    /// Validated record or operation selector bound to the exact source.
    pub entry: OperationIndexEntry,
}

/// Numeric operation result bound to its exact source segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedOperation {
    /// Exact immutable segment prefix binding this selector.
    pub source: IndexSource,
    /// Validated canonical operation, independent of any retry ID or payload.
    pub location: OperationLocation,
}

#[derive(Debug)]
struct IndexCache {
    hot: VecDeque<crate::SegmentIndex>,
    offset_hints: VecDeque<OffsetHint>,
}

impl IndexCache {
    fn new() -> Self {
        Self {
            hot: VecDeque::with_capacity(HOT_INDEX_CAPACITY),
            offset_hints: VecDeque::with_capacity(OFFSET_HINT_CAPACITY),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct OffsetHint {
    partition: PartitionIncarnation,
    offset: Offset,
    source_index: usize,
}

fn message_location(
    index: &crate::SegmentIndex,
    partition: PartitionIncarnation,
    message_id: MessageId,
) -> Result<Option<(MessageIndexEntry, OffsetIndexEntry)>, IndexCatalogError> {
    let Some(message) = index.find_message(partition, message_id) else {
        return Ok(None);
    };
    let offset = index
        .find_offset(partition, message.offset)
        .ok_or(IndexCatalogError::InconsistentIndex)?;
    Ok(Some((message, offset)))
}

fn validate_sources(sources: &[IndexSource]) -> Result<(), IndexCatalogError> {
    for pair in sources.windows(2) {
        if pair[0].group_id != pair[1].group_id
            || pair[0].segment_id >= pair[1].segment_id
            || pair[0].last_op_number.checked_add(1) != Some(pair[1].first_op_number)
        {
            return Err(IndexCatalogError::InvalidSources);
        }
    }
    Ok(())
}

/// Sealed index-set validation or lookup failure.
#[derive(Debug, Error)]
pub enum IndexCatalogError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] std::io::Error),
    #[error(transparent)]
    /// Derived index construction or validation failed.
    Index(#[from] IndexBuildError),
    #[error("index catalog path is not a directory")]
    /// The named artifact is not a directory.
    NotDirectory,
    #[error("index catalog sources are from different or discontinuous histories")]
    /// Index catalog sources are from different or discontinuous histories.
    InvalidSources,
    #[error("validated message index has no matching offset entry")]
    /// Validated message index has no matching offset entry.
    InconsistentIndex,
}
