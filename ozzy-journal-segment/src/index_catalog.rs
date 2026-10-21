//! Bounded exact lookup across immutable sealed-segment indexes.

pub(crate) mod asynchronous;

use std::collections::VecDeque;

use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::{
    IndexBuildError, IndexSource, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation,
};

const HOT_INDEX_CAPACITY: usize = 4;
const OFFSET_HINT_CAPACITY: usize = 64;
/// Operation IDs all resident filters may hold, 8 bytes each.
const OPERATION_FILTER_IDS: usize = 1 << 16;

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
    /// Operation filters by source position. An identity lookup that misses
    /// a filtered index skips reading it.
    filters: Vec<Option<OperationFilter>>,
    filtered_ids: usize,
}

impl IndexCache {
    fn new(sources: usize) -> Self {
        Self {
            hot: VecDeque::with_capacity(HOT_INDEX_CAPACITY),
            offset_hints: VecDeque::with_capacity(OFFSET_HINT_CAPACITY),
            filters: std::iter::repeat_with(|| None).take(sources).collect(),
            filtered_ids: 0,
        }
    }

    /// Keep the operation filter of the index at `at` while the budget allows.
    fn filter(&mut self, at: usize, index: &crate::SegmentIndex) {
        self.filter_ids(at, index.operation_ids());
    }

    fn filter_ids(&mut self, at: usize, ids: impl ExactSizeIterator<Item = OperationId>) {
        let count = ids.len();
        if let Some(slot @ None) = self.filters.get_mut(at)
            && count <= OPERATION_FILTER_IDS - self.filtered_ids
        {
            self.filtered_ids += count;
            *slot = Some(OperationFilter::new(ids));
        }
    }

    /// Whether the index at `at` certainly lacks `id`.
    fn lacks(&self, at: usize, id: OperationId) -> bool {
        self.filters
            .get(at)
            .and_then(Option::as_ref)
            .is_some_and(|filter| !filter.may_contain(id))
    }
}

/// Folded operation IDs of one sealed index, sorted. A present ID may still
/// be a fold collision; only its absence is exact.
#[derive(Debug)]
struct OperationFilter(Box<[u64]>);

impl OperationFilter {
    fn new(ids: impl Iterator<Item = OperationId>) -> Self {
        let mut folded: Vec<u64> = ids.map(fold).collect();
        folded.sort_unstable();
        Self(folded.into())
    }

    fn may_contain(&self, id: OperationId) -> bool {
        self.0.binary_search(&fold(id)).is_ok()
    }
}

fn fold(id: OperationId) -> u64 {
    use ozzy_proto::layout::Fixed;
    let bytes = id.as_bytes();
    bytes.u64_at::<0>() ^ bytes.u64_at::<8>()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u32) -> OperationId {
        let n = u64::from(n);
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&n.to_be_bytes());
        bytes[8..].copy_from_slice(&(n * 7).to_le_bytes());
        OperationId::from_bytes(bytes)
    }

    #[test]
    fn operation_filters_keep_every_present_id_within_their_budget() {
        let mut cache = IndexCache::new(3);
        cache.filter_ids(0, (0..1000).map(id));
        assert!((0..1000).all(|n| !cache.lacks(0, id(n))));
        assert!((1000..2000).filter(|n| cache.lacks(0, id(*n))).count() > 990);
        // No filter means no claim: the index must be read.
        assert!(!cache.lacks(1, id(5)));
        assert!(!cache.lacks(7, id(5)));
        // A filter that does not fit the remaining budget is not kept.
        cache.filter_ids(1, (0..OPERATION_FILTER_IDS as u32).map(id));
        assert!(!cache.lacks(1, id(u32::MAX)));
        cache.filter_ids(2, (0..10).map(id));
        assert!(cache.lacks(2, id(11)));
        assert_eq!(cache.filtered_ids, 1010);
    }
}
