//! Bounded exact lookup across immutable sealed-segment indexes.

pub(crate) mod asynchronous;

use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use ozzy_proto::{GroupId, MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::{
    IndexBuildError, IndexLimits, IndexSource, MessageIndexEntry, OffsetIndexEntry,
    OperationIndexEntry, open_segment_index, segment_index_name,
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

/// Exact sealed-index set with an owner-local, bounded hot index set.
///
/// Open validates every source. Clones share at most four decoded file images
/// and 64 compact partition-offset cursors on their owning thread.
#[derive(Debug, Clone)]
pub struct SegmentIndexCatalog {
    directory: PathBuf,
    group_id: Option<GroupId>,
    sources: Vec<IndexSource>,
    limits: IndexLimits,
    cache: Rc<RefCell<IndexCache>>,
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

impl SegmentIndexCatalog {
    /// Open and validate exact selected index sources within the configured bounds.
    pub fn open(
        directory: impl AsRef<Path>,
        sources: Vec<IndexSource>,
        limits: IndexLimits,
    ) -> Result<Self, IndexCatalogError> {
        let directory = directory.as_ref();
        if !fs::symlink_metadata(directory)?.file_type().is_dir() {
            return Err(IndexCatalogError::NotDirectory);
        }
        validate_sources(&sources)?;
        let mut cache = IndexCache::new();
        for source in &sources {
            let index =
                open_segment_index(directory.join(segment_index_name(*source)), *source, limits)?;
            if cache.hot.len() < HOT_INDEX_CAPACITY {
                cache.hot.push_back(index);
            }
        }
        Ok(Self {
            directory: directory.to_path_buf(),
            group_id: sources.first().map(|source| source.group_id),
            sources,
            limits,
            cache: Rc::new(RefCell::new(cache)),
        })
    }

    pub(crate) fn from_validated(
        directory: impl AsRef<Path>,
        sources: Vec<IndexSource>,
        limits: IndexLimits,
        hot: Option<crate::SegmentIndex>,
    ) -> Result<Self, IndexCatalogError> {
        let directory = directory.as_ref();
        if !fs::symlink_metadata(directory)?.file_type().is_dir() {
            return Err(IndexCatalogError::NotDirectory);
        }
        validate_sources(&sources)?;
        if hot
            .as_ref()
            .is_some_and(|index| !sources.contains(&index.source()))
        {
            return Err(IndexCatalogError::InvalidSources);
        }
        let mut cache = IndexCache::new();
        cache.hot.extend(hot);
        Ok(Self {
            directory: directory.to_path_buf(),
            group_id: sources.first().map(|source| source.group_id),
            sources,
            limits,
            cache: Rc::new(RefCell::new(cache)),
        })
    }

    /// Persistent replication-group identity.
    pub const fn group_id(&self) -> Option<GroupId> {
        self.group_id
    }

    /// Number of exact segment sources in this catalog.
    pub fn segment_count(&self) -> usize {
        self.sources.len()
    }

    /// Exact immutable segment sources in catalog order.
    pub fn sources(&self) -> &[IndexSource] {
        &self.sources
    }

    /// Look up an exact partition incarnation and global record offset.
    pub fn find_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
        through_op: u64,
    ) -> Result<Option<IndexedOffsetLocation>, IndexCatalogError> {
        if let Some(hint) = self.offset_hint(partition) {
            if let Some(location) =
                self.find_offset_in_source(hint.source_index, partition, offset, through_op)?
            {
                return Ok(Some(location));
            }
            let preferred = if offset > hint.offset {
                self.find_offset_in_sources(
                    hint.source_index.saturating_add(1)..self.sources.len(),
                    partition,
                    offset,
                    through_op,
                )?
            } else {
                self.find_offset_in_sources(
                    (0..hint.source_index).rev(),
                    partition,
                    offset,
                    through_op,
                )?
            };
            if preferred.is_some() {
                return Ok(preferred);
            }
            return if offset > hint.offset {
                self.find_offset_in_sources(
                    (0..hint.source_index).rev(),
                    partition,
                    offset,
                    through_op,
                )
            } else {
                self.find_offset_in_sources(
                    hint.source_index.saturating_add(1)..self.sources.len(),
                    partition,
                    offset,
                    through_op,
                )
            };
        }

        let hot_sources = self.hot_sources(through_op);
        for source in &hot_sources {
            let source_index = self.source_index(*source);
            if let Some(location) =
                self.find_offset_in_source(source_index, partition, offset, through_op)?
            {
                return Ok(Some(location));
            }
        }
        for source_index in (0..self.sources.len()).rev() {
            let source = self.sources[source_index];
            if source.first_op_number > through_op || hot_sources.contains(&source) {
                continue;
            }
            if let Some(location) =
                self.find_offset_in_source(source_index, partition, offset, through_op)?
            {
                return Ok(Some(location));
            }
        }
        Ok(None)
    }

    /// Look up an exact partition incarnation and record identity.
    pub fn find_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
        through_op: u64,
    ) -> Result<Option<IndexedMessageLocation>, IndexCatalogError> {
        let hot_sources = self.hot_sources(through_op);
        for source in &hot_sources {
            if let Some((message, offset)) = self.with_source(*source, |index| {
                message_location(index, partition, message_id)
            })? && offset.location.operation.op_number <= through_op
            {
                return Ok(Some(IndexedMessageLocation {
                    source: *source,
                    message,
                    offset,
                }));
            }
        }
        for source in self.eligible_sources(through_op) {
            if hot_sources.contains(source) {
                continue;
            }
            if let Some((message, offset)) = self.with_source(*source, |index| {
                message_location(index, partition, message_id)
            })? && offset.location.operation.op_number <= through_op
            {
                return Ok(Some(IndexedMessageLocation {
                    source: *source,
                    message,
                    offset,
                }));
            }
        }
        Ok(None)
    }

    /// Look up an exact canonical control-operation identity.
    pub fn find_operation(
        &self,
        operation_id: OperationId,
        through_op: u64,
    ) -> Result<Option<IndexedOperationLocation>, IndexCatalogError> {
        let hot_sources = self.hot_sources(through_op);
        for source in &hot_sources {
            if let Some(entry) =
                self.with_source(*source, |index| Ok(index.find_operation(operation_id)))?
                && entry.location.op_number <= through_op
            {
                return Ok(Some(IndexedOperationLocation {
                    source: *source,
                    entry,
                }));
            }
        }
        for source in self.eligible_sources(through_op) {
            if hot_sources.contains(source) {
                continue;
            }
            if let Some(entry) =
                self.with_source(*source, |index| Ok(index.find_operation(operation_id)))?
                && entry.location.op_number <= through_op
            {
                return Ok(Some(IndexedOperationLocation {
                    source: *source,
                    entry,
                }));
            }
        }
        Ok(None)
    }

    fn eligible_sources(&self, through_op: u64) -> impl Iterator<Item = &IndexSource> {
        self.sources
            .iter()
            .rev()
            .filter(move |source| source.first_op_number <= through_op)
    }

    fn find_offset_in_source(
        &self,
        source_index: usize,
        partition: PartitionIncarnation,
        offset: Offset,
        through_op: u64,
    ) -> Result<Option<IndexedOffsetLocation>, IndexCatalogError> {
        let source = self.sources[source_index];
        if source.first_op_number > through_op {
            return Ok(None);
        }
        let entry = self.with_source(source, |index| Ok(index.find_offset(partition, offset)))?;
        let Some(entry) = entry.filter(|entry| entry.location.operation.op_number <= through_op)
        else {
            return Ok(None);
        };
        self.remember_offset(partition, offset, source_index);
        Ok(Some(IndexedOffsetLocation { source, entry }))
    }

    fn find_offset_in_sources(
        &self,
        source_indices: impl Iterator<Item = usize>,
        partition: PartitionIncarnation,
        offset: Offset,
        through_op: u64,
    ) -> Result<Option<IndexedOffsetLocation>, IndexCatalogError> {
        for source_index in source_indices {
            if let Some(location) =
                self.find_offset_in_source(source_index, partition, offset, through_op)?
            {
                return Ok(Some(location));
            }
        }
        Ok(None)
    }

    fn source_index(&self, source: IndexSource) -> usize {
        self.sources
            .binary_search_by_key(&source.segment_id, |candidate| candidate.segment_id)
            .expect("hot source belongs to validated catalog")
    }

    fn hot_sources(
        &self,
        through_op: u64,
    ) -> smallvec::SmallVec<[IndexSource; HOT_INDEX_CAPACITY]> {
        self.cache_guard()
            .hot
            .iter()
            .map(crate::SegmentIndex::source)
            .filter(|source| source.first_op_number <= through_op)
            .collect()
    }

    fn offset_hint(&self, partition: PartitionIncarnation) -> Option<OffsetHint> {
        self.cache_guard()
            .offset_hints
            .iter()
            .find(|hint| hint.partition == partition)
            .copied()
    }

    fn remember_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
        source_index: usize,
    ) {
        let mut cache = self.cache_guard();
        if let Some(position) = cache
            .offset_hints
            .iter()
            .position(|hint| hint.partition == partition)
        {
            cache.offset_hints.remove(position);
        } else if cache.offset_hints.len() == OFFSET_HINT_CAPACITY {
            cache.offset_hints.pop_back();
        }
        cache.offset_hints.push_front(OffsetHint {
            partition,
            offset,
            source_index,
        });
    }

    fn with_source<T>(
        &self,
        source: IndexSource,
        lookup: impl FnOnce(&crate::SegmentIndex) -> Result<Option<T>, IndexCatalogError>,
    ) -> Result<Option<T>, IndexCatalogError> {
        let mut cache = self.cache_guard();
        if let Some(position) = cache.hot.iter().position(|index| index.source() == source) {
            let index = cache
                .hot
                .remove(position)
                .expect("hot index position exists");
            let result = lookup(&index);
            cache.hot.push_front(index);
            return result;
        }
        let index = open_segment_index(
            self.directory.join(segment_index_name(source)),
            source,
            self.limits,
        )?;
        let result = lookup(&index)?;
        if result.is_some() {
            if cache.hot.len() == HOT_INDEX_CAPACITY {
                cache.hot.pop_back();
            }
            cache.hot.push_front(index);
        }
        Ok(result)
    }

    fn cache_guard(&self) -> RefMut<'_, IndexCache> {
        self.cache.borrow_mut()
    }
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
