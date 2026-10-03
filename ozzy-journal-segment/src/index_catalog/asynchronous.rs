//! Shard-local catalog. No cache borrow survives an asynchronous file job.

use std::{cell::RefCell, path::PathBuf, rc::Rc};

use super::{
    HOT_INDEX_CAPACITY, IndexCache, IndexCatalogError as Error, IndexedMessageLocation,
    IndexedOffsetLocation, IndexedOperationLocation, OFFSET_HINT_CAPACITY, OffsetHint,
    message_location, validate_sources,
};
use crate::{IndexLimits, IndexSource, SegmentIndex, async_files::Access, segment_index_name};
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};

/// File lifetime protection belongs to the containing journal snapshot.
#[derive(Debug, Clone)]
pub(crate) struct Catalog {
    access: Access,
    directory: PathBuf,
    sources: Rc<[IndexSource]>,
    limits: IndexLimits,
    chunk: usize,
    cache: Rc<RefCell<IndexCache>>,
}

impl Catalog {
    pub(crate) async fn open(
        access: Access,
        directory: PathBuf,
        sources: Vec<IndexSource>,
        limits: IndexLimits,
        chunk: usize,
    ) -> Result<Self, Error> {
        validate_sources(&sources)?;
        let handle = access.open_directory(directory.clone()).await?;
        access.done(ozzy_io::Operation::Close { handle }).await?;
        let mut cache = IndexCache::new();
        for source in &sources {
            let index = crate::index_builder::asynchronous::open(
                &access,
                directory.join(segment_index_name(*source)),
                *source,
                limits,
                chunk,
            )
            .await?;
            if cache.hot.len() < HOT_INDEX_CAPACITY {
                cache.hot.push_back(index);
            }
        }
        Ok(Self {
            access,
            directory,
            sources: sources.into(),
            limits,
            chunk,
            cache: Rc::new(RefCell::new(cache)),
        })
    }

    pub(crate) async fn find_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
        through: u64,
    ) -> Result<Option<IndexedOffsetLocation>, Error> {
        let hint = self
            .cache
            .borrow()
            .offset_hints
            .iter()
            .find(|hint| hint.partition == partition)
            .copied();
        if let Some(hint) = hint {
            if let Some(location) = self
                .offset_in(hint.source_index, partition, offset, through)
                .await?
            {
                return Ok(Some(location));
            }
            let forward = hint.source_index + 1..self.sources.len();
            let backward = (0..hint.source_index).rev();
            return if offset > hint.offset {
                self.offset_in_many(forward.chain(backward), partition, offset, through)
                    .await
            } else {
                self.offset_in_many(backward.chain(forward), partition, offset, through)
                    .await
            };
        }
        let hot = self.hot_sources(through);
        for source in &hot {
            let at = self
                .sources
                .binary_search_by_key(&source.segment_id, |s| s.segment_id)
                .expect("cache source belongs to catalog");
            if let Some(location) = self.offset_in(at, partition, offset, through).await? {
                return Ok(Some(location));
            }
        }
        self.offset_in_many(
            (0..self.sources.len())
                .rev()
                .filter(|at| !hot.contains(&self.sources[*at])),
            partition,
            offset,
            through,
        )
        .await
    }

    async fn offset_in_many(
        &self,
        indices: impl Iterator<Item = usize>,
        partition: PartitionIncarnation,
        offset: Offset,
        through: u64,
    ) -> Result<Option<IndexedOffsetLocation>, Error> {
        for at in indices {
            if let Some(location) = self.offset_in(at, partition, offset, through).await? {
                return Ok(Some(location));
            }
        }
        Ok(None)
    }

    async fn offset_in(
        &self,
        at: usize,
        partition: PartitionIncarnation,
        offset: Offset,
        through: u64,
    ) -> Result<Option<IndexedOffsetLocation>, Error> {
        let source = self.sources[at];
        if source.first_op_number > through {
            return Ok(None);
        }
        let entry = self
            .with_source(source, |index| Ok(index.find_offset(partition, offset)))
            .await?;
        let Some(entry) = entry.filter(|entry| entry.location.operation.op_number <= through)
        else {
            return Ok(None);
        };
        let mut cache = self.cache.borrow_mut();
        if let Some(at) = cache
            .offset_hints
            .iter()
            .position(|hint| hint.partition == partition)
        {
            cache.offset_hints.remove(at);
        } else if cache.offset_hints.len() == OFFSET_HINT_CAPACITY {
            cache.offset_hints.pop_back();
        }
        cache.offset_hints.push_front(OffsetHint {
            partition,
            offset,
            source_index: at,
        });
        Ok(Some(IndexedOffsetLocation { source, entry }))
    }

    pub(crate) async fn find_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
        through: u64,
    ) -> Result<Option<IndexedMessageLocation>, Error> {
        let hot = self.hot_sources(through);
        for source in hot
            .iter()
            .chain(self.sources.iter().rev().filter(|s| !hot.contains(s)))
        {
            if source.first_op_number > through {
                continue;
            }
            if let Some((message, offset)) = self
                .with_source(*source, |index| {
                    message_location(index, partition, message_id)
                })
                .await?
                && offset.location.operation.op_number <= through
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

    pub(crate) async fn find_operation(
        &self,
        id: OperationId,
        through: u64,
    ) -> Result<Option<IndexedOperationLocation>, Error> {
        let hot = self.hot_sources(through);
        for source in hot
            .iter()
            .chain(self.sources.iter().rev().filter(|s| !hot.contains(s)))
        {
            if source.first_op_number > through {
                continue;
            }
            if let Some(entry) = self
                .with_source(*source, |index| Ok(index.find_operation(id)))
                .await?
                && entry.location.op_number <= through
            {
                return Ok(Some(IndexedOperationLocation {
                    source: *source,
                    entry,
                }));
            }
        }
        Ok(None)
    }

    fn hot_sources(&self, through: u64) -> smallvec::SmallVec<[IndexSource; HOT_INDEX_CAPACITY]> {
        self.cache
            .borrow()
            .hot
            .iter()
            .map(SegmentIndex::source)
            .filter(|source| source.first_op_number <= through)
            .collect()
    }

    async fn with_source<T>(
        &self,
        source: IndexSource,
        lookup: impl FnOnce(&SegmentIndex) -> Result<Option<T>, Error>,
    ) -> Result<Option<T>, Error> {
        {
            let mut cache = self.cache.borrow_mut();
            if let Some(at) = cache.hot.iter().position(|index| index.source() == source) {
                let index = cache.hot.remove(at).expect("cache position exists");
                let result = lookup(&index);
                cache.hot.push_front(index);
                return result;
            }
        }
        let index = crate::index_builder::asynchronous::open(
            &self.access,
            self.directory.join(segment_index_name(source)),
            source,
            self.limits,
            self.chunk,
        )
        .await?;
        let result = lookup(&index)?;
        if result.is_some() {
            let mut cache = self.cache.borrow_mut();
            // Another lookup may have completed while this one was waiting.
            if let Some(at) = cache.hot.iter().position(|index| index.source() == source) {
                cache.hot.remove(at);
            } else if cache.hot.len() == HOT_INDEX_CAPACITY {
                cache.hot.pop_back();
            }
            cache.hot.push_front(index);
        }
        Ok(result)
    }
}
