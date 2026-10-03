//! Immutable, checked topic pages served by the broker dispatcher.

use std::collections::{BTreeMap, BTreeSet};

use ozzy_proto::{GroupId, PartitionIncarnation, TopicId, directory};

/// Configured topic catalog. Pages must cover each topic without gaps.
#[derive(Debug)]
pub struct TopicCatalog {
    topics: BTreeMap<String, Vec<directory::TopicPage>>,
    largest_single: usize,
}

/// A malformed catalog, unknown request, or reply too small for one partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CatalogError {
    /// The checked identity and deployment produced an inconsistent catalog.
    #[error("invalid topic catalog")]
    Invalid,
    /// The requested topic or numeric partition does not exist.
    #[error("unknown topic or partition")]
    Unknown,
    /// One partition and its broker endpoints exceed the reply limit.
    #[error("topic metadata reply limit too small")]
    Size,
}

impl TopicCatalog {
    /// Build from bounded pages in numeric order. This repeats immutable topic
    /// headers but never stores shard-local placement in SDK metadata.
    pub fn new(
        pages: impl IntoIterator<Item = directory::TopicPage>,
        max_topics: usize,
        max_partitions: usize,
    ) -> Result<Self, CatalogError> {
        if max_topics == 0 || max_partitions == 0 {
            return Err(CatalogError::Invalid);
        }
        let mut topics: BTreeMap<String, Vec<directory::TopicPage>> = BTreeMap::new();
        let mut ids = BTreeSet::<TopicId>::new();
        let mut groups = BTreeSet::<GroupId>::new();
        let mut incarnations = BTreeSet::<PartitionIncarnation>::new();
        let mut largest_single = 0;
        let mut count = 0_usize;
        for page in pages {
            if !page.valid(directory::Limits::default())
                || !usize::try_from(page.total).is_ok_and(|total| total <= max_partitions)
            {
                return Err(CatalogError::Invalid);
            }
            let previous = topics.get(&page.name).and_then(|pages| pages.last());
            if let Some(previous) = previous {
                if previous.first + previous.partitions.len() as u32 != page.first
                    || previous.id != page.id
                    || previous.partitioner_seed != page.partitioner_seed
                    || previous.total != page.total
                    || previous.policy != page.policy
                    || previous.brokers != page.brokers
                {
                    return Err(CatalogError::Invalid);
                }
            } else if page.first != 0 || !ids.insert(page.id) {
                return Err(CatalogError::Invalid);
            }
            for partition in &page.partitions {
                if !groups.insert(partition.group) || !incarnations.insert(partition.incarnation) {
                    return Err(CatalogError::Invalid);
                }
                count = count.checked_add(1).ok_or(CatalogError::Invalid)?;
                if count > max_partitions {
                    return Err(CatalogError::Invalid);
                }
            }
            let mut single = page.clone();
            single.partitions.truncate(1);
            largest_single =
                largest_single.max(single.metadata_bytes().map_err(|_| CatalogError::Invalid)?);
            topics.entry(page.name.clone()).or_default().push(page);
        }
        if topics.is_empty()
            || topics.len() > max_topics
            || topics.values().any(|pages| {
                let last = pages.last().expect("nonempty topic");
                last.first + last.partitions.len() as u32 != last.total
            })
        {
            return Err(CatalogError::Invalid);
        }
        Ok(Self {
            topics,
            largest_single,
        })
    }

    /// Largest reply metadata needed for a single partition on any topic.
    pub fn largest_single_bytes(&self) -> usize {
        self.largest_single
    }

    /// Select a contiguous subpage that fits both negotiated frame and queue.
    pub fn page(
        &self,
        request: &directory::TopicRequest,
        max_metadata: usize,
    ) -> Result<directory::TopicPage, CatalogError> {
        let source = self
            .topics
            .get(&request.name)
            .and_then(|pages| {
                pages.iter().find(|page| {
                    page.first <= request.first
                        && request.first < page.first + page.partitions.len() as u32
                })
            })
            .ok_or(CatalogError::Unknown)?;
        if request.maximum == 0 {
            return Err(CatalogError::Invalid);
        }
        let start = (request.first - source.first) as usize;
        let end = source
            .partitions
            .len()
            .min(start + usize::from(request.maximum));
        let mut page = source.clone();
        page.first = request.first;
        page.partitions = source.partitions[start..end].to_vec();
        while page.metadata_bytes().map_err(|_| CatalogError::Invalid)? > max_metadata {
            if page.partitions.len() == 1 {
                return Err(CatalogError::Size);
            }
            page.partitions.pop();
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests;
