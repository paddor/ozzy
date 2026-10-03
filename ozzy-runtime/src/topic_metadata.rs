//! Client-checked topic identity assembled from bounded broker lookup pages.

use std::collections::{BTreeMap, BTreeSet};

use ozzy_proto::{
    GroupId, NodeId, PartitionIncarnation, TopicId,
    append::Policy,
    directory::{self, BrokerEndpoint, TopicPage, TopicPartition},
};

/// A page contradicts the topic identity or leaves a numeric gap.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("inconsistent or incomplete topic metadata")]
pub struct TopicMetadataError;

/// Immutable topic identity and numeric partition order for SDK placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicMetadata {
    id: TopicId,
    name: String,
    seed: u64,
    policy: Policy,
    brokers: Vec<BrokerEndpoint>,
    partitions: Vec<TopicPartition>,
    groups: BTreeMap<GroupId, usize>,
}

impl TopicMetadata {
    /// Assemble complete pages after each was decoded under its frame limit.
    /// `maximum` caps allocation independently of any untrusted page count.
    pub fn from_pages(
        pages: impl IntoIterator<Item = TopicPage>,
        maximum: usize,
    ) -> Result<Self, TopicMetadataError> {
        let mut pages = pages.into_iter();
        let first = pages.next().ok_or(TopicMetadataError)?;
        let total = usize::try_from(first.total).map_err(|_| TopicMetadataError)?;
        if maximum == 0
            || total == 0
            || total > maximum
            || first.first != 0
            || !first.valid(directory::Limits::default())
        {
            return Err(TopicMetadataError);
        }
        let mut groups = BTreeMap::<GroupId, usize>::new();
        let mut incarnations = BTreeSet::<PartitionIncarnation>::new();
        let mut partitions = Vec::with_capacity(total);
        for page in std::iter::once(first.clone()).chain(pages) {
            if !page.valid(directory::Limits::default())
                || page.id != first.id
                || page.name != first.name
                || page.partitioner_seed != first.partitioner_seed
                || page.total != first.total
                || page.policy != first.policy
                || page.brokers != first.brokers
                || usize::try_from(page.first).ok() != Some(partitions.len())
                || partitions.len() + page.partitions.len() > total
            {
                return Err(TopicMetadataError);
            }
            for partition in page.partitions {
                if groups.insert(partition.group, partitions.len()).is_some()
                    || !incarnations.insert(partition.incarnation)
                {
                    return Err(TopicMetadataError);
                }
                partitions.push(partition);
            }
        }
        if partitions.len() != total {
            return Err(TopicMetadataError);
        }
        Ok(Self {
            id: first.id,
            name: first.name,
            seed: first.partitioner_seed,
            policy: first.policy,
            brokers: first.brokers,
            partitions,
            groups,
        })
    }

    /// Persistent topic ID. A recreated topic is a distinct destination.
    pub fn id(&self) -> TopicId {
        self.id
    }

    /// Configured name used in lookup requests.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Persisted XXH3-64 seed, applied to immutable numeric partition order.
    pub fn partitioner_seed(&self) -> u64 {
        self.seed
    }

    /// Required confirmation boundary for all partitions in this topic.
    pub fn policy(&self) -> Policy {
        self.policy
    }

    /// Current configured broker addresses, without shard-local placement.
    pub fn brokers(&self) -> &[BrokerEndpoint] {
        &self.brokers
    }

    /// Find one configured broker by persistent identity.
    pub fn broker(&self, node: NodeId) -> Option<&BrokerEndpoint> {
        self.brokers.iter().find(|broker| broker.node == node)
    }

    /// Fixed partition count. More broker CPUs do not change it.
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Numeric partition lookup. Group IDs never define hash order.
    pub fn partition(&self, number: u32) -> Option<&TopicPartition> {
        self.partitions.get(usize::try_from(number).ok()?)
    }

    /// Match a watched group against this topic's immutable identity.
    pub fn partition_by_group(&self, group: GroupId) -> Option<&TopicPartition> {
        self.groups
            .get(&group)
            .map(|&index| &self.partitions[index])
    }

    /// Stable XXH3-64 seeded key mapping, before SDK batching or sequencing.
    pub fn keyed_partition(&self, key: &[u8]) -> &TopicPartition {
        let hash = xxhash_rust::xxh3::xxh3_64_with_seed(key, self.seed);
        &self.partitions[(hash % self.partitions.len() as u64) as usize]
    }
}

mod routes;
pub use routes::{RouteCache, RouteCacheError};

#[cfg(test)]
mod tests;
