//! Reader grants: the clients whose subscription a partition's live topic
//! admits. Shards publish immutable snapshots; the reader PUB's subscription
//! admission reads them on OMQ connection tasks.

use arc_swap::ArcSwap;
use ozzy_proto::NodeId;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Readers of each live topic, `group || incarnation`.
type Grants = BTreeMap<[u8; 32], Arc<BTreeSet<NodeId>>>;

/// Broker-wide reader grants, one snapshot per shard ID. Only that shard's
/// partitions write its snapshot, all on the shard thread.
#[derive(Clone, Debug, Default)]
pub struct ReaderGrants {
    shards: Arc<[(u32, ArcSwap<Grants>)]>,
}

impl ReaderGrants {
    /// Grants for the shards with these IDs, admitting nothing yet.
    pub fn new(shards: impl IntoIterator<Item = u32>) -> Self {
        Self {
            shards: shards
                .into_iter()
                .map(|id| (id, ArcSwap::from_pointee(Grants::new())))
                .collect(),
        }
    }

    /// Whether `node` holds a subscription to the live `topic` on any shard.
    pub fn admits(&self, topic: &[u8], node: NodeId) -> bool {
        let Ok(topic) = <[u8; 32]>::try_from(topic) else {
            return false;
        };
        self.shards.iter().any(|(_, shard)| {
            shard
                .load()
                .get(&topic)
                .is_some_and(|readers| readers.contains(&node))
        })
    }

    /// SUBSCRIBE admission for a reader PUB: an exact live topic whose
    /// subscription the SUB's node holds. The node is the SUB's routing
    /// identity, a claim the transport does not authenticate.
    pub fn admission(
        &self,
    ) -> impl Fn(&omq_tokio::SubscriptionRequest<'_>) -> bool + Send + Sync + 'static {
        let grants = self.clone();
        move |request| {
            request
                .peer
                .identity
                .as_deref()
                .and_then(crate::transport::decode_peer_identity)
                .is_some_and(|node| grants.admits(request.prefix, node))
        }
    }

    /// The writer of shard `id`'s snapshot, if there is that shard.
    pub fn shard(&self, id: u32) -> Option<ShardGrants> {
        let index = self.shards.iter().position(|(shard, _)| *shard == id)?;
        Some(ShardGrants {
            grants: self.clone(),
            index,
        })
    }
}

/// One shard's grants, written by that shard's partitions.
#[derive(Clone, Debug)]
pub struct ShardGrants {
    grants: ReaderGrants,
    index: usize,
}

impl ShardGrants {
    /// Replace `topic`'s readers; none removes the topic. Only the shard's own
    /// thread calls this, so no store races its load.
    pub fn set(&self, topic: [u8; 32], readers: BTreeSet<NodeId>) {
        let slot = &self.grants.shards[self.index].1;
        let mut next = Grants::clone(&slot.load());
        if readers.is_empty() {
            next.remove(&topic);
        } else {
            next.insert(topic, Arc::new(readers));
        }
        slot.store(Arc::new(next));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_admit_exact_topics_of_their_readers_on_any_shard() {
        let grants = ReaderGrants::new([3, 5]);
        let (reader, other) = (NodeId::from_bytes([1; 16]), NodeId::from_bytes([2; 16]));
        let topic = [7; 32];
        assert!(!grants.admits(&topic, reader));
        grants
            .shard(5)
            .unwrap()
            .set(topic, BTreeSet::from([reader]));
        assert!(grants.admits(&topic, reader));
        assert!(!grants.admits(&topic, other));
        assert!(!grants.admits(&topic[..16], reader));
        assert!(!grants.admits(&[], reader));
        grants.shard(5).unwrap().set(topic, BTreeSet::new());
        assert!(!grants.admits(&topic, reader));
        assert!(grants.shard(4).is_none());
    }
}
