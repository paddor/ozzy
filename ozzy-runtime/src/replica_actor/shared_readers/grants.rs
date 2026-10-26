//! This partition's entry in its shard's reader grants, which admit a
//! client's live topic subscription.

use super::{NodeId, SharedReaders};
use crate::frontend::ShardGrants;
use std::collections::BTreeSet;

/// The clients holding a subscription here, replaying or live, as last
/// published.
#[derive(Debug)]
pub(super) struct Granted {
    shard: ShardGrants,
    topic: [u8; 32],
    readers: BTreeSet<NodeId>,
    /// Slots changed since the last publication.
    pub(super) stale: bool,
}

impl Granted {
    /// The live topic admits nobody until slots publish their readers.
    pub(super) fn new(shard: ShardGrants, topic: [u8; 32]) -> Self {
        shard.set(topic, BTreeSet::new());
        Self {
            shard,
            topic,
            readers: BTreeSet::new(),
            stale: false,
        }
    }
}

impl Drop for Granted {
    fn drop(&mut self) {
        self.shard.end(self.topic);
    }
}

impl SharedReaders {
    /// Record that the slots changed; the next `grant` republishes.
    pub(super) fn slots_changed(&mut self) {
        if let Some(granted) = &mut self.grants {
            granted.stale = true;
        }
    }

    /// Publish the clients holding a subscription when slots changed. It runs
    /// after commands and before replies, so a client's live topic admits it
    /// before its SUBSCRIBED leaves.
    pub(super) fn grant(&mut self) {
        let Some(granted) = self.grants.as_mut().filter(|granted| granted.stale) else {
            return;
        };
        granted.stale = false;
        let slots = self.slots.iter().map(|slot| slot.peer);
        let readers: BTreeSet<_> = slots
            .chain(self.live.iter().map(|live| live.peer))
            .collect();
        if readers != granted.readers {
            granted.shard.set(granted.topic, readers.clone());
            granted.readers = readers;
        }
    }
}
