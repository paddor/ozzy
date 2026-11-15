//! Partitions that join and leave a running shard.

use super::{PartitionActor, PartitionActors, PartitionError};
use ozzy_proto::GroupId;

/// An actor the shard did not take. The caller still owns its shutdown.
#[derive(Debug)]
pub struct Uninserted {
    /// Why the shard refused the actor.
    pub error: PartitionError,
    /// The refused actor, untouched.
    pub actor: PartitionActor,
}

impl PartitionActors {
    /// Add `actor` to the running shard. Its first visit comes with the
    /// next poll. The shard holds at most the count given at construction,
    /// and one actor per group.
    pub fn insert(&mut self, actor: PartitionActor) -> Result<(), Box<Uninserted>> {
        let error = if self.stopped {
            Some(PartitionError::Stopped)
        } else if self.actors.len() >= self.maximum
            || self.wakeups.as_ref().is_some_and(|wakeups| wakeups.full())
        {
            Some(PartitionError::Limits)
        } else if self.index.contains_key(&actor.group()) {
            Some(PartitionError::Duplicate)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(Box::new(Uninserted { error, actor }));
        }
        let slot = self.actors.len();
        self.index.insert(actor.group(), slot);
        self.actors.push(actor);
        self.native.push(None);
        self.coordinators.push(None);
        self.readers.push(None);
        self.logs.push(None);
        self.install_beats(slot);
        if let Some(wakeups) = &mut self.wakeups {
            wakeups.grow();
        }
        // A scan in progress starts over and reaches the new actor.
        self.remaining = 0;
        Ok(())
    }

    /// Take the actor of `group` out of the running shard, with its
    /// services dropped: open client and log requests end unanswered or
    /// stopped. The caller drains the actor with its `shutdown`. Input that
    /// still names the group afterwards is unknown.
    pub fn remove(&mut self, group: GroupId) -> Result<PartitionActor, PartitionError> {
        if self.stopped {
            return Err(PartitionError::Stopped);
        }
        let slot = self.index.remove(&group).ok_or(PartitionError::Unknown)?;
        drop(self.native.swap_remove(slot));
        drop(self.coordinators.swap_remove(slot));
        drop(self.readers.swap_remove(slot));
        drop(self.logs.swap_remove(slot));
        let actor = self.actors.swap_remove(slot);
        if matches!(actor, PartitionActor::Replicated(_)) {
            // Its peers may still trust the role it had.
            self.request_withdrawal();
        }
        if let Some(wakeups) = &mut self.wakeups {
            wakeups.swap_remove(slot);
        }
        // The last actor moved into the freed slot, with its marks.
        if let Some(moved) = self.actors.get(slot) {
            self.index.insert(moved.group(), slot);
        }
        self.recovered.retain(|recovered| *recovered != group);
        self.next = 0;
        self.remaining = 0;
        Ok(actor)
    }
}
