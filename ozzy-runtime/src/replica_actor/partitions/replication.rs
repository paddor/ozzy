//! Broker replication input, follower receive windows and link fences,
//! delivered to the owning partition actor.

use super::{PartitionActor, PartitionActors, PartitionError, SessionReplacement};
use omq_tokio::Message;
use ozzy_proto::{GroupId, LinkSessionId, NodeId};
use std::time::Duration;

impl PartitionActors {
    /// Observe a ready follower's exact receive epoch for shard admission.
    /// Local partitions and replicated leaders have no follower receive window.
    pub fn receive_receipt(
        &self,
        group: GroupId,
    ) -> Option<(NodeId, ozzy_replication::flow::Report)> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
            PartitionActor::Replicated(actor) => actor.receive_receipt(),
        }
    }

    /// Exact follower epoch remains observable while its transport link or
    /// application intake is fenced. It does not grant receive capacity.
    pub fn receive_channel(&self, group: GroupId) -> Option<ozzy_replication::flow::Channel> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
            PartitionActor::Replicated(actor) => actor.receive_channel(),
        }
    }

    /// Local follower room after retained operations. Actual shard memory
    /// backing remains independently bounded.
    pub fn receive_capacity(&self, group: GroupId) -> Option<ozzy_replication::PipelineLimits> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => None,
            PartitionActor::Replicated(actor) => actor.receive_capacity(),
        }
    }

    /// Requested history replies use separate correlation from normal flow.
    /// They can be needed before the partition is ready to receive APPENDs.
    pub fn ops_receive_demand(&self, group: GroupId) -> Option<ozzy_replication::wire::FetchOps> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Local(_) => None,
            PartitionActor::Replicated(actor) => actor.ops_receive_demand(),
            PartitionActor::Recovering(actor) => actor.ops_receive_demand(),
        }
    }

    /// Requested checkpoint replies share the bounded repair data plane.
    pub fn checkpoint_receive_demand(
        &self,
        group: GroupId,
    ) -> Option<ozzy_replication::wire::CheckpointRequest> {
        if self.stopped {
            return None;
        }
        match &self.actors[*self.index.get(&group)?] {
            PartitionActor::Recovering(actor) => actor.checkpoint_receive_demand(),
            PartitionActor::Local(_) | PartitionActor::Replicated(_) => None,
        }
    }

    /// Staged, validating, or installing data keeps its reservation until settled.
    pub fn receive_has_work(&self, group: GroupId) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        Ok(match &self.actors[slot] {
            PartitionActor::Local(_) => false,
            PartitionActor::Replicated(actor) => actor.receive_has_work(),
            PartitionActor::Recovering(actor) => actor.receive_has_work(),
        })
    }

    /// Synchronous bounded delivery after shared frontend admission. The actor
    /// validates the complete protocol command and its own current link session.
    /// Local partitions have no broker replication commands.
    pub fn receive(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
    ) -> Result<(), PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        self.observe(now)?;
        self.touch(slot);
        match &mut self.actors[slot] {
            PartitionActor::Local(_) => Err(PartitionError::Mode),
            PartitionActor::Replicated(actor) => actor
                .receive(message, now)
                .map_err(|source| PartitionError::Replica { group, source }),
            PartitionActor::Recovering(actor) => actor
                .receive(message, now)
                .map_err(|source| PartitionError::Replica { group, source }),
        }
    }

    /// Bounded follower delivery; false leaves one PEER repair with its shard.
    pub fn receive_data(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
    ) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        self.observe(now)?;
        let (consumed, changed) = match &mut self.actors[slot] {
            PartitionActor::Replicated(actor) => {
                let committed = actor.status().normal.map(|normal| normal.committed);
                let consumed = actor
                    .receive_data(message, now)
                    .map_err(|source| PartitionError::Replica { group, source })?;
                (
                    consumed,
                    committed != actor.status().normal.map(|normal| normal.committed),
                )
            }
            PartitionActor::Local(_) | PartitionActor::Recovering(_) => (true, false),
        };
        if consumed || changed {
            self.touch(slot);
        }
        Ok(consumed)
    }

    /// Apply one independently established link fence to its partition actor.
    pub fn replace_session(
        &mut self,
        group: GroupId,
        peer: NodeId,
        SessionReplacement {
            expected,
            replacement,
        }: SessionReplacement,
        now: Duration,
    ) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        self.observe(now)?;
        self.touch(slot);
        match &mut self.actors[slot] {
            PartitionActor::Local(_) => Err(PartitionError::Mode),
            PartitionActor::Replicated(actor) => actor
                .replace_session(peer, expected, replacement, now)
                .map_err(|source| PartitionError::Replica { group, source }),
            PartitionActor::Recovering(actor) => actor
                .replace_session(peer, expected, replacement, now)
                .map_err(|source| PartitionError::Replica { group, source }),
        }
    }

    /// Fence one independently observed disconnect on its partition actor.
    pub fn disconnect_session(
        &mut self,
        group: GroupId,
        peer: NodeId,
        expected: LinkSessionId,
        now: Duration,
    ) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        self.observe(now)?;
        self.touch(slot);
        match &mut self.actors[slot] {
            PartitionActor::Local(_) => Err(PartitionError::Mode),
            PartitionActor::Replicated(actor) => actor
                .disconnect_session(peer, expected, now)
                .map_err(|source| PartitionError::Replica { group, source }),
            PartitionActor::Recovering(actor) => actor
                .disconnect_session(peer, expected, now)
                .map_err(|source| PartitionError::Replica { group, source }),
        }
    }
}
