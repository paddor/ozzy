//! A ready primary handing its group to another voter by leaving its view.

use super::{DriverError, ReplicaDriver, Role};
use crate::{ReplicationError, ViewChangeError, VoterIndex};
use ozzy_proto::NodeId;
use std::time::Duration;

impl ReplicaDriver {
    /// Leave the view this ready primary leads for the smallest later view
    /// that `target` leads, and return that view. `None` while an accepted
    /// operation is uncommitted: the caller stops admission and asks again.
    ///
    /// The election is the ordinary one. `target` leads only if it installs
    /// the view; a silent target costs one more election.
    ///
    /// # Errors
    ///
    /// `WrongRole` unless this voter is the ready primary and `target`
    /// another voter; `UnknownVoter` for a node outside the configuration.
    pub fn step_down(&mut self, target: NodeId, now: Duration) -> Result<Option<u64>, DriverError> {
        self.observe_time(now)?;
        self.require_healthy()?;
        let target = self.configuration.voter_index(target)?;
        let Role::Normal(normal) = &self.role else {
            return Err(ReplicationError::WrongRole.into());
        };
        let snapshot = normal.snapshot();
        if !snapshot.ready_for_appends
            || VoterIndex::primary(snapshot.scope.view) != self.local
            || target == self.local
        {
            return Err(ReplicationError::WrongRole.into());
        }
        if snapshot.accepted != snapshot.committed {
            return Ok(None);
        }
        let view = (1..=2)
            .filter_map(|step| snapshot.scope.view.checked_add(step))
            .find(|view| VoterIndex::primary(*view) == target)
            .ok_or(ViewChangeError::ViewExhausted)?;
        self.change_view(view)?;
        Ok(Some(view))
    }
}
