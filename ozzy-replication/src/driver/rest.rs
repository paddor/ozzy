//! Liveness the adapter covers by other means, and a primary's rest.
//!
//! An idle group repeats heartbeats and votes only to prove its voters
//! alive. An adapter with another proof, such as one beat per broker pair,
//! covers a voter here: a backup then waits for no primary contact, and a
//! primary doubts nothing. A resting primary also sends no heartbeat. The
//! adapter owns that proof and uncovers the voter when it fails.

use super::{DriverError, ReplicaDriver, Role, VoterIndex, VoterSet};
use crate::Status;
use ozzy_proto::NodeId;
use std::time::Duration;

/// Whether a primary repeats its heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Heartbeat {
    Sending,
    Resting,
}

impl ReplicaDriver {
    /// Whether this normal role has nothing outstanding: every accepted
    /// operation is committed, applied, and durable here, and no installed
    /// tail waits for a backup. Only then may heartbeats and votes stop.
    pub fn settled(&self) -> bool {
        let Role::Normal(normal) = &self.role else {
            return false;
        };
        let snapshot = normal.snapshot();
        snapshot.status == Status::Normal
            && snapshot.ready_for_appends
            && snapshot.accepted == snapshot.committed
            && snapshot.applied == snapshot.committed
            && snapshot.journal.durable >= snapshot.accepted.op
            && snapshot.journal_tail.is_none()
            && !self.commit_pending
            && self.activation_pending.is_empty()
    }

    /// Let the adapter's own proof stand in for `voter`'s traffic in this
    /// view. A view change uncovers every voter.
    ///
    /// # Errors
    ///
    /// `NotNormal` outside a normal role, `UnknownVoter` for a node outside
    /// the configuration, and `WrongRole` for the local voter or, on a
    /// backup, any voter but the primary.
    pub fn cover(&mut self, voter: NodeId) -> Result<(), DriverError> {
        let voter = self.configuration.voter_index(voter)?;
        let primary = VoterIndex::primary(self.require_normal()?.snapshot().scope.view);
        if voter == self.local || (primary != self.local && voter != primary) {
            return Err(crate::ReplicationError::WrongRole.into());
        }
        self.covered.insert(voter);
        Ok(())
    }

    /// End the cover of `voter`. `heard` is when the adapter's proof last
    /// held: the contact deadline of a backup and the doubt of a primary
    /// count from it, as if the voter had spoken then.
    pub fn uncover(&mut self, voter: NodeId, heard: Duration) -> Result<(), DriverError> {
        let voter = self.configuration.voter_index(voter)?;
        if !self.covered.contains(voter) {
            return Ok(());
        }
        self.covered.remove(voter);
        let heard = heard.min(self.now);
        let Some(normal) = self.normal() else {
            return Ok(());
        };
        if VoterIndex::primary(normal.snapshot().scope.view) == self.local {
            self.backup_heard_at = self.backup_heard_at.max(heard);
        } else {
            self.contact_deadline = heard.saturating_add(self.timing.primary_timeout);
        }
        Ok(())
    }

    /// Whether the adapter covers `voter` in this view.
    pub fn covered(&self, voter: NodeId) -> bool {
        self.configuration
            .voter_index(voter)
            .is_ok_and(|voter| self.covered.contains(voter))
    }

    /// Stop or resume this primary's heartbeat. A resting primary still
    /// announces commits and answers elections. A view change ends the rest.
    pub fn rest(&mut self, resting: bool) {
        if self.resting() && !resting {
            // The next poll sends the heartbeat the rest held back.
            self.retry_at = self.now;
        }
        self.heartbeat = if resting {
            Heartbeat::Resting
        } else {
            Heartbeat::Sending
        };
    }

    /// Whether this primary's heartbeat is stopped.
    pub fn resting(&self) -> bool {
        self.heartbeat == Heartbeat::Resting
    }

    pub(super) fn uncover_all(&mut self) {
        self.covered = VoterSet::EMPTY;
        self.heartbeat = Heartbeat::Sending;
    }

    /// Whether the adapter covers this backup's primary.
    pub(super) fn primary_covered(&self, primary: VoterIndex) -> bool {
        self.covered.contains(primary)
    }
}
