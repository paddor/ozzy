//! Quorum progress: backup acknowledgments, commit announcements and their
//! receipt, and the installed view the primary repeats.

use super::NormalReplica;
use crate::{Commit, Prefix, PrepareOk, QuorumPolicy, ReplicationError, RetainedPrepareOk, Scope};
use ozzy_proto::NodeId;

impl NormalReplica {
    /// Construct a backup's cumulative durable vote, never a transport receipt.
    pub fn acknowledgment(&self) -> Result<PrepareOk, ReplicationError> {
        self.require_normal()?;
        if self.configuration.policy() != QuorumPolicy::Durable {
            return Err(ReplicationError::PolicyMismatch);
        }
        if self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        Ok(PrepareOk {
            scope: self.configuration.scope(),
            durable: self.prefix_at(self.journal.snapshot().durable)?,
        })
    }

    /// Count one authenticated backup's cumulative matching durable prefix.
    /// Duplicate deliveries/reconnections do not create extra voters.
    pub fn receive_ack(&mut self, from: NodeId, ack: PrepareOk) -> Result<(), ReplicationError> {
        if self.configuration.policy() != QuorumPolicy::Durable {
            return Err(ReplicationError::PolicyMismatch);
        }
        self.receive_prefix(from, ack.scope, ack.durable)
    }

    /// Vote for the complete validated prefix retained by the payload adapter.
    /// Physical writes are independent and cannot reclaim this prefix early.
    pub fn retained_acknowledgment(&self) -> Result<RetainedPrepareOk, ReplicationError> {
        self.require_normal()?;
        if self.configuration.policy() != QuorumPolicy::Replicated {
            return Err(ReplicationError::PolicyMismatch);
        }
        if self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        Ok(RetainedPrepareOk {
            scope: self.configuration.scope(),
            retained: self.accepted,
        })
    }

    /// Count one distinct backup's exact retained-memory prefix.
    pub fn receive_retained_ack(
        &mut self,
        from: NodeId,
        ack: RetainedPrepareOk,
    ) -> Result<(), ReplicationError> {
        if self.configuration.policy() != QuorumPolicy::Replicated {
            return Err(ReplicationError::PolicyMismatch);
        }
        self.receive_prefix(from, ack.scope, ack.retained)
    }

    fn receive_prefix(
        &mut self,
        from: NodeId,
        scope: Scope,
        prefix: Prefix,
    ) -> Result<(), ReplicationError> {
        self.require_scope(scope)?;
        let voter = self.configuration.voter_index(from)?;
        if !self.is_primary() || voter == self.local {
            return Err(ReplicationError::WrongRole);
        }
        if prefix.op < self.applied.op {
            return Ok(());
        }
        if self.prefix_at(prefix.op)? != prefix {
            return Err(self.conflict());
        }
        self.acknowledged[voter] = self.acknowledged[voter].max(prefix.op);
        self.advance_commit()
    }

    /// Construct the primary's current commit announcement or idle heartbeat.
    pub fn announcement(&self) -> Result<Commit, ReplicationError> {
        self.require_normal()?;
        if !self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        Ok(Commit {
            scope: self.configuration.scope(),
            committed: self.committed,
        })
    }

    /// Retransmit the immutable installed-view descriptor after publication.
    /// Bootstrap has no descriptor. Later appends do not change this snapshot;
    /// lagging backups install it first, then catch up through normal prepares.
    pub fn start_view(&self) -> Result<Option<crate::StartView>, ReplicationError> {
        self.require_normal()?;
        if !self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        Ok(self.installed_view)
    }

    pub(crate) fn activation_prefix(&self) -> Result<Prefix, ReplicationError> {
        self.require_normal()?;
        let snapshot = self.snapshot();
        if self
            .installed_view
            .is_none_or(|installed| installed.accepted != snapshot.accepted)
            || snapshot.committed != snapshot.accepted
            || snapshot.journal.durable < snapshot.accepted.op
            || (self.is_primary() && self.backup_acknowledged() < snapshot.accepted.op)
        {
            return Err(ReplicationError::ActivationPending);
        }
        Ok(snapshot.accepted)
    }

    pub(crate) fn observe_installed_start(
        &mut self,
        from: NodeId,
        start: crate::StartView,
    ) -> Result<(), ReplicationError> {
        self.require_scope(start.scope)?;
        self.require_primary(from)?;
        if self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        if self.installed_view != Some(start) {
            return Err(self.conflict());
        }
        Ok(())
    }

    /// Observe an authenticated primary's commit, including piggybacked commit.
    /// Visibility waits for this backup's configured local evidence. Missing
    /// payload returns `HistoryGap`; retry the announcement after catch-up.
    /// An announcement past the accepted prefix still commits everything this
    /// backup accepted in the view, so that much becomes visible at once.
    pub fn receive_commit(&mut self, from: NodeId, commit: Commit) -> Result<(), ReplicationError> {
        self.require_scope(commit.scope)?;
        self.require_primary(from)?;
        if self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        if commit.committed.op < self.applied.op {
            return Ok(());
        }
        if commit.committed.op > self.accepted.op
            && self.journal_tail.is_none()
            && self.accepted.op > self.known_commit
        {
            // Otherwise a backup whose window is full behind a primary that
            // already committed past it never learns a commit it can apply,
            // and never releases the window the next suffix needs.
            self.known_commit = self.accepted.op;
            self.advance_commit()?;
        }
        if self.prefix_at(commit.committed.op)? != commit.committed {
            return Err(self.conflict());
        }
        self.known_commit = self.known_commit.max(commit.committed.op);
        self.advance_commit()
    }
}
