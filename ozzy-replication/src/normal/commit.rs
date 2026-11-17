//! Quorum progress: backup acknowledgments, commit announcements and their
//! receipt, and the installed view the primary repeats.

use super::NormalReplica;
use crate::{Commit, Prefix, PrepareOk, ReplicationError};
use ozzy_proto::NodeId;

impl NormalReplica {
    /// Construct a backup's cumulative durable vote, never a transport receipt.
    pub fn acknowledgment(&self) -> Result<PrepareOk, ReplicationError> {
        self.require_normal()?;
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
        let (scope, prefix) = (ack.scope, ack.durable);
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

    /// The prefix every backup of this primary's view holds applied, so
    /// that none is replayed operations at or below it. A view that an
    /// election installed starts with its whole selected log applied on
    /// every backup. A primary that resumed its view announces its log as
    /// it found it: a backup may hold the end of that log unapplied and
    /// need it again in a new receive epoch.
    pub fn replay_floor(&self) -> Result<Prefix, ReplicationError> {
        Ok(self
            .start_view()?
            .filter(|_| !self.rejoined)
            .map_or(Prefix::GENESIS, |start| start.accepted))
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
        // A view has one descriptor per incarnation of its primary: one that
        // resumed the view after a clean stop announces its own log. A
        // rejoined backup installed its own log, not any descriptor.
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
        if let Some(tail) = self.journal_tail.filter(|_| self.rejoined) {
            // The whole tail came from this primary or the log it installed,
            // so a commit at or past its end commits it. One inside it has
            // no local evidence to check against, and waits for a later one.
            if commit.committed.op < tail.op && commit.committed.op != self.committed.op {
                return Ok(());
            }
        }
        if commit.committed.op > self.accepted.op
            && (self.journal_tail.is_none() || self.rejoined)
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
