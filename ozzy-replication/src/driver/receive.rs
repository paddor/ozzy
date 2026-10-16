//! Inbound peer traffic: election and commit controls, and prepares from
//! the current primary.

use super::{DriverError, ReplicaDriver, Role};
use crate::wire::Control;
use crate::{
    Admission, PreparedOperation, ReplicationError, Scope, StartView, ViewChangeError, VoterIndex,
};
use ozzy_proto::NodeId;
use std::time::Duration;

impl ReplicaDriver {
    /// Receive one authenticated, wire-validated control. Stale views are ignored.
    /// A timeout only sends volatile `EXIT_VIEW` suspicion until a quorum agrees.
    /// A durably promised `START_VIEW_CHANGE` instead fences older authority,
    /// including an intact voter's restart request. These distinct messages must
    /// not be interchanged. Senders must obey the crash-fault protocol, not merely
    /// supply a claimed node identity.
    ///
    /// A returned `START_VIEW` needs bounded retention by the adapter until its
    /// local promise completes, then verified selected installation. It never
    /// activates a normal role here. Exact duplicate installed descriptors return
    /// `None` without reinstalling and permit replay of the normal durable ACK.
    /// Normal ACK grants remain adapter-owned.
    pub fn receive(
        &mut self,
        from: NodeId,
        message: Control,
        now: Duration,
    ) -> Result<Option<StartView>, DriverError> {
        self.observe_time(now)?;
        let voter = self.configuration.voter_index(from)?;
        if voter == self.local {
            return Err(ReplicationError::WrongRole.into());
        }
        let scope = message.scope();
        if scope
            != (Scope {
                view: scope.view,
                ..self.configuration.scope()
            })
        {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        if let Control::ExitView(_) = message {
            self.receive_exit(voter, scope)?;
            return Ok(None);
        }
        if scope.view < self.scope().view {
            return Ok(None);
        }
        self.check_role(voter, message)?;
        if let Control::StartViewChange(_) = message {
            self.require_healthy()?;
            // A first-phase sender has durably fenced its previous authority.
            // Timeout-only suspicion uses EXIT_VIEW and cannot reach this path.
            if scope.view > self.scope().view {
                self.change_view(scope.view)?;
            }
            if scope.view != self.scope().view {
                return Ok(None);
            }
        }
        if scope.view > self.scope().view {
            self.change_view(scope.view)?;
        }
        match &mut self.role {
            Role::Normal(_) => self.receive_normal(from, voter, message, now)?,
            Role::ViewChanging(changing) => match message {
                Control::StartViewChange(start) => changing.receive_start(from, start)?,
                Control::DoViewChange(report) => changing.receive_report(from, report)?,
                Control::StartView(start) => return Ok(Some(start)),
                Control::Commit(_)
                | Control::PrepareOk { .. }
                | Control::PrepareRetained { .. }
                | Control::ExitView(_) => {}
            },
            Role::Installing(installing) => installing.observe_view(from, scope)?,
            Role::Fenced(_) => return Err(ViewChangeError::Faulted.into()),
        }
        Ok(None)
    }

    /// Only the primary announces commits and installed views, and only the
    /// primary collects acknowledgments and election reports.
    fn check_role(&self, voter: VoterIndex, message: Control) -> Result<(), DriverError> {
        let primary = VoterIndex::primary(message.scope().view);
        match message {
            Control::Commit(_) | Control::StartView(_) if voter != primary => {
                Err(ReplicationError::WrongRole.into())
            }
            Control::PrepareOk { .. }
            | Control::PrepareRetained { .. }
            | Control::DoViewChange(_)
                if self.local != primary =>
            {
                Err(ReplicationError::WrongRole.into())
            }
            _ => Ok(()),
        }
    }

    /// Apply a current-view commit, acknowledgment, or installed view to the
    /// normal role, and restart the deadlines its progress satisfies.
    fn receive_normal(
        &mut self,
        from: NodeId,
        voter: VoterIndex,
        message: Control,
        now: Duration,
    ) -> Result<(), DriverError> {
        let Role::Normal(normal) = &mut self.role else {
            return Ok(());
        };
        let committed = normal.snapshot().committed;
        match message {
            Control::Commit(commit) => {
                normal.receive_commit(from, commit)?;
                self.contact_deadline = now.saturating_add(self.timing.primary_timeout);
            }
            Control::PrepareOk { ack, .. } => normal.receive_ack(from, ack)?,
            Control::PrepareRetained { ack, .. } => {
                normal.receive_retained_ack(from, ack)?;
            }
            Control::StartView(start) => {
                normal.observe_installed_start(from, start)?;
                self.contact_deadline = now.saturating_add(self.timing.primary_timeout);
            }
            _ => return Ok(()),
        }
        if let Some(prefix) = message.acknowledged()
            && let Some(start) = normal.start_view()?
            && prefix == start.accepted
            && start.accepted.op < normal.snapshot().committed.op
        {
            // Selected history plus an established quorum authorizes
            // replay. An old vote alone cannot create commit authority.
            self.activation_pending.insert(voter);
        }
        if normal.snapshot().committed != committed {
            self.progress_deadline = now.saturating_add(self.progress_timeout());
        }
        Ok(())
    }

    /// Admit an already body/application-validated canonical group. The returned
    /// ticket schedules disk work; transmission may overlap that work. Later
    /// views require selected installation, never implicit normal activation.
    pub fn prepare(
        &mut self,
        from: NodeId,
        scope: Scope,
        operations: &[PreparedOperation],
        now: Duration,
    ) -> Result<Admission, DriverError> {
        self.observe_time(now)?;
        let voter = self.configuration.voter_index(from)?;
        if scope
            != (Scope {
                view: scope.view,
                ..self.configuration.scope()
            })
        {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        if voter != VoterIndex::primary(scope.view) {
            return Err(ReplicationError::WrongRole.into());
        }
        if scope.view > self.scope().view {
            self.change_view(scope.view)?;
        }
        let Role::Normal(normal) = &mut self.role else {
            return Err(ReplicationError::NotNormal.into());
        };
        let previous = normal.snapshot();
        let admitted = normal.prepare(from, scope, operations)?;
        if normal.snapshot().accepted != previous.accepted
            && previous.accepted == previous.committed
            && previous.ready_for_appends
        {
            self.progress_deadline = now.saturating_add(self.timing.primary_timeout);
        }
        if normal.snapshot().accepted != previous.accepted
            && previous.accepted.op == self.storage_progress(&previous)
        {
            self.persistence_deadline = now.saturating_add(self.timing.primary_timeout);
        }
        self.contact_deadline = now.saturating_add(self.timing.primary_timeout);
        Ok(admitted)
    }
}
