//! Time-driven polling: deadlines, heartbeats, commit announcements, and
//! election sends, at most one action per poll.

use super::{Action, DriverError, ReplicaDriver, Role};
use crate::wire::Control;
use crate::{ReplicaSnapshot, ReplicationError, Status, ViewChangeError, VoterIndex};
use std::time::Duration;

impl ReplicaDriver {
    /// Advance deadlines and emit at most one action, without waiting for I/O.
    /// Repeated polls at one instant do not create unbounded retransmission.
    pub fn poll(&mut self, now: Duration) -> Result<Option<Action>, DriverError> {
        self.observe_time(now)?;
        match &self.role {
            Role::Installing(installing) => {
                installing.require_pending()?;
                return self.poll_installing(now);
            }
            Role::ViewChanging(changing)
                if changing.normal_snapshot().status == Status::Faulted =>
            {
                return Err(ViewChangeError::Faulted.into());
            }
            Role::Fenced(_) => return Err(ViewChangeError::Faulted.into()),
            Role::Normal(_) | Role::ViewChanging(_) => {}
        }
        if let Some(action) = self.exit_echo_action() {
            return Ok(Some(action));
        }
        if let Some(normal) = self.normal() {
            let snapshot = normal.snapshot();
            if snapshot.status != Status::Normal {
                return Err(ReplicationError::NotNormal.into());
            }
            if !self.normal_timed_out(&snapshot) {
                return self.poll_normal(&snapshot, now);
            }
            self.request_exit()?;
            if self.normal().is_some() {
                return Ok(self.exit_action());
            }
        } else if now >= self.election_deadline && !self.promise_pending() {
            // A voter still waiting on its own promise has no evidence about
            // the view. With stalled storage it would keep helping a peer's
            // timeout end views the others are installing.
            self.request_exit()?;
        }
        if let Some(action) = self.exit_action() {
            return Ok(Some(action));
        }
        if now >= self.retry_at {
            self.start_pending = true;
            self.report_pending = true;
            self.retry_at = now.saturating_add(self.timing.retransmit);
        }
        self.poll_election()
    }

    fn poll_installing(&mut self, now: Duration) -> Result<Option<Action>, DriverError> {
        if let Some(action) = self.exit_echo_action() {
            return Ok(Some(action));
        }
        if now >= self.election_deadline {
            self.request_exit()?;
        }
        // Suspicion can travel, but publication must settle before a promise.
        Ok(self.exit_action())
    }

    /// A healthy normal role withdraws suspicion; its primary announces progress.
    fn poll_normal(
        &mut self,
        snapshot: &ReplicaSnapshot,
        now: Duration,
    ) -> Result<Option<Action>, DriverError> {
        self.withdraw_exit();
        if snapshot.accepted == snapshot.committed && snapshot.ready_for_appends {
            self.progress_deadline = now.saturating_add(self.timing.primary_timeout);
        }
        if VoterIndex::primary(snapshot.scope.view) != self.local {
            return Ok(None);
        }
        if self.commit_pending {
            self.commit_pending = false;
            return self.announcement().map(Some);
        }
        if let Some(action) = self.activation_action()? {
            return Ok(Some(action));
        }
        if now < self.retry_at {
            return Ok(None);
        }
        self.retry_at = now.saturating_add(self.timing.heartbeat);
        if let Some(start) = self.require_normal()?.start_view()? {
            self.commit_pending = true;
            return Ok(Some(Action::Broadcast(Control::StartView(start))));
        }
        self.announcement().map(Some)
    }

    fn announcement(&self) -> Result<Action, DriverError> {
        let commit = self.require_normal()?.announcement()?;
        Ok(Action::Broadcast(Control::Commit(commit)))
    }

    /// Persist the view promise, then send the first- and second-phase messages.
    fn poll_election(&mut self) -> Result<Option<Action>, DriverError> {
        // A normal role returned or entered this election above.
        let Role::ViewChanging(changing) = &mut self.role else {
            return Ok(None);
        };
        if changing.promised_view() != changing.scope().view {
            return match changing.begin_promise() {
                Ok(ticket) => Ok(Some(Action::PersistPromise(ticket))),
                Err(ViewChangeError::StoragePending | ViewChangeError::PromisePending) => Ok(None),
                Err(error) => Err(error.into()),
            };
        }
        if self.start_pending {
            let message = changing.start_message()?;
            self.start_pending = false;
            return Ok(Some(Action::Broadcast(Control::StartViewChange(message))));
        }
        if self.report_pending {
            match changing.report() {
                Ok(report) => {
                    self.report_pending = false;
                    let primary = VoterIndex::primary(report.scope.view);
                    if primary != self.local {
                        return Ok(Some(Action::Send {
                            to: self.configuration.voter(primary),
                            message: Control::DoViewChange(report),
                        }));
                    }
                }
                Err(ViewChangeError::StartQuorumMissing) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(None)
    }
}
