//! Quorum-supported timeout suspicion, independent of durable election evidence.

use super::{Action, DriverError, ReplicaDriver, Role};
use crate::wire::Control;
use crate::{Scope, VoterIndex, VoterSet};

impl ReplicaDriver {
    pub(super) fn receive_exit(
        &mut self,
        voter: VoterIndex,
        scope: Scope,
    ) -> Result<(), DriverError> {
        self.require_healthy()?;
        // Neither a distant wish nor a duplicate from another view is evidence
        // that anyone has entered that view. START_VIEW_CHANGE supplies that fence.
        if scope.view < self.scope().view {
            // This voter left that view, possibly on the sender's own request
            // plus a local timeout, before its suspicion went out. Without
            // support the sender cannot leave until this voter's durable
            // promise lets START_VIEW_CHANGE reach it. After that promise,
            // election and normal traffic carry the newer view themselves.
            //
            // A peer that also left the view cannot tell this answer from a
            // request. Answer only the sender, and each sender at most once
            // per retransmission interval, so two such voters fall silent.
            if self.promise_pending() && self.now >= self.exit_echo_at[voter] {
                self.exit_echo_at[voter] = self.now.saturating_add(self.timing.retransmit);
                let mut asked = match self.exit_echo {
                    Some((old, asked)) if old == scope => asked,
                    _ => VoterSet::EMPTY,
                };
                asked.insert(voter);
                self.exit_echo = Some((scope, asked));
            }
            return Ok(());
        }
        if scope != self.scope() {
            return Ok(());
        }
        if let Some(normal) = self.normal() {
            let snapshot = normal.snapshot();
            if !self.normal_timed_out(&snapshot) {
                self.withdraw_exit();
            }
        }
        self.exit_votes.insert(voter);
        self.advance_if_exit_quorum()
    }

    pub(super) fn request_exit(&mut self) -> Result<(), DriverError> {
        self.exit_retry_at.get_or_insert(self.now);
        self.exit_votes.insert(self.local);
        self.advance_if_exit_quorum()
    }

    fn advance_if_exit_quorum(&mut self) -> Result<(), DriverError> {
        if self.exit_votes.len() >= 2 {
            if self
                .normal()
                .is_none_or(|normal| !normal.snapshot().ready_for_appends)
            {
                self.election_delay = self
                    .election_delay
                    .saturating_mul(2)
                    .min(self.timing.max_election_timeout);
            }
            self.next_view()?;
        }
        Ok(())
    }

    pub(super) fn withdraw_exit(&mut self) {
        self.exit_votes.remove(self.local);
        self.exit_retry_at = None;
    }

    fn promise_pending(&self) -> bool {
        matches!(
            &self.role,
            Role::ViewChanging(changing) if changing.promised_view() != changing.scope().view
        )
    }

    /// Support one sender's exit from an older view per call. The sender
    /// retransmits, so a lost answer is repeated after that interval.
    pub(super) fn exit_echo_action(&mut self) -> Option<Action> {
        let (scope, mut asked) = self.exit_echo.take()?;
        let voter = asked.pop_first()?;
        if !asked.is_empty() {
            self.exit_echo = Some((scope, asked));
        }
        Some(Action::Send {
            to: self.configuration.voter(voter),
            message: Control::ExitView(scope),
        })
    }

    pub(super) fn exit_action(&mut self) -> Option<Action> {
        if self.now < self.exit_retry_at? {
            return None;
        }
        self.exit_retry_at = Some(self.now.saturating_add(self.timing.retransmit));
        Some(Action::Broadcast(Control::ExitView(self.scope())))
    }
}
