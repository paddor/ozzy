//! Retention turn timing shared by the local and replicated actors.

use super::ingress::Proposals;
use super::periodic::Periodic;
use crate::replica_journal::{IdentityRetention, RetentionTurn};
use std::time::Duration;

/// Pending retention work: the next settled retention turn, and whether
/// identity expiry owes a bounded step that runs between foreground turns
/// without waiting for a settled journal.
#[derive(Debug, Clone, Copy)]
pub(super) struct RetentionDue {
    schedule: Option<Periodic>,
    /// A turn reported retention disabled; a repeated step enables it again.
    enabled: bool,
    identities: bool,
}

/// The retention work to start now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RetentionStep {
    /// A full turn on a settled journal.
    Turn,
    /// One owed identity-expiry step between foreground turns.
    Identities,
}

impl RetentionDue {
    /// The first turn is due one interval after start. `None` disables turns.
    pub(super) fn new(interval: Option<Duration>) -> Self {
        Self {
            schedule: interval.map(|interval| Periodic::new(interval, ())),
            enabled: true,
            identities: false,
        }
    }

    /// When the next turn is due, if any.
    pub(super) fn at(&self) -> Option<Duration> {
        self.schedule
            .filter(|_| self.enabled)
            .map(|schedule| schedule.next())
    }

    pub(super) fn due(&self, now: Duration) -> bool {
        self.at().is_some_and(|at| now >= at)
    }

    /// The step to start now that a turn is due. A settled journal gets a
    /// full turn; otherwise an owed identity step runs if `identities_ready`.
    /// A started step makes the next turn due one interval from `now`.
    pub(super) fn start(
        &mut self,
        now: Duration,
        settled: bool,
        identities_ready: bool,
    ) -> Option<RetentionStep> {
        let step = if settled {
            RetentionStep::Turn
        } else if self.identities && identities_ready {
            RetentionStep::Identities
        } else {
            return None;
        };
        self.identities = false;
        if let Some(schedule) = &mut self.schedule {
            schedule.restart(now);
        }
        Some(step)
    }

    /// Apply a finished retention turn to the schedule and proposals.
    pub(super) fn complete_turn(
        &mut self,
        turn: RetentionTurn,
        now: Duration,
        proposals: &mut Proposals,
    ) {
        if turn.expired_identities != 0 {
            proposals.release_capacity();
        }
        if !turn.enabled {
            self.enabled = false;
        } else if turn.more_work {
            // Identity expiry continues without draining the journal again;
            // record retention repeats its settled turn.
            self.identities = turn.expired_identities != 0;
            self.repeat(now);
        }
        if let Some(buffer) = turn.proposal {
            proposals.set_maintenance(buffer);
        }
    }

    /// Apply a finished identity-expiry step to the schedule and proposals.
    pub(super) fn complete_identities(
        &mut self,
        turn: IdentityRetention,
        now: Duration,
        proposals: &mut Proposals,
    ) {
        if turn.expired != 0 {
            proposals.release_capacity();
        }
        if turn.more_work {
            self.identities = true;
            self.repeat(now);
        }
        if let Some(buffer) = turn.proposal {
            proposals.set_maintenance(buffer);
        }
    }

    fn repeat(&mut self, now: Duration) {
        if let Some(schedule) = &mut self.schedule {
            self.enabled = true;
            schedule.repeat(now);
        }
    }
}
