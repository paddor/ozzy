//! Delay step timing shared by the local and replicated actors. Only the
//! primary releases or carries; one step runs at a time, and its proposal
//! takes the maintenance slot.

use super::ingress::Proposals;
use crate::replica_journal::{JournalCompletion, JournalError, ReleaseTurn, ReplicaJournal};
use std::time::Duration;

/// Wait after a step that built no release though records were due, such as
/// when every proposal arena is leased.
const RETRY: Duration = Duration::from_millis(10);

/// Wait after validation refused a release, such as when the journal has
/// no room for it until retention frees some.
const REFUSED: Duration = Duration::from_millis(100);

/// The release step in flight, and when the next one may start.
#[derive(Debug, Default)]
pub(super) struct Releases {
    running: Option<JournalCompletion<ReleaseTurn>>,
    retry_at: Option<Duration>,
}

impl Releases {
    /// When the next step is due: the earliest committed held record's due
    /// time by the journal clock, placed on actor time, or now while a carry
    /// is pending.
    pub(super) fn at(&self, journal: &ReplicaJournal, now: Duration) -> Option<Duration> {
        if self.running.is_some() {
            return None;
        }
        let due = now.saturating_add(journal.release_wait()?);
        Some(self.retry_at.map_or(due, |retry| due.max(retry)))
    }

    /// Whether a step may start now. A queued or validating maintenance
    /// proposal goes first, so the next step sees its release.
    pub(super) fn due(
        &self,
        journal: &ReplicaJournal,
        proposals: &Proposals,
        now: Duration,
    ) -> bool {
        !proposals.holds_maintenance() && self.at(journal, now).is_some_and(|at| at <= now)
    }

    pub(super) fn start(&mut self, step: JournalCompletion<ReleaseTurn>) {
        self.retry_at = None;
        assert!(self.running.replace(step).is_none());
    }

    pub(super) const fn running(&self) -> bool {
        self.running.is_some()
    }

    /// The running step, to await.
    pub(super) fn step(&mut self) -> Option<&mut JournalCompletion<ReleaseTurn>> {
        self.running.as_mut()
    }

    /// Wait before the next step after a maintenance proposal was not
    /// admitted, so a release that keeps failing cannot spin.
    pub(super) fn back_off(&mut self, now: Duration, refused: bool) {
        let wait = if refused { REFUSED } else { RETRY };
        self.retry_at = Some(now.saturating_add(wait));
    }

    /// Queue a finished step's proposal, or wait before the next step. A
    /// step that loaded held records lets the next one start at once.
    pub(super) fn complete(
        &mut self,
        turn: Result<ReleaseTurn, JournalError>,
        now: Duration,
        proposals: &mut Proposals,
    ) -> Result<(), JournalError> {
        self.running = None;
        let turn = turn?;
        match turn.proposal {
            Some(buffer) => proposals.set_maintenance(buffer),
            // Loaded held records may be due already.
            None if turn.loaded => {}
            None => self.retry_at = Some(now.saturating_add(RETRY)),
        }
        Ok(())
    }
}
