//! Delay step timing shared by the local and replicated actors. Only the
//! leader releases or carries; one step runs at a time, and its proposal
//! takes the maintenance slot. Every copy also loads sealed held rows beside
//! writes, one load at a time.

use super::ingress::Proposals;
use crate::replica_journal::{JournalCompletion, JournalError, ReleaseTurn, ReplicaJournal};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

/// Wait after a step that built no release though records were due, such as
/// when every proposal arena is leased.
const RETRY: Duration = Duration::from_millis(10);

/// Wait after validation refused a release, such as when the journal has
/// no room for it until retention frees some.
const REFUSED: Duration = Duration::from_millis(100);

/// A finished release step or held-row load.
pub(super) enum Finished {
    Step(Result<ReleaseTurn, JournalError>),
    Load(Result<ReleaseTurn, JournalError>),
}

/// The release step in flight, and when the next one may start.
#[derive(Debug, Default)]
pub(super) struct Releases {
    running: Option<JournalCompletion<ReleaseTurn>>,
    retry_at: Option<Duration>,
    /// Held-row loads beside writes, on every copy.
    pub(super) loads: HeldLoads,
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
    /// proposal goes first, so the next step sees its release. An admitted
    /// one still `committing` goes first too: the next step then gathers
    /// what came due meanwhile, so steps grow with load.
    pub(super) fn due(
        &self,
        journal: &ReplicaJournal,
        proposals: &Proposals,
        committing: bool,
        now: Duration,
    ) -> bool {
        !proposals.holds_maintenance()
            && !committing
            && self.at(journal, now).is_some_and(|at| at <= now)
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

    /// Whether a step or a held-row load runs.
    pub(super) const fn busy(&self) -> bool {
        self.running.is_some() || self.loads.running()
    }

    /// The next step or held-row load to finish.
    pub(super) fn finished(&mut self) -> impl Future<Output = Finished> + '_ {
        std::future::poll_fn(move |cx| {
            if let Some(step) = self.running.as_mut()
                && let Poll::Ready(turn) = Pin::new(step).poll(cx)
            {
                return Poll::Ready(Finished::Step(turn));
            }
            if let Some(load) = self.loads.running.as_mut()
                && let Poll::Ready(turn) = Pin::new(load).poll(cx)
            {
                return Poll::Ready(Finished::Load(turn));
            }
            Poll::Pending
        })
    }

    /// Complete what `finished` returned.
    pub(super) fn finish(
        &mut self,
        finished: Finished,
        now: Duration,
        proposals: &mut Proposals,
    ) -> Result<(), JournalError> {
        match finished {
            Finished::Step(turn) => self.complete(turn, now, proposals),
            Finished::Load(turn) => self.loads.complete(turn, now),
        }
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

/// The held-row load in flight, and when the next one may start. Every copy
/// loads sealed held rows once few are loaded, so release steps and
/// follower checks rarely wait for a load.
#[derive(Debug, Default)]
pub(super) struct HeldLoads {
    running: Option<JournalCompletion<ReleaseTurn>>,
    retry_at: Option<Duration>,
}

impl HeldLoads {
    /// When the next load may start, while the journal wants one.
    pub(super) fn at(&self, journal: &ReplicaJournal, now: Duration) -> Option<Duration> {
        if self.running.is_some() || !journal.held_load_wanted() {
            return None;
        }
        Some(self.retry_at.map_or(now, |retry| retry.max(now)))
    }

    pub(super) fn due(&self, journal: &ReplicaJournal, now: Duration) -> bool {
        self.at(journal, now).is_some_and(|at| at <= now)
    }

    pub(super) fn start(&mut self, load: JournalCompletion<ReleaseTurn>) {
        self.retry_at = None;
        assert!(self.running.replace(load).is_none());
    }

    pub(super) const fn running(&self) -> bool {
        self.running.is_some()
    }

    /// The running load, to await.
    pub(super) fn step(&mut self) -> Option<&mut JournalCompletion<ReleaseTurn>> {
        self.running.as_mut()
    }

    /// Finish a load. One that read nothing waits before the next.
    pub(super) fn complete(
        &mut self,
        turn: Result<ReleaseTurn, JournalError>,
        now: Duration,
    ) -> Result<(), JournalError> {
        self.running = None;
        if !turn?.loaded {
            self.retry_at = Some(now.saturating_add(RETRY));
        }
        Ok(())
    }
}
