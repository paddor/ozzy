//! The spare holds the next segment's space. A roll takes it and allocates
//! nothing, so a device without room for another segment refuses new records
//! instead of faulting. A preparation job allocates the spare beside writes.

use super::{JournalOwner, OwnedJournal};
use ozzy_journal_segment::{DirectoryError, SparePreparation};

/// Wait after a device had no room before allocating again, unless cleanup
/// changes the spare first.
const RETRY_MILLIS: u64 = 1000;

#[derive(Debug, Default)]
pub(super) struct Spare {
    state: State,
    /// Bumped whenever the spare file may have changed, so a preparation
    /// started before then cannot claim it.
    epoch: u64,
    running: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Not known to be allocated.
    #[default]
    Unknown,
    /// Allocated at the active segment's capacity.
    Ready,
    /// The device had no room at this time in milliseconds.
    Full(u64),
}

/// Captured spare allocation for a job beside writes.
#[derive(Debug)]
pub(in crate::replica_journal) struct SpareWork {
    epoch: u64,
    started: u64,
    preparation: SparePreparation,
}

/// Finished spare allocation awaiting its owner.
#[derive(Debug)]
pub(in crate::replica_journal) struct SpareDone {
    epoch: u64,
    started: u64,
    result: Result<(), DirectoryError>,
}

impl SpareWork {
    pub(in crate::replica_journal) async fn run(self) -> SpareDone {
        SpareDone {
            epoch: self.epoch,
            started: self.started,
            result: self.preparation.run().await,
        }
    }
}

impl Spare {
    pub(super) fn ready(&self) -> bool {
        self.state == State::Ready
    }

    /// A roll, an installation, or cleanup may have taken or replaced the
    /// spare; allocate it again before the next roll.
    pub(super) fn changed(&mut self) {
        self.state = State::Unknown;
        self.epoch += 1;
    }
}

impl OwnedJournal {
    /// Capture a spare allocation when none is known and none runs. After a
    /// full device it waits `RETRY_MILLIS` of `now`, which only a due check
    /// reads. Rolls, installations, and cleanup move the spare, so none of
    /// them may run beside it.
    pub(in crate::replica_journal) fn take_spare_work(
        &mut self,
        now: impl FnOnce() -> u64,
    ) -> Option<SpareWork> {
        if self.spare.state == State::Ready
            || self.spare.running
            || self.is_faulted()
            || self.cleaning()
        {
            return None;
        }
        let now = now();
        if let State::Full(since) = self.spare.state
            && now.saturating_sub(since) < RETRY_MILLIS
        {
            return None;
        }
        let journal = match &self.journal {
            JournalOwner::Ready(journal) => journal.as_ref(),
            JournalOwner::Writing(pipeline) => pipeline.journal(),
            _ => return None,
        };
        let preparation = match journal.prepare_spare() {
            Ok(preparation) => preparation,
            Err(error) => {
                crate::replica_journal::commands::fault_log(&error.into());
                self.faulted = true;
                return None;
            }
        };
        self.spare.running = true;
        Some(SpareWork {
            epoch: self.spare.epoch,
            started: now,
            preparation,
        })
    }

    /// Install a spare allocation. A full device refuses records until a
    /// retry finds room; any other failure fences this owner.
    pub(in crate::replica_journal) fn complete_spare(&mut self, done: SpareDone) {
        self.spare.running = false;
        if done.epoch != self.spare.epoch {
            return;
        }
        let full = matches!(self.spare.state, State::Full(_));
        match done.result {
            Ok(()) => {
                if full {
                    crate::profiling::log(
                        crate::profiling::Level::Warn,
                        "replica journal device has room for the next segment again",
                    );
                }
                self.spare.state = State::Ready;
            }
            Err(DirectoryError::Io(error)) if storage_full(&error) => {
                if !full {
                    crate::profiling::log(
                        crate::profiling::Level::Warn,
                        &format!(
                            "replica journal device has no room for the next segment: {error}; refusing new records"
                        ),
                    );
                }
                self.spare.state = State::Full(done.started);
            }
            Err(error) => {
                crate::replica_journal::commands::fault_log(&error.into());
                self.faulted = true;
            }
        }
    }
}

fn storage_full(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
    )
}
