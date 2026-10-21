//! Detached builds of sealed-segment indexes beside later writes, so view
//! changes, identity lookups, and refreshes find them ready instead of
//! building them while they hold the journal.

use super::OwnedJournal;
use ozzy_journal_segment::{BuiltJournalIndex, JournalIndexJob};

#[derive(Debug, Default)]
pub(super) struct Indexing {
    running: bool,
    /// The newest selected segment when a job failed. None starts again
    /// until another segment is selected.
    failed_at: Option<u64>,
}

impl OwnedJournal {
    /// The next sealed index to build, at most one at a time.
    pub(in crate::replica_journal) fn take_index_work(&mut self) -> Option<JournalIndexJob> {
        if self.indexing.running || self.faulted {
            return None;
        }
        let journal = self.journal.readable().ok()?;
        let newest = journal.manifest().segments.last()?.segment_id;
        if self.indexing.failed_at == Some(newest) {
            return None;
        }
        let job = journal.prepare_index_job(self.recovery.index).ok()??;
        self.indexing.running = true;
        Some(job)
    }

    /// Record a finished build. A failure publishes nothing and is no fault:
    /// the next snapshot build meets it again and fails there.
    pub(in crate::replica_journal) fn complete_index_work(&mut self, built: &BuiltJournalIndex) {
        self.indexing.running = false;
        let Ok(journal) = self.journal.readable() else {
            return;
        };
        if !built.valid() {
            self.indexing.failed_at = journal
                .manifest()
                .segments
                .last()
                .map(|reference| reference.segment_id);
        }
        journal.finish_index_job(built);
    }
}
