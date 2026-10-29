//! The owner's view of its held index: restore, admission, retention, and
//! the sealed indexes loads read.

use super::super::{JournalError, OwnedJournal};
use super::{HELD_ACTIVE, HELD_WINDOW, Held, SealedRows};

impl OwnedJournal {
    fn held_window(&self) -> usize {
        self.delay.map_or(HELD_WINDOW, |limits| limits.window)
    }

    /// Whether to roll before writing operations from `next` on, to bound
    /// the held rows in memory.
    pub(in crate::replica_journal::owned) fn held_crowded(
        &mut self,
        next: u64,
    ) -> Result<bool, JournalError> {
        self.observe_held()?;
        let active = self.delay.map_or(HELD_ACTIVE, |limits| limits.active);
        Ok(matches!(&self.held, Some(held) if held.crowded(next, active)))
    }

    /// Rebuild the held index from indexes and the active segment before
    /// admission. Accepted operations are all written here.
    pub(in crate::replica_journal) async fn restore_held(&mut self) -> Result<(), JournalError> {
        let window = self.held_window();
        // Activation holds the owner faulted until it completes.
        let Some((partition, floors)) = self
            .images
            .as_ref()
            .ok_or(JournalError::InstallationMismatch)?
            .speculative()
            .partitions()
            .find_map(|(partition, state)| {
                let delay = state.delay.as_ref()?;
                Some((partition, (delay.released(), delay.carried())))
            })
        else {
            self.held = None;
            return Ok(());
        };
        let snapshot = self
            .journal
            .ready_mut()?
            .build_index_snapshot(
                ozzy_journal_segment::JournalIndexBoundary::Accepted,
                self.recovery.index,
            )
            .await?;
        let held = Held::restore(partition, window, floors, &snapshot).await?;
        self.held = Some(held);
        self.check_held()
    }

    /// Install an admitted group's schedules and releases. A delay
    /// partition it creates starts with no held records.
    pub(in crate::replica_journal::owned) fn admit_held(
        &mut self,
        append: &crate::replica_journal::append::RetainedAppend,
    ) -> Result<(), JournalError> {
        if self.held.is_none() {
            let window = self.held_window();
            // Admission holds the owner faulted until it completes.
            let Some(partition) = self
                .images
                .as_ref()
                .ok_or(JournalError::AppendMismatch)?
                .speculative()
                .partitions()
                .find_map(|(partition, state)| state.delay.is_some().then_some(partition))
            else {
                return Ok(());
            };
            let references = &self.journal.readable()?.manifest().segments;
            self.held = Some(Held::new(partition, window, references));
        }
        let references = &self.journal.readable()?.manifest().segments;
        let held = self.held.as_mut().ok_or(JournalError::AppendMismatch)?;
        held.admit(append.operations(), self.limits.operations)?;
        held.observe(references);
        self.check_held()
    }

    /// Sealed held records loaded now.
    #[cfg(test)]
    pub(in crate::replica_journal) fn held_rows(&self) -> usize {
        self.held.as_ref().map_or(0, Held::loaded_rows)
    }

    /// Held rows in memory, loaded or of the active segment on.
    #[cfg(test)]
    pub(in crate::replica_journal) fn held_memory(&self) -> usize {
        let Some(held) = &self.held else { return 0 };
        held.recent.len() + held.sealed.len()
    }

    /// First operation of the oldest segment holding a record the last
    /// committed release and carry left, for retention.
    pub(in crate::replica_journal::owned) fn held_operation(
        &self,
    ) -> Result<Option<u64>, JournalError> {
        let images = self.images()?;
        let Some((_, delay)) = images
            .committed()
            .partitions()
            .find_map(|(partition, state)| Some((partition, state.delay.as_ref()?)))
        else {
            return Ok(None);
        };
        Ok(self
            .held
            .as_ref()
            .ok_or(JournalError::AppendMismatch)?
            .held_operation(delay.released(), delay.carried()))
    }

    /// Sealed indexes to read held rows from: those this owner validated,
    /// or a snapshot that validates them.
    pub(in crate::replica_journal::owned) async fn sealed_rows(
        &mut self,
    ) -> Result<SealedRows, JournalError> {
        let kept = self
            .held
            .as_ref()
            .and_then(|held| held.sealed_indexes.as_ref());
        let validated = self
            .journal
            .readable()?
            .capture_sealed_schedules(self.recovery.index.file, kept)?;
        if let Some(held) = self.held.as_mut() {
            held.sealed_indexes.clone_from(&validated);
        }
        Ok(match validated {
            Some(sealed) => SealedRows::Validated(sealed),
            None => SealedRows::Snapshot(Box::new(
                self.journal.retry_snapshot(self.recovery.index).await?,
            )),
        })
    }

    /// Follow segment rolls and retirement since the last admission.
    pub(in crate::replica_journal::owned) fn observe_held(&mut self) -> Result<(), JournalError> {
        if let Some(held) = self.held.as_mut() {
            held.observe(&self.journal.readable()?.manifest().segments);
        }
        Ok(())
    }

    /// Debug builds compare the held index with the canonical cursor and
    /// held totals.
    pub(in crate::replica_journal::owned) fn check_held(&self) -> Result<(), JournalError> {
        #[cfg(debug_assertions)]
        if let Some(held) = &self.held {
            let delay = self
                .images
                .as_ref()
                .ok_or(JournalError::AppendMismatch)?
                .speculative()
                .partition(held.partition())
                .and_then(|state| state.delay.as_ref())
                .ok_or(JournalError::AppendMismatch)?;
            assert!(held.matches(delay), "held index diverged: {held:?}");
        }
        Ok(())
    }
}
