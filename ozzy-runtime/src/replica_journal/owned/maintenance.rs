//! Explicit maintenance turns, with no timer or execution thread of their own.

use super::{JournalError, OwnedJournal, Scope};
use crate::replica_journal::{ValidatedStorage, commands::read_fault};
use ozzy_journal_segment::{
    AsyncStorageValidation, HistoryError, StorageValidationBudget, StorageValidationStep,
};
use ozzy_replication::driver::ValidationTicket;
use std::{cell::Cell, rc::Rc};

#[derive(Debug, Default)]
pub(super) struct Validation {
    cursor: Option<(Scope, AsyncStorageValidation)>,
    busy: Rc<Cell<bool>>,
}
impl Validation {
    pub(super) fn clear(&mut self) {
        self.cursor = None;
    }
}

#[derive(Debug)]
struct Slot(Rc<Cell<bool>>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

/// One exact captured segment validation step, independent of normal writes.
#[derive(Debug)]
pub struct PreparedStorageValidation {
    key: Rc<()>,
    ticket: ValidationTicket,
    cursor: AsyncStorageValidation,
    budget: StorageValidationBudget,
    slot: Slot,
}

/// Checked bytes awaiting observation by their exact owner incarnation.
#[derive(Debug)]
pub struct CompletedStorageValidation {
    key: Rc<()>,
    ticket: ValidationTicket,
    cursor: AsyncStorageValidation,
    result: Result<Option<StorageValidationStep>, JournalError>,
    _slot: Slot,
}

impl PreparedStorageValidation {
    /// Reread authoritative bytes under the explicit CPU/read budget. Dropping
    /// this future abandons its cursor, never restarts a partly checked segment.
    pub async fn validate(self) -> CompletedStorageValidation {
        let Self {
            key,
            ticket,
            mut cursor,
            budget,
            slot,
        } = self;
        let result = cursor
            .validate_next_with_budget(budget)
            .await
            .map_err(JournalError::from);
        CompletedStorageValidation {
            key,
            ticket,
            cursor,
            result,
            _slot: slot,
        }
    }
}

/// One cleanup class per turn. Listing bounds remain part of `OwnedConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageCleanup {
    /// Derived indexes whose source segment is no longer selected or protected.
    Indexes,
    /// Unselected segment generations and their disposable indexes.
    Segments,
    /// Unselected checkpoint directories, excluding protected build results.
    Checkpoints,
    /// Old manifests and recognized temporary publication files.
    Metadata,
}

/// Exact completed cleanup diagnostics, never replication or deletion authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanedStorage {
    /// Driver image that authorized the maintenance turn.
    pub ticket: ValidationTicket,
    /// Cleanup class executed.
    pub kind: StorageCleanup,
    /// Source objects removed. A segment or checkpoint can include several files.
    pub removed_objects: usize,
    /// All physical bytes removed, including derived child files.
    pub reclaimed_bytes: u64,
}

impl OwnedJournal {
    /// Validate authority and capture one bounded scrub step. Only capture
    /// borrows the owner; subsequent reads execute beside appends and rolls.
    /// A canceled read drops its cache and releases the single validation slot.
    pub async fn prepare_storage_validation(
        &mut self,
        ticket: ValidationTicket,
        budget: StorageValidationBudget,
    ) -> Result<PreparedStorageValidation, JournalError> {
        self.healthy()?;
        self.active(ticket)?;
        if !budget.is_valid() {
            return Err(HistoryError::Capacity.into());
        }
        if self.storage_validation.busy.get() {
            return Err(JournalError::AppendCapacity);
        }
        // Capture is ordered against authority mutation; wait for physical
        // reservations/roll publication to settle, without faulting this owner.
        self.journal.ready()?;
        let result = self.capture_validation(ticket).await;
        self.faulted |= result.as_ref().err().is_some_and(read_fault);
        let cursor = result?;
        self.storage_validation.busy.set(true);
        Ok(PreparedStorageValidation {
            key: self.read_key.clone(),
            ticket,
            cursor,
            budget,
            slot: Slot(self.storage_validation.busy.clone()),
        })
    }

    async fn capture_validation(
        &mut self,
        ticket: ValidationTicket,
    ) -> Result<AsyncStorageValidation, JournalError> {
        if let Some((scope, cursor)) = self.storage_validation.cursor.take()
            && scope == ticket.scope()
            && cursor.generation() == ticket.generation()
        {
            self.journal.ready_mut()?.validate_authority_files().await?;
            return Ok(cursor);
        }
        let journal = self.journal.ready_mut()?;
        let bytes = self.limits.io.max_segment_bytes as usize;
        Ok(if self.configuration.memory_voting() {
            journal.begin_written_storage_validation(bytes).await?
        } else {
            journal.begin_storage_validation(bytes).await?
        })
    }

    /// Install only this owner/scope/generation's diagnostics. Normal growth or
    /// roll cannot enlarge the captured scan. Observed corruption fences voting.
    pub fn complete_storage_validation(
        &mut self,
        done: CompletedStorageValidation,
    ) -> Result<ValidatedStorage, JournalError> {
        self.healthy()?;
        if !Rc::ptr_eq(&self.read_key, &done.key) {
            return Err(JournalError::CompletionMismatch);
        }
        self.active(done.ticket)?;
        self.faulted |= done.result.as_ref().err().is_some_and(read_fault);
        let Some(step) = done.result? else {
            self.faulted = true;
            return Err(HistoryError::Source.into());
        };
        if step.remaining_segments != 0 {
            self.storage_validation.cursor = Some((done.ticket.scope(), done.cursor));
        }
        Ok(ValidatedStorage {
            ticket: done.ticket,
            step,
        })
    }

    /// Reclaim one explicitly bounded class while holding exclusive journal
    /// mutation ownership. Other actors keep running; captured readers protect
    /// exact generations. Failed/canceled cleanup requires this owner's reopen.
    pub async fn cleanup_storage(
        &mut self,
        ticket: ValidationTicket,
        kind: StorageCleanup,
        max_objects: usize,
    ) -> Result<CleanedStorage, JournalError> {
        self.healthy()?;
        self.active(ticket)?;
        if max_objects == 0 {
            return Err(HistoryError::Capacity.into());
        }
        self.journal.ready()?;
        self.faulted = true;
        let journal = self.journal.ready_mut()?;
        let (removed_objects, reclaimed_bytes) = match kind {
            StorageCleanup::Indexes => {
                let result = journal.reclaim_unreferenced_indexes(max_objects).await?;
                (result.removed_files, result.reclaimed_bytes)
            }
            StorageCleanup::Segments => {
                let result = journal.reclaim_unreferenced_segments(max_objects).await?;
                (result.removed_segment_ids.len(), result.reclaimed_bytes)
            }
            StorageCleanup::Checkpoints => {
                let result = journal
                    .reclaim_unreferenced_checkpoints(max_objects)
                    .await?;
                (result.removed_checkpoint_ids.len(), result.reclaimed_bytes)
            }
            StorageCleanup::Metadata => {
                let result = journal.reclaim_unreferenced_metadata(max_objects).await?;
                (
                    result.removed_manifest_generations.len()
                        + result.removed_temporary_files.len(),
                    result.reclaimed_bytes,
                )
            }
        };
        self.faulted = false;
        Ok(CleanedStorage {
            ticket,
            kind,
            removed_objects,
            reclaimed_bytes,
        })
    }
}
