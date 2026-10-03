//! Explicit bounded storage validation on the existing device executor.

use super::commands::Action;
use super::{JournalCompletion, ReplicaJournal, SubmitError};
use ozzy_replication::driver::ValidationTicket;

/// Correlated validation completion. Carries no confirmation or repair authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedStorage {
    /// Actor must recheck this ticket before using a completion diagnostically.
    pub ticket: ValidationTicket,
    /// Exact captured source and progress through one physical segment.
    pub step: ozzy_journal_segment::StorageValidationStep,
}

impl<E> ReplicaJournal<E> {
    /// Reclaim bounded unselected artifacts on the device executor. Live history remains protected.
    pub fn cleanup_orphans(
        &mut self,
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::MaintenanceBudget,
    ) -> Result<JournalCompletion<ozzy_journal_segment::OrphanCleanupStep>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| Action::CleanupOrphans {
                ticket,
                budget,
                done,
            },
            |action| match action {
                Action::CleanupOrphans { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Reclaim bounded historical metadata on the device executor. Payloads remain selected.
    pub fn cleanup_metadata(
        &mut self,
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::MaintenanceBudget,
    ) -> Result<JournalCompletion<ozzy_journal_segment::MetadataCleanupStep>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| Action::CleanupMetadata {
                ticket,
                budget,
                done,
            },
            |action| match action {
                Action::CleanupMetadata { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Schedule one bounded storage-validation step with normal command backpressure.
    ///
    /// Rereads authority metadata and a bounded segment prefix on the device executor. A full
    /// cycle retains one segment-metadata list and one segment-sized arena. Source
    /// damage fences the worker; unsettled writes or stale tickets reject only the
    /// request. Call periodically between normal work; no timer is installed here.
    pub fn validate_storage(
        &mut self,
        ticket: ValidationTicket,
    ) -> Result<JournalCompletion<ValidatedStorage>, SubmitError> {
        self.validate_storage_with_budget(
            ticket,
            ozzy_journal_segment::StorageValidationBudget::default(),
        )
    }

    /// Bound each step's segment reads and yield between decoding units.
    pub fn validate_storage_with_budget(
        &mut self,
        ticket: ValidationTicket,
        budget: ozzy_journal_segment::StorageValidationBudget,
    ) -> Result<JournalCompletion<ValidatedStorage>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| Action::ValidateStorage {
                ticket,
                budget,
                done,
            },
            |action| match action {
                Action::ValidateStorage { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }
}
