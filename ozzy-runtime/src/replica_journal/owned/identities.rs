use super::{JournalError, OwnedJournal, prefix};
use ozzy_journal_segment::JournalIndexBoundary;
use ozzy_replication::driver::ValidationTicket;

impl OwnedJournal {
    /// New control-operation identities held in memory and their fixed capacity.
    /// APPEND retry sequences remain in writer state, outside this index.
    pub fn identity_capacity(&self) -> Result<(usize, usize), JournalError> {
        let overlay = self.images()?.speculative_identities().overlay();
        Ok((overlay.len(), overlay.capacity()))
    }

    /// Move new control identities into a validated persistent lookup view once
    /// all accepted operations are confirmed, applied, and physically installed.
    /// No full application replay, extra durability claim, or authority change.
    /// When capacity runs low, stop admitting control operations until this turn
    /// can run. Other actors keep running while bounded index jobs execute.
    pub async fn refresh_identities(
        &mut self,
        ticket: ValidationTicket,
    ) -> Result<ValidationTicket, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        self.writeback.require_idle()?;
        let journal = self.journal.ready()?;
        if !self.pending.is_empty()
            || prefix(journal.written_position()?) != self.applied
            || ticket.accepted() != self.applied
            || ticket.committed() != self.applied
        {
            return Err(JournalError::CompletionMismatch);
        }
        if self.identity_capacity()?.0 != 0 {
            self.refresh_settled_identities().await?;
        }
        Ok(ticket)
    }

    pub(in crate::replica_journal) fn identity_refresh_ready(&self) -> bool {
        self.images.is_some()
            && self.identity_capacity().is_ok_and(|(used, _)| used != 0)
            && self.pending.is_empty()
            && self.writeback.require_idle().is_ok()
            && self.journal.ready().is_ok_and(|journal| {
                journal.manifest().promised_view == journal.manifest().last_normal_view
                    && journal
                        .written_position()
                        .is_ok_and(|written| prefix(written) == self.applied)
            })
    }

    /// Called only with exclusive owner access and a fully settled image.
    pub(in crate::replica_journal) async fn refresh_settled_identities(
        &mut self,
    ) -> Result<(), JournalError> {
        self.healthy()?;
        if !self.identity_refresh_ready() {
            return Err(JournalError::CompletionMismatch);
        }
        let identities = self.images()?.committed_identities().clone();
        self.faulted = true;
        let snapshot = self
            .journal
            .ready_mut()?
            .build_index_snapshot(JournalIndexBoundary::Written, self.recovery.index)
            .await?;
        if prefix(snapshot.through()) != self.applied {
            return Err(JournalError::CompletionMismatch);
        }
        let replacement = identities.handoff(snapshot).await?;
        self.images
            .as_mut()
            .expect("active image checked")
            .replace_settled_identity_index(self.applied.op.0, replacement)?;
        self.faulted = false;
        Ok(())
    }
}
