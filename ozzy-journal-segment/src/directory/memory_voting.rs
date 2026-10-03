//! Startup/shutdown evidence for votes that may precede local persistence.

use super::{
    DirectoryError, NoopObserver, OpenGroupJournal, PersistenceObserver, evidence, publication,
    read_exact_file,
};

pub(crate) const NAME: &str = "MEMORY_VOTING";
pub(crate) const TEMPORARY: &str = ".MEMORY_VOTING.tmp";
pub(crate) const BYTES: usize = 256;
const CONTEXT: &str = "ozzy memory voting restart evidence";
#[derive(Clone, Copy)]
pub(crate) enum VoteState {
    Drained = 1,
    Running = 2,
}

impl OpenGroupJournal {
    /// Require an exact drained shutdown or freshly installed recovery image.
    /// Missing, damaged, running, or stale evidence never authorizes intact restart.
    /// Full canonical recovery and a new fenced election are still required.
    pub fn require_drained_memory_history(&self) -> Result<(), DirectoryError> {
        let bytes = read_exact_file(&self.directory.root.join(NAME), BYTES, NAME)
            .map_err(|_| DirectoryError::MemoryHistoryUnproven)?;
        if bytes != self.memory_evidence(VoteState::Drained)? {
            return Err(DirectoryError::MemoryHistoryUnproven);
        }
        Ok(())
    }

    /// Persist the running marker before enabling any memory vote. The caller
    /// must first prove drained restart, fresh bootstrap, or fresh group recovery.
    /// A crash from this point requires nonvoting recovery even if disk looks intact.
    pub fn mark_memory_voting_running(&mut self) -> Result<(), DirectoryError> {
        self.publish_memory_evidence(VoteState::Running, &mut NoopObserver)
    }

    /// Persist exact drained history after voting/admission stop and every accepted
    /// operation has reached this writer. Never call with queued memory-only work.
    /// This does not make RAM confirmations survive loss of all volatile copies.
    pub fn publish_drained_memory_history(&mut self) -> Result<(), DirectoryError> {
        self.sync_through(self.begin_sync())?;
        // Freeze the selected manifest too. Otherwise ordinary recovery would
        // advance its accepted field and invalidate this exact generation.
        self.publish_manifest_progress(&mut NoopObserver)?;
        self.publish_memory_evidence(VoteState::Drained, &mut NoopObserver)
    }

    fn publish_memory_evidence(
        &self,
        state: VoteState,
        observer: &mut impl PersistenceObserver,
    ) -> Result<(), DirectoryError> {
        let bytes = self.memory_evidence(state)?;
        publication::replace_reusable(
            &mut publication::Filesystem(&self.directory.root),
            NAME,
            TEMPORARY,
            &bytes,
            observer,
        )
    }

    fn memory_evidence(&self, state: VoteState) -> Result<[u8; BYTES], DirectoryError> {
        self.require_roll_published()?;
        let configuration = self
            .directory
            .configuration()
            .ok_or(DirectoryError::ConfigurationMismatch)?;
        let accepted = self.accepted_position()?;
        encode(&self.directory.manifest, configuration, accepted, state)
    }
}

/// Shared exact evidence encoding for filesystem and byte-model publication.
pub(crate) fn encode(
    manifest: &crate::Manifest,
    configuration: &[u8],
    accepted: crate::LogPosition,
    state: VoteState,
) -> Result<[u8; BYTES], DirectoryError> {
    let mut bytes = [0; BYTES];
    bytes[..8].copy_from_slice(b"OZYMEM\0\0");
    bytes[8] = state as u8;
    // Store, broker, volume, manifest, active segment, and exact accepted prefix.
    bytes[16..176].copy_from_slice(&evidence::encode(manifest, accepted)?[..160]);
    bytes[176..208].copy_from_slice(
        ozzy_journal::integrity::hash("ozzy memory voting configuration", configuration).as_bytes(),
    );
    bytes[208..216].copy_from_slice(&manifest.promised_view.to_be_bytes());
    bytes[216..224].copy_from_slice(&manifest.last_normal_view.to_be_bytes());
    let checksum = ozzy_journal::integrity::hash(CONTEXT, &bytes[..224]);
    bytes[224..].copy_from_slice(checksum.as_bytes());
    Ok(bytes)
}

#[cfg(test)]
mod tests;
