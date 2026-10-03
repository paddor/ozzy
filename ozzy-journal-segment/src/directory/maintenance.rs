//! Bounded storage maintenance and selected checkpoint sources.

use super::{CheckpointLimits, DirectoryError, OpenGroupJournal, checkpoint_name};
/// Bound objects removed per maintenance turn. File futures yield independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceBudget {
    /// Maximum removable objects; directory scans obey the journal listing limits.
    pub max_entries: usize,
}

impl Default for MaintenanceBudget {
    fn default() -> Self {
        Self { max_entries: 32 }
    }
}

impl OpenGroupJournal {
    pub(super) fn checkpoint_source_generation(
        &self,
        limits: CheckpointLimits,
    ) -> Result<Option<u64>, DirectoryError> {
        let Some(reference) = self.directory.manifest.checkpoint else {
            return Ok(None);
        };
        let (manifest, digest) = crate::checkpoint::read_checkpoint_manifest(
            &self
                .directory
                .root
                .join("checkpoints")
                .join(checkpoint_name(reference.checkpoint_id)),
            self.directory.identity.group_id,
            self.directory.identity.store_id,
            limits,
        )?;
        if digest != reference.manifest_digest
            || manifest.position != reference.position
            || manifest.configuration_epoch != self.directory.manifest.configuration_epoch
            || manifest.checkpoint_id != reference.checkpoint_id
        {
            return Err(DirectoryError::CheckpointMismatch);
        }
        Ok(Some(manifest.source_manifest_generation))
    }
}
