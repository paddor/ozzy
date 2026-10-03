//! Bounded directory work, preserving exact selected and live checkpoint sources.

use super::{
    Arc, CheckpointLimits, DirectoryError, GroupIdentity, JournalGeneration, OpenGroupJournal,
    PathBuf, RetentionError, StoreLock, checkpoint_name, fs, parse_current_temporary_name,
    parse_manifest_name, parse_manifest_temporary_name, remove_regular_file, sync_directory,
};
use std::time::{Duration, Instant};

/// Limit directory entries visited and yield between filesystem operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceBudget {
    pub max_entries: usize,
    pub max_work: Duration,
}

impl Default for MaintenanceBudget {
    fn default() -> Self {
        Self {
            max_entries: 32,
            max_work: Duration::from_millis(2),
        }
    }
}

/// One directory iterator, bound to an exact writer and exclusive group owner.
#[derive(Debug)]
pub struct MetadataCleanup {
    root: PathBuf,
    identity: GroupIdentity,
    generation: JournalGeneration,
    entries: Option<fs::ReadDir>,
    _lock: Arc<StoreLock>,
}

/// Scalar progress; no allocation proportional to historical file count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataCleanupStep {
    pub scanned_entries: usize,
    pub removed_files: usize,
    pub reclaimed_bytes: u64,
    pub complete: bool,
}

impl OpenGroupJournal {
    /// Capture a bounded metadata scan. No old payload is selected or removed.
    pub fn begin_metadata_cleanup(&self) -> Result<MetadataCleanup, DirectoryError> {
        self.require_roll_published()?;
        Ok(MetadataCleanup {
            root: self.directory.root.clone(),
            identity: self.directory.identity,
            generation: self.writer.written_position().generation(),
            entries: Some(fs::read_dir(&self.directory.root)?),
            _lock: Arc::clone(&self.directory.lock),
        })
    }

    /// Recheck live authority and pins, visit bounded entries, then sync deletions.
    ///
    /// Concurrent checkpoint builds retain their exact source manifest until the
    /// returned image is dropped. Later manifests do not require all their ancestors.
    /// A filesystem call can overrun the cooperative time target.
    pub fn cleanup_metadata_step(
        &self,
        cursor: &mut MetadataCleanup,
        budget: MaintenanceBudget,
        limits: CheckpointLimits,
    ) -> Result<MetadataCleanupStep, DirectoryError> {
        self.require_roll_published()?;
        if budget.max_entries == 0
            || budget.max_work.is_zero()
            || cursor.identity != self.directory.identity
            || cursor.root != self.directory.root
            || cursor.generation != self.writer.written_position().generation()
        {
            return Err(DirectoryError::CurrentMismatch);
        }
        self.validate_authority_files()?;
        let checkpoint_source = self.checkpoint_source_generation(limits)?;
        let started = Instant::now();
        let mut result = MetadataCleanupStep::default();
        while result.scanned_entries < budget.max_entries
            && (result.scanned_entries == 0 || started.elapsed() < budget.max_work)
        {
            let Some(entry) = cursor.entries.as_mut().and_then(Iterator::next) else {
                cursor.entries = None;
                result.complete = true;
                break;
            };
            let entry = entry?;
            result.scanned_entries += 1;
            let name = entry.file_name();
            let remove = if let Some(generation) = parse_manifest_name(&name) {
                generation != self.directory.current.generation
                    && Some(generation) != checkpoint_source
                    && !self.pins.metadata_is_pinned(generation)?
            } else {
                parse_manifest_temporary_name(&name).is_some()
                    || parse_current_temporary_name(&name).is_some()
                    || name == ".DURABLE.tmp"
            };
            if remove {
                result.reclaimed_bytes = result
                    .reclaimed_bytes
                    .checked_add(remove_regular_file(&entry.path(), "unreferenced metadata")?)
                    .ok_or(RetentionError::LengthOverflow)?;
                result.removed_files += 1;
            }
        }
        if result.removed_files > 0 {
            sync_directory(&self.directory.root)?;
        }
        Ok(result)
    }

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
