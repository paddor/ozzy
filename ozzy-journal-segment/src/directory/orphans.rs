//! Incremental orphan deletion. Every candidate is rechecked against live owners.

use super::{
    Arc, CheckpointId, DirectoryError, GroupIdentity, MaintenanceBudget, OpenGroupJournal, PathBuf,
    RetentionError, StoreLock, fs, index_name_segment_id, parse_checkpoint_name,
    parse_segment_name, remove_regular_file, require_directory, sync_directory,
};
use std::time::Instant;

/// A directory walk with at most one nested artifact open. Dropping it is safe.
#[derive(Debug)]
pub struct OrphanCleanup {
    root: PathBuf,
    identity: GroupIdentity,
    lock: Arc<StoreLock>,
    phase: Phase,
    entries: fs::ReadDir,
    pending: Option<Pending>,
}

#[derive(Debug, Clone, Copy)]
enum Phase {
    Segments,
    Indexes,
    Checkpoints,
    Done,
}

#[derive(Debug)]
enum Pending {
    Segment {
        id: u64,
        path: PathBuf,
        indexes: fs::ReadDir,
    },
    Checkpoint {
        id: CheckpointId,
        path: PathBuf,
        entries: fs::ReadDir,
    },
}

/// Progress includes directory visits, file removals, and deferred live sources.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OrphanCleanupStep {
    /// One entry visit or directory completion per unit, capped by `max_entries`.
    pub work_units: usize,
    pub removed_files: usize,
    pub removed_directories: usize,
    pub deferred_sources: usize,
    pub reclaimed_bytes: u64,
    pub complete: bool,
}

impl OpenGroupJournal {
    /// Begin a scan of unselected segments, derived indexes, and checkpoints.
    /// Staging workspaces belong to startup cleanup; live builders may own them.
    pub fn begin_orphan_cleanup(&self) -> Result<OrphanCleanup, DirectoryError> {
        self.require_roll_published()?;
        Ok(OrphanCleanup {
            root: self.directory.root.clone(),
            identity: self.directory.identity,
            lock: Arc::clone(&self.directory.lock),
            phase: Phase::Segments,
            entries: fs::read_dir(self.directory.root.join("segments"))?,
            pending: None,
        })
    }

    /// Visit at most the entry budget, yielding between filesystem operations.
    /// Index deletions are synchronized before deleting their source payload.
    /// Selected files, live readers, checkpoint builds, and prepared rolls are protected.
    pub fn cleanup_orphan_step(
        &self,
        cursor: &mut OrphanCleanup,
        budget: MaintenanceBudget,
    ) -> Result<OrphanCleanupStep, DirectoryError> {
        self.require_roll_published()?;
        if budget.max_entries == 0
            || budget.max_work.is_zero()
            || cursor.root != self.directory.root
            || cursor.identity != self.directory.identity
            || !Arc::ptr_eq(&cursor.lock, &self.directory.lock)
        {
            return Err(DirectoryError::CurrentMismatch);
        }
        self.validate_authority_files()?;
        let started = Instant::now();
        let mut step = OrphanCleanupStep::default();
        while step.work_units < budget.max_entries
            && (step.work_units == 0 || started.elapsed() < budget.max_work)
            && !matches!(cursor.phase, Phase::Done)
        {
            step.work_units += 1;
            cursor.advance(self, &mut step)?;
        }
        step.complete = matches!(cursor.phase, Phase::Done);
        Ok(step)
    }

    fn segment_is_live(&self, id: u64) -> Result<bool, DirectoryError> {
        Ok(self
            .directory
            .manifest
            .segments
            .iter()
            .any(|r| r.segment_id == id)
            || self.pins.is_pinned(id)?)
    }

    fn checkpoint_is_live(&self, id: CheckpointId) -> Result<bool, DirectoryError> {
        Ok(self
            .directory
            .manifest
            .checkpoint
            .is_some_and(|r| r.checkpoint_id == id)
            || self.pins.checkpoint_is_pinned(id)?)
    }
}

impl OrphanCleanup {
    fn advance(
        &mut self,
        journal: &OpenGroupJournal,
        step: &mut OrphanCleanupStep,
    ) -> Result<(), DirectoryError> {
        if let Some(pending) = self.pending.take() {
            self.pending = self.advance_pending(journal, pending, step)?;
            return Ok(());
        }
        let Some(entry) = self.entries.next() else {
            let next = match self.phase {
                Phase::Segments => (Phase::Indexes, "indexes"),
                Phase::Indexes => (Phase::Checkpoints, "checkpoints"),
                Phase::Checkpoints | Phase::Done => {
                    self.phase = Phase::Done;
                    return Ok(());
                }
            };
            self.entries = fs::read_dir(self.root.join(next.1))?;
            self.phase = next.0;
            return Ok(());
        };
        let entry = entry?;
        match self.phase {
            Phase::Segments => {
                if let Some(id) = parse_segment_name(&entry.file_name()) {
                    let selected = journal
                        .directory
                        .manifest
                        .segments
                        .iter()
                        .find(|r| r.segment_id == id)
                        .is_some_and(|r| r.file_name().as_str() == entry.file_name());
                    if selected || journal.pins.is_pinned(id)? {
                        step.deferred_sources += 1;
                    } else if journal.segment_is_live(id)? {
                        // An obsolete physical incarnation. Current derived
                        // indexes belong to the selected incarnation of this ID.
                        remove_file(&entry.path(), &self.root.join("segments"), step)?;
                    } else {
                        self.pending = Some(Pending::Segment {
                            id,
                            path: entry.path(),
                            indexes: fs::read_dir(self.root.join("indexes"))?,
                        });
                    }
                }
            }
            Phase::Indexes => {
                if let Some(id) = index_name_segment_id(&entry.file_name()) {
                    if journal.segment_is_live(id)? {
                        step.deferred_sources += 1;
                    } else {
                        remove_file(&entry.path(), &self.root.join("indexes"), step)?;
                    }
                }
            }
            Phase::Checkpoints => {
                if let Some(id) = parse_checkpoint_name(&entry.file_name()) {
                    if journal.checkpoint_is_live(id)? {
                        step.deferred_sources += 1;
                    } else {
                        require_directory(&entry.path(), "unselected checkpoint")?;
                        self.pending = Some(Pending::Checkpoint {
                            id,
                            path: entry.path(),
                            entries: fs::read_dir(entry.path())?,
                        });
                    }
                }
            }
            Phase::Done => unreachable!("completed scans do not advance"),
        }
        Ok(())
    }

    fn advance_pending(
        &self,
        journal: &OpenGroupJournal,
        mut pending: Pending,
        step: &mut OrphanCleanupStep,
    ) -> Result<Option<Pending>, DirectoryError> {
        match &mut pending {
            Pending::Segment { id, path, indexes } => {
                if journal.segment_is_live(*id)? {
                    step.deferred_sources += 1;
                    return Ok(None);
                }
                if let Some(entry) = indexes.next() {
                    let entry = entry?;
                    if index_name_segment_id(&entry.file_name()) == Some(*id) {
                        remove_file(&entry.path(), &self.root.join("indexes"), step)?;
                    }
                } else {
                    // Also covers a prior interrupted call that removed an index.
                    sync_directory(&self.root.join("indexes"))?;
                    remove_file(path, &self.root.join("segments"), step)?;
                    return Ok(None);
                }
            }
            Pending::Checkpoint { id, path, entries } => {
                if journal.checkpoint_is_live(*id)? {
                    step.deferred_sources += 1;
                    return Ok(None);
                }
                if let Some(entry) = entries.next() {
                    // Checkpoint artifacts are flat. Unexpected nested directories
                    // or symlinks refuse cleanup rather than escaping its work bound.
                    remove_file(&entry?.path(), path, step)?;
                } else {
                    sync_directory(path)?;
                    fs::remove_dir(path)?;
                    sync_directory(&self.root.join("checkpoints"))?;
                    step.removed_directories += 1;
                    return Ok(None);
                }
            }
        }
        Ok(Some(pending))
    }
}

fn remove_file(
    path: &std::path::Path,
    parent: &std::path::Path,
    step: &mut OrphanCleanupStep,
) -> Result<(), DirectoryError> {
    step.reclaimed_bytes = step
        .reclaimed_bytes
        .checked_add(remove_regular_file(path, "unselected artifact")?)
        .ok_or(RetentionError::LengthOverflow)?;
    step.removed_files += 1;
    sync_directory(parent)?;
    Ok(())
}
