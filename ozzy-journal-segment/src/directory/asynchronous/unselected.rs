//! Removal of storage objects the selected manifest no longer references.
//!
//! An exclusive pass runs while the owner holds the journal. A concurrent
//! pass runs as a job beside later writes over a captured selection: it only
//! removes objects older than that capture, so nothing the owner creates or
//! publishes afterwards can match.

use super::{DirectoryError, Journal, Limits};
use crate::async_files::Access;
use crate::directory::{
    CURRENT_FILE, index_name_segment_id, manifest_name, parse_checkpoint_name,
    parse_current_temporary_name, parse_manifest_name, parse_manifest_temporary_name,
    parse_segment_name, validate_checkpoint_source,
};
use crate::retention::PinRegistry;
use crate::{
    CurrentReference, Manifest, OrphanCleanupStep, RetentionError, UnreferencedCheckpointCleanup,
    UnreferencedMetadataCleanup, UnreferencedSegmentCleanup, decode_manifest, manifest_digest,
};
use ozzy_io::{FileKind, OpenMode, Operation};
use std::{
    cell::Cell,
    collections::HashSet,
    path::{Path, PathBuf},
    rc::Rc,
};

/// What a pass may remove besides the selection's own objects.
#[derive(Clone, Copy, Debug)]
enum Bounds {
    /// The owner holds the journal: every unselected, unpinned object.
    Exclusive,
    /// The owner keeps writing: only objects older than the captured
    /// selection, whose newest segment is `active`.
    Concurrent { active: u64 },
}

/// One cleanup pass over a selection. `interrupted` is set while a removal
/// is in progress and cleared once the pass completes.
struct Reclaim<'a> {
    access: &'a Access,
    root: &'a Path,
    limits: Limits,
    pins: &'a PinRegistry,
    manifest: &'a Manifest,
    current: CurrentReference,
    bounds: Bounds,
    interrupted: &'a mut bool,
}

impl Reclaim<'_> {
    /// Whether segment `id` existed before a concurrent capture.
    fn older(&self, id: u64) -> bool {
        match self.bounds {
            Bounds::Exclusive => true,
            Bounds::Concurrent { active } => id < active,
        }
    }

    /// Whether manifest generation `generation` may go, by age alone.
    fn older_generation(&self, generation: u64) -> bool {
        match self.bounds {
            Bounds::Exclusive => generation != self.current.generation,
            Bounds::Concurrent { .. } => generation < self.current.generation,
        }
    }

    /// Whether a temporary for `generation` may go: an exclusive pass owns
    /// every publication; a concurrent one only those before its capture.
    fn older_temporary(&self, generation: u64) -> bool {
        match self.bounds {
            Bounds::Exclusive => true,
            Bounds::Concurrent { .. } => generation <= self.current.generation,
        }
    }

    async fn list_sorted(&self, directory: PathBuf) -> Result<Vec<ozzy_io::Entry>, DirectoryError> {
        let mut entries = self
            .access
            .list(
                directory,
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            )
            .await?;
        entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    /// Remove one regular file, adding its length to `reclaimed`.
    async fn remove(&mut self, path: PathBuf, reclaimed: &mut u64) -> Result<(), DirectoryError> {
        *self.interrupted = true;
        let bytes = remove_regular_file(self.access, path).await?;
        *reclaimed = reclaimed
            .checked_add(bytes)
            .ok_or(RetentionError::LengthOverflow)?;
        Ok(())
    }

    /// Make the pass's removals in `directory` durable, if there were any.
    async fn finish(&mut self, directory: PathBuf) -> Result<(), DirectoryError> {
        if *self.interrupted {
            self.access.sync_directory(directory).await?;
            *self.interrupted = false;
        }
        Ok(())
    }

    /// Remove bounded derived indexes whose segment ID is neither selected nor
    /// protected by readers/preparation. Handles indexes left without payload
    /// files after interrupted cleanup. Unknown filenames stay untouched.
    async fn indexes(&mut self, max_files: usize) -> Result<OrphanCleanupStep, DirectoryError> {
        let directory = self.root.join("indexes");
        let entries = self.list_sorted(directory.clone()).await?;
        let mut result = OrphanCleanupStep::default();
        let count = entries.len();
        for entry in entries {
            if result.removed_files == max_files {
                break;
            }
            result.work_units += 1;
            let Some(id) = index_name_segment_id(&entry.name) else {
                continue;
            };
            if self
                .manifest
                .segments
                .iter()
                .any(|reference| reference.segment_id == id)
                || self.pins.is_pinned(id)
                || !self.older(id)
            {
                result.deferred_sources += 1;
                continue;
            }
            self.remove(directory.join(entry.name), &mut result.reclaimed_bytes)
                .await?;
            result.removed_files += 1;
        }
        self.finish(directory).await?;
        result.complete = result.work_units == count;
        Ok(result)
    }

    /// Delete at most `max_segments` unselected physical segment generations.
    /// Directory visits and each segment's derived indexes are bounded by the
    /// journal's listing limits. Selected generations and captured readers win.
    async fn segments(
        &mut self,
        max_segments: usize,
    ) -> Result<UnreferencedSegmentCleanup, DirectoryError> {
        let entries = self.list_sorted(self.root.join("segments")).await?;
        let index_entries = self.list_sorted(self.root.join("indexes")).await?;
        let selected: HashSet<_> = self
            .manifest
            .segments
            .iter()
            .map(|reference| reference.file_name())
            .collect();
        let mut result = UnreferencedSegmentCleanup {
            removed_segment_ids: Vec::new(),
            pinned_segment_ids: Vec::new(),
            reclaimed_bytes: 0,
        };
        for entry in entries {
            if result.removed_segment_ids.len() == max_segments {
                break;
            }
            let Some(id) = parse_segment_name(&entry.name) else {
                continue;
            };
            if entry
                .name
                .to_str()
                .is_some_and(|name| selected.contains(name))
                || !self.older(id)
            {
                continue;
            }
            if self.pins.is_pinned(id) {
                result.pinned_segment_ids.push(id);
                continue;
            }
            *self.interrupted = true;
            self.remove_segment(id, &entry, &index_entries, &mut result.reclaimed_bytes)
                .await?;
            result.removed_segment_ids.push(id);
            *self.interrupted = false;
        }
        result.removed_segment_ids.sort_unstable();
        result.removed_segment_ids.dedup();
        result.pinned_segment_ids.sort_unstable();
        result.pinned_segment_ids.dedup();
        Ok(result)
    }

    /// Remove one unselected segment generation. Its derived indexes go
    /// first unless a selected generation shares its ID, and each directory
    /// is synced before the next removal step.
    async fn remove_segment(
        &mut self,
        id: u64,
        segment: &ozzy_io::Entry,
        index_entries: &[ozzy_io::Entry],
        reclaimed_bytes: &mut u64,
    ) -> Result<(), DirectoryError> {
        let directory = self.root.join("segments");
        let indexes = self.root.join("indexes");
        let live_id = self
            .manifest
            .segments
            .iter()
            .any(|reference| reference.segment_id == id);
        if !live_id {
            for index in index_entries {
                if index_name_segment_id(&index.name) == Some(id) {
                    self.remove(indexes.join(&index.name), reclaimed_bytes)
                        .await?;
                }
            }
            // Required even if a prior interrupted pass already unlinked
            // every index: make that deletion durable before payload removal.
            self.access.sync_directory(indexes).await?;
        }
        self.remove(directory.join(&segment.name), reclaimed_bytes)
            .await?;
        self.access.sync_directory(directory).await?;
        Ok(())
    }

    /// Remove bounded unselected checkpoint artifacts. Live build results and
    /// captured readers protect their exact IDs and source manifest generations.
    async fn checkpoints(
        &mut self,
        max_checkpoints: usize,
    ) -> Result<UnreferencedCheckpointCleanup, DirectoryError> {
        let directory = self.root.join("checkpoints");
        let entries = self.list_sorted(directory.clone()).await?;
        let selected = self
            .manifest
            .checkpoint
            .map(|reference| reference.checkpoint_id);
        let mut result = UnreferencedCheckpointCleanup {
            removed_checkpoint_ids: Vec::new(),
            pinned_checkpoint_ids: Vec::new(),
            reclaimed_bytes: 0,
        };
        for entry in entries {
            if result.removed_checkpoint_ids.len() == max_checkpoints {
                break;
            }
            let Some(id) = parse_checkpoint_name(&entry.name) else {
                continue;
            };
            if Some(id) == selected {
                continue;
            }
            if self.pins.checkpoint_is_pinned(id) {
                result.pinned_checkpoint_ids.push(id);
                continue;
            }
            if entry.kind != FileKind::Directory {
                return Err(DirectoryError::NotDirectory("unselected checkpoint"));
            }
            let path = directory.join(entry.name);
            let children = self.list_sorted(path.clone()).await?;
            if children.iter().any(|child| child.kind != FileKind::File) {
                return Err(DirectoryError::NotRegularFile("unselected checkpoint file"));
            }
            for child in children {
                self.remove(path.join(child.name), &mut result.reclaimed_bytes)
                    .await?;
            }
            *self.interrupted = true;
            self.access.sync_directory(path.clone()).await?;
            self.access
                .done(Operation::RemoveDirectory { path })
                .await?;
            self.access.sync_directory(directory.clone()).await?;
            result.removed_checkpoint_ids.push(id);
            *self.interrupted = false;
        }
        Ok(result)
    }

    /// Delete bounded unselected manifest generations and recognized temporary
    /// metadata. The current selection, selected checkpoint source, and captured
    /// build/read sources remain protected. Publication files are never inferred
    /// from directory recency.
    async fn metadata(
        &mut self,
        max_files: usize,
        checkpoint_source: Option<u64>,
    ) -> Result<UnreferencedMetadataCleanup, DirectoryError> {
        let entries = self.list_sorted(self.root.to_path_buf()).await?;
        let mut result = UnreferencedMetadataCleanup {
            removed_manifest_generations: Vec::new(),
            removed_temporary_files: Vec::new(),
            reclaimed_bytes: 0,
        };
        for entry in entries {
            if result.removed_manifest_generations.len() + result.removed_temporary_files.len()
                == max_files
            {
                break;
            }
            let generation = parse_manifest_name(&entry.name);
            if let Some(generation) = generation {
                if !self.older_generation(generation)
                    || Some(generation) == checkpoint_source
                    || self.pins.metadata_is_pinned(generation)
                {
                    continue;
                }
            } else if !parse_manifest_temporary_name(&entry.name)
                .or_else(|| parse_current_temporary_name(&entry.name))
                .is_some_and(|temporary| self.older_temporary(temporary))
            {
                continue;
            }
            self.remove(self.root.join(&entry.name), &mut result.reclaimed_bytes)
                .await?;
            if let Some(generation) = generation {
                result.removed_manifest_generations.push(generation);
            } else {
                // Parsed temporary names are ASCII.
                result
                    .removed_temporary_files
                    .push(entry.name.to_string_lossy().into_owned());
            }
        }
        self.finish(self.root.to_path_buf()).await?;
        result.removed_manifest_generations.sort_unstable();
        Ok(result)
    }

    /// The manifest generation the selected checkpoint was built from, after
    /// checking both files.
    async fn checkpoint_source(&self) -> Result<Option<u64>, DirectoryError> {
        let checkpoint =
            super::opening::selected_checkpoint(self.access, self.root, self.manifest, self.limits)
                .await?;
        let Some(checkpoint) = checkpoint else {
            return Ok(None);
        };
        let generation = checkpoint.manifest.source_manifest_generation;
        let bytes = self
            .access
            .read_file(
                self.root.join(manifest_name(generation)),
                self.limits.metadata.max_manifest_bytes,
                self.limits.io.chunk_bytes,
            )
            .await?;
        let source = decode_manifest(&bytes, self.limits.metadata)?;
        validate_checkpoint_source(
            self.manifest.identity,
            &checkpoint.manifest,
            &source,
            manifest_digest(&bytes, self.limits.metadata)?,
        )?;
        Ok(Some(generation))
    }
}

async fn remove_regular_file(access: &Access, path: PathBuf) -> Result<u64, DirectoryError> {
    let opened = access
        .open(path.clone(), OpenMode::Read, false, false)
        .await;
    let file = match opened {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let length = access.length(&file).await?;
    access.done(Operation::Close { handle: file }).await?;
    access.done(Operation::RemoveFile { path }).await?;
    Ok(length)
}

impl Journal {
    fn reclaim(&mut self) -> Reclaim<'_> {
        Reclaim {
            access: &self.access,
            root: self.directory.root(),
            limits: self.limits,
            pins: &self.pins,
            manifest: &self.manifest,
            current: self.current,
            bounds: Bounds::Exclusive,
            interrupted: &mut self.interrupted,
        }
    }

    /// Checks shared by the exclusive passes. Authority validation fences
    /// nothing; a failed removal fences the journal until it reopens.
    async fn begin_exclusive(&mut self, budget: usize) -> Result<(), DirectoryError> {
        self.healthy()?;
        if budget == 0 {
            return Err(DirectoryError::RetentionScanBudget);
        }
        self.validate_authority_files().await
    }

    /// Remove bounded derived indexes whose segment ID is neither selected nor
    /// protected by readers/preparation. Handles indexes left without payload
    /// files after interrupted cleanup. Unknown filenames stay untouched.
    pub async fn reclaim_unreferenced_indexes(
        &mut self,
        max_files: usize,
    ) -> Result<OrphanCleanupStep, DirectoryError> {
        self.begin_exclusive(max_files).await?;
        self.reclaim().indexes(max_files).await
    }

    /// Delete at most `max_segments` unselected physical segment generations.
    /// Directory visits and each segment's derived indexes are bounded by the
    /// journal's listing limits. Selected generations and captured readers win.
    pub async fn reclaim_unreferenced_segments(
        &mut self,
        max_segments: usize,
    ) -> Result<UnreferencedSegmentCleanup, DirectoryError> {
        self.begin_exclusive(max_segments).await?;
        self.reclaim().segments(max_segments).await
    }

    /// Remove bounded unselected checkpoint artifacts. Live build results and
    /// captured readers protect their exact IDs and source manifest generations.
    pub async fn reclaim_unreferenced_checkpoints(
        &mut self,
        max_checkpoints: usize,
    ) -> Result<UnreferencedCheckpointCleanup, DirectoryError> {
        self.begin_exclusive(max_checkpoints).await?;
        let mut reclaim = self.reclaim();
        reclaim.checkpoint_source().await?;
        reclaim.checkpoints(max_checkpoints).await
    }

    /// Delete bounded unselected manifest generations and recognized temporary
    /// metadata. The current selection, selected checkpoint source, and captured
    /// build/read sources remain protected. Publication files are never inferred
    /// from directory recency.
    pub async fn reclaim_unreferenced_metadata(
        &mut self,
        max_files: usize,
    ) -> Result<UnreferencedMetadataCleanup, DirectoryError> {
        self.begin_exclusive(max_files).await?;
        let mut reclaim = self.reclaim();
        let source = reclaim.checkpoint_source().await?;
        reclaim.metadata(max_files, source).await
    }

    /// Capture the selection for one cleanup pass that runs beside later
    /// writes. One capture lives at a time, and no checkpoint is selected
    /// while it does. Take it in a settled turn: with no roll or sync
    /// publication in flight, every later publication is newer than it.
    pub fn capture_unselected(&self) -> Result<UnselectedFiles, DirectoryError> {
        self.healthy()?;
        if self.cleaning.replace(true) {
            return Err(DirectoryError::CleanupBusy);
        }
        let slot = CleanupSlot(self.cleaning.clone());
        let active = self
            .manifest
            .segments
            .last()
            .map_or(0, |reference| reference.segment_id);
        Ok(UnselectedFiles {
            access: self.access.clone(),
            root: self.root().to_path_buf(),
            limits: self.limits,
            pins: self.pins.clone(),
            manifest: self.manifest.clone(),
            current: self.current,
            active,
            source: None,
            _slot: slot,
        })
    }

    /// Refuse a new checkpoint selection while a cleanup capture lives.
    pub(super) fn require_no_cleanup(&self) -> Result<(), DirectoryError> {
        if self.cleaning.get() {
            return Err(DirectoryError::CleanupBusy);
        }
        Ok(())
    }
}

/// Releases the journal's cleanup slot when the capture drops.
#[derive(Debug)]
struct CleanupSlot(Rc<Cell<bool>>);

impl Drop for CleanupSlot {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

/// A captured selection whose unselected, older objects one pass removes
/// beside later writes. It holds no journal borrow; a failed pass must
/// fence its owner.
#[derive(Debug)]
pub struct UnselectedFiles {
    access: Access,
    root: PathBuf,
    limits: Limits,
    pins: Rc<PinRegistry>,
    manifest: Manifest,
    current: CurrentReference,
    active: u64,
    source: Option<u64>,
    _slot: CleanupSlot,
}

impl UnselectedFiles {
    fn reclaim<'a>(&'a self, interrupted: &'a mut bool) -> Reclaim<'a> {
        Reclaim {
            access: &self.access,
            root: &self.root,
            limits: self.limits,
            pins: &self.pins,
            manifest: &self.manifest,
            current: self.current,
            bounds: Bounds::Concurrent {
                active: self.active,
            },
            interrupted,
        }
    }

    /// Check once per pass that this directory still selects the captured
    /// generation or a newer one, and resolve the selected checkpoint's
    /// source manifest.
    pub async fn validate(&mut self) -> Result<(), DirectoryError> {
        let bytes = self
            .access
            .read_file(
                self.root.join(CURRENT_FILE),
                crate::CURRENT_BYTES,
                self.limits.io.chunk_bytes,
            )
            .await?;
        let current = crate::decode_current(&bytes)?;
        let identity = self.manifest.identity;
        if current.group_id != identity.group_id
            || current.store_id != identity.store_id
            || current.generation < self.current.generation
            || (current.generation == self.current.generation && current != self.current)
        {
            return Err(DirectoryError::CurrentMismatch);
        }
        let mut interrupted = false;
        self.source = self.reclaim(&mut interrupted).checkpoint_source().await?;
        Ok(())
    }

    /// Remove unselected indexes older than the capture.
    pub async fn reclaim_indexes(
        &self,
        max_files: usize,
    ) -> Result<OrphanCleanupStep, DirectoryError> {
        let mut interrupted = false;
        self.reclaim(&mut interrupted).indexes(max_files).await
    }

    /// Remove unselected segments older than the capture, with their indexes.
    pub async fn reclaim_segments(
        &self,
        max_segments: usize,
    ) -> Result<UnreferencedSegmentCleanup, DirectoryError> {
        let mut interrupted = false;
        self.reclaim(&mut interrupted).segments(max_segments).await
    }

    /// Remove unselected, unpinned checkpoints.
    pub async fn reclaim_checkpoints(
        &self,
        max_checkpoints: usize,
    ) -> Result<UnreferencedCheckpointCleanup, DirectoryError> {
        let mut interrupted = false;
        self.reclaim(&mut interrupted)
            .checkpoints(max_checkpoints)
            .await
    }

    /// Remove manifests older than the capture and earlier temporaries. Call
    /// [`Self::validate`] first; it resolves the protected checkpoint source.
    pub async fn reclaim_metadata(
        &self,
        max_files: usize,
    ) -> Result<UnreferencedMetadataCleanup, DirectoryError> {
        let mut interrupted = false;
        let source = self.source;
        self.reclaim(&mut interrupted)
            .metadata(max_files, source)
            .await
    }
}
