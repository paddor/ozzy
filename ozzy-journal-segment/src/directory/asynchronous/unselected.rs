//! Removal of storage objects the selected manifest no longer references.
//!
//! An exclusive pass runs while the owner holds the journal. A concurrent
//! pass runs as a job beside later writes over a captured selection: it only
//! removes objects older than that capture, so nothing the owner creates or
//! publishes afterwards can match.

mod captured;

use super::{DirectoryError, Journal, Limits};
use crate::async_files::Access;
use crate::directory::{
    SPARE_SEGMENT, index_name_segment_id, manifest_name, parse_checkpoint_name,
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
    collections::HashSet,
    path::{Path, PathBuf},
};

pub use captured::UnselectedFiles;

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

    /// Delete at most `max_segments` unselected physical segment generations,
    /// keeping one regular file as the spare unless one with written bytes
    /// exists. Directory visits and each segment's derived indexes are bounded
    /// by the journal's listing limits. Selected generations and captured
    /// readers win.
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
        // A preallocated spare keeps its length at zero: it has no written
        // blocks, and a retired file replaces it.
        let mut spare = false;
        if entries.iter().any(|entry| entry.name == SPARE_SEGMENT) {
            let path = self.root.join("segments").join(SPARE_SEGMENT);
            spare = regular_file_length(self.access, path)
                .await?
                .is_some_and(|length| length != 0);
        }
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
            self.remove_indexes(id, &index_entries, &mut result.reclaimed_bytes)
                .await?;
            spare = self
                .remove_or_keep(&entry, spare, &mut result.reclaimed_bytes)
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

    /// Unlink one unselected segment file, or rename it over the spare when
    /// no `spare` with written bytes exists yet and it is a regular file. The
    /// next roll then overwrites its written blocks; the rename frees a
    /// preallocated spare. Returns whether a spare with written bytes exists.
    async fn remove_or_keep(
        &mut self,
        segment: &ozzy_io::Entry,
        spare: bool,
        reclaimed_bytes: &mut u64,
    ) -> Result<bool, DirectoryError> {
        let directory = self.root.join("segments");
        let path = directory.join(&segment.name);
        let keep = !spare && segment.kind == FileKind::File;
        if keep {
            let destination = directory.join(SPARE_SEGMENT);
            self.access
                .done(Operation::Rename {
                    source: path,
                    destination,
                })
                .await?;
        } else {
            self.remove(path, reclaimed_bytes).await?;
        }
        self.access.sync_directory(directory).await?;
        Ok(spare || keep)
    }

    /// Remove the derived indexes of an unselected segment generation, unless
    /// a selected generation shares its ID, and sync their directory before
    /// the segment file goes.
    async fn remove_indexes(
        &mut self,
        id: u64,
        index_entries: &[ozzy_io::Entry],
        reclaimed_bytes: &mut u64,
    ) -> Result<(), DirectoryError> {
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
    let length = regular_file_length(access, path.clone()).await?;
    if length.is_some() {
        access.done(Operation::RemoveFile { path }).await?;
    }
    Ok(length.unwrap_or(0))
}

/// A regular file's length, or none when nothing has its name.
async fn regular_file_length(
    access: &Access,
    path: PathBuf,
) -> Result<Option<u64>, DirectoryError> {
    let opened = access.open(path, OpenMode::Read, false, false).await;
    let file = match opened {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let length = access.length(&file).await?;
    access.done(Operation::Close { handle: file }).await?;
    Ok(Some(length))
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
}
