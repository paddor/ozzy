use super::{DirectoryError, Journal};
use crate::directory::{index_name_segment_id, parse_segment_name};
use crate::{OrphanCleanupStep, RetentionError, UnreferencedSegmentCleanup};
use ozzy_io::{OpenMode, Operation};
use std::{collections::HashSet, path::PathBuf};

impl Journal {
    /// Remove bounded derived indexes whose segment ID is neither selected nor
    /// protected by readers/preparation. Handles indexes left without payload
    /// files after interrupted cleanup. Unknown filenames stay untouched.
    pub async fn reclaim_unreferenced_indexes(
        &mut self,
        max_files: usize,
    ) -> Result<OrphanCleanupStep, DirectoryError> {
        self.healthy()?;
        if max_files == 0 {
            return Err(DirectoryError::RetentionScanBudget);
        }
        self.validate_authority_files().await?;
        let directory = self.root().join("indexes");
        let mut entries = self
            .access
            .list(
                directory.clone(),
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            )
            .await?;
        entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
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
                || self.pins.is_pinned(id)?
            {
                result.deferred_sources += 1;
                continue;
            }
            self.interrupted = true;
            let bytes = self.remove_regular_file(directory.join(entry.name)).await?;
            result.reclaimed_bytes = result
                .reclaimed_bytes
                .checked_add(bytes)
                .ok_or(RetentionError::LengthOverflow)?;
            result.removed_files += 1;
        }
        if self.interrupted {
            self.access.sync_directory(directory).await?;
            self.interrupted = false;
        }
        result.complete = result.work_units == count;
        Ok(result)
    }

    /// Delete at most `max_segments` unselected physical segment generations.
    /// Directory visits and each segment's derived indexes are bounded by the
    /// journal's listing limits. Selected generations and captured readers win.
    pub async fn reclaim_unreferenced_segments(
        &mut self,
        max_segments: usize,
    ) -> Result<UnreferencedSegmentCleanup, DirectoryError> {
        self.healthy()?;
        if max_segments == 0 {
            return Err(DirectoryError::RetentionScanBudget);
        }
        self.validate_authority_files().await?;
        let directory = self.root().join("segments");
        let indexes = self.root().join("indexes");
        let mut entries = self
            .access
            .list(
                directory.clone(),
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            )
            .await?;
        let index_entries = self
            .access
            .list(
                indexes.clone(),
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            )
            .await?;
        entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
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
        let mut removed = 0;
        for entry in entries {
            if removed == max_segments {
                break;
            }
            let Some(id) = parse_segment_name(&entry.name) else {
                continue;
            };
            if selected.contains(entry.name.to_str().expect("parsed ASCII segment name")) {
                continue;
            }
            if self.pins.is_pinned(id)? {
                result.pinned_segment_ids.push(id);
                continue;
            }
            self.interrupted = true;
            let live_id = self
                .manifest
                .segments
                .iter()
                .any(|reference| reference.segment_id == id);
            if !live_id {
                for index in &index_entries {
                    if index_name_segment_id(&index.name) == Some(id) {
                        let bytes = self.remove_regular_file(indexes.join(&index.name)).await?;
                        result.reclaimed_bytes = result
                            .reclaimed_bytes
                            .checked_add(bytes)
                            .ok_or(RetentionError::LengthOverflow)?;
                    }
                }
                // Required even if a prior interrupted pass already unlinked
                // every index: make that deletion durable before payload removal.
                self.access.sync_directory(indexes.clone()).await?;
            }
            let bytes = self.remove_regular_file(directory.join(entry.name)).await?;
            result.reclaimed_bytes = result
                .reclaimed_bytes
                .checked_add(bytes)
                .ok_or(RetentionError::LengthOverflow)?;
            self.access.sync_directory(directory.clone()).await?;
            result.removed_segment_ids.push(id);
            removed += 1;
            self.interrupted = false;
        }
        result.removed_segment_ids.sort_unstable();
        result.removed_segment_ids.dedup();
        result.pinned_segment_ids.sort_unstable();
        result.pinned_segment_ids.dedup();
        Ok(result)
    }

    pub(super) async fn remove_regular_file(&self, path: PathBuf) -> Result<u64, DirectoryError> {
        let opened = self
            .access
            .open(path.clone(), OpenMode::Read, false, false)
            .await;
        let file = match opened {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        let length = self.access.length(&file).await?;
        self.access.done(Operation::Close { handle: file }).await?;
        self.access.done(Operation::RemoveFile { path }).await?;
        Ok(length)
    }
}
