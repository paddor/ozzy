use super::{DirectoryError, Journal};
use crate::directory::{
    manifest_name, parse_checkpoint_name, parse_current_temporary_name, parse_manifest_name,
    parse_manifest_temporary_name, validate_checkpoint_source,
};
use crate::{
    RetentionError, UnreferencedCheckpointCleanup, UnreferencedMetadataCleanup, decode_manifest,
    manifest_digest,
};
use ozzy_io::{FileKind, Operation};

impl Journal {
    /// Remove bounded unselected checkpoint artifacts. Live build results and
    /// captured readers protect their exact IDs and source manifest generations.
    pub async fn reclaim_unreferenced_checkpoints(
        &mut self,
        max_checkpoints: usize,
    ) -> Result<UnreferencedCheckpointCleanup, DirectoryError> {
        self.healthy()?;
        if max_checkpoints == 0 {
            return Err(DirectoryError::RetentionScanBudget);
        }
        self.validate_authority_files().await?;
        self.selected_checkpoint_source().await?;
        let directory = self.root().join("checkpoints");
        let mut entries = self
            .access
            .list(
                directory.clone(),
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            )
            .await?;
        entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
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
            if self.pins.checkpoint_is_pinned(id)? {
                result.pinned_checkpoint_ids.push(id);
                continue;
            }
            if entry.kind != FileKind::Directory {
                return Err(DirectoryError::NotDirectory("unselected checkpoint"));
            }
            let path = directory.join(entry.name);
            let children = self
                .access
                .list(
                    path.clone(),
                    self.limits.directory_entries,
                    self.limits.directory_name_bytes,
                )
                .await?;
            if children.iter().any(|child| child.kind != FileKind::File) {
                return Err(DirectoryError::NotRegularFile("unselected checkpoint file"));
            }
            self.interrupted = true;
            for child in children {
                let bytes = self.remove_regular_file(path.join(child.name)).await?;
                result.reclaimed_bytes = result
                    .reclaimed_bytes
                    .checked_add(bytes)
                    .ok_or(RetentionError::LengthOverflow)?;
            }
            self.access.sync_directory(path.clone()).await?;
            self.access
                .done(Operation::RemoveDirectory { path })
                .await?;
            self.access.sync_directory(directory.clone()).await?;
            result.removed_checkpoint_ids.push(id);
            self.interrupted = false;
        }
        Ok(result)
    }

    /// Delete bounded unselected manifest generations and recognized temporary
    /// metadata. The current selection, selected checkpoint source, and captured
    /// build/read sources remain protected. Publication files are never inferred
    /// from directory recency.
    pub async fn reclaim_unreferenced_metadata(
        &mut self,
        max_files: usize,
    ) -> Result<UnreferencedMetadataCleanup, DirectoryError> {
        self.healthy()?;
        if max_files == 0 {
            return Err(DirectoryError::RetentionScanBudget);
        }
        self.validate_authority_files().await?;
        let checkpoint_source = self.selected_checkpoint_source().await?;
        let mut entries = self
            .access
            .list(
                self.root().to_path_buf(),
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            )
            .await?;
        entries.sort_unstable_by(|a, b| a.name.cmp(&b.name));
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
                if generation == self.current.generation
                    || Some(generation) == checkpoint_source
                    || self.pins.metadata_is_pinned(generation)?
                {
                    continue;
                }
            } else if parse_manifest_temporary_name(&entry.name).is_none()
                && parse_current_temporary_name(&entry.name).is_none()
            {
                continue;
            }
            self.interrupted = true;
            let bytes = self
                .remove_regular_file(self.root().join(&entry.name))
                .await?;
            result.reclaimed_bytes = result
                .reclaimed_bytes
                .checked_add(bytes)
                .ok_or(RetentionError::LengthOverflow)?;
            if let Some(generation) = generation {
                result.removed_manifest_generations.push(generation);
            } else {
                result.removed_temporary_files.push(
                    entry
                        .name
                        .into_string()
                        .expect("parsed ASCII metadata name"),
                );
            }
        }
        if self.interrupted {
            self.access
                .sync_directory(self.root().to_path_buf())
                .await?;
            self.interrupted = false;
        }
        result.removed_manifest_generations.sort_unstable();
        Ok(result)
    }

    async fn selected_checkpoint_source(&self) -> Result<Option<u64>, DirectoryError> {
        let checkpoint = super::opening::selected_checkpoint(
            &self.access,
            self.root(),
            &self.manifest,
            self.limits,
        )
        .await?;
        let Some(checkpoint) = checkpoint else {
            return Ok(None);
        };
        let generation = checkpoint.manifest.source_manifest_generation;
        let bytes = self
            .access
            .read_file(
                self.root().join(manifest_name(generation)),
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
