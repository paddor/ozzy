//! A cleanup pass over a captured selection, run as a job beside later
//! writes.

use super::{Bounds, Reclaim};
use crate::async_files::Access;
use crate::directory::CURRENT_FILE;
use crate::directory::asynchronous::{DirectoryError, Journal, Limits};
use crate::retention::PinRegistry;
use crate::{
    CurrentReference, Manifest, OrphanCleanupStep, UnreferencedCheckpointCleanup,
    UnreferencedMetadataCleanup, UnreferencedSegmentCleanup,
};
use std::{cell::Cell, path::PathBuf, rc::Rc};

impl Journal {
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
    pub(in crate::directory::asynchronous) fn require_no_cleanup(
        &self,
    ) -> Result<(), DirectoryError> {
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
