//! Committed trim floors and live immutable-segment pins.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ozzy_journal::operation::{OperationBody, OperationLimits, decode_operation_body};
use ozzy_proto::{CheckpointId, Offset, PartitionIncarnation};
use thiserror::Error;

use crate::store_lock::StoreLock;
use crate::{CheckpointImage, CheckpointLimits, SegmentReference, SegmentScan};

/// Exact committed earliest-retained offset per partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionFloors {
    entries: Vec<(PartitionIncarnation, Offset)>,
}

impl RetentionFloors {
    pub fn new(mut entries: Vec<(PartitionIncarnation, Offset)>) -> Result<Self, RetentionError> {
        if entries
            .iter()
            .any(|(partition, _)| partition.as_bytes().iter().all(|byte| *byte == 0))
        {
            return Err(RetentionError::ZeroPartition);
        }
        entries.sort_unstable_by_key(|(partition, _)| *partition.as_bytes());
        if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(RetentionError::DuplicatePartition);
        }
        Ok(Self { entries })
    }

    pub fn get(&self, partition: PartitionIncarnation) -> Option<Offset> {
        self.entries
            .binary_search_by(|(key, _)| key.as_bytes().cmp(partition.as_bytes()))
            .ok()
            .map(|index| self.entries[index].1)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = (PartitionIncarnation, Offset)> + '_ {
        self.entries.iter().copied()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Derive every committed partition floor from one canonical state image.
    pub fn from_canonical_state(
        state: &ozzy_core::state::CanonicalState,
    ) -> Result<Self, RetentionError> {
        Self::new(
            state
                .partitions()
                .map(|(partition, value)| (partition, value.retained_from))
                .collect(),
        )
    }
}

/// Result after a manifest-first physical retention pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionResult {
    /// Segments removed from the selected manifest.
    pub unreferenced_segment_ids: Vec<u64>,
    /// Unreferenced segments physically deleted during this call.
    pub removed_segment_ids: Vec<u64>,
    pub reclaimed_bytes: u64,
    /// First otherwise-eligible segment held by a live reader/transfer.
    pub blocked_by_pin: Option<u64>,
}

/// Bounds for retiring a checkpoint-covered sealed prefix. One segment is
/// indivisible; a budget smaller than the oldest eligible segment refuses work.
#[derive(Debug, Clone, Copy)]
pub struct RetentionScanBudget {
    pub max_segments: usize,
    pub max_read_bytes: usize,
    pub max_work: std::time::Duration,
}

/// Manifest publication only. Incremental orphan cleanup performs deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredPrefix {
    pub unreferenced_segment_ids: Vec<u64>,
    pub scanned_segments: usize,
    pub scanned_bytes: usize,
}

/// Result of deleting segment generations absent from the selected manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreferencedSegmentCleanup {
    pub removed_segment_ids: Vec<u64>,
    pub pinned_segment_ids: Vec<u64>,
    pub reclaimed_bytes: u64,
}

/// Result of deleting checkpoint directories absent from the selected manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreferencedCheckpointCleanup {
    pub removed_checkpoint_ids: Vec<CheckpointId>,
    pub pinned_checkpoint_ids: Vec<CheckpointId>,
    pub reclaimed_bytes: u64,
}

/// Result of deleting manifest generations outside the protected recovery sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreferencedMetadataCleanup {
    pub removed_manifest_generations: Vec<u64>,
    pub removed_temporary_files: Vec<String>,
    pub reclaimed_bytes: u64,
}

/// Owned immutable segment set. Holding it keeps group ownership locked.
#[derive(Debug)]
pub struct SegmentPin {
    root: PathBuf,
    references: Vec<SegmentReference>,
    registry: Arc<PinRegistry>,
    _group_lock: Arc<StoreLock>,
}

impl SegmentPin {
    pub fn references(&self) -> &[SegmentReference] {
        &self.references
    }

    pub fn segment_path(&self, segment_id: u64) -> Option<PathBuf> {
        self.references
            .iter()
            .find(|reference| reference.segment_id == segment_id)
            .map(|reference| self.root.join(reference.file_name()))
    }
}

impl Drop for SegmentPin {
    fn drop(&mut self) {
        self.registry.release(&self.references);
    }
}

/// Validated selected checkpoint protected from physical cleanup.
#[derive(Debug)]
pub struct CheckpointPin {
    image: CheckpointImage,
    limits: CheckpointLimits,
}

impl CheckpointPin {
    pub const fn image(&self) -> &CheckpointImage {
        &self.image
    }

    pub fn read_state(&self) -> Result<Vec<u8>, crate::CheckpointError> {
        self.image.read_state(self.limits)
    }
}

#[derive(Debug)]
pub(crate) struct CheckpointLease<L = Arc<StoreLock>> {
    checkpoint_id: CheckpointId,
    source_manifest_generation: u64,
    registry: Arc<PinRegistry>,
    _group_lock: L,
}

impl<L> Drop for CheckpointLease<L> {
    fn drop(&mut self) {
        self.registry
            .release_checkpoint(self.checkpoint_id, self.source_manifest_generation);
    }
}

#[derive(Debug, Default)]
pub(crate) struct PinRegistry {
    segments: Mutex<HashMap<u64, usize>>,
    checkpoints: Mutex<HashMap<CheckpointId, usize>>,
    metadata: Mutex<HashMap<u64, usize>>,
}

/// Protect a reserved successor before its file becomes visible to cleanup.
#[derive(Debug)]
pub(crate) struct PreparedSegmentLease {
    segment_id: u64,
    registry: Arc<PinRegistry>,
}

impl Drop for PreparedSegmentLease {
    fn drop(&mut self) {
        let Ok(mut counts) = self.registry.segments.lock() else {
            return;
        };
        if let Some(count) = counts.get_mut(&self.segment_id) {
            debug_assert_ne!(*count, 0);
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.segment_id);
            }
        }
    }
}

impl PinRegistry {
    pub(crate) fn protect_prepared_segment(
        self: &Arc<Self>,
        segment_id: u64,
    ) -> Result<PreparedSegmentLease, RetentionError> {
        let mut counts = self.segments.lock().map_err(|_| RetentionError::Poisoned)?;
        let count = counts.entry(segment_id).or_default();
        *count = count.checked_add(1).ok_or(RetentionError::PinOverflow)?;
        Ok(PreparedSegmentLease {
            segment_id,
            registry: Arc::clone(self),
        })
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
        root: PathBuf,
        references: Vec<SegmentReference>,
        group_lock: Arc<StoreLock>,
    ) -> Result<SegmentPin, RetentionError> {
        if references.is_empty() {
            return Err(RetentionError::EmptyPin);
        }
        let mut counts = self.segments.lock().map_err(|_| RetentionError::Poisoned)?;
        if references.iter().any(|reference| {
            counts
                .get(&reference.segment_id)
                .is_some_and(|count| *count == usize::MAX)
        }) {
            return Err(RetentionError::PinOverflow);
        }
        for reference in &references {
            let count = counts.entry(reference.segment_id).or_default();
            *count += 1;
        }
        drop(counts);
        Ok(SegmentPin {
            root,
            references,
            registry: Arc::clone(self),
            _group_lock: group_lock,
        })
    }

    pub(crate) fn is_pinned(&self, segment_id: u64) -> Result<bool, RetentionError> {
        let ordinary = self
            .segments
            .lock()
            .map_err(|_| RetentionError::Poisoned)?
            .get(&segment_id)
            .is_some_and(|count| *count != 0);
        Ok(ordinary)
    }

    pub(crate) fn acquire_checkpoint(
        self: &Arc<Self>,
        mut image: CheckpointImage,
        limits: CheckpointLimits,
        group_lock: Arc<StoreLock>,
    ) -> Result<CheckpointPin, RetentionError> {
        let checkpoint_id = image.manifest().checkpoint_id;
        image.attach_lease(self.acquire_checkpoint_lease(
            checkpoint_id,
            image.manifest().source_manifest_generation,
            group_lock,
        )?);
        Ok(CheckpointPin { image, limits })
    }

    pub(crate) fn acquire_checkpoint_lease<L>(
        self: &Arc<Self>,
        checkpoint_id: CheckpointId,
        source_manifest_generation: u64,
        group_lock: L,
    ) -> Result<CheckpointLease<L>, RetentionError> {
        let mut counts = self
            .checkpoints
            .lock()
            .map_err(|_| RetentionError::Poisoned)?;
        let count = counts.entry(checkpoint_id).or_default();
        let mut metadata = self.metadata.lock().map_err(|_| RetentionError::Poisoned)?;
        let source_count = metadata.entry(source_manifest_generation).or_default();
        if *count == usize::MAX || *source_count == usize::MAX {
            return Err(RetentionError::PinOverflow);
        }
        *count += 1;
        *source_count += 1;
        drop(counts);
        Ok(CheckpointLease {
            checkpoint_id,
            source_manifest_generation,
            registry: Arc::clone(self),
            _group_lock: group_lock,
        })
    }

    pub(crate) fn checkpoint_is_pinned(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<bool, RetentionError> {
        Ok(self
            .checkpoints
            .lock()
            .map_err(|_| RetentionError::Poisoned)?
            .get(&checkpoint_id)
            .is_some_and(|count| *count != 0))
    }

    pub(crate) fn metadata_is_pinned(&self, generation: u64) -> Result<bool, RetentionError> {
        Ok(self
            .metadata
            .lock()
            .map_err(|_| RetentionError::Poisoned)?
            .get(&generation)
            .is_some_and(|count| *count != 0))
    }

    pub(crate) fn any_pinned(&self) -> Result<bool, RetentionError> {
        let segments = self
            .segments
            .lock()
            .map_err(|_| RetentionError::Poisoned)?
            .values()
            .any(|count| *count != 0);
        let checkpoints = self
            .checkpoints
            .lock()
            .map_err(|_| RetentionError::Poisoned)?
            .values()
            .any(|count| *count != 0);
        Ok(segments || checkpoints)
    }

    fn release(&self, references: &[SegmentReference]) {
        let Ok(mut counts) = self.segments.lock() else {
            return;
        };
        for reference in references {
            if let std::collections::hash_map::Entry::Occupied(mut entry) =
                counts.entry(reference.segment_id)
            {
                let count = entry.get_mut();
                debug_assert_ne!(*count, 0);
                *count = count.saturating_sub(1);
                if *count == 0 {
                    entry.remove();
                }
            }
        }
    }

    fn release_checkpoint(&self, checkpoint_id: CheckpointId, source_manifest_generation: u64) {
        let Ok(mut counts) = self.checkpoints.lock() else {
            return;
        };
        let remove = if let Some(count) = counts.get_mut(&checkpoint_id) {
            debug_assert_ne!(*count, 0);
            *count = count.saturating_sub(1);
            *count == 0
        } else {
            false
        };
        if remove {
            counts.remove(&checkpoint_id);
        }
        let Ok(mut metadata) = self.metadata.lock() else {
            return;
        };
        if let Some(count) = metadata.get_mut(&source_manifest_generation) {
            debug_assert_ne!(*count, 0);
            *count = count.saturating_sub(1);
            if *count == 0 {
                metadata.remove(&source_manifest_generation);
            }
        }
    }
}

pub(crate) fn scan_is_below_floors(
    scan: &SegmentScan<'_>,
    through_op: u64,
    floors: &RetentionFloors,
    limits: OperationLimits,
) -> Result<bool, RetentionError> {
    for operation in scan.groups.iter().flat_map(|group| &group.operations) {
        if operation.op_number > through_op {
            return Ok(false);
        }
        let OperationBody::Append(append) =
            decode_operation_body(operation.kind, operation.body.as_ref(), limits)?
        else {
            continue;
        };
        for batch in append.batches {
            let Some(floor) = floors.get(batch.partition) else {
                return Ok(false);
            };
            let record_count =
                u64::try_from(batch.records.len()).map_err(|_| RetentionError::LengthOverflow)?;
            let Some(end) = batch.first_offset.get().checked_add(record_count) else {
                return Err(RetentionError::LengthOverflow);
            };
            if end > floor.get() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

pub(crate) fn segments_root(root: &Path) -> PathBuf {
    root.join("segments")
}

/// Retention plan, pin, or canonical-body failure.
#[derive(Debug, Error)]
pub enum RetentionError {
    #[error(transparent)]
    Operation(#[from] ozzy_journal::operation::OperationCodecError),
    #[error("retention floor contains a zero partition identity")]
    ZeroPartition,
    #[error("retention floor contains a duplicate partition")]
    DuplicatePartition,
    #[error("segment pin request contains a duplicate segment")]
    DuplicateSegment,
    #[error("segment pin set is empty")]
    EmptyPin,
    #[error("segment pin count overflow")]
    PinOverflow,
    #[error("segment pin registry is poisoned")]
    Poisoned,
    #[error("retention integer or length overflow")]
    LengthOverflow,
}
