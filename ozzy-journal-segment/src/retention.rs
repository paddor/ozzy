//! Committed trim floors and live immutable-segment pins.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

use ozzy_proto::{CheckpointId, Offset, PartitionIncarnation};
use thiserror::Error;

/// Exact committed earliest-retained offset per partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionFloors {
    entries: Vec<(PartitionIncarnation, Offset)>,
    protected_operation: Option<u64>,
}

impl RetentionFloors {
    /// Validate and sort unique partition-incarnation retention floors.
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
        Ok(Self {
            entries,
            protected_operation: None,
        })
    }

    /// Look up the declared retention floor for an exact partition incarnation.
    pub fn get(&self, partition: PartitionIncarnation) -> Option<Offset> {
        self.entries
            .binary_search_by(|(key, _)| key.as_bytes().cmp(partition.as_bytes()))
            .ok()
            .map(|index| self.entries[index].1)
    }

    /// Iterate validated partition retention floors in sorted order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (PartitionIncarnation, Offset)> + '_ {
        self.entries.iter().copied()
    }

    /// Number of partition retention floors.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no partition retention floors remain.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Earliest operation carrying retained duplicate retry fingerprints or
    /// held scheduled records. A checkpoint of coordinates alone cannot
    /// replace them.
    pub const fn protected_operation(&self) -> Option<u64> {
        self.protected_operation
    }

    #[cfg(test)]
    pub(crate) fn with_protected_operation(mut self, operation: u64) -> Self {
        self.protected_operation = Some(operation);
        self
    }

    /// Also protect `operation` and later ones, such as the first operation
    /// of the oldest segment still holding scheduled records.
    #[must_use]
    pub fn protect_from(mut self, operation: Option<u64>) -> Self {
        self.protected_operation = match (self.protected_operation, operation) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self
    }

    /// Derive every committed partition floor from one canonical state image.
    /// Held scheduled records live in segment indexes; the owner adds their
    /// floor with `protect_from`.
    pub fn from_canonical_state(
        state: &ozzy_core::state::CanonicalState,
    ) -> Result<Self, RetentionError> {
        let mut floors = Self::new(
            state
                .partitions()
                .map(|(partition, value)| (partition, value.retained_from))
                .collect(),
        )?;
        floors.protected_operation = state
            .partitions()
            .flat_map(|(_, partition)| partition.producers())
            .filter_map(|(_, producer)| producer.result_operation_floor())
            .min();
        Ok(floors)
    }
}

/// Manifest publication only. Incremental orphan cleanup performs deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredPrefix {
    /// Retired segment IDs removed from the selected manifest.
    pub unreferenced_segment_ids: Vec<u64>,
    /// Physical segments checked against canonical retention floors.
    pub scanned_segments: usize,
    /// Physical segment bytes visited by this bounded scan.
    pub scanned_bytes: usize,
}

/// Result of deleting segment generations absent from the selected manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreferencedSegmentCleanup {
    /// Physical segment IDs removed after their derived indexes were synchronized.
    pub removed_segment_ids: Vec<u64>,
    /// Unselected segment IDs retained by live deletion protection.
    pub pinned_segment_ids: Vec<u64>,
    /// Logical file bytes removed, excluding allocation overhead.
    pub reclaimed_bytes: u64,
}

/// Result of deleting checkpoint directories absent from the selected manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreferencedCheckpointCleanup {
    /// Unselected checkpoint directories removed in this turn.
    pub removed_checkpoint_ids: Vec<CheckpointId>,
    /// Unselected checkpoint IDs retained by live build or read protection.
    pub pinned_checkpoint_ids: Vec<CheckpointId>,
    /// Logical file bytes removed, excluding allocation overhead.
    pub reclaimed_bytes: u64,
}

/// Result of deleting manifest generations outside the protected recovery sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreferencedMetadataCleanup {
    /// Unselected and unprotected manifest generations removed.
    pub removed_manifest_generations: Vec<u64>,
    /// Recognized unselected temporary metadata files removed.
    pub removed_temporary_files: Vec<String>,
    /// Logical file bytes removed, excluding allocation overhead.
    pub reclaimed_bytes: u64,
}

/// Protect a checkpoint and its source manifest while a build or reader holds it.
#[derive(Debug)]
pub(crate) struct CheckpointLease {
    checkpoint_id: CheckpointId,
    source_manifest_generation: u64,
    registry: Rc<PinRegistry>,
    _group_lock: ozzy_io::Handle,
}

impl Drop for CheckpointLease {
    fn drop(&mut self) {
        self.registry.checkpoints.release(self.checkpoint_id);
        self.registry
            .metadata
            .release(self.source_manifest_generation);
    }
}

/// Owner-local pin counts. Every holder lives on the journal's thread.
#[derive(Debug, Default)]
pub(crate) struct PinRegistry {
    segments: Counts<u64>,
    checkpoints: Counts<CheckpointId>,
    metadata: Counts<u64>,
}

#[derive(Debug)]
struct Counts<K>(RefCell<HashMap<K, usize>>);

impl<K> Default for Counts<K> {
    fn default() -> Self {
        Self(RefCell::new(HashMap::new()))
    }
}

impl<K: Copy + Eq + Hash> Counts<K> {
    fn acquire(&self, key: K) -> Result<(), RetentionError> {
        let mut counts = self.0.borrow_mut();
        let count = counts.entry(key).or_default();
        *count = count.checked_add(1).ok_or(RetentionError::PinOverflow)?;
        Ok(())
    }

    fn release(&self, key: K) {
        let mut counts = self.0.borrow_mut();
        if let Some(count) = counts.get_mut(&key) {
            debug_assert_ne!(*count, 0);
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&key);
            }
        }
    }

    fn contains(&self, key: K) -> bool {
        self.0.borrow().contains_key(&key)
    }
}

/// Protect a reserved successor before its file becomes visible to cleanup.
#[derive(Debug)]
pub(crate) struct PreparedSegmentLease {
    segment_id: u64,
    registry: Rc<PinRegistry>,
}

impl PreparedSegmentLease {
    pub(crate) const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Protect the same segment with an independent lease.
    pub(crate) fn duplicate(&self) -> Result<Self, RetentionError> {
        self.registry.protect_prepared_segment(self.segment_id)
    }
}

impl Drop for PreparedSegmentLease {
    fn drop(&mut self) {
        self.registry.segments.release(self.segment_id);
    }
}

impl PinRegistry {
    pub(crate) fn protect_prepared_segment(
        self: &Rc<Self>,
        segment_id: u64,
    ) -> Result<PreparedSegmentLease, RetentionError> {
        self.segments.acquire(segment_id)?;
        Ok(PreparedSegmentLease {
            segment_id,
            registry: Rc::clone(self),
        })
    }

    pub(crate) fn is_pinned(&self, segment_id: u64) -> bool {
        self.segments.contains(segment_id)
    }

    pub(crate) fn acquire_checkpoint_lease(
        self: &Rc<Self>,
        checkpoint_id: CheckpointId,
        source_manifest_generation: u64,
        group_lock: ozzy_io::Handle,
    ) -> Result<CheckpointLease, RetentionError> {
        self.checkpoints.acquire(checkpoint_id)?;
        if let Err(error) = self.metadata.acquire(source_manifest_generation) {
            self.checkpoints.release(checkpoint_id);
            return Err(error);
        }
        Ok(CheckpointLease {
            checkpoint_id,
            source_manifest_generation,
            registry: Rc::clone(self),
            _group_lock: group_lock,
        })
    }

    pub(crate) fn checkpoint_is_pinned(&self, checkpoint_id: CheckpointId) -> bool {
        self.checkpoints.contains(checkpoint_id)
    }

    pub(crate) fn metadata_is_pinned(&self, generation: u64) -> bool {
        self.metadata.contains(generation)
    }
}

/// Retention plan or pin failure.
#[derive(Debug, Error)]
pub enum RetentionError {
    #[error("retention floor contains a zero partition identity")]
    /// Retention floor contains a zero partition identity.
    ZeroPartition,
    #[error("retention floor contains a duplicate partition")]
    /// Retention floor contains a duplicate partition.
    DuplicatePartition,
    #[error("segment pin request contains a duplicate segment")]
    /// Segment pin request contains a duplicate segment.
    DuplicateSegment,
    #[error("segment pin set is empty")]
    /// Segment pin set is empty.
    EmptyPin,
    #[error("segment pin count overflow")]
    /// Segment pin count overflow.
    PinOverflow,
    #[error("retention integer or length overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
}
