//! Typed bridge between opaque checkpoint artifacts and canonical state.

use ozzy_core::checkpoint::{Decoder, Layout};
use ozzy_core::state::{
    CanonicalState, StateLimits, StateSnapshotError, StateSnapshotLimits,
    canonical_state_schema_digest,
};
use ozzy_proto::CheckpointId;
use thiserror::Error;

use crate::{
    CheckpointError, CheckpointImage, CheckpointLimits, CheckpointPlan, OpenGroupJournal,
    checkpoint_name, open_checkpoint,
};

impl CheckpointLimits {
    /// Cover a bounded core snapshot and every configured identity row. Choose
    /// physical chunk sizes that fit the directory budget without increasing
    /// backend I/O fragment sizes. No state bytes are allocated here.
    pub fn for_canonical_image(
        core_bytes: usize,
        entries: u64,
        listing: (usize, usize),
    ) -> Result<Self, CheckpointError> {
        // Chunk files use CHUNK.00000000; one additional entry holds the manifest.
        // Physical files can grow without increasing per-job I/O or codec scratch.
        let max_chunks = listing
            .0
            .saturating_sub(1)
            .min(listing.1.saturating_sub("manifest".len()) / "CHUNK.00000000".len())
            .min(512);
        if max_chunks == 0 || core_bytes < ozzy_core::state::STATE_SNAPSHOT_HEADER_BYTES {
            return Err(CheckpointError::InvalidLimits);
        }
        let max_state_bytes = entries
            .checked_mul(ozzy_core::dedup::SNAPSHOT_ROW_BYTES as u64)
            .and_then(|bytes| bytes.checked_add(core_bytes as u64))
            .and_then(|bytes| bytes.checked_add(ozzy_core::checkpoint::HEADER_BYTES as u64))
            .ok_or(CheckpointError::LengthOverflow)?;
        let chunk_bytes = usize::try_from(max_state_bytes.div_ceil(max_chunks as u64))
            .ok()
            .and_then(|bytes| {
                bytes
                    .div_ceil(crate::WRITE_GROUP_ALIGNMENT)
                    .checked_mul(crate::WRITE_GROUP_ALIGNMENT)
            })
            .filter(|&bytes| u32::try_from(bytes).is_ok())
            .ok_or(CheckpointError::InvalidChunkBytes)?;
        Ok(CheckpointLimits {
            max_manifest_bytes: crate::CHECKPOINT_HEADER_BYTES
                + max_chunks * crate::CHECKPOINT_CHUNK_ENTRY_BYTES,
            max_chunks,
            max_chunk_bytes: chunk_bytes,
            max_state_bytes,
        })
    }
}

impl CheckpointPlan {
    /// Encode and publish canonical state matching this plan's exact position.
    pub fn build_canonical(
        self,
        state: &CanonicalState,
        state_limits: StateSnapshotLimits,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<CheckpointImage, CanonicalCheckpointError> {
        let spec = self.spec();
        if spec.state_schema_digest != canonical_state_schema_digest() {
            return Err(CanonicalCheckpointError::SchemaMismatch);
        }
        if state.revision() != spec.position.op_number {
            return Err(CanonicalCheckpointError::PositionMismatch);
        }
        let mut bytes = state.encode_snapshot(state_limits)?;
        let header = Layout::new(bytes.len() as u64, None)?.encode();
        let core_bytes = bytes.len();
        bytes.resize(core_bytes + header.len(), 0);
        bytes.copy_within(..core_bytes, header.len());
        bytes[..header.len()].copy_from_slice(&header);
        Ok(self.build(&bytes, checkpoint_limits)?)
    }
}

impl CheckpointImage {
    /// Decode canonical state only when schema and operation position agree.
    pub fn decode_canonical_state(
        &self,
        state_limits: StateLimits,
        snapshot_limits: StateSnapshotLimits,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<CanonicalState, CanonicalCheckpointError> {
        if self.manifest().state_schema_digest != canonical_state_schema_digest() {
            return Err(CanonicalCheckpointError::SchemaMismatch);
        }
        let mut decoder = Decoder::new(
            self.manifest().state_bytes,
            snapshot_limits.max_snapshot_bytes,
        );
        self.visit_state(checkpoint_limits, |bytes| {
            decoder.push(bytes, |_| Ok(()), |_, _| Ok(()))?;
            Ok::<_, CanonicalCheckpointError>(())
        })?;
        let (layout, bytes) = decoder.finish()?;
        let state = CanonicalState::decode_snapshot(&bytes, state_limits, snapshot_limits)?;
        layout.validate_state(&state)?;
        if state.revision() != self.manifest().position.op_number {
            return Err(CanonicalCheckpointError::PositionMismatch);
        }
        Ok(state)
    }
}

impl crate::checkpoint::asynchronous::Checkpoint {
    pub(crate) async fn decode_canonical_state(
        &self,
        state_limits: StateLimits,
        snapshot_limits: StateSnapshotLimits,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<CanonicalState, CanonicalCheckpointError> {
        if self.manifest.state_schema_digest != canonical_state_schema_digest() {
            return Err(CanonicalCheckpointError::SchemaMismatch);
        }
        let mut budget = crate::cooperative::Budget::default();
        let mut decoder = Decoder::new(
            self.manifest.state_bytes,
            snapshot_limits.max_snapshot_bytes,
        );
        let mut reader = crate::checkpoint::asynchronous::Reader::new(self, checkpoint_limits);
        while let Some(bytes) = reader.next(64 * 1024).await? {
            decoder.push(&bytes, |_| Ok(()), |_, _| Ok(()))?;
            budget.charge(bytes.len()).await;
        }
        let (layout, bytes) = decoder.finish()?;
        let state = CanonicalState::decode_snapshot_cooperative(
            &bytes,
            state_limits,
            snapshot_limits,
            async |bytes| budget.charge(bytes).await,
        )
        .await?;
        layout.validate_state(&state)?;
        if state.revision() != self.manifest.position.op_number {
            return Err(CanonicalCheckpointError::PositionMismatch);
        }
        Ok(state)
    }
}

impl OpenGroupJournal {
    /// Freeze a checkpoint target using the built-in canonical state schema.
    pub fn canonical_checkpoint_plan(
        &self,
        checkpoint_id: CheckpointId,
        chunk_bytes: usize,
    ) -> Result<CheckpointPlan, crate::DirectoryError> {
        self.checkpoint_plan(checkpoint_id, canonical_state_schema_digest(), chunk_bytes)
    }

    /// Validate and install one canonical checkpoint artifact.
    pub fn install_canonical_checkpoint(
        self,
        checkpoint_id: CheckpointId,
        state_limits: StateLimits,
        snapshot_limits: StateSnapshotLimits,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<Self, CanonicalCheckpointError> {
        let identity = self.directory().identity();
        let image = open_checkpoint(
            self.directory()
                .root()
                .join("checkpoints")
                .join(checkpoint_name(checkpoint_id)),
            identity.group_id,
            identity.store_id,
            checkpoint_limits,
        )?;
        image.decode_canonical_state(state_limits, snapshot_limits, checkpoint_limits)?;
        Ok(self.install_checkpoint(checkpoint_id, checkpoint_limits)?)
    }

    /// Restore canonical state from the currently selected checkpoint, if any.
    pub fn selected_canonical_checkpoint(
        &self,
        state_limits: StateLimits,
        snapshot_limits: StateSnapshotLimits,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<Option<CanonicalState>, CanonicalCheckpointError> {
        let Some(reference) = self.directory().manifest().checkpoint else {
            return Ok(None);
        };
        let identity = self.directory().identity();
        let image = open_checkpoint(
            self.directory()
                .root()
                .join("checkpoints")
                .join(checkpoint_name(reference.checkpoint_id)),
            identity.group_id,
            identity.store_id,
            checkpoint_limits,
        )?;
        if image.manifest_digest() != reference.manifest_digest
            || image.manifest().position != reference.position
        {
            return Err(CanonicalCheckpointError::SelectionMismatch);
        }
        image
            .decode_canonical_state(state_limits, snapshot_limits, checkpoint_limits)
            .map(Some)
    }
}

/// Canonical state encoding, schema, selection, or outer artifact failure.
#[derive(Debug, Error)]
pub enum CanonicalCheckpointError {
    #[error(transparent)]
    /// Canonical metadata and independently streamed identity sections disagree.
    Sections(#[from] ozzy_core::checkpoint::Error),
    #[error(transparent)]
    /// Checkpoint construction or validation failed.
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    /// Journal directory validation or publication failed.
    Directory(#[from] crate::DirectoryError),
    #[error(transparent)]
    /// Canonical application-state snapshot validation failed.
    State(#[from] StateSnapshotError),
    #[error("checkpoint does not use the canonical state schema")]
    /// Checkpoint does not use the canonical state schema.
    SchemaMismatch,
    #[error("canonical state revision does not match checkpoint position")]
    /// Canonical state revision does not match checkpoint position.
    PositionMismatch,
    #[error("canonical checkpoint does not match selected manifest reference")]
    /// Canonical checkpoint does not match selected manifest reference.
    SelectionMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    const CORE_CHECKPOINT_BYTES: usize = 32 * 1024 * 1024;
    #[test]
    fn checkpoint_capacity_covers_the_full_configured_identity_window() {
        for entries in [0, 1, 24_000_000, 48_000_000] {
            for listing in [(64, 4096), (512, 8192), (1024, 64 * 1024)] {
                let limits =
                    CheckpointLimits::for_canonical_image(CORE_CHECKPOINT_BYTES, entries, listing)
                        .unwrap();
                let required = CORE_CHECKPOINT_BYTES as u64
                    + ozzy_core::checkpoint::HEADER_BYTES as u64
                    + entries * ozzy_core::dedup::SNAPSHOT_ROW_BYTES as u64;
                assert_eq!(limits.max_state_bytes, required);
                let chunks = required.div_ceil(limits.max_chunk_bytes as u64) as usize;
                assert!(chunks <= limits.max_chunks);
                assert!(chunks < listing.0);
                assert!(chunks * "CHUNK.00000000".len() + "manifest".len() <= listing.1);
                assert_eq!(limits.max_chunk_bytes % crate::WRITE_GROUP_ALIGNMENT, 0);
                assert!(u32::try_from(limits.max_chunk_bytes).is_ok());
                assert!(limits.max_manifest_bytes <= 64 * 1024);
            }
        }
    }

    #[test]
    fn checkpoint_capacity_rejects_overflow_and_impossible_directory_budgets() {
        for (entries, listing) in [
            (u64::MAX, (1024, 65536)),
            (1 << 40, (1024, 65536)),
            (0, (0, 4096)),
            (0, (1, 4096)),
            (0, (64, 20)),
            (0, (2, 21)),
        ] {
            assert!(
                CheckpointLimits::for_canonical_image(CORE_CHECKPOINT_BYTES, entries, listing)
                    .is_err()
            );
        }
        assert!(CheckpointLimits::for_canonical_image(CORE_CHECKPOINT_BYTES, 0, (2, 22)).is_ok());
        assert!(CheckpointLimits::for_canonical_image(255, 0, (64, 4096)).is_err());
    }
}
