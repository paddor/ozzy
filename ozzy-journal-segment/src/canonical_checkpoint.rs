//! Typed bridge between opaque checkpoint artifacts and canonical state.

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
        let bytes = state.encode_snapshot(state_limits)?;
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
        let state = CanonicalState::decode_snapshot(
            &self.read_state(checkpoint_limits)?,
            state_limits,
            snapshot_limits,
        )?;
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
        let state = CanonicalState::decode_snapshot_cooperative(
            &self.read_state(checkpoint_limits).await?,
            state_limits,
            snapshot_limits,
            async |bytes| budget.charge(bytes).await,
        )
        .await?;
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
