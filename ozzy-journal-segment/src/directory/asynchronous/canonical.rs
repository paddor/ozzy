//! Typed canonical-state publication and replay on the asynchronous journal.

mod replay;
mod selected;
pub use selected::Candidate;

use super::{CheckpointFiles, Journal};
use crate::{CanonicalCheckpointError as CheckpointError, CheckpointLimits};
use ozzy_core::state::{
    CanonicalState, StateLimits, StateSnapshotLimits, canonical_state_schema_digest,
};
use ozzy_proto::CheckpointId;

impl Journal {
    /// Encode canonical application state and build a bounded checkpoint from its exact source.
    pub async fn build_canonical_checkpoint(
        &mut self,
        id: CheckpointId,
        chunk_bytes: usize,
        state: &CanonicalState,
        snapshot_limits: StateSnapshotLimits,
    ) -> Result<CheckpointFiles, CheckpointError> {
        self.healthy()?;
        if state.revision() != self.committed_position()?.op_number {
            return Err(CheckpointError::PositionMismatch);
        }
        let mut budget = crate::cooperative::Budget::default();
        let bytes = state
            .encode_snapshot_cooperative(snapshot_limits, async |bytes| {
                budget.charge(bytes).await;
            })
            .await?;
        Ok(self
            .build_checkpoint(id, canonical_state_schema_digest(), chunk_bytes, &bytes)
            .await?)
    }

    /// Validate and publish a canonical checkpoint as the selected recovery source.
    pub async fn install_canonical_checkpoint(
        &mut self,
        id: CheckpointId,
        state_limits: StateLimits,
        snapshot_limits: StateSnapshotLimits,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<(), CheckpointError> {
        let checkpoint = self.open_checkpoint(id, checkpoint_limits).await?;
        checkpoint
            .decode_canonical_state(state_limits, snapshot_limits, checkpoint_limits)
            .await?;
        Ok(self.select_checkpoint(checkpoint).await?)
    }

    /// Load and validate the exact canonical checkpoint selected by the manifest.
    pub async fn selected_canonical_checkpoint(
        &self,
        state_limits: StateLimits,
        snapshot_limits: StateSnapshotLimits,
        checkpoint_limits: CheckpointLimits,
    ) -> Result<Option<CanonicalState>, CheckpointError> {
        self.healthy()?;
        let Some(reference) = self.manifest.checkpoint else {
            return Ok(None);
        };
        let checkpoint = self
            .open_checkpoint(reference.checkpoint_id, checkpoint_limits)
            .await?;
        if checkpoint.digest != reference.manifest_digest
            || checkpoint.manifest.position != reference.position
        {
            return Err(CheckpointError::SelectionMismatch);
        }
        Ok(Some(
            checkpoint
                .decode_canonical_state(state_limits, snapshot_limits, checkpoint_limits)
                .await?,
        ))
    }
}
