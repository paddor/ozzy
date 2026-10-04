use super::{DirectoryError, Journal, LogPosition};
use crate::directory::{manifest_name, validate_checkpoint_source, validate_checkpoint_successor};
use crate::{
    CheckpointReference, CheckpointSpec, Digest, checkpoint::asynchronous::Checkpoint,
    checkpoint_name, decode_manifest, manifest_digest,
};
use ozzy_proto::CheckpointId;

/// A verified immutable checkpoint with temporary deletion protection for both
/// its files and its source manifest. Capturing metadata copies no state bytes.
#[derive(Debug)]
pub struct CheckpointFiles {
    checkpoint: Checkpoint,
    limits: crate::CheckpointLimits,
    _lease: crate::retention::CheckpointLease<ozzy_io::Handle>,
}

impl CheckpointFiles {
    /// Read a bounded range from one immutable, checksum-validated chunk.
    pub async fn read_range(
        &self,
        offset: u64,
        maximum: usize,
    ) -> Result<Vec<u8>, crate::CheckpointError> {
        self.checkpoint
            .read_range(offset, maximum, self.limits)
            .await
    }

    /// Exact validated metadata manifest held by this object.
    pub const fn manifest(&self) -> &crate::CheckpointManifest {
        &self.checkpoint.manifest
    }
    /// Exact checkpoint identity, position, and manifest digest.
    pub const fn reference(&self) -> CheckpointReference {
        CheckpointReference {
            checkpoint_id: self.checkpoint.manifest.checkpoint_id,
            position: self.checkpoint.manifest.position,
            manifest_digest: self.checkpoint.digest,
        }
    }
    /// Read and validate the captured checkpoint state bytes.
    pub async fn read_state(&self) -> Result<Vec<u8>, crate::CheckpointError> {
        self.checkpoint.read_state(self.limits).await
    }

    /// Validate and reconstruct canonical state from the captured checkpoint.
    pub async fn decode_canonical_state(
        &self,
        state: ozzy_core::state::StateLimits,
        snapshot: ozzy_core::state::StateSnapshotLimits,
    ) -> Result<ozzy_core::state::CanonicalState, crate::CanonicalCheckpointError> {
        self.checkpoint
            .decode_canonical_state(state, snapshot, self.limits)
            .await
    }
}

impl Journal {
    /// Build an immutable checkpoint for the exact published committed state.
    /// This does not select it. Caller supplies state corresponding to that
    /// position, as with the existing checkpoint contract. Keeping this journal
    /// borrowed prevents concurrent source cleanup; canceled writes fence it.
    pub async fn build_checkpoint(
        &mut self,
        id: CheckpointId,
        schema: Digest,
        chunk_bytes: usize,
        state: &[u8],
    ) -> Result<CheckpointFiles, DirectoryError> {
        self.healthy()?;
        let position = self.committed_position()?;
        if position == LogPosition::GENESIS {
            return Err(DirectoryError::CheckpointAtGenesis);
        }
        if position != self.manifest.committed {
            return Err(DirectoryError::LocalProgressUnpublished);
        }
        let spec = CheckpointSpec {
            group_id: self.manifest.identity.group_id,
            store_id: self.manifest.identity.store_id,
            checkpoint_id: id,
            position,
            configuration_epoch: self.manifest.configuration_epoch,
            source_manifest_generation: self.manifest.generation,
            source_manifest_digest: self.current.manifest_digest,
            state_schema_digest: schema,
            chunk_bytes,
        };
        self.interrupted = true;
        let checkpoint = Checkpoint::build(
            self.access.clone(),
            self.root(),
            spec,
            state,
            self.limits.checkpoint,
            self.limits.io.chunk_bytes,
            (
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            ),
        )
        .await?;
        self.interrupted = false;
        self.protect_checkpoint(checkpoint)
    }

    /// Capture the selected checkpoint for a detached reader. Retention cannot
    /// delete this checkpoint or its source metadata until the capture drops.
    pub fn capture_checkpoint(&self) -> Result<CheckpointFiles, DirectoryError> {
        self.healthy()?;
        let checkpoint = self
            .checkpoint
            .as_ref()
            .ok_or(DirectoryError::RetentionRequiresCheckpoint)?;
        self.protect_checkpoint(checkpoint.clone())
    }

    fn protect_checkpoint(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<CheckpointFiles, DirectoryError> {
        let lease = self.pins.acquire_checkpoint_lease(
            checkpoint.manifest.checkpoint_id,
            checkpoint.manifest.source_manifest_generation,
            self.access
                .protection
                .clone()
                .expect("journal retains group ownership"),
        )?;
        Ok(CheckpointFiles {
            checkpoint,
            limits: self.limits.checkpoint,
            _lease: lease,
        })
    }

    /// Validate an immutable artifact and its source lineage, then select it
    /// through a successor manifest. Confirmation position never changes here.
    pub async fn install_checkpoint(&mut self, id: CheckpointId) -> Result<(), DirectoryError> {
        self.healthy()?;
        let checkpoint = self.open_checkpoint(id, self.limits.checkpoint).await?;
        self.select_checkpoint(checkpoint).await
    }

    pub(super) async fn open_checkpoint(
        &self,
        id: CheckpointId,
        limits: crate::CheckpointLimits,
    ) -> Result<Checkpoint, DirectoryError> {
        self.healthy()?;
        let checkpoint = Checkpoint::open(
            self.access.clone(),
            self.root().join("checkpoints").join(checkpoint_name(id)),
            (
                self.manifest.identity.group_id,
                self.manifest.identity.store_id,
            ),
            limits,
            self.limits.io.chunk_bytes,
            (
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            ),
        )
        .await?;
        if checkpoint.manifest.checkpoint_id != id {
            return Err(DirectoryError::CheckpointMismatch);
        }
        Ok(checkpoint)
    }

    pub(super) async fn select_checkpoint(
        &mut self,
        checkpoint: Checkpoint,
    ) -> Result<(), DirectoryError> {
        let id = checkpoint.manifest.checkpoint_id;
        validate_checkpoint_successor(&self.manifest, &checkpoint.manifest)?;
        let bytes = self
            .access
            .read_file(
                self.root().join(manifest_name(
                    checkpoint.manifest.source_manifest_generation,
                )),
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
        self.validate_positions([checkpoint.manifest.position; 2])
            .await?;
        let mut next = self.next_manifest()?;
        next.checkpoint = Some(CheckpointReference {
            checkpoint_id: id,
            position: checkpoint.manifest.position,
            manifest_digest: checkpoint.digest,
        });
        self.install_selected(next).await?;
        self.checkpoint = Some(checkpoint);
        Ok(())
    }
}
