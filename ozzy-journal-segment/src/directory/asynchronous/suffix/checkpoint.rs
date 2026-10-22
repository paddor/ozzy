//! Checkpoint import: imported state published with the staged suffix.

use super::{Error, Installer, Journal};
use crate::{CanonicalOperation, LogPosition};

/// Privately decoded state to publish under destination-local store identity.
#[derive(Debug)]
pub struct CanonicalCheckpointImport<'a> {
    /// Fresh destination-local checkpoint artifact identity.
    pub id: ozzy_proto::CheckpointId,
    /// Exact original history prefix, independently checked against retained bytes.
    pub position: LogPosition,
    /// Complete original image, including its independent identity section.
    pub image: &'a super::super::CheckpointInbox,
    /// Live canonical metadata bounds for private validation.
    pub state_limits: ozzy_core::state::StateLimits,
    /// Independent byte and name bounds for the core snapshot section.
    pub snapshot_limits: ozzy_core::state::StateSnapshotLimits,
}

impl Installer {
    /// Publish checkpoint state plus required retained operations in one final
    /// selection. Its source metadata and chunk files remain unselected until
    /// complete. Dropping this future leaves the original nonvoting store selected.
    pub async fn finish_with_checkpoint(
        self,
        import: CanonicalCheckpointImport<'_>,
    ) -> Result<Journal, Error> {
        if !self.checkpoint_seen || self.checkpoint_position != Some(import.position) {
            return Err(Error::IncompleteSuffix);
        }
        import
            .image
            .require_owner(self.journal.root(), self.journal.manifest.identity)?;
        let mut budget = crate::cooperative::Budget::default();
        let mut decoder = ozzy_core::checkpoint::Decoder::new(
            import.image.state_bytes(),
            import.snapshot_limits.max_snapshot_bytes,
        );
        let mut reader = import.image.stream()?;
        while let Some(bytes) = reader.next(64 * 1024).await? {
            decoder.push(&bytes, |_| Ok(()), |_, _| Ok(()))?;
            budget.charge(bytes.len()).await;
        }
        if !reader.is_verified() {
            return Err(Error::IncompleteSuffix);
        }
        let (layout, bytes) = decoder.finish()?;
        let state = ozzy_core::state::CanonicalState::decode_snapshot_cooperative(
            &bytes,
            import.state_limits,
            import.snapshot_limits,
            async |bytes| budget.charge(bytes).await,
        )
        .await?;
        layout.validate_state(&state)?;
        if state.revision() != import.position.op_number {
            return Err(Error::IncompleteSuffix);
        }
        drop(state);
        drop(bytes);
        Box::pin(self.finish_selection(Some(import))).await
    }

    pub(super) async fn publish_checkpoint_source(
        &mut self,
        mut source: crate::Manifest,
        import: CanonicalCheckpointImport<'_>,
    ) -> Result<crate::Manifest, Error> {
        source.committed = import.position;
        source.checkpoint = None;
        self.journal.interrupted = true;
        let (source, digest) = self
            .journal
            .directory
            .publish_manifest(&self.journal.manifest, source, self.journal.limits.metadata)
            .await?;
        let spec = crate::CheckpointSpec {
            group_id: source.identity.group_id,
            store_id: source.identity.store_id,
            checkpoint_id: import.id,
            position: import.position,
            configuration_epoch: source.configuration_epoch,
            source_manifest_generation: source.generation,
            source_manifest_digest: digest,
            state_schema_digest: ozzy_core::state::canonical_state_schema_digest(),
            chunk_bytes: self.journal.limits.checkpoint.max_chunk_bytes,
        };
        let prepared = crate::checkpoint::asynchronous::PreparedBuild::new(
            crate::checkpoint::asynchronous::BuildConfig {
                access: self.journal.access.clone(),
                root: self.journal.root().to_owned(),
                spec,
                limits: self.journal.limits.checkpoint,
                io_bytes: self.journal.limits.io.chunk_bytes,
                listing: (
                    self.journal.limits.directory_entries,
                    self.journal.limits.directory_name_bytes,
                ),
            },
            import.image.state_bytes(),
        )?;
        let mut builder = prepared.start().await?;
        let mut reader = import.image.stream()?;
        while let Some(bytes) = reader.next(builder.fragment_bytes()).await? {
            builder.push(&bytes).await?;
        }
        if !reader.is_verified() {
            return Err(Error::IncompleteSuffix);
        }
        let checkpoint = builder.finish().await?;
        if checkpoint.manifest.state_digest != import.image.digest() {
            return Err(Error::IncompleteSuffix);
        }
        self.journal.manifest = source;
        let mut selected = self.journal.next_manifest()?;
        selected.committed = self.request.replacement.committed;
        selected.checkpoint = Some(crate::CheckpointReference {
            checkpoint_id: import.id,
            position: import.position,
            manifest_digest: checkpoint.digest,
        });
        self.journal.checkpoint = Some(checkpoint);
        Ok(selected)
    }
}

/// Whether `operations` hold the checkpoint `position`. The operation there
/// must be the one the checkpoint represents.
pub(super) fn holds_checkpoint(
    position: Option<LogPosition>,
    operations: &[CanonicalOperation<'_>],
) -> Result<bool, Error> {
    let Some(position) = position else {
        return Ok(false);
    };
    let Some(operation) = operations
        .iter()
        .find(|operation| operation.op_number == position.op_number)
    else {
        return Ok(false);
    };
    if ozzy_journal::operation::logical_operation_digest(operation) != position.digest {
        return Err(Error::CommitNotSelected);
    }
    Ok(true)
}
