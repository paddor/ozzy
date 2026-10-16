use super::{DirectoryError, Journal, LogPosition};
mod cursor;
mod inbox;
use crate::directory::{manifest_name, validate_checkpoint_source, validate_checkpoint_successor};
use crate::{
    CheckpointReference, CheckpointSpec, Digest, checkpoint::asynchronous::Checkpoint,
    checkpoint_name, decode_manifest, manifest_digest,
};
pub use cursor::CheckpointCursor;
pub use inbox::Inbox as CheckpointInbox;
pub use inbox::Stream as CheckpointInboxStream;
use ozzy_proto::CheckpointId;

/// Frozen checkpoint source with deletion protection. It borrows no live journal;
/// callers charge its bounded metadata and supply matching state fragments.
#[derive(Debug)]
#[must_use = "stream the captured state or drop the plan"]
pub struct CheckpointBuildPlan {
    prepared: crate::checkpoint::asynchronous::PreparedBuild,
    limits: crate::CheckpointLimits,
    lease: crate::retention::CheckpointLease,
}

impl CheckpointBuildPlan {
    /// Exact frozen identity, committed position, and source manifest lineage.
    pub const fn spec(&self) -> CheckpointSpec {
        self.prepared.spec()
    }

    /// Reserved metadata, excluding caller-owned state fragments and backend jobs.
    pub fn retained_bytes(&self) -> usize {
        self.prepared.retained_bytes() + std::mem::size_of::<Self>()
            - std::mem::size_of_val(&self.prepared)
    }

    /// Open a private staging target or verify an existing immutable artifact.
    pub async fn start(self) -> Result<CheckpointWriter, crate::CheckpointError> {
        let Self {
            prepared,
            limits,
            lease,
        } = self;
        Ok(CheckpointWriter {
            builder: prepared.start().await?,
            limits,
            lease,
        })
    }
}

/// Frozen checkpoint source with deletion protection, awaiting the encoded
/// state size. It borrows no live journal.
#[derive(Debug)]
#[must_use = "plan the encoded state or drop the source"]
pub struct CheckpointSource {
    config: crate::checkpoint::asynchronous::BuildConfig,
    lease: crate::retention::CheckpointLease,
}

impl CheckpointSource {
    /// Exact frozen identity, committed position, and source manifest lineage.
    pub const fn spec(&self) -> CheckpointSpec {
        self.config.spec
    }

    /// Bind the exact encoded state size and reserve bounded metadata.
    pub fn plan(self, state_bytes: u64) -> Result<CheckpointBuildPlan, crate::CheckpointError> {
        let limits = self.config.limits;
        Ok(CheckpointBuildPlan {
            prepared: crate::checkpoint::asynchronous::PreparedBuild::new(
                self.config,
                state_bytes,
            )?,
            limits,
            lease: self.lease,
        })
    }
}

/// Detached checkpoint writer with one open chunk and bounded metadata. File
/// effects run through the journal's backend; publication does not select it.
#[derive(Debug)]
#[must_use = "finish and explicitly install the checkpoint or drop the writer"]
pub struct CheckpointWriter {
    builder: Box<crate::checkpoint::asynchronous::Builder>,
    limits: crate::CheckpointLimits,
    lease: crate::retention::CheckpointLease,
}

impl CheckpointWriter {
    /// Metadata held between calls; backend jobs and caller fragments have their
    /// own admission charges. No complete state image is retained.
    pub fn retained_bytes(&self) -> usize {
        self.builder.retained_bytes() + std::mem::size_of::<Self>()
            - std::mem::size_of_val(&self.builder)
    }

    /// Remaining declared state bytes. Completion requires an exact match.
    pub fn remaining_bytes(&self) -> u64 {
        self.builder.remaining()
    }

    /// Maximum fragment bytes per call, independent of physical chunk size.
    pub const fn fragment_bytes(&self) -> usize {
        self.builder.fragment_bytes()
    }

    /// Append a nonempty bounded fragment. Failed or canceled physical work
    /// poisons this writer; the live journal can continue independently.
    pub async fn push(&mut self, bytes: &[u8]) -> Result<(), crate::CheckpointError> {
        self.builder.push(bytes).await
    }

    /// Verify complete bytes, synchronize files/namespace, and return an immutable
    /// protected artifact. The journal must explicitly install it afterward.
    pub async fn finish(self) -> Result<CheckpointFiles, crate::CheckpointError> {
        let Self {
            builder,
            limits,
            lease,
        } = self;
        Ok(CheckpointFiles {
            checkpoint: builder.finish().await?,
            limits,
            _lease: lease,
        })
    }
}

/// A verified immutable checkpoint with temporary deletion protection for both
/// its files and its source manifest. Capturing metadata copies no state bytes.
#[derive(Debug)]
pub struct CheckpointFiles {
    pub(super) checkpoint: Checkpoint,
    limits: crate::CheckpointLimits,
    _lease: crate::retention::CheckpointLease,
}

impl CheckpointFiles {
    /// Maximum transfer fragment size, independent of physical chunk size.
    pub const fn fragment_bytes(&self) -> usize {
        self.checkpoint.fragment_bytes()
    }

    /// Keep a file handle across detached sequential transfer reads. The cursor
    /// owns deletion protection; recipients still verify the complete image.
    pub fn cursor(self: std::rc::Rc<Self>) -> CheckpointCursor {
        CheckpointCursor::new(self)
    }

    /// Read sequential bounded fragments without materializing complete chunks.
    /// Reconstructed state stays private until `is_verified` becomes true.
    pub fn stream(&self) -> CheckpointStream<'_> {
        CheckpointStream {
            reader: crate::checkpoint::asynchronous::Reader::new(&self.checkpoint, self.limits),
        }
    }

    /// Read a bounded range from one immutable, checksum-validated chunk.
    /// Retain only returned bytes and one physical I/O fragment while checking
    /// the whole chunk; no large chunk backing survives a small result.
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

/// Sequential private recovery input. Borrowing its captured files preserves
/// deletion protection. A failed/canceled read poisons this stream.
#[derive(Debug)]
pub struct CheckpointStream<'a> {
    reader: crate::checkpoint::asynchronous::Reader<&'a Checkpoint>,
}

impl CheckpointStream<'_> {
    /// True only after all chunk digests and the complete state digest match.
    pub const fn is_verified(&self) -> bool {
        self.reader.verified()
    }

    /// Read at most the caller's byte budget and the backend transfer size.
    /// Prefix fragments alone supply no complete-image authority.
    pub async fn next(
        &mut self,
        maximum_bytes: usize,
    ) -> Result<Option<Vec<u8>>, crate::CheckpointError> {
        self.reader.next(maximum_bytes).await
    }
}

impl Journal {
    /// Create bounded, unselected file input for one original checkpoint image.
    /// A fresh receive identity distinguishes attempts. Reopen removes abandoned staging.
    pub async fn begin_checkpoint_receive(
        &self,
        id: CheckpointId,
        bytes: u64,
        expected_digest: Digest,
    ) -> Result<CheckpointInbox, DirectoryError> {
        self.healthy()?;
        Ok(CheckpointInbox::open(self, id, bytes, expected_digest).await?)
    }

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
        let plan = self.prepare_checkpoint(id, schema, chunk_bytes, state.len() as u64)?;
        self.interrupted = true;
        let mut writer = plan.start().await?;
        for fragment in state.chunks(writer.fragment_bytes()) {
            writer.push(fragment).await?;
        }
        let checkpoint = writer.finish().await?;
        self.interrupted = false;
        Ok(checkpoint)
    }

    /// Capture a published committed source for detached incremental encoding.
    /// State bytes must describe exactly this source. Cleanup holds both the
    /// checkpoint identity and source manifest until the plan/artifact drops.
    pub fn prepare_checkpoint(
        &self,
        id: CheckpointId,
        schema: Digest,
        chunk_bytes: usize,
        state_bytes: u64,
    ) -> Result<CheckpointBuildPlan, DirectoryError> {
        Ok(self
            .prepare_checkpoint_source(id, schema, chunk_bytes)?
            .plan(state_bytes)?)
    }

    /// Capture a published committed source before its state is encoded, so
    /// encoding can run without the journal. Cleanup holds the checkpoint
    /// identity and source manifest until the source or its artifact drops.
    pub fn prepare_checkpoint_source(
        &self,
        id: CheckpointId,
        schema: Digest,
        chunk_bytes: usize,
    ) -> Result<CheckpointSource, DirectoryError> {
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
        let config = crate::checkpoint::asynchronous::BuildConfig {
            access: self.access.clone(),
            root: self.root().to_owned(),
            spec,
            limits: self.limits.checkpoint,
            io_bytes: self.limits.io.chunk_bytes,
            listing: (
                self.limits.directory_entries,
                self.limits.directory_name_bytes,
            ),
        };
        let lease = self.pins.acquire_checkpoint_lease(
            id,
            spec.source_manifest_generation,
            self.group_lock()?,
        )?;
        Ok(CheckpointSource { config, lease })
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
            self.group_lock()?,
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
            self.limits.checkpoint_bounds(limits),
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
        let reference = self.checkpoint_selection(&checkpoint).await?;
        let mut next = self.next_manifest()?;
        next.checkpoint = Some(reference);
        self.install_selected(next).await?;
        self.checkpoint = Some(checkpoint);
        Ok(())
    }

    /// Validate a checkpoint as the next selection against its source
    /// manifest and the current lineage, without publishing it.
    pub(super) async fn checkpoint_selection(
        &self,
        checkpoint: &Checkpoint,
    ) -> Result<CheckpointReference, DirectoryError> {
        self.require_no_cleanup()?;
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
        Ok(CheckpointReference {
            checkpoint_id: id,
            position: checkpoint.manifest.position,
            manifest_digest: checkpoint.digest,
        })
    }
}
