use super::{
    CHECKPOINT_CHUNK_ENTRY_BYTES, CHECKPOINT_CHUNK_HASH_CONTEXT, CHECKPOINT_HEADER_BYTES,
    CHECKPOINT_STATE_HASH_CONTEXT, CheckpointChunk, CheckpointError, CheckpointLimits,
    CheckpointManifest, CheckpointSpec, Digest, Hasher, enforce_u64, enforce_usize,
    validate_manifest, validate_spec,
};

/// The same manifest codec for materialized and streamed state. Only bounded
/// chunk descriptors and hash state are retained; no checkpoint payload is held.
#[derive(Debug)]
pub(crate) struct ManifestBuilder {
    manifest: CheckpointManifest,
    limits: CheckpointLimits,
    state_hash: Hasher,
    chunk_hash: Hasher,
    written: u64,
    chunk_written: usize,
}

impl ManifestBuilder {
    pub(crate) fn new(
        spec: CheckpointSpec,
        state_bytes: u64,
        limits: CheckpointLimits,
    ) -> Result<Self, CheckpointError> {
        validate_spec(spec, limits)?;
        enforce_u64(
            "checkpoint state bytes",
            state_bytes,
            limits.max_state_bytes,
        )?;
        if state_bytes == 0 {
            return Err(CheckpointError::EmptyState);
        }
        let count = usize::try_from(state_bytes.div_ceil(spec.chunk_bytes as u64))
            .map_err(|_| CheckpointError::LengthOverflow)?;
        u32::try_from(count).map_err(|_| CheckpointError::LengthOverflow)?;
        enforce_usize("checkpoint chunk count", count, limits.max_chunks)?;
        let manifest_bytes = count
            .checked_mul(CHECKPOINT_CHUNK_ENTRY_BYTES)
            .and_then(|bytes| bytes.checked_add(CHECKPOINT_HEADER_BYTES))
            .ok_or(CheckpointError::LengthOverflow)?;
        enforce_usize(
            "checkpoint manifest bytes",
            manifest_bytes,
            limits.max_manifest_bytes,
        )?;
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(count)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        Ok(Self {
            manifest: CheckpointManifest {
                group_id: spec.group_id,
                store_id: spec.store_id,
                checkpoint_id: spec.checkpoint_id,
                position: spec.position,
                configuration_epoch: spec.configuration_epoch,
                source_manifest_generation: spec.source_manifest_generation,
                source_manifest_digest: spec.source_manifest_digest,
                state_schema_digest: spec.state_schema_digest,
                state_bytes,
                chunk_bytes: u32::try_from(spec.chunk_bytes)
                    .map_err(|_| CheckpointError::LengthOverflow)?,
                state_digest: Digest::ZERO,
                chunks,
            },
            limits,
            state_hash: Hasher::new(CHECKPOINT_STATE_HASH_CONTEXT),
            chunk_hash: Hasher::new(CHECKPOINT_CHUNK_HASH_CONTEXT),
            written: 0,
            chunk_written: 0,
        })
    }

    pub(crate) fn remaining(&self) -> u64 {
        self.manifest.state_bytes - self.written
    }

    pub(crate) fn chunk_remaining(&self) -> usize {
        (self.manifest.chunk_bytes as usize - self.chunk_written)
            .min(self.remaining().min(usize::MAX as u64) as usize)
    }

    pub(crate) const fn chunk_written(&self) -> usize {
        self.chunk_written
    }

    pub(crate) fn ordinal(&self) -> usize {
        self.manifest.chunks.len()
    }

    pub(crate) fn chunk_count(&self) -> usize {
        self.manifest
            .state_bytes
            .div_ceil(u64::from(self.manifest.chunk_bytes)) as usize
    }

    pub(crate) fn descriptor_bytes(&self) -> usize {
        self.manifest.chunks.capacity() * std::mem::size_of::<CheckpointChunk>()
    }

    /// Add at most one chunk remainder after its physical write succeeds.
    pub(crate) fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<Option<CheckpointChunk>, CheckpointError> {
        if bytes.is_empty() || bytes.len() > self.chunk_remaining() {
            return Err(CheckpointError::LengthMismatch);
        }
        self.state_hash.update(bytes);
        self.chunk_hash.update(bytes);
        self.written += bytes.len() as u64;
        self.chunk_written += bytes.len();
        if self.chunk_written != self.manifest.chunk_bytes as usize && self.remaining() != 0 {
            return Ok(None);
        }
        let chunk = CheckpointChunk {
            ordinal: u32::try_from(self.manifest.chunks.len())
                .map_err(|_| CheckpointError::LengthOverflow)?,
            logical_offset: self.written - self.chunk_written as u64,
            bytes: self.chunk_written as u64,
            digest: self.chunk_hash.finish(),
        };
        self.manifest.chunks.push(chunk);
        self.chunk_written = 0;
        self.chunk_hash = Hasher::new(CHECKPOINT_CHUNK_HASH_CONTEXT);
        Ok(Some(chunk))
    }

    pub(crate) fn finish(mut self) -> Result<CheckpointManifest, CheckpointError> {
        if self.remaining() != 0 {
            return Err(CheckpointError::LengthMismatch);
        }
        self.manifest.state_digest = self.state_hash.finish();
        validate_manifest(&self.manifest, self.limits)?;
        Ok(self.manifest)
    }
}
