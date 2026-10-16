//! Checkpoint validation through owned backend jobs. Reuses the production
//! manifest codec, names and chunk/state digests. No filesystem calls here.

mod build;
mod read;
pub(crate) use build::{Builder, Config as BuildConfig, Prepared as PreparedBuild};
pub(crate) use read::Reader;

use super::{
    CHECKPOINT_CHUNK_HASH_CONTEXT, CHECKPOINT_MANIFEST_FILE, CHECKPOINT_STATE_HASH_CONTEXT,
    CheckpointChunk, CheckpointError, CheckpointLimits, CheckpointManifest, Hasher,
    checkpoint_manifest_digest, chunk_name, decode_checkpoint_manifest, enforce_u64,
    validate_checkpoint_names, validate_limits,
};
use crate::{
    Digest,
    async_files::{Access, Span},
};
use ozzy_io::{Handle, OpenMode, Operation};
use ozzy_proto::{GroupId, StoreId};
use std::{ops::Range, path::PathBuf};

/// Bounds for opening one checkpoint: its codec limits, the backend transfer
/// size, and the directory listing bounds (entries, name bytes).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Bounds {
    pub(crate) limits: CheckpointLimits,
    pub(crate) chunk_bytes: usize,
    pub(crate) listing: (usize, usize),
}

#[derive(Debug, Clone)]
pub(crate) struct Checkpoint {
    root: PathBuf,
    pub(crate) manifest: CheckpointManifest,
    pub(crate) digest: Digest,
    access: Access,
    chunk_bytes: usize,
}

impl Checkpoint {
    pub(crate) const fn fragment_bytes(&self) -> usize {
        self.chunk_bytes
    }

    pub(crate) async fn read_range(
        &self,
        offset: u64,
        maximum: usize,
        limits: CheckpointLimits,
    ) -> Result<Vec<u8>, CheckpointError> {
        if maximum == 0 || offset >= self.manifest.state_bytes {
            return Err(CheckpointError::LengthMismatch);
        }
        let index = self
            .manifest
            .chunks
            .partition_point(|chunk| chunk.logical_offset <= offset)
            .checked_sub(1)
            .ok_or(CheckpointError::LengthMismatch)?;
        let chunk = *self
            .manifest
            .chunks
            .get(index)
            .ok_or(CheckpointError::LengthMismatch)?;
        let length = usize::try_from(chunk.bytes).map_err(|_| CheckpointError::LengthOverflow)?;
        enforce_u64(
            "checkpoint chunk bytes",
            chunk.bytes,
            limits.max_chunk_bytes as u64,
        )?;
        let start = usize::try_from(offset - chunk.logical_offset)
            .map_err(|_| CheckpointError::LengthOverflow)?;
        let count = maximum.min(
            length
                .checked_sub(start)
                .ok_or(CheckpointError::LengthMismatch)?,
        );
        let file = self
            .access
            .open(
                self.root.join(chunk_name(chunk.ordinal)),
                OpenMode::Read,
                false,
                false,
            )
            .await?;
        let result = self.read_verified(&file, chunk, start..start + count).await;
        let closed = self.access.done(Operation::Close { handle: file }).await;
        let bytes = result?;
        closed?;
        Ok(bytes)
    }

    /// Read and hash one whole chunk file, keeping only `wanted` bytes of it.
    /// The file length is checked before and after; the digest must match.
    async fn read_verified(
        &self,
        file: &Handle,
        chunk: CheckpointChunk,
        wanted: Range<usize>,
    ) -> Result<Vec<u8>, CheckpointError> {
        let length = usize::try_from(chunk.bytes).map_err(|_| CheckpointError::LengthOverflow)?;
        if self.access.length(file).await? != chunk.bytes {
            return Err(CheckpointError::LengthMismatch);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(wanted.len())
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        let mut scratch = Vec::new();
        let mut hasher = Hasher::new(CHECKPOINT_CHUNK_HASH_CONTEXT);
        let mut through = 0;
        while through < length {
            scratch.clear();
            let span = Span {
                offset: through as u64,
                length: self.chunk_bytes.min(length - through),
            };
            self.access
                .read_append(file, span, self.chunk_bytes, &mut scratch)
                .await?;
            hasher.update(&scratch);
            let from = wanted.start.max(through);
            let end = wanted.end.min(through + scratch.len());
            if from < end
                && let Some(part) = scratch.get(from - through..end - through)
            {
                bytes.extend_from_slice(part);
            }
            through += scratch.len();
        }
        if self.access.length(file).await? != chunk.bytes {
            return Err(CheckpointError::LengthMismatch);
        }
        if hasher.finish() != chunk.digest {
            return Err(CheckpointError::DigestMismatch("checkpoint chunk"));
        }
        Ok(bytes)
    }

    pub(crate) async fn open(
        access: Access,
        root: PathBuf,
        identity: (GroupId, StoreId),
        bounds: Bounds,
    ) -> Result<Self, CheckpointError> {
        let Bounds {
            limits,
            chunk_bytes,
            listing,
        } = bounds;
        validate_limits(limits)?;
        if chunk_bytes == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput).into());
        }
        let directory = access.open_directory(root.clone()).await?;
        access.done(Operation::Close { handle: directory }).await?;
        let bytes = access
            .read_file(
                root.join(CHECKPOINT_MANIFEST_FILE),
                limits.max_manifest_bytes,
                chunk_bytes,
            )
            .await?;
        let digest = checkpoint_manifest_digest(&bytes, limits)?;
        let manifest = decode_checkpoint_manifest(&bytes, limits)?;
        if manifest.group_id != identity.0 || manifest.store_id != identity.1 {
            return Err(CheckpointError::IdentityMismatch);
        }
        let entries = access.list(root.clone(), listing.0, listing.1).await?;
        validate_checkpoint_names(entries.into_iter().map(|entry| Ok(entry.name)), &manifest)?;
        let checkpoint = Self {
            root,
            manifest,
            digest,
            access,
            chunk_bytes,
        };
        checkpoint.validate_chunks(limits, None).await?;
        Ok(checkpoint)
    }

    async fn validate_chunks(
        &self,
        limits: CheckpointLimits,
        mut state: Option<&mut Vec<u8>>,
    ) -> Result<(), CheckpointError> {
        let mut reader = Reader::new(self, limits);
        while let Some(bytes) = reader.next(self.chunk_bytes).await? {
            if let Some(state) = &mut state {
                state.extend_from_slice(&bytes);
            }
        }
        Ok(())
    }

    /// Called while the journal is borrowed, so retention cannot retire this
    /// selected checkpoint during materialization. Detached readers need their
    /// own deletion protection, not just a cloned path.
    pub(crate) async fn read_state(
        &self,
        limits: CheckpointLimits,
    ) -> Result<Vec<u8>, CheckpointError> {
        validate_limits(limits)?;
        enforce_u64(
            "checkpoint state bytes",
            self.manifest.state_bytes,
            limits.max_state_bytes,
        )?;
        let capacity = usize::try_from(self.manifest.state_bytes)
            .map_err(|_| CheckpointError::LengthOverflow)?;
        let mut state = Vec::new();
        state
            .try_reserve_exact(capacity)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
        self.validate_chunks(limits, Some(&mut state)).await?;
        Ok(state)
    }
}
