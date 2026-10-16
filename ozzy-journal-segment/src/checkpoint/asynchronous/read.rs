use super::{
    CHECKPOINT_CHUNK_HASH_CONTEXT, CHECKPOINT_STATE_HASH_CONTEXT, Checkpoint, CheckpointChunk,
    CheckpointError, CheckpointLimits, Hasher, chunk_name, enforce_u64, validate_limits,
};
use ozzy_io::{Handle, OpenMode, Operation};
use std::{io, ops::Deref};

/// Bounded private decode input. Prefix bytes have no complete-image authority;
/// the caller publishes state only after the final chunk and state checks pass.
#[derive(Debug)]
pub(crate) struct Reader<C> {
    checkpoint: C,
    limits: CheckpointLimits,
    ordinal: usize,
    offset: u64,
    file: Option<Handle>,
    chunk_hash: Hasher,
    state_hash: Hasher,
    interrupted: bool,
    verified: bool,
}

impl<C: Deref<Target = Checkpoint>> Reader<C> {
    pub(crate) fn new(checkpoint: C, limits: CheckpointLimits) -> Self {
        Self {
            checkpoint,
            limits,
            ordinal: 0,
            offset: 0,
            file: None,
            chunk_hash: Hasher::new(CHECKPOINT_CHUNK_HASH_CONTEXT),
            state_hash: Hasher::new(CHECKPOINT_STATE_HASH_CONTEXT),
            interrupted: false,
            verified: false,
        }
    }

    pub(crate) const fn verified(&self) -> bool {
        self.verified
    }

    pub(crate) async fn next(
        &mut self,
        maximum: usize,
    ) -> Result<Option<Vec<u8>>, CheckpointError> {
        validate_limits(self.limits)?;
        enforce_u64(
            "checkpoint state bytes",
            self.checkpoint.manifest.state_bytes,
            self.limits.max_state_bytes,
        )?;
        if self.interrupted {
            return Err(
                io::Error::new(io::ErrorKind::BrokenPipe, "interrupted checkpoint stream").into(),
            );
        }
        if maximum == 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        if self.verified {
            return Ok(None);
        }
        self.interrupted = true;
        let bytes = self.next_inner(maximum).await?;
        self.interrupted = false;
        Ok(Some(bytes))
    }

    async fn next_inner(&mut self, maximum: usize) -> Result<Vec<u8>, CheckpointError> {
        let source = &*self.checkpoint;
        let chunk = *source
            .manifest
            .chunks
            .get(self.ordinal)
            .ok_or(CheckpointError::LengthMismatch)?;
        enforce_u64(
            "checkpoint chunk bytes",
            chunk.bytes,
            self.limits.max_chunk_bytes as u64,
        )?;
        // The open chunk lives here during a read and in `self.file` between
        // reads; a failed read drops it with the reader.
        let file = match self.file.take() {
            Some(file) => file,
            None => open_chunk(source, chunk).await?,
        };
        let count = maximum
            .min(source.chunk_bytes)
            .min((chunk.bytes - self.offset).min(usize::MAX as u64) as usize);
        let bytes = source
            .access
            .read_range(&file, self.offset, count, source.chunk_bytes)
            .await?;
        self.chunk_hash.update(&bytes);
        self.state_hash.update(&bytes);
        self.offset += bytes.len() as u64;
        if self.offset != chunk.bytes {
            self.file = Some(file);
            return Ok(bytes);
        }
        self.finish_chunk(file, chunk).await?;
        Ok(bytes)
    }

    /// Close a fully read chunk and check its length and digest; after the
    /// last chunk, check the state digest too.
    async fn finish_chunk(
        &mut self,
        file: Handle,
        chunk: CheckpointChunk,
    ) -> Result<(), CheckpointError> {
        let source = &*self.checkpoint;
        let length = source.access.length(&file).await;
        let closed = source.access.done(Operation::Close { handle: file }).await;
        if length? != chunk.bytes {
            return Err(CheckpointError::LengthMismatch);
        }
        closed?;
        if self.chunk_hash.finish() != chunk.digest {
            return Err(CheckpointError::DigestMismatch("checkpoint chunk"));
        }
        self.ordinal += 1;
        self.offset = 0;
        self.chunk_hash = Hasher::new(CHECKPOINT_CHUNK_HASH_CONTEXT);
        if self.ordinal == source.manifest.chunks.len() {
            if self.state_hash.finish() != source.manifest.state_digest {
                return Err(CheckpointError::DigestMismatch("checkpoint state"));
            }
            self.verified = true;
        }
        Ok(())
    }
}

/// Open one chunk file and check its length against the manifest.
async fn open_chunk(
    source: &Checkpoint,
    chunk: CheckpointChunk,
) -> Result<Handle, CheckpointError> {
    let file = source
        .access
        .open(
            source.root.join(chunk_name(chunk.ordinal)),
            OpenMode::Read,
            false,
            false,
        )
        .await?;
    if source.access.length(&file).await? != chunk.bytes {
        return Err(CheckpointError::LengthMismatch);
    }
    Ok(file)
}
