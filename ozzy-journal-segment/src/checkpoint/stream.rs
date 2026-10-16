use super::{
    CHECKPOINT_CHUNK_HASH_CONTEXT, CHECKPOINT_STATE_HASH_CONTEXT, CheckpointError, CheckpointImage,
    CheckpointLimits, File, Hasher, Read, chunk_name, enforce_u64, require_regular_file,
    validate_limits,
};

impl CheckpointImage {
    /// Keep callback results private until the complete image has been checked.
    pub(crate) fn visit_state<E: From<CheckpointError>>(
        &self,
        limits: CheckpointLimits,
        mut consume: impl FnMut(&[u8]) -> Result<(), E>,
    ) -> Result<(), E> {
        validate_limits(limits)?;
        enforce_u64(
            "checkpoint state bytes",
            self.manifest.state_bytes,
            limits.max_state_bytes,
        )?;
        let mut buffer = vec![0; limits.max_chunk_bytes.min(64 * 1024)];
        let mut state_hash = Hasher::new(CHECKPOINT_STATE_HASH_CONTEXT);
        for chunk in &self.manifest.chunks {
            let path = self.root.join(chunk_name(chunk.ordinal));
            require_regular_file(&path, "checkpoint chunk")?;
            enforce_u64(
                "checkpoint chunk bytes",
                chunk.bytes,
                limits.max_chunk_bytes as u64,
            )?;
            let mut file = File::open(path).map_err(CheckpointError::from)?;
            if file.metadata().map_err(CheckpointError::from)?.len() != chunk.bytes {
                return Err(CheckpointError::LengthMismatch.into());
            }
            let mut remaining = chunk.bytes;
            let mut chunk_hash = Hasher::new(CHECKPOINT_CHUNK_HASH_CONTEXT);
            while remaining != 0 {
                let count = remaining.min(buffer.len() as u64) as usize;
                let bytes = &mut buffer[..count];
                file.read_exact(bytes).map_err(CheckpointError::from)?;
                chunk_hash.update(bytes);
                state_hash.update(bytes);
                consume(bytes)?;
                remaining -= count as u64;
            }
            if chunk_hash.finish() != chunk.digest {
                return Err(CheckpointError::DigestMismatch("checkpoint chunk").into());
            }
        }
        if state_hash.finish() != self.manifest.state_digest {
            return Err(CheckpointError::DigestMismatch("checkpoint state").into());
        }
        Ok(())
    }
}
