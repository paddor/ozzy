//! Bounds and identity checks for checkpoint specs and manifests.

use super::{
    CHECKPOINT_HEADER_BYTES, CheckpointError, CheckpointLimits, CheckpointManifest, CheckpointSpec,
};
use crate::Digest;

pub(super) fn validate_spec(
    spec: CheckpointSpec,
    limits: CheckpointLimits,
) -> Result<(), CheckpointError> {
    validate_limits(limits)?;
    require_nonzero("group", spec.group_id.as_bytes())?;
    require_nonzero("store", spec.store_id.as_bytes())?;
    require_nonzero("checkpoint", spec.checkpoint_id.as_bytes())?;
    spec.position
        .validate()
        .map_err(|_| CheckpointError::InvalidPosition)?;
    if spec.position.op_number == 0
        || spec.configuration_epoch == 0
        || spec.source_manifest_generation == 0
        || spec.source_manifest_digest == Digest::ZERO
        || spec.state_schema_digest == Digest::ZERO
    {
        return Err(CheckpointError::InvalidSpec);
    }
    if spec.chunk_bytes == 0 || spec.chunk_bytes > limits.max_chunk_bytes {
        return Err(CheckpointError::InvalidChunkBytes);
    }
    Ok(())
}

pub(super) fn validate_manifest(
    manifest: &CheckpointManifest,
    limits: CheckpointLimits,
) -> Result<(), CheckpointError> {
    validate_spec(
        CheckpointSpec {
            group_id: manifest.group_id,
            store_id: manifest.store_id,
            checkpoint_id: manifest.checkpoint_id,
            position: manifest.position,
            configuration_epoch: manifest.configuration_epoch,
            source_manifest_generation: manifest.source_manifest_generation,
            source_manifest_digest: manifest.source_manifest_digest,
            state_schema_digest: manifest.state_schema_digest,
            chunk_bytes: manifest.chunk_bytes as usize,
        },
        limits,
    )?;
    enforce_u64(
        "checkpoint state bytes",
        manifest.state_bytes,
        limits.max_state_bytes,
    )?;
    enforce_usize(
        "checkpoint chunk count",
        manifest.chunks.len(),
        limits.max_chunks,
    )?;
    if manifest.state_bytes == 0
        || manifest.state_digest == Digest::ZERO
        || manifest.chunks.is_empty()
    {
        return Err(CheckpointError::EmptyState);
    }
    let mut next_offset = 0_u64;
    for (expected_ordinal, chunk) in manifest.chunks.iter().enumerate() {
        if chunk.ordinal as usize != expected_ordinal
            || chunk.logical_offset != next_offset
            || chunk.bytes == 0
            || chunk.bytes > u64::from(manifest.chunk_bytes)
            || chunk.digest == Digest::ZERO
        {
            return Err(CheckpointError::InvalidChunk);
        }
        if expected_ordinal + 1 < manifest.chunks.len()
            && chunk.bytes != u64::from(manifest.chunk_bytes)
        {
            return Err(CheckpointError::InvalidChunk);
        }
        next_offset = next_offset
            .checked_add(chunk.bytes)
            .ok_or(CheckpointError::LengthOverflow)?;
    }
    if next_offset != manifest.state_bytes {
        return Err(CheckpointError::LengthMismatch);
    }
    Ok(())
}

fn require_nonzero(kind: &'static str, bytes: &[u8]) -> Result<(), CheckpointError> {
    if bytes.iter().all(|byte| *byte == 0) {
        Err(CheckpointError::ZeroIdentity(kind))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_limits(limits: CheckpointLimits) -> Result<(), CheckpointError> {
    if limits.max_manifest_bytes < CHECKPOINT_HEADER_BYTES
        || limits.max_chunks == 0
        || limits.max_chunk_bytes == 0
        || limits.max_state_bytes == 0
    {
        Err(CheckpointError::InvalidLimits)
    } else {
        Ok(())
    }
}

pub(super) fn enforce_usize(
    kind: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), CheckpointError> {
    if actual > limit {
        Err(CheckpointError::LimitExceeded {
            kind,
            actual: actual as u64,
            limit: limit as u64,
        })
    } else {
        Ok(())
    }
}

pub(crate) fn enforce_u64(
    kind: &'static str,
    actual: u64,
    limit: u64,
) -> Result<(), CheckpointError> {
    if actual > limit {
        Err(CheckpointError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}
