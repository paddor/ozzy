use super::{
    CHECKPOINT_MANIFEST_FILE, Checkpoint, CheckpointError, CheckpointLimits, CheckpointManifest,
    checkpoint_manifest_digest, chunk_name,
};
use crate::async_files::Access;
use crate::checkpoint::{
    CheckpointSpec, checkpoint_name, encode_checkpoint_manifest, manifest_for_state,
};
use ozzy_io::{OpenMode, Operation};
use std::{
    io,
    path::{Path, PathBuf},
};

impl Checkpoint {
    /// The journal remains exclusively borrowed while this runs. An interrupted
    /// build fences it; its group lock survives every outstanding physical job.
    pub(crate) async fn build(
        access: Access,
        group_root: &Path,
        spec: CheckpointSpec,
        state: &[u8],
        limits: CheckpointLimits,
        chunk_bytes: usize,
        listing: (usize, usize),
    ) -> Result<Self, CheckpointError> {
        let manifest = manifest_for_state(spec, state, limits)?;
        let bytes = encode_checkpoint_manifest(&manifest, limits)?;
        let digest = checkpoint_manifest_digest(&bytes, limits)?;
        validate_listing(&manifest, listing)?;
        if chunk_bytes == 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let checkpoints = group_root.join("checkpoints");
        let staging = group_root.join("staging");
        let final_root = checkpoints.join(checkpoint_name(spec.checkpoint_id));
        match access.open_directory(final_root.clone()).await {
            Ok(handle) => {
                access.done(Operation::Close { handle }).await?;
                let existing = Self::open(
                    access.clone(),
                    final_root,
                    (spec.group_id, spec.store_id),
                    limits,
                    chunk_bytes,
                    listing,
                )
                .await?;
                if existing.manifest != manifest || existing.digest != digest {
                    return Err(CheckpointError::ImmutableConflict);
                }
                access.sync_directory(checkpoints).await?;
                access.sync_directory(staging).await?;
                return Ok(existing);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let temporary = create_staging(&access, &staging, spec).await?;
        for chunk in &manifest.chunks {
            let start = usize::try_from(chunk.logical_offset)
                .map_err(|_| CheckpointError::LengthOverflow)?;
            let count =
                usize::try_from(chunk.bytes).map_err(|_| CheckpointError::LengthOverflow)?;
            let end = start
                .checked_add(count)
                .ok_or(CheckpointError::LengthOverflow)?;
            write_new_synced(
                &access,
                temporary.join(chunk_name(chunk.ordinal)),
                &state[start..end],
                chunk_bytes,
            )
            .await?;
        }
        write_new_synced(
            &access,
            temporary.join(CHECKPOINT_MANIFEST_FILE),
            &bytes,
            chunk_bytes,
        )
        .await?;
        access.sync_directory(temporary.clone()).await?;
        access
            .done(Operation::Rename {
                source: temporary,
                destination: final_root.clone(),
            })
            .await?;
        access.sync_directory(checkpoints).await?;
        access.sync_directory(staging).await?;
        Self::open(
            access,
            final_root,
            (spec.group_id, spec.store_id),
            limits,
            chunk_bytes,
            listing,
        )
        .await
    }
}

fn validate_listing(
    manifest: &CheckpointManifest,
    listing: (usize, usize),
) -> Result<(), CheckpointError> {
    let entries = manifest
        .chunks
        .len()
        .checked_add(1)
        .ok_or(CheckpointError::LengthOverflow)?;
    // Every ordinal has a fixed-width eight-digit hexadecimal name.
    let names = manifest
        .chunks
        .len()
        .checked_mul(chunk_name(0).len())
        .and_then(|value| value.checked_add(CHECKPOINT_MANIFEST_FILE.len()))
        .ok_or(CheckpointError::LengthOverflow)?;
    if entries > listing.0 || names > listing.1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "checkpoint exceeds directory listing limits",
        )
        .into());
    }
    Ok(())
}

async fn create_staging(
    access: &Access,
    staging: &Path,
    spec: CheckpointSpec,
) -> Result<PathBuf, CheckpointError> {
    for sequence in 0..1024 {
        let path = staging.join(format!(
            ".checkpoint-{}-{}-{sequence}.tmp",
            checkpoint_name(spec.checkpoint_id),
            spec.source_manifest_generation
        ));
        match access
            .done(Operation::CreateDirectory { path: path.clone() })
            .await
        {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(CheckpointError::StagingConflict)
}

async fn write_new_synced(
    access: &Access,
    path: PathBuf,
    bytes: &[u8],
    chunk_bytes: usize,
) -> Result<(), CheckpointError> {
    let file = access.open(path, OpenMode::CreateNew, false, false).await?;
    let mut offset = 0;
    for chunk in bytes.chunks(chunk_bytes) {
        access.write_all(&file, offset, chunk).await?;
        offset += chunk.len() as u64;
    }
    access.sync(&file).await?;
    access.done(Operation::Close { handle: file }).await?;
    Ok(())
}
