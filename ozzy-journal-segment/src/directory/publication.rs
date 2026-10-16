//! Crash-safe metadata publication ordering over one directory I/O driver.

use std::io;

use super::{CURRENT_FILE, DirectoryError, evidence, manifest_name};

pub(crate) trait Overwrite {
    async fn write_synced(
        &mut self,
        name: &str,
        offsets: &[usize],
        bytes: &[u8],
    ) -> Result<(), DirectoryError>;
}

pub(crate) trait Io: Overwrite {
    async fn exists(&mut self, name: &str) -> Result<bool, DirectoryError>;
    async fn read_exact(&mut self, name: &str, bytes: usize) -> Result<Vec<u8>, DirectoryError>;
    async fn write_new(&mut self, name: &str, bytes: &[u8]) -> Result<(), DirectoryError>;
    async fn sync_file(&mut self, name: &str) -> Result<(), DirectoryError>;
    async fn link(&mut self, source: &str, target: &str) -> Result<(), DirectoryError>;
    async fn remove(&mut self, name: &str) -> Result<(), DirectoryError>;
    async fn rename(&mut self, source: &str, target: &str) -> Result<(), DirectoryError>;
    async fn sync_directory(&mut self) -> Result<(), DirectoryError>;
}

async fn exact(io: &mut impl Io, name: &str, bytes: &[u8]) -> Result<(), DirectoryError> {
    if io.read_exact(name, bytes.len()).await? == bytes {
        Ok(())
    } else {
        Err(DirectoryError::ImmutableConflict)
    }
}

/// Select a free immutable generation after an interrupted publication.
pub(crate) async fn prepare_manifest(
    io: &mut impl Io,
    previous: &crate::Manifest,
    mut next: crate::Manifest,
    limits: crate::MetadataLimits,
) -> Result<(crate::Manifest, Vec<u8>, crate::Digest), DirectoryError> {
    if next.identity != previous.identity
        || next.generation
            != previous
                .generation
                .checked_add(1)
                .ok_or(DirectoryError::ManifestGeneration)?
        || next.parent_generation != previous.generation
    {
        return Err(DirectoryError::ManifestGeneration);
    }
    let bytes = loop {
        let bytes = crate::encode_manifest_with_limits(&next, limits)?;
        let name = manifest_name(next.generation);
        let occupied = if io.exists(&name).await? {
            name
        } else {
            let temporary = format!(".{name}.tmp");
            if !io.exists(&temporary).await? {
                break bytes;
            }
            temporary
        };
        match exact(io, &occupied, &bytes).await {
            Ok(()) => break bytes,
            Err(DirectoryError::ImmutableConflict | DirectoryError::WrongFileSize { .. }) => {}
            Err(error) => return Err(error),
        }
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(DirectoryError::ManifestGeneration)?;
    };
    let digest = crate::manifest_digest(&bytes, limits)?;
    Ok((next, bytes, digest))
}

/// Write and synchronize an unselected temporary. An interrupted attempt can
/// leave its name with no, some, or other bytes. Nothing refers to that name,
/// so it is removed and created again. The old file is never written: an
/// interrupted link may share it with an installed target.
async fn prepare(io: &mut impl Io, name: &str, bytes: &[u8]) -> Result<(), DirectoryError> {
    match io.write_new(name, bytes).await {
        Ok(()) => {}
        Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
            match exact(io, name, bytes).await {
                Ok(()) => {}
                Err(DirectoryError::ImmutableConflict | DirectoryError::WrongFileSize { .. }) => {
                    io.remove(name).await?;
                    io.write_new(name, bytes).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    }
    io.sync_file(name).await
}

pub(crate) async fn install_immutable(
    io: &mut impl Io,
    name: &str,
    bytes: &[u8],
) -> Result<(), DirectoryError> {
    if io.exists(name).await? {
        exact(io, name, bytes).await?;
        io.sync_directory().await?;
        return Ok(());
    }
    let temporary = format!(".{name}.tmp");
    prepare(io, &temporary, bytes).await?;
    match io.link(&temporary, name).await {
        Ok(()) => {}
        Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
            exact(io, name, bytes).await?;
        }
        Err(error) => return Err(error),
    }
    match io.remove(&temporary).await {
        Ok(()) => {}
        Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    io.sync_directory().await?;
    Ok(())
}

pub(crate) async fn replace_current(
    io: &mut impl Io,
    generation: u64,
    bytes: &[u8; crate::CURRENT_BYTES],
) -> Result<(), DirectoryError> {
    let temporary = format!(".CURRENT.{generation}.tmp");
    prepare(io, &temporary, bytes).await?;
    io.rename(&temporary, CURRENT_FILE).await?;
    io.sync_directory().await?;
    Ok(())
}

/// Publish one `DURABLE` record into both copies, `first` before the other.
/// `first` never holds the newest intact record, so a failure at any point
/// leaves an intact copy with at least the previous completed publication.
pub(crate) async fn overwrite_evidence(
    io: &mut impl Overwrite,
    first: usize,
    bytes: &[u8],
) -> Result<(), DirectoryError> {
    // Each copy lies inside one sector, and a sector is written whole or not
    // at all. So one barrier cannot tear both copies of the last completed
    // publication, and afterwards both copies hold the new record.
    io.write_synced(
        evidence::NAME,
        &[
            first * evidence::COPY_STRIDE,
            (1 - first) * evidence::COPY_STRIDE,
        ],
        bytes,
    )
    .await?;
    Ok(())
}

pub(crate) async fn replace_reusable(
    io: &mut impl Io,
    target: &str,
    temporary: &str,
    bytes: &[u8],
) -> Result<(), DirectoryError> {
    // Only this unselected temporary is reusable. The target is replaced whole.
    if io.exists(temporary).await? {
        io.remove(temporary).await?;
    }
    prepare(io, temporary, bytes).await?;
    io.rename(temporary, target).await?;
    io.sync_directory().await?;
    Ok(())
}
