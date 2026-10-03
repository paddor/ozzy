//! Shared publication ordering. Real async I/O and legacy synchronous drivers
//! execute these same decisions and barriers.

use super::{DirectoryError, PersistenceObserver, PersistencePhase, io};

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
        let name = format!("MANIFEST.{}", next.generation);
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
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    if io.exists(name).await? {
        exact(io, name, bytes).await?;
        io.sync_directory().await?;
        observer.completed(PersistencePhase::ManifestDirectorySynced)?;
        return Ok(());
    }
    let temporary = format!(".{name}.tmp");
    prepare(io, &temporary, bytes).await?;
    observer.completed(PersistencePhase::ManifestTemporarySynced)?;
    match io.link(&temporary, name).await {
        Ok(()) => {}
        Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
            exact(io, name, bytes).await?;
        }
        Err(error) => return Err(error),
    }
    observer.completed(PersistencePhase::ManifestLinked)?;
    match io.remove(&temporary).await {
        Ok(()) => {}
        Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    io.sync_directory().await?;
    observer.completed(PersistencePhase::ManifestDirectorySynced)?;
    Ok(())
}

pub(crate) async fn replace_current(
    io: &mut impl Io,
    generation: u64,
    bytes: &[u8; super::super::CURRENT_BYTES],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    let temporary = format!(".CURRENT.{generation}.tmp");
    prepare(io, &temporary, bytes).await?;
    observer.completed(PersistencePhase::CurrentTemporarySynced)?;
    io.rename(&temporary, super::super::CURRENT_FILE).await?;
    observer.completed(PersistencePhase::CurrentRenamed)?;
    io.sync_directory().await?;
    observer.completed(PersistencePhase::CurrentDirectorySynced)?;
    Ok(())
}

/// Create the full `DURABLE` image at format or relocation.
pub(crate) async fn replace_evidence(
    io: &mut impl Io,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    replace_reusable(
        io,
        super::super::evidence::NAME,
        ".DURABLE.tmp",
        bytes,
        observer,
    )
    .await
}

/// Publish one `DURABLE` record into both copies, `first` before the other.
/// `first` never holds the newest intact record, so a failure at any point
/// leaves an intact copy with at least the previous completed publication.
pub(crate) async fn overwrite_evidence(
    io: &mut impl Overwrite,
    first: usize,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    // Each copy lies inside one sector, and a sector is written whole or not
    // at all. So one barrier cannot tear both copies of the last completed
    // publication, and afterwards both copies hold the new record.
    io.write_synced(
        super::super::evidence::NAME,
        &[
            first * super::super::evidence::COPY_STRIDE,
            (1 - first) * super::super::evidence::COPY_STRIDE,
        ],
        bytes,
    )
    .await?;
    observer.completed(PersistencePhase::EvidenceCopiesSynced)?;
    Ok(())
}

pub(crate) async fn replace_reusable(
    io: &mut impl Io,
    target: &str,
    temporary: &str,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    // Only this unselected temporary is reusable. The target is replaced whole.
    if io.exists(temporary).await? {
        io.remove(temporary).await?;
    }
    prepare(io, temporary, bytes).await?;
    observer.completed(PersistencePhase::CurrentTemporarySynced)?;
    io.rename(temporary, target).await?;
    observer.completed(PersistencePhase::CurrentRenamed)?;
    io.sync_directory().await?;
    observer.completed(PersistencePhase::CurrentDirectorySynced)?;
    Ok(())
}
