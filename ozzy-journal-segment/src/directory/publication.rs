//! One publication algorithm over real or simulated file/directory operations.

use super::{DirectoryError, PersistenceObserver, PersistencePhase, io};
use std::io::Write;
use std::path::Path;

/// `DURABLE` opened once by the journal. Recovery checked its type and size.
pub(crate) struct OpenEvidence<'a>(pub &'a std::fs::File);

impl SyncedOverwrite for OpenEvidence<'_> {
    fn write_synced(
        &mut self,
        name: &str,
        offsets: &[usize],
        bytes: &[u8],
    ) -> Result<(), DirectoryError> {
        use std::os::unix::fs::FileExt;
        debug_assert_eq!(name, super::evidence::NAME);
        for offset in offsets {
            self.0.write_all_at(bytes, *offset as u64)?;
        }
        self.0.sync_data()?;
        Ok(())
    }
}

/// The one operation a `DURABLE` publication needs.
pub(crate) trait SyncedOverwrite {
    /// Overwrite existing bytes in place at every offset and return once all
    /// copies are durable. One barrier covers every copy. The file keeps its
    /// size, so no metadata or directory sync is needed.
    fn write_synced(
        &mut self,
        name: &str,
        offsets: &[usize],
        bytes: &[u8],
    ) -> Result<(), DirectoryError>;
}

pub(crate) trait MetadataIo: SyncedOverwrite {
    fn exists(&mut self, name: &str) -> Result<bool, DirectoryError>;
    fn read_exact(&mut self, name: &str, bytes: usize) -> Result<Vec<u8>, DirectoryError>;
    fn write_new(&mut self, name: &str, bytes: &[u8]) -> Result<(), DirectoryError>;
    fn sync_file(&mut self, name: &str) -> Result<(), DirectoryError>;
    fn link(&mut self, source: &str, target: &str) -> Result<(), DirectoryError>;
    fn remove(&mut self, name: &str) -> Result<(), DirectoryError>;
    fn rename(&mut self, source: &str, target: &str) -> Result<(), DirectoryError>;
    fn sync_directory(&mut self) -> Result<(), DirectoryError>;
}

pub(crate) struct Filesystem<'a>(pub &'a Path);

impl MetadataIo for Filesystem<'_> {
    fn exists(&mut self, name: &str) -> Result<bool, DirectoryError> {
        super::path_exists(&self.0.join(name))
    }

    fn read_exact(&mut self, name: &str, bytes: usize) -> Result<Vec<u8>, DirectoryError> {
        super::read_exact_file(&self.0.join(name), bytes, "immutable metadata")
    }

    fn write_new(&mut self, name: &str, bytes: &[u8]) -> Result<(), DirectoryError> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.0.join(name))?;
        file.write_all(bytes)?;
        Ok(())
    }

    fn sync_file(&mut self, name: &str) -> Result<(), DirectoryError> {
        super::open_regular_file(&self.0.join(name), "metadata")?.sync_all()?;
        Ok(())
    }

    fn link(&mut self, source: &str, target: &str) -> Result<(), DirectoryError> {
        Ok(std::fs::hard_link(
            self.0.join(source),
            self.0.join(target),
        )?)
    }

    fn remove(&mut self, name: &str) -> Result<(), DirectoryError> {
        Ok(std::fs::remove_file(self.0.join(name))?)
    }

    fn rename(&mut self, source: &str, target: &str) -> Result<(), DirectoryError> {
        Ok(std::fs::rename(self.0.join(source), self.0.join(target))?)
    }

    fn sync_directory(&mut self) -> Result<(), DirectoryError> {
        super::sync_directory(self.0)
    }
}

impl SyncedOverwrite for Filesystem<'_> {
    fn write_synced(
        &mut self,
        name: &str,
        offsets: &[usize],
        bytes: &[u8],
    ) -> Result<(), DirectoryError> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            use std::os::unix::fs::FileExt;
            let path = self.0.join(name);
            super::require_regular_file(&path, "metadata")?;
            let file = std::fs::OpenOptions::new().write(true).open(path)?;
            let len = file.metadata()?.len();
            for offset in offsets {
                let end = *offset as u64 + bytes.len() as u64;
                if len < end {
                    return Err(DirectoryError::WrongFileSize {
                        object: "metadata",
                        actual: len,
                        expected: end,
                    });
                }
            }
            for offset in offsets {
                file.write_all_at(bytes, *offset as u64)?;
            }
            file.sync_data()?;
            Ok(())
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            let _ = (name, offsets, bytes);
            Err(io::Error::new(io::ErrorKind::Unsupported, "O_DSYNC requires Linux").into())
        }
    }
}

pub(crate) mod algorithm;
mod blocking;
use blocking::{Blocking, run_ready};

pub(crate) fn prepare_manifest(
    io: &mut impl MetadataIo,
    previous: &crate::Manifest,
    next: crate::Manifest,
    limits: crate::MetadataLimits,
) -> Result<(crate::Manifest, Vec<u8>, crate::Digest), DirectoryError> {
    run_ready(algorithm::prepare_manifest(
        &mut Blocking(io),
        previous,
        next,
        limits,
    ))
}

pub(crate) fn install_immutable(
    io: &mut impl MetadataIo,
    name: &str,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    run_ready(algorithm::install_immutable(
        &mut Blocking(io),
        name,
        bytes,
        observer,
    ))
}

pub(crate) fn replace_current(
    io: &mut impl MetadataIo,
    generation: u64,
    bytes: &[u8; super::CURRENT_BYTES],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    run_ready(algorithm::replace_current(
        &mut Blocking(io),
        generation,
        bytes,
        observer,
    ))
}

pub(crate) fn replace_evidence(
    io: &mut impl MetadataIo,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    run_ready(algorithm::replace_evidence(
        &mut Blocking(io),
        bytes,
        observer,
    ))
}

pub(crate) fn overwrite_evidence(
    io: &mut impl SyncedOverwrite,
    first: usize,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    run_ready(algorithm::overwrite_evidence(
        &mut Blocking(io),
        first,
        bytes,
        observer,
    ))
}

pub(crate) fn replace_reusable(
    io: &mut impl MetadataIo,
    target: &str,
    temporary: &str,
    bytes: &[u8],
    observer: &mut impl PersistenceObserver,
) -> Result<(), DirectoryError> {
    run_ready(algorithm::replace_reusable(
        &mut Blocking(io),
        target,
        temporary,
        bytes,
        observer,
    ))
}

#[cfg(test)]
mod simulation;
