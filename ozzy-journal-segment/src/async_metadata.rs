//! Asynchronous metadata publication using the segment engine's existing
//! ordering algorithm. This is not a second journal: callers still validate
//! identity, histories and the positions selected by these metadata bytes.

mod files;
mod selected;
pub(crate) use selected::Selected;
#[cfg(test)]
mod tests;

use crate::directory::{NoopObserver, publication::algorithm};
use crate::{CurrentReference, Digest, DirectoryError, Manifest, MetadataLimits, WriterError};
use ozzy_io::Local;
use std::{io, path::PathBuf};

/// Limits on caller-owned metadata and on each physical transfer. Pending
/// publication bytes remain part of the partition's bounded resident state.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub(crate) max_file_bytes: usize,
    pub(crate) chunk_bytes: usize,
}

/// Exclusively locked metadata directory. No OS descriptors live here. Submitted
/// publication jobs retain the lock independently of this object's lifetime.
/// Failed or canceled publication fences this owner until it is reopened.
#[derive(Debug)]
pub(crate) struct Directory {
    files: files::Files,
    faulted: bool,
}

impl Directory {
    /// Only for an exclusive ownership transition: the caller must make the
    /// original owner read-only until this copy's exact publication settles.
    /// Handles share backend lifetime; this acquires no second file lock.
    pub(crate) fn detached_publication(&self) -> Result<Self, DirectoryError> {
        if self.faulted {
            return Err(WriterError::Faulted.into());
        }
        Ok(Self {
            files: self.files.clone(),
            faulted: false,
        })
    }

    pub(crate) fn access(&self) -> crate::async_files::Access {
        crate::async_files::Access {
            io: self.files.io.clone(),
            protection: Some(self.files.lock.clone()),
            readers: self.files.readers.clone(),
        }
    }

    pub(crate) async fn close(self) -> Result<(), DirectoryError> {
        self.files.close_all().await
    }

    pub(crate) fn root(&self) -> &std::path::Path {
        &self.files.root
    }

    pub(crate) async fn load_evidence(
        &mut self,
        manifest: &Manifest,
    ) -> Result<crate::directory::evidence::Copies, DirectoryError> {
        crate::directory::evidence::Copies::new(&self.files.evidence_records().await?, manifest)
    }

    pub(crate) async fn restore_evidence(
        &mut self,
        manifest: &Manifest,
    ) -> Result<crate::directory::evidence::Copies, DirectoryError> {
        use crate::directory::{evidence, publication::algorithm::Overwrite};
        let copies = self.load_evidence(manifest).await?;
        copies.protected(manifest)?;
        if !copies.mirrored() {
            let (record, copy) = copies.repair();
            self.begin()?;
            let result = self
                .files
                .write_synced(evidence::NAME, &[copy * evidence::COPY_STRIDE], &record)
                .await;
            self.finish(result)?;
            return evidence::Copies::new(&[record; 2], manifest);
        }
        Ok(copies)
    }
    /// Acquire a directory for metadata initialization. This may create its lock
    /// file, but never creates identity or journal data. Existing-store startup
    /// must use `open_existing`, which cannot create even a missing lock.
    pub(crate) async fn open(
        root: PathBuf,
        io: Local,
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        Ok(Self {
            files: files::Files::open(root, io, limits, true).await?,
            faulted: false,
        })
    }

    /// Acquire an established store without creating any missing file.
    pub(crate) async fn open_existing(
        root: PathBuf,
        io: Local,
        limits: Limits,
    ) -> Result<Self, DirectoryError> {
        Ok(Self {
            files: files::Files::open(root, io, limits, false).await?,
            faulted: false,
        })
    }

    pub(crate) const fn is_faulted(&self) -> bool {
        self.faulted
    }

    fn begin(&mut self) -> Result<(), DirectoryError> {
        if self.faulted {
            return Err(WriterError::Faulted.into());
        }
        self.faulted = true;
        Ok(())
    }

    fn finish<T>(&mut self, result: Result<T, DirectoryError>) -> Result<T, DirectoryError> {
        if result.is_ok() {
            self.faulted = false;
        }
        result
    }

    /// Select an unused immutable generation and publish its exact bytes. The
    /// caller installs this returned manifest only after selecting CURRENT.
    pub(crate) async fn publish_manifest(
        &mut self,
        previous: &Manifest,
        next: Manifest,
        limits: MetadataLimits,
    ) -> Result<(Manifest, Digest), DirectoryError> {
        if limits.max_manifest_bytes > self.files.limits.max_file_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "manifest limit exceeds metadata budget",
            )
            .into());
        }
        self.begin()?;
        let result = async {
            let (next, bytes, digest) =
                algorithm::prepare_manifest(&mut self.files, previous, next, limits).await?;
            algorithm::install_immutable(
                &mut self.files,
                &format!("MANIFEST.{}", next.generation),
                &bytes,
                &mut NoopObserver,
            )
            .await?;
            Ok((next, digest))
        }
        .await;
        self.finish(result)
    }

    /// Replace CURRENT after its referenced manifest and segments are durable.
    pub(crate) async fn select_current(
        &mut self,
        current: CurrentReference,
    ) -> Result<(), DirectoryError> {
        let bytes = crate::encode_current(current)?;
        self.begin()?;
        let result = algorithm::replace_current(
            &mut self.files,
            current.generation,
            &bytes,
            &mut NoopObserver,
        )
        .await;
        self.finish(result)
    }

    /// Install immutable metadata. Existing bytes must match exactly.
    pub(crate) async fn install_immutable(
        &mut self,
        name: &str,
        bytes: &[u8],
    ) -> Result<(), DirectoryError> {
        self.files.check(name, bytes.len())?;
        self.begin()?;
        let result =
            algorithm::install_immutable(&mut self.files, name, bytes, &mut NoopObserver).await;
        self.finish(result)
    }

    /// Replace mutable metadata through an unselected temporary and directory
    /// barrier. The temporary must be a distinct hidden `.tmp` basename.
    pub(crate) async fn replace(
        &mut self,
        target: &str,
        temporary: &str,
        bytes: &[u8],
    ) -> Result<(), DirectoryError> {
        self.files.check(target, bytes.len())?;
        self.files.check(temporary, bytes.len())?;
        if temporary == target
            || !temporary.starts_with('.')
            || temporary.strip_suffix(".tmp").is_none()
        {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput, "invalid metadata temporary").into(),
            );
        }
        self.begin()?;
        let result = algorithm::replace_reusable(
            &mut self.files,
            target,
            temporary,
            bytes,
            &mut NoopObserver,
        )
        .await;
        self.finish(result)
    }

    /// Publish a prevalidated DURABLE record into both copies. The caller owns
    /// sequence/position validation and selects the older copy first.
    pub(crate) async fn overwrite_evidence(
        &mut self,
        first: usize,
        bytes: &[u8],
    ) -> Result<(), DirectoryError> {
        if first > 1 || bytes.len() != crate::directory::evidence::RECORD_BYTES {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        self.begin()?;
        let result =
            algorithm::overwrite_evidence(&mut self.files, first, bytes, &mut NoopObserver).await;
        self.finish(result)
    }
}
