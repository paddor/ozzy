use super::{DirectoryError, Journal, SegmentReference};
use crate::index_builder::asynchronous::{self, Builder, Limits};
use crate::{IndexBuildLimits, IndexLimits, IndexSource, SegmentIndex, segment_index_name};

impl Journal {
    pub(super) fn index_source(
        &self,
        id: u64,
    ) -> Result<(SegmentReference, IndexSource), DirectoryError> {
        let at = self
            .manifest
            .segments
            .binary_search_by_key(&id, |reference| reference.segment_id)
            .map_err(|_| DirectoryError::SegmentNotSealed(id))?;
        let reference = self.manifest.segments[at];
        let successor = self
            .manifest
            .segments
            .get(at + 1)
            .ok_or(DirectoryError::SegmentNotSealed(id))?;
        let source = crate::directory::sealed_source(
            self.manifest.identity.group_id,
            &reference,
            successor,
        )?;
        Ok((reference, source))
    }

    /// Open a derived index bound to an exact selected sealed segment.
    pub async fn open_sealed_index(
        &self,
        segment: u64,
        limits: IndexLimits,
    ) -> Result<SegmentIndex, DirectoryError> {
        self.healthy()?;
        let (_, source) = self.index_source(segment)?;
        Ok(asynchronous::open(
            &self.access,
            self.root().join("indexes").join(segment_index_name(source)),
            source,
            limits,
            self.limits.io.chunk_bytes,
        )
        .await?)
    }

    /// Build a source-bound disposable index with bounded sort/merge staging.
    /// Corrupt existing files are refused; repair must be requested explicitly.
    pub async fn build_sealed_index(
        &mut self,
        segment: u64,
        limits: IndexBuildLimits,
    ) -> Result<SegmentIndex, DirectoryError> {
        self.index_build(segment, limits, false).await
    }

    /// Only a corrupt derived index may be removed. Validate authoritative
    /// segment bytes before touching its index, then rebuild through file jobs.
    pub async fn repair_sealed_index(
        &mut self,
        segment: u64,
        limits: IndexBuildLimits,
    ) -> Result<SegmentIndex, DirectoryError> {
        self.index_build(segment, limits, true).await
    }

    async fn index_build(
        &mut self,
        segment: u64,
        limits: IndexBuildLimits,
        repair: bool,
    ) -> Result<SegmentIndex, DirectoryError> {
        self.healthy()?;
        let (reference, source) = self.index_source(segment)?;
        // Validate immutable authority before reuse or explicit index repair.
        let mut validated = self.segment_groups(reference).await?;
        while validated.next().await?.is_some() {}
        validated.finish().await?;
        let groups = self.segment_groups(reference).await?;
        self.interrupted = true;
        let index = Builder {
            access: self.access.clone(),
            root: self.root().to_path_buf(),
            io: Limits {
                chunk_bytes: self.limits.io.chunk_bytes,
                directory_entries: self.limits.directory_entries,
                directory_name_bytes: self.limits.directory_name_bytes,
            },
        }
        .build(groups, source, self.limits.operations, limits, repair)
        .await?;
        self.interrupted = false;
        Ok(index)
    }
}
