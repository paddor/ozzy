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
        let scope = self.group_scope(reference)?;
        self.interrupted = true;
        let index = Box::pin(build(scope, self.index_builder(), source, limits, repair)).await?;
        self.interrupted = false;
        Ok(index)
    }

    fn index_builder(&self) -> Builder {
        Builder {
            access: self.access.clone(),
            root: self.root().to_path_buf(),
            io: Limits {
                chunk_bytes: self.limits.io.chunk_bytes,
                directory_entries: self.limits.directory_entries,
                directory_name_bytes: self.limits.directory_name_bytes,
            },
            operations: self.limits.operations,
        }
    }

    /// The oldest sealed segment this owner has not validated, as a detached
    /// index build; `finish_index_job` records it. The caller runs one at a
    /// time.
    pub fn prepare_index_job(
        &self,
        limits: IndexBuildLimits,
    ) -> Result<Option<IndexJob>, DirectoryError> {
        self.healthy()?;
        for pair in self.manifest.segments.windows(2) {
            let source = crate::directory::sealed_source(
                self.manifest.identity.group_id,
                &pair[0],
                &pair[1],
            )?;
            if self.validated_indexes.borrow().get(&source.segment_id) == Some(&source) {
                continue;
            }
            return Ok(Some(IndexJob {
                scope: self.group_scope(pair[0])?,
                builder: self.index_builder(),
                source,
                limits,
            }));
        }
        Ok(None)
    }

    /// Record a finished job while its segment is still selected. A failed
    /// job publishes nothing; the next snapshot build retries the segment.
    pub fn finish_index_job(&self, built: &BuiltIndex) {
        let selected = self
            .index_source(built.source.segment_id)
            .is_ok_and(|(_, source)| source == built.source);
        if built.valid && selected {
            self.validated_indexes
                .borrow_mut()
                .insert(built.source.segment_id, built.source);
        }
    }
}

/// A detached build of one sealed segment's index. It reads only immutable
/// sealed bytes and writes derived files, so writes continue beside it.
#[derive(Debug)]
pub struct IndexJob {
    scope: super::groups::GroupScope,
    builder: Builder,
    source: IndexSource,
    limits: IndexBuildLimits,
}

/// The outcome of an `IndexJob`.
#[derive(Debug)]
pub struct BuiltIndex {
    source: IndexSource,
    valid: bool,
}

impl BuiltIndex {
    /// Whether the index was built or validated.
    pub const fn valid(&self) -> bool {
        self.valid
    }
}

impl IndexJob {
    /// Build, or validate and reuse, the index. A failure publishes nothing.
    pub async fn run(self) -> BuiltIndex {
        let valid = build(self.scope, self.builder, self.source, self.limits, false)
            .await
            .is_ok();
        BuiltIndex {
            source: self.source,
            valid,
        }
    }
}

/// Validate the immutable segment once: before reuse or explicit repair, or
/// by the build itself, which checks every group and the segment end before
/// it publishes.
async fn build(
    scope: super::groups::GroupScope,
    builder: Builder,
    source: IndexSource,
    limits: IndexBuildLimits,
    repair: bool,
) -> Result<SegmentIndex, DirectoryError> {
    let reused = if repair {
        None
    } else {
        builder.reuse_existing(source, limits).await?
    };
    if repair || reused.is_some() {
        let mut validated = scope.clone().open(None).await?;
        while validated.next().await?.is_some() {}
        validated.finish().await?;
    }
    if let Some(index) = reused {
        return Ok(index);
    }
    // Boxed: index builds run deep inside snapshot and startup futures.
    let groups = scope.open(None).await?;
    Box::pin(builder.build(groups, source, limits, repair)).await
}
