//! Bounded index construction over owned file jobs. Derived entry encoding,
//! source validation and resource accounting are shared with the legacy builder.

mod runs;
mod workspace;

use super::{
    INDEX_DIGEST_START, INDEX_HASH_CONTEXT, INDEX_HEADER_BYTES, IndexBuildError, IndexBuildLimits,
    IndexCounts, IndexFileError, IndexLimits, IndexSource, OperationLimits, PendingRuns, RunKind,
    SegmentIndex, calculate_index_layout, derive_index_entries, encode_index_header,
    segment_index_name, validate_build_limits,
};
use crate::async_files::Access;
use ozzy_io::{OpenMode, Operation, SyncMode};
use ozzy_journal::integrity::IntegrityHasher;
use runs::{Output, merge_all};
use std::{io, path::PathBuf};
use workspace::Workspace;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) chunk_bytes: usize,
    pub(crate) directory_entries: usize,
    pub(crate) directory_name_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct Builder {
    pub(crate) access: Access,
    pub(crate) root: PathBuf,
    pub(crate) io: Limits,
    pub(crate) operations: OperationLimits,
}

/// Merged section runs in file order, with their entry counts.
struct Merged {
    counts: IndexCounts,
    runs: [Option<PathBuf>; 4],
}

pub(crate) async fn open(
    access: &Access,
    path: PathBuf,
    source: IndexSource,
    limits: IndexLimits,
    chunk: usize,
) -> Result<SegmentIndex, IndexBuildError> {
    let file = access.open(path, OpenMode::Read, false, false).await?;
    let length =
        usize::try_from(access.length(&file).await?).map_err(|_| IndexFileError::LengthOverflow)?;
    if length > limits.max_file_bytes {
        return Err(IndexFileError::LimitExceeded {
            kind: "index file bytes",
            actual: length,
            limit: limits.max_file_bytes,
        }
        .into());
    }
    let bytes = access.read_range(&file, 0, length, chunk).await?;
    access.done(Operation::Close { handle: file }).await?;
    SegmentIndex::from_bytes_async(bytes, source, limits).await
}

impl Builder {
    /// Caller exclusively borrows its journal until publication settles. No
    /// workspace destructor touches files; cancellation leaves bounded staging
    /// artifacts for recovery and the caller fences its journal.
    pub(crate) async fn build(
        &self,
        groups: Box<crate::directory::asynchronous::groups::Groups>,
        source: IndexSource,
        limits: IndexBuildLimits,
        repair: bool,
    ) -> Result<SegmentIndex, crate::DirectoryError> {
        validate_build_limits(limits)?;
        if self.io.chunk_bytes < INDEX_HEADER_BYTES || self.io.directory_entries == 0 {
            return Err(IndexBuildError::InvalidBuildLimits.into());
        }
        if let Some(index) = self
            .reuse(self.index_path(source), source, limits.file, repair)
            .await?
        {
            return Ok(index);
        }
        let mut workspace = Workspace::create(
            self.access.clone(),
            self.root.join("staging"),
            source.segment_id,
            self.io,
        )
        .await?;
        let pending = self.stage(groups, limits, &mut workspace).await?;
        let (temporary, merged) =
            merge_sections(pending, limits.max_merge_fan_in, &mut workspace).await?;
        self.write_complete(temporary.clone(), source, limits, merged)
            .await?;
        Ok(self
            .publish(temporary, source, limits.file, workspace)
            .await?)
    }

    /// A valid published index of `source`, without reading the segment. The
    /// caller validates the segment before trusting it.
    pub(crate) async fn reuse_existing(
        &self,
        source: IndexSource,
        limits: IndexBuildLimits,
    ) -> Result<Option<SegmentIndex>, crate::DirectoryError> {
        validate_build_limits(limits)?;
        Ok(self
            .reuse(self.index_path(source), source, limits.file, false)
            .await?)
    }

    fn index_path(&self, source: IndexSource) -> PathBuf {
        self.root.join("indexes").join(segment_index_name(source))
    }

    /// Derive every operation's entries into sorted staging runs, flushing
    /// whenever the bounded entry buffer would overflow.
    async fn stage(
        &self,
        mut groups: Box<crate::directory::asynchronous::groups::Groups>,
        limits: IndexBuildLimits,
        workspace: &mut Workspace,
    ) -> Result<PendingRuns, crate::DirectoryError> {
        let mut pending = PendingRuns::new(
            limits.max_entry_buffer_bytes,
            limits.max_run_files,
            limits.file,
        );
        let header = groups.header().clone();
        let mut budget = crate::cooperative::Budget::default();
        while let Some(group) = groups.next().await? {
            for operation in &group.operations {
                let derived = derive_index_entries(&header, operation, self.operations)
                    .map_err(IndexBuildError::from)?;
                if pending.requires_flush(&derived)? {
                    pending.flush_async(workspace).await?;
                }
                pending.buffer(derived)?;
                budget.charge(operation.body.len()).await;
            }
        }
        groups.finish().await?;
        pending.flush_async(workspace).await?;
        Ok(pending)
    }

    /// Open the complete staged index, then link it under its final name.
    async fn publish(
        &self,
        temporary: PathBuf,
        source: IndexSource,
        limits: IndexLimits,
        workspace: Workspace,
    ) -> Result<SegmentIndex, IndexBuildError> {
        let index = open(
            &self.access,
            temporary.clone(),
            source,
            limits,
            self.io.chunk_bytes,
        )
        .await?;
        match self
            .access
            .done(Operation::HardLink {
                source: temporary,
                destination: self.index_path(source),
            })
            .await
        {
            // Another build of this source linked first; derived bytes of one
            // source are identical, and open checks them.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                open(
                    &self.access,
                    self.index_path(source),
                    source,
                    limits,
                    self.io.chunk_bytes,
                )
                .await?;
            }
            result => {
                result?;
            }
        }
        self.access
            .sync_directory(self.root.join("indexes"))
            .await?;
        workspace.cleanup().await?;
        self.access
            .sync_directory(self.root.join("staging"))
            .await?;
        Ok(index)
    }

    async fn reuse(
        &self,
        path: PathBuf,
        source: IndexSource,
        limits: IndexLimits,
        repair: bool,
    ) -> Result<Option<SegmentIndex>, IndexBuildError> {
        match open(
            &self.access,
            path.clone(),
            source,
            limits,
            self.io.chunk_bytes,
        )
        .await
        {
            Ok(index) => {
                // A previous canceled build may have linked a valid index but
                // not yet completed its directory barrier.
                self.access
                    .sync_directory(self.root.join("indexes"))
                    .await?;
                Ok(Some(index))
            }
            Err(IndexBuildError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(IndexBuildError::File(_) | IndexBuildError::SourceMismatch) if repair => {
                // Open above already required a regular, non-symlink file.
                self.access.done(Operation::RemoveFile { path }).await?;
                self.access
                    .sync_directory(self.root.join("indexes"))
                    .await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn write_complete(
        &self,
        path: PathBuf,
        source: IndexSource,
        limits: IndexBuildLimits,
        Merged { counts, runs }: Merged,
    ) -> Result<(), IndexBuildError> {
        let layout = calculate_index_layout(counts, limits.file)?;
        let file = self
            .access
            .open(path, OpenMode::CreateNew, false, false)
            .await?;
        let mut writer = Output::new(&self.access, &file, self.io.chunk_bytes);
        let header =
            encode_index_header(source, limits.max_entry_buffer_bytes as u64, counts, layout)?;
        let mut hasher = IntegrityHasher::new(INDEX_HASH_CONTEXT);
        hasher.update(&header);
        writer.push(&header).await?;
        for (run, bytes) in runs.into_iter().zip([
            layout.message_start - INDEX_HEADER_BYTES,
            layout.operation_start - layout.message_start,
            layout.location_start - layout.operation_start,
            layout.file_bytes - layout.location_start,
        ]) {
            self.copy_run(run, bytes, &mut writer, &mut hasher).await?;
        }
        writer.flush().await?;
        if writer.position() != layout.file_bytes as u64 {
            return Err(IndexBuildError::InvalidRun);
        }
        let digest = hasher.finish();
        self.access
            .write_all(&file, INDEX_DIGEST_START as u64, digest.as_bytes())
            .await?;
        self.access
            .done(Operation::Sync {
                handle: file.clone(),
                mode: SyncMode::All,
            })
            .await?;
        self.access.done(Operation::Close { handle: file }).await?;
        Ok(())
    }

    async fn copy_run(
        &self,
        run: Option<PathBuf>,
        bytes: usize,
        output: &mut Output<'_>,
        hasher: &mut IntegrityHasher,
    ) -> Result<(), IndexBuildError> {
        let Some(path) = run else {
            return if bytes == 0 {
                Ok(())
            } else {
                Err(IndexBuildError::InvalidRun)
            };
        };
        let file = self.access.open(path, OpenMode::Read, false, false).await?;
        if self.access.length(&file).await? != bytes as u64 {
            return Err(IndexBuildError::InvalidRun);
        }
        let mut offset = 0;
        while offset < bytes {
            let count = (bytes - offset).min(self.io.chunk_bytes);
            let data = self
                .access
                .read_range(&file, offset as u64, count, self.io.chunk_bytes)
                .await?;
            hasher.update(&data);
            output.push(&data).await?;
            offset += count;
        }
        self.access.done(Operation::Close { handle: file }).await?;
        Ok(())
    }
}

/// Merge each section's runs to one. The complete file's name is reserved
/// before the location merge, so the workspace bound counts it there.
async fn merge_sections(
    pending: PendingRuns,
    fan_in: usize,
    workspace: &mut Workspace,
) -> Result<(PathBuf, Merged), IndexBuildError> {
    let offsets = merge_all(pending.offset_runs, RunKind::Offset, fan_in, workspace).await?;
    let messages = merge_all(pending.message_runs, RunKind::Message, fan_in, workspace).await?;
    let operations = merge_all(
        pending.operation_runs,
        RunKind::Operation,
        fan_in,
        workspace,
    )
    .await?;
    let temporary = workspace.reserve("complete.idx".into())?;
    let locations = merge_all(pending.location_runs, RunKind::Location, fan_in, workspace).await?;
    let merged = Merged {
        counts: pending.counts,
        runs: [offsets, messages, operations, locations],
    };
    Ok((temporary, merged))
}
