//! Bounded index construction over owned file jobs. Derived entry encoding,
//! source validation and resource accounting are shared with the legacy builder.

mod runs;
mod workspace;

use super::{
    INDEX_DIGEST_START, INDEX_HASH_CONTEXT, INDEX_HEADER_BYTES, IndexBuildError, IndexBuildLimits,
    IndexCounts, IndexFileError, IndexLimits, IndexSource, OperationLimits, PendingRuns, RunKind,
    SegmentIndex, SegmentScan, calculate_index_layout, derive_index_entries, encode_index_header,
    segment_index_name, validate_build_limits, validate_source_scan,
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
        scan: &SegmentScan<'_>,
        source: IndexSource,
        operations: OperationLimits,
        limits: IndexBuildLimits,
        repair: bool,
    ) -> Result<SegmentIndex, IndexBuildError> {
        validate_build_limits(limits)?;
        validate_source_scan(scan, source)?;
        if self.io.chunk_bytes < INDEX_HEADER_BYTES || self.io.directory_entries == 0 {
            return Err(IndexBuildError::InvalidBuildLimits);
        }
        let final_path = self.root.join("indexes").join(segment_index_name(source));
        if let Some(index) = self
            .reuse(final_path.clone(), source, limits.file, repair)
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
        let mut pending = PendingRuns::new(
            limits.max_entry_buffer_bytes,
            limits.max_run_files,
            limits.file,
        );
        let mut budget = crate::cooperative::Budget::default();
        for operation in scan.groups.iter().flat_map(|group| &group.operations) {
            let derived = derive_index_entries(&scan.header, operation, operations)?;
            if pending.requires_flush(&derived)? {
                pending.flush_async(&mut workspace).await?;
            }
            pending.buffer(derived)?;
            budget.charge(operation.body.len()).await;
        }
        pending.flush_async(&mut workspace).await?;
        let offsets = merge_all(
            pending.offset_runs,
            RunKind::Offset,
            limits.max_merge_fan_in,
            &mut workspace,
        )
        .await?;
        let messages = merge_all(
            pending.message_runs,
            RunKind::Message,
            limits.max_merge_fan_in,
            &mut workspace,
        )
        .await?;
        let operations = merge_all(
            pending.operation_runs,
            RunKind::Operation,
            limits.max_merge_fan_in,
            &mut workspace,
        )
        .await?;
        let temporary = workspace.reserve("complete.idx".into())?;
        self.write_complete(
            temporary.clone(),
            source,
            limits,
            pending.counts,
            [offsets, messages, operations],
        )
        .await?;
        let index = open(
            &self.access,
            temporary.clone(),
            source,
            limits.file,
            self.io.chunk_bytes,
        )
        .await?;
        self.access
            .done(Operation::HardLink {
                source: temporary,
                destination: final_path,
            })
            .await?;
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
        counts: IndexCounts,
        runs: [Option<PathBuf>; 3],
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
            layout.file_bytes - layout.operation_start,
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
