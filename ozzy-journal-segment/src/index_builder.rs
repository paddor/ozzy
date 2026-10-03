//! Bounded construction and crash-safe publication of derived segment indexes.

pub(crate) mod asynchronous;

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::OperationLimits;
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::index_file::{
    INDEX_DIGEST_START, INDEX_HASH_CONTEXT, IndexCounts, calculate_index_layout,
    compare_message_entries, compare_offset_entries, compare_operation_entries,
    encode_index_header, encode_message_entry, encode_offset_entry, encode_operation_entry,
};
use crate::{
    INDEX_HEADER_BYTES, IndexError, IndexFileError, IndexLimits, IndexSource,
    MESSAGE_INDEX_ENTRY_BYTES, MessageIndexEntry, OFFSET_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, OffsetIndexEntry, OperationIndexEntry, SegmentIndexView,
    SegmentScan, TailState, decode_segment_index, derive_index_entries,
};

static WORKSPACE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Resource bounds for one external index build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexBuildLimits {
    /// Maximum encoded entry bytes retained between staging-run flushes.
    pub max_entry_buffer_bytes: usize,
    /// Maximum input runs opened by one merge.
    pub max_merge_fan_in: usize,
    /// Maximum initial staging runs per index section.
    pub max_run_files: usize,
    /// Bounds for final file counts and bytes.
    pub file: IndexLimits,
    /// Bytes of recently written operations an exclusive owner keeps in RAM
    /// for readers: compressed bodies plus record selectors. The oldest
    /// operations are evicted first and then read from their segment file.
    pub max_resident_bytes: usize,
}

impl Default for IndexBuildLimits {
    fn default() -> Self {
        Self {
            max_entry_buffer_bytes: 8 * 1024 * 1024,
            max_merge_fan_in: 16,
            max_run_files: 4096,
            file: IndexLimits::default(),
            max_resident_bytes: crate::reader::DEFAULT_RESIDENT_BYTES,
        }
    }
}

/// Fully validated immutable index loaded into one bounded byte image.
#[derive(Debug, Clone)]
pub struct SegmentIndex {
    bytes: Vec<u8>,
    source: IndexSource,
    build_memory_limit: u64,
    layout: crate::index_file::IndexLayout,
}

impl SegmentIndex {
    pub(crate) fn from_bytes(
        bytes: Vec<u8>,
        expected_source: IndexSource,
        limits: IndexLimits,
    ) -> Result<Self, IndexBuildError> {
        let view = decode_segment_index(&bytes, limits)?;
        let (build_memory_limit, layout) = Self::checked_layout(view, expected_source, limits)?;
        Ok(Self {
            bytes,
            source: expected_source,
            build_memory_limit,
            layout,
        })
    }

    pub(crate) async fn from_bytes_async(
        bytes: Vec<u8>,
        expected_source: IndexSource,
        limits: IndexLimits,
    ) -> Result<Self, IndexBuildError> {
        let view = crate::index_file::decode_segment_index_async(&bytes, limits).await?;
        let (build_memory_limit, layout) = Self::checked_layout(view, expected_source, limits)?;
        Ok(Self {
            bytes,
            source: expected_source,
            build_memory_limit,
            layout,
        })
    }

    fn checked_layout(
        view: SegmentIndexView<'_>,
        expected_source: IndexSource,
        limits: IndexLimits,
    ) -> Result<(u64, crate::index_file::IndexLayout), IndexBuildError> {
        if view.source() != expected_source {
            return Err(IndexBuildError::SourceMismatch);
        }
        let counts = IndexCounts {
            offsets: view.offset_count(),
            messages: view.message_count(),
            operations: view.operation_count(),
        };
        Ok((
            view.build_memory_limit(),
            calculate_index_layout(counts, limits)?,
        ))
    }

    /// Exact immutable segment prefix from which this index was derived.
    pub fn source(&self) -> IndexSource {
        self.view().source()
    }

    /// Number of record-offset selectors.
    pub fn offset_count(&self) -> usize {
        self.view().offset_count()
    }

    /// Number of message-identity selectors.
    pub fn message_count(&self) -> usize {
        self.view().message_count()
    }

    /// Number of canonical operation-identity selectors.
    pub fn operation_count(&self) -> usize {
        self.view().operation_count()
    }

    /// Look up an exact partition incarnation and global record offset.
    pub fn find_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Option<OffsetIndexEntry> {
        self.view().find_offset(partition, offset)
    }

    /// Look up an exact partition incarnation and record identity.
    pub fn find_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Option<MessageIndexEntry> {
        self.view().find_message(partition, message_id)
    }

    /// Look up an exact canonical control-operation identity.
    pub fn find_operation(&self, operation_id: OperationId) -> Option<OperationIndexEntry> {
        self.view().find_operation(operation_id)
    }

    /// Borrow the exact encoded physical representation.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn offsets(&self) -> impl ExactSizeIterator<Item = OffsetIndexEntry> + '_ {
        self.view().offsets()
    }

    fn view(&self) -> SegmentIndexView<'_> {
        SegmentIndexView::from_validated(
            &self.bytes,
            self.source,
            self.build_memory_limit,
            self.layout,
        )
    }
}

/// Open and fully validate one bounded index file against its authoritative source.
pub fn open_segment_index(
    path: impl AsRef<Path>,
    expected_source: IndexSource,
    limits: IndexLimits,
) -> Result<SegmentIndex, IndexBuildError> {
    let path = path.as_ref();
    require_regular_file(path)?;
    let mut file = File::open(path)?;
    let length =
        usize::try_from(file.metadata()?.len()).map_err(|_| IndexFileError::LengthOverflow)?;
    if length > limits.max_file_bytes {
        return Err(IndexFileError::LimitExceeded {
            kind: "index file bytes",
            actual: length,
            limit: limits.max_file_bytes,
        }
        .into());
    }
    let mut bytes = Vec::with_capacity(length);
    file.read_to_end(&mut bytes)?;
    SegmentIndex::from_bytes(bytes, expected_source, limits)
}

/// Build, synchronize, and publish one immutable index for a validated sealed segment.
///
/// Caller must hold exclusive group ownership and serialize in-process index
/// builders/repairs. Work belongs on a storage thread.
pub fn build_segment_index(
    scan: &SegmentScan<'_>,
    source: IndexSource,
    index_directory: impl AsRef<Path>,
    staging_directory: impl AsRef<Path>,
    operation_limits: OperationLimits,
    limits: IndexBuildLimits,
) -> Result<SegmentIndex, IndexBuildError> {
    let mut publication = IndexPublication::new(
        index_directory.as_ref().to_owned(),
        staging_directory.as_ref().to_owned(),
    );
    let index = publication.build(scan, source, operation_limits, limits)?;
    publication.finish(sync_directory)?;
    Ok(index)
}

/// One publication fence for a bounded sequence of disposable index builds.
/// Each file is synchronized before installation. Callers must finish before
/// exposing catalog coverage; unfinished names may need rebuilding after a crash.
#[derive(Debug)]
pub(crate) struct IndexPublication {
    index_directory: PathBuf,
    staging_directory: PathBuf,
    indexes_touched: bool,
    staging_touched: bool,
}

impl IndexPublication {
    pub(crate) fn new(index_directory: PathBuf, staging_directory: PathBuf) -> Self {
        Self {
            index_directory,
            staging_directory,
            indexes_touched: false,
            staging_touched: false,
        }
    }

    pub(crate) fn open(
        &mut self,
        source: IndexSource,
        limits: IndexLimits,
    ) -> Result<SegmentIndex, IndexBuildError> {
        let index = open_segment_index(
            self.index_directory.join(segment_index_name(source)),
            source,
            limits,
        )?;
        // A previous interrupted build may have installed this valid file
        // without completing its directory fence.
        self.indexes_touched = true;
        Ok(index)
    }

    pub(crate) fn finish(
        self,
        mut synchronize: impl FnMut(&Path) -> Result<(), IndexBuildError>,
    ) -> Result<(), IndexBuildError> {
        if self.indexes_touched {
            synchronize(&self.index_directory)?;
        }
        if self.staging_touched {
            synchronize(&self.staging_directory)?;
        }
        Ok(())
    }

    /// Reuse a validated file or remove only an invalid regular derived file.
    /// Caller holds the shared publication lock through rebuild and finish.
    pub(crate) fn reuse(
        &mut self,
        source: IndexSource,
        limits: IndexLimits,
    ) -> Result<Option<SegmentIndex>, IndexBuildError> {
        match self.open(source, limits) {
            Ok(index) => return Ok(Some(index)),
            Err(IndexBuildError::File(_) | IndexBuildError::SourceMismatch) => {}
            Err(IndexBuildError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        let path = self.index_directory.join(segment_index_name(source));
        require_regular_file(&path)?;
        fs::remove_file(path)?;
        self.indexes_touched = true;
        Ok(None)
    }

    pub(crate) fn build(
        &mut self,
        scan: &SegmentScan<'_>,
        source: IndexSource,
        operation_limits: OperationLimits,
        limits: IndexBuildLimits,
    ) -> Result<SegmentIndex, IndexBuildError> {
        validate_build_limits(limits)?;
        validate_source_scan(scan, source)?;
        let index_directory = &self.index_directory;
        let staging_directory = &self.staging_directory;
        require_directory(index_directory)?;
        require_directory(staging_directory)?;
        let final_path = index_directory.join(segment_index_name(source));
        match fs::symlink_metadata(&final_path) {
            Ok(_) => {
                return self.open(source, limits.file);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let mut workspace = BuildWorkspace::create(staging_directory, source.segment_id)?;
        self.staging_touched = true;
        let mut pending = PendingRuns::new(
            limits.max_entry_buffer_bytes,
            limits.max_run_files,
            limits.file,
        );
        for operation in scan.groups.iter().flat_map(|group| &group.operations) {
            let derived = derive_index_entries(&scan.header, operation, operation_limits)?;
            pending.push(derived, &mut workspace)?;
        }
        pending.flush(&mut workspace)?;
        let counts = pending.counts;
        let offset_run = merge_all(
            pending.offset_runs,
            RunKind::Offset,
            limits.max_merge_fan_in,
            &mut workspace,
        )?;
        let message_run = merge_all(
            pending.message_runs,
            RunKind::Message,
            limits.max_merge_fan_in,
            &mut workspace,
        )?;
        let operation_run = merge_all(
            pending.operation_runs,
            RunKind::Operation,
            limits.max_merge_fan_in,
            &mut workspace,
        )?;
        let temporary = workspace.path.join("complete.idx");
        write_complete_index(
            &temporary,
            source,
            u64::try_from(limits.max_entry_buffer_bytes)
                .map_err(|_| IndexFileError::LengthOverflow)?,
            counts,
            limits.file,
            [offset_run, message_run, operation_run],
        )?;
        let built = open_segment_index(&temporary, source, limits.file)?;
        publish_index(&temporary, &final_path, source, limits.file)?;
        self.indexes_touched = true;
        // Release bounded run files after each build, not after the whole catalog.
        // These are disposable files; the final fence makes coverage publishable.
        workspace.cleanup()?;
        Ok(built)
    }
}

/// Content-bound final filename for one sealed source segment.
pub fn segment_index_name(source: IndexSource) -> String {
    let mut digest = String::with_capacity(64);
    for byte in source.segment_digest.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut digest, "{byte:02x}").expect("writing into String is infallible");
    }
    format!("{}-{digest}.idx", source.segment_id)
}

#[derive(Debug)]
struct PendingRuns {
    max_bytes: usize,
    max_runs: usize,
    file_limits: IndexLimits,
    buffered_bytes: usize,
    counts: IndexCounts,
    offsets: Vec<OffsetIndexEntry>,
    messages: Vec<MessageIndexEntry>,
    operations: Vec<OperationIndexEntry>,
    offset_runs: Vec<PathBuf>,
    message_runs: Vec<PathBuf>,
    operation_runs: Vec<PathBuf>,
}

impl PendingRuns {
    fn new(max_bytes: usize, max_runs: usize, file_limits: IndexLimits) -> Self {
        Self {
            max_bytes,
            max_runs,
            file_limits,
            buffered_bytes: 0,
            counts: IndexCounts {
                offsets: 0,
                messages: 0,
                operations: 0,
            },
            offsets: Vec::new(),
            messages: Vec::new(),
            operations: Vec::new(),
            offset_runs: Vec::new(),
            message_runs: Vec::new(),
            operation_runs: Vec::new(),
        }
    }

    fn push(
        &mut self,
        derived: crate::DerivedIndexEntries,
        workspace: &mut BuildWorkspace,
    ) -> Result<(), IndexBuildError> {
        if self.requires_flush(&derived)? {
            self.flush(workspace)?;
        }
        self.buffer(derived)
    }

    fn requires_flush(
        &self,
        derived: &crate::DerivedIndexEntries,
    ) -> Result<bool, IndexBuildError> {
        let derived_bytes = derived_entry_bytes(derived)?;
        if derived_bytes > self.max_bytes {
            return Err(IndexBuildError::OperationExceedsBuffer {
                actual: derived_bytes,
                limit: self.max_bytes,
            });
        }
        Ok(self.buffered_bytes != 0
            && self
                .buffered_bytes
                .checked_add(derived_bytes)
                .is_none_or(|total| total > self.max_bytes))
    }

    fn buffer(&mut self, derived: crate::DerivedIndexEntries) -> Result<(), IndexBuildError> {
        let derived_bytes = derived_entry_bytes(&derived)?;
        if self.requires_flush(&derived)? {
            return Err(IndexBuildError::InvalidBuildLimits);
        }
        let next_counts = IndexCounts {
            offsets: checked_add(self.counts.offsets, derived.offsets.len())?,
            messages: checked_add(self.counts.messages, derived.messages.len())?,
            operations: checked_add(
                self.counts.operations,
                usize::from(derived.operation.is_some()),
            )?,
        };
        calculate_index_layout(next_counts, self.file_limits)?;
        self.counts = next_counts;
        self.buffered_bytes = self
            .buffered_bytes
            .checked_add(derived_bytes)
            .ok_or(IndexFileError::LengthOverflow)?;
        self.offsets.extend(derived.offsets);
        self.messages.extend(derived.messages);
        self.operations.extend(derived.operation);
        Ok(())
    }

    fn flush(&mut self, workspace: &mut BuildWorkspace) -> Result<(), IndexBuildError> {
        write_typed_run(
            &mut self.offsets,
            RunKind::Offset,
            workspace,
            &mut self.offset_runs,
            self.max_runs,
            compare_offset_entries,
            encode_offset_entry,
        )?;
        write_typed_run(
            &mut self.messages,
            RunKind::Message,
            workspace,
            &mut self.message_runs,
            self.max_runs,
            compare_message_entries,
            encode_message_entry,
        )?;
        write_typed_run(
            &mut self.operations,
            RunKind::Operation,
            workspace,
            &mut self.operation_runs,
            self.max_runs,
            compare_operation_entries,
            encode_operation_entry,
        )?;
        self.buffered_bytes = 0;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum RunKind {
    Offset,
    Message,
    Operation,
}

impl RunKind {
    const fn width(self) -> usize {
        match self {
            Self::Offset => OFFSET_INDEX_ENTRY_BYTES,
            Self::Message => MESSAGE_INDEX_ENTRY_BYTES,
            Self::Operation => OPERATION_INDEX_ENTRY_BYTES,
        }
    }

    const fn key_bytes(self) -> usize {
        match self {
            Self::Offset => 24,
            Self::Message => 40,
            Self::Operation => 16,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Offset => "offset",
            Self::Message => "message",
            Self::Operation => "operation",
        }
    }
}

fn derived_entry_bytes(entries: &crate::DerivedIndexEntries) -> Result<usize, IndexBuildError> {
    entries
        .offsets
        .len()
        .checked_mul(size_of::<OffsetIndexEntry>())
        .and_then(|total| {
            entries
                .messages
                .len()
                .checked_mul(size_of::<MessageIndexEntry>())
                .and_then(|bytes| total.checked_add(bytes))
        })
        .and_then(|total| {
            total.checked_add(
                usize::from(entries.operation.is_some()) * size_of::<OperationIndexEntry>(),
            )
        })
        .ok_or_else(|| IndexFileError::LengthOverflow.into())
}

fn write_typed_run<T>(
    entries: &mut Vec<T>,
    kind: RunKind,
    workspace: &mut BuildWorkspace,
    runs: &mut Vec<PathBuf>,
    max_runs: usize,
    compare: fn(&T, &T) -> std::cmp::Ordering,
    encode: fn(&T, &mut [u8]),
) -> Result<(), IndexBuildError> {
    if entries.is_empty() {
        return Ok(());
    }
    if runs.len() >= max_runs {
        return Err(IndexBuildError::RunLimitExceeded(max_runs));
    }
    entries.sort_unstable_by(compare);
    if entries
        .windows(2)
        .any(|pair| compare(&pair[0], &pair[1]) != std::cmp::Ordering::Less)
    {
        return Err(IndexFileError::UnsortedOrDuplicate(kind.name()).into());
    }
    let path = workspace.next_run(kind);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    let mut writer = BufWriter::new(file);
    let mut buffer = vec![0_u8; kind.width()];
    for entry in entries.iter() {
        buffer.fill(0);
        encode(entry, &mut buffer);
        writer.write_all(&buffer)?;
    }
    writer.flush()?;
    runs.push(path);
    entries.clear();
    Ok(())
}

fn merge_all(
    mut runs: Vec<PathBuf>,
    kind: RunKind,
    fan_in: usize,
    workspace: &mut BuildWorkspace,
) -> Result<Option<PathBuf>, IndexBuildError> {
    while runs.len() > 1 {
        let mut next = Vec::with_capacity(runs.len().div_ceil(fan_in));
        for chunk in runs.chunks(fan_in) {
            if chunk.len() == 1 {
                next.push(chunk[0].clone());
            } else {
                let output = workspace.next_run(kind);
                merge_runs(chunk, &output, kind)?;
                for input in chunk {
                    fs::remove_file(input)?;
                }
                next.push(output);
            }
        }
        runs = next;
    }
    Ok(runs.pop())
}

fn merge_runs(inputs: &[PathBuf], output: &Path, kind: RunKind) -> Result<(), IndexBuildError> {
    let mut readers = inputs
        .iter()
        .map(|path| RunReader::open(path, kind.width()))
        .collect::<Result<Vec<_>, _>>()?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    let mut writer = BufWriter::new(file);
    let mut previous_key = Vec::new();
    while let Some(selected) = readers
        .iter()
        .enumerate()
        .filter_map(|(index, reader)| reader.next.as_ref().map(|entry| (index, entry)))
        .min_by(|(_, left), (_, right)| left[..kind.key_bytes()].cmp(&right[..kind.key_bytes()]))
        .map(|(index, _)| index)
    {
        let entry = readers[selected]
            .next
            .take()
            .expect("selected run has an entry");
        let key = &entry[..kind.key_bytes()];
        if previous_key.as_slice() == key {
            return Err(IndexFileError::UnsortedOrDuplicate(kind.name()).into());
        }
        writer.write_all(&entry)?;
        previous_key.clear();
        previous_key.extend_from_slice(key);
        readers[selected].advance()?;
    }
    writer.flush()?;
    Ok(())
}

#[derive(Debug)]
struct RunReader {
    reader: BufReader<File>,
    width: usize,
    remaining: usize,
    next: Option<Vec<u8>>,
}

impl RunReader {
    fn open(path: &Path, width: usize) -> Result<Self, IndexBuildError> {
        require_regular_file(path)?;
        let file = File::open(path)?;
        let bytes =
            usize::try_from(file.metadata()?.len()).map_err(|_| IndexFileError::LengthOverflow)?;
        if bytes == 0 || !bytes.is_multiple_of(width) {
            return Err(IndexBuildError::InvalidRun);
        }
        let mut result = Self {
            reader: BufReader::new(file),
            width,
            remaining: bytes / width,
            next: None,
        };
        result.advance()?;
        Ok(result)
    }

    fn advance(&mut self) -> Result<(), IndexBuildError> {
        if self.remaining == 0 {
            self.next = None;
            return Ok(());
        }
        let mut entry = vec![0_u8; self.width];
        self.reader.read_exact(&mut entry)?;
        self.remaining -= 1;
        self.next = Some(entry);
        Ok(())
    }
}

fn write_complete_index(
    path: &Path,
    source: IndexSource,
    build_memory_limit: u64,
    counts: IndexCounts,
    limits: IndexLimits,
    runs: [Option<PathBuf>; 3],
) -> Result<(), IndexBuildError> {
    let layout = calculate_index_layout(counts, limits)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&encode_index_header(
        source,
        build_memory_limit,
        counts,
        layout,
    )?)?;
    for (run, expected_bytes) in runs.into_iter().zip([
        layout.message_start - INDEX_HEADER_BYTES,
        layout.operation_start - layout.message_start,
        layout.file_bytes - layout.operation_start,
    ]) {
        copy_run(run.as_deref(), expected_bytes, &mut writer)?;
    }
    writer.flush()?;
    let mut file = writer
        .into_inner()
        .map_err(io::IntoInnerError::into_error)?;
    if usize::try_from(file.metadata()?.len()).ok() != Some(layout.file_bytes) {
        return Err(IndexBuildError::InvalidRun);
    }
    let digest = stream_index_digest(&mut file)?;
    file.seek(SeekFrom::Start(INDEX_DIGEST_START as u64))?;
    file.write_all(digest.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

fn copy_run(
    path: Option<&Path>,
    expected_bytes: usize,
    output: &mut impl Write,
) -> Result<(), IndexBuildError> {
    let Some(path) = path else {
        return if expected_bytes == 0 {
            Ok(())
        } else {
            Err(IndexBuildError::InvalidRun)
        };
    };
    require_regular_file(path)?;
    let mut input = File::open(path)?;
    if usize::try_from(input.metadata()?.len()).ok() != Some(expected_bytes) {
        return Err(IndexBuildError::InvalidRun);
    }
    let copied = io::copy(&mut input, output)?;
    if usize::try_from(copied).ok() != Some(expected_bytes) {
        return Err(IndexBuildError::InvalidRun);
    }
    Ok(())
}

fn stream_index_digest(file: &mut File) -> Result<crate::Digest, IndexBuildError> {
    file.seek(SeekFrom::Start(0))?;
    let mut hasher = Hasher::new(INDEX_HASH_CONTEXT);
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut position = 0_usize;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let chunk = &mut buffer[..read];
        let start = INDEX_DIGEST_START.saturating_sub(position).min(read);
        let end = crate::index_file::INDEX_DIGEST_END
            .saturating_sub(position)
            .min(read);
        if start < end {
            chunk[start..end].fill(0);
        }
        hasher.update(chunk);
        position = position
            .checked_add(read)
            .ok_or(IndexFileError::LengthOverflow)?;
    }
    Ok(hasher.finish())
}

fn publish_index(
    temporary: &Path,
    final_path: &Path,
    source: IndexSource,
    limits: IndexLimits,
) -> Result<(), IndexBuildError> {
    match fs::hard_link(temporary, final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            open_segment_index(final_path, source, limits)?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn validate_source_scan(
    scan: &SegmentScan<'_>,
    source: IndexSource,
) -> Result<(), IndexBuildError> {
    if matches!(scan.tail, TailState::Truncated { .. })
        || scan.header.group_id() != source.group_id
        || scan.header.segment_id() != source.segment_id
        || scan.valid_bytes != source.valid_bytes
        || scan.digest != source.segment_digest
    {
        return Err(IndexBuildError::SourceMismatch);
    }
    let mut operations = scan.groups.iter().flat_map(|group| &group.operations);
    let Some(first) = operations.next() else {
        return Err(IndexBuildError::SourceMismatch);
    };
    let last = operations.next_back().unwrap_or(first);
    if first.op_number != source.first_op_number
        || last.op_number != source.last_op_number
        || last.digest != source.last_operation_digest
        || scan.next_chain.next_op_number() != source.last_op_number.checked_add(1).unwrap_or(0)
        || scan.next_chain.previous_digest() != source.last_operation_digest
    {
        return Err(IndexBuildError::SourceMismatch);
    }
    Ok(())
}

fn validate_build_limits(limits: IndexBuildLimits) -> Result<(), IndexBuildError> {
    let minimum = size_of::<OffsetIndexEntry>() + size_of::<MessageIndexEntry>();
    if limits.max_entry_buffer_bytes < minimum
        || limits.max_merge_fan_in < 2
        || limits.max_run_files == 0
    {
        return Err(IndexBuildError::InvalidBuildLimits);
    }
    calculate_index_layout(
        IndexCounts {
            offsets: 0,
            messages: 0,
            operations: 0,
        },
        limits.file,
    )?;
    Ok(())
}

fn checked_add(left: usize, right: usize) -> Result<usize, IndexBuildError> {
    left.checked_add(right)
        .ok_or_else(|| IndexFileError::LengthOverflow.into())
}

fn require_regular_file(path: &Path) -> Result<(), IndexBuildError> {
    if fs::symlink_metadata(path)?.file_type().is_file() {
        Ok(())
    } else {
        Err(IndexBuildError::NotRegularFile)
    }
}

fn require_directory(path: &Path) -> Result<(), IndexBuildError> {
    if fs::symlink_metadata(path)?.file_type().is_dir() {
        Ok(())
    } else {
        Err(IndexBuildError::NotDirectory)
    }
}

fn sync_directory(path: &Path) -> Result<(), IndexBuildError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Debug)]
struct BuildWorkspace {
    path: PathBuf,
    next_run: u64,
    cleaned: bool,
}

impl BuildWorkspace {
    fn create(parent: &Path, segment_id: u64) -> Result<Self, IndexBuildError> {
        for _ in 0..1024 {
            let sequence = WORKSPACE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!(
                "index-{segment_id}-{}-{sequence}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        next_run: 0,
                        cleaned: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(IndexBuildError::WorkspaceExhausted)
    }

    fn next_run(&mut self, kind: RunKind) -> PathBuf {
        let path = self
            .path
            .join(format!("{}-{}.run", kind.name(), self.next_run));
        self.next_run += 1;
        path
    }

    fn cleanup(&mut self) -> Result<(), IndexBuildError> {
        fs::remove_dir_all(&self.path)?;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for BuildWorkspace {
    fn drop(&mut self) {
        if !self.cleaned {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

/// Persistent index build, publication, or open failure.
#[derive(Debug, Error)]
pub enum IndexBuildError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error(transparent)]
    /// The encoded index representation is invalid.
    File(#[from] IndexFileError),
    #[error(transparent)]
    /// Canonical records could not yield a valid derived index.
    Derive(#[from] IndexError),
    #[error("index source does not match validated segment")]
    /// Index source does not match validated segment.
    SourceMismatch,
    #[error("invalid index build resource limits")]
    /// Invalid index build resource limits.
    InvalidBuildLimits,
    #[error("one operation needs {actual} index-buffer bytes; limit is {limit}")]
    /// One operation needs index-buffer bytes; limit is.
    OperationExceedsBuffer {
        #[doc = "Observed size, count, or fenced field value."]
        actual: usize,
        #[doc = "Configured maximum for the reported resource."]
        limit: usize,
    },
    #[error("staging run is empty, truncated, or has the wrong width")]
    /// Staging run is empty, truncated, or has the wrong width.
    InvalidRun,
    #[error("index build exceeded staging run limit {0}")]
    /// Index build exceeded staging run limit.
    RunLimitExceeded(usize),
    #[error("could not allocate a unique index staging workspace")]
    /// Could not allocate a unique index staging workspace.
    WorkspaceExhausted,
    #[error("index path is not a regular file")]
    /// The named artifact is not a regular file.
    NotRegularFile,
    #[error("index or staging path is not a directory")]
    /// The named artifact is not a directory.
    NotDirectory,
}
