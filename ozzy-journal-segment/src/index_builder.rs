//! Bounded construction and crash-safe publication of derived segment indexes.

pub(crate) mod asynchronous;

use std::io;
use std::mem::size_of;
use std::path::PathBuf;

use ozzy_journal::operation::OperationLimits;
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::index_file::{
    INDEX_DIGEST_START, INDEX_HASH_CONTEXT, IndexCounts, calculate_index_layout,
    encode_index_header,
};
use crate::{
    INDEX_HEADER_BYTES, IndexError, IndexFileError, IndexLimits, IndexSource,
    MESSAGE_INDEX_ENTRY_BYTES, MessageIndexEntry, OFFSET_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation, SCHEDULE_INDEX_ENTRY_BYTES, ScheduleIndexEntry, SegmentIndexView,
    derive_index_entries,
};

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
            locations: view.location_count(),
            schedules: view.schedule_count(),
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

    /// Number of numeric selectors, including data and payload-free appends.
    pub fn location_count(&self) -> usize {
        self.view().location_count()
    }

    /// Number of scheduled-record rows.
    pub fn schedule_count(&self) -> usize {
        self.view().schedule_count()
    }

    /// Locate one canonical operation in constant table work.
    pub fn find_operation_number(&self, number: u64) -> Option<OperationLocation> {
        self.view().find_operation_number(number)
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

    pub(crate) fn operation_ids(&self) -> impl ExactSizeIterator<Item = OperationId> + '_ {
        self.view().operation_ids()
    }

    pub(crate) fn view(&self) -> SegmentIndexView<'_> {
        SegmentIndexView::from_validated(
            &self.bytes,
            self.source,
            self.build_memory_limit,
            self.layout,
        )
    }
}

/// Content-bound final filename for one sealed source segment.
pub fn segment_index_name(source: IndexSource) -> String {
    format!(
        "{}-{}.idx",
        source.segment_id,
        crate::directory::hex(source.segment_digest.as_bytes())
    )
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
    locations: Vec<OperationLocation>,
    schedules: Vec<ScheduleIndexEntry>,
    offset_runs: Vec<PathBuf>,
    message_runs: Vec<PathBuf>,
    operation_runs: Vec<PathBuf>,
    location_runs: Vec<PathBuf>,
    schedule_runs: Vec<PathBuf>,
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
                locations: 0,
                schedules: 0,
            },
            offsets: Vec::new(),
            messages: Vec::new(),
            operations: Vec::new(),
            locations: Vec::new(),
            schedules: Vec::new(),
            offset_runs: Vec::new(),
            message_runs: Vec::new(),
            operation_runs: Vec::new(),
            location_runs: Vec::new(),
            schedule_runs: Vec::new(),
        }
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
            locations: checked_add(self.counts.locations, 1)?,
            offsets: checked_add(self.counts.offsets, derived.offsets.len())?,
            messages: checked_add(self.counts.messages, derived.messages.len())?,
            operations: checked_add(
                self.counts.operations,
                usize::from(derived.operation.is_some()),
            )?,
            schedules: checked_add(self.counts.schedules, derived.schedules.len())?,
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
        self.locations.push(derived.location);
        self.schedules.extend(derived.schedules);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum RunKind {
    Offset,
    Message,
    Operation,
    Location,
    Schedule,
}

impl RunKind {
    const fn width(self) -> usize {
        match self {
            Self::Offset => OFFSET_INDEX_ENTRY_BYTES,
            Self::Message => MESSAGE_INDEX_ENTRY_BYTES,
            Self::Operation => OPERATION_INDEX_ENTRY_BYTES,
            Self::Location => OPERATION_LOCATION_BYTES,
            Self::Schedule => SCHEDULE_INDEX_ENTRY_BYTES,
        }
    }

    const fn key_bytes(self) -> usize {
        match self {
            Self::Offset => 24,
            Self::Message => 40,
            Self::Operation => 16,
            Self::Location => 8,
            Self::Schedule => 32,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Offset => "offset",
            Self::Message => "message",
            Self::Operation => "operation",
            Self::Location => "location",
            Self::Schedule => "schedule",
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
        .and_then(|total| total.checked_add(size_of::<OperationLocation>()))
        .and_then(|total| {
            entries
                .schedules
                .len()
                .checked_mul(size_of::<ScheduleIndexEntry>())
                .and_then(|bytes| total.checked_add(bytes))
        })
        .ok_or_else(|| IndexFileError::LengthOverflow.into())
}

fn validate_build_limits(limits: IndexBuildLimits) -> Result<(), IndexBuildError> {
    let minimum = size_of::<OffsetIndexEntry>()
        + size_of::<MessageIndexEntry>()
        + size_of::<OperationLocation>();
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
            locations: 0,
            schedules: 0,
        },
        limits.file,
    )?;
    Ok(())
}

fn checked_add(left: usize, right: usize) -> Result<usize, IndexBuildError> {
    left.checked_add(right)
        .ok_or_else(|| IndexFileError::LengthOverflow.into())
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
