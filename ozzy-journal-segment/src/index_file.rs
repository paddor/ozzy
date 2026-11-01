//! Immutable source-bound segment index file format.

#![cfg_attr(not(test), warn(clippy::indexing_slicing))]

use ozzy_journal::integrity::{IntegrityHasher as Hasher, hash_without_digest};
use ozzy_journal::operation::Digest;
use ozzy_proto::GroupId;
use ozzy_proto::layout::{self, Fixed};

mod asynchronous;
mod error;
mod order;
mod rows;
mod sections;
mod validate;
mod view;

pub(crate) use asynchronous::decode_segment_index_async;
pub use error::IndexFileError;
pub(crate) use order::{
    compare_batch_entries, compare_locations, compare_message_rows, compare_operation_entries,
    compare_schedule_entries,
};
pub(crate) use rows::{
    RowBase, encode_batch_entry, encode_index_header, encode_message_row, encode_operation_entry,
    encode_operation_location, encode_schedule_entry,
};
pub(crate) use sections::{IndexCounts, IndexLayout, calculate_index_layout};
pub use view::SegmentIndexView;

use rows::{decode_row_base, decode_source};
use validate::{section_checks, validate_image, validate_location_count, validate_source};

use crate::{
    BatchIndexEntry, MessageIndexEntry, MessageRow, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation, ScheduleIndexEntry,
};
use ozzy_proto::{MessageId, Offset, PartitionIncarnation};

const INDEX_MAGIC: &[u8; 8] = b"OZYIDX01";
const INDEX_VERSION: u16 = 5;
pub(crate) const INDEX_HASH_CONTEXT: &str = "ozzy segment index file v1";
pub(crate) const INDEX_DIGEST_START: usize = 256;

/// Exact encoded derived-index header length.
pub const INDEX_HEADER_BYTES: usize = 4096;
/// Exact encoded batch row length.
pub const BATCH_INDEX_ENTRY_BYTES: usize = 32;
/// Exact encoded message row length: an identity and a 32-bit offset delta.
pub const MESSAGE_INDEX_ENTRY_BYTES: usize = 20;
/// Exact encoded canonical operation selector length.
pub const OPERATION_INDEX_ENTRY_BYTES: usize = 80;
/// Exact encoded numeric operation selector length.
pub const OPERATION_LOCATION_BYTES: usize = 56;
/// Exact encoded scheduled-record row length.
pub const SCHEDULE_INDEX_ENTRY_BYTES: usize = 64;

/// Exact immutable segment prefix represented by one index file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexSource {
    /// Persistent partition replication-group identity.
    pub group_id: GroupId,
    /// Physical segment identity.
    pub segment_id: u64,
    /// Physical bytes covered by the exact validated segment prefix.
    pub valid_bytes: u64,
    /// Integrity digest binding the exact valid physical segment prefix.
    pub segment_digest: Digest,
    /// First canonical operation number in this source.
    pub first_op_number: u64,
    /// Last canonical operation number covered by this exact source.
    pub last_op_number: u64,
    /// Canonical chain digest at the last covered operation.
    pub last_operation_digest: Digest,
}

/// Resource limits checked before index allocation or section traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexLimits {
    /// Maximum encoded derived-index bytes, including its header.
    pub max_file_bytes: usize,
    /// Maximum batch rows per derived index.
    pub max_batch_entries: usize,
    /// Maximum message rows, one per record, per derived index.
    pub max_message_entries: usize,
    /// Maximum canonical operation selectors per derived index.
    pub max_operation_entries: usize,
    /// Maximum scheduled-record rows per derived index.
    pub max_schedule_entries: usize,
}

impl Default for IndexLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 1024 * 1024 * 1024,
            max_batch_entries: 16 * 1024 * 1024,
            max_message_entries: 16 * 1024 * 1024,
            max_operation_entries: 16 * 1024 * 1024,
            max_schedule_entries: 16 * 1024 * 1024,
        }
    }
}

/// Owned entries ready for deterministic immutable encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentIndexImage {
    source: IndexSource,
    build_memory_limit: u64,
    entries: IndexEntries,
}

/// Selectors for one index image, one vector per section, in any order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexEntries {
    /// Partition of the records below; `None` without records. An index holds
    /// one partition, as a journal does.
    pub partition: Option<PartitionIncarnation>,
    /// Batch rows.
    pub batches: Vec<BatchIndexEntry>,
    /// Message rows, one per record.
    pub messages: Vec<MessageRow>,
    /// Canonical operation-identity selectors.
    pub operations: Vec<OperationIndexEntry>,
    /// Numeric selectors, one per canonical operation.
    pub locations: Vec<OperationLocation>,
    /// Scheduled-record rows.
    pub schedules: Vec<ScheduleIndexEntry>,
}

impl IndexEntries {
    /// An image of these entries, each section sorted, before validation.
    fn sorted_image(mut self, source: IndexSource, build_memory_limit: u64) -> SegmentIndexImage {
        self.batches.sort_unstable_by(compare_batch_entries);
        self.messages.sort_unstable_by(compare_message_rows);
        self.operations.sort_unstable_by(compare_operation_entries);
        self.locations.sort_unstable_by(compare_locations);
        self.schedules.sort_unstable_by(compare_schedule_entries);
        SegmentIndexImage {
            source,
            build_memory_limit,
            entries: self,
        }
    }
}

impl SegmentIndexImage {
    /// Validate and retain canonical selectors for one exact segment source.
    pub fn new(
        source: IndexSource,
        build_memory_limit: u64,
        entries: IndexEntries,
    ) -> Result<Self, IndexFileError> {
        let image = entries.sorted_image(source, build_memory_limit);
        validate_image(&image)?;
        Ok(image)
    }

    /// Exact immutable segment prefix from which this index was derived.
    pub const fn source(&self) -> IndexSource {
        self.source
    }

    /// Encoded build-time memory allowance recorded in the index header.
    pub const fn build_memory_limit(&self) -> u64 {
        self.build_memory_limit
    }

    /// Partition of the records; `None` without records.
    pub const fn partition(&self) -> Option<PartitionIncarnation> {
        self.entries.partition
    }

    /// Batch rows in offset order.
    pub fn batches(&self) -> &[BatchIndexEntry] {
        &self.entries.batches
    }

    /// Message rows in identity, then offset, order.
    pub fn messages(&self) -> &[MessageRow] {
        &self.entries.messages
    }

    /// Canonical operation-identity selectors in sorted order.
    pub fn operations(&self) -> &[OperationIndexEntry] {
        &self.entries.operations
    }

    /// Every canonical operation in numeric order, including appends.
    pub fn locations(&self) -> &[OperationLocation] {
        &self.entries.locations
    }

    /// Scheduled-record rows by partition and release key.
    pub fn schedules(&self) -> &[ScheduleIndexEntry] {
        &self.entries.schedules
    }

    /// Exact operation in this validated dense numeric prefix.
    pub fn find_operation_number(&self, number: u64) -> Option<OperationLocation> {
        let at = usize::try_from(number.checked_sub(self.source.first_op_number)?).ok()?;
        self.entries.locations.get(at).copied()
    }

    /// What this image's rows count from once encoded.
    pub(crate) fn base(&self) -> RowBase {
        RowBase {
            partition: self.entries.partition,
            first_offset: self
                .entries
                .batches
                .first()
                .map_or(0, |batch| batch.first_offset.get()),
            first_op_number: self.source.first_op_number,
        }
    }

    /// The batch holding `offset`.
    pub(crate) fn batch_holding(&self, offset: Offset) -> Option<BatchIndexEntry> {
        let batches = self.batches();
        let at = batches
            .partition_point(|batch| batch.first_offset <= offset)
            .checked_sub(1)?;
        let batch = *batches.get(at)?;
        batch.record_index(offset).map(|_| batch)
    }

    /// Look up an exact partition incarnation and global record offset.
    pub fn find_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Option<OffsetIndexEntry> {
        if self.entries.partition != Some(partition) {
            return None;
        }
        let batch = self.batch_holding(offset)?;
        Some(batch.entry(
            partition,
            self.find_operation_number(batch.op_number)?,
            batch.record_index(offset)?,
        ))
    }

    /// Look up an exact partition incarnation and record identity.
    pub fn find_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Option<MessageIndexEntry> {
        let messages = self.messages();
        let row = messages.get(
            messages.partition_point(|row| row.message_id.as_bytes() < message_id.as_bytes()),
        )?;
        if row.message_id != message_id {
            return None;
        }
        Some(MessageIndexEntry {
            partition,
            message_id,
            offset: row.offset,
            operation_digest: self
                .find_offset(partition, row.offset)?
                .location
                .operation
                .operation_digest,
        })
    }

    /// The first record of each batch, in offset order. Only these carry an
    /// append time.
    pub(crate) fn batch_heads(&self) -> impl Iterator<Item = OffsetIndexEntry> + '_ {
        self.batches().iter().filter_map(|batch| {
            Some(batch.entry(
                self.entries.partition?,
                self.find_operation_number(batch.op_number)?,
                0,
            ))
        })
    }
}

/// Encode one sorted, validated source-bound index image.
pub fn encode_segment_index(
    image: &SegmentIndexImage,
    limits: IndexLimits,
) -> Result<Vec<u8>, IndexFileError> {
    validate_limits(limits)?;
    validate_image(image)?;
    let counts = IndexCounts {
        batches: image.batches().len(),
        messages: image.messages().len(),
        operations: image.operations().len(),
        locations: image.locations().len(),
        schedules: image.schedules().len(),
    };
    let layout = calculate_index_layout(counts, limits)?;

    let mut output = vec![0_u8; layout.file_bytes];
    let (header, sections) = layout
        .split_mut(&mut output)
        .ok_or(IndexFileError::InvalidSectionLayout)?;
    let base = image.base();
    *header = encode_index_header(image.source, image.build_memory_limit, base, counts, layout)?;
    encode_rows(image.batches(), sections.batches, |entry, row| {
        encode_batch_entry(entry, base, row);
    });
    encode_rows(image.messages(), sections.messages, |entry, row| {
        encode_message_row(entry, base, row);
    });
    encode_rows(
        image.operations(),
        sections.operations,
        encode_operation_entry,
    );
    encode_rows(
        image.locations(),
        sections.locations,
        encode_operation_location,
    );
    encode_rows(image.schedules(), sections.schedules, encode_schedule_entry);
    let digest = index_digest(&output);
    layout::patch(&mut output, INDEX_DIGEST_START, *digest.as_bytes())?;
    Ok(output)
}

/// Encode `entries` into consecutive `W`-byte rows of `section`.
fn encode_rows<T, const W: usize>(
    entries: &[T],
    section: &mut [u8],
    encode: impl Fn(&T, &mut [u8; W]),
) {
    for (entry, target) in entries.iter().zip(section.as_chunks_mut::<W>().0) {
        encode(entry, target);
    }
}

/// Validate one complete index without allocating per-entry state.
pub fn decode_segment_index(
    input: &[u8],
    limits: IndexLimits,
) -> Result<SegmentIndexView<'_>, IndexFileError> {
    validate_header(input, limits)?;
    let expected_digest = Digest::from_bytes(*layout::record(input, INDEX_DIGEST_START)?);
    if index_digest(input) != expected_digest {
        return Err(IndexFileError::DigestMismatch);
    }
    let view = decode_sections(input, limits)?;
    for check in section_checks(view) {
        check?;
    }
    Ok(view)
}

/// Fixed header checks precede checksumming and any entry traversal.
fn validate_header(input: &[u8], limits: IndexLimits) -> Result<(), IndexFileError> {
    validate_limits(limits)?;
    enforce_count("index file bytes", input.len(), limits.max_file_bytes)?;
    let header: &[u8; INDEX_HEADER_BYTES] = layout::record(input, 0)?;
    if header.array::<0, 8>() != *INDEX_MAGIC {
        return Err(IndexFileError::WrongMagic);
    }
    if header.u16_at::<8>() != INDEX_VERSION {
        return Err(IndexFileError::UnsupportedVersion(header.u16_at::<8>()));
    }
    if header.u16_at::<10>() != INDEX_HEADER_BYTES as u16 {
        return Err(IndexFileError::InvalidHeader);
    }
    if header.u32_at::<12>() != 0 {
        return Err(IndexFileError::UnsupportedFlags);
    }
    require_zero(header, 148..152)?;
    require_zero(header, 172..176)?;
    require_zero(header, 196..200)?;
    require_zero(header, 236..256)?;
    require_zero(header, 308..312)?;
    require_zero(header, 336..INDEX_HEADER_BYTES)?;
    if header.u32_at::<144>() != BATCH_INDEX_ENTRY_BYTES as u32
        || header.u32_at::<168>() != MESSAGE_INDEX_ENTRY_BYTES as u32
        || header.u32_at::<192>() != OPERATION_INDEX_ENTRY_BYTES as u32
        || header.u32_at::<232>() != OPERATION_LOCATION_BYTES as u32
        || header.u32_at::<304>() != SCHEDULE_INDEX_ENTRY_BYTES as u32
    {
        return Err(IndexFileError::UnsupportedEntryWidth);
    }

    let file_bytes = usize_from_u64(header.u64_at::<200>())?;
    if file_bytes != input.len() {
        return Err(IndexFileError::FileLengthMismatch {
            expected: file_bytes,
            actual: input.len(),
        });
    }
    Ok(())
}

/// Internal only: table integrity must validate before this view can escape.
fn decode_sections(
    input: &[u8],
    limits: IndexLimits,
) -> Result<SegmentIndexView<'_>, IndexFileError> {
    let header: &[u8; INDEX_HEADER_BYTES] = layout::record(input, 0)?;
    let source = decode_source(header);
    validate_source(source)?;
    let base = decode_row_base(header, source.first_op_number);
    let counts = IndexCounts {
        batches: usize_from_u64(header.u64_at::<128>())?,
        messages: usize_from_u64(header.u64_at::<152>())?,
        operations: usize_from_u64(header.u64_at::<176>())?,
        locations: usize_from_u64(header.u64_at::<216>())?,
        schedules: usize_from_u64(header.u64_at::<288>())?,
    };
    let expected = calculate_index_layout(counts, limits)?;
    validate_location_count(source, counts.locations)?;
    // The header states each section start; all must match the counts.
    let stated = IndexLayout {
        message_start: usize_from_u64(header.u64_at::<160>())?,
        operation_start: usize_from_u64(header.u64_at::<184>())?,
        location_start: usize_from_u64(header.u64_at::<224>())?,
        schedule_start: usize_from_u64(header.u64_at::<296>())?,
        file_bytes: input.len(),
    };
    if usize_from_u64(header.u64_at::<136>())? != INDEX_HEADER_BYTES || stated != expected {
        return Err(IndexFileError::InvalidSectionLayout);
    }
    let (_, sections) = expected
        .split(input)
        .ok_or(IndexFileError::InvalidSectionLayout)?;
    let first = sections
        .batches
        .first_chunk::<BATCH_INDEX_ENTRY_BYTES>()
        .map(Fixed::u64_at::<0>);
    if base.partition.is_some() != first.is_some() || base.first_offset != first.unwrap_or(0) {
        return Err(IndexFileError::InvalidRecordRange);
    }
    Ok(SegmentIndexView::new(
        source,
        header.u64_at::<208>(),
        base,
        sections,
    ))
}

fn validate_limits(limits: IndexLimits) -> Result<(), IndexFileError> {
    if limits.max_file_bytes < INDEX_HEADER_BYTES {
        return Err(IndexFileError::InvalidLimits);
    }
    Ok(())
}

fn enforce_count(kind: &'static str, actual: usize, limit: usize) -> Result<(), IndexFileError> {
    if actual > limit {
        Err(IndexFileError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn index_digest(input: &[u8]) -> Digest {
    hash_without_digest(INDEX_HASH_CONTEXT, input, INDEX_DIGEST_START)
}

fn require_zero(input: &[u8], range: std::ops::Range<usize>) -> Result<(), IndexFileError> {
    if input
        .get(range)
        .is_some_and(|bytes| bytes.iter().all(|&byte| byte == 0))
    {
        Ok(())
    } else {
        Err(IndexFileError::NonZeroReserved)
    }
}

fn u64_from_usize(value: usize) -> Result<u64, IndexFileError> {
    u64::try_from(value).map_err(|_| IndexFileError::LengthOverflow)
}

fn usize_from_u64(value: u64) -> Result<usize, IndexFileError> {
    usize::try_from(value).map_err(|_| IndexFileError::LengthOverflow)
}
