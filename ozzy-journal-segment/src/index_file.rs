//! Immutable source-bound segment index file format.

use std::cmp::Ordering;

use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::Digest;
use ozzy_proto::{GroupId, MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

mod asynchronous;
pub(crate) use asynchronous::decode_segment_index_async;

use crate::{
    ENTRY_HEADER_BYTES, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation, RecordLocation, SEGMENT_HEADER_BYTES,
};

const INDEX_MAGIC: &[u8; 8] = b"OZYIDX01";
const INDEX_VERSION: u16 = 2;
pub(crate) const INDEX_HASH_CONTEXT: &str = "ozzy segment index file v1";
pub(crate) const INDEX_DIGEST_START: usize = 256;
pub(crate) const INDEX_DIGEST_END: usize = 288;

/// Exact encoded derived-index header length.
pub const INDEX_HEADER_BYTES: usize = 4096;
/// Exact encoded record-offset selector length.
pub const OFFSET_INDEX_ENTRY_BYTES: usize = 96;
/// Exact encoded message-identity selector length.
pub const MESSAGE_INDEX_ENTRY_BYTES: usize = 80;
/// Exact encoded canonical operation selector length.
pub const OPERATION_INDEX_ENTRY_BYTES: usize = 80;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexCounts {
    pub offsets: usize,
    pub messages: usize,
    pub operations: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexLayout {
    pub message_start: usize,
    pub operation_start: usize,
    pub file_bytes: usize,
}

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
    /// Maximum record-offset selectors per derived index.
    pub max_offset_entries: usize,
    /// Maximum message-identity selectors per derived index.
    pub max_message_entries: usize,
    /// Maximum canonical operation selectors per derived index.
    pub max_operation_entries: usize,
}

impl Default for IndexLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 1024 * 1024 * 1024,
            max_offset_entries: 16 * 1024 * 1024,
            max_message_entries: 16 * 1024 * 1024,
            max_operation_entries: 16 * 1024 * 1024,
        }
    }
}

/// Owned entries ready for deterministic immutable encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentIndexImage {
    source: IndexSource,
    build_memory_limit: u64,
    offsets: Vec<OffsetIndexEntry>,
    messages: Vec<MessageIndexEntry>,
    operations: Vec<OperationIndexEntry>,
}

impl SegmentIndexImage {
    /// Validate and retain canonical selectors for one exact segment source.
    pub fn new(
        source: IndexSource,
        build_memory_limit: u64,
        mut offsets: Vec<OffsetIndexEntry>,
        mut messages: Vec<MessageIndexEntry>,
        mut operations: Vec<OperationIndexEntry>,
    ) -> Result<Self, IndexFileError> {
        offsets.sort_unstable_by(compare_offset_entries);
        messages.sort_unstable_by(compare_message_entries);
        operations.sort_unstable_by(compare_operation_entries);
        let image = Self {
            source,
            build_memory_limit,
            offsets,
            messages,
            operations,
        };
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

    /// Record-offset selectors in canonical sorted order.
    pub fn offsets(&self) -> &[OffsetIndexEntry] {
        &self.offsets
    }

    /// Message-identity selectors in canonical sorted order.
    pub fn messages(&self) -> &[MessageIndexEntry] {
        &self.messages
    }

    /// Canonical operation-identity selectors in sorted order.
    pub fn operations(&self) -> &[OperationIndexEntry] {
        &self.operations
    }
}

/// Validated zero-copy view over one complete immutable index file.
#[derive(Debug, Clone, Copy)]
pub struct SegmentIndexView<'a> {
    source: IndexSource,
    build_memory_limit: u64,
    offsets: &'a [u8],
    messages: &'a [u8],
    operations: &'a [u8],
}

impl SegmentIndexView<'_> {
    /// Exact immutable segment prefix from which this index was derived.
    pub const fn source(&self) -> IndexSource {
        self.source
    }

    /// Encoded build-time memory allowance recorded in the index header.
    pub const fn build_memory_limit(&self) -> u64 {
        self.build_memory_limit
    }

    /// Number of record-offset selectors.
    pub fn offset_count(&self) -> usize {
        self.offsets.len() / OFFSET_INDEX_ENTRY_BYTES
    }

    /// Number of message-identity selectors.
    pub fn message_count(&self) -> usize {
        self.messages.len() / MESSAGE_INDEX_ENTRY_BYTES
    }

    /// Number of canonical operation-identity selectors.
    pub fn operation_count(&self) -> usize {
        self.operations.len() / OPERATION_INDEX_ENTRY_BYTES
    }

    /// Look up an exact partition incarnation and global record offset.
    pub fn find_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Option<OffsetIndexEntry> {
        binary_search_section(self.offsets, OFFSET_INDEX_ENTRY_BYTES, |entry| {
            compare_offset_key(entry, partition, offset)
        })
        .map(|entry| decode_offset_entry(entry, self.source.segment_id))
    }

    /// Look up an exact partition incarnation and record identity.
    pub fn find_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Option<MessageIndexEntry> {
        lower_bound_section(self.messages, MESSAGE_INDEX_ENTRY_BYTES, |entry| {
            compare_message_key(entry, partition, message_id) == Ordering::Less
        })
        .filter(|entry| compare_message_key(entry, partition, message_id) == Ordering::Equal)
        .map(decode_message_entry)
    }

    /// Oldest/newest matches inside one retained partition range. Binary search
    /// includes the offset, so even millions of duplicate IDs keep bounded work.
    pub fn message_offsets(
        &self,
        partition: PartitionIncarnation,
        id: MessageId,
        floor: Offset,
        end: Offset,
    ) -> Option<(Offset, Offset)> {
        let rows = self.messages.as_chunks::<MESSAGE_INDEX_ENTRY_BYTES>().0;
        let before = |row: &[u8], offset: Offset| {
            compare_message_key(row, partition, id)
                .then_with(|| read_u64(row, 32).cmp(&offset.get()))
                .is_lt()
        };
        let first = rows.partition_point(|row| before(row, floor));
        let after = rows.partition_point(|row| before(row, end));
        let matching = rows.get(first..after)?.split_first()?;
        if compare_message_key(matching.0, partition, id) != Ordering::Equal {
            return None;
        }
        Some((
            Offset::new(read_u64(matching.0, 32)),
            Offset::new(read_u64(matching.1.last().unwrap_or(matching.0), 32)),
        ))
    }

    /// Sparse APPEND timestamps live on batch-head offset rows. Payloads remain
    /// unopened; nonmonotonic clocks are resolved in record-offset order.
    pub fn timestamp_offset(
        &self,
        partition: PartitionIncarnation,
        timestamp: u64,
        floor: Offset,
        end: Offset,
        through: u64,
    ) -> Option<Offset> {
        let at_floor = self.find_offset(partition, floor);
        self.offsets()
            .filter(|entry| {
                entry.partition == partition
                    && entry.location.record_index == 0
                    && entry.append_timestamp_millis >= timestamp
                    && entry.location.operation.op_number <= through
                    && entry.offset < end
            })
            .find_map(|head| {
                if head.offset >= floor {
                    return Some(head.offset);
                }
                at_floor
                    .filter(|entry| {
                        entry.location.operation == head.location.operation
                            && entry.location.batch_index == head.location.batch_index
                    })
                    .map(|_| floor)
            })
    }

    /// Look up an exact canonical control-operation identity.
    pub fn find_operation(&self, operation_id: OperationId) -> Option<OperationIndexEntry> {
        binary_search_section(self.operations, OPERATION_INDEX_ENTRY_BYTES, |entry| {
            entry[..16].cmp(operation_id.as_bytes())
        })
        .map(|entry| decode_operation_entry(entry, self.source.segment_id))
    }
}

impl<'a> SegmentIndexView<'a> {
    pub(crate) fn offsets(self) -> impl ExactSizeIterator<Item = OffsetIndexEntry> + 'a {
        self.offsets
            .as_chunks::<OFFSET_INDEX_ENTRY_BYTES>()
            .0
            .iter()
            .map(move |entry| decode_offset_entry(entry, self.source.segment_id))
    }

    pub(crate) fn from_validated(
        bytes: &'a [u8],
        source: IndexSource,
        build_memory_limit: u64,
        layout: IndexLayout,
    ) -> Self {
        debug_assert_eq!(bytes.len(), layout.file_bytes);
        Self {
            source,
            build_memory_limit,
            offsets: &bytes[INDEX_HEADER_BYTES..layout.message_start],
            messages: &bytes[layout.message_start..layout.operation_start],
            operations: &bytes[layout.operation_start..],
        }
    }
}

/// Encode one sorted, validated source-bound index image.
pub fn encode_segment_index(
    image: &SegmentIndexImage,
    limits: IndexLimits,
) -> Result<Vec<u8>, IndexFileError> {
    validate_limits(limits)?;
    validate_image(image)?;
    enforce_count(
        "offset entries",
        image.offsets.len(),
        limits.max_offset_entries,
    )?;
    enforce_count(
        "message entries",
        image.messages.len(),
        limits.max_message_entries,
    )?;
    enforce_count(
        "operation entries",
        image.operations.len(),
        limits.max_operation_entries,
    )?;

    let counts = IndexCounts {
        offsets: image.offsets.len(),
        messages: image.messages.len(),
        operations: image.operations.len(),
    };
    let layout = calculate_index_layout(counts, limits)?;

    let mut output = vec![0_u8; layout.file_bytes];
    output[..INDEX_HEADER_BYTES].copy_from_slice(&encode_index_header(
        image.source,
        image.build_memory_limit,
        counts,
        layout,
    )?);
    for (entry, target) in image.offsets.iter().zip(
        output[INDEX_HEADER_BYTES..layout.message_start]
            .as_chunks_mut::<OFFSET_INDEX_ENTRY_BYTES>()
            .0,
    ) {
        encode_offset_entry(entry, target);
    }
    for (entry, target) in image.messages.iter().zip(
        output[layout.message_start..layout.operation_start]
            .as_chunks_mut::<MESSAGE_INDEX_ENTRY_BYTES>()
            .0,
    ) {
        encode_message_entry(entry, target);
    }
    for (entry, target) in image.operations.iter().zip(
        output[layout.operation_start..]
            .as_chunks_mut::<OPERATION_INDEX_ENTRY_BYTES>()
            .0,
    ) {
        encode_operation_entry(entry, target);
    }
    let digest = index_digest(&output);
    output[INDEX_DIGEST_START..INDEX_DIGEST_END].copy_from_slice(digest.as_bytes());
    Ok(output)
}

/// Validate one complete index without allocating per-entry state.
pub fn decode_segment_index(
    input: &[u8],
    limits: IndexLimits,
) -> Result<SegmentIndexView<'_>, IndexFileError> {
    validate_header(input, limits)?;
    let expected_digest = digest_at(input, INDEX_DIGEST_START..INDEX_DIGEST_END);
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
    if input.len() < INDEX_HEADER_BYTES {
        return Err(IndexFileError::Truncated {
            needed: INDEX_HEADER_BYTES,
            available: input.len(),
        });
    }
    let header = &input[..INDEX_HEADER_BYTES];
    if &header[..8] != INDEX_MAGIC {
        return Err(IndexFileError::WrongMagic);
    }
    if read_u16(header, 8) != INDEX_VERSION {
        return Err(IndexFileError::UnsupportedVersion(read_u16(header, 8)));
    }
    if read_u16(header, 10) != INDEX_HEADER_BYTES as u16 {
        return Err(IndexFileError::InvalidHeader);
    }
    if read_u32(header, 12) != 0 {
        return Err(IndexFileError::UnsupportedFlags);
    }
    require_zero(header, 148..152)?;
    require_zero(header, 172..176)?;
    require_zero(header, 196..200)?;
    require_zero(header, 216..256)?;
    require_zero(header, 288..INDEX_HEADER_BYTES)?;
    if read_u32(header, 144) != OFFSET_INDEX_ENTRY_BYTES as u32
        || read_u32(header, 168) != MESSAGE_INDEX_ENTRY_BYTES as u32
        || read_u32(header, 192) != OPERATION_INDEX_ENTRY_BYTES as u32
    {
        return Err(IndexFileError::UnsupportedEntryWidth);
    }

    let file_bytes = usize_from_u64(read_u64(header, 200))?;
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
    let header = &input[..INDEX_HEADER_BYTES];
    let file_bytes = input.len();
    let source = decode_source(header)?;
    validate_source(source)?;

    let offset_count = count_from_header(header, 128, limits.max_offset_entries, "offset entries")?;
    let message_count =
        count_from_header(header, 152, limits.max_message_entries, "message entries")?;
    let operation_count = count_from_header(
        header,
        176,
        limits.max_operation_entries,
        "operation entries",
    )?;
    if offset_count != message_count {
        return Err(IndexFileError::RecordIndexCountMismatch);
    }
    let offset_start = usize_from_u64(read_u64(header, 136))?;
    let message_start = usize_from_u64(read_u64(header, 160))?;
    let operation_start = usize_from_u64(read_u64(header, 184))?;
    let expected_message_start = INDEX_HEADER_BYTES
        .checked_add(section_bytes(offset_count, OFFSET_INDEX_ENTRY_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    let expected_operation_start = expected_message_start
        .checked_add(section_bytes(message_count, MESSAGE_INDEX_ENTRY_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    let expected_file_bytes = expected_operation_start
        .checked_add(section_bytes(operation_count, OPERATION_INDEX_ENTRY_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    if offset_start != INDEX_HEADER_BYTES
        || message_start != expected_message_start
        || operation_start != expected_operation_start
        || file_bytes != expected_file_bytes
    {
        return Err(IndexFileError::InvalidSectionLayout);
    }

    let offsets = &input[offset_start..message_start];
    let messages = &input[message_start..operation_start];
    let operations = &input[operation_start..];
    Ok(SegmentIndexView {
        source,
        build_memory_limit: read_u64(header, 208),
        offsets,
        messages,
        operations,
    })
}

fn validate_image(image: &SegmentIndexImage) -> Result<(), IndexFileError> {
    validate_image_header(image)?;
    for check in image_checks(image) {
        check?;
    }
    Ok(())
}

fn validate_image_header(image: &SegmentIndexImage) -> Result<(), IndexFileError> {
    validate_source(image.source)?;
    if image.offsets.len() != image.messages.len() {
        return Err(IndexFileError::RecordIndexCountMismatch);
    }
    Ok(())
}

fn image_checks(
    image: &SegmentIndexImage,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + '_ {
    sorted_entry_checks(&image.offsets, compare_offset_entries, "offset")
        .chain(sorted_entry_checks(
            &image.messages,
            compare_message_entries,
            "message",
        ))
        .chain(sorted_entry_checks(
            &image.operations,
            compare_operation_entries,
            "operation",
        ))
        .chain(image.offsets.iter().map(|entry| {
            if entry.location.record_index != 0 && entry.append_timestamp_millis != 0 {
                return Err(IndexFileError::NonZeroReserved);
            }
            validate_location(image.source, entry.location.operation)?;
            Ok(OFFSET_INDEX_ENTRY_BYTES)
        }))
        .chain(image.operations.iter().map(|entry| {
            validate_location(image.source, entry.location)?;
            Ok(OPERATION_INDEX_ENTRY_BYTES)
        }))
        .chain(image.messages.iter().map(|message| {
            if image
                .offsets
                .binary_search_by(|entry| {
                    compare_partition_offset(
                        entry.partition,
                        entry.offset,
                        message.partition,
                        message.offset,
                    )
                })
                .is_err()
            {
                return Err(IndexFileError::MissingOffsetEntry);
            }
            Ok(MESSAGE_INDEX_ENTRY_BYTES)
        }))
}

fn validate_source(source: IndexSource) -> Result<(), IndexFileError> {
    if source.group_id.as_bytes().iter().all(|byte| *byte == 0)
        || source.segment_id == 0
        || source.valid_bytes < SEGMENT_HEADER_BYTES as u64
        || source.segment_digest == Digest::ZERO
        || source.first_op_number == 0
        || source.last_op_number < source.first_op_number
        || source.last_operation_digest == Digest::ZERO
    {
        return Err(IndexFileError::InvalidSource);
    }
    Ok(())
}

/// Check ordering first so later binary searches only see validated ordering.
/// The iterator retains no entry allocation and each step checks at most one
/// entry (including a logarithmic offset lookup for message entries).
fn section_checks(
    view: SegmentIndexView<'_>,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + '_ {
    let source = view.source;
    raw_order_checks(view.offsets, OFFSET_INDEX_ENTRY_BYTES, 24, "offset")
        .chain(raw_order_checks(
            view.messages,
            MESSAGE_INDEX_ENTRY_BYTES,
            40,
            "message",
        ))
        .chain(raw_order_checks(
            view.operations,
            OPERATION_INDEX_ENTRY_BYTES,
            16,
            "operation",
        ))
        .chain(
            view.offsets
                .as_chunks::<OFFSET_INDEX_ENTRY_BYTES>()
                .0
                .iter()
                .map(move |entry| {
                    if read_u32(entry, 84) != 0 {
                        require_zero(entry, 88..96)?;
                    }
                    validate_location(
                        source,
                        decode_offset_entry(entry, source.segment_id)
                            .location
                            .operation,
                    )?;
                    Ok(OFFSET_INDEX_ENTRY_BYTES)
                }),
        )
        .chain(
            view.messages
                .as_chunks::<MESSAGE_INDEX_ENTRY_BYTES>()
                .0
                .iter()
                .map(move |entry| {
                    require_zero(entry, 72..80)?;
                    let message = decode_message_entry(entry);
                    if binary_search_section(view.offsets, OFFSET_INDEX_ENTRY_BYTES, |entry| {
                        compare_offset_key(entry, message.partition, message.offset)
                    })
                    .is_none()
                    {
                        return Err(IndexFileError::MissingOffsetEntry);
                    }
                    Ok(MESSAGE_INDEX_ENTRY_BYTES)
                }),
        )
        .chain(
            view.operations
                .as_chunks::<OPERATION_INDEX_ENTRY_BYTES>()
                .0
                .iter()
                .map(move |entry| {
                    require_zero(entry, 72..80)?;
                    validate_location(
                        source,
                        decode_operation_entry(entry, source.segment_id).location,
                    )?;
                    Ok(OPERATION_INDEX_ENTRY_BYTES)
                }),
        )
}

fn validate_location(
    source: IndexSource,
    location: OperationLocation,
) -> Result<(), IndexFileError> {
    let end = location
        .entry_offset
        .checked_add(location.entry_bytes)
        .ok_or(IndexFileError::LengthOverflow)?;
    if location.segment_id != source.segment_id
        || location.entry_offset < SEGMENT_HEADER_BYTES as u64
        || !location.entry_offset.is_multiple_of(8)
        || location.entry_bytes < ENTRY_HEADER_BYTES as u64
        || !location.entry_bytes.is_multiple_of(8)
        || end > source.valid_bytes
        || location.op_number < source.first_op_number
        || location.op_number > source.last_op_number
        || (location.op_number == source.last_op_number
            && location.operation_digest != source.last_operation_digest)
    {
        return Err(IndexFileError::InvalidLocation);
    }
    Ok(())
}

pub(crate) fn encode_index_header(
    source: IndexSource,
    build_memory_limit: u64,
    counts: IndexCounts,
    layout: IndexLayout,
) -> Result<[u8; INDEX_HEADER_BYTES], IndexFileError> {
    let mut header = [0_u8; INDEX_HEADER_BYTES];
    header[..8].copy_from_slice(INDEX_MAGIC);
    put_u16(&mut header, 8, INDEX_VERSION);
    put_u16(&mut header, 10, INDEX_HEADER_BYTES as u16);
    header[16..32].copy_from_slice(source.group_id.as_bytes());
    put_u64(&mut header, 32, source.segment_id);
    put_u64(&mut header, 40, source.valid_bytes);
    header[48..80].copy_from_slice(source.segment_digest.as_bytes());
    put_u64(&mut header, 80, source.first_op_number);
    put_u64(&mut header, 88, source.last_op_number);
    header[96..128].copy_from_slice(source.last_operation_digest.as_bytes());
    put_u64(&mut header, 128, u64_from_usize(counts.offsets)?);
    put_u64(&mut header, 136, INDEX_HEADER_BYTES as u64);
    put_u32(&mut header, 144, OFFSET_INDEX_ENTRY_BYTES as u32);
    put_u64(&mut header, 152, u64_from_usize(counts.messages)?);
    put_u64(&mut header, 160, u64_from_usize(layout.message_start)?);
    put_u32(&mut header, 168, MESSAGE_INDEX_ENTRY_BYTES as u32);
    put_u64(&mut header, 176, u64_from_usize(counts.operations)?);
    put_u64(&mut header, 184, u64_from_usize(layout.operation_start)?);
    put_u32(&mut header, 192, OPERATION_INDEX_ENTRY_BYTES as u32);
    put_u64(&mut header, 200, u64_from_usize(layout.file_bytes)?);
    put_u64(&mut header, 208, build_memory_limit);
    Ok(header)
}

fn decode_source(header: &[u8]) -> Result<IndexSource, IndexFileError> {
    Ok(IndexSource {
        group_id: GroupId::from_bytes(array_16(header, 16)?),
        segment_id: read_u64(header, 32),
        valid_bytes: read_u64(header, 40),
        segment_digest: digest_at(header, 48..80),
        first_op_number: read_u64(header, 80),
        last_op_number: read_u64(header, 88),
        last_operation_digest: digest_at(header, 96..128),
    })
}

pub(crate) fn encode_offset_entry(entry: &OffsetIndexEntry, output: &mut [u8]) {
    output[..16].copy_from_slice(entry.partition.as_bytes());
    put_u64(output, 16, entry.offset.get());
    encode_location(entry.location.operation, output, 24);
    put_u32(output, 80, entry.location.batch_index);
    put_u32(output, 84, entry.location.record_index);
    put_u64(output, 88, entry.append_timestamp_millis);
}

fn decode_offset_entry(input: &[u8], segment_id: u64) -> OffsetIndexEntry {
    OffsetIndexEntry {
        partition: PartitionIncarnation::from_bytes(array_16_unchecked(input, 0)),
        offset: Offset::new(read_u64(input, 16)),
        append_timestamp_millis: read_u64(input, 88),
        location: RecordLocation {
            operation: decode_location(input, 24, segment_id),
            batch_index: read_u32(input, 80),
            record_index: read_u32(input, 84),
        },
    }
}

pub(crate) fn encode_message_entry(entry: &MessageIndexEntry, output: &mut [u8]) {
    output[..16].copy_from_slice(entry.partition.as_bytes());
    output[16..32].copy_from_slice(entry.message_id.as_bytes());
    put_u64(output, 32, entry.offset.get());
    output[40..72].copy_from_slice(entry.operation_digest.as_bytes());
}

fn decode_message_entry(input: &[u8]) -> MessageIndexEntry {
    MessageIndexEntry {
        partition: PartitionIncarnation::from_bytes(array_16_unchecked(input, 0)),
        message_id: MessageId::from_bytes(array_16_unchecked(input, 16)),
        offset: Offset::new(read_u64(input, 32)),
        operation_digest: digest_at(input, 40..72),
    }
}

pub(crate) fn encode_operation_entry(entry: &OperationIndexEntry, output: &mut [u8]) {
    output[..16].copy_from_slice(entry.operation_id.as_bytes());
    put_u64(output, 16, entry.location.op_number);
    put_u64(output, 24, entry.location.entry_offset);
    put_u64(output, 32, entry.location.entry_bytes);
    output[40..72].copy_from_slice(entry.location.operation_digest.as_bytes());
}

fn decode_operation_entry(input: &[u8], segment_id: u64) -> OperationIndexEntry {
    OperationIndexEntry {
        operation_id: OperationId::from_bytes(array_16_unchecked(input, 0)),
        location: OperationLocation {
            segment_id,
            op_number: read_u64(input, 16),
            entry_offset: read_u64(input, 24),
            entry_bytes: read_u64(input, 32),
            operation_digest: digest_at(input, 40..72),
        },
    }
}

fn encode_location(location: OperationLocation, output: &mut [u8], start: usize) {
    put_u64(output, start, location.entry_offset);
    put_u64(output, start + 8, location.entry_bytes);
    put_u64(output, start + 16, location.op_number);
    output[start + 24..start + 56].copy_from_slice(location.operation_digest.as_bytes());
}

fn decode_location(input: &[u8], start: usize, segment_id: u64) -> OperationLocation {
    OperationLocation {
        segment_id,
        entry_offset: read_u64(input, start),
        entry_bytes: read_u64(input, start + 8),
        op_number: read_u64(input, start + 16),
        operation_digest: digest_at(input, start + 24..start + 56),
    }
}

pub(crate) fn compare_offset_entries(
    left: &OffsetIndexEntry,
    right: &OffsetIndexEntry,
) -> Ordering {
    compare_partition_offset(left.partition, left.offset, right.partition, right.offset)
}

fn compare_partition_offset(
    left_partition: PartitionIncarnation,
    left_offset: Offset,
    right_partition: PartitionIncarnation,
    right_offset: Offset,
) -> Ordering {
    left_partition
        .as_bytes()
        .cmp(right_partition.as_bytes())
        .then_with(|| left_offset.get().cmp(&right_offset.get()))
}

pub(crate) fn compare_message_entries(
    left: &MessageIndexEntry,
    right: &MessageIndexEntry,
) -> Ordering {
    left.partition
        .as_bytes()
        .cmp(right.partition.as_bytes())
        .then_with(|| left.message_id.as_bytes().cmp(right.message_id.as_bytes()))
        .then_with(|| left.offset.cmp(&right.offset))
}

pub(crate) fn compare_operation_entries(
    left: &OperationIndexEntry,
    right: &OperationIndexEntry,
) -> Ordering {
    left.operation_id
        .as_bytes()
        .cmp(right.operation_id.as_bytes())
}

fn compare_offset_key(entry: &[u8], partition: PartitionIncarnation, offset: Offset) -> Ordering {
    entry[..16]
        .cmp(partition.as_bytes())
        .then_with(|| read_u64(entry, 16).cmp(&offset.get()))
}

fn compare_message_key(
    entry: &[u8],
    partition: PartitionIncarnation,
    message_id: MessageId,
) -> Ordering {
    entry[..16]
        .cmp(partition.as_bytes())
        .then_with(|| entry[16..32].cmp(message_id.as_bytes()))
}

fn binary_search_section(
    section: &[u8],
    width: usize,
    compare: impl Fn(&[u8]) -> Ordering,
) -> Option<&[u8]> {
    let mut left = 0;
    let mut right = section.len() / width;
    while left < right {
        let middle = left + (right - left) / 2;
        let entry = &section[middle * width..(middle + 1) * width];
        match compare(entry) {
            Ordering::Less => left = middle + 1,
            Ordering::Greater => right = middle,
            Ordering::Equal => return Some(entry),
        }
    }
    None
}

fn lower_bound_section(
    section: &[u8],
    width: usize,
    is_less: impl Fn(&[u8]) -> bool,
) -> Option<&[u8]> {
    let mut left = 0;
    let mut right = section.len() / width;
    while left < right {
        let middle = left + (right - left) / 2;
        let entry = &section[middle * width..(middle + 1) * width];
        if is_less(entry) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    (left < section.len() / width).then(|| &section[left * width..(left + 1) * width])
}

fn sorted_entry_checks<'a, T>(
    entries: &'a [T],
    compare: fn(&T, &T) -> Ordering,
    kind: &'static str,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + 'a {
    entries.windows(2).map(move |pair| {
        if compare(&pair[0], &pair[1]) == Ordering::Less {
            Ok(size_of::<T>())
        } else {
            Err(IndexFileError::UnsortedOrDuplicate(kind))
        }
    })
}

fn raw_order_checks<'a>(
    section: &'a [u8],
    width: usize,
    key_bytes: usize,
    kind: &'static str,
) -> impl Iterator<Item = Result<usize, IndexFileError>> + 'a {
    section
        .chunks_exact(width)
        .zip(section.chunks_exact(width).skip(1))
        .map(move |(previous, entry)| {
            if previous[..key_bytes] >= entry[..key_bytes] {
                Err(IndexFileError::UnsortedOrDuplicate(kind))
            } else {
                Ok(width)
            }
        })
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

fn count_from_header(
    header: &[u8],
    offset: usize,
    limit: usize,
    kind: &'static str,
) -> Result<usize, IndexFileError> {
    let count = usize_from_u64(read_u64(header, offset))?;
    enforce_count(kind, count, limit)?;
    Ok(count)
}

fn section_bytes(count: usize, width: usize) -> Result<usize, IndexFileError> {
    count
        .checked_mul(width)
        .ok_or(IndexFileError::LengthOverflow)
}

pub(crate) fn calculate_index_layout(
    counts: IndexCounts,
    limits: IndexLimits,
) -> Result<IndexLayout, IndexFileError> {
    validate_limits(limits)?;
    enforce_count("offset entries", counts.offsets, limits.max_offset_entries)?;
    enforce_count(
        "message entries",
        counts.messages,
        limits.max_message_entries,
    )?;
    enforce_count(
        "operation entries",
        counts.operations,
        limits.max_operation_entries,
    )?;
    if counts.offsets != counts.messages {
        return Err(IndexFileError::RecordIndexCountMismatch);
    }
    let message_start = INDEX_HEADER_BYTES
        .checked_add(section_bytes(counts.offsets, OFFSET_INDEX_ENTRY_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    let operation_start = message_start
        .checked_add(section_bytes(counts.messages, MESSAGE_INDEX_ENTRY_BYTES)?)
        .ok_or(IndexFileError::LengthOverflow)?;
    let file_bytes = operation_start
        .checked_add(section_bytes(
            counts.operations,
            OPERATION_INDEX_ENTRY_BYTES,
        )?)
        .ok_or(IndexFileError::LengthOverflow)?;
    enforce_count("index file bytes", file_bytes, limits.max_file_bytes)?;
    Ok(IndexLayout {
        message_start,
        operation_start,
        file_bytes,
    })
}

fn index_digest(input: &[u8]) -> Digest {
    let mut hasher = Hasher::new(INDEX_HASH_CONTEXT);
    if input.len() >= INDEX_DIGEST_END {
        hasher.update(&input[..INDEX_DIGEST_START]);
        hasher.update(&[0; INDEX_DIGEST_END - INDEX_DIGEST_START]);
        hasher.update(&input[INDEX_DIGEST_END..]);
    } else {
        hasher.update(input);
    }
    hasher.finish()
}

fn require_zero(input: &[u8], range: std::ops::Range<usize>) -> Result<(), IndexFileError> {
    if input[range].iter().any(|byte| *byte != 0) {
        Err(IndexFileError::NonZeroReserved)
    } else {
        Ok(())
    }
}

fn digest_at(input: &[u8], range: std::ops::Range<usize>) -> Digest {
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&input[range]);
    Digest::from_bytes(bytes)
}

fn array_16(input: &[u8], start: usize) -> Result<[u8; 16], IndexFileError> {
    input
        .get(start..start + 16)
        .map(|bytes| {
            let mut output = [0_u8; 16];
            output.copy_from_slice(bytes);
            output
        })
        .ok_or(IndexFileError::InvalidHeader)
}

fn array_16_unchecked(input: &[u8], start: usize) -> [u8; 16] {
    let mut output = [0_u8; 16];
    output.copy_from_slice(&input[start..start + 16]);
    output
}

fn u64_from_usize(value: usize) -> Result<u64, IndexFileError> {
    u64::try_from(value).map_err(|_| IndexFileError::LengthOverflow)
}

fn usize_from_u64(value: u64) -> Result<usize, IndexFileError> {
    usize::try_from(value).map_err(|_| IndexFileError::LengthOverflow)
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(
        input[offset..offset + 2]
            .try_into()
            .expect("validated width"),
    )
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        input[offset..offset + 4]
            .try_into()
            .expect("validated width"),
    )
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("validated width"),
    )
}

/// Immutable index construction, validation, or lookup failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IndexFileError {
    #[error("index file is truncated: need {needed} bytes, have {available}")]
    /// Input bytes end before a complete encoded object.
    Truncated {
        #[doc = "Required bytes for a complete encoded field."]
        needed: usize,
        #[doc = "Available bytes, capacity, or canonical prefix at failure."]
        available: usize,
    },
    #[error("wrong segment index magic")]
    /// The named object has an incorrect format magic.
    WrongMagic,
    #[error("unsupported segment index version {0}")]
    /// The encoded format version is unsupported.
    UnsupportedVersion(u16),
    #[error("invalid segment index header")]
    /// Invalid segment index header.
    InvalidHeader,
    #[error("unsupported segment index flags")]
    /// A field contains unsupported nonzero flags.
    UnsupportedFlags,
    #[error("unsupported segment index entry width")]
    /// Unsupported segment index entry width.
    UnsupportedEntryWidth,
    #[error("nonzero reserved segment index bytes")]
    /// Reserved bytes are nonzero.
    NonZeroReserved,
    #[error("segment index file length is {actual}; expected {expected}")]
    /// Segment index file length is; expected.
    FileLengthMismatch {
        #[doc = "Expected size, count, or fenced field value."]
        expected: usize,
        #[doc = "Observed size, count, or fenced field value."]
        actual: usize,
    },
    #[error("segment index digest mismatch")]
    /// The named object does not match its expected integrity digest.
    DigestMismatch,
    #[error("invalid segment index source identity")]
    /// Invalid segment index source identity.
    InvalidSource,
    #[error("invalid segment index section layout")]
    /// Invalid segment index section layout.
    InvalidSectionLayout,
    #[error("offset and message index counts differ")]
    /// Offset and message index counts differ.
    RecordIndexCountMismatch,
    #[error("{0} index keys are unsorted or duplicated")]
    /// The named object index keys are unsorted or duplicated.
    UnsortedOrDuplicate(&'static str),
    #[error("derived index operation location is invalid")]
    /// Derived index operation location is invalid.
    InvalidLocation,
    #[error("message index entry has no corresponding offset entry")]
    /// Message index entry has no corresponding offset entry.
    MissingOffsetEntry,
    #[error("invalid segment index resource limits")]
    /// Invalid segment index resource limits.
    InvalidLimits,
    #[error("integer or length arithmetic overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    /// The named resource exceeds its configured bound.
    LimitExceeded {
        /// Resource bound that rejected the operation.
        kind: &'static str,
        /// Observed size, count, or fenced field value.
        actual: usize,
        /// Configured maximum for the reported resource.
        limit: usize,
    },
}
