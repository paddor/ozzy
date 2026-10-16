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
    compare_locations, compare_message_entries, compare_offset_entries, compare_operation_entries,
};
pub(crate) use rows::{
    encode_index_header, encode_message_entry, encode_offset_entry, encode_operation_entry,
    encode_operation_location,
};
pub(crate) use sections::{IndexCounts, IndexLayout, calculate_index_layout};
pub use view::SegmentIndexView;

use rows::decode_source;
use validate::{section_checks, validate_image, validate_location_count, validate_source};

use crate::{MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry, OperationLocation};

const INDEX_MAGIC: &[u8; 8] = b"OZYIDX01";
const INDEX_VERSION: u16 = 3;
pub(crate) const INDEX_HASH_CONTEXT: &str = "ozzy segment index file v1";
pub(crate) const INDEX_DIGEST_START: usize = 256;

/// Exact encoded derived-index header length.
pub const INDEX_HEADER_BYTES: usize = 4096;
/// Exact encoded record-offset selector length.
pub const OFFSET_INDEX_ENTRY_BYTES: usize = 96;
/// Exact encoded message-identity selector length.
pub const MESSAGE_INDEX_ENTRY_BYTES: usize = 80;
/// Exact encoded canonical operation selector length.
pub const OPERATION_INDEX_ENTRY_BYTES: usize = 80;
/// Exact encoded numeric operation selector length.
pub const OPERATION_LOCATION_BYTES: usize = 56;

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
    entries: IndexEntries,
}

/// Selectors for one index image, one vector per section, in any order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexEntries {
    /// Record-offset selectors.
    pub offsets: Vec<OffsetIndexEntry>,
    /// Message-identity selectors.
    pub messages: Vec<MessageIndexEntry>,
    /// Canonical operation-identity selectors.
    pub operations: Vec<OperationIndexEntry>,
    /// Numeric selectors, one per canonical operation.
    pub locations: Vec<OperationLocation>,
}

impl IndexEntries {
    /// An image of these entries, each section sorted, before validation.
    fn sorted_image(mut self, source: IndexSource, build_memory_limit: u64) -> SegmentIndexImage {
        self.offsets.sort_unstable_by(compare_offset_entries);
        self.messages.sort_unstable_by(compare_message_entries);
        self.operations.sort_unstable_by(compare_operation_entries);
        self.locations.sort_unstable_by(compare_locations);
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

    /// Record-offset selectors in canonical sorted order.
    pub fn offsets(&self) -> &[OffsetIndexEntry] {
        &self.entries.offsets
    }

    /// Message-identity selectors in canonical sorted order.
    pub fn messages(&self) -> &[MessageIndexEntry] {
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

    /// Exact operation in this validated dense numeric prefix.
    pub fn find_operation_number(&self, number: u64) -> Option<OperationLocation> {
        let at = usize::try_from(number.checked_sub(self.source.first_op_number)?).ok()?;
        self.entries.locations.get(at).copied()
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
        image.offsets().len(),
        limits.max_offset_entries,
    )?;
    enforce_count(
        "message entries",
        image.messages().len(),
        limits.max_message_entries,
    )?;
    enforce_count(
        "operation entries",
        image.operations().len(),
        limits.max_operation_entries,
    )?;

    let counts = IndexCounts {
        offsets: image.offsets().len(),
        messages: image.messages().len(),
        operations: image.operations().len(),
        locations: image.locations().len(),
    };
    let layout = calculate_index_layout(counts, limits)?;

    let mut output = vec![0_u8; layout.file_bytes];
    let (header, sections) = layout
        .split_mut(&mut output)
        .ok_or(IndexFileError::InvalidSectionLayout)?;
    *header = encode_index_header(image.source, image.build_memory_limit, counts, layout)?;
    let offsets = sections
        .offsets
        .as_chunks_mut::<OFFSET_INDEX_ENTRY_BYTES>()
        .0;
    for (entry, target) in image.offsets().iter().zip(offsets) {
        encode_offset_entry(entry, target);
    }
    let messages = sections
        .messages
        .as_chunks_mut::<MESSAGE_INDEX_ENTRY_BYTES>()
        .0;
    for (entry, target) in image.messages().iter().zip(messages) {
        encode_message_entry(entry, target);
    }
    let operations = sections
        .operations
        .as_chunks_mut::<OPERATION_INDEX_ENTRY_BYTES>()
        .0;
    for (entry, target) in image.operations().iter().zip(operations) {
        encode_operation_entry(entry, target);
    }
    let locations = sections
        .locations
        .as_chunks_mut::<OPERATION_LOCATION_BYTES>()
        .0;
    for (entry, target) in image.locations().iter().zip(locations) {
        encode_operation_location(entry, target);
    }
    let digest = index_digest(&output);
    layout::patch(&mut output, INDEX_DIGEST_START, *digest.as_bytes())?;
    Ok(output)
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
    require_zero(header, 288..INDEX_HEADER_BYTES)?;
    if header.u32_at::<144>() != OFFSET_INDEX_ENTRY_BYTES as u32
        || header.u32_at::<168>() != MESSAGE_INDEX_ENTRY_BYTES as u32
        || header.u32_at::<192>() != OPERATION_INDEX_ENTRY_BYTES as u32
        || header.u32_at::<232>() != OPERATION_LOCATION_BYTES as u32
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
    let counts = IndexCounts {
        offsets: usize_from_u64(header.u64_at::<128>())?,
        messages: usize_from_u64(header.u64_at::<152>())?,
        operations: usize_from_u64(header.u64_at::<176>())?,
        locations: usize_from_u64(header.u64_at::<216>())?,
    };
    let expected = calculate_index_layout(counts, limits)?;
    validate_location_count(source, counts.locations)?;
    // The header states each section start; all must match the counts.
    let stated = IndexLayout {
        message_start: usize_from_u64(header.u64_at::<160>())?,
        operation_start: usize_from_u64(header.u64_at::<184>())?,
        location_start: usize_from_u64(header.u64_at::<224>())?,
        file_bytes: input.len(),
    };
    if usize_from_u64(header.u64_at::<136>())? != INDEX_HEADER_BYTES || stated != expected {
        return Err(IndexFileError::InvalidSectionLayout);
    }
    let (_, sections) = expected
        .split(input)
        .ok_or(IndexFileError::InvalidSectionLayout)?;
    Ok(SegmentIndexView::new(
        source,
        header.u64_at::<208>(),
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
