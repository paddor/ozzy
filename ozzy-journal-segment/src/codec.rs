use std::borrow::Cow;
use std::ops::Range;

use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::{
    CanonicalOperation, ChainPosition, Digest, OperationKind, canonical_body_digest,
    logical_operation_digest_with_body_digest,
};
use ozzy_proto::GroupId;
use smallvec::SmallVec;
use thiserror::Error;

mod extents;
pub(crate) use extents::prepare_raw_group_extents;
mod packed;
mod scanning;
use scanning::scan;
pub(crate) use scanning::{scan_segment_async, scan_segment_prefix_async};
mod shared;
use ozzy_journal::operation::OperationHeader;
pub(crate) use packed::PackedOperation;
pub(crate) use shared::prepare_shared_group_bodies;

#[cfg(test)]
mod tests;

/// Exact encoded physical segment header length.
pub const SEGMENT_HEADER_BYTES: usize = 4 * 1024;
/// Exact encoded canonical operation entry header length.
pub const ENTRY_HEADER_BYTES: usize = 192;
/// Exact encoded physical write-group seal length.
pub const GROUP_SEAL_BYTES: usize = 96;
/// Required byte alignment of physical write groups.
pub const WRITE_GROUP_ALIGNMENT: usize = 4 * 1024;

/// Body representation selected by one storage writer. Deferred writes collect
/// operation bodies into one shared block; synchronous writes encode separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyEncoding {
    /// Store canonical operation bodies without physical compression.
    Raw,
    /// Use physical LZ4 only when its encoded representation meets the savings bound.
    Lz4 {
        #[doc = "Minimum encoded byte savings required before physical compression is used."]
        min_savings_bytes: usize,
    },
}

impl BodyEncoding {
    /// Whether this build includes the requested encoder. This does not validate
    /// codec-specific settings or imply that existing stored bytes are readable.
    pub const fn is_supported(self) -> bool {
        match self {
            Self::Raw => true,
            Self::Lz4 { .. } => cfg!(feature = "lz4"),
        }
    }
}

const FORMAT_VERSION: u16 = 3;
const SEGMENT_MAGIC: &[u8; 8] = b"OZYSEG\0\0";
const ENTRY_MAGIC: &[u8; 4] = b"OZJE";
const SEAL_MAGIC: &[u8; 8] = b"OZYSEAL\0";

const SEGMENT_HEADER_HASH_CONTEXT: &str = "ozzy journal segment header v2";
const SEGMENT_HASH_CONTEXT: &str = "ozzy journal structural segment v2";
const ENTRY_HEADER_HASH_CONTEXT: &str = "ozzy journal entry header v2";
const GROUP_HASH_CONTEXT: &str = "ozzy journal structural group v2";

const SEGMENT_DIGEST_RANGE: Range<usize> = 88..120;
const ENTRY_DIGEST_RANGE: Range<usize> = 136..168;
const SEAL_DIGEST_RANGE: Range<usize> = 40..72;

/// Immutable identity and capacity of one physical segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    group_id: GroupId,
    segment_id: u64,
    file_generation: u64,
    predecessor_segment_id: Option<u64>,
    predecessor_digest: Digest,
    capacity: u64,
}

impl SegmentHeader {
    /// Validate and construct a segment header.
    pub fn new(
        group_id: GroupId,
        segment_id: u64,
        predecessor_segment_id: Option<u64>,
        predecessor_digest: Digest,
        capacity: u64,
    ) -> Result<Self, CodecError> {
        if segment_id == 0 {
            return Err(CodecError::InvalidSegmentId);
        }
        let predecessor_is_valid = match predecessor_segment_id {
            None => predecessor_digest == Digest::ZERO,
            Some(id) => id != 0 && predecessor_digest != Digest::ZERO,
        };
        if !predecessor_is_valid {
            return Err(CodecError::InvalidPredecessor);
        }
        validate_segment_capacity(capacity)?;
        Ok(Self {
            group_id,
            segment_id,
            file_generation: 0,
            predecessor_segment_id,
            predecessor_digest,
            capacity,
        })
    }

    /// Persistent replication-group identity.
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Physical segment identity.
    pub const fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Physical replacement incarnation; zero names the original file.
    pub const fn file_generation(&self) -> u64 {
        self.file_generation
    }

    pub(crate) fn with_file_generation(mut self, generation: u64) -> Self {
        self.file_generation = generation;
        self
    }

    /// Previous physical segment ID, or none at genesis.
    pub const fn predecessor_segment_id(&self) -> Option<u64> {
        self.predecessor_segment_id
    }

    /// Canonical history digest immediately before this segment, not the
    /// predecessor file's physical checksum. Genesis has the zero digest.
    pub const fn predecessor_digest(&self) -> Digest {
        self.predecessor_digest
    }

    /// Configured physical segment capacity in bytes.
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }
}

pub(crate) fn validate_segment_capacity(capacity: u64) -> Result<(), CodecError> {
    if capacity < (SEGMENT_HEADER_BYTES + WRITE_GROUP_ALIGNMENT) as u64
        || !capacity.is_multiple_of(WRITE_GROUP_ALIGNMENT as u64)
    {
        return Err(CodecError::InvalidSegmentCapacity(capacity));
    }
    Ok(())
}

/// Bounds enforced before accepting declared entry lengths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Maximum physical write groups decoded in one segment.
    pub max_groups: usize,
    /// Maximum canonical operation entries decoded in one physical group.
    pub max_entries: usize,
    /// Maximum encoded bytes per physical operation entry.
    pub max_entry_bytes: usize,
    /// Maximum decoded canonical bytes per operation body.
    pub max_decoded_body_bytes: usize,
    /// Maximum combined decoded canonical bytes per physical group.
    pub max_group_decoded_body_bytes: usize,
    /// Maximum combined decoded canonical bytes per segment.
    pub max_segment_decoded_body_bytes: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_groups: 65_536,
            max_entries: 65_536,
            max_entry_bytes: 64 * 1024 * 1024,
            max_decoded_body_bytes: 64 * 1024 * 1024,
            max_group_decoded_body_bytes: 256 * 1024 * 1024,
            max_segment_decoded_body_bytes: 1024 * 1024 * 1024,
        }
    }
}

impl DecodeLimits {
    pub(crate) fn group_metadata_bytes(self) -> Option<usize> {
        // Parsing grows a Vec geometrically; decoded operations coexist with
        // its consuming iterator until the final entry has been checked.
        self.max_entries
            .checked_mul(2 * size_of::<ParsedEntry>() + size_of::<DecodedOperation<'_>>())
    }

    /// Maximum indexed extent, including a shared compressed body and its
    /// uncompressed operation headers. Individual operations keep their limits.
    pub fn max_indexed_entry_bytes(self) -> usize {
        self.max_entry_bytes.max(
            self.max_entries
                .saturating_mul(ENTRY_HEADER_BYTES)
                .saturating_add(shared::HEADER_BYTES)
                .saturating_add(self.max_group_decoded_body_bytes)
                .saturating_add(7),
        )
    }
}

/// Encoded physical group ready for a short-write-safe file writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedGroup {
    bytes: Vec<u8>,
    decoded_body_bytes: usize,
    digest: Digest,
    group_number: u64,
    start_offset: u64,
    end_offset: u64,
    entry_count: u64,
    next_chain: ChainPosition,
}

pub(crate) const INLINE_GROUP_OPERATIONS: usize = 64;

#[derive(Debug)]
pub(crate) struct PreparedGroupBodies {
    bytes: Vec<u8>,
    entries: SmallVec<[PreparedEntry; INLINE_GROUP_OPERATIONS]>,
    decoded_body_bytes: usize,
    entry_bytes: usize,
    borrowed_raw: bool,
}

#[derive(Debug, Clone, Copy)]
struct PreparedEntry {
    start: usize,
    header_start: usize,
    encoded_len: usize,
    decoded_len: usize,
    codec: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedEntryLayout {
    pub(crate) entry_offset: u64,
    pub(crate) entry_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FinalizedGroup {
    pub(crate) digest: Digest,
    pub(crate) group_number: u64,
    start_offset: u64,
    pub(crate) end_offset: u64,
    entry_count: u64,
    pub(crate) next_chain: ChainPosition,
}

impl PreparedGroupBodies {
    pub(crate) fn validate_decode_limits(&self, limits: DecodeLimits) -> Result<(), CodecError> {
        enforce_limit("entry count", self.entries.len(), limits.max_entries)?;
        enforce_limit(
            "physical group decoded body bytes",
            self.decoded_body_bytes,
            limits.max_group_decoded_body_bytes,
        )?;
        for entry in &self.entries {
            enforce_limit(
                "entry bytes",
                encoded_entry_len(entry.encoded_len)?,
                limits.max_entry_bytes,
            )?;
            enforce_limit(
                "decoded body bytes",
                entry.decoded_len,
                limits.max_decoded_body_bytes,
            )?;
        }
        Ok(())
    }

    pub(crate) fn require_capacity(&self, start: u64, capacity: u64) -> Result<(), CodecError> {
        let bytes = aligned_group_bytes(self.entry_bytes)?;
        let end = start
            .checked_add(u64::try_from(bytes).map_err(|_| CodecError::LengthOverflow)?)
            .ok_or(CodecError::LengthOverflow)?;
        if end > capacity {
            return Err(CodecError::GroupExceedsSegment);
        }
        Ok(())
    }
    pub(crate) fn into_owned_bytes(self) -> Vec<u8> {
        assert!(
            !self.borrowed_raw,
            "borrowed extents cannot become owned bytes"
        );
        self.bytes
    }

    pub(crate) fn entry_layouts(
        &self,
        group_start_offset: u64,
    ) -> Result<SmallVec<[PreparedEntryLayout; 8]>, CodecError> {
        if self
            .entries
            .first()
            .is_some_and(|entry| entry.codec == shared::CODEC)
        {
            return Ok(std::iter::repeat_n(
                PreparedEntryLayout {
                    entry_offset: group_start_offset,
                    entry_bytes: self.entry_bytes as u64,
                },
                self.entries.len(),
            )
            .collect());
        }
        self.entries
            .iter()
            .map(|entry| {
                let relative =
                    u64::try_from(entry.start).map_err(|_| CodecError::LengthOverflow)?;
                let entry_bytes = u64::try_from(encoded_entry_len(entry.encoded_len)?)
                    .map_err(|_| CodecError::LengthOverflow)?;
                let entry_offset = group_start_offset
                    .checked_add(relative)
                    .ok_or(CodecError::LengthOverflow)?;
                Ok(PreparedEntryLayout {
                    entry_offset,
                    entry_bytes,
                })
            })
            .collect()
    }

    pub(crate) fn finish(self, finalized: FinalizedGroup) -> EncodedGroup {
        assert!(
            !self.borrowed_raw,
            "borrowed extents cannot become owned bytes"
        );
        EncodedGroup {
            bytes: self.bytes,
            decoded_body_bytes: self.decoded_body_bytes,
            digest: finalized.digest,
            group_number: finalized.group_number,
            start_offset: finalized.start_offset,
            end_offset: finalized.end_offset,
            entry_count: finalized.entry_count,
            next_chain: finalized.next_chain,
        }
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub(crate) fn decoded_body_bytes(&self) -> usize {
        self.decoded_body_bytes
    }
}

impl EncodedGroup {
    /// Borrow the exact encoded physical representation.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Take ownership of the exact encoded physical representation.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Combined canonical body bytes after physical decoding.
    pub const fn decoded_body_bytes(&self) -> usize {
        self.decoded_body_bytes
    }

    /// Integrity digest of this exact encoded or selected object.
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Consecutive physical write-group number.
    pub const fn group_number(&self) -> u64 {
        self.group_number
    }

    /// First physical byte offset of this write group.
    pub const fn start_offset(&self) -> u64 {
        self.start_offset
    }

    /// Exclusive physical byte offset after this write group.
    pub const fn end_offset(&self) -> u64 {
        self.end_offset
    }

    /// Number of canonical operation entries in this physical group.
    pub const fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Next canonical operation number and predecessor digest.
    pub const fn next_chain(&self) -> ChainPosition {
        self.next_chain
    }
}

/// One decoded canonical operation borrowing its raw body from a group buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedOperation<'a> {
    /// Absolute byte offset of this entry or shared compressed extent.
    pub entry_offset: u64,
    /// Complete aligned extent length, excluding physical-group padding/seal.
    pub entry_bytes: u64,
    /// Persistent partition replication-group identity.
    pub group_id: GroupId,
    /// Selected membership/configuration epoch.
    pub configuration_epoch: u64,
    /// Original canonical admission view, preserved during repair.
    pub original_view: u64,
    /// Partition-local canonical operation number.
    pub op_number: u64,
    /// Canonical chain digest immediately before this operation.
    pub previous_digest: Digest,
    /// Integrity digest bound to this exact object or canonical prefix.
    pub digest: Digest,
    /// Body digest checked during decoding. Mutating `body` invalidates it.
    pub body_digest: Digest,
    /// Canonical operation discriminator.
    pub kind: OperationKind,
    /// Canonical body backing, borrowed or owned after physical decoding.
    pub body: Cow<'a, [u8]>,
}

/// One complete, physically and logically validated group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedGroup<'a> {
    /// Consecutive physical write-group number.
    pub group_number: u64,
    /// First physical byte offset of this group.
    pub start_offset: u64,
    /// Exclusive physical byte offset after this group.
    pub end_offset: u64,
    /// Structural digest derived from the validated logical operation digests.
    pub digest: Digest,
    /// Validated canonical operation entries in group order.
    pub operations: Vec<DecodedOperation<'a>>,
    /// Next canonical operation number and predecessor digest.
    pub next_chain: ChainPosition,
}

impl DecodedGroup<'_> {
    /// Physical bytes occupied by the validated complete group prefix.
    pub fn consumed_bytes(&self) -> usize {
        usize::try_from(self.end_offset - self.start_offset)
            .expect("validated physical group length fits usize")
    }
}

/// Complete groups and the first unusable byte found during segment recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentScan<'a> {
    /// Validated physical segment or canonical operation header.
    pub header: SegmentHeader,
    /// Integrity digest bound to this exact object or canonical prefix.
    pub digest: Digest,
    /// Validated complete physical groups in segment order.
    pub groups: Vec<DecodedGroup<'a>>,
    /// Combined canonical body bytes after physical decoding.
    pub decoded_body_bytes: usize,
    /// Physical bytes covered by the exact validated segment prefix.
    pub valid_bytes: u64,
    /// Next consecutive physical write-group number.
    pub next_group_number: u64,
    /// Next canonical operation number and predecessor digest.
    pub next_chain: ChainPosition,
    /// Classification of bytes after the validated physical prefix.
    pub tail: TailState,
}

/// Classification of bytes after the last complete physical group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TailState {
    /// File ends exactly at a complete group boundary.
    Clean,
    /// Preallocated or otherwise unused zero bytes follow the valid prefix.
    ZeroFilled {
        #[doc = "Physical bytes in the unvalidated segment tail."]
        bytes: usize,
    },
    /// A sequential append ended before its complete seal was present.
    Truncated {
        #[doc = "Physical bytes in the unvalidated segment tail."]
        bytes: usize,
        #[doc = "Physical format or integrity error classifying this tail."]
        cause: CodecError,
    },
    /// Only from recovery of an active segment: a group after the valid prefix
    /// failed to decode. `bytes` runs to the end of the input.
    Damaged {
        #[doc = "Physical bytes in the unvalidated segment tail."]
        bytes: usize,
        #[doc = "Physical format or integrity error classifying this tail."]
        cause: CodecError,
    },
}

/// Physical format validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    #[error("segment ID zero is reserved")]
    /// Segment ID zero is reserved.
    InvalidSegmentId,
    #[error("invalid predecessor segment identity")]
    /// Invalid predecessor segment identity.
    InvalidPredecessor,
    #[error("invalid aligned segment capacity {0}")]
    /// Invalid aligned segment capacity.
    InvalidSegmentCapacity(u64),
    #[error("truncated {object}: need {needed} bytes, have {available}")]
    /// Input bytes end before a complete encoded object.
    Truncated {
        /// Named physical or encoded object that failed validation.
        object: &'static str,
        /// Required bytes for a complete encoded field.
        needed: usize,
        /// Available bytes, capacity, or canonical prefix at failure.
        available: usize,
    },
    #[error("wrong {0} magic")]
    /// The named object has an incorrect format magic.
    WrongMagic(&'static str),
    #[error("unsupported disk format version {0}")]
    /// The encoded format version is unsupported.
    UnsupportedVersion(u16),
    #[error("unsupported operation kind {0}")]
    /// The canonical operation discriminator is unsupported.
    UnsupportedOperationKind(u16),
    #[error("unsupported entry codec {0}")]
    /// The physical body codec is unsupported.
    UnsupportedCodec(u16),
    #[error("{0} body compression failed")]
    /// Canonical body compression failed.
    CompressionFailed(&'static str),
    #[error("{0} compression scratch allocation failed")]
    /// Bounded compression scratch storage could not grow.
    CompressionScratchAllocation(&'static str),
    #[error("{0} body decompression failed")]
    /// The compressed body is malformed or has the wrong decoded size.
    DecompressionFailed(&'static str),
    #[error("unsupported nonzero flags in {0}")]
    /// A field contains unsupported nonzero flags.
    UnsupportedFlags(&'static str),
    #[error("nonzero reserved bytes in {0}")]
    /// Reserved bytes are nonzero.
    NonZeroReserved(&'static str),
    #[error("{0} digest mismatch")]
    /// The named object does not match its expected integrity digest.
    DigestMismatch(&'static str),
    #[error("integer or length arithmetic overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
    #[error("physical group must contain at least one operation")]
    /// Physical group must contain at least one operation.
    EmptyGroup,
    #[error("canonical body digest count does not match operation count")]
    /// Canonical body digest count does not match operation count.
    BodyDigestCountMismatch,
    #[error("prepared bodies do not match canonical operations")]
    /// Prepared bodies do not match canonical operations.
    PreparedBodyMismatch,
    #[error("physical group number zero is reserved")]
    /// Physical group number zero is reserved.
    InvalidGroupNumber,
    #[error("physical group offset {0} is not valid or aligned")]
    /// Physical group offset is not valid or aligned.
    InvalidGroupOffset(u64),
    #[error("operation entry offset {0} is not valid or aligned")]
    /// Operation entry offset is not valid or aligned.
    InvalidEntryOffset(u64),
    #[error("operation belongs to another group")]
    /// Operation belongs to another group.
    WrongGroup,
    #[error("operation chain expected op {expected_op}")]
    /// Canonical operation numbers or predecessor digests are not consecutive.
    ChainMismatch {
        #[doc = "Next required canonical operation number."]
        expected_op: u64,
    },
    #[error("operation number space exhausted")]
    /// Operation number space exhausted.
    OperationNumberExhausted,
    #[error("entry has invalid length fields")]
    /// Entry has invalid length fields.
    InvalidEntryLength,
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
    #[error("physical group exceeds segment capacity")]
    /// Physical group exceeds segment capacity.
    GroupExceedsSegment,
    #[error("segment decoded body bytes exceed limit: {actual} > {limit}")]
    /// Segment decoded body bytes exceed limit.
    SegmentDecodedBodyLimit {
        #[doc = "Observed size, count, or fenced field value."]
        actual: usize,
        #[doc = "Configured maximum for the reported resource."]
        limit: usize,
    },
    #[error("segment file length exceeds configured capacity")]
    /// Segment file length exceeds configured capacity.
    SegmentExceedsCapacity,
    #[error("nonzero entry or physical-group padding")]
    /// Nonzero entry or physical-group padding.
    NonZeroPadding,
    #[error("physical seal metadata does not match scanned group")]
    /// Physical seal metadata does not match scanned group.
    SealMismatch,
}

#[derive(Debug, Clone)]
struct ParsedEntry {
    group_id: GroupId,
    configuration_epoch: u64,
    original_view: u64,
    op_number: u64,
    previous_digest: Digest,
    body_digest: Digest,
    kind: OperationKind,
    body: Range<usize>,
    decoded_body_bytes: usize,
    codec: u16,
    end: usize,
}

#[derive(Debug)]
struct ParsedGroup {
    entries: Vec<ParsedEntry>,
    shared: Option<shared::Body>,
    end_offset: u64,
    digest: Digest,
}

/// Encode the fixed-size physical segment header and integrity fields.
pub fn encode_segment_header(header: &SegmentHeader) -> [u8; SEGMENT_HEADER_BYTES] {
    let mut output = [0_u8; SEGMENT_HEADER_BYTES];
    output[0..8].copy_from_slice(SEGMENT_MAGIC);
    put_u16(&mut output, 8, FORMAT_VERSION);
    put_u16(&mut output, 10, SEGMENT_HEADER_BYTES as u16);
    put_u32(&mut output, 12, 0);
    output[16..32].copy_from_slice(header.group_id.as_bytes());
    put_u64(&mut output, 32, header.segment_id);
    put_u64(
        &mut output,
        40,
        header.predecessor_segment_id.unwrap_or_default(),
    );
    output[48..80].copy_from_slice(header.predecessor_digest.as_bytes());
    put_u64(&mut output, 80, header.capacity);
    put_u64(&mut output, 120, header.file_generation);
    let digest = hash_with_zeroed_range(SEGMENT_HEADER_HASH_CONTEXT, &output, SEGMENT_DIGEST_RANGE);
    output[SEGMENT_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
    output
}

/// Validate and decode the fixed-size physical segment header.
pub fn decode_segment_header(input: &[u8]) -> Result<SegmentHeader, CodecError> {
    require_len(input, SEGMENT_HEADER_BYTES, "segment header")?;
    let input = &input[..SEGMENT_HEADER_BYTES];
    if &input[0..8] != SEGMENT_MAGIC {
        return Err(CodecError::WrongMagic("segment header"));
    }
    require_version(read_u16(input, 8))?;
    if read_u16(input, 10) != SEGMENT_HEADER_BYTES as u16 {
        return Err(CodecError::InvalidEntryLength);
    }
    if read_u32(input, 12) != 0 {
        return Err(CodecError::UnsupportedFlags("segment header"));
    }
    let expected = digest_at(input, SEGMENT_DIGEST_RANGE.clone());
    let actual = hash_with_zeroed_range(SEGMENT_HEADER_HASH_CONTEXT, input, SEGMENT_DIGEST_RANGE);
    if expected != actual {
        return Err(CodecError::DigestMismatch("segment header"));
    }
    let group_id = GroupId::from_bytes(array_16(input, 16));
    let segment_id = read_u64(input, 32);
    let predecessor_id = read_u64(input, 40);
    let predecessor_digest = digest_at(input, 48..80);
    let predecessor_segment_id = (predecessor_id != 0).then_some(predecessor_id);
    if !all_zero(&input[128..]) {
        return Err(CodecError::NonZeroReserved("segment header"));
    }
    SegmentHeader::new(
        group_id,
        segment_id,
        predecessor_segment_id,
        predecessor_digest,
        read_u64(input, 80),
    )
    .map(|header| header.with_file_generation(read_u64(input, 120)))
}

/// Encode consecutive canonical operations as one aligned physical write group.
pub fn encode_group(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    expected_chain: ChainPosition,
    operations: &[CanonicalOperation<'_>],
) -> Result<EncodedGroup, CodecError> {
    encode_group_with_body_encoding(
        segment,
        group_number,
        start_offset,
        expected_chain,
        operations,
        BodyEncoding::Raw,
    )
}

/// Encode one physical group using independently decodable operation bodies.
pub fn encode_group_with_body_encoding(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    expected_chain: ChainPosition,
    operations: &[CanonicalOperation<'_>],
    encoding: BodyEncoding,
) -> Result<EncodedGroup, CodecError> {
    let body_digests = operations
        .iter()
        .map(|operation| canonical_body_digest(operation.body))
        .collect::<Vec<_>>();
    encode_group_with_body_encoding_and_digests(
        segment,
        group_number,
        start_offset,
        expected_chain,
        operations,
        &body_digests,
        encoding,
    )
}

pub(crate) fn encode_group_with_body_encoding_and_digests(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    expected_chain: ChainPosition,
    operations: &[CanonicalOperation<'_>],
    body_digests: &[Digest],
    encoding: BodyEncoding,
) -> Result<EncodedGroup, CodecError> {
    let mut scratch = BodyEncodeScratch::default();
    encode_group_reusing_buffer(
        segment,
        group_number,
        start_offset,
        expected_chain,
        operations,
        body_digests,
        encoding,
        Vec::new(),
        &mut scratch,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "physical group identity and caller-owned scratch are independent inputs"
)]
pub(crate) fn encode_group_reusing_buffer(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    expected_chain: ChainPosition,
    operations: &[CanonicalOperation<'_>],
    body_digests: &[Digest],
    encoding: BodyEncoding,
    bytes: Vec<u8>,
    scratch: &mut BodyEncodeScratch,
) -> Result<EncodedGroup, CodecError> {
    let mut prepared = prepare_group_bodies(
        operations.iter().map(|operation| operation.body),
        encoding,
        bytes,
        scratch,
    )?;
    let finalized = finalize_group_bodies(
        segment,
        group_number,
        start_offset,
        expected_chain,
        operations,
        body_digests,
        &mut prepared,
    )?;
    Ok(prepared.finish(finalized))
}

pub(crate) fn prepare_group_bodies<'a>(
    bodies: impl Iterator<Item = &'a [u8]> + Clone,
    encoding: BodyEncoding,
    mut bytes: Vec<u8>,
    scratch: &mut BodyEncodeScratch,
) -> Result<PreparedGroupBodies, CodecError> {
    let encoding = group_body_encoding(bodies.clone(), encoding)?;
    bytes.clear();
    let mut entries = SmallVec::new();
    let mut decoded_body_bytes = 0_usize;
    for body in bodies {
        decoded_body_bytes = decoded_body_bytes
            .checked_add(body.len())
            .ok_or(CodecError::LengthOverflow)?;
        let encoded = encode_body(body, encoding, scratch)?;
        let encoded_len = encoded.bytes.len();
        let total_len = encoded_entry_len(encoded_len)?;
        let start = bytes.len();
        let end = start
            .checked_add(total_len)
            .ok_or(CodecError::LengthOverflow)?;
        bytes.resize(end, 0);
        bytes[start + ENTRY_HEADER_BYTES..start + ENTRY_HEADER_BYTES + encoded_len]
            .copy_from_slice(&encoded.bytes);
        entries.push(PreparedEntry {
            start,
            header_start: start,
            encoded_len,
            decoded_len: body.len(),
            codec: encoded.codec,
        });
    }
    if entries.is_empty() {
        return Err(CodecError::EmptyGroup);
    }
    Ok(PreparedGroupBodies {
        entry_bytes: bytes.len(),
        borrowed_raw: false,
        bytes,
        entries,
        decoded_body_bytes,
    })
}

fn group_body_encoding<'a>(
    mut bodies: impl Iterator<Item = &'a [u8]>,
    requested: BodyEncoding,
) -> Result<BodyEncoding, CodecError> {
    if requested == BodyEncoding::Raw {
        return Ok(requested);
    }
    // Even perfect compression cannot save a write-alignment unit below this
    // bound. Check the complete group, since several small bodies can cross it.
    let (raw_entries, minimum_entries) =
        bodies.try_fold((0_usize, 0_usize), |(raw, minimum), body| {
            let raw = raw
                .checked_add(encoded_entry_len(body.len())?)
                .ok_or(CodecError::LengthOverflow)?;
            let minimum = minimum
                .checked_add(encoded_entry_len(0)?)
                .ok_or(CodecError::LengthOverflow)?;
            Ok::<_, CodecError>((raw, minimum))
        })?;
    let raw_group = aligned_group_bytes(raw_entries)?;
    let minimum_group = aligned_group_bytes(minimum_entries)?;
    Ok(if raw_group == minimum_group {
        BodyEncoding::Raw
    } else {
        requested
    })
}

fn aligned_group_bytes(entry_bytes: usize) -> Result<usize, CodecError> {
    align_up(
        entry_bytes
            .checked_add(GROUP_SEAL_BYTES)
            .ok_or(CodecError::LengthOverflow)?,
        WRITE_GROUP_ALIGNMENT,
    )
}

pub(crate) fn finalize_group_bodies(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    expected_chain: ChainPosition,
    operations: &[CanonicalOperation<'_>],
    body_digests: &[Digest],
    prepared: &mut PreparedGroupBodies,
) -> Result<FinalizedGroup, CodecError> {
    if operations.len() != body_digests.len() {
        return Err(CodecError::BodyDigestCountMismatch);
    }
    finalize_group_descriptors(
        segment,
        group_number,
        start_offset,
        expected_chain,
        operations
            .iter()
            .zip(body_digests)
            .map(|(operation, digest)| PackedOperation {
                header: operation.header(),
                body_bytes: operation.body.len(),
                body_digest: *digest,
            }),
        prepared,
    )
}

pub(crate) fn finalize_group_descriptors(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    expected_chain: ChainPosition,
    operations: impl ExactSizeIterator<Item = PackedOperation>,
    prepared: &mut PreparedGroupBodies,
) -> Result<FinalizedGroup, CodecError> {
    validate_group_position(group_number, start_offset)?;
    let count = operations.len();
    if count == 0 {
        return Err(CodecError::EmptyGroup);
    }
    if count != prepared.entries.len() {
        return Err(CodecError::BodyDigestCountMismatch);
    }

    let mut chain = expected_chain;
    let mut group_digest = GroupDigestBuilder::new(segment, group_number, start_offset, count)?;
    for (description, entry) in operations.zip(&prepared.entries) {
        let operation = &description.header;
        validate_operation(segment, chain, operation)?;
        if description.body_bytes != entry.decoded_len {
            return Err(CodecError::PreparedBodyMismatch);
        }
        encode_entry_header(
            &mut prepared.bytes,
            *entry,
            operation,
            description.body_digest,
        )?;
        let digest = operation.digest(description.body_digest);
        group_digest.push(digest);
        chain = ChainPosition::new(
            operation
                .op_number
                .checked_add(1)
                .ok_or(CodecError::OperationNumberExhausted)?,
            digest,
        );
    }

    let group_bytes = align_up(
        prepared
            .entry_bytes
            .checked_add(GROUP_SEAL_BYTES)
            .ok_or(CodecError::LengthOverflow)?,
        WRITE_GROUP_ALIGNMENT,
    )?;
    let end_offset = start_offset
        .checked_add(u64::try_from(group_bytes).map_err(|_| CodecError::LengthOverflow)?)
        .ok_or(CodecError::LengthOverflow)?;
    if end_offset > segment.capacity {
        return Err(CodecError::GroupExceedsSegment);
    }
    let buffer_bytes = prepared.bytes.len() + (group_bytes - prepared.entry_bytes);
    let seal_offset = buffer_bytes - GROUP_SEAL_BYTES;
    prepared.bytes.resize(buffer_bytes, 0);
    let entry_count = u64::try_from(count).map_err(|_| CodecError::LengthOverflow)?;
    let digest = group_digest.finish(end_offset);
    encode_seal(
        &mut prepared.bytes,
        seal_offset,
        group_number,
        start_offset,
        end_offset,
        entry_count,
        digest,
    );

    Ok(FinalizedGroup {
        digest,
        group_number,
        start_offset,
        end_offset,
        entry_count,
        next_chain: chain,
    })
}

/// Validate one complete physical group against exact segment and chain evidence.
pub fn decode_group<'a>(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    expected_chain: ChainPosition,
    input: &'a [u8],
    limits: DecodeLimits,
) -> Result<DecodedGroup<'a>, CodecError> {
    validate_group_position(group_number, start_offset)?;
    validate_decode_limits(limits)?;
    let parsed = scan_physical_group(segment, group_number, start_offset, input, limits)?;
    let expected_group_digest = parsed.digest;
    let (operations, next_chain) = decode_operations(
        segment,
        start_offset,
        expected_chain,
        input,
        parsed.entries,
        parsed.shared.as_ref(),
        limits,
    )?;
    let mut group_digest =
        GroupDigestBuilder::new(segment, group_number, start_offset, operations.len())?;
    for operation in &operations {
        group_digest.push(operation.digest);
    }
    let digest = group_digest.finish(parsed.end_offset);
    if digest != expected_group_digest {
        return Err(CodecError::DigestMismatch("physical group seal"));
    }

    Ok(DecodedGroup {
        group_number,
        start_offset,
        end_offset: parsed.end_offset,
        digest,
        operations,
        next_chain,
    })
}

/// Probe physical framing without allocating decoded bodies. The caller then
/// decodes exactly once after all encoded bytes are available.
pub(crate) fn group_extent(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    input: &[u8],
    limits: DecodeLimits,
) -> Result<u64, CodecError> {
    validate_group_position(group_number, start_offset)?;
    validate_decode_limits(limits)?;
    scan_physical_group(segment, group_number, start_offset, input, limits)
        .map(|parsed| parsed.end_offset)
}

/// Decode one exact indexed entry independently of physical-group framing.
///
/// Caller still needs a validated index/source binding. This verifies entry
/// header, canonical body, logical digest, group identity, and physical bounds.
fn decode_standalone_operation<'a>(
    segment: &SegmentHeader,
    entry_offset: u64,
    input: &'a [u8],
    limits: DecodeLimits,
) -> Result<DecodedOperation<'a>, CodecError> {
    validate_decode_limits(limits)?;
    if entry_offset < SEGMENT_HEADER_BYTES as u64 || !entry_offset.is_multiple_of(8) {
        return Err(CodecError::InvalidEntryOffset(entry_offset));
    }
    let input_bytes = u64::try_from(input.len()).map_err(|_| CodecError::LengthOverflow)?;
    if entry_offset
        .checked_add(input_bytes)
        .is_none_or(|end| end > segment.capacity)
    {
        return Err(CodecError::GroupExceedsSegment);
    }
    let parsed = parse_entry(input, 0, limits)?;
    if parsed.end != input.len() {
        return Err(CodecError::InvalidEntryLength);
    }
    let expected = ChainPosition::new(parsed.op_number, parsed.previous_digest);
    let (mut operations, _) = decode_operations(
        segment,
        entry_offset,
        expected,
        input,
        vec![parsed],
        None,
        limits,
    )?;
    operations.pop().ok_or(CodecError::EmptyGroup)
}

/// Decode an exact indexed operation. Shared LZ4 extents also require the
/// operation number, since several independent operations share the same bytes.
pub(crate) fn decode_indexed_operation<'a>(
    segment: &SegmentHeader,
    entry_offset: u64,
    input: &'a [u8],
    op_number: u64,
    limits: DecodeLimits,
) -> Result<DecodedOperation<'a>, CodecError> {
    validate_decode_limits(limits)?;
    if entry_offset < SEGMENT_HEADER_BYTES as u64 || !entry_offset.is_multiple_of(8) {
        return Err(CodecError::InvalidEntryOffset(entry_offset));
    }
    if entry_offset
        .checked_add(input.len() as u64)
        .is_none_or(|end| end > segment.capacity)
    {
        return Err(CodecError::GroupExceedsSegment);
    }
    let operation = if shared::is_shared(input) {
        shared::decode(segment, entry_offset, input, limits)?
            .into_iter()
            .find(|operation| operation.op_number == op_number)
            .ok_or(CodecError::ChainMismatch {
                expected_op: op_number,
            })?
    } else {
        decode_standalone_operation(segment, entry_offset, input, limits)?
    };
    if operation.op_number != op_number {
        return Err(CodecError::ChainMismatch {
            expected_op: op_number,
        });
    }
    Ok(operation)
}

/// Nonvoting repair only. Discover independently valid entries after damaged
/// framing without granting them authority. The installer must verify exact
/// manifest-anchored canonical history before publishing any recovered bytes.
pub(crate) fn salvage_entries(
    segment: &SegmentHeader,
    input: &[u8],
    first: u64,
    last: u64,
    limits: DecodeLimits,
) -> Result<Vec<DecodedOperation<'static>>, CodecError> {
    validate_decode_limits(limits)?;
    let mut entries = Vec::new();
    let mut cursor = SEGMENT_HEADER_BYTES;
    let mut decoded_bytes = 0usize;
    while cursor.saturating_add(ENTRY_HEADER_BYTES) <= input.len() {
        if shared::is_shared(&input[cursor..])
            && let Ok((_, body)) = shared::parse(&input[cursor..], limits)
            && let Ok(operations) = shared::decode(
                segment,
                cursor as u64,
                &input[cursor..cursor + body.entry_bytes],
                limits,
            )
        {
            cursor += body.entry_bytes;
            for operation in operations
                .into_iter()
                .filter(|op| (first..=last).contains(&op.op_number))
            {
                decoded_bytes = decoded_bytes
                    .checked_add(operation.body.len())
                    .ok_or(CodecError::LengthOverflow)?;
                enforce_limit(
                    "salvaged decoded body bytes",
                    decoded_bytes,
                    limits.max_segment_decoded_body_bytes,
                )?;
                entries.push(DecodedOperation {
                    body: Cow::Owned(operation.body.into_owned()),
                    ..operation
                });
            }
            continue;
        }
        if input.get(cursor..cursor + ENTRY_MAGIC.len()) != Some(ENTRY_MAGIC) {
            cursor += 8;
            continue;
        }
        let Ok(parsed) = parse_entry(input, cursor, limits) else {
            cursor += 8;
            continue;
        };
        let start = cursor;
        cursor = parsed.end;
        if parsed.op_number < first || parsed.op_number > last {
            continue;
        }
        let Ok(operation) =
            decode_standalone_operation(segment, start as u64, &input[start..cursor], limits)
        else {
            continue;
        };
        decoded_bytes = decoded_bytes
            .checked_add(operation.body.len())
            .ok_or(CodecError::LengthOverflow)?;
        enforce_limit(
            "salvaged decoded body bytes",
            decoded_bytes,
            limits.max_segment_decoded_body_bytes,
        )?;
        entries.push(DecodedOperation {
            body: Cow::Owned(operation.body.into_owned()),
            ..operation
        });
    }
    Ok(entries)
}

/// Scan a segment without skipping an invalid physical group.
///
/// Only an end-of-file truncation is classified as a repairable crash tail.
/// Digest, padding, chain, and other structural failures remain corruption.
pub fn scan_segment(
    input: &[u8],
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
) -> Result<SegmentScan<'_>, CodecError> {
    scan(input, first_group_number, initial_chain, limits, false)
}

/// Scan the valid prefix of an active segment after a crash.
///
/// The first physical group that fails to decode ends the prefix as
/// [`TailState::Damaged`], for example a write torn by a power loss or a later
/// write that reached the device before an earlier one. Header, limit and
/// resource failures still fail. The caller must prove that every durably
/// confirmed operation lies in the prefix, then zero the damaged bytes.
pub(crate) fn scan_segment_prefix(
    input: &[u8],
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
) -> Result<SegmentScan<'_>, CodecError> {
    scan(input, first_group_number, initial_chain, limits, true)
}

#[derive(Clone)]
pub(crate) struct SegmentDigestBuilder {
    hasher: Hasher,
}

impl std::fmt::Debug for SegmentDigestBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SegmentDigestBuilder")
            .field("digest", &self.finish())
            .finish()
    }
}

impl SegmentDigestBuilder {
    pub(crate) fn new(header: &SegmentHeader) -> Self {
        let header_bytes = encode_segment_header(header);
        let header_digest = digest_at(&header_bytes, SEGMENT_DIGEST_RANGE);
        let mut hasher = Hasher::new(SEGMENT_HASH_CONTEXT);
        hasher.update(header_digest.as_bytes());
        Self { hasher }
    }

    pub(crate) fn push(&mut self, group_digest: Digest) {
        self.hasher.update(group_digest.as_bytes());
    }

    pub(crate) fn finish(&self) -> Digest {
        self.hasher.finish()
    }
}

pub(crate) fn validate_decode_limits(limits: DecodeLimits) -> Result<(), CodecError> {
    if limits.max_groups == 0
        || limits.max_entries == 0
        || limits.max_entry_bytes < ENTRY_HEADER_BYTES
        || limits.max_decoded_body_bytes == 0
        || limits.max_group_decoded_body_bytes < limits.max_decoded_body_bytes
        || limits.max_segment_decoded_body_bytes < limits.max_group_decoded_body_bytes
    {
        return Err(CodecError::LimitExceeded {
            kind: "decoder configuration",
            actual: 0,
            limit: 0,
        });
    }
    Ok(())
}

fn scan_physical_group(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    input: &[u8],
    limits: DecodeLimits,
) -> Result<ParsedGroup, CodecError> {
    let (mut parsed, shared, mut cursor) = if shared::is_shared(input) {
        let (entries, body) = shared::parse(input, limits)?;
        let end = body.entry_bytes;
        (entries, Some(body), end)
    } else {
        (Vec::new(), None, 0)
    };
    while shared.is_none() {
        if input.get(cursor..cursor.saturating_add(ENTRY_MAGIC.len())) == Some(ENTRY_MAGIC) {
            if parsed.len() == limits.max_entries {
                return Err(CodecError::LimitExceeded {
                    kind: "entry count",
                    actual: parsed.len() + 1,
                    limit: limits.max_entries,
                });
            }
            let entry = parse_entry(input, cursor, limits)?;
            cursor = entry.end;
            parsed.push(entry);
            continue;
        }
        if parsed.is_empty() {
            let available = input.len().saturating_sub(cursor);
            if available < ENTRY_MAGIC.len()
                && ENTRY_MAGIC.starts_with(input.get(cursor..).unwrap_or_default())
            {
                return Err(CodecError::Truncated {
                    object: "entry magic",
                    needed: ENTRY_MAGIC.len(),
                    available,
                });
            }
            return Err(CodecError::WrongMagic("journal entry"));
        }
        break;
    }

    let group_bytes = align_up(
        cursor
            .checked_add(GROUP_SEAL_BYTES)
            .ok_or(CodecError::LengthOverflow)?,
        WRITE_GROUP_ALIGNMENT,
    )?;
    require_len(input, group_bytes, "physical group seal")?;
    let seal_offset = group_bytes - GROUP_SEAL_BYTES;
    if !all_zero(&input[cursor..seal_offset]) {
        return Err(CodecError::NonZeroPadding);
    }
    let end_offset = start_offset
        .checked_add(u64::try_from(group_bytes).map_err(|_| CodecError::LengthOverflow)?)
        .ok_or(CodecError::LengthOverflow)?;
    if end_offset > segment.capacity {
        return Err(CodecError::GroupExceedsSegment);
    }
    let digest = validate_seal(
        &input[..group_bytes],
        seal_offset,
        group_number,
        start_offset,
        end_offset,
        parsed.len(),
    )?;

    Ok(ParsedGroup {
        entries: parsed,
        shared,
        end_offset,
        digest,
    })
}

fn decode_operations<'a>(
    segment: &SegmentHeader,
    group_start_offset: u64,
    expected_chain: ChainPosition,
    input: &'a [u8],
    parsed: Vec<ParsedEntry>,
    shared: Option<&shared::Body>,
    limits: DecodeLimits,
) -> Result<(Vec<DecodedOperation<'a>>, ChainPosition), CodecError> {
    let group_decoded_bytes = parsed.iter().try_fold(0_usize, |total, entry| {
        total.checked_add(entry.decoded_body_bytes)
    });
    enforce_limit(
        "physical group decoded body bytes",
        group_decoded_bytes.ok_or(CodecError::LengthOverflow)?,
        limits.max_group_decoded_body_bytes,
    )?;
    let decoded = shared
        .as_ref()
        .map(|body| decode_lz4_body(&input[body.encoded.clone()], group_decoded_bytes.unwrap()))
        .transpose()?;
    let mut chain = expected_chain;
    let mut operations = Vec::with_capacity(parsed.len());
    for entry in parsed {
        let entry_start = if shared.is_some() {
            0
        } else {
            entry
                .body
                .start
                .checked_sub(ENTRY_HEADER_BYTES)
                .ok_or(CodecError::InvalidEntryLength)?
        };
        let entry_offset = group_start_offset
            .checked_add(u64::try_from(entry_start).map_err(|_| CodecError::LengthOverflow)?)
            .ok_or(CodecError::LengthOverflow)?;
        let entry_bytes = u64::try_from(
            shared
                .as_ref()
                .map_or(entry.end - entry_start, |body| body.entry_bytes),
        )
        .map_err(|_| CodecError::LengthOverflow)?;
        if entry.group_id != segment.group_id {
            return Err(CodecError::WrongGroup);
        }
        if entry.op_number != chain.next_op_number()
            || entry.previous_digest != chain.previous_digest()
        {
            return Err(CodecError::ChainMismatch {
                expected_op: chain.next_op_number(),
            });
        }
        let body = match &decoded {
            Some(bytes) => Cow::Owned(bytes[entry.body.clone()].to_vec()),
            None => decode_entry_body(input, &entry)?,
        };
        let body_digest = canonical_body_digest(body.as_ref());
        if body_digest != entry.body_digest {
            return Err(CodecError::DigestMismatch("canonical body"));
        }
        let canonical = CanonicalOperation {
            group_id: entry.group_id,
            configuration_epoch: entry.configuration_epoch,
            original_view: entry.original_view,
            op_number: entry.op_number,
            previous_digest: entry.previous_digest,
            kind: entry.kind,
            body: body.as_ref(),
        };
        let digest = logical_operation_digest_with_body_digest(&canonical, body_digest);
        chain = ChainPosition::new(
            entry
                .op_number
                .checked_add(1)
                .ok_or(CodecError::OperationNumberExhausted)?,
            digest,
        );
        operations.push(DecodedOperation {
            entry_offset,
            entry_bytes,
            group_id: entry.group_id,
            configuration_epoch: entry.configuration_epoch,
            original_view: entry.original_view,
            op_number: entry.op_number,
            previous_digest: entry.previous_digest,
            digest,
            body_digest,
            kind: entry.kind,
            body,
        });
    }
    Ok((operations, chain))
}

fn decode_entry_body<'a>(
    input: &'a [u8],
    entry: &ParsedEntry,
) -> Result<Cow<'a, [u8]>, CodecError> {
    let encoded = &input[entry.body.clone()];
    match entry.codec {
        0 => Ok(Cow::Borrowed(encoded)),
        1 => decode_lz4_body(encoded, entry.decoded_body_bytes).map(Cow::Owned),
        codec => Err(CodecError::UnsupportedCodec(codec)),
    }
}

#[cfg(feature = "lz4")]
fn decode_lz4_body(input: &[u8], decoded_len: usize) -> Result<Vec<u8>, CodecError> {
    let mut output = vec![0_u8; decoded_len];
    let actual = lz4rip::decompress_into(input, &mut output)
        .map_err(|_| CodecError::DecompressionFailed("LZ4"))?;
    if actual != decoded_len {
        return Err(CodecError::DecompressionFailed("LZ4"));
    }
    Ok(output)
}

#[cfg(not(feature = "lz4"))]
fn decode_lz4_body(_input: &[u8], _decoded_len: usize) -> Result<Vec<u8>, CodecError> {
    Err(CodecError::UnsupportedCodec(1))
}

fn validate_group_position(group_number: u64, start_offset: u64) -> Result<(), CodecError> {
    if group_number == 0 {
        return Err(CodecError::InvalidGroupNumber);
    }
    if start_offset < SEGMENT_HEADER_BYTES as u64
        || !start_offset.is_multiple_of(WRITE_GROUP_ALIGNMENT as u64)
    {
        return Err(CodecError::InvalidGroupOffset(start_offset));
    }
    Ok(())
}

fn validate_operation(
    segment: &SegmentHeader,
    chain: ChainPosition,
    operation: &OperationHeader,
) -> Result<(), CodecError> {
    if operation.group_id != segment.group_id {
        return Err(CodecError::WrongGroup);
    }
    if operation.op_number != chain.next_op_number()
        || operation.previous_digest != chain.previous_digest()
    {
        return Err(CodecError::ChainMismatch {
            expected_op: chain.next_op_number(),
        });
    }
    Ok(())
}

fn encode_entry_header(
    output: &mut [u8],
    entry: PreparedEntry,
    operation: &OperationHeader,
    body_digest: Digest,
) -> Result<(), CodecError> {
    let total_len = encoded_entry_len(entry.encoded_len)?;
    let header_end = entry
        .header_start
        .checked_add(ENTRY_HEADER_BYTES)
        .ok_or(CodecError::LengthOverflow)?;
    if header_end > output.len() {
        return Err(CodecError::InvalidEntryLength);
    }
    let header = &mut output[entry.header_start..header_end];
    header[0..4].copy_from_slice(ENTRY_MAGIC);
    put_u16(header, 4, FORMAT_VERSION);
    put_u16(header, 6, operation.kind as u16);
    put_u32(header, 8, ENTRY_HEADER_BYTES as u32);
    put_u32(header, 12, 0);
    put_u64(
        header,
        16,
        u64::try_from(total_len).map_err(|_| CodecError::LengthOverflow)?,
    );
    put_u64(
        header,
        24,
        u64::try_from(entry.encoded_len).map_err(|_| CodecError::LengthOverflow)?,
    );
    header[32..48].copy_from_slice(operation.group_id.as_bytes());
    put_u64(header, 48, operation.configuration_epoch);
    put_u64(header, 56, operation.original_view);
    put_u64(header, 64, operation.op_number);
    header[72..104].copy_from_slice(operation.previous_digest.as_bytes());
    header[104..136].copy_from_slice(body_digest.as_bytes());
    put_u64(
        header,
        168,
        u64::try_from(entry.decoded_len).map_err(|_| CodecError::LengthOverflow)?,
    );
    put_u16(header, 176, entry.codec);
    put_u16(header, 178, 0);
    put_u64(header, 180, 0);
    let header_digest =
        hash_with_zeroed_range(ENTRY_HEADER_HASH_CONTEXT, header, ENTRY_DIGEST_RANGE);
    header[ENTRY_DIGEST_RANGE].copy_from_slice(header_digest.as_bytes());
    Ok(())
}

#[derive(Debug)]
struct EncodedBody<'a> {
    bytes: Cow<'a, [u8]>,
    codec: u16,
}

#[derive(Default)]
pub(crate) struct BodyEncodeScratch {
    #[cfg(feature = "lz4")]
    joined: Vec<u8>,
    #[cfg(feature = "lz4")]
    lz4: Vec<u8>,
    #[cfg(feature = "lz4")]
    lz4_compressor: lz4rip::block::Compressor,
}

impl std::fmt::Debug for BodyEncodeScratch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("BodyEncodeScratch");
        #[cfg(feature = "lz4")]
        debug
            .field("joined_capacity", &self.joined.capacity())
            .field("lz4_capacity", &self.lz4.capacity())
            .field("lz4_compressor", &self.lz4_compressor);
        debug.finish()
    }
}

impl BodyEncodeScratch {
    pub(crate) fn reserve(
        &mut self,
        body_capacity: usize,
        encoding: BodyEncoding,
    ) -> Result<(), CodecError> {
        match encoding {
            BodyEncoding::Raw => Ok(()),
            BodyEncoding::Lz4 { .. } => self.reserve_lz4(body_capacity),
        }
    }

    #[cfg(feature = "lz4")]
    fn reserve_lz4(&mut self, body_capacity: usize) -> Result<(), CodecError> {
        let output_capacity = lz4rip::get_maximum_output_size(body_capacity);
        if self.lz4.len() < output_capacity {
            self.lz4
                .try_reserve_exact(output_capacity - self.lz4.len())
                .map_err(|_| CodecError::CompressionScratchAllocation("LZ4"))?;
            self.lz4.resize(output_capacity, 0);
        }
        Ok(())
    }

    #[cfg(not(feature = "lz4"))]
    #[allow(
        clippy::unused_self,
        reason = "method call shape is shared with the feature-enabled implementation"
    )]
    fn reserve_lz4(&mut self, _body_capacity: usize) -> Result<(), CodecError> {
        Err(CodecError::UnsupportedCodec(1))
    }
}

fn encode_body<'a>(
    input: &'a [u8],
    encoding: BodyEncoding,
    scratch: &'a mut BodyEncodeScratch,
) -> Result<EncodedBody<'a>, CodecError> {
    match encoding {
        BodyEncoding::Raw => Ok(EncodedBody {
            bytes: Cow::Borrowed(input),
            codec: 0,
        }),
        BodyEncoding::Lz4 { min_savings_bytes } => {
            encode_lz4_body(input, min_savings_bytes, scratch)
        }
    }
}

#[cfg(feature = "lz4")]
fn compressed_or_raw<'a>(
    input: &'a [u8],
    compressed: Cow<'a, [u8]>,
    codec: u16,
    min_savings_bytes: usize,
) -> EncodedBody<'a> {
    if compressed
        .len()
        .checked_add(min_savings_bytes)
        .is_some_and(|minimum| minimum <= input.len())
    {
        EncodedBody {
            bytes: compressed,
            codec,
        }
    } else {
        EncodedBody {
            bytes: Cow::Borrowed(input),
            codec: 0,
        }
    }
}

#[cfg(feature = "lz4")]
fn encode_lz4_body<'a>(
    input: &'a [u8],
    min_savings_bytes: usize,
    scratch: &'a mut BodyEncodeScratch,
) -> Result<EncodedBody<'a>, CodecError> {
    scratch.reserve_lz4(input.len())?;
    let length = scratch
        .lz4_compressor
        .compress_into(input, &mut scratch.lz4)
        .map_err(|_| CodecError::CompressionFailed("LZ4"))?;
    Ok(compressed_or_raw(
        input,
        Cow::Borrowed(&scratch.lz4[..length]),
        1,
        min_savings_bytes,
    ))
}

#[cfg(not(feature = "lz4"))]
fn encode_lz4_body<'a>(
    _input: &'a [u8],
    _min_savings_bytes: usize,
    _scratch: &mut BodyEncodeScratch,
) -> Result<EncodedBody<'a>, CodecError> {
    Err(CodecError::UnsupportedCodec(1))
}

fn encoded_entry_len(body_len: usize) -> Result<usize, CodecError> {
    align_up(
        ENTRY_HEADER_BYTES
            .checked_add(body_len)
            .ok_or(CodecError::LengthOverflow)?,
        8,
    )
}

fn parse_entry(
    input: &[u8],
    start: usize,
    limits: DecodeLimits,
) -> Result<ParsedEntry, CodecError> {
    parse_entry_representation(input, start, limits, false)
}

fn parse_entry_representation(
    input: &[u8],
    start: usize,
    limits: DecodeLimits,
    shared: bool,
) -> Result<ParsedEntry, CodecError> {
    let available = input.len().saturating_sub(start);
    if available < ENTRY_HEADER_BYTES {
        return Err(CodecError::Truncated {
            object: "entry header",
            needed: ENTRY_HEADER_BYTES,
            available,
        });
    }
    let header = &input[start..start + ENTRY_HEADER_BYTES];
    if &header[..4] != ENTRY_MAGIC {
        return Err(CodecError::WrongMagic("journal entry"));
    }
    require_version(read_u16(header, 4))?;
    let kind_value = read_u16(header, 6);
    let kind = OperationKind::try_from(kind_value)
        .map_err(|_| CodecError::UnsupportedOperationKind(kind_value))?;
    if read_u32(header, 8) != ENTRY_HEADER_BYTES as u32 {
        return Err(CodecError::InvalidEntryLength);
    }
    if read_u32(header, 12) != 0 {
        return Err(CodecError::UnsupportedFlags("entry"));
    }
    let total_len = usize_from_u64(read_u64(header, 16))?;
    let encoded_len = usize_from_u64(read_u64(header, 24))?;
    let decoded_len = usize_from_u64(read_u64(header, 168))?;
    if total_len < ENTRY_HEADER_BYTES
        || !total_len.is_multiple_of(8)
        || encoded_len > total_len - ENTRY_HEADER_BYTES
    {
        return Err(CodecError::InvalidEntryLength);
    }
    enforce_limit("entry bytes", total_len, limits.max_entry_bytes)?;
    enforce_limit(
        "decoded body bytes",
        decoded_len,
        limits.max_decoded_body_bytes,
    )?;
    let end = start
        .checked_add(total_len)
        .ok_or(CodecError::LengthOverflow)?;
    if input.len() < end {
        return Err(CodecError::Truncated {
            object: "journal entry",
            needed: total_len,
            available,
        });
    }
    let codec = read_u16(header, 176);
    if if shared {
        codec != shared::CODEC
    } else {
        codec > 1
    } {
        return Err(CodecError::UnsupportedCodec(codec));
    }
    if shared && (encoded_len != 0 || total_len != ENTRY_HEADER_BYTES) {
        return Err(CodecError::InvalidEntryLength);
    }
    if read_u16(header, 178) != 0 {
        return Err(CodecError::UnsupportedFlags("entry codec"));
    }
    if !all_zero(&header[180..192]) {
        return Err(CodecError::NonZeroReserved("entry"));
    }
    if codec == 0 && encoded_len != decoded_len {
        return Err(CodecError::InvalidEntryLength);
    }
    let expected_header_digest = digest_at(header, ENTRY_DIGEST_RANGE.clone());
    let actual_header_digest =
        hash_with_zeroed_range(ENTRY_HEADER_HASH_CONTEXT, header, ENTRY_DIGEST_RANGE);
    if expected_header_digest != actual_header_digest {
        return Err(CodecError::DigestMismatch("entry header"));
    }
    let body_start = start + ENTRY_HEADER_BYTES;
    let body_end = body_start
        .checked_add(encoded_len)
        .ok_or(CodecError::LengthOverflow)?;
    if !all_zero(&input[body_end..end]) {
        return Err(CodecError::NonZeroPadding);
    }
    Ok(ParsedEntry {
        group_id: GroupId::from_bytes(array_16(header, 32)),
        configuration_epoch: read_u64(header, 48),
        original_view: read_u64(header, 56),
        op_number: read_u64(header, 64),
        previous_digest: digest_at(header, 72..104),
        body_digest: digest_at(header, 104..136),
        kind,
        body: body_start..body_end,
        decoded_body_bytes: decoded_len,
        codec,
        end,
    })
}

fn encode_seal(
    group: &mut [u8],
    seal_offset: usize,
    group_number: u64,
    start_offset: u64,
    end_offset: u64,
    entry_count: u64,
    digest: Digest,
) {
    let seal = &mut group[seal_offset..];
    seal[0..8].copy_from_slice(SEAL_MAGIC);
    put_u64(seal, 8, group_number);
    put_u64(seal, 16, start_offset);
    put_u64(seal, 24, end_offset);
    put_u64(seal, 32, entry_count);
    seal[SEAL_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
}

fn validate_seal(
    group: &[u8],
    seal_offset: usize,
    group_number: u64,
    start_offset: u64,
    end_offset: u64,
    entry_count: usize,
) -> Result<Digest, CodecError> {
    let seal = &group[seal_offset..];
    if &seal[0..8] != SEAL_MAGIC {
        return Err(CodecError::WrongMagic("physical group seal"));
    }
    if !all_zero(&seal[72..]) {
        return Err(CodecError::NonZeroReserved("physical group seal"));
    }
    let expected_count = u64::try_from(entry_count).map_err(|_| CodecError::LengthOverflow)?;
    if read_u64(seal, 8) != group_number
        || read_u64(seal, 16) != start_offset
        || read_u64(seal, 24) != end_offset
        || read_u64(seal, 32) != expected_count
    {
        return Err(CodecError::SealMismatch);
    }
    Ok(digest_at(seal, SEAL_DIGEST_RANGE))
}

struct GroupDigestBuilder {
    hasher: Hasher,
}

impl GroupDigestBuilder {
    fn new(
        segment: &SegmentHeader,
        group_number: u64,
        start_offset: u64,
        entry_count: usize,
    ) -> Result<Self, CodecError> {
        let segment_bytes = encode_segment_header(segment);
        let segment_digest = digest_at(&segment_bytes, SEGMENT_DIGEST_RANGE);
        let entry_count = u64::try_from(entry_count).map_err(|_| CodecError::LengthOverflow)?;
        let mut hasher = Hasher::new(GROUP_HASH_CONTEXT);
        hasher.update(segment_digest.as_bytes());
        hasher.update(&group_number.to_be_bytes());
        hasher.update(&start_offset.to_be_bytes());
        hasher.update(&entry_count.to_be_bytes());
        Ok(Self { hasher })
    }

    fn push(&mut self, operation_digest: Digest) {
        self.hasher.update(operation_digest.as_bytes());
    }

    fn finish(mut self, end_offset: u64) -> Digest {
        self.hasher.update(&end_offset.to_be_bytes());
        self.hasher.finish()
    }
}

fn hash_with_zeroed_range(context: &'static str, input: &[u8], zero: Range<usize>) -> Digest {
    let mut hasher = Hasher::new(context);
    hasher.update(&input[..zero.start]);
    hasher.update(&[0; 32]);
    hasher.update(&input[zero.end..]);
    hasher.finish()
}

fn require_version(version: u16) -> Result<(), CodecError> {
    if version == FORMAT_VERSION {
        Ok(())
    } else {
        Err(CodecError::UnsupportedVersion(version))
    }
}

fn require_len(input: &[u8], needed: usize, object: &'static str) -> Result<(), CodecError> {
    if input.len() < needed {
        Err(CodecError::Truncated {
            object,
            needed,
            available: input.len(),
        })
    } else {
        Ok(())
    }
}

fn enforce_limit(kind: &'static str, actual: usize, limit: usize) -> Result<(), CodecError> {
    if actual > limit {
        Err(CodecError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn align_up(value: usize, alignment: usize) -> Result<usize, CodecError> {
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or(CodecError::LengthOverflow)
    }
}

fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

fn usize_from_u64(value: u64) -> Result<usize, CodecError> {
    usize::try_from(value).map_err(|_| CodecError::LengthOverflow)
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
    u16::from_be_bytes([input[offset], input[offset + 1]])
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
    ])
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
        input[offset + 4],
        input[offset + 5],
        input[offset + 6],
        input[offset + 7],
    ])
}

fn array_16(input: &[u8], offset: usize) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&input[offset..offset + 16]);
    bytes
}

fn digest_at(input: &[u8], range: Range<usize>) -> Digest {
    let mut bytes = [0; 32];
    bytes.copy_from_slice(&input[range]);
    Digest::from_bytes(bytes)
}
