//! Segment file header: fields, encoding, and validation.

use super::{
    CodecError, FORMAT_VERSION, SEGMENT_DIGEST_RANGE, SEGMENT_HEADER_BYTES,
    SEGMENT_HEADER_HASH_CONTEXT, WRITE_GROUP_ALIGNMENT, all_zero, require_len, require_version,
};
use ozzy_journal::integrity::hash_without_digest;
use ozzy_journal::operation::Digest;
use ozzy_proto::GroupId;
use ozzy_proto::layout::{self, Fixed};

const SEGMENT_MAGIC: &[u8; 8] = b"OZYSEG\0\0";

/// Immutable identity and capacity of one physical segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub(super) group_id: GroupId,
    segment_id: u64,
    file_generation: u64,
    predecessor_segment_id: Option<u64>,
    predecessor_digest: Digest,
    pub(super) capacity: u64,
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

/// Encode the fixed-size physical segment header and integrity fields.
pub fn encode_segment_header(header: &SegmentHeader) -> [u8; SEGMENT_HEADER_BYTES] {
    let mut output = [0_u8; SEGMENT_HEADER_BYTES];
    output[0..8].copy_from_slice(SEGMENT_MAGIC);
    output.put_u16::<8>(FORMAT_VERSION);
    output.put_u16::<10>(SEGMENT_HEADER_BYTES as u16);
    output.put_u32::<12>(0);
    output.put::<16, 16>(*header.group_id.as_bytes());
    output.put_u64::<32>(header.segment_id);
    output.put_u64::<40>(header.predecessor_segment_id.unwrap_or_default());
    output.put::<48, 32>(*header.predecessor_digest.as_bytes());
    output.put_u64::<80>(header.capacity);
    output.put_u64::<120>(header.file_generation);
    let digest = hash_without_digest(
        SEGMENT_HEADER_HASH_CONTEXT,
        &output,
        SEGMENT_DIGEST_RANGE.start,
    );
    output[SEGMENT_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
    output
}

/// Validate and decode the fixed-size physical segment header.
pub fn decode_segment_header(input: &[u8]) -> Result<SegmentHeader, CodecError> {
    require_len(input, SEGMENT_HEADER_BYTES, "segment header")?;
    let input: &[u8; SEGMENT_HEADER_BYTES] = layout::record(input, 0)?;
    if input.array::<0, 8>() != *SEGMENT_MAGIC {
        return Err(CodecError::WrongMagic("segment header"));
    }
    require_version(input.u16_at::<8>())?;
    if input.u16_at::<10>() != SEGMENT_HEADER_BYTES as u16 {
        return Err(CodecError::InvalidEntryLength);
    }
    if input.u32_at::<12>() != 0 {
        return Err(CodecError::UnsupportedFlags("segment header"));
    }
    let expected = Digest::from_bytes(input.array::<{ SEGMENT_DIGEST_RANGE.start }, 32>());
    let actual = hash_without_digest(
        SEGMENT_HEADER_HASH_CONTEXT,
        input,
        SEGMENT_DIGEST_RANGE.start,
    );
    if expected != actual {
        return Err(CodecError::DigestMismatch("segment header"));
    }
    let group_id = GroupId::from_bytes(input.array::<16, 16>());
    let segment_id = input.u64_at::<32>();
    let predecessor_id = input.u64_at::<40>();
    let predecessor_digest = Digest::from_bytes(input.array::<48, 32>());
    let predecessor_segment_id = (predecessor_id != 0).then_some(predecessor_id);
    if !all_zero(input.slice::<128, { SEGMENT_HEADER_BYTES - 128 }>()) {
        return Err(CodecError::NonZeroReserved("segment header"));
    }
    SegmentHeader::new(
        group_id,
        segment_id,
        predecessor_segment_id,
        predecessor_digest,
        input.u64_at::<80>(),
    )
    .map(|header| header.with_file_generation(input.u64_at::<120>()))
}
