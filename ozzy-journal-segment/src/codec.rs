#![cfg_attr(not(test), warn(clippy::indexing_slicing))]

use std::ops::Range;

use ozzy_journal::operation::{ChainPosition, Digest};
use smallvec::SmallVec;

mod body;
mod decode;
mod encode;
mod entry;
mod error;
mod extents;
mod header;
mod limits;
mod packed;
mod scanning;
mod seal;
mod shared;
#[cfg(test)]
mod tests;

pub(crate) use body::BodyEncodeScratch;
pub use body::BodyEncoding;
pub use decode::{DecodedGroup, DecodedOperation, decode_group};
pub(crate) use decode::{decode_indexed_operation, durable_mark_ending_at, group_extent};
pub use encode::{EncodedGroup, encode_group, encode_group_with_body_encoding};
pub(crate) use encode::{
    FinalizedGroup, GroupWrite, PreparedGroupBodies, finalize_group_bodies,
    finalize_group_descriptors, prepare_group_bodies,
};
pub use error::CodecError;
pub(crate) use extents::prepare_raw_group_extents;
pub(crate) use header::validate_segment_capacity;
pub use header::{SegmentHeader, decode_segment_header, encode_segment_header};
pub use limits::DecodeLimits;
pub(crate) use limits::validate_decode_limits;
pub(crate) use packed::PackedOperation;
pub(crate) use scanning::{
    SegmentDigestBuilder, salvage_entries, scan, scan_segment_async, scan_segment_prefix_async,
};
pub use scanning::{SegmentScan, TailState, scan_segment};
pub(crate) use shared::prepare_shared_group_bodies;

/// Exact encoded physical segment header length.
pub const SEGMENT_HEADER_BYTES: usize = 4 * 1024;
/// Exact encoded canonical operation entry header length.
pub const ENTRY_HEADER_BYTES: usize = 192;
/// Exact encoded physical write-group seal length.
pub const GROUP_SEAL_BYTES: usize = 96;
/// Required byte alignment of physical write groups.
pub const WRITE_GROUP_ALIGNMENT: usize = 4 * 1024;

const FORMAT_VERSION: u16 = 3;
const ENTRY_MAGIC: &[u8; 4] = b"OZJE";
const SEGMENT_HEADER_HASH_CONTEXT: &str = "ozzy journal segment header v2";
const SEGMENT_DIGEST_RANGE: Range<usize> = 88..120;

/// Where one physical write group starts: its number, first byte offset, and
/// the canonical chain position before its first operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupStart {
    /// Consecutive physical write-group number.
    pub group_number: u64,
    /// First physical byte offset of this write group.
    pub start_offset: u64,
    /// Next canonical operation number and predecessor digest.
    pub chain: ChainPosition,
}

impl GroupStart {
    /// Name one group start; encoding and decoding validate it.
    pub const fn new(group_number: u64, start_offset: u64, chain: ChainPosition) -> Self {
        Self {
            group_number,
            start_offset,
            chain,
        }
    }
}

/// The chain position after operation `op_number` with logical `digest`.
fn chain_after(op_number: u64, digest: Digest) -> Result<ChainPosition, CodecError> {
    let next = op_number
        .checked_add(1)
        .ok_or(CodecError::OperationNumberExhausted)?;
    Ok(ChainPosition::new(next, digest))
}

/// Exclusive end of `group_bytes` starting at `start_offset`, inside the
/// segment `capacity`.
fn group_end_offset(
    start_offset: u64,
    group_bytes: usize,
    capacity: u64,
) -> Result<u64, CodecError> {
    let end_offset = start_offset
        .checked_add(u64::try_from(group_bytes).map_err(|_| CodecError::LengthOverflow)?)
        .ok_or(CodecError::LengthOverflow)?;
    if end_offset > capacity {
        return Err(CodecError::GroupExceedsSegment);
    }
    Ok(end_offset)
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

/// The bytes of `input` in `range`, or a truncation error naming `object`.
pub(super) fn span<'a>(
    input: &'a [u8],
    range: Range<usize>,
    object: &'static str,
) -> Result<&'a [u8], CodecError> {
    let needed = range.end;
    input.get(range).ok_or(CodecError::Truncated {
        object,
        needed,
        available: input.len(),
    })
}

fn usize_from_u64(value: u64) -> Result<usize, CodecError> {
    usize::try_from(value).map_err(|_| CodecError::LengthOverflow)
}
