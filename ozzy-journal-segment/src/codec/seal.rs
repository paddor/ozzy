//! Group seals and the structural group digest.

use super::{
    CodecError, GROUP_SEAL_BYTES, SEGMENT_DIGEST_RANGE, SegmentHeader, all_zero,
    encode_segment_header,
};
use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::Digest;
use ozzy_proto::layout::{self, Fixed};
use std::ops::Range;

const SEAL_MAGIC: &[u8; 8] = b"OZYSEAL\0";
const GROUP_HASH_CONTEXT: &str = "ozzy journal structural group v2";
const SEAL_DIGEST_RANGE: Range<usize> = 40..72;

/// Physical identity a group seal records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GroupSeal {
    pub(super) group_number: u64,
    pub(super) start_offset: u64,
    pub(super) end_offset: u64,
    pub(super) entry_count: u64,
}

pub(super) fn encode_seal(
    group: &mut [u8],
    seal_offset: usize,
    fields: GroupSeal,
    digest: Digest,
) -> Result<(), CodecError> {
    let seal: &mut [u8; GROUP_SEAL_BYTES] = layout::record_mut(group, seal_offset)?;
    seal[0..8].copy_from_slice(SEAL_MAGIC);
    seal.put_u64::<8>(fields.group_number);
    seal.put_u64::<16>(fields.start_offset);
    seal.put_u64::<24>(fields.end_offset);
    seal.put_u64::<32>(fields.entry_count);
    seal[SEAL_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
    Ok(())
}

pub(super) fn validate_seal(
    group: &[u8],
    seal_offset: usize,
    expected: GroupSeal,
) -> Result<Digest, CodecError> {
    let seal: &[u8; GROUP_SEAL_BYTES] = layout::record(group, seal_offset)?;
    if seal.array::<0, 8>() != *SEAL_MAGIC {
        return Err(CodecError::WrongMagic("physical group seal"));
    }
    if !all_zero(seal.slice::<72, { GROUP_SEAL_BYTES - 72 }>()) {
        return Err(CodecError::NonZeroReserved("physical group seal"));
    }
    let recorded = GroupSeal {
        group_number: seal.u64_at::<8>(),
        start_offset: seal.u64_at::<16>(),
        end_offset: seal.u64_at::<24>(),
        entry_count: seal.u64_at::<32>(),
    };
    if recorded != expected {
        return Err(CodecError::SealMismatch);
    }
    Ok(Digest::from_bytes(
        seal.array::<{ SEAL_DIGEST_RANGE.start }, 32>(),
    ))
}

pub(super) struct GroupDigestBuilder {
    hasher: Hasher,
}

impl GroupDigestBuilder {
    pub(super) fn new(
        segment: &SegmentHeader,
        group_number: u64,
        start_offset: u64,
        entry_count: usize,
    ) -> Result<Self, CodecError> {
        let segment_bytes = encode_segment_header(segment);
        let segment_digest =
            Digest::from_bytes(segment_bytes.array::<{ SEGMENT_DIGEST_RANGE.start }, 32>());
        let entry_count = u64::try_from(entry_count).map_err(|_| CodecError::LengthOverflow)?;
        let mut hasher = Hasher::new(GROUP_HASH_CONTEXT);
        hasher.update(segment_digest.as_bytes());
        hasher.update(&group_number.to_be_bytes());
        hasher.update(&start_offset.to_be_bytes());
        hasher.update(&entry_count.to_be_bytes());
        Ok(Self { hasher })
    }

    pub(super) fn push(&mut self, operation_digest: Digest) {
        self.hasher.update(operation_digest.as_bytes());
    }

    pub(super) fn finish(mut self, end_offset: u64) -> Digest {
        self.hasher.update(&end_offset.to_be_bytes());
        self.hasher.finish()
    }
}
