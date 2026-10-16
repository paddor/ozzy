//! Write-group encoding: prepared bodies, entry layouts, and finalized group
//! bytes.

use super::{
    BodyEncodeScratch, BodyEncoding, CodecError, DecodeLimits, ENTRY_HEADER_BYTES,
    GROUP_SEAL_BYTES, GroupStart, PackedOperation, SegmentHeader, WRITE_GROUP_ALIGNMENT, align_up,
    body::encode_body,
    chain_after,
    decode::{validate_group_position, validate_operation},
    enforce_limit,
    entry::{encode_entry_header, encoded_entry_len},
    group_end_offset,
    seal::{GroupDigestBuilder, GroupSeal, SealRecord, encode_seal},
    shared,
};
use ozzy_journal::operation::{CanonicalOperation, ChainPosition, Digest, canonical_body_digest};
use smallvec::SmallVec;

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
    pub(super) bytes: Vec<u8>,
    pub(super) entries: SmallVec<[PreparedEntry; INLINE_GROUP_OPERATIONS]>,
    pub(super) decoded_body_bytes: usize,
    pub(super) entry_bytes: usize,
    pub(super) borrowed_raw: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PreparedEntry {
    pub(super) start: usize,
    pub(super) header_start: usize,
    pub(super) encoded_len: usize,
    pub(super) decoded_len: usize,
    pub(super) codec: u16,
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
        group_end_offset(start, aligned_group_bytes(self.entry_bytes)?, capacity).map(|_| ())
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

/// Encode consecutive canonical operations as one aligned physical write group.
/// Its seal marks every earlier group durable.
pub fn encode_group(
    segment: &SegmentHeader,
    start: GroupStart,
    operations: &[CanonicalOperation<'_>],
) -> Result<EncodedGroup, CodecError> {
    encode_group_with_body_encoding(segment, start, operations, BodyEncoding::Raw)
}

/// Encode one physical group using independently decodable operation bodies.
/// Its seal marks every earlier group durable, as for a file written whole
/// and synchronized before use.
pub fn encode_group_with_body_encoding(
    segment: &SegmentHeader,
    start: GroupStart,
    operations: &[CanonicalOperation<'_>],
    encoding: BodyEncoding,
) -> Result<EncodedGroup, CodecError> {
    let body_digests = operations
        .iter()
        .map(|operation| canonical_body_digest(operation.body))
        .collect::<Vec<_>>();
    let mut prepared = prepare_group_bodies(
        operations.iter().map(|operation| operation.body),
        encoding,
        Vec::new(),
        &mut BodyEncodeScratch::default(),
    )?;
    let write = GroupWrite {
        start,
        durable_through: start.group_number.saturating_sub(1),
    };
    let finalized =
        finalize_group_bodies(segment, write, operations, &body_digests, &mut prepared)?;
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
        // The entry body follows its header inside the bytes just reserved.
        bytes
            .get_mut(start..)
            .and_then(|entry| entry.get_mut(ENTRY_HEADER_BYTES..))
            .and_then(|tail| tail.get_mut(..encoded_len))
            .ok_or(CodecError::LengthOverflow)?
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

pub(super) fn group_body_encoding<'a>(
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

pub(super) fn aligned_group_bytes(entry_bytes: usize) -> Result<usize, CodecError> {
    align_up(
        entry_bytes
            .checked_add(GROUP_SEAL_BYTES)
            .ok_or(CodecError::LengthOverflow)?,
        WRITE_GROUP_ALIGNMENT,
    )
}

/// Where a group being written starts, and the highest group number already
/// durable, which its seal records. That group must precede this one.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GroupWrite {
    pub(crate) start: GroupStart,
    pub(crate) durable_through: u64,
}

pub(crate) fn finalize_group_bodies(
    segment: &SegmentHeader,
    write: GroupWrite,
    operations: &[CanonicalOperation<'_>],
    body_digests: &[Digest],
    prepared: &mut PreparedGroupBodies,
) -> Result<FinalizedGroup, CodecError> {
    if operations.len() != body_digests.len() {
        return Err(CodecError::BodyDigestCountMismatch);
    }
    finalize_group_descriptors(
        segment,
        write,
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
    write: GroupWrite,
    operations: impl ExactSizeIterator<Item = PackedOperation>,
    prepared: &mut PreparedGroupBodies,
) -> Result<FinalizedGroup, CodecError> {
    let GroupWrite {
        start,
        durable_through,
    } = write;
    validate_group_position(start.group_number, start.start_offset)?;
    let count = operations.len();
    if count == 0 {
        return Err(CodecError::EmptyGroup);
    }
    if count != prepared.entries.len() {
        return Err(CodecError::BodyDigestCountMismatch);
    }

    let mut chain = start.chain;
    let mut group_digest =
        GroupDigestBuilder::new(segment, start.group_number, start.start_offset, count)?;
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
        chain = chain_after(operation.op_number, digest)?;
    }

    let group_bytes = aligned_group_bytes(prepared.entry_bytes)?;
    let end_offset = group_end_offset(start.start_offset, group_bytes, segment.capacity)?;
    let buffer_bytes = prepared.bytes.len() + (group_bytes - prepared.entry_bytes);
    let seal_offset = buffer_bytes - GROUP_SEAL_BYTES;
    prepared.bytes.resize(buffer_bytes, 0);
    let seal = GroupSeal {
        group_number: start.group_number,
        start_offset: start.start_offset,
        end_offset,
        entry_count: u64::try_from(count).map_err(|_| CodecError::LengthOverflow)?,
    };
    let digest = group_digest.finish(end_offset, durable_through);
    let record = SealRecord {
        digest,
        durable_through,
    };
    encode_seal(&mut prepared.bytes, seal_offset, seal, record)?;

    Ok(FinalizedGroup {
        digest,
        group_number: seal.group_number,
        start_offset: seal.start_offset,
        end_offset,
        entry_count: seal.entry_count,
        next_chain: chain,
    })
}
