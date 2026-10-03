//! One LZ4 block for the bodies of a collected write group. Operation headers
//! remain separate and checksummed; canonical identity never depends on grouping.

use super::{
    BodyEncodeScratch, BodyEncoding, ChainPosition, CodecError, DecodeLimits, DecodedOperation,
    ENTRY_HEADER_BYTES, ParsedEntry, PreparedGroupBodies, SegmentHeader, align_up, all_zero,
    decode_operations, digest_at, enforce_limit, group_body_encoding, hash_with_zeroed_range,
    parse_entry_representation, prepare_group_bodies, read_u64, require_len, usize_from_u64,
};
#[cfg(feature = "lz4")]
use super::{PreparedEntry, SmallVec, encoded_entry_len, put_u64};
use std::ops::Range;

pub(super) const HEADER_BYTES: usize = 64;
pub(super) const CODEC: u16 = 2;
const MAGIC: &[u8; 8] = b"OZYBODY\0";
const HASH_CONTEXT: &str = "ozzy journal shared body header";
const HASH: Range<usize> = 32..64;

pub(super) fn is_shared(input: &[u8]) -> bool {
    input.starts_with(MAGIC) || (input.len() < MAGIC.len() && MAGIC.starts_with(input))
}

#[derive(Debug)]
pub(super) struct Body {
    pub(super) encoded: Range<usize>,
    pub(super) entry_bytes: usize,
}

pub(crate) fn prepare_shared_group_bodies<'a>(
    bodies: impl Iterator<Item = &'a [u8]> + Clone,
    encoding: BodyEncoding,
    bytes: Vec<u8>,
    scratch: &mut BodyEncodeScratch,
) -> Result<PreparedGroupBodies, CodecError> {
    let encoding = group_body_encoding(bodies.clone(), encoding)?;
    match encoding {
        BodyEncoding::Raw => prepare_group_bodies(bodies, encoding, bytes, scratch),
        BodyEncoding::Lz4 { min_savings_bytes } => {
            prepare_lz4(bodies, min_savings_bytes, bytes, scratch)
        }
    }
}

#[cfg(feature = "lz4")]
fn prepare_lz4<'a>(
    bodies: impl Iterator<Item = &'a [u8]> + Clone,
    min_savings: usize,
    mut bytes: Vec<u8>,
    scratch: &mut BodyEncodeScratch,
) -> Result<PreparedGroupBodies, CodecError> {
    let mut entries = SmallVec::new();
    let mut decoded = 0usize;
    let mut raw_bytes = 0usize;
    for body in bodies.clone() {
        let start = HEADER_BYTES + entries.len() * ENTRY_HEADER_BYTES;
        entries.push(PreparedEntry {
            start,
            header_start: start,
            encoded_len: 0,
            decoded_len: body.len(),
            codec: CODEC,
        });
        decoded = decoded
            .checked_add(body.len())
            .ok_or(CodecError::LengthOverflow)?;
        raw_bytes = raw_bytes
            .checked_add(encoded_entry_len(body.len())?)
            .ok_or(CodecError::LengthOverflow)?;
    }
    if entries.is_empty() {
        return Err(CodecError::EmptyGroup);
    }
    let headers = entries
        .len()
        .checked_mul(ENTRY_HEADER_BYTES)
        .and_then(|n| n.checked_add(HEADER_BYTES))
        .ok_or(CodecError::LengthOverflow)?;
    let capacity = headers
        .checked_add(lz4rip::get_maximum_output_size(decoded))
        .ok_or(CodecError::LengthOverflow)?;
    bytes.clear();
    bytes
        .try_reserve(capacity)
        .map_err(|_| CodecError::CompressionScratchAllocation("lz4"))?;
    bytes.resize(capacity, 0);
    let input = if entries.len() == 1 {
        bodies.clone().next().expect("nonempty group")
    } else {
        scratch.joined.clear();
        scratch
            .joined
            .try_reserve(decoded)
            .map_err(|_| CodecError::CompressionScratchAllocation("lz4"))?;
        for body in bodies.clone() {
            scratch.joined.extend_from_slice(body);
        }
        &scratch.joined
    };
    // Encode directly into the final physical write buffer, after the headers.
    let encoded = scratch
        .lz4_compressor
        .compress_into(input, &mut bytes[headers..])
        .map_err(|_| CodecError::CompressionFailed("lz4"))?;
    let entry_bytes = align_up(
        headers
            .checked_add(encoded)
            .ok_or(CodecError::LengthOverflow)?,
        8,
    )?;
    if encoded >= decoded || entry_bytes >= raw_bytes || raw_bytes - entry_bytes < min_savings {
        return prepare_group_bodies(bodies, BodyEncoding::Raw, bytes, scratch);
    }
    bytes.truncate(headers + encoded);
    bytes.resize(entry_bytes, 0);
    bytes[..8].copy_from_slice(MAGIC);
    put_u64(&mut bytes, 8, entries.len() as u64);
    put_u64(&mut bytes, 16, encoded as u64);
    put_u64(&mut bytes, 24, decoded as u64);
    let digest = hash_with_zeroed_range(HASH_CONTEXT, &bytes[..HEADER_BYTES], HASH);
    bytes[HASH].copy_from_slice(digest.as_bytes());
    Ok(PreparedGroupBodies {
        bytes,
        entries,
        decoded_body_bytes: decoded,
        entry_bytes,
        borrowed_raw: false,
    })
}

#[cfg(not(feature = "lz4"))]
fn prepare_lz4<'a>(
    _bodies: impl Iterator<Item = &'a [u8]> + Clone,
    _min_savings: usize,
    _bytes: Vec<u8>,
    _scratch: &mut BodyEncodeScratch,
) -> Result<PreparedGroupBodies, CodecError> {
    Err(CodecError::UnsupportedCodec(CODEC))
}

pub(super) fn parse(
    input: &[u8],
    limits: DecodeLimits,
) -> Result<(Vec<ParsedEntry>, Body), CodecError> {
    require_len(input, HEADER_BYTES, "shared body header")?;
    if !input.starts_with(MAGIC) {
        return Err(CodecError::WrongMagic("shared body header"));
    }
    if digest_at(input, HASH) != hash_with_zeroed_range(HASH_CONTEXT, &input[..HEADER_BYTES], HASH)
    {
        return Err(CodecError::DigestMismatch("shared body header"));
    }
    let count = usize_from_u64(read_u64(input, 8))?;
    if count == 0 {
        return Err(CodecError::EmptyGroup);
    }
    enforce_limit("entry count", count, limits.max_entries)?;
    let encoded = usize_from_u64(read_u64(input, 16))?;
    let decoded = usize_from_u64(read_u64(input, 24))?;
    enforce_limit(
        "physical group decoded body bytes",
        decoded,
        limits.max_group_decoded_body_bytes,
    )?;
    if encoded >= decoded {
        return Err(CodecError::InvalidEntryLength);
    }
    let headers = count
        .checked_mul(ENTRY_HEADER_BYTES)
        .and_then(|n| n.checked_add(HEADER_BYTES))
        .ok_or(CodecError::LengthOverflow)?;
    let end = headers
        .checked_add(encoded)
        .ok_or(CodecError::LengthOverflow)?;
    let entry_bytes = align_up(end, 8)?;
    require_len(input, entry_bytes, "shared group bodies")?;
    if !all_zero(&input[end..entry_bytes]) {
        return Err(CodecError::NonZeroPadding);
    }
    let mut entries = Vec::with_capacity(count);
    let mut cursor = 0usize;
    for index in 0..count {
        let mut entry = parse_entry_representation(
            input,
            HEADER_BYTES + index * ENTRY_HEADER_BYTES,
            limits,
            true,
        )?;
        let next = cursor
            .checked_add(entry.decoded_body_bytes)
            .ok_or(CodecError::LengthOverflow)?;
        if next > decoded {
            return Err(CodecError::InvalidEntryLength);
        }
        entry.body = cursor..next;
        cursor = next;
        entries.push(entry);
    }
    if cursor != decoded {
        return Err(CodecError::InvalidEntryLength);
    }
    Ok((
        entries,
        Body {
            encoded: headers..end,
            entry_bytes,
        },
    ))
}

pub(super) fn decode<'a>(
    segment: &SegmentHeader,
    start: u64,
    input: &'a [u8],
    limits: DecodeLimits,
) -> Result<Vec<DecodedOperation<'a>>, CodecError> {
    let (entries, body) = parse(input, limits)?;
    if body.entry_bytes != input.len() {
        return Err(CodecError::InvalidEntryLength);
    }
    let first = &entries[0];
    let chain = ChainPosition::new(first.op_number, first.previous_digest);
    decode_operations(segment, start, chain, input, entries, Some(&body), limits)
        .map(|(operations, _)| operations)
}

#[cfg(all(test, feature = "lz4"))]
mod tests;
