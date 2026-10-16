//! One LZ4 block for the bodies of a collected write group. Operation headers
//! remain separate and checksummed; canonical identity never depends on grouping.

use super::{
    BodyEncodeScratch, BodyEncoding, ChainPosition, CodecError, DecodeLimits, DecodedOperation,
    ENTRY_HEADER_BYTES, PreparedGroupBodies, SegmentHeader, align_up, all_zero,
    decode::{ParsedExtent, decode_operations},
    encode::group_body_encoding,
    enforce_limit,
    entry::{ParsedEntry, parse_entry_representation},
    prepare_group_bodies, require_len, span, usize_from_u64,
};
#[cfg(feature = "lz4")]
use super::{
    SmallVec,
    encode::{INLINE_GROUP_OPERATIONS, PreparedEntry},
    entry::encoded_entry_len,
};
use ozzy_journal::integrity::hash_without_digest;
use ozzy_journal::operation::Digest;
use ozzy_proto::layout::{self, Fixed};
use std::ops::Range;

pub(super) const HEADER_BYTES: usize = 64;
pub(super) const CODEC: u16 = 2;
const MAGIC: &[u8; 8] = b"OZYBODY\0";
const HASH_CONTEXT: &str = "ozzy journal shared body header";
const HASH: Range<usize> = 32..64;

pub(super) fn is_shared(input: &[u8]) -> bool {
    input.starts_with(MAGIC) || (input.len() < MAGIC.len() && MAGIC.starts_with(input))
}

/// The lengths a shared body header records.
#[derive(Debug, Clone, Copy)]
struct Header {
    /// Operations sharing the body.
    entries: usize,
    /// Compressed body bytes after the entry headers.
    encoded: usize,
    /// Combined decoded body bytes.
    decoded: usize,
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
    let Lz4Plan {
        entries,
        decoded,
        raw_bytes,
    } = plan_lz4(bodies.clone())?;
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
    let input = lz4_input(bodies.clone(), decoded, &mut scratch.joined)?;
    // Encode directly into the final physical write buffer, after the headers.
    let encoded = scratch
        .lz4_compressor
        .compress_into(
            input,
            bytes.get_mut(headers..).ok_or(CodecError::LengthOverflow)?,
        )
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
    seal_header(
        &mut bytes,
        Header {
            entries: entries.len(),
            encoded,
            decoded,
        },
    )?;
    Ok(PreparedGroupBodies {
        bytes,
        entries,
        decoded_body_bytes: decoded,
        entry_bytes,
        borrowed_raw: false,
    })
}

/// Entry headers and totals for one LZ4 group, before compression.
#[cfg(feature = "lz4")]
struct Lz4Plan {
    /// One entry header per body.
    entries: SmallVec<[PreparedEntry; INLINE_GROUP_OPERATIONS]>,
    /// Combined decoded body bytes.
    decoded: usize,
    /// Bytes the group would take with raw bodies.
    raw_bytes: usize,
}

#[cfg(feature = "lz4")]
fn plan_lz4<'a>(bodies: impl Iterator<Item = &'a [u8]>) -> Result<Lz4Plan, CodecError> {
    let mut entries = SmallVec::new();
    let mut decoded = 0usize;
    let mut raw_bytes = 0usize;
    for body in bodies {
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
    Ok(Lz4Plan {
        entries,
        decoded,
        raw_bytes,
    })
}

/// The compressor input: a lone body as is, or all bodies joined in scratch.
#[cfg(feature = "lz4")]
fn lz4_input<'a: 'b, 'b>(
    bodies: impl Iterator<Item = &'a [u8]> + Clone,
    decoded: usize,
    joined: &'b mut Vec<u8>,
) -> Result<&'b [u8], CodecError> {
    let mut lone = bodies.clone();
    if let (Some(body), None) = (lone.next(), lone.next()) {
        return Ok(body);
    }
    joined.clear();
    joined
        .try_reserve(decoded)
        .map_err(|_| CodecError::CompressionScratchAllocation("lz4"))?;
    for body in bodies {
        joined.extend_from_slice(body);
    }
    Ok(joined)
}

/// Write the checksummed shared body header at the start of `bytes`.
#[cfg(feature = "lz4")]
fn seal_header(bytes: &mut [u8], fields: Header) -> Result<(), CodecError> {
    let header: &mut [u8; HEADER_BYTES] = layout::record_mut(bytes, 0)?;
    header.put::<0, 8>(*MAGIC);
    header.put_u64::<8>(fields.entries as u64);
    header.put_u64::<16>(fields.encoded as u64);
    header.put_u64::<24>(fields.decoded as u64);
    let digest = hash_without_digest(HASH_CONTEXT, header, HASH.start);
    header.put::<{ HASH.start }, 32>(*digest.as_bytes());
    Ok(())
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
    let Header {
        entries: count,
        encoded,
        decoded,
    } = parse_header(input, limits)?;
    let headers = count
        .checked_mul(ENTRY_HEADER_BYTES)
        .and_then(|n| n.checked_add(HEADER_BYTES))
        .ok_or(CodecError::LengthOverflow)?;
    let end = headers
        .checked_add(encoded)
        .ok_or(CodecError::LengthOverflow)?;
    let entry_bytes = align_up(end, 8)?;
    require_len(input, entry_bytes, "shared group bodies")?;
    if !all_zero(span(input, end..entry_bytes, "shared group padding")?) {
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

/// Validate the shared body header and return the lengths it records.
fn parse_header(input: &[u8], limits: DecodeLimits) -> Result<Header, CodecError> {
    require_len(input, HEADER_BYTES, "shared body header")?;
    let header: &[u8; HEADER_BYTES] = layout::record(input, 0)?;
    if header.array::<0, 8>() != *MAGIC {
        return Err(CodecError::WrongMagic("shared body header"));
    }
    let expected = Digest::from_bytes(header.array::<{ HASH.start }, 32>());
    if expected != hash_without_digest(HASH_CONTEXT, header, HASH.start) {
        return Err(CodecError::DigestMismatch("shared body header"));
    }
    let count = usize_from_u64(header.u64_at::<8>())?;
    if count == 0 {
        return Err(CodecError::EmptyGroup);
    }
    enforce_limit("entry count", count, limits.max_entries)?;
    let encoded = usize_from_u64(header.u64_at::<16>())?;
    let decoded = usize_from_u64(header.u64_at::<24>())?;
    enforce_limit(
        "physical group decoded body bytes",
        decoded,
        limits.max_group_decoded_body_bytes,
    )?;
    if encoded >= decoded {
        return Err(CodecError::InvalidEntryLength);
    }
    Ok(Header {
        entries: count,
        encoded,
        decoded,
    })
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
    let first = entries.first().ok_or(CodecError::InvalidEntryLength)?;
    let chain = ChainPosition::new(first.op_number, first.previous_digest);
    let extent = ParsedExtent {
        input,
        start_offset: start,
        entries,
        shared: Some(&body),
    };
    decode_operations(segment, chain, &extent, limits).map(|(operations, _)| operations)
}

#[cfg(all(test, feature = "lz4"))]
mod tests;
