//! Entry header layout: encoding and parsing one operation entry.

use super::{
    CodecError, DecodeLimits, ENTRY_HEADER_BYTES, ENTRY_MAGIC, FORMAT_VERSION, align_up, all_zero,
    encode::PreparedEntry, enforce_limit, require_version, shared, span, usize_from_u64,
};
use ozzy_journal::integrity::hash_without_digest;
use ozzy_journal::operation::{Digest, OperationHeader, OperationKind};
use ozzy_proto::GroupId;
use ozzy_proto::layout::{self, Fixed};
use std::ops::Range;

const ENTRY_HEADER_HASH_CONTEXT: &str = "ozzy journal entry header v2";
const ENTRY_DIGEST_RANGE: Range<usize> = 136..168;

#[derive(Debug, Clone)]
pub(super) struct ParsedEntry {
    group_id: GroupId,
    configuration_epoch: u64,
    original_view: u64,
    pub(super) op_number: u64,
    pub(super) previous_digest: Digest,
    pub(super) body_digest: Digest,
    kind: OperationKind,
    pub(super) body: Range<usize>,
    pub(super) decoded_body_bytes: usize,
    pub(super) codec: u16,
    pub(super) end: usize,
}

impl ParsedEntry {
    /// Canonical identity recorded in this entry header.
    pub(super) fn header(&self) -> OperationHeader {
        OperationHeader {
            group_id: self.group_id,
            configuration_epoch: self.configuration_epoch,
            original_view: self.original_view,
            op_number: self.op_number,
            previous_digest: self.previous_digest,
            kind: self.kind,
        }
    }
}

pub(super) fn encode_entry_header(
    output: &mut [u8],
    entry: PreparedEntry,
    operation: &OperationHeader,
    body_digest: Digest,
) -> Result<(), CodecError> {
    let total_len = encoded_entry_len(entry.encoded_len)?;
    let header: &mut [u8; ENTRY_HEADER_BYTES] = layout::record_mut(output, entry.header_start)
        .map_err(|_| CodecError::InvalidEntryLength)?;
    header[0..4].copy_from_slice(ENTRY_MAGIC);
    header.put_u16::<4>(FORMAT_VERSION);
    header.put_u16::<6>(operation.kind as u16);
    header.put_u32::<8>(ENTRY_HEADER_BYTES as u32);
    header.put_u32::<12>(0);
    header.put_u64::<16>(u64::try_from(total_len).map_err(|_| CodecError::LengthOverflow)?);
    header.put_u64::<24>(u64::try_from(entry.encoded_len).map_err(|_| CodecError::LengthOverflow)?);
    header.put::<32, 16>(*operation.group_id.as_bytes());
    header.put_u64::<48>(operation.configuration_epoch);
    header.put_u64::<56>(operation.original_view);
    header.put_u64::<64>(operation.op_number);
    header.put::<72, 32>(*operation.previous_digest.as_bytes());
    header.put::<104, 32>(*body_digest.as_bytes());
    header
        .put_u64::<168>(u64::try_from(entry.decoded_len).map_err(|_| CodecError::LengthOverflow)?);
    header.put_u16::<176>(entry.codec);
    header.put_u16::<178>(0);
    header.put_u64::<180>(0);
    let header_digest =
        hash_without_digest(ENTRY_HEADER_HASH_CONTEXT, header, ENTRY_DIGEST_RANGE.start);
    header[ENTRY_DIGEST_RANGE].copy_from_slice(header_digest.as_bytes());
    Ok(())
}

pub(super) fn encoded_entry_len(body_len: usize) -> Result<usize, CodecError> {
    align_up(
        ENTRY_HEADER_BYTES
            .checked_add(body_len)
            .ok_or(CodecError::LengthOverflow)?,
        8,
    )
}

pub(super) fn parse_entry(
    input: &[u8],
    start: usize,
    limits: DecodeLimits,
) -> Result<ParsedEntry, CodecError> {
    parse_entry_representation(input, start, limits, false)
}

pub(super) fn parse_entry_representation(
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
    let header: &[u8; ENTRY_HEADER_BYTES] = layout::record(input, start)?;
    let kind = entry_kind(header)?;
    let lengths = entry_lengths(header, limits)?;
    let end = start
        .checked_add(lengths.total)
        .ok_or(CodecError::LengthOverflow)?;
    if input.len() < end {
        return Err(CodecError::Truncated {
            object: "journal entry",
            needed: lengths.total,
            available,
        });
    }
    let codec = entry_codec(header, lengths, shared)?;
    let expected_header_digest =
        Digest::from_bytes(header.array::<{ ENTRY_DIGEST_RANGE.start }, 32>());
    let actual_header_digest =
        hash_without_digest(ENTRY_HEADER_HASH_CONTEXT, header, ENTRY_DIGEST_RANGE.start);
    if expected_header_digest != actual_header_digest {
        return Err(CodecError::DigestMismatch("entry header"));
    }
    let body_start = start + ENTRY_HEADER_BYTES;
    let body_end = body_start
        .checked_add(lengths.encoded)
        .ok_or(CodecError::LengthOverflow)?;
    if !all_zero(span(input, body_end..end, "entry padding")?) {
        return Err(CodecError::NonZeroPadding);
    }
    Ok(ParsedEntry {
        group_id: GroupId::from_bytes(header.array::<32, 16>()),
        configuration_epoch: header.u64_at::<48>(),
        original_view: header.u64_at::<56>(),
        op_number: header.u64_at::<64>(),
        previous_digest: Digest::from_bytes(header.array::<72, 32>()),
        body_digest: Digest::from_bytes(header.array::<104, 32>()),
        kind,
        body: body_start..body_end,
        decoded_body_bytes: lengths.decoded,
        codec,
        end,
    })
}

/// Entry magic, format version, header length and flags; returns the kind.
fn entry_kind(header: &[u8; ENTRY_HEADER_BYTES]) -> Result<OperationKind, CodecError> {
    if header.array::<0, 4>() != *ENTRY_MAGIC {
        return Err(CodecError::WrongMagic("journal entry"));
    }
    require_version(header.u16_at::<4>())?;
    let kind_value = header.u16_at::<6>();
    let kind = OperationKind::try_from(kind_value)
        .map_err(|_| CodecError::UnsupportedOperationKind(kind_value))?;
    if header.u32_at::<8>() != ENTRY_HEADER_BYTES as u32 {
        return Err(CodecError::InvalidEntryLength);
    }
    if header.u32_at::<12>() != 0 {
        return Err(CodecError::UnsupportedFlags("entry"));
    }
    Ok(kind)
}

/// Declared byte lengths of one entry.
#[derive(Clone, Copy)]
struct EntryLengths {
    /// Aligned entry length, header included.
    total: usize,
    /// Encoded body bytes after the header.
    encoded: usize,
    /// Canonical body bytes after physical decoding.
    decoded: usize,
}

/// Consistent declared lengths inside the decode limits.
fn entry_lengths(
    header: &[u8; ENTRY_HEADER_BYTES],
    limits: DecodeLimits,
) -> Result<EntryLengths, CodecError> {
    let total = usize_from_u64(header.u64_at::<16>())?;
    let encoded = usize_from_u64(header.u64_at::<24>())?;
    let decoded = usize_from_u64(header.u64_at::<168>())?;
    let body_fits = encoded <= total.saturating_sub(ENTRY_HEADER_BYTES);
    if total < ENTRY_HEADER_BYTES || !total.is_multiple_of(8) || !body_fits {
        return Err(CodecError::InvalidEntryLength);
    }
    enforce_limit("entry bytes", total, limits.max_entry_bytes)?;
    enforce_limit("decoded body bytes", decoded, limits.max_decoded_body_bytes)?;
    Ok(EntryLengths {
        total,
        encoded,
        decoded,
    })
}

/// The body codec, which must match the representation, and the zeroed
/// codec flags and reserved bytes after it.
fn entry_codec(
    header: &[u8; ENTRY_HEADER_BYTES],
    lengths: EntryLengths,
    shared: bool,
) -> Result<u16, CodecError> {
    let codec = header.u16_at::<176>();
    let supported = if shared {
        codec == shared::CODEC
    } else {
        codec <= 1
    };
    if !supported {
        return Err(CodecError::UnsupportedCodec(codec));
    }
    if shared && (lengths.encoded != 0 || lengths.total != ENTRY_HEADER_BYTES) {
        return Err(CodecError::InvalidEntryLength);
    }
    if header.u16_at::<178>() != 0 {
        return Err(CodecError::UnsupportedFlags("entry codec"));
    }
    if !all_zero(header.slice::<180, 12>()) {
        return Err(CodecError::NonZeroReserved("entry"));
    }
    if codec == 0 && lengths.encoded != lengths.decoded {
        return Err(CodecError::InvalidEntryLength);
    }
    Ok(codec)
}
