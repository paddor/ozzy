//! Write-group decoding: physical groups, operations, and indexed reads.

use super::{
    CodecError, DecodeLimits, ENTRY_HEADER_BYTES, ENTRY_MAGIC, GROUP_SEAL_BYTES, GroupStart,
    SEGMENT_HEADER_BYTES, SegmentHeader, WRITE_GROUP_ALIGNMENT, all_zero,
    body::{decode_entry_body, decode_lz4_body},
    chain_after,
    encode::aligned_group_bytes,
    enforce_limit,
    entry::{ParsedEntry, parse_entry},
    group_end_offset, require_len,
    seal::{GroupDigestBuilder, GroupSeal, SealRecord, read_seal, validate_seal},
    shared, span, validate_decode_limits,
};
use ozzy_journal::operation::{
    ChainPosition, Digest, OperationHeader, OperationKind, canonical_body_digest,
};
use ozzy_proto::GroupId;
use std::borrow::Cow;

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
    /// Highest group number durable when this group was prepared.
    pub durable_through: u64,
    /// Validated canonical operation entries in group order.
    pub operations: Vec<DecodedOperation<'a>>,
    /// Next canonical operation number and predecessor digest.
    pub next_chain: ChainPosition,
}

impl DecodedGroup<'_> {
    /// Physical bytes occupied by the validated complete group prefix.
    pub fn consumed_bytes(&self) -> usize {
        // Segment capacity bounds a validated group.
        (self.end_offset - self.start_offset) as usize
    }
}

#[derive(Debug)]
struct ParsedGroup {
    entries: Vec<ParsedEntry>,
    shared: Option<shared::Body>,
    end_offset: u64,
    seal: SealRecord,
}

/// Parsed entries of one physical extent and the bytes they index.
pub(super) struct ParsedExtent<'a, 'b> {
    /// Encoded extent bytes, starting at `start_offset`.
    pub(super) input: &'a [u8],
    /// Absolute segment byte offset of `input`.
    pub(super) start_offset: u64,
    pub(super) entries: Vec<ParsedEntry>,
    /// One compressed block holding every entry body, when present.
    pub(super) shared: Option<&'b shared::Body>,
}

impl ParsedExtent<'_, '_> {
    /// Absolute offset and aligned length of the extent holding `entry`.
    fn entry_extent(&self, entry: &ParsedEntry) -> Result<(u64, u64), CodecError> {
        let entry_start = if self.shared.is_some() {
            0
        } else {
            entry
                .body
                .start
                .checked_sub(ENTRY_HEADER_BYTES)
                .ok_or(CodecError::InvalidEntryLength)?
        };
        let entry_offset = self
            .start_offset
            .checked_add(u64::try_from(entry_start).map_err(|_| CodecError::LengthOverflow)?)
            .ok_or(CodecError::LengthOverflow)?;
        let entry_bytes = u64::try_from(
            self.shared
                .map_or(entry.end - entry_start, |body| body.entry_bytes),
        )
        .map_err(|_| CodecError::LengthOverflow)?;
        Ok((entry_offset, entry_bytes))
    }

    /// Check the combined decoded size, then decode the shared block, if any.
    fn decode_shared_body(&self, limits: DecodeLimits) -> Result<Option<Vec<u8>>, CodecError> {
        let group_decoded_bytes = self
            .entries
            .iter()
            .try_fold(0_usize, |total, entry| {
                total.checked_add(entry.decoded_body_bytes)
            })
            .ok_or(CodecError::LengthOverflow)?;
        enforce_limit(
            "physical group decoded body bytes",
            group_decoded_bytes,
            limits.max_group_decoded_body_bytes,
        )?;
        self.shared
            .map(|body| {
                decode_lz4_body(
                    span(self.input, body.encoded.clone(), "shared group body")?,
                    group_decoded_bytes,
                )
            })
            .transpose()
    }
}

/// Validate one complete physical group against exact segment and chain evidence.
pub fn decode_group<'a>(
    segment: &SegmentHeader,
    start: GroupStart,
    input: &'a [u8],
    limits: DecodeLimits,
) -> Result<DecodedGroup<'a>, CodecError> {
    validate_group_position(start.group_number, start.start_offset)?;
    validate_decode_limits(limits)?;
    let parsed = scan_physical_group(
        segment,
        start.group_number,
        start.start_offset,
        input,
        limits,
    )?;
    decode_parsed(segment, start, input, parsed, limits)
}

/// The durable mark of a valid group that ends at `end`, follows `damaged`,
/// the group that failed to decode at `from`, and starts at or after it.
/// `None` when no such group ends there. Groups from an earlier use of the
/// file have lower numbers and never validate against this header.
pub(crate) fn durable_mark_ending_at(
    segment: &SegmentHeader,
    image: &[u8],
    (damaged, from): (u64, u64),
    end: usize,
    limits: DecodeLimits,
) -> Option<u64> {
    let seal = image.get(end.checked_sub(GROUP_SEAL_BYTES)?..end)?;
    let recorded = read_seal(seal.try_into().ok()?)?;
    if recorded.end_offset != u64::try_from(end).ok()?
        || recorded.start_offset < from
        || recorded.group_number <= damaged
    {
        return None;
    }
    let input = image.get(usize::try_from(recorded.start_offset).ok()?..end)?;
    probe_durable_mark(
        segment,
        recorded.group_number,
        recorded.start_offset,
        input,
        limits,
    )
    .ok()
}

/// Validate a group whose predecessor is unknown: its chain starts at its
/// first entry, and framing, bodies, the chain within it, and the digest must
/// all hold.
fn probe_durable_mark(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    input: &[u8],
    limits: DecodeLimits,
) -> Result<u64, CodecError> {
    validate_group_position(group_number, start_offset)?;
    validate_decode_limits(limits)?;
    let parsed = scan_physical_group(segment, group_number, start_offset, input, limits)?;
    let first = parsed.entries.first().ok_or(CodecError::EmptyGroup)?;
    let chain = ChainPosition::new(first.op_number, first.previous_digest);
    let start = GroupStart::new(group_number, start_offset, chain);
    decode_parsed(segment, start, input, parsed, limits).map(|group| group.durable_through)
}

fn decode_parsed<'a>(
    segment: &SegmentHeader,
    start: GroupStart,
    input: &'a [u8],
    parsed: ParsedGroup,
    limits: DecodeLimits,
) -> Result<DecodedGroup<'a>, CodecError> {
    let ParsedGroup {
        entries,
        shared,
        end_offset,
        seal,
    } = parsed;
    let extent = ParsedExtent {
        input,
        start_offset: start.start_offset,
        entries,
        shared: shared.as_ref(),
    };
    let (operations, next_chain) = decode_operations(segment, start.chain, &extent, limits)?;
    let mut group_digest = GroupDigestBuilder::new(
        segment,
        start.group_number,
        start.start_offset,
        operations.len(),
    )?;
    for operation in &operations {
        group_digest.push(operation.digest);
    }
    let digest = group_digest.finish(end_offset, seal.durable_through);
    if digest != seal.digest {
        return Err(CodecError::DigestMismatch("physical group seal"));
    }

    Ok(DecodedGroup {
        group_number: start.group_number,
        start_offset: start.start_offset,
        end_offset,
        digest,
        durable_through: seal.durable_through,
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
pub(super) fn decode_standalone_operation<'a>(
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
    let extent = ParsedExtent {
        input,
        start_offset: entry_offset,
        entries: vec![parsed],
        shared: None,
    };
    let (mut operations, _) = decode_operations(segment, expected, &extent, limits)?;
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

fn scan_physical_group(
    segment: &SegmentHeader,
    group_number: u64,
    start_offset: u64,
    input: &[u8],
    limits: DecodeLimits,
) -> Result<ParsedGroup, CodecError> {
    let (entries, shared, cursor) = if shared::is_shared(input) {
        let (entries, body) = shared::parse(input, limits)?;
        let end = body.entry_bytes;
        (entries, Some(body), end)
    } else {
        let (entries, end) = parse_entries(input, limits)?;
        (entries, None, end)
    };

    let group_bytes = aligned_group_bytes(cursor)?;
    require_len(input, group_bytes, "physical group seal")?;
    let seal_offset = group_bytes - GROUP_SEAL_BYTES;
    if !all_zero(span(input, cursor..seal_offset, "physical group padding")?) {
        return Err(CodecError::NonZeroPadding);
    }
    let end_offset = group_end_offset(start_offset, group_bytes, segment.capacity)?;
    let seal = GroupSeal {
        group_number,
        start_offset,
        end_offset,
        entry_count: u64::try_from(entries.len()).map_err(|_| CodecError::LengthOverflow)?,
    };
    let seal = validate_seal(
        span(input, 0..group_bytes, "physical group")?,
        seal_offset,
        seal,
    )?;

    Ok(ParsedGroup {
        entries,
        shared,
        end_offset,
        seal,
    })
}

/// Parse the independently encoded entries at the start of one physical
/// group. Returns them and the byte offset after the last one.
fn parse_entries(
    input: &[u8],
    limits: DecodeLimits,
) -> Result<(Vec<ParsedEntry>, usize), CodecError> {
    let mut parsed = Vec::new();
    let mut cursor = 0_usize;
    while input.get(cursor..cursor.saturating_add(ENTRY_MAGIC.len())) == Some(ENTRY_MAGIC) {
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
    Ok((parsed, cursor))
}

pub(super) fn decode_operations<'a>(
    segment: &SegmentHeader,
    expected_chain: ChainPosition,
    extent: &ParsedExtent<'a, '_>,
    limits: DecodeLimits,
) -> Result<(Vec<DecodedOperation<'a>>, ChainPosition), CodecError> {
    let decoded = extent.decode_shared_body(limits)?;
    let mut chain = expected_chain;
    let mut operations = Vec::with_capacity(extent.entries.len());
    for entry in &extent.entries {
        let (entry_offset, entry_bytes) = extent.entry_extent(entry)?;
        let header = entry.header();
        validate_operation(segment, chain, &header)?;
        let body = match &decoded {
            Some(bytes) => {
                Cow::Owned(span(bytes, entry.body.clone(), "decoded entry body")?.to_vec())
            }
            None => decode_entry_body(extent.input, entry)?,
        };
        let body_digest = canonical_body_digest(body.as_ref());
        if body_digest != entry.body_digest {
            return Err(CodecError::DigestMismatch("canonical body"));
        }
        let digest = header.digest(body_digest);
        chain = chain_after(header.op_number, digest)?;
        operations.push(DecodedOperation {
            entry_offset,
            entry_bytes,
            group_id: header.group_id,
            configuration_epoch: header.configuration_epoch,
            original_view: header.original_view,
            op_number: header.op_number,
            previous_digest: header.previous_digest,
            digest,
            body_digest,
            kind: header.kind,
            body,
        });
    }
    Ok((operations, chain))
}

pub(super) fn validate_group_position(
    group_number: u64,
    start_offset: u64,
) -> Result<(), CodecError> {
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

pub(super) fn validate_operation(
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
