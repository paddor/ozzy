//! Shared scan state for synchronous tooling and cooperative shard recovery.

use super::{
    CodecError, DecodeLimits, DecodedGroup, DecodedOperation, ENTRY_HEADER_BYTES, ENTRY_MAGIC,
    GroupStart, SEGMENT_DIGEST_RANGE, SEGMENT_HEADER_BYTES, SegmentHeader, all_zero,
    decode::decode_standalone_operation, decode_group, decode_segment_header,
    encode_segment_header, enforce_limit, entry::parse_entry, shared, span, validate_decode_limits,
};
use crate::cooperative::Budget;
use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::{ChainPosition, Digest};
use ozzy_proto::layout::Fixed;
use std::borrow::Cow;

#[cfg(test)]
use crate::cooperative::TURN_BYTES;

const ZERO_CHUNK_BYTES: usize = 64 * 1024;

pub(crate) fn scan(
    input: &[u8],
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
    damaged_tail: bool,
) -> Result<SegmentScan<'_>, CodecError> {
    let mut scan = Scan::new(
        input,
        first_group_number,
        initial_chain,
        limits,
        damaged_tail,
    )?;
    let tail = loop {
        if let Some(tail) = scan.tail.take() {
            break tail;
        }
        scan.step()?;
    };
    Ok(scan.finish(tail))
}

/// Yield between bounded scan units, including long unused zero-filled tails.
/// One physical group remains indivisible and is bounded by decoder limits.
/// Encoded and decoded sizes both consume the byte allowance. Cancellation
/// releases private scan state and grants no recovery or repair authority.
pub(crate) async fn scan_segment_async(
    input: &[u8],
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
) -> Result<SegmentScan<'_>, CodecError> {
    scan_async(input, first_group_number, initial_chain, limits, false).await
}

/// Explicit recovery-only scan that may classify a damaged unprotected suffix.
pub(crate) async fn scan_segment_prefix_async(
    input: &[u8],
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
) -> Result<SegmentScan<'_>, CodecError> {
    scan_async(input, first_group_number, initial_chain, limits, true).await
}

async fn scan_async(
    input: &[u8],
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
    damaged_tail: bool,
) -> Result<SegmentScan<'_>, CodecError> {
    let mut scan = Scan::new(
        input,
        first_group_number,
        initial_chain,
        limits,
        damaged_tail,
    )?;
    let mut budget = Budget::default();
    let tail = loop {
        if let Some(tail) = scan.tail.take() {
            break tail;
        }
        let work = scan.step()?;
        if scan.tail.is_none() {
            budget.charge(work).await;
        }
    };
    Ok(scan.finish(tail))
}

struct Scan<'a> {
    input: &'a [u8],
    limits: DecodeLimits,
    damaged_tail: bool,
    result: SegmentScan<'a>,
    digest: SegmentDigestBuilder,
    offset: usize,
    zero_probe: usize,
    tail: Option<TailState>,
}

impl<'a> Scan<'a> {
    fn new(
        input: &'a [u8],
        first_group_number: u64,
        initial_chain: ChainPosition,
        limits: DecodeLimits,
        damaged_tail: bool,
    ) -> Result<Self, CodecError> {
        validate_decode_limits(limits)?;
        let header = decode_segment_header(input)?;
        if u64::try_from(input.len()).map_err(|_| CodecError::LengthOverflow)? > header.capacity {
            return Err(CodecError::SegmentExceedsCapacity);
        }
        if first_group_number == 0 {
            return Err(CodecError::InvalidGroupNumber);
        }
        let digest = SegmentDigestBuilder::new(&header);
        Ok(Self {
            input,
            limits,
            damaged_tail,
            result: SegmentScan {
                header,
                digest: digest.finish(),
                groups: Vec::new(),
                decoded_body_bytes: 0,
                valid_bytes: SEGMENT_HEADER_BYTES as u64,
                next_group_number: first_group_number,
                next_chain: initial_chain,
                tail: TailState::Clean,
            },
            digest,
            offset: SEGMENT_HEADER_BYTES,
            zero_probe: SEGMENT_HEADER_BYTES,
            tail: None,
        })
    }

    /// One group or one zero-tail chunk. No header or logical chain is skipped.
    fn step(&mut self) -> Result<usize, CodecError> {
        if self.offset == self.input.len() {
            self.tail = Some(TailState::Clean);
            return Ok(0);
        }
        let remaining = span(self.input, self.offset..self.input.len(), "segment tail")?;
        let mut checked = 0;
        if self.input.get(self.zero_probe) == Some(&0) {
            let end = self
                .zero_probe
                .saturating_add(ZERO_CHUNK_BYTES)
                .min(self.input.len());
            checked = end - self.zero_probe;
            if self.input.get(self.zero_probe..end).is_some_and(all_zero) {
                self.zero_probe = end;
                if end == self.input.len() {
                    self.tail = Some(TailState::ZeroFilled {
                        bytes: remaining.len(),
                    });
                }
                return Ok(checked);
            }
        }
        if self.result.groups.len() == self.limits.max_groups {
            return Err(CodecError::LimitExceeded {
                kind: "physical group count",
                actual: self.result.groups.len() + 1,
                limit: self.limits.max_groups,
            });
        }
        let start = GroupStart::new(
            self.result.next_group_number,
            u64::try_from(self.offset).map_err(|_| CodecError::LengthOverflow)?,
            self.result.next_chain,
        );
        match decode_group(&self.result.header, start, remaining, self.limits) {
            Ok(group) => {
                let work = self.accept(group)?;
                return Ok(checked.saturating_add(work));
            }
            Err(cause @ CodecError::Truncated { .. }) => {
                self.tail = Some(TailState::Truncated {
                    bytes: remaining.len(),
                    cause,
                });
            }
            Err(
                cause @ (CodecError::LimitExceeded { .. }
                | CodecError::CompressionScratchAllocation(_)),
            ) => return Err(cause),
            Err(cause) if self.damaged_tail => {
                self.tail = Some(TailState::Damaged {
                    bytes: remaining.len(),
                    cause,
                });
            }
            Err(error) => return Err(error),
        }
        Ok(checked)
    }

    /// Append one decoded group to the scan; returns its scan work.
    fn accept(&mut self, group: DecodedGroup<'a>) -> Result<usize, CodecError> {
        let decoded = group
            .operations
            .iter()
            .try_fold(0usize, |total, operation| {
                total.checked_add(operation.body.len())
            })
            .ok_or(CodecError::LengthOverflow)?;
        let total = self
            .result
            .decoded_body_bytes
            .checked_add(decoded)
            .ok_or(CodecError::LengthOverflow)?;
        if total > self.limits.max_segment_decoded_body_bytes {
            return Err(CodecError::SegmentDecodedBodyLimit {
                actual: total,
                limit: self.limits.max_segment_decoded_body_bytes,
            });
        }
        let consumed = group.consumed_bytes();
        self.offset = self
            .offset
            .checked_add(consumed)
            .ok_or(CodecError::LengthOverflow)?;
        self.result.next_group_number = self
            .result
            .next_group_number
            .checked_add(1)
            .ok_or(CodecError::InvalidGroupNumber)?;
        self.result.next_chain = group.next_chain;
        self.result.decoded_body_bytes = total;
        self.digest.push(group.digest);
        self.result.groups.push(group);
        self.zero_probe = self.offset;
        Ok(consumed.max(decoded))
    }

    fn finish(mut self, tail: TailState) -> SegmentScan<'a> {
        self.result.tail = tail;
        self.result.valid_bytes = self.offset as u64;
        self.result.digest = self.digest.finish();
        self.result
    }
}

#[cfg(test)]
mod tests;

const SEGMENT_HASH_CONTEXT: &str = "ozzy journal structural segment v2";

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
    let mut salvaged = Salvaged {
        entries: Vec::new(),
        decoded_bytes: 0,
        limit: limits.max_segment_decoded_body_bytes,
    };
    let mut cursor = SEGMENT_HEADER_BYTES;
    while cursor.saturating_add(ENTRY_HEADER_BYTES) <= input.len() {
        let rest = input.get(cursor..).unwrap_or_default();
        if shared::is_shared(rest)
            && let Ok((_, body)) = shared::parse(rest, limits)
            && let Some(entry) = rest.get(..body.entry_bytes)
            && let Ok(operations) = shared::decode(segment, cursor as u64, entry, limits)
        {
            cursor += body.entry_bytes;
            for operation in operations
                .into_iter()
                .filter(|op| (first..=last).contains(&op.op_number))
            {
                salvaged.keep(operation)?;
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
        let Some(Ok(operation)) = input
            .get(start..cursor)
            .map(|entry| decode_standalone_operation(segment, start as u64, entry, limits))
        else {
            continue;
        };
        salvaged.keep(operation)?;
    }
    Ok(salvaged.entries)
}

/// Salvaged operations with owned bodies, bounded by combined decoded bytes.
struct Salvaged {
    entries: Vec<DecodedOperation<'static>>,
    decoded_bytes: usize,
    limit: usize,
}

impl Salvaged {
    fn keep(&mut self, operation: DecodedOperation<'_>) -> Result<(), CodecError> {
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(operation.body.len())
            .ok_or(CodecError::LengthOverflow)?;
        enforce_limit(
            "salvaged decoded body bytes",
            self.decoded_bytes,
            self.limit,
        )?;
        self.entries.push(DecodedOperation {
            body: Cow::Owned(operation.body.into_owned()),
            ..operation
        });
        Ok(())
    }
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
        let header_digest =
            Digest::from_bytes(header_bytes.array::<{ SEGMENT_DIGEST_RANGE.start }, 32>());
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
