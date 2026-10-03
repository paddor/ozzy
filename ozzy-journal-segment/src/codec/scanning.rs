//! Shared scan state for synchronous tooling and cooperative shard recovery.

use super::{
    ChainPosition, CodecError, DecodeLimits, SEGMENT_HEADER_BYTES, SegmentDigestBuilder,
    SegmentScan, TailState, all_zero, decode_group, decode_segment_header, validate_decode_limits,
};

const ZERO_CHUNK_BYTES: usize = 64 * 1024;
use crate::cooperative::Budget;
#[cfg(test)]
use crate::cooperative::TURN_BYTES;

pub(super) fn scan(
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
    while scan.tail.is_none() {
        scan.step()?;
    }
    Ok(scan.finish())
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
    while scan.tail.is_none() {
        let work = scan.step()?;
        if scan.tail.is_none() {
            budget.charge(work).await;
        }
    }
    Ok(scan.finish())
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
        let remaining = &self.input[self.offset..];
        let mut checked = 0;
        if self.input[self.zero_probe] == 0 {
            let end = self
                .zero_probe
                .saturating_add(ZERO_CHUNK_BYTES)
                .min(self.input.len());
            checked = end - self.zero_probe;
            if all_zero(&self.input[self.zero_probe..end]) {
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
        match decode_group(
            &self.result.header,
            self.result.next_group_number,
            u64::try_from(self.offset).map_err(|_| CodecError::LengthOverflow)?,
            self.result.next_chain,
            remaining,
            self.limits,
        ) {
            Ok(group) => {
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
                return Ok(checked.saturating_add(consumed.max(decoded)));
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

    fn finish(mut self) -> SegmentScan<'a> {
        self.result.tail = self.tail.expect("complete scan");
        self.result.valid_bytes = self.offset as u64;
        self.result.digest = self.digest.finish();
        self.result
    }
}

#[cfg(test)]
mod tests;
