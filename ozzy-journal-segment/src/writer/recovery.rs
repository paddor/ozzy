//! Pure recovery decisions, shared by blocking and asynchronous execution.

use super::asynchronous::Start;
use super::{
    BodyEncodeScratch, ChainPosition, DecodeLimits, JournalGeneration, RecoveryValidation,
    SegmentDigestBuilder, SegmentState, TailState, WriterError, WriterPosition,
};
use crate::{DecodedOperation, SegmentScan, cooperative::Budget};
use ozzy_journal::operation::validate_operation_body;
use std::ops::Range;

#[cfg(test)]
mod tests;

pub(super) struct Recovery {
    pub(super) writer: SegmentState,
    pub(super) truncate: Option<u64>,
    /// Seal blocks of later groups of this use, past the end of the log.
    pub(super) invalidate: Vec<Range<u64>>,
}

pub(super) async fn prepare_async(
    image: &[u8],
    start: Start,
    limits: DecodeLimits,
    validation: RecoveryValidation,
) -> Result<Recovery, WriterError> {
    let Start {
        generation,
        first_group_number,
        initial_chain,
    } = start;
    let scan = if validation.discard_damaged_tail {
        crate::codec::scan_segment_prefix_async(image, first_group_number, initial_chain, limits)
            .await?
    } else {
        crate::scan_segment_async(image, first_group_number, initial_chain, limits).await?
    };
    let mut checks = Checks::new(initial_chain, validation);
    let mut segment_digest = SegmentDigestBuilder::new(&scan.header);
    let mut budget = Budget::default();
    for group in &scan.groups {
        for operation in &group.operations {
            checks.operation(operation)?;
            budget.charge(operation.body.len()).await;
        }
        segment_digest.push(group.digest);
    }
    checks.finish(scan.next_chain)?;
    let later = match scan.tail {
        TailState::Truncated { .. } | TailState::Damaged { .. } => {
            unmarked_later_groups(image, &scan, limits, &mut budget).await?
        }
        TailState::Clean | TailState::ZeroFilled { .. } => Vec::new(),
    };
    let truncate = matches!(scan.tail, TailState::Truncated { .. }).then_some(scan.valid_bytes);
    // Truncation removes later groups. Otherwise each loses its seal, so a
    // rewrite of the damaged group cannot chain into it again.
    let invalidate = if truncate.is_some() {
        Vec::new()
    } else {
        let block = crate::WRITE_GROUP_ALIGNMENT as u64;
        later.into_iter().map(|end| end - block..end).collect()
    };
    Ok(finish(
        scan,
        generation,
        segment_digest,
        truncate,
        invalidate,
    ))
}

/// End offsets of the valid groups of this use after the first group that
/// failed to decode. The log may end at that group only if none of them marks
/// it durable. Earlier groups mark only their predecessors.
async fn unmarked_later_groups(
    image: &[u8],
    scan: &SegmentScan<'_>,
    limits: DecodeLimits,
    budget: &mut Budget,
) -> Result<Vec<u64>, WriterError> {
    let damaged = scan.next_group_number;
    let start = usize::try_from(scan.valid_bytes).map_err(|_| crate::CodecError::LengthOverflow)?;
    let alignment = crate::WRITE_GROUP_ALIGNMENT;
    let mut later = Vec::new();
    for end in (start + alignment..=image.len()).step_by(alignment) {
        budget.charge(alignment).await;
        let Some(mark) = crate::codec::durable_mark_ending_at(
            &scan.header,
            image,
            (damaged, scan.valid_bytes),
            end,
            limits,
        ) else {
            continue;
        };
        if mark >= damaged {
            return Err(WriterError::DurableGroupDamaged(damaged));
        }
        later.push(end as u64);
    }
    Ok(later)
}

/// Both execution paths consume the same canonical and protected-history checks.
/// A partial scan or canceled validation never produces a writer or repair plan.
struct Checks {
    initial: ChainPosition,
    validation: RecoveryValidation,
    matched: [bool; 2],
}

impl Checks {
    fn new(initial: ChainPosition, validation: RecoveryValidation) -> Self {
        Self {
            initial,
            matched: validation.protected.map(|prefix| prefix == Some(initial)),
            validation,
        }
    }

    fn operation(&mut self, operation: &DecodedOperation<'_>) -> Result<(), WriterError> {
        if let Some(limits) = self.validation.operation_limits {
            validate_operation_body(operation.kind, operation.body.as_ref(), limits)?;
            if let Some(expected) = self.validation.configuration_epoch
                && operation.configuration_epoch != expected
            {
                return Err(WriterError::ConfigurationMismatch {
                    op_number: operation.op_number,
                    actual: operation.configuration_epoch,
                    expected,
                });
            }
            if let Some(promised) = self.validation.promised_view
                && operation.original_view > promised
            {
                return Err(WriterError::ViewBeyondPromise {
                    op_number: operation.op_number,
                    actual: operation.original_view,
                    promised,
                });
            }
        }
        for (protected, matched) in self.validation.protected.iter().zip(&mut self.matched) {
            if let Some(protected) = protected
                && operation.op_number.checked_add(1) == Some(protected.next_op_number())
                && operation.digest == protected.previous_digest()
            {
                *matched = true;
            }
        }
        Ok(())
    }

    fn finish(self, end: ChainPosition) -> Result<(), WriterError> {
        for (protected, matched) in self.validation.protected.into_iter().zip(self.matched) {
            if let Some(protected) = protected {
                let next = protected.next_op_number();
                if next == 0 {
                    return Err(WriterError::ProtectedPrefixMismatch(u64::MAX));
                }
                if next < self.initial.next_op_number() || next > end.next_op_number() || !matched {
                    return Err(WriterError::ProtectedPrefixMismatch(next - 1));
                }
            }
        }
        Ok(())
    }
}

fn finish(
    scan: SegmentScan<'_>,
    generation: JournalGeneration,
    segment_digest: SegmentDigestBuilder,
    truncate: Option<u64>,
    invalidate: Vec<Range<u64>>,
) -> Recovery {
    let position = WriterPosition {
        generation,
        segment_id: scan.header.segment_id(),
        group_number: scan.next_group_number - 1,
        end_offset: scan.valid_bytes,
        decoded_body_bytes: scan.decoded_body_bytes,
        next_chain: scan.next_chain,
    };
    Recovery {
        writer: SegmentState {
            generation,
            header: scan.header,
            written: position,
            durable: position,
            next_group_number: scan.next_group_number,
            segment_digest,
            encode_buffer: Vec::new(),
            body_encode_scratch: Some(BodyEncodeScratch::default()),
            data_sync: false,
            faulted: false,
        },
        truncate,
        invalidate,
    }
}
