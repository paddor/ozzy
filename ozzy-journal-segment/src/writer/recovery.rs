//! Pure recovery decisions, shared by blocking and asynchronous execution.

use super::{
    BodyEncodeScratch, ChainPosition, DecodeLimits, JournalGeneration, RecoveryValidation,
    SegmentDigestBuilder, SegmentWriter, TailState, WriterError, WriterPosition, damaged_tail_end,
    scan_segment,
};
use crate::{DecodedOperation, SegmentScan, cooperative::Budget};
use ozzy_journal::operation::validate_operation_body;
use std::ops::Range;

#[cfg(test)]
mod tests;

pub(super) struct Recovery<I> {
    pub(super) writer: SegmentWriter<I>,
    pub(super) truncate: Option<u64>,
    pub(super) zero: Range<u64>,
}

pub(super) fn prepare<I>(
    io: I,
    image: &[u8],
    generation: JournalGeneration,
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
    validation: RecoveryValidation,
) -> Result<Recovery<I>, WriterError> {
    let scan = if validation.discard_damaged_tail {
        crate::codec::scan_segment_prefix(image, first_group_number, initial_chain, limits)?
    } else {
        scan_segment(image, first_group_number, initial_chain, limits)?
    };
    let mut checks = Checks::new(initial_chain, validation);
    let mut segment_digest = SegmentDigestBuilder::new(&scan.header);
    for group in &scan.groups {
        for operation in &group.operations {
            checks.operation(operation)?;
        }
        segment_digest.push(group.digest);
    }
    checks.finish(scan.next_chain)?;
    let truncate = matches!(scan.tail, TailState::Truncated { .. }).then_some(scan.valid_bytes);
    let zero = if matches!(scan.tail, TailState::Damaged { .. }) {
        scan.valid_bytes..damaged_tail_end(image, scan.valid_bytes as usize) as u64
    } else {
        0..0
    };
    Ok(finish(io, scan, generation, segment_digest, truncate, zero))
}

pub(super) async fn prepare_async(
    image: &[u8],
    generation: JournalGeneration,
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
    validation: RecoveryValidation,
) -> Result<Recovery<()>, WriterError> {
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
    let truncate = matches!(scan.tail, TailState::Truncated { .. }).then_some(scan.valid_bytes);
    let zero = if matches!(scan.tail, TailState::Damaged { .. }) {
        let start = scan.valid_bytes as usize;
        let mut end = image.len();
        while end > start {
            let block = ((end - 1) / crate::WRITE_GROUP_ALIGNMENT * crate::WRITE_GROUP_ALIGNMENT)
                .max(start);
            if image[block..end].iter().any(|byte| *byte != 0) {
                break;
            }
            budget.charge(end - block).await;
            end = block;
        }
        scan.valid_bytes..end as u64
    } else {
        0..0
    };
    Ok(finish((), scan, generation, segment_digest, truncate, zero))
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

fn finish<I>(
    io: I,
    scan: SegmentScan<'_>,
    generation: JournalGeneration,
    segment_digest: SegmentDigestBuilder,
    truncate: Option<u64>,
    zero: Range<u64>,
) -> Recovery<I> {
    let position = WriterPosition {
        generation,
        segment_id: scan.header.segment_id(),
        group_number: scan.next_group_number - 1,
        end_offset: scan.valid_bytes,
        decoded_body_bytes: scan.decoded_body_bytes,
        next_chain: scan.next_chain,
    };
    Recovery {
        writer: SegmentWriter {
            io,
            generation,
            header: scan.header,
            written: position,
            durable: position,
            next_group_number: scan.next_group_number,
            segment_digest,
            encode_buffer: Vec::new(),
            body_encode_scratch: Some(BodyEncodeScratch::default()),
            data_sync: false,
            direct: None,
            faulted: false,
        },
        truncate,
        zero,
    }
}
