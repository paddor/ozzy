//! One partition's stored records and durable outcomes share the append codec.

use crate::operation::codec::encode_atomic;
use std::ops::Range;

use super::append_encoder::BatchLayout;
use super::codec::append::encode_append_batch;
use super::results::{read_results, write_results};
use super::{
    AppendBatchView, AppendHeader, AppendResults, AppendTotals, Encoder, OperationCodecError,
    OperationLimits, OperationOutput, ReadFields, Reader, RecordPosition, ResolvedAppend,
    decode_append_batch, enforce_limit, validate_limits,
};

/// Validated stored-record boundaries and attempt results, borrowing one body.
#[derive(Debug, Clone)]
pub struct ResolvedAppendView<'a> {
    /// Stored records only, in partition-offset order.
    pub batch: AppendBatchView<'a>,
    /// Every attempted sequence, including records discarded as duplicates.
    pub results: AppendResults<'a>,
}

/// Append one resolved operation atomically, including an empty stored batch.
pub fn append_resolved_record_batch(
    output: &mut impl OperationOutput,
    value: &ResolvedAppend<'_>,
    limits: OperationLimits,
) -> Result<Range<usize>, OperationCodecError> {
    encode_atomic(output, limits, |encoder| {
        encode_resolved_append(value, limits, encoder)
    })
}

/// Preserve a validated SDK descriptor table and payload block, adding only
/// canonical outcome metadata. No payload decode or recompression occurs.
pub fn append_resolved_wire_record_batch(
    output: &mut impl OperationOutput,
    batch: super::WireBatch<'_>,
    results: AppendResults<'_>,
    limits: OperationLimits,
) -> Result<Range<usize>, OperationCodecError> {
    validate_limits(limits)?;
    let (header, records) = (batch.header, batch.records);
    results.validate_positions(header.first_sequence, header.first_offset, records.len())?;
    let result_bytes = results.encoded_bytes()?;
    enforce_limit("append result bytes", result_bytes, limits.max_body_bytes)?;
    let body_limits = OperationLimits {
        max_body_bytes: limits.max_body_bytes - result_bytes,
        ..limits
    };
    let start = output.len();
    let result = (|| {
        super::append_wire_record_batch(output, batch, body_limits)?;
        super::append_results(output, results, limits)
    })();
    if let Err(error) = result {
        output.truncate(start);
        return Err(error);
    }
    Ok(start..output.len())
}

pub(super) fn encode_resolved_append(
    value: &ResolvedAppend<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    value.results.validate_positions(
        value.batch.first_sequence,
        value.batch.first_offset,
        value.batch.records.len(),
    )?;
    encoder.count(1)?;
    let mut totals = AppendTotals::new(limits);
    if value.batch.records.is_empty() {
        AppendHeader::from(&value.batch).encode(0, BatchLayout::Empty, &mut totals, encoder)?;
    } else {
        encode_append_batch(&value.batch, &mut totals, encoder)?;
    }
    write_results(encoder, value.results, limits)
}

/// Validate the entire resolved body without copying payloads or record tables.
pub fn decode_resolved_append(
    input: &[u8],
    limits: OperationLimits,
) -> Result<ResolvedAppendView<'_>, OperationCodecError> {
    decode_resolved_inner::<true>(input, limits, None)
}

/// Validate the body while recording the same stored-record byte selectors used
/// by ordinary APPEND. Failed validation restores the selector buffer length.
pub fn decode_resolved_append_indexed<'a>(
    input: &'a [u8],
    limits: OperationLimits,
    positions: &mut Vec<RecordPosition>,
) -> Result<ResolvedAppendView<'a>, OperationCodecError> {
    let start = positions.len();
    let result = decode_resolved_inner::<true>(input, limits, Some(positions));
    if result.is_err() {
        positions.truncate(start);
    }
    result
}

/// Skip rechecking a whole-payload block already validated for these exact
/// immutable bytes. Every descriptor, outcome, position, and limit is checked.
pub fn decode_resolved_append_with_validated_payload(
    input: &[u8],
    limits: OperationLimits,
) -> Result<ResolvedAppendView<'_>, OperationCodecError> {
    decode_resolved_inner::<false>(input, limits, None)
}

fn decode_resolved_inner<'a, const VALIDATE_PREPARED: bool>(
    input: &'a [u8],
    limits: OperationLimits,
    positions: Option<&mut Vec<RecordPosition>>,
) -> Result<ResolvedAppendView<'a>, OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Reader::new(input);
    let (_, view) =
        read_resolved_append::<false, VALIDATE_PREPARED>(&mut decoder, limits, positions)?;
    decoder.finish()?;
    Ok(view)
}

pub(super) fn read_resolved_append<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
    positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(ResolvedAppend<'a>, ResolvedAppendView<'a>), OperationCodecError> {
    if decoder.count("append batch count", limits.max_append_batches)? != 1 {
        return Err(OperationCodecError::InvalidAppendResults);
    }
    let (batch, view) = decode_append_batch::<MATERIALIZE, VALIDATE_PREPARED, true>(
        decoder,
        &mut AppendTotals::new(limits),
        positions,
    )?;
    let results = read_results(decoder, limits)?;
    results.validate_positions(
        view.summary.first_sequence,
        view.summary.first_offset,
        view.summary.record_count,
    )?;
    Ok((
        ResolvedAppend { batch, results },
        ResolvedAppendView {
            batch: view,
            results,
        },
    ))
}
