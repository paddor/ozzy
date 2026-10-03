//! Public body codec with shared schema validation and byte limits.

use super::{
    AppendBatchView, AppendSummary, AppendView, Barrier, OperationBody, OperationCodecError,
    OperationKind, OperationLimits, OperationOutput, RecordPosition, decode_append_batches,
    decode_append_batches_indexed, decode_append_batches_with_validated_payload,
};
use ozzy_proto::OperationId;
use smallvec::SmallVec;
use std::ops::Range;

mod append;
mod control;
mod primitives;
pub(super) use append::{
    AppendTotals, decode_append_batch, encode_append_records, records_are_tiny,
    validate_append_descriptors, validate_tiny_descriptors,
};
use append::{decode_append, encode_append};
use control::{
    decode_assign, decode_create, decode_open_producer, decode_policy,
    decode_producer_result_floor, decode_progress, decode_trim, encode_assign, encode_create,
    encode_open_producer, encode_policy, encode_producer_result_floor, encode_progress,
    encode_trim,
};
pub(super) use primitives::{
    Decoder, Encoder, enforce_limit, validate_limits, validate_position_count,
};

/// Encode one typed body into its canonical network-byte-order representation.
pub fn encode_operation_body(
    body: &OperationBody<'_>,
    limits: OperationLimits,
) -> Result<Vec<u8>, OperationCodecError> {
    let mut output = Vec::new();
    append_operation_body(&mut output, body, limits)?;
    Ok(output)
}

/// Append one typed body in canonical network-byte order.
///
/// The returned range identifies this body in `output`. Bytes already present
/// in `output` are preserved. A failed encode restores its original length so
/// callers can safely reuse one arena for a complete operation group.
pub fn append_operation_body(
    output: &mut Vec<u8>,
    body: &OperationBody<'_>,
    limits: OperationLimits,
) -> Result<Range<usize>, OperationCodecError> {
    append_operation_body_to(output, body, limits)
}

/// Append through a reusable contiguous or segmented output. Failure restores
/// the original length. The codec preserves descriptor and payload ordering.
pub fn append_operation_body_to(
    output: &mut impl OperationOutput,
    body: &OperationBody<'_>,
    limits: OperationLimits,
) -> Result<Range<usize>, OperationCodecError> {
    validate_limits(limits)?;
    let start = output.len();
    let mut encoder = Encoder::new(output, start, limits.max_body_bytes);
    let result = (|| {
        match body {
            OperationBody::CreatePartition(value) => encode_create(value, limits, &mut encoder)?,
            OperationBody::OpenProducer(value) => encode_open_producer(*value, &mut encoder)?,
            OperationBody::Append(value) => encode_append(value, limits, &mut encoder)?,
            OperationBody::Progress(value) => encode_progress(*value, &mut encoder)?,
            OperationBody::Assign(value) => encode_assign(*value, &mut encoder)?,
            OperationBody::Trim(value) => encode_trim(*value, &mut encoder)?,
            OperationBody::PartitionPolicy(value) => encode_policy(*value, &mut encoder)?,
            OperationBody::Barrier(value) => encoder.id(value.operation_id.as_bytes())?,
            OperationBody::ProducerResultFloor(value) => {
                encode_producer_result_floor(*value, &mut encoder)?;
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        encoder.restore_start();
        return Err(error);
    }
    Ok(start..encoder.finish())
}

/// Validate exactly one canonical body and its configured schema limits.
///
/// Uses the same schema walker as [`decode_operation_body`] without allocating
/// record or part vectors. Text and opaque payload bytes stay borrowed.
/// This checks syntax, ranges, and framing, not canonical state transitions or
/// integrity digests. Callers remain responsible for those independent checks.
pub fn validate_operation_body(
    kind: OperationKind,
    input: &[u8],
    limits: OperationLimits,
) -> Result<(), OperationCodecError> {
    decode_body::<false, true>(kind, input, limits).map(|_| ())
}

/// Like [`validate_operation_body`], but skip whole-payload LZ4 blocks that the
/// caller already validated for these exact bytes, or produced itself.
/// Descriptors, decoded lengths, and every limit are still checked.
pub fn validate_operation_body_with_validated_payload(
    kind: OperationKind,
    input: &[u8],
    limits: OperationLimits,
) -> Result<(), OperationCodecError> {
    decode_body::<false, false>(kind, input, limits).map(|_| ())
}

/// Decode exactly one typed body without copying text or application payloads.
///
/// Append decoding allocates output batch/record vectors. Records with at most
/// two parts keep their part slices inline; larger multipart records also
/// allocate an output part vector. No per-record descriptor scratch is allocated.
pub fn decode_operation_body(
    kind: OperationKind,
    input: &[u8],
    limits: OperationLimits,
) -> Result<OperationBody<'_>, OperationCodecError> {
    decode_body::<true, true>(kind, input, limits)
}

/// Validate an append and return only metadata needed for state admission.
///
/// Uses the same bounded schema walker as full decoding, including every record
/// and part descriptor, cumulative limits, overflow, and trailing-byte checks.
/// No record/part vectors or payload copies are made. More than four partition
/// batches allocate summary storage; the ordinary single-partition path does not.
pub fn decode_append_summary(
    input: &[u8],
    limits: OperationLimits,
) -> Result<AppendSummary, OperationCodecError> {
    decode_append_summary_and_view(input, limits).map(|(summary, _)| summary)
}

/// Validate append metadata once and retain a borrowed view for derived indexes.
/// Both outputs describe the same completely validated immutable operation.
pub fn decode_append_summary_and_view(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(AppendSummary, AppendView<'_>), OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Decoder::new(input);
    let count = decoder.count("append batch count", limits.max_append_batches)?;
    if count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    let mut batches = SmallVec::with_capacity(count);
    let mut totals = AppendTotals::default();
    for _ in 0..count {
        let (_, view) =
            decode_append_batch::<false, true>(&mut decoder, limits, &mut totals, None)?;
        batches.push(view.summary);
    }
    decoder.finish()?;
    Ok((
        AppendSummary { batches },
        AppendView::validated(input, limits),
    ))
}

/// Validate once and retain batch boundaries for immediate index construction.
/// Both outputs describe the same immutable bytes. Up to four batches allocate
/// no heap storage; record tables and payloads remain borrowed.
pub fn decode_append_summary_and_batches(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(AppendSummary, SmallVec<[AppendBatchView<'_>; 4]>), OperationCodecError> {
    let views = decode_append_batches(input, limits)?;
    let summary = AppendSummary {
        batches: views.iter().map(|view| view.summary).collect(),
    };
    Ok((summary, views))
}

/// Validate once, retaining batch boundaries and every record's byte position
/// for an owner's read index, so admission never walks the records again.
/// Positions are appended per batch in canonical order, each batch ending
/// with an end checkpoint.
pub fn decode_append_summary_and_batches_indexed<'a>(
    input: &'a [u8],
    limits: OperationLimits,
    positions: &mut Vec<RecordPosition>,
) -> Result<(AppendSummary, SmallVec<[AppendBatchView<'a>; 4]>), OperationCodecError> {
    let views = decode_append_batches_indexed(input, limits, positions)?;
    let summary = AppendSummary {
        batches: views.iter().map(|view| view.summary).collect(),
    };
    Ok((summary, views))
}

/// Validate descriptors and retain batch boundaries without revalidating a
/// whole-payload codec already checked before canonical body construction.
pub fn decode_append_summary_and_batches_with_validated_payload(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(AppendSummary, SmallVec<[AppendBatchView<'_>; 4]>), OperationCodecError> {
    let views = decode_append_batches_with_validated_payload(input, limits)?;
    let summary = AppendSummary {
        batches: views.iter().map(|view| view.summary).collect(),
    };
    Ok((summary, views))
}

// Validation and materialization share every schema check. The false variant
// never exposes its empty append vectors outside this module.
fn decode_body<const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    kind: OperationKind,
    input: &[u8],
    limits: OperationLimits,
) -> Result<OperationBody<'_>, OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Decoder::new(input);
    let body = match kind {
        OperationKind::CreatePartition => {
            OperationBody::CreatePartition(decode_create(&mut decoder, limits)?)
        }
        OperationKind::OpenProducer => {
            OperationBody::OpenProducer(decode_open_producer(&mut decoder)?)
        }
        OperationKind::Append => OperationBody::Append(decode_append::<
            MATERIALIZE,
            VALIDATE_PREPARED,
        >(&mut decoder, limits)?),
        OperationKind::Progress => OperationBody::Progress(decode_progress(&mut decoder)?),
        OperationKind::Assign => OperationBody::Assign(decode_assign(&mut decoder)?),
        OperationKind::Trim => OperationBody::Trim(decode_trim(&mut decoder)?),
        OperationKind::PartitionPolicy => {
            OperationBody::PartitionPolicy(decode_policy(&mut decoder)?)
        }
        OperationKind::Barrier => OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes(decoder.id()?),
        }),
        OperationKind::ProducerResultFloor => {
            OperationBody::ProducerResultFloor(decode_producer_result_floor(&mut decoder)?)
        }
    };
    decoder.finish()?;
    Ok(body)
}
