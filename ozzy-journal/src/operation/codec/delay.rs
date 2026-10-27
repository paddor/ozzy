//! Scheduled records and their release share the append batch codec.
//!
//! `Schedule` is one batch, its first schedule sequence, and one due time per
//! record. `Release` is one batch and the release key of its last record.

use super::AppendTotals;
use super::append::{decode_append_batch, encode_append_batch};
use super::primitives::{
    Encoder, ReadFields, Reader, enforce_limit, validate_limits, validate_position_count,
};
use crate::operation::{
    AppendBatch, AppendBatchView, OperationCodecError, OperationLimits, OperationOutput, Release,
    ReleaseKey, Schedule,
};

/// Bytes of a `Release` body beyond its records: the one-batch header and
/// the release key.
pub const RELEASE_FRAME_BYTES: usize = super::super::append_encoder::ONE_BATCH_HEADER_BYTES + 16;

/// Bytes of one record in the general batch layout: its descriptor head,
/// then a length and the bytes of each part. The tiny layout is never larger.
pub fn encoded_record_bytes<'a>(
    encoding: ozzy_proto::data::Encoding,
    parts: impl IntoIterator<Item = &'a [u8]>,
) -> usize {
    encoding.head_bytes() + parts.into_iter().map(|part| 4 + part.len()).sum::<usize>()
}

pub(super) fn encode_schedule(
    value: &Schedule<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    let records = value.batch.records.len();
    if records == 0 || value.due_millis.len() != records {
        return Err(OperationCodecError::InvalidSchedule);
    }
    validate_position_count(value.first_schedule, records)?;
    encode_batch(&value.batch, limits, encoder)?;
    encoder.u64(value.first_schedule)?;
    for due in &value.due_millis {
        encoder.u64(*due)?;
    }
    Ok(())
}

pub(super) fn decode_schedule<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<Schedule<'a>, OperationCodecError> {
    let (batch, records) = decode_batch::<MATERIALIZE, VALIDATE_PREPARED>(decoder, limits)?;
    let first_schedule = decoder.u64()?;
    validate_position_count(first_schedule, records)?;
    let mut due_millis = Vec::with_capacity(if MATERIALIZE { records } else { 0 });
    for _ in 0..records {
        let due = decoder.u64()?;
        if MATERIALIZE {
            due_millis.push(due);
        }
    }
    Ok(Schedule {
        batch,
        first_schedule,
        due_millis,
    })
}

pub(super) fn encode_release(
    value: &Release<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    if value.batch.records.is_empty() {
        return Err(OperationCodecError::InvalidSchedule);
    }
    encode_batch(&value.batch, limits, encoder)?;
    encoder.u64(value.through.millis)?;
    encoder.u64(value.through.schedule)
}

pub(super) fn decode_release<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<Release<'a>, OperationCodecError> {
    let (batch, _) = decode_batch::<MATERIALIZE, VALIDATE_PREPARED>(decoder, limits)?;
    Ok(Release {
        batch,
        through: ReleaseKey {
            millis: decoder.u64()?,
            schedule: decoder.u64()?,
        },
    })
}

/// Validate a whole `Schedule` body and view its batch, for retention.
/// Records stay borrowed; nothing is materialized.
pub fn decode_schedule_batch(
    input: &[u8],
    limits: OperationLimits,
) -> Result<AppendBatchView<'_>, OperationCodecError> {
    let (mut decoder, view) = view_batch(input, limits)?;
    validate_position_count(decoder.u64()?, view.summary.record_count)?;
    for _ in 0..view.summary.record_count {
        decoder.u64()?;
    }
    decoder.finish()?;
    Ok(view)
}

/// Validate a whole `Release` body and view its batch, for readers. Records
/// stay borrowed; nothing is materialized.
pub fn decode_release_batch(
    input: &[u8],
    limits: OperationLimits,
) -> Result<AppendBatchView<'_>, OperationCodecError> {
    let (mut decoder, view) = view_batch(input, limits)?;
    decoder.u64()?;
    decoder.u64()?;
    decoder.finish()?;
    Ok(view)
}

/// The one batch at the start of a whole body, and the reader after it.
fn view_batch(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(Reader<'_>, AppendBatchView<'_>), OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Reader::new(input);
    if decoder.count("append batch count", limits.max_append_batches)? != 1 {
        return Err(OperationCodecError::InvalidSchedule);
    }
    let (_, view) = decode_append_batch::<false, true, false>(
        &mut decoder,
        &mut AppendTotals::new(limits),
        None,
    )?;
    Ok((decoder, view))
}

fn encode_batch(
    batch: &AppendBatch<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.count(1)?;
    encode_append_batch(batch, &mut AppendTotals::new(limits), encoder)
}

/// One nonempty batch and its record count.
fn decode_batch<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<(AppendBatch<'a>, usize), OperationCodecError> {
    if decoder.count("append batch count", limits.max_append_batches)? != 1 {
        return Err(OperationCodecError::InvalidSchedule);
    }
    let (batch, view) = decode_append_batch::<MATERIALIZE, VALIDATE_PREPARED, false>(
        decoder,
        &mut AppendTotals::new(limits),
        None,
    )?;
    Ok((batch, view.summary.record_count))
}
