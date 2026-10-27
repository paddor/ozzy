//! Scheduled records and their release share the append batch codec.
//!
//! `Schedule` is one batch, its first schedule sequence, and one due time per
//! record. `Release` is one batch and the release key of its last record.

use super::AppendTotals;
use super::append::{decode_append_batch, encode_append_batch};
use super::primitives::{Encoder, ReadFields, Reader, validate_position_count};
use crate::operation::{
    AppendBatch, OperationCodecError, OperationLimits, OperationOutput, Release, ReleaseKey,
    Schedule,
};

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
