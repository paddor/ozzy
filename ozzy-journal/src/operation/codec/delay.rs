//! Scheduled records, their release, and their carry share the append batch
//! codec.
//!
//! `Schedule` is one batch, its first schedule sequence, and one due time per
//! record. `Release` is one batch and the release key of its last record.
//! `Carry` is one batch, possibly empty, its two floor positions, and one
//! release key per record.

use super::AppendTotals;
use super::append::{decode_append_batch, encode_append_batch};
use super::primitives::{
    Encoder, ReadFields, Reader, enforce_limit, validate_limits, validate_position_count,
};
use crate::operation::{
    AppendBatch, AppendBatchView, Carry, HeldPosition, OperationBody, OperationCodecError,
    OperationKind, OperationLimits, OperationOutput, Release, ReleaseKey, Schedule,
};

/// Bytes of a `Release` body beyond its records: the one-batch header and
/// the release key.
pub const RELEASE_FRAME_BYTES: usize = super::super::append_encoder::ONE_BATCH_HEADER_BYTES + 16;

/// Bytes of a `Carry` body beyond its records and their release keys: the
/// one-batch header and the two floor positions. Each key takes 16 bytes.
pub const CARRY_FRAME_BYTES: usize = super::super::append_encoder::ONE_BATCH_HEADER_BYTES + 24;

/// Bytes of one record in the general batch layout: its descriptor head,
/// then a length and the bytes of each part. The tiny layout is never larger.
pub fn encoded_record_bytes<'a>(
    encoding: ozzy_proto::data::Encoding,
    parts: impl IntoIterator<Item = &'a [u8]>,
) -> usize {
    encoding.head_bytes() + parts.into_iter().map(|part| 4 + part.len()).sum::<usize>()
}

/// The body of a `Schedule`, `Release`, or `Carry`.
pub(super) fn decode<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    kind: OperationKind,
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<OperationBody<'a>, OperationCodecError> {
    Ok(match kind {
        OperationKind::Schedule => OperationBody::Schedule(decode_schedule::<
            MATERIALIZE,
            VALIDATE_PREPARED,
        >(decoder, limits)?),
        OperationKind::Release => OperationBody::Release(decode_release::<
            MATERIALIZE,
            VALIDATE_PREPARED,
        >(decoder, limits)?),
        OperationKind::Carry => OperationBody::Carry(
            decode_carry::<MATERIALIZE, VALIDATE_PREPARED>(decoder, limits)?,
        ),
        other => return Err(OperationCodecError::UnsupportedOperationKind(other as u16)),
    })
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

fn decode_schedule<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
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

fn decode_release<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
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

pub(super) fn encode_carry(
    value: &Carry<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    if value.keys.len() != value.batch.records.len() {
        return Err(OperationCodecError::InvalidSchedule);
    }
    if value.batch.records.is_empty() {
        encoder.count(1)?;
        crate::operation::AppendHeader::from(&value.batch).encode(
            0,
            crate::operation::append_encoder::BatchLayout::Empty,
            &mut AppendTotals::new(limits),
            encoder,
        )?;
    } else {
        encode_batch(&value.batch, limits, encoder)?;
    }
    for position in [value.from, value.through] {
        encoder.u64(position.operation)?;
        encoder.u32(position.index)?;
    }
    for key in &value.keys {
        encoder.u64(key.millis)?;
        encoder.u64(key.schedule)?;
    }
    Ok(())
}

fn decode_carry<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<Carry<'a>, OperationCodecError> {
    let (batch, records) =
        decode_batch_within::<MATERIALIZE, VALIDATE_PREPARED, true>(decoder, limits)?;
    let from = position(decoder)?;
    let through = position(decoder)?;
    let mut keys = Vec::with_capacity(if MATERIALIZE { records } else { 0 });
    for _ in 0..records {
        let key = ReleaseKey {
            millis: decoder.u64()?,
            schedule: decoder.u64()?,
        };
        if MATERIALIZE {
            keys.push(key);
        }
    }
    Ok(Carry {
        batch,
        from,
        through,
        keys,
    })
}

fn position(decoder: &mut Reader<'_>) -> Result<HeldPosition, OperationCodecError> {
    Ok(HeldPosition {
        operation: decoder.u64()?,
        index: decoder.u32()?,
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

/// Validate a whole `Carry` body and view its batch, for retention. Records
/// stay borrowed; nothing is materialized.
pub fn decode_carry_batch(
    input: &[u8],
    limits: OperationLimits,
) -> Result<AppendBatchView<'_>, OperationCodecError> {
    let (mut decoder, view) = view_batch_within::<true>(input, limits)?;
    position(&mut decoder)?;
    position(&mut decoder)?;
    for _ in 0..view.summary.record_count {
        decoder.u64()?;
        decoder.u64()?;
    }
    decoder.finish()?;
    Ok(view)
}

/// The one batch at the start of a whole body, and the reader after it.
fn view_batch(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(Reader<'_>, AppendBatchView<'_>), OperationCodecError> {
    view_batch_within::<false>(input, limits)
}

fn view_batch_within<const ALLOW_EMPTY: bool>(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(Reader<'_>, AppendBatchView<'_>), OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Reader::new(input);
    if decoder.count("append batch count", limits.max_append_batches)? != 1 {
        return Err(OperationCodecError::InvalidSchedule);
    }
    let (_, view) = decode_append_batch::<false, true, ALLOW_EMPTY>(
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
    decode_batch_within::<MATERIALIZE, VALIDATE_PREPARED, false>(decoder, limits)
}

/// One batch and its record count; empty only when `ALLOW_EMPTY`.
fn decode_batch_within<
    'a,
    const MATERIALIZE: bool,
    const VALIDATE_PREPARED: bool,
    const ALLOW_EMPTY: bool,
>(
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<(AppendBatch<'a>, usize), OperationCodecError> {
    if decoder.count("append batch count", limits.max_append_batches)? != 1 {
        return Err(OperationCodecError::InvalidSchedule);
    }
    let (batch, view) = decode_append_batch::<MATERIALIZE, VALIDATE_PREPARED, ALLOW_EMPTY>(
        decoder,
        &mut AppendTotals::new(limits),
        None,
    )?;
    Ok((batch, view.summary.record_count))
}
