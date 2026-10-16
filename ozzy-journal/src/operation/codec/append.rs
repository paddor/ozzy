//! One bounded APPEND walker for validation, materialization, and views.

use super::primitives::{Encoder, ReadFields, Reader, enforce_limit};
use crate::operation::append_encoder::BatchLayout;
use crate::operation::{
    Append, AppendBatch, AppendBatchView, AppendHeader, AppendRecord, AppendRecordDescriptors,
    AppendRecordList, AppendRecords, BatchPayload, OperationCodecError, OperationLimits,
    OperationOutput, PREPARED_PAYLOAD, PreparedAppendPayload, RECORD_COUNT_MASK, RecordPosition,
    TINY_RECORDS,
};
use bytes::Bytes;
use ozzy_proto::MessageId;
use smallvec::SmallVec;

mod descriptors;
pub(in crate::operation) use descriptors::{
    AppendTotals, TinyTable, read_descriptor_head, validate_append_descriptors,
    validate_tiny_descriptors,
};

pub(in crate::operation) fn encode_append(
    value: &Append<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    if value.batches.is_empty() {
        return Err(OperationCodecError::EmptyAppend);
    }
    enforce_limit(
        "append batch count",
        value.batches.len(),
        limits.max_append_batches,
    )?;
    encoder.count(value.batches.len())?;
    let mut totals = AppendTotals::new(limits);
    for batch in &value.batches {
        encode_append_batch(batch, &mut totals, encoder)?;
    }
    Ok(())
}

pub(in crate::operation) fn encode_append_batch(
    batch: &AppendBatch<'_>,
    totals: &mut AppendTotals,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    let limits = totals.limits();
    if let Some(slices) = batch.records.packed_slices() {
        AppendHeader::from(batch).encode(
            batch.records.len(),
            BatchLayout::Compact,
            totals,
            encoder,
        )?;
        totals.add_parts(batch.records.len())?;
        for (ids, _, _) in slices.clone() {
            for id in ids {
                encoder.id(id.as_bytes())?;
            }
        }
        for (_, lengths, payload) in slices.clone() {
            totals.add_payload(payload.len())?;
            encoder.bytes(lengths)?;
        }
        for (_, _, payload) in slices {
            encoder.payload(payload)?;
        }
        return Ok(());
    }
    let records = batch
        .records
        .iter()
        .map(|r| (r.message_id, r.encoding, r.parts.iter()));
    let tiny = records_are_tiny(records.clone());
    AppendHeader::from(batch).encode(
        batch.records.len(),
        BatchLayout::records(tiny),
        totals,
        encoder,
    )?;
    if !tiny && let Some(chunks) = batch.records.encoded_slices() {
        // Native and canonical general descriptors have identical bytes.
        // Frames are immutable and validated; enforce this operation's possibly
        // tighter limits without rebuilding descriptors or per-record owners.
        for chunk in chunks.clone() {
            for record in chunk.iter() {
                if !record.encoding.validate(
                    record.parts.len(),
                    limits.max_parts,
                    limits.max_payload_bytes,
                ) {
                    return Err(OperationCodecError::EmptyRecordParts);
                }
                totals.add_parts(record.parts.len())?;
            }
            totals.add_payload(chunk.payload_bytes())?;
            encoder.bytes(chunk.encoded().0)?;
        }
        for chunk in chunks {
            encoder.payload(chunk.encoded().1)?;
        }
        return Ok(());
    }
    encode_append_records(records, tiny, totals, encoder)
}

pub(in crate::operation) fn records_are_tiny<'a, P: ExactSizeIterator<Item = &'a [u8]>>(
    records: impl Iterator<Item = (MessageId, ozzy_proto::data::Encoding, P)>,
) -> bool {
    records.into_iter().all(|(_, encoding, mut parts)| {
        encoding == ozzy_proto::data::Encoding::Raw
            && parts.len() == 1
            && parts
                .next()
                .is_some_and(|part| u8::try_from(part.len()).is_ok())
    })
}

pub(in crate::operation) fn encode_append_records<'a, P: ExactSizeIterator<Item = &'a [u8]>>(
    records: impl Iterator<Item = (MessageId, ozzy_proto::data::Encoding, P)> + Clone,
    tiny: bool,
    totals: &mut AppendTotals,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    let limits = totals.limits();
    for (id, encoding, mut parts) in records.clone() {
        if !encoding.validate(parts.len(), limits.max_parts, limits.max_payload_bytes) {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        totals.add_parts(parts.len())?;
        if tiny {
            encoder.id(id.as_bytes())?;
        } else {
            let mut descriptor = [0; 24];
            descriptor[..16].copy_from_slice(id.as_bytes());
            let count =
                u32::try_from(parts.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
            descriptor[16..20].copy_from_slice(&(count | encoding.tag()).to_be_bytes());
            let single = if parts.len() == 1 && encoding == ozzy_proto::data::Encoding::Raw {
                parts.next()
            } else {
                None
            };
            if let Some(part) = single {
                totals.add_payload(part.len())?;
                let len =
                    u32::try_from(part.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
                descriptor[20..].copy_from_slice(&len.to_be_bytes());
                encoder.bytes(&descriptor)?;
            } else {
                encoder.bytes(&descriptor[..20])?;
                if let ozzy_proto::data::Encoding::Lz4 { decoded_bytes } = encoding {
                    encoder.u32(decoded_bytes)?;
                }
                for part in parts {
                    totals.add_payload(part.len())?;
                    encoder.length(part.len())?;
                }
            }
        }
    }
    if tiny {
        for (_, _, mut parts) in records.clone() {
            let part = parts.next().ok_or(OperationCodecError::EmptyRecordParts)?;
            totals.add_payload(part.len())?;
            encoder
                .u8(u8::try_from(part.len()).map_err(|_| OperationCodecError::LengthOverflow)?)?;
        }
    }
    for (_, _, parts) in records {
        for part in parts {
            encoder.payload(part)?;
        }
    }
    Ok(())
}

pub(in crate::operation) fn decode_append<
    'a,
    const MATERIALIZE: bool,
    const VALIDATE_PREPARED: bool,
>(
    decoder: &mut Reader<'a>,
    limits: OperationLimits,
) -> Result<Append<'a>, OperationCodecError> {
    let batch_count = decoder.count("append batch count", limits.max_append_batches)?;
    if batch_count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    let mut batches = if MATERIALIZE {
        Vec::with_capacity(batch_count)
    } else {
        Vec::new()
    };
    let mut totals = AppendTotals::new(limits);
    for _ in 0..batch_count {
        let (batch, _) = decode_append_batch::<MATERIALIZE, VALIDATE_PREPARED, false>(
            decoder,
            &mut totals,
            None,
        )?;
        if MATERIALIZE {
            batches.push(batch);
        }
    }
    Ok(Append { batches })
}

pub(in crate::operation) fn decode_append_batch<
    'a,
    const MATERIALIZE: bool,
    const VALIDATE_PREPARED: bool,
    const ALLOW_EMPTY: bool,
>(
    decoder: &mut Reader<'a>,
    totals: &mut AppendTotals,
    positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(AppendBatch<'a>, AppendBatchView<'a>), OperationCodecError> {
    let header = AppendHeader::decode(decoder)?;
    let count_and_layout = decoder.u32()?;
    let tiny = count_and_layout & TINY_RECORDS != 0;
    let prepared = count_and_layout & PREPARED_PAYLOAD != 0;
    if tiny && prepared {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let record_count = (count_and_layout & RECORD_COUNT_MASK) as usize;
    enforce_limit(
        "append record count",
        record_count,
        totals.limits().max_records,
    )?;
    if record_count == 0 && (!ALLOW_EMPTY || tiny || prepared) {
        return Err(OperationCodecError::EmptyAppend);
    }
    header.admit(record_count, totals)?;

    let descriptors = decode_record_descriptors(decoder, record_count, tiny, totals, positions)?;
    if prepared && !descriptors.all_raw {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let payload = if prepared {
        BatchPayload::Prepared(decode_prepared_payload::<VALIDATE_PREPARED>(
            decoder,
            descriptors.payload_bytes,
        )?)
    } else {
        BatchPayload::Raw(AppendRecords::with_lengths(
            descriptors.bytes,
            descriptors.lengths,
            decoder.take(descriptors.payload_bytes)?,
            record_count,
        ))
    };
    let view = AppendBatchView {
        summary: header.summary(record_count, descriptors.nonzero_message_ids),
        append_timestamp_millis: header.append_timestamp_millis,
        payload,
        descriptors: AppendRecordDescriptors::new(
            descriptors.bytes,
            descriptors.lengths,
            record_count,
        ),
        uniform_part_bytes: descriptors.uniform_part_bytes,
    };
    Ok((materialize_batch::<MATERIALIZE>(&view)?, view))
}

struct BatchDescriptors<'a> {
    bytes: &'a [u8],
    lengths: Option<&'a [u8]>,
    payload_bytes: usize,
    nonzero_message_ids: bool,
    all_raw: bool,
    uniform_part_bytes: Option<usize>,
}

fn decode_record_descriptors<'a>(
    decoder: &mut Reader<'a>,
    record_count: usize,
    tiny: bool,
    totals: &mut AppendTotals,
    positions: Option<&mut Vec<RecordPosition>>,
) -> Result<BatchDescriptors<'a>, OperationCodecError> {
    // The canonical descriptor table already stores every ID and part length.
    // Validate it once, then reread those immutable bytes only when constructing
    // output records. This avoids a temporary Vec for every record's lengths.
    let mut descriptors = *decoder;
    let (payload_bytes, nonzero_message_ids, all_raw, lengths, uniform_part_bytes) = if tiny {
        let ids = decoder.take(
            record_count
                .checked_mul(16)
                .ok_or(OperationCodecError::LengthOverflow)?,
        )?;
        let lengths = decoder.take(record_count)?;
        let (bytes, nonzero) = validate_tiny_descriptors(
            TinyTable::new(ids, lengths, record_count)?,
            totals,
            positions,
        )?;
        (bytes, nonzero, true, Some(lengths), None)
    } else {
        let (bytes, nonzero, all_raw, uniform_part_bytes) =
            validate_append_descriptors(decoder, record_count, totals, positions)?;
        (bytes, nonzero, all_raw, None, uniform_part_bytes)
    };
    let descriptor_bytes = descriptors
        .take(decoder.offset() - descriptors.offset() - lengths.map_or(0, <[u8]>::len))?;
    Ok(BatchDescriptors {
        bytes: descriptor_bytes,
        lengths,
        payload_bytes,
        nonzero_message_ids,
        all_raw,
        uniform_part_bytes,
    })
}

pub(in crate::operation) fn materialize_batch<'a, const MATERIALIZE: bool>(
    view: &AppendBatchView<'a>,
) -> Result<AppendBatch<'a>, OperationCodecError> {
    let records = match &view.payload {
        _ if !MATERIALIZE => AppendRecordList::from(Vec::new()),
        BatchPayload::Prepared(prepared) => {
            materialize_prepared_records(view.descriptors(), *prepared)?
        }
        BatchPayload::Raw(records) => records
            .clone()
            .map(|record| AppendRecord {
                encoding: record.encoding,
                message_id: record.message_id,
                parts: record.parts.collect(),
            })
            .collect(),
    };
    let summary = view.summary;
    Ok(AppendBatch {
        partition: summary.partition,
        owner_epoch: summary.owner_epoch,
        producer_id: summary.producer_id,
        producer_epoch: summary.producer_epoch,
        first_sequence: summary.first_sequence,
        first_offset: summary.first_offset,
        append_timestamp_millis: view.append_timestamp_millis,
        records,
    })
}

fn decode_prepared_payload<'a, const VALIDATE: bool>(
    decoder: &mut Reader<'a>,
    expected_decoded_bytes: usize,
) -> Result<PreparedAppendPayload<'a>, OperationCodecError> {
    let encoding = ozzy_proto::append::PayloadEncoding::try_from(decoder.u8()?)
        .map_err(|_| OperationCodecError::InvalidPreparedPayload)?;
    let decoded_bytes = decoder.u32()? as usize;
    let encoded_bytes = decoder.u32()? as usize;
    let encoded = decoder.take(encoded_bytes)?;
    if encoding != ozzy_proto::append::PayloadEncoding::Lz4
        || decoded_bytes != expected_decoded_bytes
        || encoded.is_empty()
    {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    if VALIDATE && !ozzy_proto::append::lz4_block_is_valid(encoded, decoded_bytes) {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    Ok(PreparedAppendPayload {
        encoding,
        decoded_bytes,
        encoded,
    })
}

fn materialize_prepared_records<'a>(
    descriptors: AppendRecordDescriptors<'_>,
    prepared: PreparedAppendPayload<'_>,
) -> Result<AppendRecordList<'a>, OperationCodecError> {
    let payload = Bytes::from(
        lz4rip::block::decompress(prepared.encoded, prepared.decoded_bytes)
            .map_err(|_| OperationCodecError::InvalidPreparedPayload)?,
    );
    let mut offset = 0_usize;
    let mut records = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        if descriptor.encoding != ozzy_proto::data::Encoding::Raw {
            return Err(OperationCodecError::InvalidPreparedPayload);
        }
        let mut parts = SmallVec::<[Bytes; 2]>::new();
        for length in descriptor.part_lengths {
            let end = offset
                .checked_add(length)
                .filter(|&end| end <= payload.len())
                .ok_or(OperationCodecError::InvalidPreparedPayload)?;
            parts.push(payload.slice(offset..end));
            offset = end;
        }
        records.push(ozzy_proto::data::OwnedRecord {
            encoding: descriptor.encoding,
            message_id: descriptor.message_id,
            payload: parts.into_iter().collect(),
        });
    }
    if offset != payload.len() {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    Ok(AppendRecordList::owned(records))
}
