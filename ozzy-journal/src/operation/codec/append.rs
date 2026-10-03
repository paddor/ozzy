//! One bounded APPEND walker for validation, materialization, and views.

use super::primitives::{Decoder, Encoder, enforce_limit, validate_position_count};
use crate::operation::{
    Append, AppendBatch, AppendBatchSummary, AppendBatchView, AppendHeader, AppendRecord,
    AppendRecordDescriptors, AppendRecordList, AppendRecords, OperationCodecError, OperationLimits,
    OperationOutput, PREPARED_PAYLOAD, PreparedAppendPayload, RECORD_COUNT_MASK, RecordPosition,
    TINY_RECORDS,
};
use bytes::Bytes;
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};
use smallvec::SmallVec;

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
    let mut totals = AppendTotals::default();
    for batch in &value.batches {
        encode_append_batch(batch, limits, &mut totals, encoder)?;
    }
    Ok(())
}

pub(in crate::operation) fn encode_append_batch(
    batch: &AppendBatch<'_>,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    if batch.records.is_packed() {
        AppendHeader::from(batch).encode(
            batch.records.len(),
            true,
            false,
            limits,
            totals,
            encoder,
        )?;
        totals.add_parts(batch.records.len(), limits)?;
        for (ids, _, _) in batch.records.packed_slices().expect("packed list") {
            for id in ids {
                encoder.id(id.as_bytes())?;
            }
        }
        for (_, lengths, payload) in batch.records.packed_slices().expect("packed list") {
            totals.add_payload(payload.len(), limits)?;
            encoder.bytes(lengths)?;
        }
        for (_, _, payload) in batch.records.packed_slices().expect("packed list") {
            encoder.payload(payload)?;
        }
        return Ok(());
    }
    let records = batch
        .records
        .iter()
        .map(|r| (r.message_id, r.encoding, r.parts.iter()));
    let tiny = records_are_tiny(records.clone());
    AppendHeader::from(batch).encode(batch.records.len(), tiny, false, limits, totals, encoder)?;
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
                totals.add_parts(record.parts.len(), limits)?;
            }
            totals.add_payload(chunk.payload_bytes(), limits)?;
            encoder.bytes(chunk.encoded().0)?;
        }
        for chunk in chunks {
            encoder.payload(chunk.encoded().1)?;
        }
        return Ok(());
    }
    encode_append_records(records, tiny, limits, totals, encoder)
}

pub(in crate::operation) fn records_are_tiny<'a, P: ExactSizeIterator<Item = &'a [u8]>>(
    records: impl Iterator<Item = (MessageId, ozzy_proto::data::Encoding, P)>,
) -> bool {
    records.into_iter().all(|(_, encoding, mut parts)| {
        encoding == ozzy_proto::data::Encoding::Raw
            && parts.len() == 1
            && u8::try_from(parts.next().unwrap().len()).is_ok()
    })
}

pub(in crate::operation) fn encode_append_records<'a, P: ExactSizeIterator<Item = &'a [u8]>>(
    records: impl Iterator<Item = (MessageId, ozzy_proto::data::Encoding, P)> + Clone,
    tiny: bool,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    for (id, encoding, mut parts) in records.clone() {
        if !encoding.validate(parts.len(), limits.max_parts, limits.max_payload_bytes) {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        totals.add_parts(parts.len(), limits)?;
        if tiny {
            encoder.id(id.as_bytes())?;
        } else {
            let mut descriptor = [0; 24];
            descriptor[..16].copy_from_slice(id.as_bytes());
            let count =
                u32::try_from(parts.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
            descriptor[16..20].copy_from_slice(&(count | encoding.tag()).to_be_bytes());
            if parts.len() == 1 && encoding == ozzy_proto::data::Encoding::Raw {
                let part = parts.next().unwrap();
                totals.add_payload(part.len(), limits)?;
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
                    totals.add_payload(part.len(), limits)?;
                    encoder.length(part.len())?;
                }
            }
        }
    }
    if tiny {
        for (_, _, mut parts) in records.clone() {
            let part = parts.next().expect("single-part compact record");
            totals.add_payload(part.len(), limits)?;
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
    decoder: &mut Decoder<'a>,
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
    let mut totals = AppendTotals::default();
    for _ in 0..batch_count {
        let (batch, _) = decode_append_batch::<MATERIALIZE, VALIDATE_PREPARED>(
            decoder,
            limits,
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
>(
    decoder: &mut Decoder<'a>,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(AppendBatch<'a>, AppendBatchView<'a>), OperationCodecError> {
    let partition = PartitionIncarnation::from_bytes(decoder.id()?);
    let owner_epoch = OwnerEpoch::new(decoder.u64()?);
    let producer_id = ProducerId::from_bytes(decoder.id()?);
    let producer_epoch = ProducerEpoch::new(decoder.u64()?);
    let first_sequence = ProducerSequence::new(decoder.u64()?);
    let first_offset = Offset::new(decoder.u64()?);
    let append_timestamp_millis = decoder.u64()?;
    let count_and_layout = decoder.u32()?;
    let tiny = count_and_layout & TINY_RECORDS != 0;
    let prepared = count_and_layout & PREPARED_PAYLOAD != 0;
    if tiny && prepared {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let record_count = (count_and_layout & RECORD_COUNT_MASK) as usize;
    enforce_limit("append record count", record_count, limits.max_records)?;
    if record_count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    totals.add_records(record_count, limits)?;
    validate_position_count(first_sequence.get(), record_count)?;
    validate_position_count(first_offset.get(), record_count)?;

    let descriptors =
        decode_record_descriptors(decoder, record_count, tiny, limits, totals, positions)?;
    if prepared && !descriptors.all_raw {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let payload = if prepared {
        None
    } else {
        Some(decoder.take(descriptors.payload_bytes)?)
    };
    let prepared_payload = prepared
        .then(|| decode_prepared_payload::<VALIDATE_PREPARED>(decoder, descriptors.payload_bytes))
        .transpose()?;
    let summary = AppendBatchSummary {
        partition,
        owner_epoch,
        producer_id,
        producer_epoch,
        first_sequence,
        first_offset,
        record_count,
        nonzero_message_ids: descriptors.nonzero_message_ids,
    };
    let view = AppendBatchView {
        summary,
        append_timestamp_millis,
        prepared_payload,
        records: payload.map(|payload| {
            AppendRecords::with_lengths(
                descriptors.bytes,
                descriptors.lengths,
                payload,
                record_count,
            )
        }),
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
    decoder: &mut Decoder<'a>,
    record_count: usize,
    tiny: bool,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    positions: Option<&mut Vec<RecordPosition>>,
) -> Result<BatchDescriptors<'a>, OperationCodecError> {
    // The canonical descriptor table already stores every ID and part length.
    // Validate it once, then reread those immutable bytes only when constructing
    // output records. This avoids a temporary Vec for every record's lengths.
    let descriptors = *decoder;
    let (payload_bytes, nonzero_message_ids, all_raw, lengths, uniform_part_bytes) = if tiny {
        let ids = decoder.take(
            record_count
                .checked_mul(16)
                .ok_or(OperationCodecError::LengthOverflow)?,
        )?;
        let lengths = decoder.take(record_count)?;
        let (bytes, nonzero) =
            validate_tiny_descriptors(ids, lengths, record_count, limits, totals, positions)?;
        (bytes, nonzero, true, Some(lengths), None)
    } else {
        let (bytes, nonzero, all_raw, uniform_part_bytes) =
            validate_append_descriptors(decoder, record_count, limits, totals, positions)?;
        (bytes, nonzero, all_raw, None, uniform_part_bytes)
    };
    let descriptor_end = decoder.offset - lengths.map_or(0, <[u8]>::len);
    let descriptor_bytes = &decoder.input[descriptors.offset..descriptor_end];
    Ok(BatchDescriptors {
        bytes: descriptor_bytes,
        lengths,
        payload_bytes,
        nonzero_message_ids,
        all_raw,
        uniform_part_bytes,
    })
}

fn materialize_batch<'a, const MATERIALIZE: bool>(
    view: &AppendBatchView<'a>,
) -> Result<AppendBatch<'a>, OperationCodecError> {
    let records = if MATERIALIZE {
        if let Some(prepared) = view.prepared_payload {
            materialize_prepared_records(view.descriptors(), prepared)?
        } else {
            view.records()
                .map(|record| AppendRecord {
                    encoding: record.encoding,
                    message_id: record.message_id,
                    parts: record.parts.collect(),
                })
                .collect()
        }
    } else {
        AppendRecordList::from(Vec::new())
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
    decoder: &mut Decoder<'a>,
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
        let mut parts = SmallVec::new();
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
            payload: parts,
        });
    }
    if offset != payload.len() {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    Ok(AppendRecordList::owned(records))
}

pub(in crate::operation) fn validate_tiny_descriptors(
    ids: &[u8],
    lengths: &[u8],
    count: usize,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    mut positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(usize, bool), OperationCodecError> {
    if ids.len()
        != count
            .checked_mul(16)
            .ok_or(OperationCodecError::LengthOverflow)?
        || lengths.len() != count
    {
        return Err(OperationCodecError::LengthOverflow);
    }
    totals.add_parts(count, limits)?;
    let mut bytes = 0_usize;
    for (index, &len) in lengths.iter().enumerate() {
        if let Some(positions) = positions.as_deref_mut() {
            positions.push(position(index * 16, bytes, 1)?);
        }
        bytes = bytes
            .checked_add(len as usize)
            .ok_or(OperationCodecError::LengthOverflow)?;
    }
    if let Some(positions) = positions {
        positions.push(position(ids.len(), bytes, 0)?);
    }
    totals.add_payload(bytes, limits)?;
    Ok((
        bytes,
        ids.as_chunks::<16>().0.iter().all(|id| id != &[0; 16]),
    ))
}

fn position(
    descriptor: usize,
    payload: usize,
    parts: usize,
) -> Result<RecordPosition, OperationCodecError> {
    Ok(RecordPosition {
        descriptor: u32::try_from(descriptor).map_err(|_| OperationCodecError::LengthOverflow)?,
        payload: u32::try_from(payload).map_err(|_| OperationCodecError::LengthOverflow)?,
        parts: u32::try_from(parts).map_err(|_| OperationCodecError::LengthOverflow)?,
    })
}

pub(in crate::operation) fn validate_append_descriptors(
    decoder: &mut Decoder<'_>,
    record_count: usize,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    mut positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(usize, bool, bool, Option<usize>), OperationCodecError> {
    let mut batch_payload_bytes = 0_usize;
    let mut nonzero_message_ids = true;
    let mut all_raw = true;
    let mut uniform_part_bytes = None;
    let table_start = decoder.offset;
    for index in 0..record_count {
        let descriptor_start = decoder.offset - table_start;
        let payload_start = batch_payload_bytes;
        nonzero_message_ids &= decoder.id()? != [0; 16];
        let tagged = decoder.u32()?;
        let part_count = (tagged & 0x00ff_ffff) as usize;
        let encoding = match tagged >> 24 {
            0 => ozzy_proto::data::Encoding::Raw,
            1 => ozzy_proto::data::Encoding::Lz4 {
                decoded_bytes: decoder.u32()?,
            },
            _ => return Err(OperationCodecError::EmptyRecordParts),
        };
        all_raw &= encoding == ozzy_proto::data::Encoding::Raw;
        if !encoding.validate(part_count, limits.max_parts, limits.max_payload_bytes) {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        if part_count == 0 {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        totals.add_parts(part_count, limits)?;
        for _ in 0..part_count {
            let length = decoder.u32()? as usize;
            totals.add_payload(length, limits)?;
            batch_payload_bytes = batch_payload_bytes
                .checked_add(length)
                .ok_or(OperationCodecError::LengthOverflow)?;
        }
        let single = (part_count == 1 && encoding == ozzy_proto::data::Encoding::Raw)
            .then_some(batch_payload_bytes - payload_start);
        if index == 0 {
            uniform_part_bytes = single;
        } else if uniform_part_bytes != single {
            uniform_part_bytes = None;
        }
        if let Some(positions) = positions.as_deref_mut() {
            positions.push(position(descriptor_start, payload_start, part_count)?);
        }
    }
    if let Some(positions) = positions {
        positions.push(position(
            decoder.offset - table_start,
            batch_payload_bytes,
            0,
        )?);
    }
    Ok((
        batch_payload_bytes,
        nonzero_message_ids,
        all_raw,
        uniform_part_bytes,
    ))
}

#[derive(Debug, Default)]
pub(in crate::operation) struct AppendTotals {
    records: usize,
    parts: usize,
    pub(in crate::operation) payload_bytes: usize,
}

impl AppendTotals {
    pub(in crate::operation) fn add_records(
        &mut self,
        count: usize,
        limits: OperationLimits,
    ) -> Result<(), OperationCodecError> {
        self.records = self
            .records
            .checked_add(count)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit("append record count", self.records, limits.max_records)
    }

    pub(in crate::operation) fn add_parts(
        &mut self,
        count: usize,
        limits: OperationLimits,
    ) -> Result<(), OperationCodecError> {
        self.parts = self
            .parts
            .checked_add(count)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit("append part count", self.parts, limits.max_parts)
    }

    pub(in crate::operation) fn add_payload(
        &mut self,
        bytes: usize,
        limits: OperationLimits,
    ) -> Result<(), OperationCodecError> {
        self.payload_bytes = self
            .payload_bytes
            .checked_add(bytes)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit(
            "append payload bytes",
            self.payload_bytes,
            limits.max_payload_bytes,
        )
    }
}
