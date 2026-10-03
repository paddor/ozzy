//! Canonical encoding directly from caller-owned record iterators.

use super::{
    AppendBatch, AppendBatchSummary, AppendBatchView, AppendTotals, Encoder, MessageId, Offset,
    OperationCodecError, OperationLimits, OperationOutput, OwnerEpoch, PREPARED_PAYLOAD,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, Range, TINY_RECORDS,
    encode_append_records, enforce_limit, records_are_tiny, validate_limits,
    validate_position_count,
};

/// Proof that one canonical one-batch APPEND was constructed from a validated
/// producer request. It remains paired with that body across position patching
/// and optional local whole-payload compression.
#[derive(Debug, Clone, Copy)]
pub struct ValidatedWireAppend {
    descriptor_bytes: u32,
    decoded_bytes: u32,
    encoded_bytes: u32,
    record_count: u32,
    nonzero_message_ids: bool,
}

impl ValidatedWireAppend {
    /// Recover state metadata from fixed fields while retaining descriptor and
    /// codec validation performed before canonical construction.
    pub fn summary(self, body: &[u8]) -> Result<AppendBatchSummary, OperationCodecError> {
        self.batch(body).map(|batch| batch.summary)
    }

    /// Borrow the trusted body layout without walking descriptors or LZ4 again.
    pub fn batch(self, body: &[u8]) -> Result<AppendBatchView<'_>, OperationCodecError> {
        super::view::validated_wire_batch(body, self)
    }

    pub(super) const fn descriptor_bytes(self) -> usize {
        self.descriptor_bytes as usize
    }

    pub(super) const fn decoded_bytes(self) -> usize {
        self.decoded_bytes as usize
    }

    pub(super) const fn encoded_bytes(self) -> Option<usize> {
        if self.encoded_bytes == 0 {
            None
        } else {
            Some(self.encoded_bytes as usize)
        }
    }

    pub(super) const fn record_count(self) -> usize {
        self.record_count as usize
    }

    pub(super) fn packed(&mut self, encoded_bytes: usize) -> Result<(), OperationCodecError> {
        self.encoded_bytes =
            u32::try_from(encoded_bytes).map_err(|_| OperationCodecError::LengthOverflow)?;
        Ok(())
    }

    pub(super) const fn nonzero_message_ids(self) -> bool {
        self.nonzero_message_ids
    }

    /// Whether this body stores a whole-payload LZ4 block.
    pub const fn payload_is_prepared(self) -> bool {
        self.encoded_bytes != 0
    }
}

/// Partition metadata shared by every record in a canonical append batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendHeader {
    pub partition: PartitionIncarnation,
    pub owner_epoch: OwnerEpoch,
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
    pub first_sequence: ProducerSequence,
    pub first_offset: Offset,
    pub append_timestamp_millis: u64,
}

impl From<&AppendBatch<'_>> for AppendHeader {
    fn from(batch: &AppendBatch<'_>) -> Self {
        Self {
            partition: batch.partition,
            owner_epoch: batch.owner_epoch,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            first_sequence: batch.first_sequence,
            first_offset: batch.first_offset,
            append_timestamp_millis: batch.append_timestamp_millis,
        }
    }
}

impl AppendHeader {
    pub(super) fn encode(
        self,
        records: usize,
        tiny: bool,
        prepared: bool,
        limits: OperationLimits,
        totals: &mut AppendTotals,
        encoder: &mut Encoder<'_, impl OperationOutput>,
    ) -> Result<(), OperationCodecError> {
        if records == 0 {
            return Err(OperationCodecError::EmptyAppend);
        }
        totals.add_records(records, limits)?;
        validate_position_count(self.first_sequence.get(), records)?;
        validate_position_count(self.first_offset.get(), records)?;
        encoder.id(self.partition.as_bytes())?;
        encoder.u64(self.owner_epoch.get())?;
        encoder.id(self.producer_id.as_bytes())?;
        encoder.u64(self.producer_epoch.get())?;
        encoder.u64(self.first_sequence.get())?;
        encoder.u64(self.first_offset.get())?;
        encoder.u64(self.append_timestamp_millis)?;
        let count = u32::try_from(records).map_err(|_| OperationCodecError::LengthOverflow)?;
        if count & TINY_RECORDS != 0 {
            return Err(OperationCodecError::LengthOverflow);
        }
        encoder.u32(
            count
                | if tiny { TINY_RECORDS } else { 0 }
                | if prepared { PREPARED_PAYLOAD } else { 0 },
        )
    }
}

/// Record descriptor layout chosen by an owner encoding its own groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescriptorLayout {
    /// One-byte lengths when every record is raw, single-part and at most 255
    /// bytes. Smallest body; readers must rebuild each descriptor.
    Compact,
    /// The general 24-byte descriptor, identical to the reader wire table.
    /// Readers forward a stored batch's descriptors and payload as one block.
    General,
}

/// Canonical append operation assembled one batch at a time. A wire batch
/// keeps the validated descriptor table and payload a writer sent; a record
/// batch encodes from iterators. Every batch is checked against the same
/// canonical limits, and any failure restores the output's original length.
#[derive(Debug)]
pub struct AppendBodyEncoder<'a, O: OperationOutput> {
    encoder: Encoder<'a, O>,
    totals: AppendTotals,
    remaining: usize,
    limits: OperationLimits,
}

impl<'a, O: OperationOutput> AppendBodyEncoder<'a, O> {
    /// Reserve the batch count. Exactly `batches` batches must follow.
    pub fn new(
        output: &'a mut O,
        batches: usize,
        limits: OperationLimits,
    ) -> Result<Self, OperationCodecError> {
        validate_limits(limits)?;
        if batches == 0 {
            return Err(OperationCodecError::EmptyAppend);
        }
        enforce_limit("append batch count", batches, limits.max_append_batches)?;
        let start = output.len();
        let mut encoder = Encoder::new(output, start, limits.max_body_bytes);
        if let Err(error) = encoder.count(batches) {
            encoder.restore_start();
            return Err(error);
        }
        Ok(Self {
            encoder,
            totals: AppendTotals::default(),
            remaining: batches,
            limits,
        })
    }

    /// Store writers' validated general descriptor tables and raw payloads
    /// unchanged as one batch. Each chunk is one contiguous request:
    /// descriptors without their count, then the exact payload those
    /// descriptors cover. `records` and `parts` are the chunk totals. The
    /// wire codec checked every chunk, and canonical admission validates the
    /// assembled body again.
    pub fn wire_batch<'b>(
        &mut self,
        header: AppendHeader,
        records: usize,
        parts: usize,
        chunks: impl Iterator<Item = (&'b [u8], &'b [u8])> + Clone,
    ) -> Result<(), OperationCodecError> {
        self.batch(|encoder, totals, limits| {
            header.encode(records, false, false, limits, totals, encoder)?;
            enforce_limit("append part count", parts, limits.max_parts)?;
            totals.add_parts(parts, limits)?;
            for (descriptors, _) in chunks.clone() {
                encoder.bytes(descriptors)?;
            }
            for (_, payload) in chunks {
                totals.add_payload(payload.len(), limits)?;
                encoder.payload(payload)?;
            }
            Ok(())
        })
    }

    /// Encode one batch from record iterators. Iterators must report exact
    /// lengths; cloning a record iterator must yield identical records.
    pub fn record_batch<'b, P, I>(
        &mut self,
        header: AppendHeader,
        records: I,
        layout: DescriptorLayout,
    ) -> Result<(), OperationCodecError>
    where
        P: AsRef<[u8]> + 'b,
        I: ExactSizeIterator<Item = (MessageId, ozzy_proto::data::Encoding, &'b [P])> + Clone,
    {
        self.batch(|encoder, totals, limits| {
            let records = records
                .map(|(id, encoding, parts)| (id, encoding, parts.iter().map(AsRef::as_ref)));
            let tiny = layout == DescriptorLayout::Compact && records_are_tiny(records.clone());
            header.encode(records.len(), tiny, false, limits, totals, encoder)?;
            encode_append_records(records, tiny, limits, totals, encoder)
        })
    }

    /// Return the encoded body's range once every reserved batch was added.
    pub fn finish(mut self) -> Result<Range<usize>, OperationCodecError> {
        if self.remaining != 0 {
            self.encoder.restore_start();
            return Err(OperationCodecError::EmptyAppend);
        }
        let start = self.encoder.start;
        Ok(start..self.encoder.finish())
    }

    fn batch(
        &mut self,
        encode: impl FnOnce(
            &mut Encoder<'a, O>,
            &mut AppendTotals,
            OperationLimits,
        ) -> Result<(), OperationCodecError>,
    ) -> Result<(), OperationCodecError> {
        if self.remaining == 0 {
            self.encoder.restore_start();
            return Err(OperationCodecError::LengthOverflow);
        }
        if let Err(error) = encode(&mut self.encoder, &mut self.totals, self.limits) {
            self.encoder.restore_start();
            return Err(error);
        }
        self.remaining -= 1;
        Ok(())
    }
}

/// Append canonical batches without constructing record descriptor vectors.
/// Iterators must report exact lengths; cloning a record iterator must yield
/// identical records. The ordinary codec checks all limits and positions.
/// On failure, output is restored to its original length and prefix.
pub fn append_record_batches<'a, P, I>(
    output: &mut impl OperationOutput,
    batches: impl ExactSizeIterator<Item = (AppendHeader, I)>,
    layout: DescriptorLayout,
    limits: OperationLimits,
) -> Result<Range<usize>, OperationCodecError>
where
    P: AsRef<[u8]> + 'a,
    I: ExactSizeIterator<Item = (MessageId, ozzy_proto::data::Encoding, &'a [P])> + Clone,
{
    let mut encoder = AppendBodyEncoder::new(output, batches.len(), limits)?;
    for (header, records) in batches {
        encoder.record_batch(header, records, layout)?;
    }
    encoder.finish()
}

/// Encode one validated wire batch without changing its payload representation.
/// Whole-APPEND LZ4 stores descriptors plus only the exact encoded block.
pub fn append_wire_record_batch(
    output: &mut impl OperationOutput,
    header: AppendHeader,
    records: ozzy_proto::data::RecordDescriptors<'_>,
    payload_encoding: ozzy_proto::append::PayloadEncoding,
    payload: &[u8],
    limits: OperationLimits,
) -> Result<(Range<usize>, ValidatedWireAppend), OperationCodecError> {
    validate_limits(limits)?;
    if records.is_empty() {
        return Err(OperationCodecError::EmptyAppend);
    }
    let proof = ValidatedWireAppend {
        descriptor_bytes: u32::try_from(records.encoded().len())
            .map_err(|_| OperationCodecError::LengthOverflow)?,
        decoded_bytes: u32::try_from(records.payload_bytes())
            .map_err(|_| OperationCodecError::LengthOverflow)?,
        encoded_bytes: match payload_encoding {
            ozzy_proto::append::PayloadEncoding::Raw => 0,
            ozzy_proto::append::PayloadEncoding::Lz4 => {
                u32::try_from(payload.len()).map_err(|_| OperationCodecError::LengthOverflow)?
            }
        },
        record_count: u32::try_from(records.len())
            .map_err(|_| OperationCodecError::LengthOverflow)?,
        nonzero_message_ids: records.nonzero_message_ids(),
    };
    let start = output.len();
    let mut encoder = Encoder::new(output, start, limits.max_body_bytes);
    let result = (|| {
        encoder.count(1)?;
        let mut totals = AppendTotals::default();
        header.encode(
            records.len(),
            false,
            payload_encoding == ozzy_proto::append::PayloadEncoding::Lz4,
            limits,
            &mut totals,
            &mut encoder,
        )?;
        enforce_limit("append part count", records.parts(), limits.max_parts)?;
        totals.add_payload(records.payload_bytes(), limits)?;
        encoder.bytes(records.encoded())?;
        match payload_encoding {
            ozzy_proto::append::PayloadEncoding::Raw => {
                if payload.len() != records.payload_bytes() {
                    return Err(OperationCodecError::LengthOverflow);
                }
                encoder.payload(payload)
            }
            ozzy_proto::append::PayloadEncoding::Lz4 => {
                if payload.is_empty() {
                    return Err(OperationCodecError::InvalidPreparedPayload);
                }
                encoder.u8(payload_encoding as u8)?;
                encoder.length(records.payload_bytes())?;
                encoder.length(payload.len())?;
                encoder.payload(payload)
            }
        }
    })();
    if let Err(error) = result {
        encoder.restore_start();
        return Err(error);
    }
    Ok((start..encoder.finish(), proof))
}

/// Encode one whole-APPEND LZ4 batch from decoded record descriptors. Payload
/// bytes are inspected only for their lengths; canonical storage keeps no copy.
pub fn append_prepared_record_batch<'a, P, I>(
    output: &mut impl OperationOutput,
    header: AppendHeader,
    records: I,
    encoded_payload: &[u8],
    limits: OperationLimits,
) -> Result<Range<usize>, OperationCodecError>
where
    P: ExactSizeIterator<Item = &'a [u8]> + Clone,
    I: ExactSizeIterator<Item = (MessageId, ozzy_proto::data::Encoding, P)> + Clone,
{
    validate_limits(limits)?;
    if records.len() == 0 || encoded_payload.is_empty() {
        return Err(OperationCodecError::EmptyAppend);
    }
    let start = output.len();
    let mut encoder = Encoder::new(output, start, limits.max_body_bytes);
    let result = (|| {
        encoder.count(1)?;
        let mut totals = AppendTotals::default();
        header.encode(
            records.len(),
            false,
            true,
            limits,
            &mut totals,
            &mut encoder,
        )?;
        encode_append_record_descriptors(records, limits, &mut totals, &mut encoder)?;
        encoder.u8(ozzy_proto::append::PayloadEncoding::Lz4 as u8)?;
        encoder.length(totals.payload_bytes)?;
        encoder.length(encoded_payload.len())?;
        encoder.payload(encoded_payload)
    })();
    if let Err(error) = result {
        encoder.restore_start();
        return Err(error);
    }
    Ok(start..encoder.finish())
}

fn encode_append_record_descriptors<'a, P, I>(
    records: I,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError>
where
    P: ExactSizeIterator<Item = &'a [u8]> + Clone,
    I: ExactSizeIterator<Item = (MessageId, ozzy_proto::data::Encoding, P)> + Clone,
{
    for (id, encoding, parts) in records {
        if encoding != ozzy_proto::data::Encoding::Raw
            || !encoding.validate(parts.len(), limits.max_parts, limits.max_payload_bytes)
        {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        totals.add_parts(parts.len(), limits)?;
        let count = u32::try_from(parts.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
        encoder.id(id.as_bytes())?;
        encoder.u32(count)?;
        for part in parts {
            totals.add_payload(part.len(), limits)?;
            encoder.length(part.len())?;
        }
    }
    Ok(())
}
