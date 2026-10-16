//! Canonical encoding directly from caller-owned record iterators.

use crate::operation::codec::encode_atomic;
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence, append::PayloadEncoding, data::RecordDescriptors, layout::Fixed,
};
use std::ops::Range;

use super::{
    AppendBatch, AppendBatchSummary, AppendBatchView, AppendTotals, Encoder, OperationCodecError,
    OperationLimits, OperationOutput, PREPARED_PAYLOAD, RECORD_COUNT_MASK, ReadFields, Reader,
    TINY_RECORDS, encode_append_records, enforce_limit, records_are_tiny, validate_limits,
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
    part_count: u32,
    uniform_part_bytes: Option<u32>,
    nonzero_message_ids: bool,
}

/// Fixed bytes before the descriptors of a one-batch APPEND: the batch count
/// and the batch header.
pub(super) const ONE_BATCH_HEADER_BYTES: usize = 80;
/// Whole-payload codec fields: encoding, decoded and encoded lengths.
pub(super) const CODEC_METADATA_BYTES: usize = 9;

impl ValidatedWireAppend {
    fn new(
        records: RecordDescriptors<'_>,
        encoding: PayloadEncoding,
        payload: &[u8],
    ) -> Result<Self, OperationCodecError> {
        let length =
            |bytes: usize| u32::try_from(bytes).map_err(|_| OperationCodecError::LengthOverflow);
        Ok(Self {
            descriptor_bytes: length(records.encoded().len())?,
            decoded_bytes: length(records.payload_bytes())?,
            encoded_bytes: match encoding {
                PayloadEncoding::Raw => 0,
                PayloadEncoding::Lz4 => length(payload.len())?,
            },
            record_count: length(records.len())?,
            part_count: length(records.parts())?,
            uniform_part_bytes: records.uniform_part_bytes(),
            nonzero_message_ids: records.nonzero_message_ids(),
        })
    }

    /// Check that `body` has exactly the length and header count bits this
    /// proof describes. Returns where its descriptors end and the count.
    pub(super) fn check_layout(self, body: &[u8]) -> Result<(usize, u32), OperationCodecError> {
        let descriptor_end = ONE_BATCH_HEADER_BYTES
            .checked_add(self.descriptor_bytes())
            .ok_or(OperationCodecError::LengthOverflow)?;
        let codec_bytes = usize::from(self.payload_is_prepared()) * CODEC_METADATA_BYTES;
        let payload_bytes = self.encoded_bytes().unwrap_or(self.decoded_bytes());
        let expected = descriptor_end
            .checked_add(codec_bytes)
            .and_then(|bytes| bytes.checked_add(payload_bytes))
            .ok_or(OperationCodecError::LengthOverflow)?;
        let header: &[u8; ONE_BATCH_HEADER_BYTES] = body
            .first_chunk()
            .ok_or(OperationCodecError::InvalidPreparedPayload)?;
        let count = header.u32_at::<{ ONE_BATCH_HEADER_BYTES - 4 }>();
        if body.len() != expected
            || count & RECORD_COUNT_MASK != self.record_count
            || count & TINY_RECORDS != 0
            || (count & PREPARED_PAYLOAD != 0) != self.payload_is_prepared()
        {
            return Err(OperationCodecError::InvalidPreparedPayload);
        }
        Ok((descriptor_end, count))
    }

    pub(super) const fn uniform_part_bytes(self) -> Option<usize> {
        match self.uniform_part_bytes {
            Some(bytes) => Some(bytes as usize),
            None => None,
        }
    }
    /// Recover state metadata from fixed fields while retaining descriptor and
    /// codec validation performed before canonical construction.
    pub fn summary(self, body: &[u8]) -> Result<AppendBatchSummary, OperationCodecError> {
        super::view::validated_wire_batch(body, self).map(|batch| batch.summary)
    }

    /// Check the destination's limits and patched positions without walking
    /// validated descriptors or LZ4 again. The proof must belong to this body.
    pub fn batch(
        self,
        body: &[u8],
        limits: OperationLimits,
    ) -> Result<AppendBatchView<'_>, OperationCodecError> {
        validate_limits(limits)?;
        enforce_limit("operation body bytes", body.len(), limits.max_body_bytes)?;
        enforce_limit(
            "append record count",
            self.record_count(),
            limits.max_records,
        )?;
        enforce_limit(
            "append part count",
            self.part_count as usize,
            limits.max_parts,
        )?;
        enforce_limit(
            "append payload bytes",
            self.decoded_bytes(),
            limits.max_payload_bytes,
        )?;
        let batch = super::view::validated_wire_batch(body, self)?;
        validate_position_count(batch.summary.first_sequence.get(), self.record_count())?;
        validate_position_count(batch.summary.first_offset.get(), self.record_count())?;
        Ok(batch)
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
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Expected partition ownership fence.
    pub owner_epoch: OwnerEpoch,
    /// Producer identity scoped to this partition.
    pub producer_id: ProducerId,
    /// Expected producer-session fence.
    pub producer_epoch: ProducerEpoch,
    /// First contiguous producer-local sequence.
    pub first_sequence: ProducerSequence,
    /// First assigned partition-global record offset.
    pub first_offset: Offset,
    /// Primary-resolved Unix timestamp in milliseconds.
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
    pub(super) fn decode(decoder: &mut Reader<'_>) -> Result<Self, OperationCodecError> {
        Ok(Self {
            partition: PartitionIncarnation::from_bytes(decoder.id()?),
            owner_epoch: OwnerEpoch::new(decoder.u64()?),
            producer_id: ProducerId::from_bytes(decoder.id()?),
            producer_epoch: ProducerEpoch::new(decoder.u64()?),
            first_sequence: ProducerSequence::new(decoder.u64()?),
            first_offset: Offset::new(decoder.u64()?),
            append_timestamp_millis: decoder.u64()?,
        })
    }

    /// Count a batch's records and check that both of its position ranges
    /// can hold them.
    pub(super) fn admit(
        self,
        records: usize,
        totals: &mut AppendTotals,
    ) -> Result<(), OperationCodecError> {
        totals.add_records(records)?;
        if records != 0 {
            validate_position_count(self.first_sequence.get(), records)?;
            validate_position_count(self.first_offset.get(), records)?;
        }
        Ok(())
    }

    pub(super) const fn summary(
        self,
        record_count: usize,
        nonzero_message_ids: bool,
    ) -> AppendBatchSummary {
        AppendBatchSummary {
            partition: self.partition,
            owner_epoch: self.owner_epoch,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            first_sequence: self.first_sequence,
            first_offset: self.first_offset,
            record_count,
            nonzero_message_ids,
        }
    }

    pub(super) fn encode(
        self,
        records: usize,
        layout: BatchLayout,
        totals: &mut AppendTotals,
        encoder: &mut Encoder<'_, impl OperationOutput>,
    ) -> Result<(), OperationCodecError> {
        if (records == 0) != (layout == BatchLayout::Empty) {
            return Err(OperationCodecError::EmptyAppend);
        }
        self.admit(records, totals)?;
        encoder.id(self.partition.as_bytes())?;
        encoder.u64(self.owner_epoch.get())?;
        encoder.id(self.producer_id.as_bytes())?;
        encoder.u64(self.producer_epoch.get())?;
        encoder.u64(self.first_sequence.get())?;
        encoder.u64(self.first_offset.get())?;
        encoder.u64(self.append_timestamp_millis)?;
        let count = u32::try_from(records).map_err(|_| OperationCodecError::LengthOverflow)?;
        if count & (TINY_RECORDS | PREPARED_PAYLOAD) != 0 {
            return Err(OperationCodecError::LengthOverflow);
        }
        encoder.u32(
            count
                | match layout {
                    BatchLayout::Compact => TINY_RECORDS,
                    BatchLayout::Prepared => PREPARED_PAYLOAD,
                    BatchLayout::General | BatchLayout::Empty => 0,
                },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BatchLayout {
    General,
    Compact,
    Prepared,
    Empty,
}

impl BatchLayout {
    pub(super) const fn records(tiny: bool) -> Self {
        if tiny { Self::Compact } else { Self::General }
    }

    const fn payload(encoding: PayloadEncoding) -> Self {
        match encoding {
            PayloadEncoding::Raw => Self::General,
            PayloadEncoding::Lz4 => Self::Prepared,
        }
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
            totals: AppendTotals::new(limits),
            remaining: batches,
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
        self.batch(|encoder, totals| {
            header.encode(records, BatchLayout::General, totals, encoder)?;
            enforce_limit("append part count", parts, totals.limits().max_parts)?;
            totals.add_parts(parts)?;
            for (descriptors, _) in chunks.clone() {
                encoder.bytes(descriptors)?;
            }
            for (_, payload) in chunks {
                totals.add_payload(payload.len())?;
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
        self.batch(|encoder, totals| {
            let records = records
                .map(|(id, encoding, parts)| (id, encoding, parts.iter().map(AsRef::as_ref)));
            let tiny = layout == DescriptorLayout::Compact && records_are_tiny(records.clone());
            header.encode(records.len(), BatchLayout::records(tiny), totals, encoder)?;
            encode_append_records(records, tiny, totals, encoder)
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
        encode: impl FnOnce(&mut Encoder<'a, O>, &mut AppendTotals) -> Result<(), OperationCodecError>,
    ) -> Result<(), OperationCodecError> {
        if self.remaining == 0 {
            self.encoder.restore_start();
            return Err(OperationCodecError::LengthOverflow);
        }
        if let Err(error) = encode(&mut self.encoder, &mut self.totals) {
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

/// One validated wire batch as an SDK sent it: its canonical header, record
/// descriptors, and payload block in its wire encoding.
#[derive(Clone, Copy, Debug)]
pub struct WireBatch<'a> {
    /// Canonical batch header.
    pub header: AppendHeader,
    /// Record descriptors exactly as received.
    pub records: ozzy_proto::data::RecordDescriptors<'a>,
    /// Encoding of `payload`.
    pub encoding: ozzy_proto::append::PayloadEncoding,
    /// Raw payload bytes, or the exact LZ4 block.
    pub payload: &'a [u8],
}

/// Encode one validated wire batch without changing its payload representation.
/// Whole-APPEND LZ4 stores descriptors plus only the exact encoded block.
pub fn append_wire_record_batch(
    output: &mut impl OperationOutput,
    batch: WireBatch<'_>,
    limits: OperationLimits,
) -> Result<(Range<usize>, ValidatedWireAppend), OperationCodecError> {
    let WireBatch {
        header,
        records,
        encoding,
        payload,
    } = batch;
    validate_limits(limits)?;
    if records.is_empty() {
        return Err(OperationCodecError::EmptyAppend);
    }
    let proof = ValidatedWireAppend::new(records, encoding, payload)?;
    let range = encode_atomic(output, limits, |encoder| {
        encoder.count(1)?;
        let mut totals = AppendTotals::new(limits);
        header.encode(
            records.len(),
            BatchLayout::payload(encoding),
            &mut totals,
            encoder,
        )?;
        enforce_limit("append part count", records.parts(), limits.max_parts)?;
        totals.add_payload(records.payload_bytes())?;
        encoder.bytes(records.encoded())?;
        match encoding {
            PayloadEncoding::Raw => {
                if payload.len() != records.payload_bytes() {
                    return Err(OperationCodecError::LengthOverflow);
                }
                encoder.payload(payload)
            }
            PayloadEncoding::Lz4 => {
                if payload.is_empty() {
                    return Err(OperationCodecError::InvalidPreparedPayload);
                }
                encoder.u8(encoding as u8)?;
                encoder.length(records.payload_bytes())?;
                encoder.length(payload.len())?;
                encoder.payload(payload)
            }
        }
    })?;
    Ok((range, proof))
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
    if records.len() == 0 || encoded_payload.is_empty() {
        return Err(OperationCodecError::EmptyAppend);
    }
    encode_atomic(output, limits, |encoder| {
        encoder.count(1)?;
        let mut totals = AppendTotals::new(limits);
        header.encode(records.len(), BatchLayout::Prepared, &mut totals, encoder)?;
        encode_append_record_descriptors(records, &mut totals, encoder)?;
        encoder.u8(PayloadEncoding::Lz4 as u8)?;
        encoder.length(totals.payload_bytes)?;
        encoder.length(encoded_payload.len())?;
        encoder.payload(encoded_payload)
    })
}

fn encode_append_record_descriptors<'a, P, I>(
    records: I,
    totals: &mut AppendTotals,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError>
where
    P: ExactSizeIterator<Item = &'a [u8]> + Clone,
    I: ExactSizeIterator<Item = (MessageId, ozzy_proto::data::Encoding, P)> + Clone,
{
    let limits = totals.limits();
    for (id, encoding, parts) in records {
        if encoding != ozzy_proto::data::Encoding::Raw
            || !encoding.validate(parts.len(), limits.max_parts, limits.max_payload_bytes)
        {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        totals.add_parts(parts.len())?;
        let count = u32::try_from(parts.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
        encoder.id(id.as_bytes())?;
        encoder.u32(count)?;
        for part in parts {
            totals.add_payload(part.len())?;
            encoder.length(part.len())?;
        }
    }
    Ok(())
}
