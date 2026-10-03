//! Borrowed canonical append views, validated by the ordinary schema walker.

use std::iter::FusedIterator;

use ozzy_proto::MessageId;
use smallvec::SmallVec;

use super::{
    AppendBatchSummary, AppendTotals, Decoder, OperationCodecError, OperationKind, OperationLimits,
    PREPARED_PAYLOAD, RECORD_COUNT_MASK, RecordPosition, TINY_RECORDS, ValidatedWireAppend,
    decode_append_batch, enforce_limit, validate_append_descriptors, validate_limits,
    validate_operation_body, validate_tiny_descriptors,
};

const WIRE_HEADER_BYTES: usize = 80;
const CODEC_METADATA_BYTES: usize = 9;

/// Fully validated canonical append bytes. Iteration allocates no record tables
/// and borrows every payload part from the input. This establishes syntax only,
/// not commit, authority, or a canonical state transition.
#[derive(Debug, Clone, Copy)]
pub struct AppendView<'a> {
    input: &'a [u8],
    limits: OperationLimits,
}

/// Validate the complete operation before exposing any batches or records.
/// Uses the same schema checks as materialized decoding, including cumulative
/// limits and trailing bytes. No allocation or payload copy occurs.
pub fn decode_append_view(
    input: &[u8],
    limits: OperationLimits,
) -> Result<AppendView<'_>, OperationCodecError> {
    validate_operation_body(OperationKind::Append, input, limits)?;
    Ok(AppendView { input, limits })
}

/// Validate once and retain each batch's descriptor and payload boundaries.
///
/// Unlike [`AppendView::batches`], iteration need not walk the schema again to
/// find those boundaries. Up to four batches stay inline; larger operations
/// allocate only batch views, never record tables or copied payloads. Nothing is
/// returned until all cumulative limits and trailing bytes have been checked.
pub fn decode_append_batches(
    input: &[u8],
    limits: OperationLimits,
) -> Result<SmallVec<[AppendBatchView<'_>; 4]>, OperationCodecError> {
    decode_append_batches_inner::<true>(input, limits, None)
}

/// Validate as [`decode_append_batches`] while recording every record's
/// batch-relative byte position for an owner's read index.
pub fn decode_append_batches_indexed<'a>(
    input: &'a [u8],
    limits: OperationLimits,
    positions: &mut Vec<RecordPosition>,
) -> Result<SmallVec<[AppendBatchView<'a>; 4]>, OperationCodecError> {
    decode_append_batches_inner::<true>(input, limits, Some(positions))
}

/// Decode canonical APPEND boundaries whose whole-payload codec was already
/// validated before this immutable body was constructed.
///
/// All body, descriptor, count, length, and cumulative limits are checked. An
/// existing LZ4 block is not decoded or validated again.
pub fn decode_append_batches_with_validated_payload(
    input: &[u8],
    limits: OperationLimits,
) -> Result<SmallVec<[AppendBatchView<'_>; 4]>, OperationCodecError> {
    decode_append_batches_inner::<false>(input, limits, None)
}

pub(super) fn validated_wire_batch(
    body: &[u8],
    proof: ValidatedWireAppend,
) -> Result<AppendBatchView<'_>, OperationCodecError> {
    let descriptor_end = WIRE_HEADER_BYTES
        .checked_add(proof.descriptor_bytes())
        .ok_or(OperationCodecError::LengthOverflow)?;
    let codec_bytes = usize::from(proof.encoded_bytes().is_some()) * CODEC_METADATA_BYTES;
    let payload_bytes = proof.encoded_bytes().unwrap_or(proof.decoded_bytes());
    let expected = descriptor_end
        .checked_add(codec_bytes)
        .and_then(|bytes| bytes.checked_add(payload_bytes))
        .ok_or(OperationCodecError::LengthOverflow)?;
    if body.len() != expected || body.get(..4) != Some(1_u32.to_be_bytes().as_slice()) {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let count = u32::from_be_bytes(
        body[WIRE_HEADER_BYTES - 4..WIRE_HEADER_BYTES]
            .try_into()
            .expect("fixed count field"),
    );
    if count & RECORD_COUNT_MASK != proof.record_count() as u32
        || count & TINY_RECORDS != 0
        || (count & PREPARED_PAYLOAD != 0) != proof.encoded_bytes().is_some()
    {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let summary = AppendBatchSummary {
        partition: super::PartitionIncarnation::from_bytes(body[4..20].try_into().unwrap()),
        owner_epoch: super::OwnerEpoch::new(u64::from_be_bytes(body[20..28].try_into().unwrap())),
        producer_id: super::ProducerId::from_bytes(body[28..44].try_into().unwrap()),
        producer_epoch: super::ProducerEpoch::new(u64::from_be_bytes(
            body[44..52].try_into().unwrap(),
        )),
        first_sequence: super::ProducerSequence::new(u64::from_be_bytes(
            body[52..60].try_into().unwrap(),
        )),
        first_offset: super::Offset::new(u64::from_be_bytes(body[60..68].try_into().unwrap())),
        record_count: proof.record_count(),
        nonzero_message_ids: proof.nonzero_message_ids(),
    };
    let timestamp = u64::from_be_bytes(body[68..76].try_into().unwrap());
    let descriptors = &body[WIRE_HEADER_BYTES..descriptor_end];
    let descriptor_view = AppendRecordDescriptors::new(descriptors, None, proof.record_count());
    let (records, prepared_payload) = if let Some(encoded_bytes) = proof.encoded_bytes() {
        if body[descriptor_end] != ozzy_proto::append::PayloadEncoding::Lz4 as u8
            || u32::from_be_bytes(
                body[descriptor_end + 1..descriptor_end + 5]
                    .try_into()
                    .unwrap(),
            ) as usize
                != proof.decoded_bytes()
            || u32::from_be_bytes(
                body[descriptor_end + 5..descriptor_end + 9]
                    .try_into()
                    .unwrap(),
            ) as usize
                != encoded_bytes
        {
            return Err(OperationCodecError::InvalidPreparedPayload);
        }
        (
            None,
            Some(PreparedAppendPayload {
                encoding: ozzy_proto::append::PayloadEncoding::Lz4,
                decoded_bytes: proof.decoded_bytes(),
                encoded: &body[descriptor_end + CODEC_METADATA_BYTES..],
            }),
        )
    } else {
        (
            Some(AppendRecords::with_lengths(
                descriptors,
                None,
                &body[descriptor_end..],
                proof.record_count(),
            )),
            None,
        )
    };
    Ok(AppendBatchView {
        summary,
        append_timestamp_millis: timestamp,
        prepared_payload,
        records,
        descriptors: descriptor_view,
    })
}

fn decode_append_batches_inner<'a, const VALIDATE_PREPARED: bool>(
    input: &'a [u8],
    limits: OperationLimits,
    mut positions: Option<&mut Vec<RecordPosition>>,
) -> Result<SmallVec<[AppendBatchView<'a>; 4]>, OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Decoder::new(input);
    let count = decoder.count("append batch count", limits.max_append_batches)?;
    if count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    // The declared count is untrusted until the bodies have been walked. A
    // truncated input must not reserve space for nonexistent batches.
    let mut batches = SmallVec::new();
    let mut totals = AppendTotals::default();
    for _ in 0..count {
        let (_, view) = decode_append_batch::<false, VALIDATE_PREPARED>(
            &mut decoder,
            limits,
            &mut totals,
            positions.as_deref_mut(),
        )?;
        batches.push(view);
    }
    decoder.finish()?;
    Ok(batches)
}

impl<'a> AppendView<'a> {
    pub(super) const fn validated(input: &'a [u8], limits: OperationLimits) -> Self {
        Self { input, limits }
    }

    /// Iterate in canonical order. The schema walker rereads immutable
    /// descriptors to locate each batch; it never hashes or copies payloads.
    pub fn batches(self) -> AppendBatches<'a> {
        let mut decoder = Decoder::new(self.input);
        let remaining = decoder.u32().expect("validated batch count") as usize;
        AppendBatches {
            decoder,
            remaining,
            limits: self.limits,
            totals: AppendTotals::default(),
        }
    }
}

/// Allocation-free iterator over a validated canonical append.
#[derive(Debug)]
pub struct AppendBatches<'a> {
    decoder: Decoder<'a>,
    remaining: usize,
    limits: OperationLimits,
    totals: AppendTotals,
}

impl<'a> Iterator for AppendBatches<'a> {
    type Item = AppendBatchView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        // The view was validated when built, including any LZ4 block.
        let (_, view) = decode_append_batch::<false, false>(
            &mut self.decoder,
            self.limits,
            &mut self.totals,
            None,
        )
        .expect("immutable validated append");
        self.remaining -= 1;
        Some(view)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for AppendBatches<'_> {}
impl FusedIterator for AppendBatches<'_> {}

/// One partition-contiguous batch. Metadata is copied; descriptors and opaque
/// payload remain borrowed, including all empty multipart boundaries.
#[derive(Debug, Clone)]
pub struct AppendBatchView<'a> {
    /// Canonical addresses, fences, positions, and record count.
    pub summary: AppendBatchSummary,
    /// Original leader-resolved Unix timestamp, in milliseconds.
    pub append_timestamp_millis: u64,
    /// Exact producer codec block stored instead of decoded payload bytes.
    pub prepared_payload: Option<PreparedAppendPayload<'a>>,
    pub(super) records: Option<AppendRecords<'a>>,
    pub(super) descriptors: AppendRecordDescriptors<'a>,
}

/// Producer payload block preserved unchanged through replication and storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedAppendPayload<'a> {
    pub encoding: ozzy_proto::append::PayloadEncoding,
    pub decoded_bytes: usize,
    pub encoded: &'a [u8],
}

impl<'a> AppendBatchView<'a> {
    /// Independently iterable records. Whole-APPEND encoded views must use
    /// [`Self::raw_records`] or descriptor iteration until explicitly decoded.
    pub fn records(&self) -> AppendRecords<'a> {
        self.raw_records()
            .expect("whole-APPEND encoded payload has no raw canonical view")
    }

    /// Independently iterable records when canonical payload bytes are raw.
    pub fn raw_records(&self) -> Option<AppendRecords<'a>> {
        self.records.clone()
    }

    /// Record IDs and multipart lengths, available for every payload encoding.
    pub fn descriptors(&self) -> AppendRecordDescriptors<'a> {
        self.descriptors.clone()
    }
}

/// Descriptor-only iterator, independent of raw or whole-batch payload storage.
#[derive(Debug, Clone)]
pub struct AppendRecordDescriptors<'a> {
    descriptors: Decoder<'a>,
    lengths: Option<Decoder<'a>>,
    remaining: usize,
}

impl<'a> AppendRecordDescriptors<'a> {
    pub(super) const fn new(
        descriptors: &'a [u8],
        lengths: Option<&'a [u8]>,
        count: usize,
    ) -> Self {
        Self {
            descriptors: Decoder::new(descriptors),
            lengths: match lengths {
                Some(lengths) => Some(Decoder::new(lengths)),
                None => None,
            },
            remaining: count,
        }
    }

    /// Remaining canonical descriptor bytes; compact one-byte lengths are separate.
    pub fn remaining_bytes(&self) -> (&'a [u8], Option<&'a [u8]>) {
        (
            &self.descriptors.input[self.descriptors.offset..],
            self.lengths
                .as_ref()
                .map(|lengths| &lengths.input[lengths.offset..]),
        )
    }
}

/// One record identity and its logical multipart lengths.
#[derive(Debug, Clone)]
pub struct AppendRecordDescriptor<'a> {
    pub encoding: ozzy_proto::data::Encoding,
    pub message_id: MessageId,
    pub part_lengths: AppendPartLengths<'a>,
}

/// Exact logical part lengths without payload access.
#[derive(Debug, Clone)]
pub struct AppendPartLengths<'a> {
    lengths: std::slice::Iter<'a, [u8; 4]>,
    single: Option<usize>,
}

impl Iterator for AppendPartLengths<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.single.take().or_else(|| {
            self.lengths
                .next()
                .map(|length| u32::from_be_bytes(*length) as usize)
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let count = self.lengths.len() + usize::from(self.single.is_some());
        (count, Some(count))
    }
}

impl ExactSizeIterator for AppendPartLengths<'_> {}
impl FusedIterator for AppendPartLengths<'_> {}

impl<'a> Iterator for AppendRecordDescriptors<'a> {
    type Item = AppendRecordDescriptor<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let message_id = MessageId::from_bytes(self.descriptors.id().expect("validated record"));
        let (encoding, part_lengths) = if let Some(lengths) = &mut self.lengths {
            (
                ozzy_proto::data::Encoding::Raw,
                AppendPartLengths {
                    lengths: [].iter(),
                    single: Some(lengths.u8().expect("validated tiny length") as usize),
                },
            )
        } else {
            let tagged = self.descriptors.u32().expect("validated parts");
            let count = (tagged & 0x00ff_ffff) as usize;
            let encoding = match tagged >> 24 {
                0 => ozzy_proto::data::Encoding::Raw,
                1 => ozzy_proto::data::Encoding::Lz4 {
                    decoded_bytes: self.descriptors.u32().expect("validated encoding"),
                },
                _ => unreachable!("validated encoding"),
            };
            let lengths = self
                .descriptors
                .take(count * 4)
                .expect("validated part lengths")
                .as_chunks::<4>()
                .0;
            (
                encoding,
                AppendPartLengths {
                    lengths: lengths.iter(),
                    single: None,
                },
            )
        };
        self.remaining -= 1;
        Some(AppendRecordDescriptor {
            encoding,
            message_id,
            part_lengths,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for AppendRecordDescriptors<'_> {}
impl FusedIterator for AppendRecordDescriptors<'_> {}

/// Borrowed record iterator. Cloning it copies cursor state, not payloads.
#[derive(Debug, Clone)]
pub struct AppendRecords<'a> {
    descriptors: Decoder<'a>,
    lengths: Option<Decoder<'a>>,
    payload: Decoder<'a>,
    remaining: usize,
}

impl<'a> AppendRecords<'a> {
    pub(super) const fn with_lengths(
        descriptors: &'a [u8],
        lengths: Option<&'a [u8]>,
        payload: &'a [u8],
        count: usize,
    ) -> Self {
        Self {
            lengths: match lengths {
                Some(lengths) => Some(Decoder::new(lengths)),
                None => None,
            },
            descriptors: Decoder::new(descriptors),
            payload: Decoder::new(payload),
            remaining: count,
        }
    }

    /// Borrow a fragment whose spans an owner's read index recorded during the
    /// validating walk at admission. No descriptor is checked again, so the
    /// spans must come from that index, never from a client or a file.
    pub const fn indexed(
        descriptors: &'a [u8],
        lengths: Option<&'a [u8]>,
        payload: &'a [u8],
        count: usize,
    ) -> Self {
        Self::with_lengths(descriptors, lengths, payload, count)
    }

    /// Remaining canonical descriptor and payload bytes. These borrowed slices
    /// let an owner record byte positions while admitting immutable history.
    pub fn tiny_lengths(&self) -> Option<&'a [u8]> {
        self.lengths.as_ref().map(|d| &d.input[d.offset..])
    }

    /// Remaining ID/descriptors and payload; compact lengths are in `tiny_lengths`.
    pub fn remaining_bytes(&self) -> (&'a [u8], &'a [u8]) {
        (
            &self.descriptors.input[self.descriptors.offset..],
            &self.payload.input[self.payload.offset..],
        )
    }
}

/// Decode exactly one indexed record fragment, without inspecting preceding or
/// following records. Owners must separately establish the source operation's
/// identity, confirmation and partition metadata. Index offsets are not proof:
/// all selected descriptors and the exact payload length are checked here.
pub fn decode_append_records<'a>(
    descriptors: &'a [u8],
    payload: &'a [u8],
    count: usize,
    limits: OperationLimits,
) -> Result<AppendRecords<'a>, OperationCodecError> {
    decode_packed_append_records(descriptors, None, payload, count, limits)
}

/// Validate an indexed fragment, including its optional compact length run.
pub fn decode_packed_append_records<'a>(
    descriptors: &'a [u8],
    lengths: Option<&'a [u8]>,
    payload: &'a [u8],
    count: usize,
    limits: OperationLimits,
) -> Result<AppendRecords<'a>, OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("append record count", count, limits.max_records)?;
    if count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    let bytes = descriptors
        .len()
        .checked_add(lengths.map_or(0, <[u8]>::len))
        .and_then(|n| n.checked_add(payload.len()))
        .ok_or(OperationCodecError::LengthOverflow)?;
    enforce_limit("operation body bytes", bytes, limits.max_body_bytes)?;
    let mut decoder = Decoder::new(descriptors);
    let mut totals = AppendTotals::default();
    let payload_bytes = if let Some(lengths) = lengths {
        validate_tiny_descriptors(descriptors, lengths, count, limits, &mut totals, None)?.0
    } else {
        let bytes = validate_append_descriptors(&mut decoder, count, limits, &mut totals, None)?.0;
        decoder.finish()?;
        bytes
    };
    let mut data = Decoder::new(payload);
    data.take(payload_bytes)?;
    data.finish()?;
    Ok(AppendRecords::with_lengths(
        descriptors,
        lengths,
        payload,
        count,
    ))
}

impl<'a> Iterator for AppendRecords<'a> {
    type Item = AppendRecordView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let message_id = MessageId::from_bytes(self.descriptors.id().expect("validated record"));
        if let Some(lengths) = &mut self.lengths {
            let bytes = lengths.u8().expect("validated tiny length") as usize;
            let payload = self.payload.take(bytes).expect("validated tiny payload");
            self.remaining -= 1;
            return Some(AppendRecordView {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id,
                parts: AppendParts {
                    lengths: [].iter(),
                    single: Some(payload),
                    payload: Decoder::new(payload),
                },
            });
        }
        let tagged = self.descriptors.u32().expect("validated parts");
        let count = (tagged & 0x00ff_ffff) as usize;
        let encoding = match tagged >> 24 {
            0 => ozzy_proto::data::Encoding::Raw,
            1 => ozzy_proto::data::Encoding::Lz4 {
                decoded_bytes: self.descriptors.u32().expect("validated encoding"),
            },
            _ => unreachable!("validated encoding"),
        };
        let lengths = self
            .descriptors
            .take(count * 4)
            .expect("validated part lengths")
            .as_chunks::<4>()
            .0;
        let bytes = lengths
            .iter()
            .map(|n| u32::from_be_bytes(*n) as usize)
            .sum();
        let payload = self.payload.take(bytes).expect("validated payload");
        self.remaining -= 1;
        Some(AppendRecordView {
            encoding,
            message_id,
            parts: AppendParts {
                lengths: lengths.iter(),
                single: None,
                payload: Decoder::new(payload),
            },
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for AppendRecords<'_> {}
impl FusedIterator for AppendRecords<'_> {}

/// One record with independently iterable borrowed parts.
#[derive(Debug, Clone)]
pub struct AppendRecordView<'a> {
    /// Payload representation, preserved through persistence and replay.
    pub encoding: ozzy_proto::data::Encoding,
    /// Stable message identity, not proof of an applied operation.
    pub message_id: MessageId,
    /// At least one part; a part may be empty.
    pub parts: AppendParts<'a>,
}

/// Ordered multipart payload. No allocation, coalescing, or payload copy.
#[derive(Debug, Clone)]
pub struct AppendParts<'a> {
    lengths: std::slice::Iter<'a, [u8; 4]>,
    single: Option<&'a [u8]>,
    payload: Decoder<'a>,
}

impl AppendParts<'_> {
    /// Sum of the remaining opaque part lengths, excluding all descriptors.
    pub fn payload_bytes(&self) -> usize {
        self.payload.input.len() - self.payload.offset
    }
}

impl<'a> Iterator for AppendParts<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(payload) = self.single.take() {
            self.payload
                .take(payload.len())
                .expect("validated tiny payload");
            return Some(payload);
        }
        let length = u32::from_be_bytes(*self.lengths.next()?) as usize;
        Some(self.payload.take(length).expect("validated part payload"))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.lengths.len() + usize::from(self.single.is_some());
        (n, Some(n))
    }
}

impl ExactSizeIterator for AppendParts<'_> {}
impl FusedIterator for AppendParts<'_> {}
