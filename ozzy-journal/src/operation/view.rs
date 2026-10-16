//! Borrowed canonical append views, validated by the ordinary schema walker.

use std::iter::FusedIterator;

use ozzy_proto::layout::Fixed;
use smallvec::SmallVec;

mod records;
pub use records::{
    AppendPartLengths, AppendParts, AppendRecordDescriptor, AppendRecordDescriptors,
    AppendRecordView, AppendRecords, decode_append_records, decode_packed_append_records,
};

use super::append_encoder::CODEC_METADATA_BYTES;
use super::{
    AppendBatchSummary, AppendHeader, AppendTotals, OperationCodecError, OperationKind,
    OperationLimits, ReadFields, Reader, RecordPosition, ValidatedWireAppend, decode_append_batch,
    enforce_limit, validate_limits, validate_operation_body,
};

/// Fully validated canonical append bytes. Iteration allocates no record tables
/// and borrows every payload part from the input. This establishes syntax only,
/// not commit, authority, or a canonical state transition.
#[derive(Debug, Clone, Copy)]
pub struct AppendView<'a> {
    /// Batch bodies after the count.
    batches: &'a [u8],
    count: usize,
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
    AppendView::validated(input, limits)
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
    proof.check_layout(body)?;
    // The checked length covers the header, so these reads cannot fail.
    let mut reader = Reader::new(body);
    if reader.u32()? != 1 {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let header = AppendHeader::decode(&mut reader)?;
    reader.u32()?;
    let descriptors = reader.take(proof.descriptor_bytes())?;
    let payload = if let Some(encoded_bytes) = proof.encoded_bytes() {
        let codec: &[u8; CODEC_METADATA_BYTES] = reader.chunk()?;
        if codec.u8_at::<0>() != ozzy_proto::append::PayloadEncoding::Lz4 as u8
            || codec.u32_at::<1>() as usize != proof.decoded_bytes()
            || codec.u32_at::<5>() as usize != encoded_bytes
        {
            return Err(OperationCodecError::InvalidPreparedPayload);
        }
        BatchPayload::Prepared(PreparedAppendPayload {
            encoding: ozzy_proto::append::PayloadEncoding::Lz4,
            decoded_bytes: proof.decoded_bytes(),
            encoded: reader.rest(),
        })
    } else {
        BatchPayload::Raw(AppendRecords::with_lengths(
            descriptors,
            None,
            reader.rest(),
            proof.record_count(),
        ))
    };
    Ok(AppendBatchView {
        summary: header.summary(proof.record_count(), proof.nonzero_message_ids()),
        append_timestamp_millis: header.append_timestamp_millis,
        payload,
        descriptors: AppendRecordDescriptors::new(descriptors, None, proof.record_count()),
        uniform_part_bytes: proof.uniform_part_bytes(),
    })
}

fn decode_append_batches_inner<'a, const VALIDATE_PREPARED: bool>(
    input: &'a [u8],
    limits: OperationLimits,
    mut positions: Option<&mut Vec<RecordPosition>>,
) -> Result<SmallVec<[AppendBatchView<'a>; 4]>, OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Reader::new(input);
    let count = decoder.count("append batch count", limits.max_append_batches)?;
    if count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    // The declared count is untrusted until the bodies have been walked. A
    // truncated input must not reserve space for nonexistent batches.
    let mut batches = SmallVec::new();
    let mut totals = AppendTotals::new(limits);
    for _ in 0..count {
        let (_, view) = decode_append_batch::<false, VALIDATE_PREPARED, false>(
            &mut decoder,
            &mut totals,
            positions.as_deref_mut(),
        )?;
        batches.push(view);
    }
    decoder.finish()?;
    Ok(batches)
}

impl<'a> AppendView<'a> {
    /// View an operation body the schema walker accepted.
    pub(super) fn validated(
        input: &'a [u8],
        limits: OperationLimits,
    ) -> Result<Self, OperationCodecError> {
        let mut decoder = Reader::new(input);
        let count = decoder.u32()? as usize;
        Ok(Self {
            batches: decoder.rest(),
            count,
            limits,
        })
    }

    /// Iterate in canonical order. The schema walker rereads immutable
    /// descriptors to locate each batch; it never hashes or copies payloads.
    pub fn batches(self) -> AppendBatches<'a> {
        AppendBatches {
            decoder: Reader::new(self.batches),
            remaining: self.count,
            totals: AppendTotals::new(self.limits),
        }
    }
}

/// Allocation-free iterator over a validated canonical append.
#[derive(Debug)]
pub struct AppendBatches<'a> {
    decoder: Reader<'a>,
    remaining: usize,
    totals: AppendTotals,
}

impl<'a> Iterator for AppendBatches<'a> {
    type Item = AppendBatchView<'a>;

    #[expect(
        clippy::expect_used,
        reason = "the view was validated when built, including any LZ4 block"
    )]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let (_, view) =
            decode_append_batch::<false, false, false>(&mut self.decoder, &mut self.totals, None)
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
    /// Raw records, or the producer codec block stored instead of them.
    pub payload: BatchPayload<'a>,
    pub(super) descriptors: AppendRecordDescriptors<'a>,
    pub(super) uniform_part_bytes: Option<usize>,
}

/// Canonical payload bytes of one batch.
#[derive(Debug, Clone)]
pub enum BatchPayload<'a> {
    /// Raw parts, independently iterable per record.
    Raw(AppendRecords<'a>),
    /// Exact producer codec block stored instead of decoded payload bytes.
    Prepared(PreparedAppendPayload<'a>),
}

/// Producer payload block preserved unchanged through replication and storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedAppendPayload<'a> {
    /// Payload representation preserved through persistence and replay.
    pub encoding: ozzy_proto::append::PayloadEncoding,
    /// Exact whole-payload byte count after decoding the prepared block.
    pub decoded_bytes: usize,
    /// Exact prepared block bytes, borrowed from the canonical body.
    pub encoded: &'a [u8],
}

impl<'a> AppendBatchView<'a> {
    /// Materialize this validated batch, decoding a whole-payload block once.
    /// Raw multipart bytes remain borrowed; decoded blocks have owned backing.
    pub fn materialize(&self) -> Result<super::AppendBatch<'a>, OperationCodecError> {
        super::codec::append::materialize_batch::<true>(self)
    }
    /// Part length for a general descriptor table containing only single-part
    /// raw records of that length. Packed tables and mixed shapes return `None`.
    /// The validating walk establishes this property without a second scan.
    pub const fn uniform_raw_record_bytes(&self) -> Option<usize> {
        self.uniform_part_bytes
    }
    /// Independently iterable records when canonical payload bytes are raw.
    /// Whole-APPEND encoded views have none until explicitly decoded; use
    /// descriptor iteration for their identities and lengths.
    pub fn raw_records(&self) -> Option<AppendRecords<'a>> {
        match &self.payload {
            BatchPayload::Raw(records) => Some(records.clone()),
            BatchPayload::Prepared(_) => None,
        }
    }

    /// The producer codec block, when one replaces raw payload bytes.
    pub const fn prepared_payload(&self) -> Option<PreparedAppendPayload<'a>> {
        match self.payload {
            BatchPayload::Prepared(prepared) => Some(prepared),
            BatchPayload::Raw(_) => None,
        }
    }

    /// Record IDs and multipart lengths, available for every payload encoding.
    pub fn descriptors(&self) -> AppendRecordDescriptors<'a> {
        self.descriptors.clone()
    }
}
