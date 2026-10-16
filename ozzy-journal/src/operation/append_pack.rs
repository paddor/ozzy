//! Adaptive whole-APPEND packing without materializing record payloads.

use super::append_encoder::{CODEC_METADATA_BYTES, ONE_BATCH_HEADER_BYTES as HEADER_BYTES};
use super::{
    BatchPayload, OperationCodecError, OperationLimits, PREPARED_PAYLOAD, TINY_RECORDS,
    ValidatedWireAppend, decode_append_view,
};
use ozzy_proto::layout::{self, Fixed, Writer};

/// Reusable bounded state for one journal owner.
#[derive(Debug)]
pub struct AppendPackScratch {
    compressor: lz4rip::block::CompressorN<2048>,
    encoded: Vec<u8>,
    tiny_lengths: Vec<u8>,
    max_payload_bytes: usize,
}

impl AppendPackScratch {
    /// Construct lazy scratch bounded by canonical payload limits.
    pub fn new(max_payload_bytes: usize) -> Self {
        Self {
            compressor: lz4rip::block::CompressorN::new(),
            encoded: Vec::new(),
            tiny_lengths: Vec::new(),
            max_payload_bytes,
        }
    }

    /// Physical reusable scratch, including the explicitly sized LZ4 table.
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + 2048 * size_of::<u32>()
            + self.encoded.capacity()
            + self.tiny_lengths.capacity()
    }

    fn reserve_encoded(&mut self, bytes: usize) -> Result<(), OperationCodecError> {
        if bytes > self.max_payload_bytes {
            return Err(OperationCodecError::LimitExceeded {
                kind: "append payload bytes",
                actual: bytes,
                limit: self.max_payload_bytes,
            });
        }
        let capacity = lz4rip::get_maximum_output_size(bytes);
        if capacity > self.encoded.capacity() {
            self.encoded
                .try_reserve_exact(capacity.saturating_sub(self.encoded.len()))
                .map_err(|_| OperationCodecError::OutputAllocation)?;
        }
        self.encoded.resize(capacity, 0);
        Ok(())
    }

    /// Compress `payload` into the reusable block; returns its encoded bytes.
    fn compress(&mut self, payload: &[u8]) -> Result<usize, OperationCodecError> {
        self.reserve_encoded(payload.len())?;
        self.compressor
            .compress_into(payload, &mut self.encoded)
            .map_err(|_| OperationCodecError::AppendCompression)
    }
}

/// Outcome of one adaptive packing decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendPackResult {
    /// Payload was already prepared by its producer.
    Prepared,
    /// Raw payload was too small or contained record-level encodings.
    Ineligible,
    /// LZ4 was attempted, but complete canonical representation was not smaller.
    Raw {
        /// Original contiguous payload bytes.
        decoded_bytes: usize,
        /// Candidate LZ4 block bytes, excluding codec metadata.
        encoded_bytes: usize,
    },
    /// Canonical body now contains one whole-APPEND LZ4 block.
    Packed {
        /// Original contiguous payload bytes.
        decoded_bytes: usize,
        /// Retained LZ4 block bytes, excluding codec metadata.
        encoded_bytes: usize,
    },
}

/// Pack one canonical producer APPEND in place when the complete body shrinks.
///
/// Input must contain exactly one batch. Producer-prepared LZ4 stays byte-exact.
/// Raw record payload is already contiguous, so compression reads it directly.
pub fn pack_append_payload(
    body: &mut Vec<u8>,
    limits: OperationLimits,
    scratch: &mut AppendPackScratch,
) -> Result<AppendPackResult, OperationCodecError> {
    let view = decode_append_view(body, limits)?;
    let mut batches = view.batches();
    let batch = batches.next().ok_or(OperationCodecError::EmptyAppend)?;
    if batches.next().is_some() {
        return Ok(AppendPackResult::Ineligible);
    }
    let BatchPayload::Raw(records) = batch.payload else {
        return Ok(AppendPackResult::Prepared);
    };
    if records
        .clone()
        .any(|record| record.encoding != ozzy_proto::data::Encoding::Raw)
    {
        return Ok(AppendPackResult::Ineligible);
    }
    let (descriptors, payload) = records.remaining_bytes();
    if payload.len() < ozzy_proto::append::ADAPTIVE_LZ4_THRESHOLD {
        return Ok(AppendPackResult::Ineligible);
    }
    let encoded_bytes = scratch.compress(payload)?;

    let tiny_lengths = records.tiny_lengths();
    let descriptor_start = descriptors.as_ptr() as usize - body.as_ptr() as usize;
    let payload_start = payload.as_ptr() as usize - body.as_ptr() as usize;
    if descriptor_start != HEADER_BYTES || payload_start > body.len() {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let prepared_descriptor_bytes = if let Some(lengths) = tiny_lengths {
        scratch.tiny_lengths.clear();
        scratch.tiny_lengths.extend_from_slice(lengths);
        batch
            .summary
            .record_count
            .checked_mul(24)
            .ok_or(OperationCodecError::LengthOverflow)?
    } else {
        descriptors.len()
    };
    let packing = Packing {
        descriptor_end: HEADER_BYTES
            .checked_add(prepared_descriptor_bytes)
            .ok_or(OperationCodecError::LengthOverflow)?,
        decoded_bytes: payload.len(),
        encoded_bytes,
    };
    let Some(packed_body_bytes) = packing.shrunk_bytes(body.len())? else {
        return Ok(packing.raw());
    };
    if tiny_lengths.is_some() {
        widen_tiny_descriptors(body, packed_body_bytes, &scratch.tiny_lengths)?;
    } else {
        body.truncate(packing.descriptor_end);
        body.resize(packed_body_bytes, 0);
    }
    let count = (header_count(body)? & !TINY_RECORDS) | PREPARED_PAYLOAD;
    packing.write(body, count, &scratch.encoded)
}

/// Adaptively pack a one-batch wire APPEND already validated by `ozzy-proto`.
///
/// The descriptor and payload sizes are proof carried with the immutable
/// application-thread validation result. This checks their fixed canonical
/// layout but does not rescan descriptors or revalidate an existing LZ4 block.
pub fn pack_validated_wire_append_payload(
    body: &mut Vec<u8>,
    proof: &mut ValidatedWireAppend,
    scratch: &mut AppendPackScratch,
) -> Result<AppendPackResult, OperationCodecError> {
    let (descriptor_end, count) = proof.check_layout(body)?;
    if proof.payload_is_prepared() {
        return Ok(AppendPackResult::Prepared);
    }
    let decoded_bytes = proof.decoded_bytes();
    if decoded_bytes < ozzy_proto::append::ADAPTIVE_LZ4_THRESHOLD {
        return Ok(AppendPackResult::Ineligible);
    }
    let payload = body
        .get(descriptor_end..)
        .ok_or(OperationCodecError::InvalidPreparedPayload)?;
    let packing = Packing {
        descriptor_end,
        decoded_bytes,
        encoded_bytes: scratch.compress(payload)?,
    };
    let Some(packed_body_bytes) = packing.shrunk_bytes(body.len())? else {
        return Ok(packing.raw());
    };
    body.truncate(descriptor_end);
    body.resize(packed_body_bytes, 0);
    let result = packing.write(body, count | PREPARED_PAYLOAD, &scratch.encoded)?;
    proof.packed(packing.encoded_bytes)?;
    Ok(result)
}

/// One LZ4 block compressed from the payload after a body's descriptors.
#[derive(Debug, Clone, Copy)]
struct Packing {
    descriptor_end: usize,
    decoded_bytes: usize,
    encoded_bytes: usize,
}

impl Packing {
    /// The packed body length when it is smaller than `body_bytes`. Both
    /// lengths are checked to fit their fields before the body changes.
    fn shrunk_bytes(self, body_bytes: usize) -> Result<Option<usize>, OperationCodecError> {
        let packed_body_bytes = self
            .descriptor_end
            .checked_add(CODEC_METADATA_BYTES)
            .and_then(|bytes| bytes.checked_add(self.encoded_bytes))
            .ok_or(OperationCodecError::LengthOverflow)?;
        if packed_body_bytes >= body_bytes {
            return Ok(None);
        }
        u32::try_from(self.decoded_bytes).map_err(|_| OperationCodecError::LengthOverflow)?;
        u32::try_from(self.encoded_bytes).map_err(|_| OperationCodecError::LengthOverflow)?;
        Ok(Some(packed_body_bytes))
    }

    const fn raw(self) -> AppendPackResult {
        AppendPackResult::Raw {
            decoded_bytes: self.decoded_bytes,
            encoded_bytes: self.encoded_bytes,
        }
    }

    /// Set the header count, then write the LZ4 codec metadata and block
    /// after the descriptors of a body already sized for them.
    fn write(
        self,
        body: &mut [u8],
        count: u32,
        encoded: &[u8],
    ) -> Result<AppendPackResult, OperationCodecError> {
        let length =
            |bytes: usize| u32::try_from(bytes).map_err(|_| OperationCodecError::LengthOverflow);
        layout::patch(body, HEADER_BYTES - 4, count.to_be_bytes())?;
        let tail = body
            .get_mut(self.descriptor_end..)
            .ok_or(OperationCodecError::LengthOverflow)?;
        let mut writer = Writer::new(tail);
        writer.u8(ozzy_proto::append::PayloadEncoding::Lz4 as u8)?;
        writer.u32(length(self.decoded_bytes)?)?;
        writer.u32(length(self.encoded_bytes)?)?;
        writer.bytes(encoded.get(..self.encoded_bytes).unwrap_or_default())?;
        Ok(AppendPackResult::Packed {
            decoded_bytes: self.decoded_bytes,
            encoded_bytes: self.encoded_bytes,
        })
    }
}

/// Resize a tiny-record body and widen each 16-byte ID to a raw single-part
/// 24-byte descriptor, last first so no ID is overwritten before it moves.
fn widen_tiny_descriptors(
    body: &mut Vec<u8>,
    packed_body_bytes: usize,
    lengths: &[u8],
) -> Result<(), OperationCodecError> {
    body.resize(packed_body_bytes, 0);
    for (index, &length) in lengths.iter().enumerate().rev() {
        let source = HEADER_BYTES + index * 16;
        let destination = HEADER_BYTES + index * 24;
        body.copy_within(source..source + 16, destination);
        let [a, b, c, d] = u32::from(length).to_be_bytes();
        layout::patch(body, destination + 16, [0, 0, 0, 1, a, b, c, d])?;
    }
    Ok(())
}

/// The record count at the end of the fixed APPEND header.
fn header_count(body: &[u8]) -> Result<u32, OperationCodecError> {
    let header: &[u8; HEADER_BYTES] = body
        .first_chunk()
        .ok_or(OperationCodecError::InvalidPreparedPayload)?;
    Ok(header.u32_at::<{ HEADER_BYTES - 4 }>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::{
        Append, AppendBatch, AppendRecord, OperationBody, encode_operation_body,
    };
    use ozzy_proto::{
        MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
        ProducerSequence,
    };
    use smallvec::smallvec;

    fn encode(parts: &[Vec<u8>], encoding: ozzy_proto::data::Encoding) -> Vec<u8> {
        let records = parts
            .iter()
            .enumerate()
            .map(|(index, part)| AppendRecord {
                encoding,
                message_id: MessageId::from_bytes([(index + 1) as u8; 16]),
                parts: smallvec![part.as_slice()],
            })
            .collect::<Vec<_>>();
        encode_operation_body(
            &OperationBody::Append(Append {
                batches: vec![AppendBatch {
                    partition: PartitionIncarnation::from_bytes([1; 16]),
                    owner_epoch: OwnerEpoch::new(1),
                    producer_id: ProducerId::from_bytes([2; 16]),
                    producer_epoch: ProducerEpoch::new(1),
                    first_sequence: ProducerSequence::new(0),
                    first_offset: Offset::ZERO,
                    append_timestamp_millis: 1,
                    records: records.into(),
                }],
            }),
            OperationLimits::default(),
        )
        .unwrap()
    }

    fn prepared(body: &[u8]) -> super::super::PreparedAppendPayload<'_> {
        decode_append_view(body, OperationLimits::default())
            .unwrap()
            .batches()
            .next()
            .unwrap()
            .prepared_payload()
            .unwrap()
    }

    #[test]
    fn validated_payload_check_skips_only_the_lz4_block() {
        use crate::operation::{
            OperationKind, validate_operation_body, validate_operation_body_with_validated_payload,
        };
        let threshold = ozzy_proto::append::ADAPTIVE_LZ4_THRESHOLD;
        let mut body = encode(&[vec![7; 4 * threshold]], ozzy_proto::data::Encoding::Raw);
        let mut scratch = AppendPackScratch::new(8 * threshold);
        assert!(matches!(
            pack_append_payload(&mut body, OperationLimits::default(), &mut scratch).unwrap(),
            AppendPackResult::Packed { .. }
        ));
        // Zero tokens with zero offsets: every length stays, the block is invalid.
        let encoded = prepared(&body).encoded.len();
        let start = body.len() - encoded;
        body[start..].fill(0);
        let limits = OperationLimits::default();
        assert!(validate_operation_body(OperationKind::Append, &body, limits).is_err());
        validate_operation_body_with_validated_payload(OperationKind::Append, &body, limits)
            .unwrap();
        // Descriptors and limits stay checked.
        let small = OperationLimits {
            max_payload_bytes: threshold,
            ..limits
        };
        assert!(
            validate_operation_body_with_validated_payload(OperationKind::Append, &body, small)
                .is_err()
        );
    }

    #[test]
    fn threshold_is_inclusive_and_prepared_bytes_are_stable() {
        let threshold = ozzy_proto::append::ADAPTIVE_LZ4_THRESHOLD;
        let mut below = encode(&[vec![7; threshold - 1]], ozzy_proto::data::Encoding::Raw);
        let original = below.clone();
        let mut scratch = AppendPackScratch::new(2 * threshold);
        assert_eq!(
            pack_append_payload(&mut below, OperationLimits::default(), &mut scratch).unwrap(),
            AppendPackResult::Ineligible
        );
        assert_eq!(below, original);

        let mut at = encode(&[vec![7; threshold]], ozzy_proto::data::Encoding::Raw);
        assert!(matches!(
            pack_append_payload(&mut at, OperationLimits::default(), &mut scratch).unwrap(),
            AppendPackResult::Packed {
                decoded_bytes,
                ..
            } if decoded_bytes == threshold
        ));
        let packed = prepared(&at);
        assert_eq!(
            lz4rip::block::decompress(packed.encoded, packed.decoded_bytes).unwrap(),
            vec![7; threshold]
        );
        let frozen = at.clone();
        assert_eq!(
            pack_append_payload(&mut at, OperationLimits::default(), &mut scratch).unwrap(),
            AppendPackResult::Prepared
        );
        assert_eq!(at, frozen);
    }

    #[test]
    fn tiny_records_expand_descriptors_only_when_complete_body_shrinks() {
        let parts = vec![vec![9; 16]; 128];
        let mut body = encode(&parts, ozzy_proto::data::Encoding::Raw);
        let original_bytes = body.len();
        let mut scratch = AppendPackScratch::new(4096);
        assert!(matches!(
            pack_append_payload(&mut body, OperationLimits::default(), &mut scratch).unwrap(),
            AppendPackResult::Packed {
                decoded_bytes: 2048,
                ..
            }
        ));
        assert!(body.len() < original_bytes);
        let view = decode_append_view(&body, OperationLimits::default()).unwrap();
        let batch = view.batches().next().unwrap();
        assert_eq!(batch.descriptors().len(), 128);
        assert!(
            batch
                .descriptors()
                .all(|record| record.part_lengths.eq([16]))
        );
    }

    #[test]
    fn incompressible_and_record_encoded_payloads_stay_raw() {
        let mut state = 0x0917_5348_abcd_ef12_u64;
        let noise = (0..4096)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let mut body = encode(&[noise], ozzy_proto::data::Encoding::Raw);
        let original = body.clone();
        let mut scratch = AppendPackScratch::new(8192);
        assert!(matches!(
            pack_append_payload(&mut body, OperationLimits::default(), &mut scratch).unwrap(),
            AppendPackResult::Raw { .. }
        ));
        assert_eq!(body, original);

        let mut encoded = encode(
            &[vec![3; 4096]],
            ozzy_proto::data::Encoding::Lz4 {
                decoded_bytes: 8192,
            },
        );
        let original = encoded.clone();
        assert_eq!(
            pack_append_payload(&mut encoded, OperationLimits::default(), &mut scratch).unwrap(),
            AppendPackResult::Ineligible
        );
        assert_eq!(encoded, original);
    }
}
