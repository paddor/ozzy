//! Proof that a canonical one-batch APPEND came from a validated request.

use super::super::{
    AppendBatchSummary, AppendBatchView, OperationCodecError, OperationLimits, PREPARED_PAYLOAD,
    RECORD_COUNT_MASK, TINY_RECORDS, enforce_limit, validate_limits, validate_position_count,
};
use super::{CODEC_METADATA_BYTES, ONE_BATCH_HEADER_BYTES};
use crate::operation::view::validated_wire_batch;
use ozzy_proto::{append::PayloadEncoding, data::RecordDescriptors, layout::Fixed};

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

impl ValidatedWireAppend {
    pub(super) fn new(
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
    pub(in crate::operation) fn check_layout(
        self,
        body: &[u8],
    ) -> Result<(usize, u32), OperationCodecError> {
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

    pub(in crate::operation) const fn uniform_part_bytes(self) -> Option<usize> {
        match self.uniform_part_bytes {
            Some(bytes) => Some(bytes as usize),
            None => None,
        }
    }

    /// Recover state metadata from fixed fields while retaining descriptor and
    /// codec validation performed before canonical construction.
    pub fn summary(self, body: &[u8]) -> Result<AppendBatchSummary, OperationCodecError> {
        validated_wire_batch(body, self).map(|batch| batch.summary)
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
        let batch = validated_wire_batch(body, self)?;
        validate_position_count(batch.summary.first_sequence.get(), self.record_count())?;
        validate_position_count(batch.summary.first_offset.get(), self.record_count())?;
        Ok(batch)
    }

    pub(in crate::operation) const fn descriptor_bytes(self) -> usize {
        self.descriptor_bytes as usize
    }

    pub(in crate::operation) const fn decoded_bytes(self) -> usize {
        self.decoded_bytes as usize
    }

    pub(in crate::operation) const fn encoded_bytes(self) -> Option<usize> {
        if self.encoded_bytes == 0 {
            None
        } else {
            Some(self.encoded_bytes as usize)
        }
    }

    pub(in crate::operation) const fn record_count(self) -> usize {
        self.record_count as usize
    }

    pub(in crate::operation) fn packed(
        &mut self,
        encoded_bytes: usize,
    ) -> Result<(), OperationCodecError> {
        self.encoded_bytes =
            u32::try_from(encoded_bytes).map_err(|_| OperationCodecError::LengthOverflow)?;
        Ok(())
    }

    pub(in crate::operation) const fn nonzero_message_ids(self) -> bool {
        self.nonzero_message_ids
    }

    /// Whether this body stores a whole-payload LZ4 block.
    pub const fn payload_is_prepared(self) -> bool {
        self.encoded_bytes != 0
    }
}
