//! Rebuild only the fresh suffix of a compressed retry. Fresh compressed
//! APPENDs still retain their exact SDK block without decoding or recompression.

use ozzy_journal::operation::{
    Append, AppendBatch, AppendPackScratch, AppendRecord, OperationBody, OperationKind,
    OperationLimits, decode_operation_body, encode_operation_body, pack_append_payload,
};
use ozzy_proto::ProducerSequence;

use super::{AppendBuffer, JournalError};

impl AppendBuffer {
    pub(super) fn trim_prepared_prefix(
        &mut self,
        count: usize,
        limits: OperationLimits,
    ) -> Result<(), JournalError> {
        let OperationBody::Append(decoded) =
            decode_operation_body(OperationKind::Append, &self.bodies, limits)?
        else {
            return Err(JournalError::AppendMismatch);
        };
        let [batch] = decoded.batches.as_slice() else {
            return Err(JournalError::AppendMismatch);
        };
        let sequence = batch
            .first_sequence
            .get()
            .checked_add(count as u64)
            .ok_or(JournalError::AppendMismatch)?;
        let suffix = OperationBody::Append(Append {
            batches: vec![AppendBatch {
                first_sequence: ProducerSequence::new(sequence),
                records: batch
                    .records
                    .iter()
                    .skip(count)
                    .map(|record| AppendRecord {
                        encoding: record.encoding,
                        message_id: record.message_id,
                        parts: record.parts.iter().collect(),
                    })
                    .collect(),
                ..*batch
            }],
        });
        // Retry-only scratch has explicit decoded-payload plus descriptor bounds.
        // Do not require its temporary raw representation to fit the leased arena.
        let expanded_limits = OperationLimits {
            max_body_bytes: limits
                .max_body_bytes
                .checked_add(limits.max_payload_bytes)
                .ok_or(JournalError::AppendCapacity)?,
            ..limits
        };
        let mut body = encode_operation_body(&suffix, expanded_limits)?;
        pack_append_payload(
            &mut body,
            expanded_limits,
            &mut AppendPackScratch::new(limits.max_payload_bytes),
        )?;
        if body.len() > self.limits.max_body_bytes.min(limits.max_body_bytes) {
            return Err(JournalError::AppendCapacity);
        }
        drop(suffix);
        drop(decoded);
        // Everything above is read-only. Failed rebuilds preserve the request.
        self.bodies
            .reserve(body.len().saturating_sub(self.bodies.len()))?;
        let target = self.bodies.mutable()?;
        target.clear();
        target.extend_from_slice(&body);
        self.entries[0].body = 0..body.len();
        self.entries[0].proof = None;
        self.entries[0].digest = None;
        Ok(())
    }
}
