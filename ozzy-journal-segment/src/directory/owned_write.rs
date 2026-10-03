//! Canonical group encoding shared by async journal writes.

use super::{DirectoryError, OperationEnvelope, validate_operation, validate_operation_metadata};
use crate::codec::{PackedOperation, PreparedGroupBodies};
use crate::{CanonicalOperation, OperationLocation};
use ozzy_journal::operation::{
    Digest, canonical_body_digest, validate_operation_body_with_validated_payload,
};

mod shared;
pub use shared::SharedJournalOperation;
mod encoding;
pub use encoding::{JournalGroupEncoder, JournalGroupEncoding};

/// Owned encoded bodies, prepared independently of the preceding physical write.
/// Physical headers are finalized when the owner selects or reserves placement.
#[derive(Debug)]
pub struct PreencodedJournalGroup {
    pub(super) operations: Vec<PackedOperation>,
    pub(super) bodies: PreparedGroupBodies,
    pub(super) shared: Vec<bytes::Bytes>,
}

/// How much of each body encoding walks again before describing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BodyCheck {
    /// Walk the whole body, including any whole-payload LZ4 block.
    Full,
    /// Walk descriptors and limits; the caller validated or produced each
    /// body's LZ4 block.
    Descriptors,
    /// The caller decoded every body with the journal's limits.
    Metadata,
}

fn describe_operations<'a>(
    operations: impl ExactSizeIterator<Item = (CanonicalOperation<'a>, Option<Digest>)>,
    decode: crate::DecodeLimits,
    limits: crate::OperationLimits,
    check: BodyCheck,
    configuration_epoch: u64,
    promised_view: u64,
) -> Result<Vec<PackedOperation>, DirectoryError> {
    if operations.len() == 0 {
        return Err(crate::CodecError::EmptyGroup.into());
    }
    require_limit("group entries", operations.len(), decode.max_entries)?;
    let mut body_bytes = 0usize;
    let mut descriptors = Vec::with_capacity(operations.len());
    for (operation, body_digest) in operations {
        let envelope = OperationEnvelope {
            kind: operation.kind,
            body: operation.body,
            op_number: operation.op_number,
            configuration_epoch: operation.configuration_epoch,
            original_view: operation.original_view,
        };
        match check {
            BodyCheck::Full => {
                validate_operation(envelope, limits, configuration_epoch, promised_view)?;
            }
            BodyCheck::Descriptors => {
                validate_operation_body_with_validated_payload(
                    envelope.kind,
                    envelope.body,
                    limits,
                )?;
                validate_operation_metadata(envelope, configuration_epoch, promised_view)?;
            }
            BodyCheck::Metadata => {
                validate_operation_metadata(envelope, configuration_epoch, promised_view)?;
            }
        }
        body_bytes = body_bytes
            .checked_add(operation.body.len())
            .ok_or(crate::CodecError::LengthOverflow)?;
        require_limit(
            "decoded operation bytes",
            operation.body.len(),
            decode.max_decoded_body_bytes,
        )?;
        require_limit(
            "decoded group bytes",
            body_bytes,
            decode.max_group_decoded_body_bytes,
        )?;
        descriptors.push(PackedOperation {
            header: operation.header(),
            body_bytes: operation.body.len(),
            body_digest: body_digest.unwrap_or_else(|| canonical_body_digest(operation.body)),
        });
    }
    Ok(descriptors)
}

impl PreencodedJournalGroup {
    pub(super) fn locations(
        &self,
        header: &crate::SegmentHeader,
        offset: u64,
    ) -> Result<Vec<OperationLocation>, DirectoryError> {
        let layouts = self.bodies.entry_layouts(offset)?;
        Ok(self
            .operations
            .iter()
            .zip(layouts)
            .map(|(op, layout)| OperationLocation {
                segment_id: header.segment_id(),
                entry_offset: layout.entry_offset,
                entry_bytes: layout.entry_bytes,
                op_number: op.header.op_number,
                operation_digest: op.header.digest(op.body_digest),
            })
            .collect())
    }
}

fn require_limit(kind: &'static str, actual: usize, limit: usize) -> Result<(), DirectoryError> {
    if actual > limit {
        Err(crate::CodecError::LimitExceeded {
            kind,
            actual,
            limit,
        }
        .into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
