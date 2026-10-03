//! Owned canonical bodies for buffered vectored writes. Only framing is copied.

use bytes::Bytes;
use ozzy_journal::operation::{CanonicalOperation, Digest, OperationHeader};

/// An immutable body and its owner-computed digest. The caller must compute the
/// digest over these exact bytes before handing ownership to the write pipeline.
#[derive(Debug)]
pub struct SharedJournalOperation {
    /// Validated physical segment or canonical operation header.
    pub header: OperationHeader,
    /// Immutable canonical operation-body backing.
    pub body: Bytes,
    /// Integrity digest over the exact canonical operation body.
    pub body_digest: Digest,
}

impl SharedJournalOperation {
    pub(super) fn canonical(&self) -> CanonicalOperation<'_> {
        CanonicalOperation {
            group_id: self.header.group_id,
            configuration_epoch: self.header.configuration_epoch,
            original_view: self.header.original_view,
            op_number: self.header.op_number,
            previous_digest: self.header.previous_digest,
            kind: self.header.kind,
            body: &self.body,
        }
    }
}
