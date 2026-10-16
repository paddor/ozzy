//! Canonical group encoding shared by async journal writes.

use super::DirectoryError;
use crate::codec::{PackedOperation, PreparedGroupBodies};
use crate::{CanonicalOperation, OperationLocation};

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

#[cfg(test)]
mod tests;
