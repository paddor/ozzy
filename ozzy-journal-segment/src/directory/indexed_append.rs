//! Physical append plus exact disposable record-index coordinates.

use ozzy_journal::operation::{
    CanonicalOperation, canonical_body_digest, logical_operation_digest_with_body_digest,
};

use super::{DirectoryError, OpenGroupJournal, OperationEnvelope, validate_operation};
use crate::{BodyEncoding, OperationLocation, WriterPosition};

impl OpenGroupJournal {
    /// Append one physical group and return its exact operation locations.
    /// This retains ordinary schema/hash validation and does not claim a write
    /// is synchronized or confirmed. No readback or active-history scan occurs.
    pub fn append_indexed_with_body_encoding(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
    ) -> Result<(WriterPosition, Vec<OperationLocation>), DirectoryError> {
        for operation in operations {
            validate_operation(
                OperationEnvelope {
                    kind: operation.kind,
                    body: operation.body,
                    op_number: operation.op_number,
                    configuration_epoch: operation.configuration_epoch,
                    original_view: operation.original_view,
                },
                self.operation_limits,
                self.directory.manifest.configuration_epoch,
                self.directory.manifest.promised_view,
            )?;
        }
        self.require_segment_decoded_capacity(operations)?;
        let digests = operations
            .iter()
            .map(|operation| canonical_body_digest(operation.body))
            .collect::<Vec<_>>();
        let prepared = self
            .writer
            .prepare_group_bodies(operations.iter().map(|op| op.body), encoding)?;
        let layouts = prepared.entry_layouts(self.writer.written_position().end_offset())?;
        let segment = self.writer.header().segment_id();
        let locations = operations
            .iter()
            .zip(&digests)
            .zip(layouts)
            .map(|((operation, digest), layout)| OperationLocation {
                segment_id: segment,
                entry_offset: layout.entry_offset,
                entry_bytes: layout.entry_bytes,
                op_number: operation.op_number,
                operation_digest: logical_operation_digest_with_body_digest(operation, *digest),
            })
            .collect();
        let written = self
            .writer
            .append_prepared_with_digests(operations, &digests, prepared)?;
        Ok((written, locations))
    }
}
