//! One physical group at a time; sealed authority and zero tails remain checked.

use super::super::{DirectoryError, Journal, SegmentReference};
use crate::{CodecError, DecodedOperation, RetentionFloors};
use ozzy_journal::operation::{OperationKind, decode_append_summary_and_batches};
use ozzy_proto::{Offset, PartitionIncarnation};

pub(super) struct Summary {
    pub(super) segment: ozzy_core::retention::Segment,
    pub(super) below_floors: bool,
}

impl Journal {
    /// Resident scratch and decode reservation for one maintenance scan.
    /// Independent of segment capacity; physical jobs retain separate admission.
    pub fn retention_scratch_bytes(&self) -> Result<usize, DirectoryError> {
        let metadata = self.limits.decode.group_metadata_bytes();
        self.group_scan_bytes()?
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(self.limits.io.chunk_bytes))
            .and_then(|bytes| {
                bytes.checked_add(
                    self.limits
                        .decode
                        .max_group_decoded_body_bytes
                        .checked_mul(2)?,
                )
            })
            .and_then(|bytes| bytes.checked_add(self.limits.operations.max_payload_bytes))
            .and_then(|bytes| bytes.checked_add(metadata?))
            .and_then(|bytes| {
                bytes.checked_add(self.limits.operations.max_append_batches.checked_mul(512)?)
            })
            .ok_or(DirectoryError::RetentionScanBudget)
    }

    pub(super) async fn scan_retention(
        &self,
        reference: SegmentReference,
        partition: Option<PartitionIncarnation>,
        floors: Option<(&RetentionFloors, u64)>,
    ) -> Result<Summary, DirectoryError> {
        let mut groups = self.segment_groups(reference).await?;
        let mut summary = Summary::new(reference);
        while let Some(group) = groups.next().await? {
            for operation in &group.operations {
                self.fold_retention(&mut summary, operation, partition, floors)?;
                summary.segment.last_operation = operation.op_number;
            }
        }
        groups.finish().await?;
        Ok(summary)
    }

    fn fold_retention(
        &self,
        result: &mut Summary,
        operation: &DecodedOperation<'_>,
        partition: Option<PartitionIncarnation>,
        floors: Option<(&RetentionFloors, u64)>,
    ) -> Result<(), DirectoryError> {
        if let Some((_, through)) = floors {
            result.below_floors &= operation.op_number <= through;
        }
        if operation.kind != OperationKind::Append {
            return Ok(());
        }
        let (_, batches) =
            decode_append_summary_and_batches(&operation.body, self.limits.operations)?;
        for batch in &batches {
            let end = batch
                .summary
                .first_offset
                .get()
                .checked_add(batch.summary.record_count as u64)
                .ok_or(CodecError::LengthOverflow)?;
            if partition == Some(batch.summary.partition) {
                result.segment.record_end = result.segment.record_end.max(Offset::new(end));
                result.segment.newest_append_millis = Some(
                    result
                        .segment
                        .newest_append_millis
                        .unwrap_or(0)
                        .max(batch.append_timestamp_millis),
                );
            }
            if let Some((floors, _)) = floors {
                result.below_floors &= floors
                    .get(batch.summary.partition)
                    .is_some_and(|floor| end <= floor.get());
            }
        }
        Ok(())
    }
}

impl Summary {
    fn new(reference: SegmentReference) -> Self {
        Self {
            segment: ozzy_core::retention::Segment {
                id: reference.segment_id,
                capacity: reference.capacity,
                sealed: true,
                last_operation: reference.first_chain.next_op_number() - 1,
                record_end: Offset::ZERO,
                newest_append_millis: None,
            },
            below_floors: true,
        }
    }
}
