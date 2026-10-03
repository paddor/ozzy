use super::{
    ActiveIndexError, IndexLimits, IndexSource, SegmentIndexImage, SegmentScan, enforce_footprint,
    extend_bounded, validate_limits,
};
use crate::{
    DecodedOperation, Digest, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry,
    SegmentHeader, derive_index_entries,
};
use ozzy_journal::operation::OperationLimits;

pub(super) struct Entries {
    offsets: Vec<OffsetIndexEntry>,
    messages: Vec<MessageIndexEntry>,
    operations: Vec<OperationIndexEntry>,
    first: Option<u64>,
    last: Option<u64>,
    digest: Digest,
    limits: IndexLimits,
}

impl Entries {
    pub(super) fn new(limits: IndexLimits) -> Result<Self, ActiveIndexError> {
        validate_limits(limits)?;
        Ok(Self {
            offsets: Vec::new(),
            messages: Vec::new(),
            operations: Vec::new(),
            first: None,
            last: None,
            digest: Digest::ZERO,
            limits,
        })
    }

    pub(super) fn push(
        &mut self,
        header: &SegmentHeader,
        operation: &DecodedOperation<'_>,
        limits: OperationLimits,
    ) -> Result<(), ActiveIndexError> {
        self.first.get_or_insert(operation.op_number);
        let derived = derive_index_entries(header, operation, limits)?;
        extend_bounded(
            &mut self.offsets,
            derived.offsets,
            self.limits.max_offset_entries,
            "active offset entries",
        )?;
        extend_bounded(
            &mut self.messages,
            derived.messages,
            self.limits.max_message_entries,
            "active message entries",
        )?;
        if let Some(entry) = derived.operation {
            extend_bounded(
                &mut self.operations,
                [entry],
                self.limits.max_operation_entries,
                "active operation entries",
            )?;
        }
        self.last = Some(operation.op_number);
        self.digest = operation.digest;
        Ok(())
    }

    pub(super) fn source(
        &self,
        scan: &SegmentScan<'_>,
        through_op: u64,
    ) -> Result<Option<IndexSource>, ActiveIndexError> {
        let Some(first_op_number) = self.first else {
            return Ok(None);
        };
        let last_op_number = self
            .last
            .expect("first operation establishes last operation");
        if last_op_number != through_op {
            return Err(ActiveIndexError::PositionNotCovered {
                requested: through_op,
                last: last_op_number,
            });
        }
        enforce_footprint(
            self.offsets.len(),
            self.messages.len(),
            self.operations.len(),
            self.limits,
        )?;
        Ok(Some(IndexSource {
            group_id: scan.header.group_id(),
            segment_id: scan.header.segment_id(),
            valid_bytes: scan.valid_bytes,
            segment_digest: scan.digest,
            first_op_number,
            last_op_number,
            last_operation_digest: self.digest,
        }))
    }

    pub(super) fn finish(self, source: IndexSource) -> Result<SegmentIndexImage, ActiveIndexError> {
        Ok(SegmentIndexImage::new(
            source,
            0,
            self.offsets,
            self.messages,
            self.operations,
        )?)
    }

    pub(super) async fn finish_async(
        self,
        source: IndexSource,
    ) -> Result<SegmentIndexImage, ActiveIndexError> {
        Ok(
            SegmentIndexImage::new_async(source, 0, self.offsets, self.messages, self.operations)
                .await?,
        )
    }
}
