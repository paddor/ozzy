use super::{
    ActiveIndexError, IndexLimits, IndexSource, SegmentIndexImage, enforce_footprint,
    extend_bounded, validate_limits,
};
use crate::{
    DecodedOperation, Digest, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation, SegmentHeader, derive_index_entries,
};
use ozzy_journal::operation::OperationLimits;

pub(super) struct Entries {
    offsets: Vec<OffsetIndexEntry>,
    messages: Vec<MessageIndexEntry>,
    operations: Vec<OperationIndexEntry>,
    locations: Vec<OperationLocation>,
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
            locations: Vec::new(),
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
            &mut self.locations,
            [derived.location],
            self.limits.max_operation_entries,
            "active operation locations",
        )?;
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

    /// Source of these entries appended to an optional validated `base`.
    pub(super) fn source(
        &self,
        header: &SegmentHeader,
        valid_bytes: u64,
        digest: Digest,
        through_op: u64,
        base: Option<&SegmentIndexImage>,
    ) -> Result<Option<IndexSource>, ActiveIndexError> {
        let Some(first_op_number) = base
            .map(|base| base.source().first_op_number)
            .or(self.first)
        else {
            return Ok(None);
        };
        let (last_op_number, last_operation_digest) = match (self.last, base) {
            (Some(last), _) => (last, self.digest),
            (None, Some(base)) => (
                base.source().last_op_number,
                base.source().last_operation_digest,
            ),
            (None, None) => unreachable!("a first operation has a last operation"),
        };
        if last_op_number != through_op {
            return Err(ActiveIndexError::PositionNotCovered {
                requested: through_op,
                last: last_op_number,
            });
        }
        let base_len = |len: fn(&SegmentIndexImage) -> usize| base.map_or(0, len);
        enforce_footprint(
            self.offsets.len() + base_len(|base| base.offsets().len()),
            self.messages.len() + base_len(|base| base.messages().len()),
            self.operations.len() + base_len(|base| base.operations().len()),
            self.locations.len() + base_len(|base| base.locations().len()),
            self.limits,
        )?;
        Ok(Some(IndexSource {
            group_id: header.group_id(),
            segment_id: header.segment_id(),
            valid_bytes,
            segment_digest: digest,
            first_op_number,
            last_op_number,
            last_operation_digest,
        }))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.last.is_none()
    }

    pub(super) async fn extend_async(
        self,
        base: &SegmentIndexImage,
        source: IndexSource,
    ) -> Result<SegmentIndexImage, ActiveIndexError> {
        Ok(base
            .extend_async(
                source,
                self.offsets,
                self.messages,
                self.operations,
                self.locations,
            )
            .await?)
    }

    pub(super) fn finish(self, source: IndexSource) -> Result<SegmentIndexImage, ActiveIndexError> {
        Ok(SegmentIndexImage::new(
            source,
            0,
            self.offsets,
            self.messages,
            self.operations,
            self.locations,
        )?)
    }

    pub(super) async fn finish_async(
        self,
        source: IndexSource,
    ) -> Result<SegmentIndexImage, ActiveIndexError> {
        Ok(SegmentIndexImage::new_async(
            source,
            0,
            self.offsets,
            self.messages,
            self.operations,
            self.locations,
        )
        .await?)
    }
}
