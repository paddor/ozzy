use super::{
    ActiveIndexError, IndexCounts, IndexLimits, IndexSource, SegmentIndexImage, enforce_footprint,
    extend_bounded, validate_limits,
};
use crate::{
    DecodedOperation, Digest, IndexEntries, SealedSegment, SegmentHeader, derive_index_entries,
};
use ozzy_journal::operation::OperationLimits;

pub(super) struct Entries {
    selectors: IndexEntries,
    first: Option<u64>,
    last: Option<u64>,
    digest: Digest,
    limits: IndexLimits,
}

impl Entries {
    pub(super) fn new(limits: IndexLimits) -> Result<Self, ActiveIndexError> {
        validate_limits(limits)?;
        Ok(Self {
            selectors: IndexEntries::default(),
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
        let selectors = &mut self.selectors;
        extend_bounded(
            &mut selectors.locations,
            [derived.location],
            self.limits.max_operation_entries,
            "active operation locations",
        )?;
        extend_bounded(
            &mut selectors.offsets,
            derived.offsets,
            self.limits.max_offset_entries,
            "active offset entries",
        )?;
        extend_bounded(
            &mut selectors.messages,
            derived.messages,
            self.limits.max_message_entries,
            "active message entries",
        )?;
        if let Some(entry) = derived.operation {
            extend_bounded(
                &mut selectors.operations,
                [entry],
                self.limits.max_operation_entries,
                "active operation entries",
            )?;
        }
        extend_bounded(
            &mut selectors.schedules,
            derived.schedules,
            self.limits.max_schedule_entries,
            "active schedule entries",
        )?;
        self.last = Some(operation.op_number);
        self.digest = operation.digest;
        Ok(())
    }

    /// Source of these entries appended to an optional validated `base`.
    pub(super) fn source(
        &self,
        header: &SegmentHeader,
        SealedSegment {
            valid_bytes,
            digest,
        }: SealedSegment,
        through_op: u64,
        base: Option<&SegmentIndexImage>,
    ) -> Result<Option<IndexSource>, ActiveIndexError> {
        let (first_op_number, last_op_number, last_operation_digest) =
            match (self.first.zip(self.last), base) {
                (Some((first, last)), base) => (
                    base.map_or(first, |base| base.source().first_op_number),
                    last,
                    self.digest,
                ),
                (None, Some(base)) => (
                    base.source().first_op_number,
                    base.source().last_op_number,
                    base.source().last_operation_digest,
                ),
                (None, None) => return Ok(None),
            };
        if last_op_number != through_op {
            return Err(ActiveIndexError::PositionNotCovered {
                requested: through_op,
                last: last_op_number,
            });
        }
        let base_len = |len: fn(&SegmentIndexImage) -> usize| base.map_or(0, len);
        let selectors = &self.selectors;
        enforce_footprint(
            IndexCounts {
                offsets: selectors.offsets.len() + base_len(|base| base.offsets().len()),
                messages: selectors.messages.len() + base_len(|base| base.messages().len()),
                operations: selectors.operations.len() + base_len(|base| base.operations().len()),
                locations: selectors.locations.len() + base_len(|base| base.locations().len()),
                schedules: selectors.schedules.len() + base_len(|base| base.schedules().len()),
            },
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
        Ok(base.extend_async(source, self.selectors).await?)
    }

    #[cfg(test)]
    pub(super) fn finish(self, source: IndexSource) -> Result<SegmentIndexImage, ActiveIndexError> {
        Ok(SegmentIndexImage::new(source, 0, self.selectors)?)
    }

    pub(super) async fn finish_async(
        self,
        source: IndexSource,
    ) -> Result<SegmentIndexImage, ActiveIndexError> {
        Ok(SegmentIndexImage::new_async(source, 0, self.selectors).await?)
    }
}
