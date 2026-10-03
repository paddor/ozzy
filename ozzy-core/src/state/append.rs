//! Append admission for materialized records and validated metadata.

use super::{
    AppendCursor, CanonicalState, IdentityClaim, Mutation, Offset, PartitionIncarnation,
    ProducerSequence, StateError, TransitionPlan, require_id,
};
use ahash::AHashSet as HashSet;
use ozzy_journal::operation::{Append, AppendBatchSummary, AppendSummary};
use smallvec::SmallVec;

// Four or fewer partition cursors stay inline. Larger grouped appends switch
// to a set so hostile/many-partition input cannot create a quadratic scan.
fn insert_append_partition(
    cursors: &[AppendCursor],
    partitions: &mut HashSet<PartitionIncarnation>,
    partition: PartitionIncarnation,
) -> bool {
    if cursors.len() < 4 {
        return !cursors.iter().any(|cursor| cursor.partition == partition);
    }
    if partitions.is_empty() {
        partitions.extend(cursors.iter().map(|cursor| cursor.partition));
    }
    partitions.insert(partition)
}

impl CanonicalState {
    pub(super) fn prepare_append(
        &self,
        value: &Append<'_>,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        if value.batches.is_empty() {
            return Err(StateError::MalformedBody(
                "append must contain at least one batch",
            ));
        }
        let mut cursors = SmallVec::with_capacity(value.batches.len());
        let claims = Vec::new();
        let mut partitions = HashSet::new();
        for batch in &value.batches {
            if batch.records.is_empty() {
                return Err(StateError::MalformedBody(
                    "append batch must contain at least one record",
                ));
            }
            if !insert_append_partition(&cursors, &mut partitions, batch.partition) {
                return Err(StateError::DuplicateAppendPartition);
            }
            let cursor = self.append_cursor(AppendBatchSummary {
                partition: batch.partition,
                owner_epoch: batch.owner_epoch,
                producer_id: batch.producer_id,
                producer_epoch: batch.producer_epoch,
                first_sequence: batch.first_sequence,
                first_offset: batch.first_offset,
                record_count: batch.records.len(),
                nonzero_message_ids: true,
            })?;
            for record in &batch.records {
                require_id("message", record.message_id.as_bytes())?;
                if record.parts.is_empty() {
                    return Err(StateError::MalformedBody(
                        "append record must contain at least one part",
                    ));
                }
            }
            cursors.push(cursor);
        }
        self.check_retry_span_budget(&cursors)?;
        Ok((Mutation::Append(cursors), claims))
    }

    /// Validate schema-checked append metadata without materializing records.
    /// Same epoch, ownership, position, ID, and duplicate-partition checks as
    /// [`Self::prepare`]. The summary is constructed only by the body decoder.
    pub fn prepare_append_summary(
        &self,
        op_number: u64,
        summary: &AppendSummary,
    ) -> Result<TransitionPlan, StateError> {
        self.require_next_operation(op_number)?;
        let mut cursors = SmallVec::with_capacity(summary.batches().len());
        let mut partitions = HashSet::new();
        for &batch in summary.batches() {
            if !insert_append_partition(&cursors, &mut partitions, batch.partition) {
                return Err(StateError::DuplicateAppendPartition);
            }
            cursors.push(self.append_cursor(batch)?);
        }
        self.check_retry_span_budget(&cursors)?;
        Ok(TransitionPlan {
            expected_revision: self.revision,
            op_number,
            mutation: Mutation::Append(cursors),
            claims: Vec::new(),
        })
    }

    fn append_cursor(&self, batch: AppendBatchSummary) -> Result<AppendCursor, StateError> {
        let partition = self.require_partition(batch.partition)?;
        if batch.owner_epoch != partition.owner_epoch {
            return Err(StateError::OwnerEpochMismatch);
        }
        let producer = partition
            .producer(batch.producer_id)
            .ok_or(StateError::ProducerMismatch)?;
        if batch.producer_epoch != producer.producer_epoch {
            return Err(StateError::ProducerEpochMismatch);
        }
        if batch.first_sequence != producer.next_producer_sequence {
            return Err(StateError::ProducerSequenceMismatch);
        }
        if batch.first_offset != partition.next_offset {
            return Err(StateError::OffsetMismatch);
        }
        let count = u64::try_from(batch.record_count).map_err(|_| StateError::PositionExhausted)?;
        let next_sequence = batch
            .first_sequence
            .get()
            .checked_add(count)
            .map(ProducerSequence::new)
            .ok_or(StateError::PositionExhausted)?;
        let next_offset = batch
            .first_offset
            .get()
            .checked_add(count)
            .map(Offset::new)
            .ok_or(StateError::PositionExhausted)?;
        if !batch.nonzero_message_ids {
            return Err(StateError::ZeroValue("message"));
        }
        Ok(AppendCursor {
            partition: batch.partition,
            producer: batch.producer_id,
            first_sequence: batch.first_sequence,
            first_offset: batch.first_offset,
            records: count,
            new_span: producer.needs_result_span(batch.first_offset),
            next_sequence,
            next_offset,
        })
    }

    fn check_retry_span_budget(&self, cursors: &[AppendCursor]) -> Result<(), StateError> {
        let additional = cursors.iter().filter(|cursor| cursor.new_span).count();
        if self
            .retry_span_count
            .checked_add(additional)
            .is_none_or(|total| total > self.limits.max_retry_spans)
        {
            return Err(StateError::LimitExceeded("producer retry spans"));
        }
        Ok(())
    }
}
