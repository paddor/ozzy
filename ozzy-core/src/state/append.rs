//! Append admission for materialized records and validated metadata.

use super::{
    AppendCursor, CanonicalState, IdentityClaim, Mutation, Offset, PartitionIncarnation,
    ProducerSequence, StateError, TransitionPlan, require_id,
};
use ahash::AHashSet as HashSet;
use ozzy_journal::operation::{
    Append, AppendBatchSummary, AppendResults, AppendSummary, ResolvedAppend,
};
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
    pub(super) fn prepare_resolved_append(
        &self,
        op_number: u64,
        value: &ResolvedAppend<'_>,
    ) -> Result<TransitionPlan, StateError> {
        for record in &value.batch.records {
            require_id("message", record.message_id.as_bytes())?;
            if record.parts.is_empty() {
                return Err(StateError::MalformedBody(
                    "append record must contain at least one part",
                ));
            }
        }
        self.prepare_resolved_append_summary(
            op_number,
            AppendBatchSummary {
                partition: value.batch.partition,
                owner_epoch: value.batch.owner_epoch,
                producer_id: value.batch.producer_id,
                producer_epoch: value.batch.producer_epoch,
                first_sequence: value.batch.first_sequence,
                first_offset: value.batch.first_offset,
                record_count: value.batch.records.len(),
                nonzero_message_ids: true,
            },
            value.results,
        )
    }

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
        self.plan_retry_spans(&mut cursors)?;
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
        self.plan_retry_spans(&mut cursors)?;
        Ok(TransitionPlan {
            expected_revision: self.revision,
            op_number,
            mutation: Mutation::Append(cursors),
            claims: Vec::new(),
        })
    }

    fn append_cursor(&self, batch: AppendBatchSummary) -> Result<AppendCursor, StateError> {
        self.append_coordinates(batch, batch.record_count)
    }

    /// Validate schema-checked attempt outcomes against this exact state image.
    /// Sequence results remain distinct from partition-global stored offsets.
    pub fn prepare_resolved_append_summary(
        &self,
        op_number: u64,
        batch: AppendBatchSummary,
        results: AppendResults<'_>,
    ) -> Result<TransitionPlan, StateError> {
        self.require_next_operation(op_number)?;
        // Only the coordinate checks matter; the cursor is rebuilt from results.
        self.append_coordinates(batch, results.attempts() as usize)?;
        if batch.record_count != results.stored() as usize {
            return Err(StateError::MalformedBody(
                "stored outcomes do not match records",
            ));
        }
        let partition = self.require_partition(batch.partition)?;
        if results.logical_time() < partition.dedup_time {
            return Err(StateError::MalformedBody("deduplication time regressed"));
        }
        let producer = partition
            .producer(batch.producer_id)
            .ok_or(StateError::ProducerMismatch)?;
        let mut prepared = producer.prepare_results(
            batch.first_sequence,
            batch.first_offset,
            results,
            op_number,
        )?;
        // The span its first result extends stays.
        let kept = prepared.spans.len() - prepared.additional;
        prepared.evict = self.retry_span_evictions(
            producer.result_spans().len().saturating_sub(kept),
            prepared.additional,
        )?;
        Ok(TransitionPlan {
            expected_revision: self.revision,
            op_number,
            mutation: Mutation::ResolvedAppend {
                partition: batch.partition,
                producer: batch.producer_id,
                results: prepared,
                time: results.logical_time(),
            },
            claims: Vec::new(),
        })
    }

    fn append_coordinates(
        &self,
        batch: AppendBatchSummary,
        attempts: usize,
    ) -> Result<AppendCursor, StateError> {
        if attempts == 0 {
            return Err(StateError::MalformedBody("empty producer assignment"));
        }
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
        let attempts = u64::try_from(attempts).map_err(|_| StateError::PositionExhausted)?;
        let next_sequence = batch
            .first_sequence
            .get()
            .checked_add(attempts)
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
            evict: false,
            next_sequence,
            next_offset,
        })
    }

    /// Charge each batch's new result span against the limit. At the limit,
    /// a batch's producer drops its own oldest span for the new one.
    fn plan_retry_spans(&self, cursors: &mut [AppendCursor]) -> Result<(), StateError> {
        let mut total = self.retry_span_count;
        for cursor in cursors.iter_mut().filter(|cursor| cursor.new_span) {
            if total < self.limits.max_retry_spans {
                total += 1;
                continue;
            }
            let held = self
                .partitions
                .get(&cursor.partition)
                .and_then(|partition| partition.producer(cursor.producer))
                .map_or(0, |producer| producer.result_spans().len());
            if held == 0 {
                return Err(StateError::LimitExceeded("producer retry spans"));
            }
            cursor.evict = true;
        }
        Ok(())
    }

    /// Oldest spans a producer holding `held` drops so `additional` new ones
    /// fit the limit. A producer gives up only its own retry history; one
    /// without enough is refused.
    pub(super) fn retry_span_evictions(
        &self,
        held: usize,
        additional: usize,
    ) -> Result<usize, StateError> {
        let excess = self
            .retry_span_count
            .checked_add(additional)
            .ok_or(StateError::LimitExceeded("producer retry spans"))?
            .saturating_sub(self.limits.max_retry_spans);
        if excess > held || additional > self.limits.max_retry_spans {
            return Err(StateError::LimitExceeded("producer retry spans"));
        }
        Ok(excess)
    }
}
