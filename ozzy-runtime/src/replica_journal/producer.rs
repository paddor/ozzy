//! Storage-independent writer assignment and exact retry policy.

use ozzy_core::state::{CanonicalProducer, CanonicalState, ProducerResultSpan};
use ozzy_journal::operation::{
    AppendBatchSummary, OperationKind, OperationLimits, RecordRef, decode_append_batches,
};
use ozzy_journal_segment::IndexedRecord;
use ozzy_proto::{Offset, ProducerSequence};
use ozzy_replication::{JournalGeneration, Prefix};
use smallvec::SmallVec;

use super::{AppendAdmissionError, AppendBuffer, JournalError};

pub(super) struct Assignment {
    pub position: [u8; 16],
    pub retry: Option<Prefix>,
    pub trim: usize,
    pub results: SmallVec<[ProducerResultSpan; 2]>,
}

impl Assignment {
    pub(super) fn fresh(offset: Offset, timestamp: u64) -> Self {
        Self {
            position: position(offset, timestamp),
            retry: None,
            trim: 0,
            results: SmallVec::new(),
        }
    }
}

pub(super) enum Plan {
    Fresh(Offset),
    Retry(Retry),
}

/// Only the bounded requested ranges survive into disk awaits, not a clone of
/// a writer's complete retry history or a borrow of the canonical state.
pub(super) struct Retry {
    pub batch: AppendBatchSummary,
    pub retained: usize,
    next_offset: Offset,
    results: SmallVec<[ProducerResultSpan; 2]>,
}

impl Retry {
    pub(super) fn offset(&self, index: usize) -> Result<Offset, JournalError> {
        let sequence = self.batch.first_sequence.get() + index as u64;
        self.results
            .iter()
            .find_map(|span| {
                let delta = sequence.checked_sub(span.first_sequence.get())?;
                (delta < span.records).then(|| Offset::new(span.first_offset.get() + delta))
            })
            .ok_or_else(|| AppendAdmissionError::RetryHistoryExpired.into())
    }

    pub(super) fn finish(mut self, timestamp: u64, through: Prefix, now: u64) -> Assignment {
        if self.retained == self.batch.record_count {
            return Assignment {
                position: position(self.results[0].first_offset, timestamp),
                retry: Some(through),
                trim: 0,
                results: self.results,
            };
        }
        push_result(
            &mut self.results,
            ProducerResultSpan {
                first_sequence: ProducerSequence::new(
                    self.batch.first_sequence.get() + self.retained as u64,
                ),
                first_offset: self.next_offset,
                records: (self.batch.record_count - self.retained) as u64,
            },
        );
        Assignment {
            trim: self.retained,
            results: self.results,
            ..Assignment::fresh(self.next_offset, now)
        }
    }

    pub(super) fn verify(
        &self,
        index: usize,
        actual: &IndexedRecord,
        expected: RecordRef<'_>,
    ) -> Result<(), JournalError> {
        if actual.partition != self.batch.partition
            || actual.owner_epoch != self.batch.owner_epoch
            || actual.producer_id != self.batch.producer_id
            || actual.producer_epoch != self.batch.producer_epoch
            || actual.producer_sequence.get() != self.batch.first_sequence.get() + index as u64
            || actual.offset != self.offset(index)?
            || actual.encoding != expected.encoding
            || actual.message_id != expected.message_id
            || actual.parts.len() != expected.parts.len()
            || !actual
                .parts
                .iter()
                .zip(expected.parts.iter())
                .all(|(a, b)| a.as_ref() == b)
        {
            return Err(AppendAdmissionError::RetryConflict.into());
        }
        Ok(())
    }
}

pub(super) fn plan(
    image: &CanonicalState,
    buffer: &AppendBuffer,
    index: usize,
    generation: JournalGeneration,
    limits: OperationLimits,
) -> Result<Plan, JournalError> {
    if buffer.owner_generation() != generation {
        return Err(JournalError::AppendMismatch);
    }
    let batch = summary(buffer, index, limits)?;
    let state = image
        .partition(batch.partition)
        .ok_or(AppendAdmissionError::UnknownPartition)?;
    let producer = state
        .producer(batch.producer_id)
        .ok_or(AppendAdmissionError::Fenced)?;
    if state.owner_epoch != batch.owner_epoch || producer.producer_epoch != batch.producer_epoch {
        return Err(AppendAdmissionError::Fenced.into());
    }
    let (offset_delta, sequence_delta) = prior_records(buffer, index, &batch, limits)?;
    let next_offset = Offset::new(
        state
            .next_offset
            .get()
            .checked_add(offset_delta)
            .ok_or(AppendAdmissionError::Sequence)?,
    );
    let next_sequence = producer
        .next_producer_sequence
        .get()
        .checked_add(sequence_delta)
        .ok_or(AppendAdmissionError::Sequence)?;
    if batch.first_sequence < producer.producer_result_floor {
        return Err(AppendAdmissionError::RetryHistoryExpired.into());
    }
    if batch.first_sequence.get() > next_sequence {
        return Err(AppendAdmissionError::SequenceGap.into());
    }
    if batch.first_sequence.get() == next_sequence {
        return Ok(Plan::Fresh(next_offset));
    }
    let end = batch
        .first_sequence
        .get()
        .checked_add(batch.record_count as u64)
        .ok_or(AppendAdmissionError::Sequence)?;
    if end > producer.next_producer_sequence.get() && !buffer.producer_stream {
        return Err(AppendAdmissionError::Sequence.into());
    }
    let offset = producer
        .result_offset(batch.first_sequence)
        .ok_or(AppendAdmissionError::RetryHistoryExpired)?;
    if offset < state.retained_from {
        return Err(AppendAdmissionError::RetryHistoryExpired.into());
    }
    let retained =
        usize::try_from(producer.next_producer_sequence.get() - batch.first_sequence.get())
            .unwrap_or(usize::MAX)
            .min(batch.record_count);
    let results = retained_results(producer, batch.first_sequence, retained)?;
    if retained == batch.record_count && !buffer.producer_stream && results.len() != 1 {
        return Err(AppendAdmissionError::Sequence.into());
    }
    Ok(Plan::Retry(Retry {
        batch,
        retained,
        next_offset,
        results,
    }))
}

fn summary(
    buffer: &AppendBuffer,
    index: usize,
    limits: OperationLimits,
) -> Result<AppendBatchSummary, JournalError> {
    let operation = buffer
        .operations()
        .nth(index)
        .ok_or(JournalError::AppendMismatch)?;
    if operation.kind != OperationKind::Append {
        return Err(JournalError::AppendMismatch);
    }
    if let Some(summary) = buffer.validated_producer_summary(index) {
        return Ok(summary);
    }
    let decoded = decode_append_batches(operation.body, limits)?;
    let [view] = decoded.as_slice() else {
        return Err(JournalError::AppendMismatch);
    };
    Ok(view.summary)
}

fn prior_records(
    buffer: &AppendBuffer,
    index: usize,
    batch: &AppendBatchSummary,
    limits: OperationLimits,
) -> Result<(u64, u64), JournalError> {
    let (mut offsets, mut sequences) = (0_u64, 0_u64);
    for prior_index in 0..index {
        let prior = summary(buffer, prior_index, limits)?;
        if prior.partition == batch.partition {
            offsets = offsets
                .checked_add(prior.record_count as u64)
                .ok_or(AppendAdmissionError::Sequence)?;
            if prior.producer_id == batch.producer_id
                && prior.producer_epoch == batch.producer_epoch
            {
                sequences = sequences
                    .checked_add(prior.record_count as u64)
                    .ok_or(AppendAdmissionError::Sequence)?;
            }
        }
    }
    Ok((offsets, sequences))
}

fn retained_results(
    producer: &CanonicalProducer,
    first: ProducerSequence,
    records: usize,
) -> Result<SmallVec<[ProducerResultSpan; 2]>, JournalError> {
    let mut results = SmallVec::new();
    for index in 0..records {
        let sequence = ProducerSequence::new(first.get() + index as u64);
        let offset = producer
            .result_offset(sequence)
            .ok_or(AppendAdmissionError::RetryHistoryExpired)?;
        push_result(
            &mut results,
            ProducerResultSpan {
                first_sequence: sequence,
                first_offset: offset,
                records: 1,
            },
        );
    }
    Ok(results)
}

fn push_result(results: &mut SmallVec<[ProducerResultSpan; 2]>, next: ProducerResultSpan) {
    if let Some(last) = results.last_mut()
        && last.first_sequence.get().checked_add(last.records) == Some(next.first_sequence.get())
        && last.first_offset.get().checked_add(last.records) == Some(next.first_offset.get())
    {
        last.records += next.records;
    } else {
        results.push(next);
    }
}

fn position(offset: Offset, timestamp: u64) -> [u8; 16] {
    let mut position = [0; 16];
    position[..8].copy_from_slice(&offset.get().to_be_bytes());
    position[8..].copy_from_slice(&timestamp.to_be_bytes());
    position
}
