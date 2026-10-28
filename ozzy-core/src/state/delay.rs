//! Scheduled records held per partition until due. New records always sort
//! after released ones, so each release takes a prefix in release order.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use ozzy_journal::operation::{AppendBatch, Release, ReleaseKey, Schedule};
use ozzy_proto::{MessageId, Offset, PartitionIncarnation, ProducerId, ProducerSequence};

use super::{
    CanonicalState, IdentityClaim, Mutation, ProducerResultSource, ProducerResultSpan, StateError,
    require_id, require_record_parts,
};

/// One held record. Its payload stays in its `Schedule` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingSchedule {
    /// `Schedule` operation holding the record.
    pub operation: u64,
    /// Record index inside that operation's batch.
    pub index: u32,
    /// The record's encoded bytes: descriptor, part lengths, and payload.
    pub bytes: u32,
    /// Application record identity, kept through release.
    pub message_id: MessageId,
}

/// Held records of one delay-enabled partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelayState {
    /// Immutable maximum delay between acceptance and due time.
    pub max_delay_millis: NonZeroU64,
    /// Next schedule sequence.
    pub next_schedule: u64,
    /// Latest schedule or release timestamp; later ones never go below it.
    pub time: u64,
    pending: BTreeMap<ReleaseKey, PendingSchedule>,
    /// Held records per `Schedule` operation, for the retention floor.
    operations: BTreeMap<u64, usize>,
    /// Encoded bytes of every held record.
    bytes: u64,
}

impl DelayState {
    /// Empty state with no schedules yet.
    pub const fn new(max_delay_millis: NonZeroU64) -> Self {
        Self {
            max_delay_millis,
            next_schedule: 0,
            time: 0,
            pending: BTreeMap::new(),
            operations: BTreeMap::new(),
            bytes: 0,
        }
    }

    /// Held records in release order.
    pub fn pending(&self) -> impl ExactSizeIterator<Item = (ReleaseKey, &PendingSchedule)> {
        self.pending.iter().map(|(key, value)| (*key, value))
    }

    /// Number of held records.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Encoded bytes of every held record.
    pub const fn held_bytes(&self) -> u64 {
        self.bytes
    }

    /// Release key of the next record to release.
    pub fn next_release(&self) -> Option<ReleaseKey> {
        self.pending.keys().next().copied()
    }

    /// Held records due at `now_millis`, in release order.
    pub fn due(&self, now_millis: u64) -> impl Iterator<Item = (ReleaseKey, &PendingSchedule)> {
        let through = ReleaseKey {
            millis: now_millis,
            schedule: u64::MAX,
        };
        self.pending
            .range(..=through)
            .map(|(key, value)| (*key, value))
    }

    /// Oldest `Schedule` operation that still holds a record. Retention keeps
    /// it and every later operation.
    pub fn oldest_operation(&self) -> Option<u64> {
        self.operations.keys().next().copied()
    }

    /// Rebuild from validated snapshot entries in release order.
    pub(super) fn restore(&mut self, pending: BTreeMap<ReleaseKey, PendingSchedule>) {
        let mut operations = BTreeMap::<u64, usize>::new();
        for value in pending.values() {
            *operations.entry(value.operation).or_default() += 1;
        }
        self.bytes = pending.values().map(|value| u64::from(value.bytes)).sum();
        self.pending = pending;
        self.operations = operations;
    }

    fn hold(&mut self, operation: u64, pending: Vec<(ReleaseKey, PendingSchedule)>) {
        self.operations.insert(operation, pending.len());
        self.bytes += pending
            .iter()
            .map(|(_, value)| u64::from(value.bytes))
            .sum::<u64>();
        self.pending.extend(pending);
    }

    fn release_through(&mut self, through: ReleaseKey) {
        while let Some(entry) = self.pending.first_entry() {
            if *entry.key() > through {
                break;
            }
            let value = entry.remove();
            self.bytes -= u64::from(value.bytes);
            let operation = value.operation;
            if let Some(count) = self.operations.get_mut(&operation) {
                *count -= 1;
                if *count == 0 {
                    self.operations.remove(&operation);
                }
            }
        }
    }
}

/// One validated schedule or release.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PreparedDelay {
    Schedule(PreparedSchedule),
    Release(PreparedRelease),
}

/// Held records and coordinates of one validated `Schedule`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreparedSchedule {
    partition: PartitionIncarnation,
    producer: ProducerId,
    span: ProducerResultSpan,
    time: u64,
    pending: Vec<(ReleaseKey, PendingSchedule)>,
    /// Oldest spans the producer drops first to fit the span limit.
    evict: usize,
}

/// Coordinates of one validated `Release`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PreparedRelease {
    partition: PartitionIncarnation,
    through: ReleaseKey,
    time: u64,
    next_offset: Offset,
}

impl CanonicalState {
    /// A schedule consumes producer sequences and schedule sequences, but no
    /// offsets. Each due time lies within the partition's maximum delay.
    pub(super) fn prepare_schedule(
        &self,
        op_number: u64,
        value: &Schedule<'_>,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let batch = &value.batch;
        let delay = self.schedule_target(value)?;
        let time = batch.append_timestamp_millis;
        if time < delay.time {
            return Err(StateError::DelayTimeRegressed);
        }
        if delay
            .pending
            .len()
            .checked_add(batch.records.len())
            .is_none_or(|total| total > self.limits.max_pending_schedules)
        {
            return Err(StateError::LimitExceeded("pending schedules"));
        }
        let held = self
            .require_partition(batch.partition)?
            .producer(batch.producer_id)
            .map_or(0, |producer| producer.result_spans().len());
        let evict = self.retry_span_evictions(held, 1)?;
        let latest = time.saturating_add(delay.max_delay_millis.get());
        let pending = held_records(op_number, value, latest)?;
        let span = ProducerResultSpan {
            first_sequence: batch.first_sequence,
            first_offset: Offset::new(value.first_schedule),
            records: u64::try_from(pending.len()).map_err(|_| StateError::PositionExhausted)?,
            source: ProducerResultSource::Scheduled {
                operation: op_number,
            },
        };
        Ok((
            Mutation::Delay(PreparedDelay::Schedule(PreparedSchedule {
                partition: batch.partition,
                producer: batch.producer_id,
                span,
                time,
                pending,
                evict,
            })),
            Vec::new(),
        ))
    }

    /// The delay state a schedule extends, once its owner, producer, offset,
    /// and both sequence ranges check out.
    fn schedule_target(&self, value: &Schedule<'_>) -> Result<&DelayState, StateError> {
        let batch = &value.batch;
        let records = batch.records.len();
        if records == 0 || value.due_millis.len() != records {
            return Err(StateError::MalformedBody(
                "schedule due times do not match its records",
            ));
        }
        let partition = self.require_partition(batch.partition)?;
        let delay = partition.delay.as_ref().ok_or(StateError::DelayDisabled)?;
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
        if batch.first_offset != Offset::ZERO {
            return Err(StateError::OffsetMismatch);
        }
        if value.first_schedule != delay.next_schedule {
            return Err(StateError::ScheduleMismatch);
        }
        let count = u64::try_from(records).map_err(|_| StateError::PositionExhausted)?;
        batch
            .first_sequence
            .get()
            .checked_add(count)
            .and(value.first_schedule.checked_add(count))
            .ok_or(StateError::PositionExhausted)?;
        Ok(delay)
    }

    /// A release copies exactly the held records through its key, in release
    /// order, at the next offsets. Its timestamp is at least their due time.
    pub(super) fn prepare_release(
        &self,
        value: &Release<'_>,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let batch = &value.batch;
        let partition = self.require_partition(batch.partition)?;
        let delay = partition.delay.as_ref().ok_or(StateError::DelayDisabled)?;
        if batch.owner_epoch != partition.owner_epoch {
            return Err(StateError::OwnerEpochMismatch);
        }
        if !is_broker_batch(batch) {
            return Err(StateError::MalformedBody(
                "release batch must not name a producer",
            ));
        }
        if batch.first_offset != partition.next_offset {
            return Err(StateError::OffsetMismatch);
        }
        let time = batch.append_timestamp_millis;
        if time < delay.time || time < value.through.millis {
            return Err(StateError::DelayTimeRegressed);
        }
        let count =
            u64::try_from(batch.records.len()).map_err(|_| StateError::PositionExhausted)?;
        let next_offset = batch
            .first_offset
            .get()
            .checked_add(count)
            .map(Offset::new)
            .ok_or(StateError::PositionExhausted)?;
        let mut held = delay.pending.range(..=value.through);
        let mut last = None;
        for record in &batch.records {
            let (key, pending) = held.next().ok_or(StateError::ReleaseMismatch)?;
            if record.message_id != pending.message_id || record_bytes(record)? != pending.bytes {
                return Err(StateError::ReleaseMismatch);
            }
            last = Some(*key);
        }
        if held.next().is_some() || last != Some(value.through) {
            return Err(StateError::ReleaseMismatch);
        }
        Ok((
            Mutation::Delay(PreparedDelay::Release(PreparedRelease {
                partition: batch.partition,
                through: value.through,
                time,
                next_offset,
            })),
            Vec::new(),
        ))
    }

    pub(super) fn install_delay(&mut self, prepared: PreparedDelay) -> Result<(), StateError> {
        match prepared {
            PreparedDelay::Schedule(prepared) => self.install_schedule(prepared),
            PreparedDelay::Release(prepared) => self.install_release(prepared),
        }
    }

    fn install_schedule(&mut self, prepared: PreparedSchedule) -> Result<(), StateError> {
        let max_spans = self.limits.max_retry_spans;
        let state = self.partition_mut(prepared.partition)?;
        let producer = state.producer_mut(prepared.producer)?;
        producer.evict_results(prepared.evict);
        producer.record_schedule(prepared.span, max_spans)?;
        let delay = state.delay.as_mut().ok_or(StateError::DelayDisabled)?;
        delay.next_schedule = prepared.span.first_offset.get() + prepared.span.records;
        delay.time = prepared.time;
        if let ProducerResultSource::Scheduled { operation } = prepared.span.source {
            delay.hold(operation, prepared.pending);
        }
        self.count_retry_spans(1, prepared.evict)
    }

    fn install_release(&mut self, prepared: PreparedRelease) -> Result<(), StateError> {
        let state = self.partition_mut(prepared.partition)?;
        state.next_offset = prepared.next_offset;
        let delay = state.delay.as_mut().ok_or(StateError::DelayDisabled)?;
        delay.time = prepared.time;
        delay.release_through(prepared.through);
        Ok(())
    }
}

/// Held entries of a new `Schedule`, each due no later than `latest`.
fn held_records(
    op_number: u64,
    value: &Schedule<'_>,
    latest: u64,
) -> Result<Vec<(ReleaseKey, PendingSchedule)>, StateError> {
    for record in &value.batch.records {
        require_id("message", record.message_id.as_bytes())?;
        require_record_parts(record.parts.len())?;
    }
    if value.due_millis.iter().any(|&due| due > latest) {
        return Err(StateError::DueBeyondMaximum);
    }
    schedule_entries(op_number, value)
}

/// Entries of a `Schedule` keyed by `(max(due, acceptance), schedule)`, in
/// batch order. Segment indexes derive their held-record rows with it.
pub fn schedule_entries(
    op_number: u64,
    value: &Schedule<'_>,
) -> Result<Vec<(ReleaseKey, PendingSchedule)>, StateError> {
    let time = value.batch.append_timestamp_millis;
    value
        .batch
        .records
        .iter()
        .zip(&value.due_millis)
        .enumerate()
        .map(|(index, (record, &due))| {
            let index = u32::try_from(index).map_err(|_| StateError::PositionExhausted)?;
            let schedule = value
                .first_schedule
                .checked_add(u64::from(index))
                .ok_or(StateError::PositionExhausted)?;
            Ok((
                ReleaseKey {
                    millis: due.max(time),
                    schedule,
                },
                PendingSchedule {
                    operation: op_number,
                    index,
                    bytes: record_bytes(record)?,
                    message_id: record.message_id,
                },
            ))
        })
        .collect()
}

/// A release batch is written by the broker, not a producer session.
fn is_broker_batch(batch: &AppendBatch<'_>) -> bool {
    batch.producer_id.as_bytes() == &[0; 16]
        && batch.producer_epoch.get() == 0
        && batch.first_sequence == ProducerSequence::ZERO
}

fn record_bytes(record: ozzy_journal::operation::RecordRef<'_>) -> Result<u32, StateError> {
    let total = ozzy_journal::operation::encoded_record_bytes(record.encoding, record.parts.iter());
    u32::try_from(total).map_err(|_| StateError::LimitExceeded("record bytes"))
}
