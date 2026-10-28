//! Scheduled records held per partition until due. New records always sort
//! after released ones, so each release takes a prefix in release order.
//! Partition state keeps only the last released key, the carry floor, and
//! held totals; each copy checks a release's or carry's records against its
//! own held index.

use std::num::NonZeroU64;

use ozzy_journal::operation::{AppendBatch, Carry, HeldPosition, Release, ReleaseKey, Schedule};
use ozzy_proto::{MessageId, Offset, PartitionIncarnation, ProducerId, ProducerSequence};

use super::{
    CanonicalState, IdentityClaim, Mutation, ProducerResultSource, ProducerResultSpan, StateError,
    require_id, require_record_parts,
};

/// One scheduled record. Its payload stays in its `Schedule` operation, or
/// the `Carry` that last copied it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingSchedule {
    /// `Schedule` or `Carry` operation holding the record.
    pub operation: u64,
    /// Record index inside that operation's batch.
    pub index: u32,
    /// The record's encoded bytes: descriptor, part lengths, and payload.
    pub bytes: u32,
    /// Application record identity, kept through release.
    pub message_id: MessageId,
}

/// Held records of one delay-enabled partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelayState {
    /// Immutable maximum delay between acceptance and due time.
    pub max_delay_millis: NonZeroU64,
    /// Next schedule sequence.
    pub next_schedule: u64,
    /// Latest schedule or release timestamp; later ones never go below it.
    pub time: u64,
    /// Release key of the last released record. Every held key is above it.
    released: Option<ReleaseKey>,
    /// Every held record before this position was released or carried.
    carried: HeldPosition,
    /// Number of held records.
    records: u64,
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
            released: None,
            carried: HeldPosition {
                operation: 0,
                index: 0,
            },
            records: 0,
            bytes: 0,
        }
    }

    /// Number of held records.
    pub const fn held_records(&self) -> u64 {
        self.records
    }

    /// Encoded bytes of every held record.
    pub const fn held_bytes(&self) -> u64 {
        self.bytes
    }

    /// Release key of the last released record.
    pub const fn released(&self) -> Option<ReleaseKey> {
        self.released
    }

    /// Every held record before this position was released or carried.
    pub const fn carried(&self) -> HeldPosition {
        self.carried
    }

    /// Rebuild from validated snapshot fields.
    pub(super) const fn restore(
        &mut self,
        released: Option<ReleaseKey>,
        carried: HeldPosition,
        records: u64,
        bytes: u64,
    ) {
        self.released = released;
        self.carried = carried;
        self.records = records;
        self.bytes = bytes;
    }
}

/// One validated schedule, release, or carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreparedDelay {
    Schedule(PreparedSchedule),
    Release(PreparedRelease),
    Carry(PreparedCarry),
}

/// Held totals and coordinates of one validated `Schedule`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PreparedSchedule {
    partition: PartitionIncarnation,
    producer: ProducerId,
    span: ProducerResultSpan,
    time: u64,
    bytes: u64,
    /// Oldest spans the producer drops first to fit the span limit.
    evict: usize,
}

/// Coordinates and released totals of one validated `Release`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PreparedRelease {
    partition: PartitionIncarnation,
    through: ReleaseKey,
    time: u64,
    next_offset: Offset,
    records: u64,
    bytes: u64,
}

/// The new floor and time of one validated `Carry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PreparedCarry {
    partition: PartitionIncarnation,
    through: HeldPosition,
    time: u64,
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
        let records =
            u64::try_from(batch.records.len()).map_err(|_| StateError::PositionExhausted)?;
        if delay
            .records
            .checked_add(records)
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
        let bytes = held_records(op_number, value, latest)?
            .iter()
            .map(|(_, record)| u64::from(record.bytes))
            .sum();
        let span = ProducerResultSpan {
            first_sequence: batch.first_sequence,
            first_offset: Offset::new(value.first_schedule),
            records,
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
                bytes,
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

    /// A release takes held records through a key past the last release, at
    /// the next offsets, and its timestamp is at least that key's time. Each
    /// copy checks the records against its held index before this.
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
        let records =
            u64::try_from(batch.records.len()).map_err(|_| StateError::PositionExhausted)?;
        let next_offset = batch
            .first_offset
            .get()
            .checked_add(records)
            .map(Offset::new)
            .ok_or(StateError::PositionExhausted)?;
        let mut bytes = 0_u64;
        for record in &batch.records {
            require_id("message", record.message_id.as_bytes())?;
            bytes += u64::from(record_bytes(record)?);
        }
        // Released records are a nonempty prefix of the held ones, after the
        // last release, with issued schedule sequences.
        let held = records <= delay.records && bytes <= delay.bytes;
        let whole = records == delay.records;
        if records == 0
            || !held
            || whole != (bytes == delay.bytes)
            || value.through.schedule >= delay.next_schedule
            || delay
                .released
                .is_some_and(|released| value.through <= released)
        {
            return Err(StateError::ReleaseMismatch);
        }
        Ok((
            Mutation::Delay(PreparedDelay::Release(PreparedRelease {
                partition: batch.partition,
                through: value.through,
                time,
                next_offset,
                records,
                bytes,
            })),
            Vec::new(),
        ))
    }

    /// A carry moves the carry floor past earlier positions and copies held
    /// records there. Held totals, the last release, and offsets stay. Each
    /// copy checks the records against its held index before this.
    pub(super) fn prepare_carry(
        &self,
        op_number: u64,
        value: &Carry<'_>,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let batch = &value.batch;
        let partition = self.require_partition(batch.partition)?;
        let delay = partition.delay.as_ref().ok_or(StateError::DelayDisabled)?;
        if batch.owner_epoch != partition.owner_epoch {
            return Err(StateError::OwnerEpochMismatch);
        }
        if !is_broker_batch(batch) {
            return Err(StateError::MalformedBody(
                "carry batch must not name a producer",
            ));
        }
        if batch.first_offset != Offset::ZERO {
            return Err(StateError::OffsetMismatch);
        }
        let time = batch.append_timestamp_millis;
        if time < delay.time {
            return Err(StateError::DelayTimeRegressed);
        }
        let records =
            u64::try_from(batch.records.len()).map_err(|_| StateError::PositionExhausted)?;
        let mut bytes = 0_u64;
        for record in &batch.records {
            require_id("message", record.message_id.as_bytes())?;
            require_record_parts(record.parts.len())?;
            bytes += u64::from(record_bytes(record)?);
        }
        // Carried records are held ones from the floor on, before this
        // operation, with issued schedule sequences after the last release.
        if value.keys.len() != batch.records.len()
            || value.from != delay.carried
            || value.through <= value.from
            || value.through.operation >= op_number
            || records > delay.records
            || bytes > delay.bytes
            || value.keys.iter().any(|key| {
                key.schedule >= delay.next_schedule
                    || delay.released.is_some_and(|released| *key <= released)
            })
        {
            return Err(StateError::CarryMismatch);
        }
        Ok((
            Mutation::Delay(PreparedDelay::Carry(PreparedCarry {
                partition: batch.partition,
                through: value.through,
                time,
            })),
            Vec::new(),
        ))
    }

    pub(super) fn install_delay(&mut self, prepared: PreparedDelay) -> Result<(), StateError> {
        match prepared {
            PreparedDelay::Schedule(prepared) => self.install_schedule(prepared),
            PreparedDelay::Release(prepared) => self.install_release(prepared),
            PreparedDelay::Carry(prepared) => {
                let state = self.partition_mut(prepared.partition)?;
                let delay = state.delay.as_mut().ok_or(StateError::DelayDisabled)?;
                delay.carried = prepared.through;
                delay.time = prepared.time;
                Ok(())
            }
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
        delay.records += prepared.span.records;
        delay.bytes += prepared.bytes;
        self.count_retry_spans(1, prepared.evict)
    }

    fn install_release(&mut self, prepared: PreparedRelease) -> Result<(), StateError> {
        let state = self.partition_mut(prepared.partition)?;
        state.next_offset = prepared.next_offset;
        let delay = state.delay.as_mut().ok_or(StateError::DelayDisabled)?;
        delay.time = prepared.time;
        delay.released = Some(prepared.through);
        delay.records -= prepared.records;
        delay.bytes -= prepared.bytes;
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

/// Entries of a `Carry` keyed by their release keys, in batch order.
/// Segment indexes derive their held-record rows with it.
pub fn carry_entries(
    op_number: u64,
    value: &Carry<'_>,
) -> Result<Vec<(ReleaseKey, PendingSchedule)>, StateError> {
    if value.keys.len() != value.batch.records.len() {
        return Err(StateError::CarryMismatch);
    }
    value
        .batch
        .records
        .iter()
        .zip(&value.keys)
        .enumerate()
        .map(|(index, (record, &key))| {
            Ok((
                key,
                PendingSchedule {
                    operation: op_number,
                    index: u32::try_from(index).map_err(|_| StateError::PositionExhausted)?,
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
