//! A fresh scheduled batch is checked against its partition's delay state,
//! then becomes a `Schedule` at the next schedule sequences.

use super::super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::{AppendAdmissionError, producer};

impl OwnedJournal {
    /// Refuse a scheduled batch the partition cannot hold, then convert it.
    /// Its acceptance time never goes below the partition's latest schedule
    /// or release, so new records sort after every released one.
    pub(super) fn schedule_proposal(
        &self,
        buffer: &mut AppendBuffer,
        now_millis: u64,
    ) -> Result<(), JournalError> {
        let batch = producer::summary(buffer, 0, self.limits.operations)?;
        let delay = self
            .images()?
            .speculative()
            .partition(batch.partition)
            .ok_or(AppendAdmissionError::UnknownPartition)?
            .delay
            .as_ref()
            .ok_or(AppendAdmissionError::DelayDisabled)?;
        let due = &buffer
            .scheduled()
            .ok_or(JournalError::AppendMismatch)?
            .due_millis;
        if self.delay.is_some_and(|limits| {
            delay
                .pending_len()
                .checked_add(due.len())
                .is_none_or(|total| total > limits.max_entries)
        }) {
            return Err(AppendAdmissionError::ScheduleCapacity.into());
        }
        let time = now_millis.max(delay.time);
        let latest = time.saturating_add(delay.max_delay_millis.get());
        if due.iter().any(|&due| due > latest) {
            return Err(AppendAdmissionError::DueOutOfRange.into());
        }
        buffer.convert_schedule(delay.next_schedule, time)
    }
}
