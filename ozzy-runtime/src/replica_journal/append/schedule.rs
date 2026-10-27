//! A scheduled producer batch keeps its APPEND body, so retry checks and
//! prefix trims work unchanged. Once the leader assigns its schedule
//! sequences, the entry becomes a `Schedule`: no offsets, the acceptance
//! time, then the schedule trailer.

use super::{AppendBuffer, JournalError};
use ozzy_core::state::{ProducerResultSource, ProducerResultSpan};
use ozzy_journal::operation::OperationKind;
use ozzy_proto::{Offset, ProducerSequence};

/// Due times of the one scheduled batch in a producer buffer.
#[derive(Debug)]
pub(in crate::replica_journal) struct Scheduled {
    /// One absolute Unix due time in milliseconds per record.
    pub(in crate::replica_journal) due_millis: Vec<u64>,
    /// First schedule sequence, once the entry is a `Schedule`.
    first_schedule: Option<u64>,
}

/// Byte offset of the first sequence in a one-batch APPEND body: batch
/// count, partition, owner epoch, producer, producer epoch.
const FIRST_SEQUENCE: usize = 4 + 16 + 8 + 16 + 8;

fn trailer_bytes(records: usize) -> Result<usize, JournalError> {
    records
        .checked_add(1)
        .and_then(|fields| fields.checked_mul(8))
        .ok_or(JournalError::AppendCapacity)
}

impl AppendBuffer {
    /// Mark the one producer batch as scheduled. The trailer is reserved now,
    /// so a full arena refuses the request before validation.
    pub(in crate::replica_journal) fn schedule(
        &mut self,
        due_millis: impl ExactSizeIterator<Item = u64>,
    ) -> Result<(), JournalError> {
        if !self.is_producer()
            || self.schedule.is_some()
            || self.entries.len() != 1
            || self.entries[0].envelope.kind != OperationKind::Append
        {
            return Err(JournalError::AppendMismatch);
        }
        let trailer = trailer_bytes(due_millis.len())?;
        if trailer > self.room() {
            return Err(JournalError::AppendCapacity);
        }
        self.bodies.reserve(trailer)?;
        self.schedule = Some(Box::new(Scheduled {
            due_millis: due_millis.collect(),
            first_schedule: None,
        }));
        Ok(())
    }

    /// The due times of a scheduled batch.
    pub(in crate::replica_journal) fn scheduled(&self) -> Option<&Scheduled> {
        self.schedule.as_deref()
    }

    /// Drop the due times of a verified retry prefix along with its records.
    pub(in crate::replica_journal) fn trim_schedule(&mut self, count: usize) {
        if let Some(scheduled) = self.schedule.as_mut() {
            scheduled
                .due_millis
                .drain(..count.min(scheduled.due_millis.len()));
        }
    }

    /// Turn the assigned APPEND entry into a `Schedule` holding no offsets,
    /// the acceptance time, and the schedule trailer.
    pub(in crate::replica_journal) fn convert_schedule(
        &mut self,
        first_schedule: u64,
        timestamp: u64,
    ) -> Result<(), JournalError> {
        let Some(scheduled) = self.schedule.as_deref() else {
            return Err(JournalError::AppendMismatch);
        };
        if scheduled.first_schedule.is_some()
            || self.entries.len() != 1
            || self.entries[0].body.end != self.bodies.len()
        {
            return Err(JournalError::AppendMismatch);
        }
        let mut trailer = Vec::with_capacity(trailer_bytes(scheduled.due_millis.len())?);
        trailer.extend_from_slice(&first_schedule.to_be_bytes());
        for due in &scheduled.due_millis {
            trailer.extend_from_slice(&due.to_be_bytes());
        }
        let mut position = [0; 16];
        position[8..].copy_from_slice(&timestamp.to_be_bytes());
        self.replace_append_position(0, position)?;
        self.bodies.extend_from_slice(&trailer)?;
        let entry = &mut self.entries[0];
        entry.body.end = self.bodies.len();
        entry.envelope.kind = OperationKind::Schedule;
        entry.digest = None;
        entry.proof = None;
        self.validated = false;
        if let Some(scheduled) = self.schedule.as_mut() {
            scheduled.first_schedule = Some(first_schedule);
        }
        Ok(())
    }

    /// Turn a refused `Schedule` back into its APPEND entry. The caller then
    /// restores the original position.
    pub(in crate::replica_journal) fn revert_schedule(&mut self) -> Result<(), JournalError> {
        let Some(scheduled) = self.schedule.as_mut() else {
            return Ok(());
        };
        if scheduled.first_schedule.take().is_none() {
            return Ok(());
        }
        let end = self.entries[0]
            .body
            .end
            .checked_sub(trailer_bytes(scheduled.due_millis.len())?)
            .ok_or(JournalError::AppendMismatch)?;
        self.bodies.truncate(end)?;
        let entry = &mut self.entries[0];
        entry.body.end = end;
        entry.envelope.kind = OperationKind::Append;
        self.validated = false;
        Ok(())
    }

    /// Reply coordinates of a converted `Schedule`: its sequences map to
    /// schedule sequences. The operation is not known before admission;
    /// replies read only the disposition and first schedule sequence.
    pub(in crate::replica_journal) fn scheduled_result(&self) -> Option<ProducerResultSpan> {
        let scheduled = self.schedule.as_deref()?;
        let first_schedule = scheduled.first_schedule?;
        let entry = self.entries.first()?;
        let start = entry.body.start + FIRST_SEQUENCE;
        let first_sequence = self.bodies.get(start..start + 8)?.try_into().ok()?;
        Some(ProducerResultSpan {
            first_sequence: ProducerSequence::new(u64::from_be_bytes(first_sequence)),
            first_offset: Offset::new(first_schedule),
            records: scheduled.due_millis.len() as u64,
            source: ProducerResultSource::Scheduled { operation: 0 },
        })
    }
}
