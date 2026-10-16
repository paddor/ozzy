//! Visibility-fenced read capture on the owner; detached async file execution.

use super::{JournalError, OwnedJournal};
use crate::replica_journal::{
    PartitionReadCursor, PartitionReadError, PartitionReadLease, PartitionReadLimits,
    ReadPartition, commands::read_fault, records::CapturedRecords,
};
use ozzy_proto::{Offset, PartitionIncarnation};
use ozzy_replication::driver::ValidationTicket;
use std::rc::Rc;
mod delivery;
pub use delivery::{CompletedDelivery, PartitionDelivery};

#[derive(Debug)]
#[expect(clippy::large_enum_variant, reason = "captured read jobs stay inline")]
enum Source {
    Memory(CapturedRecords),
    Stored(ozzy_journal_segment::PartitionRead),
    Empty,
}

/// Exact visible range plus an output lease. Execution borrows no owner and
/// never installs authority. Its completion must return to the capturing owner.
#[derive(Debug)]
pub struct PreparedRead {
    key: Rc<()>,
    cursor: PartitionReadCursor,
    limits: PartitionReadLimits,
    buffer: PartitionReadLease,
    source: Source,
}

/// Unobserved read result. Dropping it releases files/payloads and the lease,
/// but cannot advance a subscription, confirm writes, or change journal state.
#[derive(Debug)]
pub struct CompletedRead {
    key: Rc<()>,
    cursor: PartitionReadCursor,
    result: Result<ReadPartition, JournalError>,
}

impl OwnedJournal {
    /// Open a metadata-only cursor at an explicit offset or current applied end.
    pub fn open_reader(
        &self,
        ticket: ValidationTicket,
        partition: PartitionIncarnation,
        from: Option<Offset>,
    ) -> Result<PartitionReadCursor, JournalError> {
        self.active(ticket)
            .map_err(|_| PartitionReadError::Fenced)?;
        if ticket.applied().op > self.applied.op {
            return Err(PartitionReadError::Unavailable.into());
        }
        let state = self
            .images()?
            .committed()
            .partition(partition)
            .ok_or(PartitionReadError::Partition)?;
        let from = from.unwrap_or(state.next_offset);
        check_position(from, state.retained_from, state.next_offset)?;
        Ok(PartitionReadCursor {
            scope: self.scope,
            generation: ticket.generation(),
            partition,
            owner_epoch: state.owner_epoch,
            next: from,
            observed: self.applied,
            end: state.next_offset,
            retained_from: state.retained_from,
        })
    }

    /// Capture committed records without file access. Accepted RAM-only records
    /// are readable only after the configured group policy has confirmed them.
    pub fn prepare_read(
        &self,
        mut cursor: PartitionReadCursor,
        limits: PartitionReadLimits,
        mut buffer: PartitionReadLease,
    ) -> Result<PreparedRead, JournalError> {
        self.check_reader(cursor)?;
        if buffer.owner_generation() != self.buffer_generation
            || !buffer.is_empty()
            || buffer.body_bytes() != 0
            || !buffer.records().is_empty()
        {
            return Err(PartitionReadError::Fenced.into());
        }
        if limits.max_records == 0
            || limits.max_parts == 0
            || limits.max_payload_bytes > buffer.limits().max_body_bytes
            || limits.max_records > self.limits.operations.max_records
            || limits.max_parts > self.limits.operations.max_parts
        {
            return Err(PartitionReadError::Limits.into());
        }
        buffer.prepare(limits)?;
        let state = self
            .images()?
            .committed()
            .partition(cursor.partition)
            .ok_or(PartitionReadError::Partition)?;
        cursor.end = state.next_offset;
        cursor.retained_from = state.retained_from;
        cursor.observed = self.applied;
        let source = self.read_source(&cursor, limits)?;
        Ok(PreparedRead {
            key: self.read_key.clone(),
            cursor,
            limits,
            buffer,
            source,
        })
    }

    /// Records from `cursor.next` to its end: captured from writeback once
    /// they reach records still held there, otherwise read from segments.
    fn read_source(
        &self,
        cursor: &PartitionReadCursor,
        limits: PartitionReadLimits,
    ) -> Result<Source, JournalError> {
        if cursor.next == cursor.end {
            return Ok(Source::Empty);
        }
        let memory_first = self.writeback.first_offset(cursor.partition);
        if memory_first.is_some_and(|first| cursor.next >= first) {
            return Ok(Source::Memory(
                self.writeback.capture_records(cursor, limits)?,
            ));
        }
        let read = self
            .reader
            .as_ref()
            .ok_or(PartitionReadError::Unavailable)?
            .prepare_read(
                self.journal.readable()?,
                ozzy_journal_segment::PartitionReadRange {
                    partition: cursor.partition,
                    start: cursor.next,
                    end: memory_first.map_or(cursor.end, |first| cursor.end.min(first)),
                    through: self.applied.op.0,
                },
                ozzy_journal::ReadLimits {
                    max_records: limits.max_records,
                    max_bytes: self.limits.operations.max_payload_bytes.max(1),
                },
            )?;
        Ok(Source::Stored(read))
    }

    /// Reject stale/foreign results before delivery. The subscription still owns
    /// its own generation/credit checks. Observed authoritative read faults fence
    /// this journal; normal cursor/limit rejections do not.
    pub fn complete_read(&mut self, done: CompletedRead) -> Result<ReadPartition, JournalError> {
        self.healthy()?;
        if !Rc::ptr_eq(&done.key, &self.read_key) {
            return Err(PartitionReadError::Fenced.into());
        }
        self.check_reader(done.cursor)?;
        self.faulted |= done.result.as_ref().err().is_some_and(read_fault);
        done.result
    }

    pub(super) fn check_reader(&self, cursor: PartitionReadCursor) -> Result<(), JournalError> {
        self.active_scope(cursor.scope, cursor.generation)
            .map_err(|_| PartitionReadError::Fenced)?;
        let state = self
            .images()?
            .committed()
            .partition(cursor.partition)
            .ok_or(PartitionReadError::Partition)?;
        if state.owner_epoch != cursor.owner_epoch || cursor.observed.op > self.applied.op {
            return Err(PartitionReadError::Fenced.into());
        }
        check_position(cursor.next, state.retained_from, state.next_offset)?;
        Ok(())
    }
}

impl PreparedRead {
    /// Run independently of the application owner. The bounded output copies
    /// opaque payloads, never a whole physical group or unrelated record.
    pub async fn read(self) -> CompletedRead {
        let Self {
            key,
            cursor,
            limits,
            buffer,
            source,
        } = self;
        let mut copy = RecordCopy {
            cursor,
            limits,
            buffer,
            parts: 0,
            rejected: None,
        };
        let result = match source {
            Source::Empty => Ok(()),
            Source::Memory(records) => {
                records.visit_spans(|span| copy.span(span));
                Ok(())
            }
            Source::Stored(records) => records
                .visit(|span| copy.span(span))
                .await
                .map(|_| ())
                .map_err(JournalError::from),
        }
        .and_then(|()| copy.finish(cursor.next));
        CompletedRead {
            key,
            cursor,
            result,
        }
    }
}

/// Whole records copied into the output lease within the read limits. The
/// cursor advances past each copied record.
struct RecordCopy {
    cursor: PartitionReadCursor,
    limits: PartitionReadLimits,
    buffer: PartitionReadLease,
    parts: usize,
    rejected: Option<JournalError>,
}

impl RecordCopy {
    /// Copy the leading records of `span` that fit. Returns how many.
    fn span(&mut self, span: &ozzy_journal_segment::RecordSpan<'_>) -> usize {
        if let Err(error) = self.buffer.reserve_payload(self.fitting_bytes(span)) {
            self.rejected = Some(error);
            return 0;
        }
        let limits = self.limits;
        let mut taken = 0;
        for record in span.records() {
            let bytes = record.payload_bytes();
            let record_parts = record.part_count();
            if bytes > limits.max_payload_bytes - self.buffer.body_bytes()
                || record_parts > limits.max_parts - self.parts
            {
                if self.buffer.records().is_empty() {
                    self.rejected = Some(
                        PartitionReadError::RecordTooLarge {
                            bytes,
                            parts: record_parts,
                        }
                        .into(),
                    );
                }
                break;
            }
            self.parts += record_parts;
            let pushed = self.buffer.push_view(&record).and_then(|()| {
                self.cursor
                    .next
                    .checked_next()
                    .ok_or_else(|| PartitionReadError::Limits.into())
            });
            match pushed {
                Ok(next) => self.cursor.next = next,
                Err(error) => {
                    self.rejected = Some(error);
                    break;
                }
            }
            taken += 1;
        }
        taken
    }

    /// Payload bytes of the records of this stored group that the copy will
    /// accept, so they are reserved once with the same bounds.
    fn fitting_bytes(&self, span: &ozzy_journal_segment::RecordSpan<'_>) -> usize {
        let limits = self.limits;
        let mut group_bytes = 0;
        let mut group_parts = 0;
        for record in span.records() {
            let bytes = record.payload_bytes();
            let record_parts = record.part_count();
            if bytes > limits.max_payload_bytes - self.buffer.body_bytes() - group_bytes
                || record_parts > limits.max_parts - self.parts - group_parts
            {
                break;
            }
            group_bytes += bytes;
            group_parts += record_parts;
        }
        group_bytes
    }

    /// The delivery starting at `first`, unless a record was rejected.
    fn finish(self, first: Offset) -> Result<ReadPartition, JournalError> {
        if let Some(error) = self.rejected {
            return Err(error);
        }
        Ok(ReadPartition {
            cursor: self.cursor,
            first,
            buffer: self.buffer,
        })
    }
}

fn check_position(from: Offset, earliest: Offset, end: Offset) -> Result<(), PartitionReadError> {
    if from < earliest {
        return Err(PartitionReadError::RetentionGap { earliest });
    }
    if from > end {
        return Err(PartitionReadError::Ahead { committed_end: end });
    }
    Ok(())
}
