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
    Stored(ozzy_journal_segment::AsyncPartitionRead),
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
        let memory_first = self.writeback.first_offset(cursor.partition);
        let source = if cursor.next == cursor.end {
            Source::Empty
        } else if memory_first.is_some_and(|first| cursor.next >= first) {
            Source::Memory(self.writeback.capture_records(
                cursor.partition,
                cursor.next,
                cursor.end,
                self.applied,
                limits,
            )?)
        } else {
            Source::Stored(
                self.reader
                    .as_ref()
                    .ok_or(PartitionReadError::Unavailable)?
                    .prepare_read(
                        self.journal.readable()?,
                        cursor.partition,
                        cursor.next,
                        memory_first.map_or(cursor.end, |first| cursor.end.min(first)),
                        self.applied.op.0,
                        ozzy_journal::ReadLimits {
                            max_records: limits.max_records,
                            max_bytes: self.limits.operations.max_payload_bytes.max(1),
                        },
                    )?,
            )
        };
        Ok(PreparedRead {
            key: self.read_key.clone(),
            cursor,
            limits,
            buffer,
            source,
        })
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
            mut cursor,
            limits,
            mut buffer,
            source,
        } = self;
        let original = cursor;
        let mut parts = 0usize;
        let mut rejected = None;
        let visit = |span: &ozzy_journal_segment::RecordSpan<'_>| {
            // Reserve the accepted records of this stored group once, using
            // the same bounds as the copy below.
            let mut group_bytes = 0;
            let mut group_parts = 0;
            for record in span.records() {
                let bytes = record.payload_bytes();
                let record_parts = record.part_count();
                if bytes > limits.max_payload_bytes - buffer.body_bytes() - group_bytes
                    || record_parts > limits.max_parts - parts - group_parts
                {
                    break;
                }
                group_bytes += bytes;
                group_parts += record_parts;
            }
            if let Err(error) = buffer.reserve_payload(group_bytes) {
                rejected = Some(error);
                return 0;
            }
            let mut taken = 0;
            for record in span.records() {
                let bytes = record.payload_bytes();
                let record_parts = record.part_count();
                if bytes > limits.max_payload_bytes - buffer.body_bytes()
                    || record_parts > limits.max_parts - parts
                {
                    if buffer.records().is_empty() {
                        rejected = Some(
                            PartitionReadError::RecordTooLarge {
                                bytes,
                                parts: record_parts,
                            }
                            .into(),
                        );
                    }
                    break;
                }
                parts += record_parts;
                buffer.push_view(&record);
                cursor.next = cursor.next.checked_next().expect("validated read end");
                taken += 1;
            }
            taken
        };
        let result = match source {
            Source::Empty => Ok(()),
            Source::Memory(records) => {
                records.visit_spans(visit);
                Ok(())
            }
            Source::Stored(records) => records
                .visit(visit)
                .await
                .map(|_| ())
                .map_err(JournalError::from),
        }
        .and_then(|()| {
            if let Some(error) = rejected {
                Err(error)
            } else {
                Ok(ReadPartition {
                    cursor,
                    first: original.next,
                    buffer,
                })
            }
        });
        CompletedRead {
            key,
            cursor: original,
            result,
        }
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
