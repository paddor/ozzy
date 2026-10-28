//! Bounded logical admission, physical grouping, and owner-installed completions.

mod roll;
mod sync;
mod write;
pub use roll::{CompletedRoll, PreparedRoll};
pub use sync::{CompletedSync, PreparedSync};
pub use write::{CompletedWrite, PreparedWrite, WriteStep, WrittenRecords};

use super::{JournalError, JournalOwner, OwnedJournal, PipelineLimits};
use crate::{
    completion,
    replica_journal::{
        AdmittedAppend, JournalCompletion, ValidatedAppend, append::RetainedAppend, canonical,
    },
};
use ozzy_journal_segment::{PreencodedJournalGroup, PreparedOperationRecords};
use ozzy_replication::{SyncTicket, WriteTicket, driver::ValidationTicket};
use std::{cell::Cell, collections::VecDeque, ops::Range, rc::Rc, sync::Arc};

#[derive(Debug)]
pub(super) struct Writeback {
    pub(super) limits: PipelineLimits,
    pub(super) group_bytes: usize,
    queue: VecDeque<Entry>,
    operations: usize,
    bytes: usize,
    encoded: Option<Encoded>,
    abandoned: Rc<Cell<bool>>,
}

#[derive(Debug)]
struct Entry {
    ticket: WriteTicket,
    append: Arc<RetainedAppend>,
    records: Vec<PreparedOperationRecords>,
    prepared: usize,
    written: usize,
    done: completion::Sender<Result<WriteTicket, JournalError>>,
}

impl Writeback {
    /// Accepted immutable bytes remain authoritative while physical work runs.
    pub(super) fn operation(
        &self,
        number: u64,
    ) -> Option<(
        ozzy_journal::operation::CanonicalOperation<'_>,
        ozzy_replication::Prefix,
    )> {
        self.queue
            .iter()
            .flat_map(|entry| entry.append.operations().zip(&entry.append.prepared))
            .find(|(operation, _)| operation.op_number == number)
            .map(|(operation, prepared)| (operation, prepared.prefix()))
    }

    /// Kind and shared body bytes of an operation still in writeback.
    pub(super) fn operation_bytes(
        &self,
        number: u64,
    ) -> Option<(ozzy_journal::operation::OperationKind, bytes::Bytes)> {
        self.queue.iter().find_map(|entry| {
            entry
                .append
                .operations()
                .find(|operation| operation.op_number == number)
                .map(|operation| (operation.kind, entry.append.payload(operation.body)))
        })
    }

    /// Kinds and bodies of the `Schedule` and `Carry` operations numbered in
    /// `range` still in writeback.
    pub(super) fn held_sources(
        &self,
        range: std::ops::Range<u64>,
    ) -> Vec<(u64, ozzy_journal::operation::OperationKind, bytes::Bytes)> {
        use ozzy_journal::operation::OperationKind::{Carry, Schedule};
        self.queue
            .iter()
            .flat_map(|entry| {
                entry
                    .append
                    .operations()
                    .filter(|operation| {
                        range.contains(&operation.op_number)
                            && matches!(operation.kind, Schedule | Carry)
                    })
                    .map(|operation| {
                        let body = entry.append.payload(operation.body);
                        (operation.op_number, operation.kind, body)
                    })
            })
            .collect()
    }

    pub(super) fn seek_records(&self) -> Vec<PreparedOperationRecords> {
        self.queue
            .iter()
            .flat_map(|entry| entry.records.iter().cloned())
            .collect()
    }

    /// Accepted control bodies remain here until physical installation. A RAM
    /// confirmation can precede the index snapshot that will later cover them.
    pub(super) fn producer_open(
        &self,
        id: ozzy_proto::OperationId,
        number: u64,
        limits: ozzy_journal::operation::OperationLimits,
    ) -> Result<Option<ozzy_journal::operation::OpenProducer>, JournalError> {
        use ozzy_journal::operation::{OperationBody, decode_operation_body};
        for entry in &self.queue {
            if let Some(operation) = entry
                .append
                .operations()
                .find(|operation| operation.op_number == number)
            {
                let OperationBody::OpenProducer(open) =
                    decode_operation_body(operation.kind, operation.body, limits)?
                else {
                    return Err(JournalError::AppendMismatch);
                };
                if open.operation_id != id {
                    return Err(JournalError::AppendMismatch);
                }
                return Ok(Some(open));
            }
        }
        Ok(None)
    }
}

#[derive(Debug)]
struct Part {
    ticket: WriteTicket,
    append: Arc<RetainedAppend>,
    range: Range<usize>,
}

#[derive(Debug)]
struct Encoded {
    parts: Vec<Part>,
    group: PreencodedJournalGroup,
    backing_bytes: usize,
}

/// Dropped work/results fence the owner, without waiting for physical I/O.
#[derive(Debug)]
struct Observation {
    abandoned: Rc<Cell<bool>>,
    armed: bool,
}
impl Drop for Observation {
    fn drop(&mut self) {
        if self.armed {
            self.abandoned.set(true);
        }
    }
}

impl Writeback {
    pub(super) fn first_offset(
        &self,
        partition: ozzy_proto::PartitionIncarnation,
    ) -> Option<ozzy_proto::Offset> {
        self.queue
            .iter()
            .flat_map(|entry| &entry.records)
            .find_map(|records| records.first_offset(partition))
    }

    /// Queued records from `cursor.next` to its end, visible through the
    /// applied prefix the cursor observed.
    pub(super) fn capture_records(
        &self,
        cursor: &crate::replica_journal::PartitionReadCursor,
        limits: crate::replica_journal::PartitionReadLimits,
    ) -> Result<crate::replica_journal::records::CapturedRecords, JournalError> {
        crate::replica_journal::records::CapturedRecords::capture(
            self.queue
                .iter()
                .flat_map(|entry| entry.records.iter().zip(&entry.append.prepared))
                .filter(|(_, operation)| operation.prefix().op <= cursor.observed.op)
                .map(|(records, _)| records),
            cursor.partition,
            cursor.next,
            cursor.end,
            limits,
        )
    }

    pub(super) fn new(limits: PipelineLimits, group_bytes: usize) -> Self {
        Self {
            limits,
            group_bytes,
            queue: VecDeque::new(),
            operations: 0,
            bytes: 0,
            encoded: None,
            abandoned: Rc::new(Cell::new(false)),
        }
    }
    pub(super) fn is_faulted(&self) -> bool {
        self.abandoned.get()
    }
    /// Retained bytes admitted but not yet written.
    pub(super) const fn queued_bytes(&self) -> usize {
        self.bytes
    }
    pub(super) fn require_idle(&self) -> Result<(), JournalError> {
        if self.is_faulted() || !self.queue.is_empty() || self.encoded.is_some() {
            return Err(JournalError::CompletionMismatch);
        }
        Ok(())
    }
    fn observation(&self) -> Observation {
        Observation {
            abandoned: self.abandoned.clone(),
            armed: true,
        }
    }
    fn room(&self, count: usize, bytes: usize) -> bool {
        count <= self.limits.max_operations.saturating_sub(self.operations)
            && bytes <= self.limits.max_body_bytes.saturating_sub(self.bytes)
    }

    /// Record selectors of the written `parts`, each the next unwritten
    /// range of its queued entry.
    fn written_records(
        &self,
        parts: &[Part],
    ) -> Result<Vec<PreparedOperationRecords>, JournalError> {
        let mut records = Vec::new();
        for part in parts {
            let entry = self
                .queue
                .iter()
                .find(|entry| entry.ticket == part.ticket)
                .ok_or(JournalError::CompletionMismatch)?;
            if entry.written != part.range.start
                || entry.prepared < part.range.end
                || !Arc::ptr_eq(&entry.append, &part.append)
            {
                return Err(JournalError::CompletionMismatch);
            }
            records.extend_from_slice(&entry.records[part.range.clone()]);
        }
        Ok(records)
    }

    /// Mark installed `parts` written and complete each fully written entry.
    fn release_written(&mut self, parts: Vec<Part>) -> Result<(), JournalError> {
        for part in parts {
            self.queue
                .iter_mut()
                .find(|entry| entry.ticket == part.ticket)
                .ok_or(JournalError::CompletionMismatch)?
                .written = part.range.end;
        }
        while let Some(entry) = self
            .queue
            .pop_front_if(|entry| entry.written == entry.append.prepared.len())
        {
            self.operations -= entry.append.prepared.len();
            self.bytes -= entry.append.retained_bytes();
            let _ = entry.done.send(Ok(entry.ticket));
        }
        Ok(())
    }

    pub(super) fn record(
        &self,
        partition: ozzy_proto::PartitionIncarnation,
        offset: ozzy_proto::Offset,
        decoded: &mut ozzy_journal_segment::DecodedBatches,
    ) -> Option<(
        ozzy_journal_segment::IndexedRecord,
        ozzy_replication::Prefix,
    )> {
        let (records, operation) = self
            .queue
            .iter()
            .flat_map(|entry| entry.records.iter().zip(&entry.append.prepared))
            .find(|(records, _)| records.contains(partition, offset))?;
        records.reuse_decoded(decoded);
        Some((
            records.record(partition, offset, decoded)?.materialize(),
            operation.prefix(),
        ))
    }
}

impl OwnedJournal {
    pub(crate) fn writes_settled(&self) -> bool {
        self.writeback.require_idle().is_ok()
    }

    /// Check exact retained-body capacity before the driver admits a validated
    /// group. Freezing preserves immutable backing through later arena reuse.
    pub fn can_admit(&self, validated: &mut ValidatedAppend) -> bool {
        let _ = validated.buffer.shared_bodies();
        !self.is_faulted()
            && self
                .writeback
                .room(validated.buffer.len(), validated.buffer.retained_bytes())
    }

    /// Install canonical acceptance without waiting for disk. Call only after
    /// `can_admit` succeeds and the driver issues its exact write ticket. The
    /// returned physical completion must be observed separately; dropping that
    /// observer never cancels admitted storage or frees the retained data early.
    pub fn admit_append(
        &mut self,
        ticket: WriteTicket,
        validated: ValidatedAppend,
    ) -> Result<AdmittedAppend, JournalError> {
        self.healthy()?;
        let result = self.admit_inner(ticket, validated);
        self.faulted |= result.is_err();
        result
    }

    fn admit_inner(
        &mut self,
        ticket: WriteTicket,
        mut validated: ValidatedAppend,
    ) -> Result<AdmittedAppend, JournalError> {
        if !self.can_admit(&mut validated) {
            return Err(JournalError::AppendCapacity);
        }
        let validation = validated.validation;
        self.active(validation)?;
        let images = self.images()?;
        if validation.accepted() != self.accepted()
            || validation.applied().op > self.applied.op
            || images.speculative().revision() != self.accepted().op.0
            || images.committed().revision() != self.applied.op.0
            || ticket.generation() != validation.generation()
            || Some(ticket.first().0) != validation.accepted().op.0.checked_add(1)
            || validated
                .buffer
                .prepared
                .last()
                .is_none_or(|last| last.prefix().op != ticket.through())
        {
            return Err(JournalError::AppendMismatch);
        }
        let ValidatedAppend {
            mut buffer,
            plan,
            mut records,
            dedup,
            ..
        } = validated;
        let append = Arc::new(buffer.retain());
        if records.is_empty() {
            records = canonical::retained_records(&append, self.limits.operations)?;
        }
        self.faulted = true;
        self.images
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?
            .install_prepared_group(plan)?;
        self.dedup.install(dedup)?;
        self.admit_held(&append)?;
        self.pending
            .extend(append.prepared.iter().map(|op| op.prefix()));
        self.writeback.operations += append.prepared.len();
        self.writeback.bytes += append.retained_bytes();
        let (done, receiver) = completion::channel();
        self.writeback.queue.push_back(Entry {
            ticket,
            append,
            records,
            prepared: 0,
            written: 0,
            done,
        });
        self.faulted = false;
        Ok(AdmittedAppend {
            ticket,
            buffer,
            persisted: JournalCompletion { receiver },
        })
    }

    /// Synchronize only the installed prefix and publish its restart evidence.
    /// Later reservations are never included implicitly.
    pub async fn sync(&mut self, ticket: SyncTicket) -> Result<SyncTicket, JournalError> {
        self.healthy()?;
        let journal = self.journal.readable()?;
        let written = journal.writer().written_position();
        if ticket.generation() != written.generation()
            || ticket.through().0 > written.next_chain().next_op_number() - 1
            || matches!(&self.journal, JournalOwner::Writing(pipeline) if pipeline.sync_pending())
        {
            return Err(JournalError::CompletionMismatch);
        }
        self.faulted = true;
        match &mut self.journal {
            JournalOwner::Ready(journal) => {
                journal.sync_through(written).await?;
                journal.publish_durable_progress().await?;
            }
            JournalOwner::Writing(pipeline) => {
                pipeline.sync_installed().await?;
                pipeline.publish_durable_progress().await?;
            }
            _ => return Err(JournalError::CompletionMismatch),
        }
        self.faulted = false;
        Ok(ticket)
    }

    /// Apply only the exact driver-confirmed, synchronized prefix through
    /// canonical admission.
    pub fn apply(&mut self, ticket: ValidationTicket) -> Result<ValidationTicket, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        let through = ticket.committed();
        if through.op.0
            > self
                .journal
                .readable()?
                .writer()
                .durable_position()
                .next_chain()
                .next_op_number()
                - 1
        {
            return Err(JournalError::CompletionMismatch);
        }
        self.faulted = true;
        self.images
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?
            .commit_through(through.op.0)?;
        self.dedup.commit_through(through.op.0)?;
        while self
            .pending
            .front()
            .is_some_and(|prefix| prefix.op <= through.op)
        {
            self.pending.pop_front();
        }
        self.applied = through;
        self.faulted = false;
        Ok(ticket)
    }
}
