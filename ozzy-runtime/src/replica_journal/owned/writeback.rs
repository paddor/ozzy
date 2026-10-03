//! Bounded logical admission, physical grouping, and owner-installed completions.

mod sync;
pub use sync::{CompletedSync, PreparedSync};

use super::{JournalError, JournalOwner, OwnedJournal, PipelineLimits};
use crate::{
    completion,
    replica_journal::{
        AdmittedAppend, JournalCompletion, ValidatedAppend, append::RetainedAppend, canonical,
    },
};
use ozzy_journal_segment::{
    AsyncCompletedJournalRoll, AsyncCompletedJournalWrite, AsyncPreparedJournalRoll,
    AsyncPreparedJournalWrite, ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, OperationLocation,
    PreencodedJournalGroup, PreparedOperationRecords, SEGMENT_HEADER_BYTES, SharedJournalOperation,
    WRITE_GROUP_ALIGNMENT,
};
use ozzy_replication::{JournalGeneration, SyncTicket, WriteTicket, driver::ValidationTicket};
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

/// One physical write, possibly spanning several admitted SDK/core requests.
/// Execute outside the journal borrow, then install its result in order.
#[derive(Debug)]
#[must_use = "execute and install, or fence and reopen the owner"]
pub struct PreparedWrite {
    parts: Vec<Part>,
    work: AsyncPreparedJournalWrite,
    observed: Observation,
}

/// Physical completion only. No durable-prefix evidence or vote is implied.
#[derive(Debug)]
#[must_use = "install on the originating journal in reservation order"]
pub struct CompletedWrite {
    parts: Vec<Part>,
    completed: AsyncCompletedJournalWrite,
    observed: Observation,
}

/// Progress decision after one bounded physical-group preparation turn.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "keep bounded write jobs inline without per-group boxes"
)]
pub enum WriteStep {
    /// No accepted data remains to write.
    Idle,
    /// Existing writes or a roll must finish before more preparation.
    Waiting,
    /// Drain existing writes, then roll; encoded bodies remain owned here.
    RollRequired,
    /// Execute this independent backend future and install its result.
    Write(PreparedWrite),
}

/// Exact record selectors for an installed physical group. Keep these for
/// incremental reader-index installation; payload backing remains shared.
#[derive(Debug)]
pub struct WrittenRecords {
    /// Writer incarnation whose exact completion was installed.
    pub generation: JournalGeneration,
    /// Selected segment containing these physical locations.
    pub segment_id: u64,
    /// One exact file placement per canonical operation, in operation order.
    pub locations: Vec<OperationLocation>,
    /// Shared, already-validated selectors in the same operation order. The
    /// reader/index owner must charge retained backing until final release.
    pub records: Vec<PreparedOperationRecords>,
}

/// Detached segment publication. Other actors and this owner's logical
/// admission may continue, but physical writes await exact installation.
#[derive(Debug)]
#[must_use = "publish and install, or fence and reopen the owner"]
pub struct PreparedRoll {
    work: AsyncPreparedJournalRoll,
    observed: Observation,
}

/// Publication result awaiting exact-generation installation by its owner.
#[derive(Debug)]
#[must_use = "install on the originating journal"]
pub struct CompletedRoll {
    completed: AsyncCompletedJournalRoll,
    observed: Observation,
}

impl PreparedWrite {
    /// Execute physical I/O without borrowing or changing journal authority.
    pub async fn write(self) -> CompletedWrite {
        let Self {
            parts,
            work,
            observed,
        } = self;
        CompletedWrite {
            parts,
            completed: work.write().await,
            observed,
        }
    }
}
impl PreparedRoll {
    /// Publish selected segment metadata through the shared async backend.
    pub async fn publish(self) -> CompletedRoll {
        let Self { work, observed } = self;
        CompletedRoll {
            completed: work.publish().await,
            observed,
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

    pub(super) fn capture_records(
        &self,
        partition: ozzy_proto::PartitionIncarnation,
        first: ozzy_proto::Offset,
        end: ozzy_proto::Offset,
        through: ozzy_replication::Prefix,
        limits: crate::replica_journal::PartitionReadLimits,
    ) -> Result<crate::replica_journal::records::CapturedRecords, JournalError> {
        crate::replica_journal::records::CapturedRecords::capture(
            self.queue
                .iter()
                .flat_map(|entry| entry.records.iter().zip(&entry.append.prepared))
                .filter(|(_, operation)| operation.prefix().op <= through.op)
                .map(|(records, _)| records),
            partition,
            first,
            end,
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

    /// Select and encode one bounded raw physical group. SDK-packed payloads
    /// remain byte-exact and share their existing backing with transport.
    pub fn prepare_write(&mut self) -> Result<WriteStep, JournalError> {
        self.healthy()?;
        if matches!(self.journal, JournalOwner::Rolling(_)) {
            return Ok(WriteStep::Waiting);
        }
        if self.writeback.queue.is_empty() {
            return Ok(WriteStep::Idle);
        }
        let result = self.prepare_write_inner();
        self.faulted |= result.is_err();
        result
    }

    fn prepare_write_inner(&mut self) -> Result<WriteStep, JournalError> {
        if self.writeback.encoded.is_none() {
            let parts = self.select_write()?;
            if parts.is_empty() {
                return Ok(WriteStep::Waiting);
            }
            let pipeline = self
                .journal
                .pipeline(self.writeback.limits.max_operations)?;
            let mut encoding = pipeline.begin_group_encoding()?;
            if parts.iter().all(|part| part.append.validated()) {
                encoding = encoding.with_validated_bodies(self.limits.operations);
            } else if parts.iter().all(|part| {
                part.append
                    .validated_payloads_prepared(part.range.clone())
                    .is_some()
            }) {
                encoding = encoding.with_validated_payloads();
            }
            let operations = parts
                .iter()
                .flat_map(|part| {
                    part.append
                        .verified_operations()
                        .skip(part.range.start)
                        .take(part.range.len())
                        .map(|(operation, body_digest)| SharedJournalOperation {
                            header: operation.header(),
                            body: part.append.payload(operation.body),
                            body_digest,
                        })
                })
                .collect();
            let backing_bytes = parts
                .iter()
                .try_fold(0_usize, |sum, part| {
                    sum.checked_add(part.append.retained_bytes())
                })
                .ok_or(JournalError::AppendCapacity)?;
            self.writeback.encoded = Some(Encoded {
                parts,
                group: encoding.encode_shared_raw(operations)?,
                backing_bytes,
            });
        }
        let encoded = self.writeback.encoded.as_ref().expect("encoded group");
        let pipeline = self
            .journal
            .pipeline(self.writeback.limits.max_operations)?;
        if let Err(error) = pipeline.check(&encoded.group) {
            if crate::replica_journal::canonical::segment_full(&error) {
                return Ok(WriteStep::RollRequired);
            }
            return Err(error.into());
        }
        let Encoded {
            parts,
            group,
            backing_bytes,
        } = self.writeback.encoded.take().expect("checked group");
        let work = pipeline.prepare(group, backing_bytes)?;
        for part in &parts {
            let entry = self
                .writeback
                .queue
                .iter_mut()
                .find(|entry| entry.ticket == part.ticket)
                .ok_or(JournalError::CompletionMismatch)?;
            if entry.prepared != part.range.start || !Arc::ptr_eq(&entry.append, &part.append) {
                return Err(JournalError::CompletionMismatch);
            }
            entry.prepared = part.range.end;
        }
        Ok(WriteStep::Write(PreparedWrite {
            parts,
            work,
            observed: self.writeback.observation(),
        }))
    }

    fn select_write(&self) -> Result<Vec<Part>, JournalError> {
        let capacity =
            self.journal.readable()?.writer().header().capacity() as usize - SEGMENT_HEADER_BYTES;
        let mut selected = Vec::new();
        let mut bytes = 0usize;
        let mut physical = GROUP_SEAL_BYTES;
        let mut count = 0usize;
        let max_count = self
            .limits
            .decode
            .max_entries
            .min(crate::replica_journal::MAX_APPEND_OPERATIONS);
        for entry in &self.writeback.queue {
            let first = entry.prepared;
            let mut last = first;
            for operation in entry.append.operations().skip(first) {
                let added = (operation.body.len() + ENTRY_HEADER_BYTES + 7) & !7;
                let next_physical = physical.saturating_add(added);
                let aligned = next_physical.saturating_add(WRITE_GROUP_ALIGNMENT - 1)
                    & !(WRITE_GROUP_ALIGNMENT - 1);
                if count == max_count
                    || aligned > capacity
                    || operation.body.len()
                        > self
                            .limits
                            .decode
                            .max_group_decoded_body_bytes
                            .saturating_sub(bytes)
                    || (count != 0
                        && operation.body.len() > self.writeback.group_bytes.saturating_sub(bytes))
                {
                    break;
                }
                bytes += operation.body.len();
                physical = next_physical;
                count += 1;
                last += 1;
            }
            if last != first {
                selected.push(Part {
                    ticket: entry.ticket,
                    append: entry.append.clone(),
                    range: first..last,
                });
            }
            if last != entry.append.prepared.len() {
                break;
            }
        }
        Ok(selected)
    }

    /// Install only the next result from this owner. Exact file placements and
    /// prepared record selectors are returned for incremental reader indexing.
    /// Observer cancellation does not change which writes must be installed.
    pub fn complete_write(&mut self, done: CompletedWrite) -> Result<WrittenRecords, JournalError> {
        self.healthy()?;
        self.faulted = true;
        let CompletedWrite {
            parts,
            completed,
            mut observed,
        } = done;
        if !Rc::ptr_eq(&observed.abandoned, &self.writeback.abandoned) {
            return Err(JournalError::CompletionMismatch);
        }
        let mut records = Vec::new();
        for part in &parts {
            let entry = self
                .writeback
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
        let pipeline = self
            .journal
            .pipeline(self.writeback.limits.max_operations)?;
        let locations = pipeline.complete(completed)?;
        if records.len() != locations.len() {
            return Err(JournalError::CompletionMismatch);
        }
        let written = WrittenRecords {
            generation: pipeline.journal().writer().written_position().generation(),
            segment_id: pipeline.journal().writer().header().segment_id(),
            locations,
            records,
        };
        self.reader
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?
            .appended(pipeline.journal(), &written.locations, &written.records)?;
        for part in parts {
            self.writeback
                .queue
                .iter_mut()
                .find(|entry| entry.ticket == part.ticket)
                .expect("checked entry")
                .written = part.range.end;
        }
        while self
            .writeback
            .queue
            .front()
            .is_some_and(|entry| entry.written == entry.append.prepared.len())
        {
            let entry = self.writeback.queue.pop_front().expect("complete entry");
            self.writeback.operations -= entry.append.prepared.len();
            self.writeback.bytes -= entry.append.retained_bytes();
            let _ = entry.done.send(Ok(entry.ticket));
        }
        if let JournalOwner::Writing(pipeline) = &self.journal
            && pipeline.pending() == 0
            && !pipeline.sync_pending()
        {
            self.journal.finish_writes()?;
        }
        observed.armed = false;
        self.faulted = false;
        Ok(written)
    }

    /// Synchronize only the installed prefix and publish its restart evidence.
    /// Later reservations or logical RAM acceptance are never included implicitly.
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

    /// Apply only the exact driver-confirmed prefix. RAM confirmation may
    /// precede its buffered write, but never bypasses canonical admission.
    pub fn apply(&mut self, ticket: ValidationTicket) -> Result<ValidationTicket, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        let through = ticket.committed();
        if !self.configuration.memory_voting()
            && through.op.0
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

    /// Start a roll after physical reservations settle. Accepted queued groups
    /// remain owned and may span the roll without changing retry boundaries.
    pub fn begin_roll(&mut self, max_probes: usize) -> Result<PreparedRoll, JournalError> {
        self.healthy()?;
        self.journal.finish_writes()?;
        let capacity = self.journal.ready()?.writer().header().capacity();
        self.faulted = true;
        let (pending, work) = self
            .journal
            .take_ready()?
            .begin_owned_roll(capacity, max_probes)?;
        self.journal = JournalOwner::Rolling(Box::new(pending));
        self.faulted = false;
        Ok(PreparedRoll {
            work,
            observed: self.writeback.observation(),
        })
    }

    /// Install this owner's exact completed roll, keeping queued canonical
    /// requests and their immutable encoded bodies unchanged.
    pub fn complete_roll(&mut self, done: CompletedRoll) -> Result<(), JournalError> {
        self.healthy()?;
        self.faulted = true;
        let CompletedRoll {
            completed,
            mut observed,
        } = done;
        if !Rc::ptr_eq(&observed.abandoned, &self.writeback.abandoned) {
            return Err(JournalError::CompletionMismatch);
        }
        self.journal =
            JournalOwner::Ready(Box::new(self.journal.take_rolling()?.complete(completed)?));
        self.reader
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?
            .rolled(self.journal.readable()?)?;
        observed.armed = false;
        self.faulted = false;
        Ok(())
    }
}
