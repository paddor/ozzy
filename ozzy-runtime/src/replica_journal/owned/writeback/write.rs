//! Physical group writes: select and encode admitted operations into one
//! bounded group, run it outside the journal borrow, and install its
//! completion in order.

use super::{Encoded, JournalError, JournalOwner, Observation, OwnedJournal, Part};
use ozzy_journal_segment::{
    CompletedJournalWrite, ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, OperationLocation,
    PreparedJournalWrite, PreparedOperationRecords, SEGMENT_HEADER_BYTES, SharedJournalOperation,
    WRITE_GROUP_ALIGNMENT,
};
use ozzy_replication::JournalGeneration;
use std::{rc::Rc, sync::Arc};

/// One physical write, possibly spanning several admitted SDK/core requests.
/// Execute outside the journal borrow, then install its result in order.
#[derive(Debug)]
#[must_use = "execute and install, or fence and reopen the owner"]
pub struct PreparedWrite {
    parts: Vec<Part>,
    work: PreparedJournalWrite,
    observed: Observation,
}

/// Physical completion only. No durable-prefix evidence or vote is implied.
#[derive(Debug)]
#[must_use = "install on the originating journal in reservation order"]
pub struct CompletedWrite {
    parts: Vec<Part>,
    completed: CompletedJournalWrite,
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

impl OwnedJournal {
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
        if self.writes_held()? {
            return Ok(WriteStep::Waiting);
        }
        let encoded = match self.writeback.encoded.take() {
            Some(encoded) => encoded,
            None => match self.encode_write()? {
                Some(encoded) => encoded,
                None => return Ok(WriteStep::Waiting),
            },
        };
        if self.roll_early(&encoded)? {
            self.writeback.encoded = Some(encoded);
            return Ok(WriteStep::RollRequired);
        }
        // A group that does not fit stays encoded until the journal rolls.
        let pipeline = match self.journal.pipeline(self.writeback.limits.max_operations) {
            Ok(pipeline) => pipeline,
            Err(error) => {
                self.writeback.encoded = Some(encoded);
                return Err(error);
            }
        };
        if let Err(error) = pipeline.check(&encoded.group) {
            self.writeback.encoded = Some(encoded);
            if crate::replica_journal::canonical::segment_full(&error) {
                if !self.can_roll()? {
                    self.hold_writes()?;
                    return Ok(WriteStep::Waiting);
                }
                if !self.spare.ready() {
                    self.await_spare()?;
                    return Ok(WriteStep::Waiting);
                }
                return Ok(WriteStep::RollRequired);
            }
            return Err(error.into());
        }
        let Encoded {
            parts,
            group,
            backing_bytes,
        } = encoded;
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

    /// Whether to roll before `encoded` because the active segment holds
    /// too many held records. Without room to roll it writes on.
    fn roll_early(&mut self, encoded: &Encoded) -> Result<bool, JournalError> {
        let Some(next) = encoded.parts.first().and_then(|part| {
            part.append
                .operations()
                .nth(part.range.start)
                .map(|operation| operation.op_number)
        }) else {
            return Ok(false);
        };
        Ok(self.held_crowded(next)? && self.can_roll()? && self.spare.ready())
    }

    /// Select and encode the next bounded group; `None` when nothing waits.
    fn encode_write(&mut self) -> Result<Option<Encoded>, JournalError> {
        let parts = self.select_write()?;
        if parts.is_empty() {
            return Ok(None);
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
        Ok(Some(Encoded {
            parts,
            group: encoding.encode_shared_raw(operations)?,
            backing_bytes,
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
        let records = self.writeback.written_records(&parts)?;
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
        self.writeback.release_written(parts)?;
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
}
