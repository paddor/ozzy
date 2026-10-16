//! One owner-local identity image, separate from cloned canonical core images.

pub(super) mod checkpoint;
mod producer;
mod restore;
mod retention;

pub use retention::IdentityRetention;

use ozzy_core::dedup::{Index, Limits, PreparedAdmission, StoredOperation};
use ozzy_journal::operation::{
    CanonicalOperation, OperationBody, OperationKind, OperationLimits, ResolvedAppendView,
    decode_operation_body, decode_resolved_append_with_validated_payload,
};
use ozzy_proto::{MessageId, Offset, PartitionIncarnation};
use ozzy_replication::driver::ValidationTicket;

use super::{AppendBuffer, JournalError, OwnedJournal};

#[derive(Debug)]
pub(super) struct State {
    pub(super) limits: Option<Limits>,
    pub(super) image: Option<Image>,
    codec: Option<Box<crate::replica_journal::append::dedup::Codec>>,
    clock: Option<producer::Clock>,
    memory_wait: std::cell::Cell<Option<crate::memory::Capacity>>,
    validation_memory: std::cell::Cell<crate::memory::Capacity>,
    /// Applied operation a received suffix waits to pass before it retries
    /// validation that ran out of identity capacity.
    pub(super) apply_wait: std::cell::Cell<Option<u64>>,
}

#[derive(Debug)]
pub(super) struct Image {
    pub(super) index: Index,
    partition: Option<PartitionIncarnation>,
    time: u64,
    end: Offset,
    reservation: Option<Installed>,
}

/// Holds an installed image's reservation in the live dedup gauges.
#[derive(Debug)]
struct Installed {
    bytes: i64,
    entries: i64,
}

impl Installed {
    /// Count one installed image.
    fn count(bytes: usize, entries: usize) -> Self {
        use crate::profiling::{Event, count, event, gauge};
        event(Event::DedupIndexImage);
        count(Event::DedupIndexBytes, bytes as u64);
        count(Event::DedupIndexEntries, entries as u64);
        let installed = Self {
            bytes: i64::try_from(bytes).unwrap_or(i64::MAX),
            entries: i64::try_from(entries).unwrap_or(i64::MAX),
        };
        gauge(Event::DedupLiveBytes, installed.bytes);
        gauge(Event::DedupLiveEntries, installed.entries);
        installed
    }
}

impl Drop for Installed {
    fn drop(&mut self) {
        use crate::profiling::{Event, gauge};
        gauge(Event::DedupLiveBytes, -self.bytes);
        gauge(Event::DedupLiveEntries, -self.entries);
    }
}

/// A reversible identity image for the same exact validated canonical group.
#[derive(Debug)]
pub(in crate::replica_journal) struct Prepared {
    admission: PreparedAdmission,
    partition: Option<PartitionIncarnation>,
    _charge: Option<crate::memory::Charge>,
}

struct Rows {
    operation: u64,
    time: u64,
    first: Offset,
    ids: Vec<MessageId>,
}

impl State {
    pub(super) const fn new(limits: Option<Limits>) -> Self {
        Self {
            limits,
            image: None,
            codec: None,
            clock: None,
            memory_wait: std::cell::Cell::new(None),
            validation_memory: std::cell::Cell::new(crate::memory::Capacity::EMPTY),
            apply_wait: std::cell::Cell::new(None),
        }
    }

    pub(super) fn clear(&mut self) {
        self.image = None;
        self.codec = None;
        self.clock = None;
        self.memory_wait.set(None);
    }

    pub(super) fn install(&mut self, prepared: Option<Box<Prepared>>) -> Result<(), JournalError> {
        match (self.limits, prepared) {
            (None, None) => Ok(()),
            (Some(_), Some(prepared)) => {
                let image = self.image.as_mut().ok_or(JournalError::AppendMismatch)?;
                let time = prepared.admission.logical_time();
                let end = prepared.admission.next_offset();
                image.index.install(prepared.admission)?;
                image.partition = prepared.partition;
                image.time = time;
                image.end = end;
                Ok(())
            }
            _ => Err(JournalError::AppendMismatch),
        }
    }

    pub(super) fn commit_through(&mut self, operation: u64) -> Result<(), JournalError> {
        if self.limits.is_some() {
            let image = self.image.as_mut().ok_or(JournalError::AppendMismatch)?;
            image.index.commit_through(operation)?;
        }
        Ok(())
    }
}

impl OwnedJournal {
    pub(super) fn begin_validation_memory(&self) {
        self.clear_validation_memory_wait();
        self.dedup
            .validation_memory
            .set(self.append_memory.as_ref().map_or(
                crate::memory::Capacity::EMPTY,
                crate::memory::Owner::begin_working_set,
            ));
    }

    pub(super) fn prepare_dedup(
        &mut self,
        ticket: ValidationTicket,
        buffer: &AppendBuffer,
    ) -> Result<Option<Box<Prepared>>, JournalError> {
        let Some(limits) = self.dedup.limits else {
            self.refuse_dedup_creation(buffer)?;
            return Ok(None);
        };
        // Decode each stored batch once. Charge returned preparation and
        // descriptor scratch before allocating rows; the long-lived index has
        // its own explicit per-partition byte budget.
        let mut views = smallvec::SmallVec::<[_; 4]>::new();
        let mut records = 0_usize;
        for operation in buffer.operations() {
            if operation.kind == OperationKind::ResolvedAppend {
                // The canonical validator already checked this exact payload.
                views.push(stored_view(
                    operation.body,
                    self.limits.operations,
                    limits,
                    &mut records,
                )?);
            }
        }
        let bytes = records
            .checked_mul(128)
            .and_then(|bytes| bytes.checked_add(buffer.len() * 256 + 512))
            .ok_or(JournalError::AppendCapacity)?;
        let charge = self.charge_validation(bytes)?;
        let image = self
            .dedup
            .image
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?;
        let (partition, rows) = image.rows(
            buffer.operations(),
            views.into_iter(),
            self.limits.operations,
            limits,
        )?;
        let stored = rows.iter().map(|row| row.ids.len()).sum();
        image.index.expire(stored, limits.bytes);
        let operations: smallvec::SmallVec<[_; 4]> = rows
            .iter()
            .map(|row| StoredOperation {
                operation: row.operation,
                logical_time: row.time,
                first_offset: row.first,
                ids: &row.ids,
            })
            .collect();
        let admission = image
            .index
            .prepare_stored(&operations, ticket.committed().op.0)?;
        if admission.retained_bytes() > bytes {
            return Err(JournalError::AppendCapacity);
        }
        Ok(Some(Box::new(Prepared {
            admission,
            partition,
            _charge: charge,
        })))
    }
}

impl OwnedJournal {
    /// Never publish an enabled policy without its independently owned image.
    fn refuse_dedup_creation(&self, buffer: &AppendBuffer) -> Result<(), JournalError> {
        for operation in buffer
            .operations()
            .filter(|op| op.kind == OperationKind::CreatePartition)
        {
            let OperationBody::CreatePartition(create) =
                decode_operation_body(operation.kind, operation.body, self.limits.operations)?
            else {
                return Err(JournalError::AppendMismatch);
            };
            if create.dedup_window_millis.is_some() {
                return Err(JournalError::Configuration);
            }
        }
        Ok(())
    }
}

impl OwnedJournal {
    pub(super) fn charge_validation(
        &self,
        bytes: usize,
    ) -> Result<Option<crate::memory::Charge>, JournalError> {
        Ok(self
            .append_memory
            .as_ref()
            .map(|memory| memory.try_charge(bytes))
            .transpose()?)
    }

    pub(super) fn retain_validation_pressure(&self) -> Result<(), JournalError> {
        let memory = self
            .append_memory
            .as_ref()
            .ok_or(JournalError::AppendCapacity)?;
        // Capture the failed peak before retrying. Dropped scratch alone does
        // not make room for another complete validation attempt.
        let capacity = memory.failed_working_set(self.dedup.validation_memory.get())?;
        self.dedup.memory_wait.set(Some(capacity));
        Ok(())
    }

    /// Whether application passed the operation a deferred received suffix
    /// waits for. Expiry then has the confirmed time the leader admitted at.
    pub(in crate::replica_journal) fn validation_apply_ready(&self) -> bool {
        self.dedup
            .apply_wait
            .get()
            .is_none_or(|applied| self.applied.op.0 > applied)
    }

    pub(in crate::replica_journal) fn validation_memory_ready(&self) -> bool {
        self.dedup.memory_wait.get().is_none_or(|bytes| {
            self.append_memory
                .as_ref()
                .is_none_or(|memory| memory.has_working_capacity(bytes))
        })
    }

    pub(in crate::replica_journal) fn validation_memory_wait(
        &self,
    ) -> Option<impl Future<Output = ()> + use<>> {
        let bytes = self.dedup.memory_wait.get()?;
        let memory = self.append_memory.clone()?;
        Some(async move {
            loop {
                let generation = memory.generation();
                if memory.has_working_capacity(bytes) {
                    return;
                }
                memory.changed_after(generation).await;
            }
        })
    }

    pub(in crate::replica_journal) fn clear_validation_memory_wait(&self) {
        self.dedup.memory_wait.set(None);
    }
}

/// Decode one stored batch and add its records to `total` within `limits`.
fn stored_view<'a>(
    body: &'a [u8],
    decoding: OperationLimits,
    limits: Limits,
    total: &mut usize,
) -> Result<ResolvedAppendView<'a>, JournalError> {
    let view = decode_resolved_append_with_validated_payload(body, decoding)?;
    let count = view.batch.summary.record_count;
    *total = total
        .checked_add(count)
        .ok_or(JournalError::AppendCapacity)?;
    if count > limits.batch_records || *total > limits.pending_records {
        return Err(JournalError::AppendCapacity);
    }
    Ok(view)
}

/// The IDs of a stored batch that continues `partition` at `first`, with the
/// log time and record end it leaves.
fn stored_ids(
    view: &ResolvedAppendView<'_>,
    partition: Option<PartitionIncarnation>,
    first: Offset,
) -> Result<(u64, Offset, Vec<MessageId>), JournalError> {
    let summary = &view.batch.summary;
    if Some(summary.partition) != partition {
        return Err(JournalError::AppendMismatch);
    }
    let end = summary
        .first_offset
        .get()
        .checked_add(summary.record_count as u64)
        .map(Offset::new)
        .ok_or(JournalError::AppendCapacity)?;
    if summary.first_offset != first {
        return Err(JournalError::AppendMismatch);
    }
    let mut ids = Vec::new();
    ids.try_reserve_exact(summary.record_count)
        .map_err(std::io::Error::other)?;
    ids.extend(view.batch.descriptors().map(|record| record.message_id));
    Ok((view.results.logical_time(), end, ids))
}

impl Image {
    /// The row of one recovered operation.
    fn row(
        &self,
        operation: CanonicalOperation<'_>,
        decoding: OperationLimits,
        limits: Limits,
    ) -> Result<(Option<PartitionIncarnation>, Rows), JournalError> {
        let view = (operation.kind == OperationKind::ResolvedAppend)
            .then(|| stored_view(operation.body, decoding, limits, &mut 0))
            .transpose()?;
        let (partition, mut rows) = self.rows(
            std::iter::once(operation),
            view.into_iter(),
            decoding,
            limits,
        )?;
        Ok((partition, rows.pop().ok_or(JournalError::AppendMismatch)?))
    }

    /// Rows of `operations`, given the decoded view of each `ResolvedAppend`
    /// among them in order. Their record counts are already within limits.
    fn rows<'a>(
        &self,
        operations: impl Iterator<Item = CanonicalOperation<'a>>,
        mut views: impl Iterator<Item = ResolvedAppendView<'a>>,
        decoding: OperationLimits,
        limits: Limits,
    ) -> Result<(Option<PartitionIncarnation>, Vec<Rows>), JournalError> {
        let mut rows = Vec::new();
        let mut partition = self.partition;
        let mut time = self.time;
        let mut end = self.end;
        for operation in operations {
            let first = end;
            let ids = match operation.kind {
                OperationKind::ResolvedAppend => {
                    let view = views.next().ok_or(JournalError::AppendMismatch)?;
                    let ids;
                    (time, end, ids) = stored_ids(&view, partition, first)?;
                    ids
                }
                // Enabled partitions require canonical outcomes and log time.
                OperationKind::Append => return Err(JournalError::UnsupportedHistory),
                _ => {
                    match decode_operation_body(operation.kind, operation.body, decoding)? {
                        OperationBody::CreatePartition(create) => {
                            if partition.is_some()
                                || create.dedup_window_millis.map(std::num::NonZeroU64::get)
                                    != Some(limits.window_millis)
                            {
                                return Err(JournalError::Configuration);
                            }
                            partition = Some(create.partition);
                        }
                        OperationBody::DedupWatermark(watermark) => {
                            if Some(watermark.partition) != partition {
                                return Err(JournalError::AppendMismatch);
                            }
                            time = watermark.logical_time;
                        }
                        _ => {}
                    }
                    Vec::new()
                }
            };
            rows.push(Rows {
                operation: operation.op_number,
                time,
                first,
                ids,
            });
        }
        Ok((partition, rows))
    }
}
