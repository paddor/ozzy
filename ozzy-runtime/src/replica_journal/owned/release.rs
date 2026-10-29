//! Due scheduled records leave their partition in a broker-built `Release`.
//! The owner picks the due prefix from its held index; a detached job reads
//! the `Schedule` or `Carry` operations holding it, beside writes, and builds
//! the proposal. When no held record is loaded but sealed ones are, the job
//! loads the next ones instead. The same delay step carries held records
//! forward when retention would free their segment.

use super::{JournalError, OwnedJournal};
use crate::replica_journal::ProposalBuffer;
use bytes::Bytes;
use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::{
    AppendBatch, AppendRecord, OperationBody, OperationKind, OperationLimits, RELEASE_FRAME_BYTES,
    Release, ReleaseKey, decode_operation_body, encode_operation_body, encoded_record_bytes,
};
use ozzy_journal_segment::{JournalIndexError, JournalIndexSnapshot};
use ozzy_proto::{
    Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence,
};
use ozzy_replication::driver::ValidationTicket;
use std::rc::Rc;

mod carry;

/// Distinct `Schedule` or `Carry` operations one release may read.
const MAX_SOURCES: usize = 64;

/// One bounded delay step: a release or a carry.
#[derive(Debug, Default)]
pub struct ReleaseTurn {
    /// `Release` of due records or `Carry` of held ones, awaiting ordinary
    /// confirmation.
    pub proposal: Option<ProposalBuffer>,
    /// The step loaded held records or stopped carrying instead; the next
    /// one may start now.
    pub loaded: bool,
}

/// A delay step's result once the owner picked its records.
#[derive(Debug)]
pub(in crate::replica_journal) enum ReleaseStep {
    /// Nothing to release or carry now.
    Idle,
    /// Read the held records, or load more, beside writes.
    Read(ReleaseRead),
}

/// A delay step's detached work.
#[derive(Debug)]
pub(in crate::replica_journal) struct ReleaseRead {
    key: Rc<()>,
    work: Work,
}

#[derive(Debug)]
enum Work {
    Records(Box<Records>),
    Load(Box<super::held::Refill>),
    Carry(Box<carry::Carrying>),
}

/// Due held records and where their `Schedule` or `Carry` operations are.
#[derive(Debug)]
struct Records {
    partition: PartitionIncarnation,
    owner_epoch: OwnerEpoch,
    entries: Vec<(ReleaseKey, PendingSchedule)>,
    /// Kinds and bodies still in writeback, by operation number.
    hot: Vec<(u64, OperationKind, Bytes)>,
    /// Operations to read from storage, by number.
    cold: Vec<u64>,
    stored: Option<JournalIndexSnapshot>,
    buffer: ProposalBuffer,
    limits: OperationLimits,
}

#[derive(Debug)]
pub(in crate::replica_journal) struct CompletedRelease {
    key: Rc<()>,
    result: Result<Built, JournalError>,
}

#[derive(Debug)]
enum Built {
    Release(Option<Box<ProposalBuffer>>),
    Load(super::held::Load),
    /// Stop carrying toward the segment ending at this operation.
    Stop(u64),
}

impl OwnedJournal {
    /// Release key millis of the earliest held record whose `Schedule` is
    /// committed, or zero while a carry is pending. Later records wait for
    /// it, since a release is a prefix.
    pub(in crate::replica_journal) fn next_release(&self) -> Option<u64> {
        let held = self.held.as_ref()?;
        if held.carrying().is_some() {
            return Some(0);
        }
        self.next_due()
    }

    fn next_due(&self) -> Option<u64> {
        let committed = self.images().ok()?.committed().revision();
        self.held.as_ref()?.next(committed)
    }

    /// Run one release step on the owner, with its reads inline.
    #[cfg(test)]
    pub(in crate::replica_journal) async fn release_turn(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
    ) -> Result<ReleaseTurn, JournalError> {
        match self.prepare_release(ticket, now_millis).await? {
            ReleaseStep::Idle => Ok(ReleaseTurn::default()),
            ReleaseStep::Read(read) => {
                let completed = read.execute().await;
                self.complete_release(completed)
            }
        }
    }

    /// Pick the due prefix of committed held records on one partition,
    /// bounded by one proposal arena, or carry held records toward the
    /// retention target. Only the primary releases and carries.
    pub(in crate::replica_journal) async fn prepare_release(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
    ) -> Result<ReleaseStep, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        if self.configuration.primary(ticket.scope().view)
            != self.journal.readable()?.manifest().identity.replica_node_id
        {
            return Ok(ReleaseStep::Idle);
        }
        let due = self.next_due().is_some_and(|due| due <= now_millis);
        if let Some(target) = self.held.as_mut().and_then(|held| held.carry_turn(due)) {
            self.observe_held()?;
            return self.prepare_carry(target).await;
        }
        if !due {
            return Ok(ReleaseStep::Idle);
        }
        self.observe_held()?;
        if let Some(load) = self.prepare_load().await? {
            return Ok(load);
        }
        let buffer = match self.lease_proposal_buffer() {
            Ok(buffer) => buffer,
            Err(crate::replica_journal::SubmitError::Full) => {
                return Ok(ReleaseStep::Idle);
            }
            Err(_) => return Err(JournalError::Stopped),
        };
        let limits = self.limits.operations;
        // Record bytes bound the payload too.
        let budget = buffer
            .limits()
            .max_body_bytes
            .min(limits.max_body_bytes)
            .saturating_sub(RELEASE_FRAME_BYTES)
            .min(limits.max_payload_bytes);
        let Some((partition, owner_epoch, entries)) =
            self.due_prefix(now_millis, limits.max_records, budget)?
        else {
            return Ok(ReleaseStep::Idle);
        };
        let (hot, cold) = self.sources(&entries);
        let stored = match cold.last() {
            Some(&last) => Some(self.release_index(last).await?),
            None => None,
        };
        Ok(ReleaseStep::Read(ReleaseRead {
            key: self.read_key.clone(),
            work: Work::Records(Box::new(Records {
                partition,
                owner_epoch,
                entries,
                hot,
                cold,
                stored,
                buffer,
                limits,
            })),
        }))
    }

    /// An index snapshot holding operations through `last`. Building one
    /// holds the owner while it opens indexes and scans the active segment.
    /// Due records were scheduled about a delay ago, so one snapshot serves
    /// the releases of about that long. It pins its segments until a later
    /// build or storage cleanup drops it.
    async fn release_index(&mut self, last: u64) -> Result<JournalIndexSnapshot, JournalError> {
        let held = self.held.as_ref().ok_or(JournalError::AppendMismatch)?;
        if let Some(index) = &held.index
            && index.through().op_number >= last
        {
            return Ok(index.clone());
        }
        // OPTIMIZE: read cold release sources without rebuilding the index.
        // The partition reader sees each written group's operation locations.
        let index = self.journal.retry_snapshot(self.recovery.index).await?;
        if let Some(held) = self.held.as_mut() {
            held.index = Some(index.clone());
        }
        Ok(index)
    }

    /// Kinds and bodies of the entries' `Schedule` or `Carry` operations
    /// still in writeback or kept with the held rows, and the numbers of
    /// those to read from storage.
    fn sources(
        &self,
        entries: &[(ReleaseKey, PendingSchedule)],
    ) -> (Vec<(u64, OperationKind, Bytes)>, Vec<u64>) {
        let mut hot = Vec::new();
        let mut cold = Vec::new();
        let mut sources = entries
            .iter()
            .map(|(_, pending)| pending.operation)
            .collect::<Vec<_>>();
        sources.sort_unstable();
        sources.dedup();
        for number in sources {
            let kept = || self.held.as_ref()?.body(number);
            match self.writeback.operation_bytes(number).or_else(kept) {
                Some((kind, body)) => hot.push((number, kind, body)),
                None => cold.push(number),
            }
        }
        (hot, cold)
    }

    /// Load the next sealed held records when none is loaded.
    async fn prepare_load(&mut self) -> Result<Option<ReleaseStep>, JournalError> {
        if !self
            .held
            .as_ref()
            .is_some_and(super::held::Held::needs_refill)
        {
            return Ok(None);
        }
        let snapshot = self.journal.retry_snapshot(self.recovery.index).await?;
        let refill = self
            .held
            .as_ref()
            .ok_or(JournalError::AppendMismatch)?
            .refill(snapshot);
        Ok(Some(ReleaseStep::Read(ReleaseRead {
            key: self.read_key.clone(),
            work: Work::Load(Box::new(refill)),
        })))
    }

    /// The due prefix of committed held records, bounded by count, record
    /// bytes, and source operations.
    #[expect(
        clippy::type_complexity,
        reason = "one private result tuple read by the caller only"
    )]
    fn due_prefix(
        &self,
        now_millis: u64,
        max_records: usize,
        budget: usize,
    ) -> Result<
        Option<(
            PartitionIncarnation,
            OwnerEpoch,
            Vec<(ReleaseKey, PendingSchedule)>,
        )>,
        JournalError,
    > {
        let images = self.images()?;
        let committed = images.committed().revision();
        let Some(held) = &self.held else {
            return Ok(None);
        };
        let partition = held.partition();
        let state = images
            .speculative()
            .partition(partition)
            .ok_or(JournalError::AppendMismatch)?;
        let mut entries = Vec::new();
        let mut sources = Vec::<u64>::new();
        let mut bytes = 0_usize;
        let due = held
            .prefix()
            .take_while(|(key, _)| key.millis <= now_millis);
        for (key, pending) in due {
            let total = bytes.saturating_add(pending.bytes as usize);
            let fresh = !sources.contains(&pending.operation);
            if pending.operation > committed
                || entries.len() == max_records
                || (total > budget && !entries.is_empty())
                || (fresh && sources.len() == MAX_SOURCES)
            {
                break;
            }
            if fresh {
                sources.push(pending.operation);
            }
            bytes = total;
            entries.push((key, *pending));
        }
        if entries.is_empty() {
            return Ok(None);
        }
        Ok(Some((partition, state.owner_epoch, entries)))
    }

    pub(in crate::replica_journal) fn complete_release(
        &mut self,
        completed: CompletedRelease,
    ) -> Result<ReleaseTurn, JournalError> {
        if !Rc::ptr_eq(&self.read_key, &completed.key) {
            return Err(crate::replica_journal::PartitionReadError::Fenced.into());
        }
        self.faulted |= completed
            .result
            .as_ref()
            .err()
            .is_some_and(crate::replica_journal::commands::read_fault);
        match completed.result? {
            Built::Release(proposal) => Ok(ReleaseTurn {
                proposal: proposal.map(|proposal| *proposal),
                loaded: false,
            }),
            Built::Load(load) => {
                if let Some(held) = self.held.as_mut() {
                    held.load(load);
                }
                self.check_held()?;
                Ok(ReleaseTurn {
                    proposal: None,
                    loaded: true,
                })
            }
            Built::Stop(end) => {
                if let Some(held) = self.held.as_mut() {
                    held.drop_target(end);
                }
                Ok(ReleaseTurn {
                    proposal: None,
                    loaded: true,
                })
            }
        }
    }
}

impl ReleaseRead {
    pub(in crate::replica_journal) async fn execute(self) -> CompletedRelease {
        let result = match self.work {
            Work::Records(records) => records
                .build()
                .await
                .map(|proposal| Built::Release(proposal.map(Box::new))),
            Work::Load(refill) => refill.read().await.map(Built::Load),
            Work::Carry(carrying) => carrying.build().await,
        };
        CompletedRelease {
            key: self.key,
            result,
        }
    }
}

impl Records {
    /// Copy the held records in release order into one `Release` with no
    /// position yet. Validation places it at the partition's next offset.
    async fn build(self) -> Result<Option<ProposalBuffer>, JournalError> {
        let Self {
            partition,
            owner_epoch,
            entries,
            hot,
            cold,
            stored,
            mut buffer,
            limits,
        } = self;
        let bodies = read_sources(hot, &cold, stored.as_ref()).await?;
        let sources = batches(&bodies, limits)?;
        let room = buffer.limits().max_body_bytes.min(limits.max_body_bytes);
        let (records, _) =
            held_records(&sources, &entries, limits, room, (RELEASE_FRAME_BYTES, 0))?;
        let Some(through) = records.len().checked_sub(1).map(|last| entries[last].0) else {
            return Ok(None);
        };
        let body = OperationBody::Release(Release {
            batch: AppendBatch {
                partition,
                owner_epoch,
                producer_id: ProducerId::from_bytes([0; 16]),
                producer_epoch: ProducerEpoch::new(0),
                first_sequence: ProducerSequence::ZERO,
                first_offset: Offset::ZERO,
                append_timestamp_millis: 0,
                records: records.into(),
            },
            through,
        });
        let encoded = encode_operation_body(&body, limits)?;
        buffer.reserve_payload(encoded.len()).await?;
        buffer.push(OperationKind::Release, &encoded)?;
        Ok(Some(buffer))
    }
}

/// Kinds and bodies of the source operations: `hot` ones, then `cold` ones
/// read from `stored`, by number.
async fn read_sources(
    mut hot: Vec<(u64, OperationKind, Bytes)>,
    cold: &[u64],
    stored: Option<&JournalIndexSnapshot>,
) -> Result<Vec<(u64, OperationKind, Bytes)>, JournalError> {
    if let Some(stored) = stored {
        for &number in cold {
            let operation = stored
                .read_operation_number(number)
                .await?
                .ok_or(JournalIndexError::MissingOperationNumber(number))?;
            hot.push((
                number,
                operation.kind,
                Bytes::from(operation.body.into_owned()),
            ));
        }
    }
    hot.sort_unstable_by_key(|(number, _, _)| *number);
    Ok(hot)
}

/// The batch of each `Schedule` or `Carry` source, by number.
fn batches(
    bodies: &[(u64, OperationKind, Bytes)],
    limits: OperationLimits,
) -> Result<Vec<(u64, AppendBatch<'_>)>, JournalError> {
    bodies
        .iter()
        .map(
            |(number, kind, body)| match decode_operation_body(*kind, body, limits)? {
                OperationBody::Schedule(schedule) => Ok((*number, schedule.batch)),
                OperationBody::Carry(carry) => Ok((*number, carry.batch)),
                _ => Err(JournalError::AppendMismatch),
            },
        )
        .collect()
}

/// The first held records of `entries` that fit one operation of `room`
/// bytes, and that operation's size. `framing` is its bytes beyond the
/// records, and the bytes it adds per record.
fn held_records<'a>(
    sources: &'a [(u64, AppendBatch<'_>)],
    entries: &[(ReleaseKey, PendingSchedule)],
    limits: OperationLimits,
    room: usize,
    (frame, extra): (usize, usize),
) -> Result<(Vec<AppendRecord<'a>>, usize), JournalError> {
    let mut records = Vec::with_capacity(entries.len());
    let (mut bytes, mut payload, mut parts) = (frame, 0_usize, 0_usize);
    for (_, pending) in entries {
        let index = sources
            .binary_search_by_key(&pending.operation, |(number, _)| *number)
            .map_err(|_| JournalError::AppendMismatch)?;
        let record = sources[index]
            .1
            .records
            .get(pending.index as usize)
            .filter(|record| record.message_id == pending.message_id)
            .ok_or(JournalError::AppendMismatch)?;
        let size = record.parts.iter().map(<[u8]>::len).sum::<usize>();
        let count = record.parts.len();
        let total = bytes + encoded_record_bytes(record.encoding, record.parts.iter()) + extra;
        payload += size;
        parts += count;
        // The first record always fits a `Release`: its `Schedule` or
        // `Carry` was at least as large as a `Release` of it alone.
        if !records.is_empty()
            && (total > room || payload > limits.max_payload_bytes || parts > limits.max_parts)
        {
            break;
        }
        bytes = total;
        records.push(AppendRecord {
            encoding: record.encoding,
            message_id: record.message_id,
            parts: record.parts.iter().collect(),
        });
    }
    Ok((records, bytes))
}
