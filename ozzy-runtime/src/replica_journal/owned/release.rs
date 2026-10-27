//! Due scheduled records leave their partition in a broker-built `Release`.
//! The owner picks the due prefix; a detached job reads the `Schedule`
//! operations holding it, beside writes, and builds the proposal.

use super::{JournalError, OwnedJournal};
use crate::replica_journal::ProposalBuffer;
use bytes::Bytes;
use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::{
    AppendBatch, AppendRecord, OperationBody, OperationKind, OperationLimits, Release, ReleaseKey,
    Schedule, decode_operation_body, encode_operation_body,
};
use ozzy_journal_segment::{JournalIndexError, JournalIndexSnapshot};
use ozzy_proto::{
    Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence,
};
use ozzy_replication::driver::ValidationTicket;
use std::rc::Rc;

/// Distinct `Schedule` operations one release may read.
const MAX_SOURCES: usize = 64;

/// Conservative descriptor bytes per record and part.
const RECORD_BYTES: usize = 24;
const PART_BYTES: usize = 4;

/// Batch framing and the release key, beyond the record descriptors.
const FRAME_BYTES: usize = 128;

/// One bounded release step.
#[derive(Debug, Default)]
pub struct ReleaseTurn {
    /// `Release` of due records, awaiting ordinary confirmation.
    pub proposal: Option<ProposalBuffer>,
}

/// A release step's result once the owner picked its records.
#[derive(Debug)]
pub(in crate::replica_journal) enum ReleaseStep {
    /// Nothing to release now.
    Idle,
    /// Read the held records beside writes.
    Read(Box<ReleaseRead>),
}

/// Due held records and where their `Schedule` operations are.
#[derive(Debug)]
pub(in crate::replica_journal) struct ReleaseRead {
    key: Rc<()>,
    partition: PartitionIncarnation,
    owner_epoch: OwnerEpoch,
    entries: Vec<(ReleaseKey, PendingSchedule)>,
    /// Bodies still in writeback, by operation number.
    hot: Vec<(u64, Bytes)>,
    /// Operations to read from storage, by number.
    cold: Vec<u64>,
    stored: Option<JournalIndexSnapshot>,
    buffer: ProposalBuffer,
    limits: OperationLimits,
}

#[derive(Debug)]
pub(in crate::replica_journal) struct CompletedRelease {
    key: Rc<()>,
    result: Result<Option<ProposalBuffer>, JournalError>,
}

impl OwnedJournal {
    /// Release key millis of the earliest held record whose `Schedule` is
    /// committed. Later records wait for it, since a release is a prefix.
    pub(in crate::replica_journal) fn next_release(&self) -> Option<u64> {
        let images = self.images().ok()?;
        let committed = images.committed().revision();
        images
            .speculative()
            .partitions()
            .filter_map(|(_, state)| {
                let (key, pending) = state.delay.as_ref()?.pending().next()?;
                (pending.operation <= committed).then_some(key.millis)
            })
            .min()
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
    /// bounded by one proposal arena. Only the primary releases.
    pub(in crate::replica_journal) async fn prepare_release(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
    ) -> Result<ReleaseStep, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        if self.configuration.primary(ticket.scope().view)
            != self.journal.readable()?.manifest().identity.replica_node_id
            || self.next_release().is_none_or(|due| due > now_millis)
        {
            return Ok(ReleaseStep::Idle);
        }
        let buffer = match self.lease_proposal_buffer() {
            Ok(buffer) => buffer,
            Err(crate::replica_journal::SubmitError::Full) => {
                return Ok(ReleaseStep::Idle);
            }
            Err(_) => return Err(JournalError::Stopped),
        };
        let limits = self.limits.operations;
        let budget = buffer
            .limits()
            .max_body_bytes
            .min(limits.max_body_bytes)
            .min(limits.max_payload_bytes);
        let Some((partition, owner_epoch, entries)) =
            self.due_prefix(now_millis, limits.max_records, budget)?
        else {
            return Ok(ReleaseStep::Idle);
        };
        let mut hot = Vec::new();
        let mut cold = Vec::new();
        let mut sources = entries
            .iter()
            .map(|(_, pending)| pending.operation)
            .collect::<Vec<_>>();
        sources.sort_unstable();
        sources.dedup();
        for number in sources {
            match self.writeback.operation_bytes(number) {
                Some(body) => hot.push((number, body)),
                None => cold.push(number),
            }
        }
        let stored = if cold.is_empty() {
            None
        } else {
            Some(self.journal.retry_snapshot(self.recovery.index).await?)
        };
        Ok(ReleaseStep::Read(Box::new(ReleaseRead {
            key: self.read_key.clone(),
            partition,
            owner_epoch,
            entries,
            hot,
            cold,
            stored,
            buffer,
            limits,
        })))
    }

    /// The due prefix of the partition with the earliest committed held
    /// record, bounded by count, payload bytes, and source operations.
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
        let Some((partition, state, delay)) = images
            .speculative()
            .partitions()
            .filter_map(|(partition, state)| Some((partition, state, state.delay.as_ref()?)))
            .filter(|(_, _, delay)| {
                delay.pending().next().is_some_and(|(key, pending)| {
                    key.millis <= now_millis && pending.operation <= committed
                })
            })
            .min_by_key(|(_, _, delay)| delay.next_release())
        else {
            return Ok(None);
        };
        let mut entries = Vec::new();
        let mut sources = Vec::<u64>::new();
        let mut bytes = 0_usize;
        for (key, pending) in delay.due(now_millis) {
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
        Ok(ReleaseTurn {
            proposal: completed.result?,
        })
    }
}

impl ReleaseRead {
    pub(in crate::replica_journal) async fn execute(self) -> CompletedRelease {
        let key = self.key.clone();
        CompletedRelease {
            key,
            result: self.build().await,
        }
    }

    /// Copy the held records in release order into one `Release` with no
    /// position yet. Validation places it at the partition's next offset.
    async fn build(self) -> Result<Option<ProposalBuffer>, JournalError> {
        let Self {
            partition,
            owner_epoch,
            entries,
            mut hot,
            cold,
            stored,
            mut buffer,
            limits,
            ..
        } = self;
        if let Some(stored) = &stored {
            for number in cold {
                let operation = stored
                    .read_operation_number(number)
                    .await?
                    .ok_or(JournalIndexError::MissingOperationNumber(number))?;
                if operation.kind != OperationKind::Schedule {
                    return Err(JournalError::AppendMismatch);
                }
                hot.push((number, Bytes::from(operation.body.into_owned())));
            }
        }
        hot.sort_unstable_by_key(|(number, _)| *number);
        let sources = hot
            .iter()
            .map(|(number, body)| {
                match decode_operation_body(OperationKind::Schedule, body, limits)? {
                    OperationBody::Schedule(schedule) => Ok((*number, schedule)),
                    _ => Err(JournalError::AppendMismatch),
                }
            })
            .collect::<Result<Vec<_>, JournalError>>()?;
        let room = buffer.limits().max_body_bytes.min(limits.max_body_bytes);
        let (records, through) = held_records(&sources, &entries, limits, room)?;
        let Some(through) = through else {
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
        buffer.push(
            OperationKind::Release,
            &encode_operation_body(&body, limits)?,
        )?;
        Ok(Some(buffer))
    }
}

/// The held records in release order that fit one operation, and the
/// release key of the last one.
fn held_records<'a>(
    sources: &'a [(u64, Schedule<'_>)],
    entries: &[(ReleaseKey, PendingSchedule)],
    limits: OperationLimits,
    room: usize,
) -> Result<(Vec<AppendRecord<'a>>, Option<ReleaseKey>), JournalError> {
    let mut records = Vec::with_capacity(entries.len());
    let mut through = None;
    let (mut bytes, mut payload, mut parts) = (FRAME_BYTES, 0_usize, 0_usize);
    for (key, pending) in entries {
        let index = sources
            .binary_search_by_key(&pending.operation, |(number, _)| *number)
            .map_err(|_| JournalError::AppendMismatch)?;
        let record = sources[index]
            .1
            .batch
            .records
            .get(pending.index as usize)
            .filter(|record| record.message_id == pending.message_id)
            .ok_or(JournalError::AppendMismatch)?;
        let size = record.parts.iter().map(<[u8]>::len).sum::<usize>();
        let count = record.parts.len();
        bytes += RECORD_BYTES + PART_BYTES * count + size;
        payload += size;
        parts += count;
        // The estimate runs high, but the first record always fits: its
        // `Schedule` was at least as large as a `Release` of it alone.
        if !records.is_empty()
            && (bytes > room || payload > limits.max_payload_bytes || parts > limits.max_parts)
        {
            break;
        }
        records.push(AppendRecord {
            encoding: record.encoding,
            message_id: record.message_id,
            parts: record.parts.iter().collect(),
        });
        through = Some(*key);
    }
    Ok((records, through))
}
