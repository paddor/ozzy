//! The primary carries held records forward so retention can free their
//! segment. A carry step reads the held rows from the carry floor up to the
//! target's end in position order, beside writes, and copies a bounded prefix
//! into one `Carry` that ends where the next row starts. A byte target
//! carries only while those rows fill at most half the segment, so each carry
//! frees more than it writes.

use super::super::held::{Between, boundary};
use super::super::{JournalError, OwnedJournal};
use super::{
    Built, MAX_SOURCES, ReleaseRead, ReleaseStep, Work, batches, held_records, read_sources,
};
use crate::replica_journal::ProposalBuffer;
use bytes::Bytes;
use ozzy_core::retention::Carry as Target;
use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::{
    AppendBatch, CARRY_FRAME_BYTES, Carry, HeldPosition, OperationBody, OperationKind,
    OperationLimits, ReleaseKey, encode_operation_body,
};
use ozzy_journal_segment::JournalIndexSnapshot;
use ozzy_proto::{
    Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence,
};

/// Bytes of one carried record's release key.
const KEY_BYTES: usize = 16;

/// A carry step's detached work.
#[derive(Debug)]
pub(super) struct Carrying {
    between: Between,
    stored: JournalIndexSnapshot,
    partition: PartitionIncarnation,
    owner_epoch: OwnerEpoch,
    from: HeldPosition,
    end: u64,
    /// Held record bytes above which a byte target waits instead.
    guard: Option<u64>,
    /// Kinds and bodies of source operations still in writeback.
    hot: Vec<(u64, OperationKind, Bytes)>,
    buffer: ProposalBuffer,
    limits: OperationLimits,
}

impl OwnedJournal {
    /// Read the held rows from the carry floor toward `target` beside writes.
    pub(super) async fn prepare_carry(
        &mut self,
        target: Target,
    ) -> Result<ReleaseStep, JournalError> {
        let buffer = match self.lease_proposal_buffer() {
            Ok(buffer) => buffer,
            Err(crate::replica_journal::SubmitError::Full) => {
                return Ok(ReleaseStep::Idle);
            }
            Err(_) => return Err(JournalError::Stopped),
        };
        let held = self.held.as_ref().ok_or(JournalError::AppendMismatch)?;
        let (partition, from) = (held.partition(), held.carried());
        let owner_epoch = self
            .images()?
            .speculative()
            .partition(partition)
            .ok_or(JournalError::AppendMismatch)?
            .owner_epoch;
        let hot = self.writeback.held_sources(from.operation..target.end);
        let stored = self.journal.retry_snapshot(self.recovery.index).await?;
        let limits = self.limits.operations;
        // One row past a full carry shows where it ends.
        let between = self
            .held
            .as_ref()
            .ok_or(JournalError::AppendMismatch)?
            .between(
                stored.clone(),
                (from, boundary(target.end)),
                limits.max_records.saturating_add(1),
            );
        Ok(ReleaseStep::Read(ReleaseRead {
            key: self.read_key.clone(),
            work: Work::Carry(Box::new(Carrying {
                between,
                stored,
                partition,
                owner_epoch,
                from,
                end: target.end,
                guard: (!target.expired).then_some(target.capacity / 2),
                hot,
                buffer,
                limits,
            })),
        }))
    }
}

impl Carrying {
    /// Copy the first held rows, bounded by count, bytes, and source
    /// operations, into one `Carry` with no time yet. Validation stamps it.
    /// Stop carrying instead when the guard trips or one record alone does
    /// not fit.
    pub(super) async fn build(self) -> Result<Built, JournalError> {
        let Self {
            between,
            stored,
            partition,
            owner_epoch,
            from,
            end,
            guard,
            hot,
            mut buffer,
            limits,
        } = self;
        let (rows, held) = between.read().await?;
        if guard.is_some_and(|half| held > half) {
            return Ok(Built::Stop(end));
        }
        let room = buffer.limits().max_body_bytes.min(limits.max_body_bytes);
        let (taken, sources) = first_rows(&rows, limits, room);
        let bodies = read_used(hot, &sources, &stored).await?;
        let sources = batches(&bodies, limits)?;
        let (records, size) = held_records(
            &sources,
            &rows[..taken],
            limits,
            room,
            (CARRY_FRAME_BYTES, KEY_BYTES),
        )?;
        if size > room {
            // The record's segment stays until it is released.
            return Ok(Built::Stop(end));
        }
        let through = rows
            .get(records.len())
            .map_or(boundary(end), |(_, row)| row.position());
        let keys = rows[..records.len()].iter().map(|(key, _)| *key).collect();
        let body = OperationBody::Carry(Carry {
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
            from,
            through,
            keys,
        });
        buffer.push(OperationKind::Carry, &encode_operation_body(&body, limits)?)?;
        Ok(Built::Release(Some(Box::new(buffer))))
    }
}

/// Kinds and bodies of the `sources` operations, from `hot` when it has
/// them, else from `stored`.
async fn read_used(
    hot: Vec<(u64, OperationKind, Bytes)>,
    sources: &[u64],
    stored: &JournalIndexSnapshot,
) -> Result<Vec<(u64, OperationKind, Bytes)>, JournalError> {
    let cold: Vec<u64> = sources
        .iter()
        .copied()
        .filter(|number| hot.iter().all(|(hot, _, _)| hot != number))
        .collect();
    let hot = hot
        .into_iter()
        .filter(|(number, _, _)| sources.contains(number))
        .collect();
    read_sources(hot, &cold, Some(stored)).await
}

/// How many of the first `rows` one `Carry` of `room` bytes may take,
/// bounded by count, record bytes, and source operations, and their
/// sources.
fn first_rows(
    rows: &[(ReleaseKey, PendingSchedule)],
    limits: OperationLimits,
    room: usize,
) -> (usize, Vec<u64>) {
    // Record bytes bound the payload too.
    let budget = room
        .saturating_sub(CARRY_FRAME_BYTES)
        .min(limits.max_payload_bytes);
    let mut sources = Vec::<u64>::new();
    let (mut bytes, mut taken) = (0_usize, 0_usize);
    for (_, row) in rows {
        let total = bytes.saturating_add(row.bytes as usize + KEY_BYTES);
        let fresh = !sources.contains(&row.operation);
        if taken == limits.max_records
            || (total > budget && taken != 0)
            || (fresh && sources.len() == MAX_SOURCES)
        {
            break;
        }
        if fresh {
            sources.push(row.operation);
        }
        (bytes, taken) = (total, taken + 1);
    }
    (taken, sources)
}
