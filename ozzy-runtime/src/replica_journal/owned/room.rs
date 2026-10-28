//! Segment room. The segment limit bounds every replica's journal. A nearly
//! full journal refuses new client work with a capacity rejection, and a full
//! one holds its writes until retirement or a pack frees a segment. Neither
//! faults.

use super::{AppendBuffer, JournalError, OwnedJournal, ownership::JournalOwner};
use ozzy_journal::operation::{OperationKind, RELEASE_FRAME_BYTES};
use ozzy_journal_segment::{ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, WRITE_GROUP_ALIGNMENT};

/// Whether a refusal or a write hold is in effect; each start logs once.
#[derive(Debug, Default)]
pub(super) struct Room {
    refusing: bool,
    holding: bool,
    awaiting_spare: bool,
}

/// Segments kept free at least for records and for control requests.
/// Followers group the same operations on their own timing; one can need
/// its active segment and sealed ones not yet packed beyond its leader's.
const RECORD_SEGMENTS: u64 = 4;
const CONTROL_SEGMENTS: u64 = 2;

/// Physical bytes of one operation beyond its body: entry header and padding.
const ENTRY_BYTES: u64 = ENTRY_HEADER_BYTES as u64 + 7;

/// Most bytes one held record takes beyond its encoded size when released
/// alone: the `Release` frame, its entry, and a write group's alignment and
/// seal, on any copy.
const RELEASE_RECORD_BYTES: u64 =
    (RELEASE_FRAME_BYTES + WRITE_GROUP_ALIGNMENT + GROUP_SEAL_BYTES) as u64 + ENTRY_BYTES;

/// Free segments below which new records stop: a sixteenth of the limit.
pub(super) const fn record_reserve(max_segments: u64) -> u64 {
    let sixteenth = max_segments / 16;
    if sixteenth > RECORD_SEGMENTS {
        sixteenth
    } else {
        RECORD_SEGMENTS
    }
}

/// Room an operation of `kind` must leave: none for operations that free
/// space, `control` for control requests, and `records` for records. A
/// release writes records but ends the retention hold of their schedules,
/// so it takes the control reserve alone. Every other request also leaves
/// `held` free, the room to release every held record, so one can always
/// leave. A carry copies held records while their sources stay until
/// retention, so it leaves that room too.
const fn reserve(kind: OperationKind, records: u64, control: u64, held: u64) -> u64 {
    match kind {
        OperationKind::Trim
        | OperationKind::PartitionPolicy
        | OperationKind::ProducerResultFloor
        | OperationKind::RetireProducer
        | OperationKind::DedupWatermark
        | OperationKind::GroupDelete => 0,
        OperationKind::Release => control,
        OperationKind::CreatePartition
        | OperationKind::OpenProducer
        | OperationKind::Progress
        | OperationKind::Assign
        | OperationKind::GroupPlan
        | OperationKind::Carry => control.saturating_add(held),
        OperationKind::Append
        | OperationKind::ResolvedAppend
        | OperationKind::Schedule
        | OperationKind::Barrier => records.saturating_add(held),
    }
}

impl OwnedJournal {
    /// Refuse new operations a nearly full journal must not take. Records
    /// stop with a sixteenth of the segment space left and control requests
    /// with a sixty-fourth, each at least a few segments, so consumers and
    /// writers can still settle. Both also leave room to release every held
    /// record. Without a spare a roll may find no room on the device, so the
    /// same shares of the active segment apply too, and new work must leave
    /// them free. Trims and policy changes always pass: they free space. The
    /// reserve covers followers that need more space for the same
    /// operations; one that still fills holds.
    pub(super) fn check_room(&mut self, buffer: &AppendBuffer) -> Result<(), JournalError> {
        let journal = self.journal.readable()?;
        let segment = journal.writer().header().capacity();
        let max = self.limits.metadata.max_segments as u64;
        let records = segment.saturating_mul(record_reserve(max));
        let control = segment.saturating_mul((max / 64).max(CONTROL_SEGMENTS));
        let held = self.release_reserve(buffer, segment)?;
        let needed = |records, control, held| {
            buffer
                .operations()
                .map(|operation| reserve(operation.kind, records, control, held))
                .max()
                .unwrap_or(0)
        };
        let active = segment.saturating_sub(journal.writer().written_position().end_offset());
        let queued = self.writeback.queued_bytes() as u64;
        let room = max
            .saturating_sub(journal.manifest().segments.len() as u64)
            .saturating_mul(segment)
            .saturating_add(active)
            .saturating_sub(queued);
        let (tail, tail_records, tail_needed) = if self.spare.ready() {
            (active, 0, 0)
        } else {
            let reserve = needed(segment / 16, segment / 64, 0);
            let tail = active.saturating_sub(queued);
            let body = if reserve == 0 {
                0
            } else {
                buffer.body_bytes() as u64
            };
            (tail, segment / 16, reserve.saturating_add(body))
        };
        let full = room < records.saturating_add(held);
        let refusing = full || tail < tail_records;
        if refusing && !self.room.refusing {
            let message = if full {
                let total = segment.saturating_mul(max);
                format!(
                    "replica journal nearly full: {room} of {total} bytes left; refusing new records"
                )
            } else {
                format!(
                    "replica journal has no allocated spare segment and {tail} bytes left in its active one; refusing new records"
                )
            };
            crate::profiling::log(crate::profiling::Level::Warn, &message);
        }
        self.room.refusing = refusing;
        if room < needed(records, control, held) || tail < tail_needed {
            return Err(JournalError::RollCapacity);
        }
        Ok(())
    }

    /// Room to release every held record, a scheduled `buffer`'s included,
    /// each record alone in its write group. Each roll on the way can leave
    /// up to one write group of its segment unused.
    fn release_reserve(&self, buffer: &AppendBuffer, segment: u64) -> Result<u64, JournalError> {
        let (mut bytes, mut records) = self
            .images()?
            .speculative()
            .partitions()
            .filter_map(|(_, state)| state.delay.as_ref())
            .fold((0_u64, 0_u64), |(bytes, records), delay| {
                (
                    bytes.saturating_add(delay.held_bytes()),
                    records.saturating_add(delay.held_records()),
                )
            });
        if let Some(scheduled) = buffer.scheduled() {
            bytes = bytes.saturating_add(buffer.body_bytes() as u64);
            records = records.saturating_add(scheduled.due_millis.len() as u64);
        }
        if records == 0 {
            return Ok(0);
        }
        let held = bytes.saturating_add(records.saturating_mul(RELEASE_RECORD_BYTES));
        let limits = self.writeback.limits;
        let group = (limits.max_operations as u64)
            .saturating_mul(ENTRY_BYTES)
            .saturating_add(
                (limits.max_body_bytes + WRITE_GROUP_ALIGNMENT + GROUP_SEAL_BYTES) as u64,
            );
        let rolls = held
            .div_ceil(segment.saturating_sub(group).max(1))
            .saturating_add(1);
        Ok(held.saturating_add(rolls.saturating_mul(group)))
    }

    /// Whether one more segment fits the segment limit.
    pub(super) fn can_roll(&self) -> Result<bool, JournalError> {
        Ok(self.journal.readable()?.manifest().segments.len() < self.limits.metadata.max_segments)
    }

    /// Hold writes at the segment limit instead of rolling. With none in
    /// flight the journal leaves its write pipeline, so a retirement or a
    /// pack can install and free a segment.
    pub(super) fn hold_writes(&mut self) -> Result<(), JournalError> {
        if !std::mem::replace(&mut self.room.holding, true) {
            crate::profiling::log(
                crate::profiling::Level::Warn,
                "replica journal full: holding writes until a segment is freed",
            );
        }
        self.leave_pipeline()
    }

    /// Hold writes until the spare is allocated instead of rolling into new
    /// space. The journal leaves its write pipeline as at the segment limit.
    pub(super) fn await_spare(&mut self) -> Result<(), JournalError> {
        self.room.awaiting_spare = true;
        self.leave_pipeline()
    }

    fn leave_pipeline(&mut self) -> Result<(), JournalError> {
        if let JournalOwner::Writing(pipeline) = &self.journal
            && pipeline.pending() == 0
            && !pipeline.sync_pending()
        {
            self.journal.finish_writes()?;
        }
        Ok(())
    }

    /// Whether writes still hold: a segment must be freed or the spare
    /// allocated before the next roll.
    pub(super) fn writes_held(&self) -> Result<bool, JournalError> {
        Ok(self.room.holding && !self.can_roll()?
            || self.room.awaiting_spare && !self.spare.ready())
    }

    /// A roll installed or a segment was freed; writes no longer hold.
    pub(super) fn release_writes(&mut self) {
        self.room.holding = false;
        self.room.awaiting_spare = false;
    }
}
