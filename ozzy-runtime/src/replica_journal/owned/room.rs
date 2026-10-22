//! Segment room. The segment limit bounds every replica's journal. A nearly
//! full journal refuses new client work with a capacity rejection, and a full
//! one holds its writes until retirement or a pack frees a segment. Neither
//! faults.

use super::{AppendBuffer, JournalError, OwnedJournal, ownership::JournalOwner};
use ozzy_journal::operation::OperationKind;

/// Whether a refusal or a write hold is in effect; each start logs once.
#[derive(Debug, Default)]
pub(super) struct Room {
    refusing: bool,
    holding: bool,
}

/// Segments kept free at least for records and for control requests.
/// Followers group the same operations on their own timing; one can need
/// its active segment and sealed ones not yet packed beyond its leader's.
const RECORD_SEGMENTS: u64 = 4;
const CONTROL_SEGMENTS: u64 = 2;

/// Free segments below which new records stop: a sixteenth of the limit.
pub(super) const fn record_reserve(max_segments: u64) -> u64 {
    let sixteenth = max_segments / 16;
    if sixteenth > RECORD_SEGMENTS {
        sixteenth
    } else {
        RECORD_SEGMENTS
    }
}

impl OwnedJournal {
    /// Refuse new operations a nearly full journal must not take. Records
    /// stop with a sixteenth of the segment space left and control requests
    /// with a sixty-fourth, each at least a few segments, so consumers and
    /// writers can still settle. Trims and policy changes always pass: they
    /// free space. The reserve covers followers that need more space for
    /// the same operations; one that still fills holds.
    pub(super) fn check_room(&mut self, buffer: &AppendBuffer) -> Result<(), JournalError> {
        let journal = self.journal.readable()?;
        let segment = journal.writer().header().capacity();
        let max = self.limits.metadata.max_segments as u64;
        let total = segment.saturating_mul(max);
        let records = segment.saturating_mul(record_reserve(max));
        let control = segment.saturating_mul((max / 64).max(CONTROL_SEGMENTS));
        let reserve = buffer
            .operations()
            .map(|operation| match operation.kind {
                OperationKind::Trim
                | OperationKind::PartitionPolicy
                | OperationKind::ProducerResultFloor
                | OperationKind::RetireProducer
                | OperationKind::DedupWatermark
                | OperationKind::GroupDelete => 0,
                OperationKind::CreatePartition
                | OperationKind::OpenProducer
                | OperationKind::Progress
                | OperationKind::Assign
                | OperationKind::GroupPlan => control,
                OperationKind::Append | OperationKind::ResolvedAppend | OperationKind::Barrier => {
                    records
                }
            })
            .max()
            .unwrap_or(0);
        let rolls = max.saturating_sub(journal.manifest().segments.len() as u64);
        let active = segment.saturating_sub(journal.writer().written_position().end_offset());
        let room = rolls
            .saturating_mul(segment)
            .saturating_add(active)
            .saturating_sub(self.writeback.queued_bytes() as u64);
        let refusing = room < records;
        if refusing && !self.room.refusing {
            crate::profiling::log(
                crate::profiling::Level::Warn,
                &format!(
                    "replica journal nearly full: {room} of {total} bytes left; refusing new records"
                ),
            );
        }
        self.room.refusing = refusing;
        if room < reserve {
            return Err(JournalError::RollCapacity);
        }
        Ok(())
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
        if let JournalOwner::Writing(pipeline) = &self.journal
            && pipeline.pending() == 0
            && !pipeline.sync_pending()
        {
            self.journal.finish_writes()?;
        }
        Ok(())
    }

    /// Whether writes still hold: a segment must be freed before the next
    /// roll.
    pub(super) fn writes_held(&self) -> Result<bool, JournalError> {
        Ok(self.room.holding && !self.can_roll()?)
    }

    /// A roll installed or a segment was freed; writes no longer hold.
    pub(super) fn release_writes(&mut self) {
        self.room.holding = false;
    }
}
