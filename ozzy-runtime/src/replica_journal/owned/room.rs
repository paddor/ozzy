//! Segment room. The segment limit bounds every replica's journal. A nearly
//! full journal refuses new client work with a capacity rejection, and a full
//! one holds its writes until retirement frees a segment. Neither faults.

use super::{AppendBuffer, JournalError, OwnedJournal};
use ozzy_journal::operation::OperationKind;

/// Whether a refusal or a write hold is in effect; each start logs once.
#[derive(Debug, Default)]
pub(super) struct Room {
    refusing: bool,
    holding: bool,
}

impl OwnedJournal {
    /// Refuse new operations a nearly full journal must not take. Records
    /// stop with a sixteenth of the segment space left and control requests
    /// with a sixty-fourth, so consumers and writers can still settle. Trims
    /// and policy changes always pass: they free space. Followers group the
    /// same operations on their own timing and may need more space. The
    /// margin covers some of that; a follower that still fills holds.
    pub(super) fn check_room(&mut self, buffer: &AppendBuffer) -> Result<(), JournalError> {
        let journal = self.journal.readable()?;
        let segment = journal.writer().header().capacity();
        let max = self.limits.metadata.max_segments as u64;
        let total = segment.saturating_mul(max);
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
                | OperationKind::GroupPlan => total / 64,
                OperationKind::Append | OperationKind::ResolvedAppend | OperationKind::Barrier => {
                    total / 16
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
        let refusing = room < total / 16;
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

    /// Hold writes at the segment limit instead of rolling.
    pub(super) fn hold_writes(&mut self) {
        if !std::mem::replace(&mut self.room.holding, true) {
            crate::profiling::log(
                crate::profiling::Level::Warn,
                "replica journal full: holding writes until a segment retires",
            );
        }
    }

    /// A roll installed; writes no longer hold.
    pub(super) fn release_writes(&mut self) {
        self.room.holding = false;
    }
}
