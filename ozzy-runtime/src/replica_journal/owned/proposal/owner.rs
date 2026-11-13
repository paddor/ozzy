//! The leader admits an owner batch only at the partition's next offset,
//! counting every accepted operation. A refused batch takes no offset, so
//! the writer's later batches fail too.

use super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::{ClockReading, OwnerAppendError};

impl OwnedJournal {
    /// Check the owner batch's first offset against the accepted image and
    /// stamp the partition's owner epoch and the leader's wall time.
    pub(super) fn owner_proposal(
        &self,
        buffer: &mut AppendBuffer,
        clock: ClockReading,
    ) -> Result<(), JournalError> {
        let (partition, first_offset) = buffer.owner_target()?;
        let state = self
            .images()?
            .speculative()
            .partition(partition)
            .ok_or(OwnerAppendError::UnknownPartition)?;
        if state.delay.is_some() || state.dedup_window_millis.is_some() {
            return Err(OwnerAppendError::Disabled.into());
        }
        if first_offset != state.next_offset {
            return Err(OwnerAppendError::Offset {
                next: state.next_offset,
            }
            .into());
        }
        buffer.place_owner(state.owner_epoch, clock.wall_millis)
    }
}
