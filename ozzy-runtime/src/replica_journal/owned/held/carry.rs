//! Held rows across a `Carry`. A carry moves its records to rows of the
//! `Carry` operation, and rows before the carry floor are gone. Held rows of
//! a position range let a copy check a `Carry`.

use std::collections::BTreeMap;

use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::{Carry, HeldPosition, ReleaseKey};
use ozzy_journal_segment::JournalIndexSnapshot;
use ozzy_proto::PartitionIncarnation;

use super::{Held, JournalError};

/// Held positions from the first up to the second.
pub(in crate::replica_journal::owned) type Span = (HeldPosition, HeldPosition);

/// One bounded read of the held rows at positions in a span.
#[derive(Debug)]
pub(in crate::replica_journal::owned) struct Between {
    partition: PartitionIncarnation,
    released: Option<ReleaseKey>,
    range: Span,
    limit: usize,
    /// Rows of the active segment and later operations in the range.
    recent: Vec<(ReleaseKey, PendingSchedule)>,
    snapshot: JournalIndexSnapshot,
}

impl Held {
    /// Move a carry's records to its rows. Every row before its end is
    /// gone, so segments wholly before it hold none.
    pub(super) fn carry(&mut self, operation: u64, carry: &Carry<'_>) -> Result<(), JournalError> {
        let entries = ozzy_core::state::carry_entries(operation, carry)
            .map_err(|_| JournalError::AppendMismatch)?;
        for (key, record) in entries {
            // The earlier row is loaded, in memory, or in its index file.
            self.sealed.remove(&key);
            self.schedule(key, record);
        }
        self.carried = carry.through;
        self.recent_operations = self.recent_operations.split_off(&carry.through.operation);
        self.segments
            .retain(|segment| segment.last >= carry.through.operation);
        self.settle();
        Ok(())
    }

    /// Read at most `limit` held rows at positions in `range`, in position
    /// order: sealed ones from `snapshot`, later ones from memory.
    pub(in crate::replica_journal::owned) fn between(
        &self,
        snapshot: JournalIndexSnapshot,
        range: Span,
        limit: usize,
    ) -> Between {
        let (from, through) = range;
        let mut recent = Vec::new();
        if through.operation >= self.active {
            recent.extend(
                self.recent
                    .iter()
                    .filter(|(_, row)| (from..through).contains(&row.position()))
                    .map(|(key, row)| (*key, *row)),
            );
        }
        Between {
            partition: self.partition,
            released: self.released,
            range,
            limit,
            recent,
            snapshot,
        }
    }
}

impl Between {
    pub(in crate::replica_journal::owned) async fn read(
        self,
    ) -> Result<Vec<(ReleaseKey, PendingSchedule)>, JournalError> {
        let mut rows: BTreeMap<HeldPosition, (ReleaseKey, PendingSchedule)> = self
            .recent
            .into_iter()
            .map(|(key, row)| (row.position(), (key, row)))
            .collect();
        let sealed = self
            .snapshot
            .sealed_schedules_between(self.partition, self.released, self.range, self.limit)
            .await?;
        rows.extend(
            sealed
                .into_iter()
                .map(|entry| (entry.record.position(), (entry.key, entry.record))),
        );
        Ok(rows.into_values().take(self.limit).collect())
    }
}
