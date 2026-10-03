//! Owned bounded record ranges for reads detached from a mutable journal.

use super::{
    ActiveReadIndex, ActiveReadIndexError, ActiveReadRun, Offset, OffsetIndexEntry,
    PartitionIncarnation,
};
use smallvec::SmallVec;

/// Exact selected ranges only. No borrowed index, payloads, or later appends.
#[derive(Debug, Clone)]
pub(crate) struct CapturedReadEntries {
    partition: PartitionIncarnation,
    runs: SmallVec<[ActiveReadRun; 2]>,
}

impl CapturedReadEntries {
    pub(crate) fn ranges(self) -> impl Iterator<Item = (OffsetIndexEntry, usize)> {
        self.runs.into_iter().map(move |run| {
            (
                run.entry(self.partition, run.first_offset)
                    .expect("captured record range"),
                run.records as usize,
            )
        })
    }

    pub(crate) fn operations(&self) -> impl Iterator<Item = crate::OperationLocation> + '_ {
        self.runs.iter().map(|run| run.operation)
    }

    pub(crate) fn entries(self) -> impl Iterator<Item = OffsetIndexEntry> {
        self.runs.into_iter().flat_map(move |run| {
            (0..run.records).map(move |index| {
                // Capture checked both endpoints and the exact first selector.
                run.entry(
                    self.partition,
                    Offset::new(run.first_offset.get() + u64::from(index)),
                )
                .expect("captured record range")
            })
        })
    }
}

impl ActiveReadIndex {
    /// Copy ranges, not individual index entries. The count and applied operation
    /// boundary are fixed now; subsequent appends cannot enlarge this read.
    pub(crate) fn capture(
        &self,
        partition: PartitionIncarnation,
        start: Offset,
        end: Offset,
        max_records: usize,
        through_operation: u64,
    ) -> Result<CapturedReadEntries, ActiveReadIndexError> {
        let mut captured = CapturedReadEntries {
            partition,
            runs: SmallVec::new(),
        };
        let count = end
            .get()
            .saturating_sub(start.get())
            .min(u64::try_from(max_records).unwrap_or(u64::MAX));
        let end = start.get() + count;
        let mut next = start;
        for run in self.runs_from(partition, start) {
            if next.get() == end {
                break;
            }
            let first = run
                .entry(partition, next)
                .ok_or(ActiveReadIndexError::MissingOffset(next))?;
            if run.operation.op_number > through_operation {
                return Ok(captured);
            }
            let stop = run
                .end_offset()
                .ok_or(ActiveReadIndexError::PositionOverflow)?
                .get()
                .min(end);
            let records = u32::try_from(stop - next.get())
                .map_err(|_| ActiveReadIndexError::PositionOverflow)?;
            first
                .location
                .record_index
                .checked_add(records - 1)
                .ok_or(ActiveReadIndexError::PositionOverflow)?;
            captured.runs.push(ActiveReadRun {
                first_offset: next,
                records,
                first_record_index: first.location.record_index,
                ..*run
            });
            next = Offset::new(stop);
        }
        if next.get() != end {
            return Err(ActiveReadIndexError::MissingOffset(next));
        }
        Ok(captured)
    }
}

#[cfg(test)]
mod tests;
