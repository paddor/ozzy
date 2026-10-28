//! Compact incremental offset index for the live segment read path.

use ahash::AHashMap as HashMap;
use ozzy_journal::operation::OperationKind;

mod body;
pub(crate) use body::{ReadBatch, ReadIndexBody};
use ozzy_proto::{Offset, PartitionIncarnation};
use thiserror::Error;

use crate::{
    ActiveSegmentIndex, INDEX_HEADER_BYTES, IndexLimits, IndexSource, MESSAGE_INDEX_ENTRY_BYTES,
    OFFSET_INDEX_ENTRY_BYTES, OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES,
    OffsetIndexEntry, OperationLocation, RecordLocation,
};

mod capture;
pub(crate) use capture::CapturedReadEntries;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveReadRun {
    first_offset: Offset,
    records: u32,
    operation: OperationLocation,
    batch_index: u32,
    first_record_index: u32,
    append_timestamp_millis: u64,
}

impl ActiveReadRun {
    fn end_offset(self) -> Option<Offset> {
        self.first_offset
            .get()
            .checked_add(u64::from(self.records))
            .map(Offset::new)
    }

    fn entry(self, partition: PartitionIncarnation, offset: Offset) -> Option<OffsetIndexEntry> {
        let delta = offset.get().checked_sub(self.first_offset.get())?;
        if delta >= u64::from(self.records) {
            return None;
        }
        let delta = u32::try_from(delta).ok()?;
        self.first_record_index.checked_add(delta)?;
        Some(self.entry_at(partition, delta))
    }

    /// The entry `delta` records into this run; `delta` is below `records`,
    /// whose record indexes the run checked when it was built.
    fn entry_at(self, partition: PartitionIncarnation, delta: u32) -> OffsetIndexEntry {
        let record_index = self.first_record_index + delta;
        OffsetIndexEntry {
            partition,
            offset: Offset::new(self.first_offset.get() + u64::from(delta)),
            append_timestamp_millis: if record_index == 0 {
                self.append_timestamp_millis
            } else {
                0
            },
            location: RecordLocation {
                operation: self.operation,
                batch_index: self.batch_index,
                record_index,
            },
        }
    }
}

/// Record-run index updated after each successful active-segment append.
#[derive(Debug)]
pub(crate) struct ActiveReadIndex {
    source: IndexSource,
    partitions: HashMap<PartitionIncarnation, Vec<ActiveReadRun>>,
    records: usize,
    operations: usize,
    locations: usize,
}

impl ActiveReadIndex {
    /// Record end and newest append time of every partition with records.
    pub(crate) fn partition_ends(
        &self,
    ) -> impl Iterator<Item = (PartitionIncarnation, Offset, u64)> {
        self.partitions.iter().filter_map(|(partition, runs)| {
            Some((
                *partition,
                runs.last()?.end_offset()?,
                runs.iter().map(|run| run.append_timestamp_millis).max()?,
            ))
        })
    }

    pub(crate) fn newest_append_millis(&self, partition: PartitionIncarnation) -> Option<u64> {
        self.partitions
            .get(&partition)?
            .iter()
            .map(|run| run.append_timestamp_millis)
            .max()
    }

    pub(crate) async fn from_persisted_async(
        persisted: &crate::SegmentIndex,
    ) -> Result<Self, ActiveReadIndexError> {
        let mut index = Self {
            source: persisted.source(),
            partitions: HashMap::new(),
            records: 0,
            operations: persisted.operation_count(),
            locations: persisted.location_count(),
        };
        let mut budget = crate::cooperative::Budget::default();
        for entry in persisted.offsets() {
            index.push_entry(entry)?;
            budget.charge(OFFSET_INDEX_ENTRY_BYTES).await;
        }
        Ok(index)
    }

    /// Conservative allocated-size charge, including spare run capacity and
    /// hash buckets/control bytes. `HashMap`'s usable capacity omits empty slots;
    /// twice the bucket footprint covers those plus alignment/control overhead.
    pub(crate) fn retained_bytes(&self) -> usize {
        let bucket = size_of::<(PartitionIncarnation, Vec<ActiveReadRun>)>() + 1;
        size_of::<Self>()
            .saturating_add(16)
            .saturating_add(self.partitions.capacity().saturating_mul(2 * bucket))
            .saturating_add(self.partitions.values().fold(0_usize, |total, runs| {
                total.saturating_add(runs.capacity().saturating_mul(size_of::<ActiveReadRun>()))
            }))
    }

    pub(crate) async fn from_snapshot_async(
        active: &ActiveSegmentIndex,
    ) -> Result<Self, ActiveReadIndexError> {
        let mut index = Self {
            source: active.source(),
            partitions: HashMap::new(),
            records: 0,
            operations: active.operation_count(),
            locations: active.location_count(),
        };
        let mut budget = crate::cooperative::Budget::default();
        for entry in active.offsets() {
            index.push_entry(*entry)?;
            budget.charge(OFFSET_INDEX_ENTRY_BYTES).await;
        }
        Ok(index)
    }

    pub(crate) fn from_group(
        source: IndexSource,
        locations: &[OperationLocation],
        bodies: &[impl ReadIndexBody],
        limits: IndexLimits,
    ) -> Result<Self, ActiveReadIndexError> {
        let mut index = Self {
            source,
            partitions: HashMap::new(),
            records: 0,
            operations: 0,
            locations: 0,
        };
        Self::validate_group_locations(None, source, locations, bodies)?;
        index.extend_bodies(locations, bodies, limits)?;
        Ok(index)
    }

    pub(crate) fn append_group(
        &mut self,
        source: IndexSource,
        locations: &[OperationLocation],
        bodies: &[impl ReadIndexBody],
        limits: IndexLimits,
    ) -> Result<(), ActiveReadIndexError> {
        Self::validate_group_locations(Some(self.source), source, locations, bodies)?;
        self.extend_bodies(locations, bodies, limits)?;
        self.source = source;
        Ok(())
    }

    pub(crate) const fn source(&self) -> IndexSource {
        self.source
    }

    pub(crate) fn covers(&self, partition: PartitionIncarnation, start: Offset) -> bool {
        self.first_offset(partition)
            .is_some_and(|first| first <= start)
    }

    pub(crate) fn first_offset(&self, partition: PartitionIncarnation) -> Option<Offset> {
        self.partitions
            .get(&partition)
            .and_then(|runs| runs.first())
            .map(|run| run.first_offset)
    }

    pub(crate) fn end_offset(&self, partition: PartitionIncarnation) -> Option<Offset> {
        self.partitions
            .get(&partition)
            .and_then(|runs| runs.last())
            .and_then(|run| run.end_offset())
    }

    pub(crate) fn entry(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Result<OffsetIndexEntry, ActiveReadIndexError> {
        let runs = self
            .partitions
            .get(&partition)
            .ok_or(ActiveReadIndexError::MissingOffset(offset))?;
        let index = runs.partition_point(|run| run.first_offset <= offset);
        index
            .checked_sub(1)
            .and_then(|index| runs[index].entry(partition, offset))
            .ok_or(ActiveReadIndexError::MissingOffset(offset))
    }

    /// Resolve one partition/run, then walk consecutive offsets. A bounded
    /// batch must not repeat the hash lookup and binary search for every record.
    #[cfg(test)]
    pub(crate) fn entries(
        &self,
        partition: PartitionIncarnation,
        start: Offset,
        end: Offset,
    ) -> impl Iterator<Item = Result<OffsetIndexEntry, ActiveReadIndexError>> + '_ {
        let mut runs = self.runs_from(partition, start);
        (start.get()..end.get()).map(move |offset| {
            let offset = Offset::new(offset);
            while runs.get(1).is_some_and(|run| run.first_offset <= offset) {
                runs = &runs[1..];
            }
            runs.first()
                .and_then(|run| run.entry(partition, offset))
                .ok_or(ActiveReadIndexError::MissingOffset(offset))
        })
    }

    fn runs_from(&self, partition: PartitionIncarnation, start: Offset) -> &[ActiveReadRun] {
        let runs = self
            .partitions
            .get(&partition)
            .map_or(&[][..], Vec::as_slice);
        let first = runs
            .partition_point(|run| run.first_offset <= start)
            .saturating_sub(1);
        &runs[first..]
    }

    fn validate_group_locations(
        previous: Option<IndexSource>,
        source: IndexSource,
        locations: &[OperationLocation],
        bodies: &[impl ReadIndexBody],
    ) -> Result<(), ActiveReadIndexError> {
        let Some(last) = locations.last() else {
            return Err(ActiveReadIndexError::GroupMismatch);
        };
        if locations.len() != bodies.len() {
            return Err(ActiveReadIndexError::GroupMismatch);
        }
        if let Some(previous) = previous
            && (source.group_id != previous.group_id
                || source.segment_id != previous.segment_id
                || source.first_op_number != previous.first_op_number
                || source.valid_bytes <= previous.valid_bytes
                || source.last_op_number <= previous.last_op_number)
        {
            return Err(ActiveReadIndexError::SourceMismatch);
        }
        let expected_first = previous.map_or(Ok(source.first_op_number), |previous| {
            previous
                .last_op_number
                .checked_add(1)
                .ok_or(ActiveReadIndexError::PositionOverflow)
        })?;
        for (index, location) in locations.iter().enumerate() {
            let delta = u64::try_from(index).map_err(|_| ActiveReadIndexError::PositionOverflow)?;
            let expected = expected_first
                .checked_add(delta)
                .ok_or(ActiveReadIndexError::PositionOverflow)?;
            if location.segment_id != source.segment_id || location.op_number != expected {
                return Err(ActiveReadIndexError::SourceMismatch);
            }
        }
        if last.op_number != source.last_op_number
            || last.operation_digest != source.last_operation_digest
        {
            return Err(ActiveReadIndexError::SourceMismatch);
        }
        Ok(())
    }

    pub(crate) fn capacity_after(
        previous: Option<&Self>,
        bodies: &[impl ReadIndexBody],
        limits: IndexLimits,
    ) -> Result<(usize, usize, usize), ActiveReadIndexError> {
        let additional = bodies.iter().try_fold(0_usize, |count, body| {
            body.batches()
                .try_fold(count, |count, batch| count.checked_add(batch.records))
        });
        let actual = additional.and_then(|additional| {
            previous
                .map_or(0, |index| index.records)
                .checked_add(additional)
        });
        let Some(actual) = actual else {
            return Err(ActiveReadIndexError::PositionOverflow);
        };
        let additional_operations = bodies
            .iter()
            .filter(|body| {
                !matches!(
                    body.kind(),
                    OperationKind::CreatePartition
                        | OperationKind::Append
                        | OperationKind::ResolvedAppend
                        | OperationKind::Release
                )
            })
            .count();
        let operations = previous
            .map_or(0, |index| index.operations)
            .checked_add(additional_operations)
            .ok_or(ActiveReadIndexError::PositionOverflow)?;
        let locations = previous
            .map_or(0, |index| index.locations)
            .checked_add(bodies.len())
            .ok_or(ActiveReadIndexError::PositionOverflow)?;
        let footprint = actual
            .checked_mul(OFFSET_INDEX_ENTRY_BYTES)
            .and_then(|bytes| {
                actual
                    .checked_mul(MESSAGE_INDEX_ENTRY_BYTES)
                    .and_then(|section| bytes.checked_add(section))
            })
            .and_then(|bytes| {
                operations
                    .checked_mul(OPERATION_INDEX_ENTRY_BYTES)
                    .and_then(|section| bytes.checked_add(section))
            })
            .and_then(|bytes| {
                locations
                    .checked_mul(OPERATION_LOCATION_BYTES)
                    .and_then(|section| bytes.checked_add(section))
            })
            .and_then(|bytes| bytes.checked_add(INDEX_HEADER_BYTES))
            .ok_or(ActiveReadIndexError::PositionOverflow)?;
        if actual > limits.max_offset_entries
            || actual > limits.max_message_entries
            || operations > limits.max_operation_entries
            || locations > limits.max_operation_entries
            || footprint > limits.max_file_bytes
        {
            return Err(ActiveReadIndexError::LimitExceeded);
        }
        Ok((actual, operations, locations))
    }

    fn extend_bodies(
        &mut self,
        locations: &[OperationLocation],
        bodies: &[impl ReadIndexBody],
        limits: IndexLimits,
    ) -> Result<(), ActiveReadIndexError> {
        let (actual, operations, operation_count) =
            Self::capacity_after(Some(self), bodies, limits)?;
        for (location, body) in locations.iter().zip(bodies) {
            for (batch_index, batch) in body.batches().enumerate() {
                let records = u32::try_from(batch.records)
                    .map_err(|_| ActiveReadIndexError::PositionOverflow)?;
                let batch_index = u32::try_from(batch_index)
                    .map_err(|_| ActiveReadIndexError::PositionOverflow)?;
                self.push_run(
                    batch.partition,
                    ActiveReadRun {
                        first_offset: batch.first_offset,
                        records,
                        operation: *location,
                        batch_index,
                        first_record_index: 0,
                        append_timestamp_millis: batch.append_timestamp_millis,
                    },
                )?;
            }
        }
        self.records = actual;
        self.operations = operations;
        self.locations = operation_count;
        Ok(())
    }

    fn push_entry(&mut self, entry: OffsetIndexEntry) -> Result<(), ActiveReadIndexError> {
        let run = ActiveReadRun {
            first_offset: entry.offset,
            records: 1,
            operation: entry.location.operation,
            batch_index: entry.location.batch_index,
            first_record_index: entry.location.record_index,
            append_timestamp_millis: entry.append_timestamp_millis,
        };
        self.push_run(entry.partition, run)?;
        self.records = self
            .records
            .checked_add(1)
            .ok_or(ActiveReadIndexError::PositionOverflow)?;
        Ok(())
    }

    fn push_run(
        &mut self,
        partition: PartitionIncarnation,
        run: ActiveReadRun,
    ) -> Result<(), ActiveReadIndexError> {
        if run.records == 0 {
            return Err(ActiveReadIndexError::GroupMismatch);
        }
        let runs = self.partitions.entry(partition).or_default();
        if let Some(last) = runs.last_mut() {
            if last.end_offset() != Some(run.first_offset) {
                return Err(ActiveReadIndexError::OffsetDiscontinuity);
            }
            let expected_record = last.first_record_index.checked_add(last.records);
            if last.operation == run.operation
                && last.batch_index == run.batch_index
                && expected_record == Some(run.first_record_index)
            {
                last.records = last
                    .records
                    .checked_add(run.records)
                    .ok_or(ActiveReadIndexError::PositionOverflow)?;
                return Ok(());
            }
        }
        runs.push(run);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum ActiveReadIndexError {
    #[error("active read index group shape does not match its operations")]
    GroupMismatch,
    #[error("active read index source does not extend its current segment")]
    SourceMismatch,
    #[error("active read index offsets are not contiguous")]
    OffsetDiscontinuity,
    #[error("active read index position space exhausted")]
    PositionOverflow,
    #[error("active read index resource limit exceeded")]
    LimitExceeded,
    #[error("active read index is missing offset {0:?}")]
    MissingOffset(Offset),
}

#[cfg(test)]
mod tests;
