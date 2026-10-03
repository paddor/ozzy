//! Compact incremental offset index for the live segment read path.

use ahash::AHashMap as HashMap;
use ozzy_journal::operation::OperationKind;

mod body;
pub(crate) use body::{ReadBatch, ReadIndexBody};
use ozzy_proto::{Offset, PartitionIncarnation};
use thiserror::Error;

use crate::{
    ActiveSegmentIndex, INDEX_HEADER_BYTES, IndexLimits, IndexSource, MESSAGE_INDEX_ENTRY_BYTES,
    OFFSET_INDEX_ENTRY_BYTES, OPERATION_INDEX_ENTRY_BYTES, OffsetIndexEntry, OperationLocation,
    RecordLocation,
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
        let record_index = self
            .first_record_index
            .checked_add(u32::try_from(delta).ok()?)?;
        Some(OffsetIndexEntry {
            partition,
            offset,
            location: RecordLocation {
                operation: self.operation,
                batch_index: self.batch_index,
                record_index,
            },
        })
    }
}

/// Record-run index updated after each successful active-segment append.
#[derive(Debug)]
pub(crate) struct ActiveReadIndex {
    source: IndexSource,
    partitions: HashMap<PartitionIncarnation, Vec<ActiveReadRun>>,
    records: usize,
    operations: usize,
}

impl ActiveReadIndex {
    pub(crate) async fn from_persisted_async(
        persisted: &crate::SegmentIndex,
    ) -> Result<Self, ActiveReadIndexError> {
        let mut index = Self {
            source: persisted.source(),
            partitions: HashMap::new(),
            records: 0,
            operations: persisted.operation_count(),
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

    #[cfg(test)]
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
        if locations.is_empty() || locations.len() != bodies.len() {
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
        let last = locations.last().expect("nonempty checked");
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
    ) -> Result<(usize, usize), ActiveReadIndexError> {
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
                    OperationKind::CreatePartition | OperationKind::Append
                )
            })
            .count();
        let operations = previous
            .map_or(0, |index| index.operations)
            .checked_add(additional_operations)
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
            .and_then(|bytes| bytes.checked_add(INDEX_HEADER_BYTES))
            .ok_or(ActiveReadIndexError::PositionOverflow)?;
        if actual > limits.max_offset_entries
            || actual > limits.max_message_entries
            || operations > limits.max_operation_entries
            || footprint > limits.max_file_bytes
        {
            return Err(ActiveReadIndexError::LimitExceeded);
        }
        Ok((actual, operations))
    }

    fn extend_bodies(
        &mut self,
        locations: &[OperationLocation],
        bodies: &[impl ReadIndexBody],
        limits: IndexLimits,
    ) -> Result<(), ActiveReadIndexError> {
        let (actual, operations) = Self::capacity_after(Some(self), bodies, limits)?;
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
                    },
                )?;
            }
        }
        self.records = actual;
        self.operations = operations;
        Ok(())
    }

    fn push_entry(&mut self, entry: OffsetIndexEntry) -> Result<(), ActiveReadIndexError> {
        let run = ActiveReadRun {
            first_offset: entry.offset,
            records: 1,
            operation: entry.location.operation,
            batch_index: entry.location.batch_index,
            first_record_index: entry.location.record_index,
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
mod tests {
    use ozzy_journal::operation::{Append, AppendBatch, AppendRecord, OperationBody};
    use ozzy_proto::{GroupId, MessageId, OwnerEpoch, ProducerEpoch, ProducerId, ProducerSequence};
    use smallvec::smallvec;

    use super::*;
    use crate::Digest;

    fn source(last: u64, valid_bytes: u64) -> IndexSource {
        IndexSource {
            group_id: GroupId::from_bytes([1; 16]),
            segment_id: 1,
            valid_bytes,
            segment_digest: Digest::from_bytes([last as u8; 32]),
            first_op_number: 1,
            last_op_number: last,
            last_operation_digest: Digest::from_bytes([(last + 10) as u8; 32]),
        }
    }

    fn location(op_number: u64, entry_offset: u64) -> OperationLocation {
        OperationLocation {
            segment_id: 1,
            entry_offset,
            entry_bytes: 4_096,
            op_number,
            operation_digest: Digest::from_bytes([(op_number + 10) as u8; 32]),
        }
    }

    fn append_body(first: u64, records: usize) -> OperationBody<'static> {
        OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: PartitionIncarnation::from_bytes([2; 16]),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([3; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(first),
                first_offset: Offset::new(first),
                append_timestamp_millis: 1,
                records: (0..records)
                    .map(|index| AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([index as u8 + 1; 16]),
                        parts: smallvec![b"x".as_slice()],
                    })
                    .collect(),
            }],
        })
    }

    #[test]
    fn packed_segment_can_exceed_default_index_file_limit() {
        let body = append_body(0, 1);
        let mut index = ActiveReadIndex::from_group(
            source(1, 8192),
            &[location(1, 4096)],
            &[body],
            IndexLimits::default(),
        )
        .unwrap();
        // A 256 MiB canonical body can hold roughly eight million 16-byte
        // records. Compact RAM runs do not shrink their persisted index entries.
        index.records = 256 * 1024 * 1024 / (16 + 17);
        assert_eq!(
            ActiveReadIndex::capacity_after(
                Some(&index),
                &[append_body(1, 1)],
                IndexLimits::default()
            ),
            Err(ActiveReadIndexError::LimitExceeded),
        );
        assert!(
            ActiveReadIndex::capacity_after(
                Some(&index),
                &[append_body(1, 1)],
                IndexLimits {
                    max_file_bytes: 2 * 1024 * 1024 * 1024,
                    ..IndexLimits::default()
                }
            )
            .is_ok()
        );
    }

    #[test]
    fn incremental_runs_resolve_exact_record_selectors() {
        let partition = PartitionIncarnation::from_bytes([2; 16]);
        let mut index = ActiveReadIndex::from_group(
            source(1, 8_192),
            &[location(1, 4_096)],
            &[append_body(0, 2)],
            IndexLimits::default(),
        )
        .unwrap();
        index
            .append_group(
                source(2, 12_288),
                &[location(2, 8_192)],
                &[append_body(2, 1)],
                IndexLimits::default(),
            )
            .unwrap();

        let entries = [0, 1, 2].map(|offset| index.entry(partition, Offset::new(offset)).unwrap());
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].location.record_index, 0);
        assert_eq!(entries[1].location.record_index, 1);
        assert_eq!(entries[2].location.operation.op_number, 2);
        assert_eq!(entries[2].location.record_index, 0);
    }

    #[test]
    fn sequential_entries_match_point_lookups_at_every_range_boundary() {
        let partition = PartitionIncarnation::from_bytes([2; 16]);
        let mut index = ActiveReadIndex::from_group(
            source(1, 8192),
            &[location(1, 4096)],
            &[append_body(10, 3)],
            IndexLimits::default(),
        )
        .unwrap();
        index
            .append_group(
                source(2, 12288),
                &[location(2, 8192)],
                &[append_body(13, 2)],
                IndexLimits::default(),
            )
            .unwrap();
        for key in [partition, PartitionIncarnation::new()] {
            for start in 9..=16 {
                for end in start..=17 {
                    for count in 0..=8 {
                        let expected = (start..end)
                            .take(count)
                            .map(|offset| index.entry(key, Offset::new(offset)))
                            .collect::<Vec<_>>();
                        assert_eq!(
                            index
                                .entries(key, Offset::new(start), Offset::new(end))
                                .take(count)
                                .collect::<Vec<_>>(),
                            expected
                        );
                    }
                }
            }
        }
        let maximum = Offset::new(u64::MAX);
        assert_eq!(index.entries(partition, maximum, maximum).count(), 0);
    }
}
