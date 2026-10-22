//! Record reads by offset, offset range, and message identity.

use super::super::{
    IndexedMessageLocation, IndexedOffsetLocation, IndexedReadError, IndexedRecord,
    JournalIndexError as Error, LogPosition, MessageId, Offset, PartitionIncarnation, ReadLimits,
};
use super::Snapshot;

impl Snapshot {
    /// Read and validate one record at an exact partition-global offset.
    pub async fn read_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Result<Option<IndexedRecord>, Error> {
        let Some(location) = self.offset_location(partition, offset).await? else {
            return Ok(None);
        };
        Ok(Some(self.read_location(location).await?))
    }

    /// Read one exact record together with its canonical operation coordinate.
    pub async fn read_offset_with_position(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Result<Option<(IndexedRecord, LogPosition)>, Error> {
        let Some(location) = self.offset_location(partition, offset).await? else {
            return Ok(None);
        };
        let operation = location.entry.location.operation;
        Ok(Some((
            self.read_location(location).await?,
            LogPosition {
                op_number: operation.op_number,
                digest: operation.operation_digest,
            },
        )))
    }

    async fn offset_location(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Result<Option<IndexedOffsetLocation>, Error> {
        if let Some(active) = &self.active
            && let Some(entry) = active.find_offset(partition, offset)
            && entry.location.operation.op_number <= self.through.op_number
        {
            return Ok(Some(IndexedOffsetLocation {
                source: active.source(),
                entry,
            }));
        }
        Ok(self
            .sealed
            .find_offset(partition, offset, self.through.op_number)
            .await?)
    }

    /// `end` is exclusive and comes from the matching canonical state. A hole
    /// before it is an error, not an implicit end of stream.
    pub async fn read_range(
        &self,
        partition: PartitionIncarnation,
        start: Offset,
        end: Offset,
        limits: ReadLimits,
    ) -> Result<Vec<IndexedRecord>, Error> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(Error::InvalidReadLimits);
        }
        let mut next = start;
        let mut locations = Vec::new();
        while next < end && locations.len() < limits.max_records {
            let location = self
                .offset_location(partition, next)
                .await?
                .ok_or(Error::MissingOffset(next))?;
            locations.push(location);
            next = next.checked_next().ok_or(Error::OffsetExhausted)?;
        }
        self.read_locations(&locations, limits, |record, _| record)
            .await
    }

    /// Read selected offsets in caller order, retaining each exact operation
    /// position. Count and byte limits bound the returned prefix. Noncontiguous
    /// offsets in one operation share its authoritative read and decoding.
    pub async fn read_offsets_with_positions(
        &self,
        partition: PartitionIncarnation,
        offsets: &[Offset],
        limits: ReadLimits,
    ) -> Result<Vec<(IndexedRecord, LogPosition)>, Error> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(Error::InvalidReadLimits);
        }
        let mut locations = Vec::with_capacity(offsets.len().min(limits.max_records));
        for &offset in offsets.iter().take(limits.max_records) {
            locations.push(
                self.offset_location(partition, offset)
                    .await?
                    .ok_or(Error::MissingOffset(offset))?,
            );
        }
        self.read_locations(&locations, limits, |record, operation| {
            (
                record,
                LogPosition {
                    op_number: operation.op_number,
                    digest: operation.operation_digest,
                },
            )
        })
        .await
    }

    async fn read_locations<T>(
        &self,
        locations: &[IndexedOffsetLocation],
        limits: ReadLimits,
        mut project: impl FnMut(IndexedRecord, crate::OperationLocation) -> T,
    ) -> Result<Vec<T>, Error> {
        let mut records = Vec::with_capacity(locations.len());
        let mut payload_bytes = 0usize;
        let mut first = 0;
        while first < locations.len() {
            let location = locations[first];
            let mut end = first + 1;
            while end < locations.len()
                && locations[end].source == location.source
                && locations[end].entry.location.operation == location.entry.location.operation
            {
                end += 1;
            }
            let entries = locations[first..end]
                .iter()
                .map(|location| location.entry)
                .collect::<Vec<_>>();
            let decoded = self.reader(location.source)?.records(&entries).await?;
            for record in decoded {
                let bytes = record.payload_bytes();
                if records.is_empty() && bytes > limits.max_bytes {
                    return Err(Error::RecordExceedsReadLimit {
                        actual: bytes,
                        limit: limits.max_bytes,
                    });
                }
                if bytes > limits.max_bytes - payload_bytes {
                    return Ok(records);
                }
                records.push(project(record, location.entry.location.operation));
                payload_bytes += bytes;
            }
            first = end;
        }
        Ok(records)
    }

    /// Read and validate one record by exact partition and message identity.
    pub async fn read_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Result<Option<IndexedRecord>, Error> {
        let location = match self.active_message(partition, message_id)? {
            Some(location) => Some(location),
            None => {
                self.sealed
                    .find_message(partition, message_id, self.through.op_number)
                    .await?
            }
        };
        let Some(location) = location else {
            return Ok(None);
        };
        if location.message.partition != location.offset.partition
            || location.message.offset != location.offset.offset
        {
            return Err(IndexedReadError::RecordMismatch.into());
        }
        let record = self
            .read_location(IndexedOffsetLocation {
                source: location.source,
                entry: location.offset,
            })
            .await?;
        if record.message_id != location.message.message_id
            || location.offset.location.operation.operation_digest
                != location.message.operation_digest
        {
            return Err(IndexedReadError::RecordMismatch.into());
        }
        Ok(Some(record))
    }

    /// The message's location in the active index, when this capture sees it.
    fn active_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Result<Option<IndexedMessageLocation>, Error> {
        let Some(active) = &self.active else {
            return Ok(None);
        };
        let Some(message) = active.find_message(partition, message_id) else {
            return Ok(None);
        };
        let offset = active
            .find_offset(partition, message.offset)
            .ok_or(Error::InconsistentActiveIndex)?;
        Ok(
            (offset.location.operation.op_number <= self.through.op_number).then_some(
                IndexedMessageLocation {
                    source: active.source(),
                    message,
                    offset,
                },
            ),
        )
    }

    async fn read_location(&self, location: IndexedOffsetLocation) -> Result<IndexedRecord, Error> {
        Ok(self.reader(location.source)?.record(location.entry).await?)
    }
}
