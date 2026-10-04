//! Exact, deletion-protected snapshot. Every cold lookup and payload read uses
//! file jobs. The active prefix is frozen before releasing the journal borrow.

use std::{path::PathBuf, rc::Rc};

use super::{
    ActiveSegmentIndex, DecodeLimits, IndexedMessageLocation, IndexedOffsetLocation,
    IndexedOperationLocation, IndexedReadError, IndexedRecord, JournalIndexError as Error,
    LogPosition, MessageId, Offset, OperationId, OperationLimits, PartitionIncarnation, ReadLimits,
    TailState, scan_segment,
};
use crate::{
    GroupIdentity, SegmentReference, async_files::Access, index_catalog::asynchronous::Catalog,
    retention::PreparedSegmentLease,
};
use ozzy_core::state::{IdentityClaim, IdentityKey};
use ozzy_io::{OpenMode, Operation};

#[derive(Debug, Clone)]
/// Exact captured journal index and deletion protection for bounded reads.
pub struct Snapshot {
    pub(crate) access: Access,
    pub(crate) root: PathBuf,
    pub(crate) references: Vec<SegmentReference>,
    pub(crate) identity: GroupIdentity,
    pub(crate) sealed: Catalog,
    pub(crate) active: Option<Rc<ActiveSegmentIndex>>,
    pub(crate) active_bytes: u64,
    pub(crate) active_digest: crate::Digest,
    pub(crate) through: LogPosition,
    pub(crate) decode: DecodeLimits,
    pub(crate) operations: OperationLimits,
    pub(crate) chunk: usize,
    pub(crate) _leases: Rc<[PreparedSegmentLease]>,
}

impl Snapshot {
    /// Scan metadata across every captured source, independent of hot-cache order.
    /// Caller-held deletion pins preserve exact files throughout this lookup.
    pub async fn seek(
        &self,
        mut query: crate::SeekQuery,
        result: &mut ozzy_core::reader::seek::Selection,
    ) -> Result<(), Error> {
        query.through = query.through.min(self.through.op_number);
        self.sealed.seek(query, result).await?;
        if let Some(active) = &self.active {
            active.seek(query, result);
        }
        Ok(())
    }

    /// Exact group, node, volume, store, and store-generation binding.
    pub const fn identity(&self) -> GroupIdentity {
        self.identity
    }
    /// Exact upper canonical operation prefix visible through this capture.
    pub const fn through(&self) -> LogPosition {
        self.through
    }
    /// Exact segment references protected by this captured journal view.
    pub fn segment_references(&self) -> &[SegmentReference] {
        &self.references
    }

    /// Check exact lineage, never just an operation-number comparison. Read
    /// only the captured prefix, so later appends or their incomplete tail do
    /// not change an older snapshot's answer.
    pub async fn contains_position(&self, position: LogPosition) -> Result<bool, Error> {
        if position == LogPosition::GENESIS {
            return Ok(true);
        }
        if position.op_number > self.through.op_number {
            return Ok(false);
        }
        if position.op_number == self.through.op_number {
            return Ok(position.digest == self.through.digest);
        }
        let Some(reference) = self
            .references
            .iter()
            .rev()
            .find(|r| r.first_chain.next_op_number() <= position.op_number)
        else {
            return Ok(false);
        };
        let (bytes, digest) = reference
            .sealed
            .map_or((self.active_bytes, self.active_digest), |sealed| {
                (sealed.valid_bytes, sealed.digest)
            });
        let handle = self
            .access
            .open(
                self.segment_path(reference.segment_id)?,
                OpenMode::Read,
                false,
                false,
            )
            .await?;
        let length = self.access.length(&handle).await?;
        if length > reference.capacity || length < bytes {
            return Err(Error::LineageSegmentMismatch(reference.segment_id));
        }
        let count = usize::try_from(bytes)
            .map_err(|_| Error::LineageSegmentMismatch(reference.segment_id))?;
        let image = self
            .access
            .read_range(&handle, 0, count, self.chunk)
            .await?;
        self.access.done(Operation::Close { handle }).await?;
        let scan = scan_segment(
            &image,
            reference.first_group_number,
            reference.first_chain,
            self.decode,
        )?;
        if scan.header.group_id() != self.identity.group_id
            || scan.header.segment_id() != reference.segment_id
            || scan.header.capacity() != reference.capacity
            || scan.valid_bytes != bytes
            || scan.digest != digest
            || matches!(scan.tail, TailState::Truncated { .. })
        {
            return Err(Error::LineageSegmentMismatch(reference.segment_id));
        }
        Ok(scan
            .groups
            .iter()
            .flat_map(|group| &group.operations)
            .find(|operation| operation.op_number == position.op_number)
            .is_some_and(|operation| operation.digest == position.digest))
    }

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
            let decoded = crate::reader::asynchronous::read_records(
                &self.access,
                self.segment_path(location.source.segment_id)?,
                location.source,
                &entries,
                self.decode,
                self.operations,
                self.chunk,
            )
            .await?;
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
        let active = self.active.as_ref().and_then(|active| {
            active
                .find_message(partition, message_id)
                .map(|message| (active, message))
        });
        let active_location = if let Some((active, message)) = active {
            let offset = active
                .find_offset(partition, message.offset)
                .ok_or(Error::InconsistentActiveIndex)?;
            (offset.location.operation.op_number <= self.through.op_number).then_some(
                IndexedMessageLocation {
                    source: active.source(),
                    message,
                    offset,
                },
            )
        } else {
            None
        };
        let location = if active_location.is_some() {
            active_location
        } else {
            self.sealed
                .find_message(partition, message_id, self.through.op_number)
                .await?
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

    /// Look up an exact canonical control-operation identity.
    pub async fn find_operation(
        &self,
        id: OperationId,
    ) -> Result<Option<IndexedOperationLocation>, Error> {
        self.find_operation_through(id, self.through.op_number)
            .await
    }

    /// Read an exact control operation by retry ID, verifying its canonical
    /// digest and operation identity against the captured index. The snapshot
    /// keeps each referenced segment protected until this read settles.
    pub async fn read_operation(
        &self,
        id: OperationId,
    ) -> Result<Option<crate::DecodedOperation<'static>>, Error> {
        let Some(location) = self.find_operation(id).await? else {
            return Ok(None);
        };
        Ok(Some(
            crate::reader::asynchronous::read_control(
                &self.access,
                self.segment_path(location.source.segment_id)?,
                location.source,
                location.entry,
                self.decode,
                self.operations,
                self.chunk,
            )
            .await?,
        ))
    }

    pub(crate) async fn find_operation_through(
        &self,
        id: OperationId,
        through: u64,
    ) -> Result<Option<IndexedOperationLocation>, Error> {
        if through > self.through.op_number {
            return Err(Error::BoundaryBeyondSnapshot {
                requested: through,
                available: self.through.op_number,
            });
        }
        if let Some(active) = &self.active
            && let Some(entry) = active.find_operation(id)
            && entry.location.op_number <= through
        {
            return Ok(Some(IndexedOperationLocation {
                source: active.source(),
                entry,
            }));
        }
        Ok(self.sealed.find_operation(id, through).await?)
    }

    /// Resolve exact persistent control-operation identity without inventing missing evidence.
    pub async fn identity_claim(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, Error> {
        self.identity_claim_through(key, self.through.op_number)
            .await
    }

    pub(crate) async fn identity_claim_through(
        &self,
        key: IdentityKey,
        through: u64,
    ) -> Result<Option<IdentityClaim>, Error> {
        Ok(self
            .find_operation_through(key.operation_id(), through)
            .await?
            .map(|location| IdentityClaim {
                operation_id: key.operation_id(),
                op_number: location.entry.location.op_number,
            }))
    }

    fn segment_path(&self, id: u64) -> Result<PathBuf, Error> {
        self.references
            .iter()
            .find(|r| r.segment_id == id)
            .map(|r| self.root.join(r.file_name()))
            .ok_or(Error::UnpinnedSegment(id))
    }

    async fn read_location(&self, location: IndexedOffsetLocation) -> Result<IndexedRecord, Error> {
        Ok(crate::reader::asynchronous::read_record(
            &self.access,
            self.segment_path(location.source.segment_id)?,
            location.source,
            location.entry,
            self.decode,
            self.operations,
            self.chunk,
        )
        .await?)
    }
}
