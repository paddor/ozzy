//! Stable indexed-read snapshot spanning sealed and active segments.

pub(crate) mod asynchronous;

use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

use ozzy_journal::{ReadLimits, operation::OperationLimits};
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::reader::read_indexed_records;
use crate::{
    ActiveSegmentIndex, DecodeLimits, IndexedMessageLocation, IndexedOffsetLocation,
    IndexedOperationLocation, IndexedReadError, IndexedRecord, LogPosition, SegmentIndexCatalog,
    SegmentPin, TailState, read_indexed_message, read_indexed_record, scan_segment,
};

/// Pinned exact lookup view through one captured journal position.
#[derive(Debug)]
pub struct JournalIndexSnapshot {
    pin: SegmentPin,
    identity: crate::GroupIdentity,
    sealed: SegmentIndexCatalog,
    active: Option<ActiveSegmentIndex>,
    through: LogPosition,
    decode_limits: DecodeLimits,
    operation_limits: OperationLimits,
}

/// Journal boundary captured by one stable lookup snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalIndexBoundary {
    /// Complete local writes, including unsynced speculative records. Not durability.
    Written,
    /// Accepted prefix under the journal's mode; external mode requires local sync.
    Accepted,
    /// Locally published committed prefix, which may lag live quorum commit.
    Committed,
}

impl JournalIndexSnapshot {
    pub(crate) const fn new(
        pin: SegmentPin,
        identity: crate::GroupIdentity,
        sealed: SegmentIndexCatalog,
        active: Option<ActiveSegmentIndex>,
        through: LogPosition,
        decode_limits: DecodeLimits,
        operation_limits: OperationLimits,
    ) -> Self {
        Self {
            pin,
            identity,
            sealed,
            active,
            through,
            decode_limits,
            operation_limits,
        }
    }

    /// Exact upper canonical operation prefix visible through this capture.
    pub const fn through(&self) -> LogPosition {
        self.through
    }

    /// Exact group, node, volume, store, and store-generation binding.
    pub const fn identity(&self) -> crate::GroupIdentity {
        self.identity
    }

    /// Exact segment references protected by this captured journal view.
    pub fn segment_references(&self) -> &[crate::SegmentReference] {
        self.pin.references()
    }

    /// Verify one exact logical position exists in this snapshot's lineage.
    ///
    /// Used before persistent-index handoff. Comparing only tip operation numbers
    /// cannot distinguish an extension from a conflicting replacement suffix.
    pub fn contains_position(&self, position: LogPosition) -> Result<bool, JournalIndexError> {
        if position == LogPosition::GENESIS {
            return Ok(true);
        }
        if position.op_number > self.through.op_number {
            return Ok(false);
        }
        if position.op_number == self.through.op_number {
            return Ok(position.digest == self.through.digest);
        }

        let reference = self
            .pin
            .references()
            .iter()
            .rev()
            .find(|reference| reference.first_chain.next_op_number() <= position.op_number);
        let Some(reference) = reference else {
            return Ok(false);
        };
        let path = self.segment_path(reference.segment_id)?;
        let capacity = usize::try_from(reference.capacity)
            .map_err(|_| JournalIndexError::LineageSegmentMismatch(reference.segment_id))?;
        let mut file = File::open(path)?;
        let length = usize::try_from(file.metadata()?.len())
            .map_err(|_| JournalIndexError::LineageSegmentMismatch(reference.segment_id))?;
        if length > capacity {
            return Err(JournalIndexError::LineageSegmentMismatch(
                reference.segment_id,
            ));
        }
        let mut image = Vec::with_capacity(length);
        file.by_ref()
            .take(capacity.saturating_add(1) as u64)
            .read_to_end(&mut image)?;
        if image.len() > capacity {
            return Err(JournalIndexError::LineageSegmentMismatch(
                reference.segment_id,
            ));
        }
        let scan = scan_segment(
            &image,
            reference.first_group_number,
            reference.first_chain,
            self.decode_limits,
        )?;
        if scan.header.group_id() != self.identity.group_id
            || scan.header.segment_id() != reference.segment_id
            || scan.header.capacity() != reference.capacity
            || (reference.sealed.is_some() && matches!(scan.tail, TailState::Truncated { .. }))
        {
            return Err(JournalIndexError::LineageSegmentMismatch(
                reference.segment_id,
            ));
        }
        Ok(scan
            .groups
            .iter()
            .flat_map(|group| &group.operations)
            .find(|operation| operation.op_number == position.op_number)
            .is_some_and(|operation| operation.digest == position.digest))
    }

    /// Resolve and revalidate one partition offset from authoritative bytes.
    pub fn read_offset(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Result<Option<IndexedRecord>, JournalIndexError> {
        self.read_offset_through(partition, offset, self.through.op_number)
    }

    /// Revalidate a record and return the exact enclosing canonical operation.
    ///
    /// The position is journal identity, not durability, quorum, or commit evidence.
    /// Callers must separately establish that their authority covers this prefix.
    pub fn read_offset_with_position(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
    ) -> Result<Option<(IndexedRecord, LogPosition)>, JournalIndexError> {
        self.offset_location_through(partition, offset, self.through.op_number)?
            .map(|location| {
                let operation = location.entry.location.operation;
                let record = self.read_offset_location(location)?;
                Ok((
                    record,
                    LogPosition {
                        op_number: operation.op_number,
                        digest: operation.operation_digest,
                    },
                ))
            })
            .transpose()
    }

    pub(crate) fn read_offset_through(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
        through: u64,
    ) -> Result<Option<IndexedRecord>, JournalIndexError> {
        self.offset_location_through(partition, offset, through)?
            .map(|location| self.read_offset_location(location))
            .transpose()
    }

    fn offset_location_through(
        &self,
        partition: PartitionIncarnation,
        offset: Offset,
        through: u64,
    ) -> Result<Option<IndexedOffsetLocation>, JournalIndexError> {
        self.require_boundary(through)?;
        if let Some(active) = &self.active
            && let Some(entry) = active.find_offset(partition, offset)
            && entry.location.operation.op_number <= through
        {
            return Ok(Some(IndexedOffsetLocation {
                source: active.source(),
                entry,
            }));
        }
        Ok(self.sealed.find_offset(partition, offset, through)?)
    }

    /// Read one bounded contiguous range below an owner-captured end offset.
    ///
    /// `end` is exclusive and must come from the same committed state image as
    /// this snapshot. A missing offset below it is corruption or an invalid
    /// retention/state pairing, never an implicit end of stream.
    pub fn read_range(
        &self,
        partition: PartitionIncarnation,
        start: Offset,
        end: Offset,
        limits: ReadLimits,
    ) -> Result<Vec<IndexedRecord>, JournalIndexError> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(JournalIndexError::InvalidReadLimits);
        }
        let mut next = start;
        let mut locations = Vec::new();
        while next < end && locations.len() < limits.max_records {
            let location = self
                .offset_location_through(partition, next, self.through.op_number)?
                .ok_or(JournalIndexError::MissingOffset(next))?;
            locations.push(location);
            next = next
                .checked_next()
                .ok_or(JournalIndexError::OffsetExhausted)?;
        }

        let mut payload_bytes = 0_usize;
        let mut records = Vec::with_capacity(locations.len());
        let mut first = 0_usize;
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
            let decoded = read_indexed_records(
                self.segment_path(location.source.segment_id)?,
                location.source,
                &entries,
                self.decode_limits,
                self.operation_limits,
            )?;
            for record in decoded {
                let bytes = record.payload_bytes();
                if records.is_empty() && bytes > limits.max_bytes {
                    return Err(JournalIndexError::RecordExceedsReadLimit {
                        actual: bytes,
                        limit: limits.max_bytes,
                    });
                }
                if payload_bytes.saturating_add(bytes) > limits.max_bytes {
                    return Ok(records);
                }
                records.push(record);
                payload_bytes += bytes;
            }
            first = end;
        }
        Ok(records)
    }

    /// Resolve one message ID, cross-check offset index, then revalidate bytes.
    pub fn read_message(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
    ) -> Result<Option<IndexedRecord>, JournalIndexError> {
        self.read_message_through(partition, message_id, self.through.op_number)
    }

    pub(crate) fn read_message_through(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
        through: u64,
    ) -> Result<Option<IndexedRecord>, JournalIndexError> {
        self.message_location_through(partition, message_id, through)?
            .map(|location| self.read_message_location(&location))
            .transpose()
    }

    fn message_location_through(
        &self,
        partition: PartitionIncarnation,
        message_id: MessageId,
        through: u64,
    ) -> Result<Option<IndexedMessageLocation>, JournalIndexError> {
        self.require_boundary(through)?;
        if let Some(active) = &self.active
            && let Some(message) = active.find_message(partition, message_id)
        {
            let offset = active
                .find_offset(partition, message.offset)
                .ok_or(JournalIndexError::InconsistentActiveIndex)?;
            if offset.location.operation.op_number <= through {
                return Ok(Some(IndexedMessageLocation {
                    source: active.source(),
                    message,
                    offset,
                }));
            }
        }
        Ok(self.sealed.find_message(partition, message_id, through)?)
    }

    /// Resolve one control-operation retry result without reading its body.
    pub fn find_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<IndexedOperationLocation>, JournalIndexError> {
        self.find_operation_through(operation_id, self.through.op_number)
    }

    pub(crate) fn find_operation_through(
        &self,
        operation_id: OperationId,
        through: u64,
    ) -> Result<Option<IndexedOperationLocation>, JournalIndexError> {
        self.require_boundary(through)?;
        if let Some(active) = &self.active
            && let Some(entry) = active.find_operation(operation_id)
            && entry.location.op_number <= through
        {
            return Ok(Some(IndexedOperationLocation {
                source: active.source(),
                entry,
            }));
        }
        Ok(self.sealed.find_operation(operation_id, through)?)
    }

    fn require_boundary(&self, through: u64) -> Result<(), JournalIndexError> {
        if through > self.through.op_number {
            Err(JournalIndexError::BoundaryBeyondSnapshot {
                requested: through,
                available: self.through.op_number,
            })
        } else {
            Ok(())
        }
    }

    fn read_offset_location(
        &self,
        location: IndexedOffsetLocation,
    ) -> Result<IndexedRecord, JournalIndexError> {
        Ok(read_indexed_record(
            self.segment_path(location.source.segment_id)?,
            location.source,
            location.entry,
            self.decode_limits,
            self.operation_limits,
        )?)
    }

    fn read_message_location(
        &self,
        location: &IndexedMessageLocation,
    ) -> Result<IndexedRecord, JournalIndexError> {
        Ok(read_indexed_message(
            self.segment_path(location.source.segment_id)?,
            location.source,
            location.offset,
            location.message,
            self.decode_limits,
            self.operation_limits,
        )?)
    }

    fn segment_path(&self, segment_id: u64) -> Result<PathBuf, JournalIndexError> {
        self.pin
            .segment_path(segment_id)
            .ok_or(JournalIndexError::UnpinnedSegment(segment_id))
    }
}

/// Journal-index construction, lookup, or authoritative-read failure.
#[derive(Debug, Error)]
pub enum JournalIndexError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error(transparent)]
    /// Physical segment framing or integrity validation failed.
    Codec(#[from] crate::CodecError),
    #[error(transparent)]
    /// Canonical operation-body validation failed.
    Operation(#[from] ozzy_journal::operation::OperationCodecError),
    #[error(transparent)]
    /// Journal directory validation or publication failed.
    Directory(#[from] crate::DirectoryError),
    #[error(transparent)]
    /// The active-segment index rejected this prefix.
    Active(#[from] crate::ActiveIndexError),
    #[error(transparent)]
    /// The captured index catalog rejected its source or selection.
    Catalog(#[from] crate::IndexCatalogError),
    #[error(transparent)]
    /// Indexed record reading or validation failed.
    Read(#[from] IndexedReadError),
    #[error("segment {0} was not pinned by this index snapshot")]
    /// Segment was not pinned by this index snapshot.
    UnpinnedSegment(u64),
    #[error("segment {0} cannot prove the requested index lineage")]
    /// Segment cannot prove the requested index lineage.
    LineageSegmentMismatch(u64),
    #[error("index boundary {requested} exceeds snapshot boundary {available}")]
    /// Index boundary exceeds snapshot boundary.
    BoundaryBeyondSnapshot {
        #[doc = "Requested canonical operation number."]
        requested: u64,
        #[doc = "Available bytes, capacity, or canonical prefix at failure."]
        available: u64,
    },
    #[error("active message index has no matching offset entry")]
    /// Active message index has no matching offset entry.
    InconsistentActiveIndex,
    #[error("active read index exceeds configured entry or file-size limits")]
    /// Active read index exceeds configured entry or file-size limits.
    ReadIndexCapacity,
    #[error("sealed index catalog no longer matches the selected manifest")]
    /// Sealed index catalog no longer matches the selected manifest.
    StaleCatalog,
    #[error("indexed read limits must be nonzero")]
    /// Indexed read limits must be nonzero.
    InvalidReadLimits,
    #[error("record payload is {actual} bytes; read limit is {limit}")]
    /// Record payload is bytes; read limit is.
    RecordExceedsReadLimit {
        #[doc = "Observed size, count, or fenced field value."]
        actual: usize,
        #[doc = "Configured maximum for the reported resource."]
        limit: usize,
    },
    #[error("committed range is missing offset {0:?}")]
    /// Committed range is missing offset.
    MissingOffset(Offset),
    #[error("partition offset space exhausted during range read")]
    /// Partition offset space exhausted during range read.
    OffsetExhausted,
}
