//! Exact, deletion-protected snapshot. Every cold lookup and payload read uses
//! file jobs. The active prefix is frozen before releasing the journal borrow.

mod records;

use std::{path::PathBuf, rc::Rc};

use super::{
    ActiveSegmentIndex, DecodeLimits, IndexedOperation, IndexedOperationLocation,
    JournalIndexError as Error, LogPosition, OperationId, OperationLimits, TailState, scan_segment,
};
use crate::{
    GroupIdentity, SegmentReference, async_files::Access, index_catalog::asynchronous::Catalog,
    retention::PreparedSegmentLease,
};
use ozzy_core::state::{IdentityClaim, IdentityKey};
use ozzy_io::{OpenMode, Operation};
use ozzy_journal::operation::ReleaseKey;
use ozzy_proto::PartitionIncarnation;

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
    pub(crate) leases: Rc<[PreparedSegmentLease]>,
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
        if let Some(contained) = self.bounds_contain(position) {
            return Ok(contained);
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
        let image = self.read_prefix(reference, bytes).await?;
        let scan = scan_segment(
            &image,
            reference.first_group_number,
            reference.first_chain,
            self.decode,
        )?;
        let captured = scan.header.group_id() == self.identity.group_id
            && scan.header.segment_id() == reference.segment_id
            && scan.header.capacity() == reference.capacity
            && scan.valid_bytes == bytes
            && scan.digest == digest
            && !matches!(scan.tail, TailState::Truncated { .. });
        if !captured {
            return Err(Error::LineageSegmentMismatch(reference.segment_id));
        }
        Ok(scan
            .groups
            .iter()
            .flat_map(|group| &group.operations)
            .find(|operation| operation.op_number == position.op_number)
            .is_some_and(|operation| operation.digest == position.digest))
    }

    /// The answer when `position` is genesis, at or past the captured end, or
    /// the first segment's predecessor; `None` when a segment must be read.
    fn bounds_contain(&self, position: LogPosition) -> Option<bool> {
        if position == LogPosition::GENESIS {
            return Some(true);
        }
        if position.op_number > self.through.op_number {
            return Some(false);
        }
        if position.op_number == self.through.op_number {
            return Some(position.digest == self.through.digest);
        }
        // The selected first segment binds its predecessor, like genesis.
        self.references
            .first()
            .is_some_and(|first| anchor(first.first_chain) == Some(position))
            .then_some(true)
    }

    /// Read the captured `bytes` prefix of one referenced segment file.
    async fn read_prefix(
        &self,
        reference: &SegmentReference,
        bytes: u64,
    ) -> Result<Vec<u8>, Error> {
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
        Ok(image)
    }

    /// Drop sources before `first`, the first segment the same journal still
    /// selects after retirement, and release their deletion protection. Reads
    /// no files. The cut falls where `first`'s operations start, so a packed
    /// `first` keeps the captured segments it replaced. When every captured
    /// source retired, the view re-anchors at the predecessor position
    /// `first` binds.
    pub(crate) fn retain_from(&self, first: SegmentReference) -> Result<Self, Error> {
        let mismatch = Error::LineageSegmentMismatch(first.segment_id);
        let start = first.first_chain.next_op_number();
        let at = self
            .references
            .partition_point(|reference| reference.first_chain.next_op_number() < start);
        let (mut retained, from) = match self.references.get(at) {
            // The same segment, or a packed one that replaced it and later ones.
            Some(kept)
                if kept.first_chain != first.first_chain
                    || kept.segment_id > first.segment_id
                    || (kept.segment_id == first.segment_id
                        && kept.file_generation != first.file_generation) =>
            {
                return Err(mismatch);
            }
            Some(_) if at == 0 => return Ok(self.clone()),
            Some(kept) => (self.clone(), kept.segment_id),
            None => {
                let anchor = anchor(first.first_chain)
                    .filter(|anchor| anchor.op_number >= self.through.op_number)
                    .ok_or(mismatch)?;
                let retained = Self {
                    active: None,
                    active_bytes: 0,
                    through: anchor,
                    ..self.clone()
                };
                (retained, first.segment_id)
            }
        };
        retained.references.drain(..at);
        retained.sealed = self.sealed.retain_from(from);
        retained.leases = self
            .leases
            .iter()
            .filter(|lease| lease.segment_id() >= from)
            .map(PreparedSegmentLease::duplicate)
            .collect::<Result<Vec<_>, _>>()
            .map_err(crate::DirectoryError::from)?
            .into();
        Ok(retained)
    }

    /// Release key of the partition's last scheduled record in each sealed
    /// segment holding one, oldest first.
    pub fn sealed_schedules(
        &self,
        partition: PartitionIncarnation,
    ) -> impl Iterator<Item = (crate::IndexSource, ReleaseKey)> + '_ {
        self.sealed.last_schedules(partition)
    }

    /// The partition's first `limit` scheduled records in sealed segments
    /// after `after`, or from its first one, in release order.
    pub async fn sealed_schedules_after(
        &self,
        partition: PartitionIncarnation,
        after: Option<ReleaseKey>,
        limit: usize,
    ) -> Result<Vec<crate::ScheduleIndexEntry>, Error> {
        Ok(self.sealed.schedules_after(partition, after, limit).await?)
    }

    /// The partition's scheduled records in the captured active prefix, in
    /// release order.
    pub fn active_schedules(
        &self,
        partition: PartitionIncarnation,
    ) -> impl Iterator<Item = crate::ScheduleIndexEntry> + '_ {
        self.active
            .iter()
            .flat_map(|active| active.schedules())
            .filter(move |entry| entry.partition == partition)
            .copied()
    }

    /// Look up an exact canonical control-operation identity.
    pub async fn find_operation(
        &self,
        id: OperationId,
    ) -> Result<Option<IndexedOperationLocation>, Error> {
        self.find_operation_through(id, self.through.op_number)
            .await
    }

    /// Resolve any operation number in this pinned prefix, including an append
    /// containing only duplicates. Later physical writes stay invisible.
    pub async fn find_operation_number(
        &self,
        number: u64,
    ) -> Result<Option<IndexedOperation>, Error> {
        if number == 0 || number > self.through.op_number {
            return Ok(None);
        }
        if let Some(active) = &self.active
            && let Some(location) = active.find_operation_number(number)
        {
            return Ok(Some(IndexedOperation {
                source: active.source(),
                location,
            }));
        }
        Ok(self
            .sealed
            .find_operation_number(number, self.through.op_number)
            .await?)
    }

    /// Read authoritative operation bytes with framing and digest validation.
    /// The captured source remains deletion-protected throughout this file job.
    pub async fn read_operation_number(
        &self,
        number: u64,
    ) -> Result<Option<crate::DecodedOperation<'static>>, Error> {
        let Some(indexed) = self.find_operation_number(number).await? else {
            return Ok(None);
        };
        Ok(Some(
            self.reader(indexed.source)?
                .operation(indexed.location)
                .await?,
        ))
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
            self.reader(location.source)?
                .control(location.entry)
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

    /// Read extents of one captured segment.
    fn reader(
        &self,
        source: crate::IndexSource,
    ) -> Result<crate::reader::asynchronous::ExtentReader<'_>, Error> {
        Ok(crate::reader::asynchronous::ExtentReader {
            access: &self.access,
            path: self.segment_path(source.segment_id)?,
            source,
            decode: self.decode,
            operations: self.operations,
            chunk: self.chunk,
        })
    }
}

fn anchor(chain: ozzy_journal::operation::ChainPosition) -> Option<LogPosition> {
    Some(LogPosition {
        op_number: chain.next_op_number().checked_sub(1)?,
        digest: chain.previous_digest(),
    })
}
