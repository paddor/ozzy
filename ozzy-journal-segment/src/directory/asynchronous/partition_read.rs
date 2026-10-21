//! Incremental shard-local record selectors and detached bounded async reads.

use super::{Journal, validate_operation_bodies};
use crate::{
    DirectoryError, GroupIdentity, IndexBuildLimits, IndexSource, IndexedRecord,
    JournalIndexError as Error, LogPosition, OperationLocation, PreparedOperationRecords,
    SegmentReference,
    active_read_index::{ActiveReadIndex, CapturedReadEntries},
    async_files::Access,
    reader::ResidentOperations,
};
use ozzy_journal::ReadLimits;
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::{Offset, PartitionIncarnation};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    path::PathBuf,
    rc::Rc,
};

mod read;
mod sources;

pub use read::Read;
use sources::scan_index;

/// Per-owner bounds, separate from backend queue limits and caller output leases.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Active/index-file work and resident payload bounds. As with the existing
    /// reader cache, one newest indivisible operation may exceed residency.
    pub index: IndexBuildLimits,
    /// Maximum recently written operations retained for readers. Each may
    /// hold one physical payload buffer even when its body is small.
    pub max_resident_operations: usize,
    /// Compact sealed-index cache budget. Entries never retain payloads or files.
    pub cached_index_bytes: usize,
    /// Bookkeeping bound, including indexes with no records.
    pub cached_indexes: usize,
    /// Captured/executing reads; cancellation releases this admission slot.
    pub concurrent_reads: usize,
}

/// One requested record range: offsets `start..end` of `partition`, visible
/// through operation `through`. The caller supplies committed bounds.
#[derive(Debug, Clone, Copy)]
pub struct ReadRange {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// First offset to read.
    pub start: Offset,
    /// Exclusive end offset.
    pub end: Offset,
    /// Last operation number whose records are visible.
    pub through: u64,
}

impl ReadRange {
    /// Whether `index` holds the record at `start`.
    fn starts_in(&self, index: &ActiveReadIndex) -> bool {
        index.covers(self.partition, self.start)
            && index
                .end_offset(self.partition)
                .is_some_and(|end| self.start < end)
    }

    /// Capture at most `max_records` of this range that `index` holds.
    fn capture(
        &self,
        index: &ActiveReadIndex,
        max_records: usize,
    ) -> Result<CapturedReadEntries, Error> {
        let held = index
            .end_offset(self.partition)
            .ok_or(Error::InconsistentActiveIndex)?;
        index
            .capture(
                self.partition,
                self.start..self.end.min(held),
                max_records,
                self.through,
            )
            .map_err(index_error)
    }
}

#[derive(Debug)]
struct Cache {
    hot: VecDeque<Rc<ActiveReadIndex>>,
    bytes: usize,
    budget: usize,
    slots: usize,
}

impl Cache {
    fn remember(&mut self, index: Rc<ActiveReadIndex>) {
        if self.hot.iter().any(|old| old.source() == index.source()) {
            return;
        }
        let bytes = index.retained_bytes();
        if bytes > self.budget || self.slots == 0 {
            return;
        }
        while (self.hot.len() >= self.slots || bytes > self.budget - self.bytes)
            && let Some(oldest) = self.hot.pop_front()
        {
            self.bytes -= oldest.retained_bytes();
        }
        self.bytes += bytes;
        self.hot.push_back(index);
    }

    fn find(&self, source: IndexSource) -> Option<Rc<ActiveReadIndex>> {
        self.hot
            .iter()
            .find(|index| index.source() == source)
            .cloned()
    }

    fn covering(
        &self,
        sources: &[Source],
        range: ReadRange,
    ) -> Option<(Source, Rc<ActiveReadIndex>)> {
        self.hot.iter().rev().find_map(|index| {
            if !range.starts_in(index) {
                return None;
            }
            sources
                .iter()
                .find(|source| {
                    source.index == index.source() && source.index.first_op_number <= range.through
                })
                .map(|source| (*source, index.clone()))
        })
    }
}

/// Owned on the same application shard as its journal. No task, mutex-held
/// file operation, descriptor, or independent storage authority lives here.
#[derive(Debug)]
pub struct Index {
    identity: GroupIdentity,
    generation: JournalGeneration,
    active: Option<ActiveReadIndex>,
    previous: Option<ActiveReadIndex>,
    resident: ResidentOperations,
    cold: Rc<RefCell<Cache>>,
    outstanding: Rc<Cell<usize>>,
    limits: Limits,
}

#[derive(Debug, Clone, Copy)]
struct Source {
    reference: SegmentReference,
    index: IndexSource,
}

#[derive(Debug)]
struct Admission(Rc<Cell<usize>>);
impl Drop for Admission {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

#[derive(Debug)]
struct Scope {
    access: Access,
    root: PathBuf,
    limits: super::Limits,
    configuration_epoch: u64,
    promised_view: u64,
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "keep hot indexed captures inline without per-read boxes"
)]
enum Selection {
    Indexed {
        source: Source,
        entries: CapturedReadEntries,
        resident: Option<ResidentOperations>,
    },
    Cold(Vec<Source>),
}

fn index_error(error: crate::active_read_index::ActiveReadIndexError) -> Error {
    match error {
        crate::active_read_index::ActiveReadIndexError::LimitExceeded => Error::ReadIndexCapacity,
        _ => Error::InconsistentActiveIndex,
    }
}

impl Index {
    /// Newest append time in the installed active segment, without file access.
    pub fn newest_active_append_millis(&self, partition: PartitionIncarnation) -> Option<u64> {
        self.active.as_ref()?.newest_append_millis(partition)
    }

    /// Record ends the newest sealed segment's retained read index holds,
    /// bound to that segment's exact digest and operation range, without file
    /// access. `None` when no sealed predecessor is retained.
    pub fn sealed_retirement_proof(
        &self,
        journal: &Journal,
    ) -> Result<Option<super::RetirementProof>, Error> {
        self.check_owner(journal)?;
        let Some(previous) = &self.previous else {
            return Ok(None);
        };
        let source = journal.sealed_record_source(previous.source().segment_id)?;
        if source.index != previous.source() {
            return Err(Error::StaleCatalog);
        }
        let mut ends = previous.partition_ends().collect::<Vec<_>>();
        ends.sort_unstable_by_key(|(partition, _, _)| *partition);
        Ok(Some(super::RetirementProof {
            source: source.index,
            capacity: source.reference.capacity,
            ends,
        }))
    }

    /// Scan active history once at owner activation. Subsequent write deltas and
    /// read captures are memory-only. Sealed indexes load lazily on read misses.
    pub async fn open(journal: &Journal, limits: Limits) -> Result<Self, Error> {
        journal.healthy()?;
        if limits.concurrent_reads == 0 || limits.max_resident_operations == 0 {
            return Err(Error::InvalidReadLimits);
        }
        let active = match journal.record_source()? {
            Some(source) => Some(scan_index(&journal.read_scope(), source, limits.index).await?),
            None => None,
        };
        Ok(Self {
            identity: journal.manifest.identity,
            generation: journal.writer.written_position().generation(),
            active,
            previous: None,
            resident: ResidentOperations::with_limits(
                limits.index.max_resident_bytes,
                limits.max_resident_operations,
            ),
            cold: Rc::new(RefCell::new(Cache {
                hot: VecDeque::new(),
                bytes: 0,
                budget: limits.cached_index_bytes,
                slots: limits.cached_indexes,
            })),
            outstanding: Rc::new(Cell::new(0)),
            limits,
        })
    }

    /// Forget only the exact sealed prefix removed by this owner's retirement.
    /// Active selectors and previously captured reads keep their own ownership.
    pub fn retired(&mut self, journal: &Journal, segments: &[u64]) -> Result<(), Error> {
        self.check_owner(journal)?;
        if segments.iter().any(|id| {
            journal
                .manifest
                .segments
                .iter()
                .any(|reference| reference.segment_id == *id)
        }) {
            return Err(Error::StaleCatalog);
        }
        self.forget(segments);
        Ok(())
    }

    /// Forget the sealed segments a pack replaced. The packed segment keeps
    /// the last one's ID as a new source; its index loads on a read miss.
    pub fn packed(&mut self, journal: &Journal, segments: &[u64]) -> Result<(), Error> {
        self.check_owner(journal)?;
        self.forget(segments);
        Ok(())
    }

    fn forget(&mut self, segments: &[u64]) {
        if self
            .previous
            .as_ref()
            .is_some_and(|index| segments.contains(&index.source().segment_id))
        {
            self.previous = None;
        }
        let mut cold = self.cold.borrow_mut();
        cold.hot
            .retain(|index| !segments.contains(&index.source().segment_id));
        cold.bytes = cold.hot.iter().map(|index| index.retained_bytes()).sum();
        self.resident.retired(self.identity.group_id, segments);
    }

    fn check_owner(&self, journal: &Journal) -> Result<(), Error> {
        journal.healthy()?;
        if self.identity != journal.manifest.identity
            || self.generation != journal.writer.written_position().generation()
        {
            return Err(Error::StaleCatalog);
        }
        Ok(())
    }

    /// Read exact written offsets from retained incremental selectors. A miss
    /// leaves uncached sealed history to the snapshot path. Reuse the active,
    /// predecessor, and bounded cold indexes without rescanning their segments.
    pub async fn read_offsets_with_positions(
        &self,
        journal: &Journal,
        partition: PartitionIncarnation,
        offsets: &[Offset],
        limits: ReadLimits,
    ) -> Result<Option<Vec<(IndexedRecord, LogPosition)>>, Error> {
        self.check_owner(journal)?;
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(Error::InvalidReadLimits);
        }
        let Some(&first) = offsets.first() else {
            return Ok(Some(Vec::new()));
        };
        let retained = self
            .active
            .iter()
            .chain(self.previous.iter())
            .find(|index| index.entry(partition, first).is_ok());
        let cached = retained
            .is_none()
            .then(|| {
                self.cold
                    .borrow()
                    .hot
                    .iter()
                    .find(|index| index.entry(partition, first).is_ok())
                    .cloned()
            })
            .flatten();
        let Some(index) = retained.or(cached.as_deref()) else {
            return Ok(None);
        };
        let active = journal
            .manifest
            .segments
            .last()
            .is_some_and(|reference| reference.segment_id == index.source().segment_id);
        let source = journal.indexed_source(index, active)?;
        let mut entries = Vec::with_capacity(offsets.len().min(limits.max_records));
        for &offset in offsets.iter().take(limits.max_records) {
            let Ok(entry) = index.entry(partition, offset) else {
                return Ok(None);
            };
            entries.push(entry);
        }
        journal
            .read_entries(source, &entries, limits)
            .await
            .map(Some)
    }

    /// Install exactly the selectors and placements returned by an observed
    /// physical completion. Any mismatch invalidates this derived state.
    pub fn appended(
        &mut self,
        journal: &Journal,
        locations: &[OperationLocation],
        records: &[PreparedOperationRecords],
    ) -> Result<(), Error> {
        self.check_owner(journal)?;
        if locations.len() != records.len()
            || records.is_empty()
            || locations.iter().zip(records).any(|(location, record)| {
                !record.matches_location(self.identity.group_id, *location)
            })
        {
            return Err(Error::InconsistentActiveIndex);
        }
        let source = journal
            .record_source()?
            .ok_or(Error::InconsistentActiveIndex)?;
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.source().segment_id != source.index.segment_id)
        {
            self.retire(journal)?;
        }
        let refs = records.iter().collect::<Vec<_>>();
        match &mut self.active {
            Some(active) => {
                active.append_group(source.index, locations, &refs, self.limits.index.file)
            }
            None => {
                ActiveReadIndex::from_group(source.index, locations, &refs, self.limits.index.file)
                    .map(|active| self.active = Some(active))
            }
        }
        .map_err(index_error)?;
        let prepared = records
            .iter()
            .zip(locations)
            .filter_map(|(record, location)| record.bind(*location))
            .collect::<Vec<_>>();
        self.resident
            .insert_prepared(source.index.group_id, source.index.segment_id, &prepared);
        Ok(())
    }

    /// Retain the predecessor's compact runs. No read waits on index publication.
    pub fn rolled(&mut self, journal: &Journal) -> Result<(), Error> {
        self.check_owner(journal)?;
        if journal.record_source()?.is_some() {
            return Err(Error::StaleCatalog);
        }
        self.retire(journal)
    }

    fn retire(&mut self, journal: &Journal) -> Result<(), Error> {
        let Some(active) = &self.active else {
            return Ok(());
        };
        if journal
            .sealed_record_source(active.source().segment_id)?
            .index
            != active.source()
        {
            return Err(Error::StaleCatalog);
        }
        if let Some(previous) = self.previous.take() {
            self.cold.borrow_mut().remember(Rc::new(previous));
        }
        self.previous = self.active.take();
        Ok(())
    }

    /// Capture one bounded visible prefix. The caller supplies committed record
    /// and operation bounds; storage completion alone grants no read visibility.
    pub fn prepare_read(
        &self,
        journal: &Journal,
        range: ReadRange,
        limits: ReadLimits,
    ) -> Result<Read, Error> {
        self.check_owner(journal)?;
        if limits.max_records == 0 || limits.max_bytes == 0 || range.start > range.end {
            return Err(Error::InvalidReadLimits);
        }
        if self.outstanding.get() >= self.limits.concurrent_reads {
            return Err(Error::ReadIndexCapacity);
        }
        if journal.record_source()?.map(|source| source.index)
            != self.active.as_ref().map(ActiveReadIndex::source)
        {
            return Err(Error::StaleCatalog);
        }
        let (selection, ids) = self.selection(journal, range, limits.max_records)?;
        let leases = ids
            .into_iter()
            .map(|id| journal.pins.protect_prepared_segment(id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(DirectoryError::from)?;
        self.outstanding.set(self.outstanding.get() + 1);
        Ok(Read {
            scope: journal.read_scope(),
            selection,
            cold: self.cold.clone(),
            index_limits: self.limits.index,
            range,
            limits,
            _leases: leases,
            _admission: Admission(self.outstanding.clone()),
        })
    }

    /// Capture from the retained index holding the range start, or select
    /// every sealed source for a cold read. Also names the segments to protect.
    fn selection(
        &self,
        journal: &Journal,
        range: ReadRange,
        max_records: usize,
    ) -> Result<(Selection, Vec<u64>), Error> {
        if range.start == range.end {
            return Ok((Selection::Cold(Vec::new()), Vec::new()));
        }
        if let Some(index) = self
            .active
            .iter()
            .chain(self.previous.iter())
            .find(|index| range.starts_in(index))
        {
            let active = self
                .active
                .as_ref()
                .is_some_and(|active| active.source() == index.source());
            let source = journal.indexed_source(index, active)?;
            let entries = range.capture(index, max_records)?;
            let resident = self.resident.capture(source.index, entries.operations())?;
            let ids = vec![source.index.segment_id];
            return Ok((
                Selection::Indexed {
                    source,
                    entries,
                    resident,
                },
                ids,
            ));
        }
        let sources = journal
            .manifest
            .segments
            .iter()
            .filter(|r| r.sealed.is_some())
            .map(|r| journal.sealed_record_source(r.segment_id))
            .collect::<Result<Vec<_>, _>>()?;
        let ids = sources
            .iter()
            .map(|source| source.index.segment_id)
            .collect();
        Ok((Selection::Cold(sources), ids))
    }
}
