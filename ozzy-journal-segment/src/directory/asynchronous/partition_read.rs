//! Incremental shard-local record selectors and detached bounded async reads.

use super::{Journal, validate_operation_bodies};
use crate::{
    ActiveSegmentIndex, DirectoryError, GroupIdentity, IndexBuildLimits, IndexSource,
    IndexedReadError, IndexedRecord, JournalIndexError as Error, LogPosition, OperationLocation,
    PreparedOperationRecords, RecordSpan, SegmentReference, TailState,
    active_read_index::{ActiveReadIndex, CapturedReadEntries},
    async_files::Access,
    reader::ResidentOperations,
    retention::PreparedSegmentLease,
};
use ozzy_io::{OpenMode, Operation};
use ozzy_journal::ReadLimits;
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::{Offset, PartitionIncarnation};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    path::PathBuf,
    rc::Rc,
};

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
        while self.hot.len() >= self.slots || bytes > self.budget - self.bytes {
            self.bytes -= self
                .hot
                .pop_front()
                .expect("charged cache")
                .retained_bytes();
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
        partition: PartitionIncarnation,
        start: Offset,
        through: u64,
    ) -> Option<(Source, Rc<ActiveReadIndex>)> {
        self.hot.iter().rev().find_map(|index| {
            if !index.covers(partition, start)
                || index.end_offset(partition).is_none_or(|end| start >= end)
            {
                return None;
            }
            sources
                .iter()
                .find(|source| {
                    source.index == index.source() && source.index.first_op_number <= through
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

/// Captured visibility and temporary deletion protection. Execution borrows no
/// journal/index owner. Later writes, rolls, or replacement cannot enlarge it.
#[derive(Debug)]
pub struct Read {
    scope: Scope,
    selection: Selection,
    cold: Rc<RefCell<Cache>>,
    index_limits: IndexBuildLimits,
    partition: PartitionIncarnation,
    start: Offset,
    end: Offset,
    through: u64,
    limits: ReadLimits,
    _leases: Vec<PreparedSegmentLease>,
    _admission: Admission,
}

fn index_error(error: crate::active_read_index::ActiveReadIndexError) -> Error {
    match error {
        crate::active_read_index::ActiveReadIndexError::LimitExceeded => Error::ReadIndexCapacity,
        _ => Error::InconsistentActiveIndex,
    }
}

impl Journal {
    fn record_source(&self) -> Result<Option<Source>, Error> {
        self.healthy()?;
        let reference = *self.manifest.segments.last().expect("active segment");
        let position = self.writer.written_position();
        if position.segment_id() != reference.segment_id {
            return Err(Error::StaleCatalog);
        }
        let first = reference.first_chain.next_op_number();
        let next = position.next_chain().next_op_number();
        if first == next {
            return Ok(None);
        }
        if next < first {
            return Err(Error::StaleCatalog);
        }
        Ok(Some(Source {
            reference,
            index: IndexSource {
                group_id: self.manifest.identity.group_id,
                segment_id: reference.segment_id,
                valid_bytes: position.end_offset(),
                segment_digest: self.writer.state().structural_digest(),
                first_op_number: first,
                last_op_number: next - 1,
                last_operation_digest: position.next_chain().previous_digest(),
            },
        }))
    }

    fn sealed_record_source(&self, id: u64) -> Result<Source, Error> {
        let at = self
            .manifest
            .segments
            .binary_search_by_key(&id, |r| r.segment_id)
            .map_err(|_| Error::StaleCatalog)?;
        let reference = self.manifest.segments[at];
        let successor = self
            .manifest
            .segments
            .get(at + 1)
            .ok_or(Error::StaleCatalog)?;
        Ok(Source {
            reference,
            index: crate::directory::sealed_source(
                self.manifest.identity.group_id,
                &reference,
                successor,
            )?,
        })
    }

    fn read_scope(&self) -> Scope {
        Scope {
            access: self.access.clone(),
            root: self.root().to_path_buf(),
            limits: self.limits,
            configuration_epoch: self.manifest.configuration_epoch,
            promised_view: self.manifest.promised_view,
        }
    }
}

impl Index {
    /// Newest append time in the installed active segment, without file access.
    pub fn newest_active_append_millis(&self, partition: PartitionIncarnation) -> Option<u64> {
        self.active.as_ref()?.newest_append_millis(partition)
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
        Ok(())
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

    /// Read exact written offsets from the incremental active index. A miss
    /// leaves older sealed history to the snapshot path. This avoids scanning
    /// the growing active segment for each producer retry.
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
        let Some(active) = &self.active else {
            return Ok(None);
        };
        let Some(source) = journal.record_source()? else {
            return Ok(None);
        };
        if active.source() != source.index {
            return Err(Error::StaleCatalog);
        }
        let mut entries = Vec::with_capacity(offsets.len().min(limits.max_records));
        for &offset in offsets.iter().take(limits.max_records) {
            let Ok(entry) = active.entry(partition, offset) else {
                return Ok(None);
            };
            entries.push(entry);
        }
        let _lease = journal
            .pins
            .protect_prepared_segment(source.index.segment_id)
            .map_err(DirectoryError::from)?;
        let path = journal
            .root()
            .join("segments")
            .join(source.reference.file_name());
        let mut output = Vec::with_capacity(entries.len());
        let mut bytes = 0usize;
        let mut first = 0;
        while first < entries.len() {
            let location = entries[first].location.operation;
            let mut end = first + 1;
            while end < entries.len() && entries[end].location.operation == location {
                end += 1;
            }
            let records = crate::reader::asynchronous::read_records(
                &journal.access,
                path.clone(),
                source.index,
                &entries[first..end],
                journal.limits.decode,
                journal.limits.operations,
                journal.limits.io.chunk_bytes,
            )
            .await?;
            for record in records {
                let size = record.payload_bytes();
                if output.is_empty() && size > limits.max_bytes {
                    return Err(Error::RecordExceedsReadLimit {
                        actual: size,
                        limit: limits.max_bytes,
                    });
                }
                if size > limits.max_bytes - bytes {
                    return Ok(Some(output));
                }
                output.push((
                    record,
                    LogPosition {
                        op_number: location.op_number,
                        digest: location.operation_digest,
                    },
                ));
                bytes += size;
            }
            first = end;
        }
        Ok(Some(output))
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
        partition: PartitionIncarnation,
        start: Offset,
        end: Offset,
        through: u64,
        limits: ReadLimits,
    ) -> Result<Read, Error> {
        self.check_owner(journal)?;
        if limits.max_records == 0 || limits.max_bytes == 0 || start > end {
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
        let (selection, ids) = if start == end {
            (Selection::Cold(Vec::new()), Vec::new())
        } else if let Some(index) = self
            .active
            .iter()
            .chain(self.previous.iter())
            .find(|index| {
                index.covers(partition, start)
                    && index.end_offset(partition).is_some_and(|end| start < end)
            })
        {
            let source = if self
                .active
                .as_ref()
                .is_some_and(|active| active.source() == index.source())
            {
                journal.record_source()?.ok_or(Error::StaleCatalog)?
            } else {
                journal.sealed_record_source(index.source().segment_id)?
            };
            if source.index != index.source() {
                return Err(Error::StaleCatalog);
            }
            let entries = index
                .capture(
                    partition,
                    start,
                    end.min(index.end_offset(partition).expect("covered")),
                    limits.max_records,
                    through,
                )
                .map_err(index_error)?;
            let resident = self.resident.capture(source.index, entries.operations())?;
            (
                Selection::Indexed {
                    source,
                    entries,
                    resident,
                },
                vec![source.index.segment_id],
            )
        } else {
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
            (Selection::Cold(sources), ids)
        };
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
            partition,
            start,
            end,
            through,
            limits,
            _leases: leases,
            _admission: Admission(self.outstanding.clone()),
        })
    }
}

async fn scan_index(
    scope: &Scope,
    source: Source,
    limits: IndexBuildLimits,
) -> Result<ActiveReadIndex, Error> {
    let file = scope
        .access
        .open(
            scope
                .root
                .join("segments")
                .join(source.reference.file_name()),
            OpenMode::Read,
            false,
            false,
        )
        .await?;
    let length = scope.access.length(&file).await?;
    if length < source.index.valid_bytes || length > source.reference.capacity {
        return Err(Error::LineageSegmentMismatch(source.index.segment_id));
    }
    let count = usize::try_from(source.index.valid_bytes).map_err(|_| Error::ReadIndexCapacity)?;
    let bytes = scope
        .access
        .read_range(&file, 0, count, scope.limits.io.chunk_bytes)
        .await?;
    scope.access.done(Operation::Close { handle: file }).await?;
    let scan = crate::scan_segment_async(
        &bytes,
        source.reference.first_group_number,
        source.reference.first_chain,
        scope.limits.decode,
    )
    .await?;
    if scan.header.group_id() != source.index.group_id
        || scan.header.segment_id() != source.index.segment_id
        || scan.header.capacity() != source.reference.capacity
        || scan.valid_bytes != source.index.valid_bytes
        || scan.digest != source.index.segment_digest
        || scan.tail != TailState::Clean
    {
        return Err(Error::LineageSegmentMismatch(source.index.segment_id));
    }
    validate_operation_bodies(
        &scan,
        scope.limits.operations,
        scope.configuration_epoch,
        scope.promised_view,
    )
    .await?;
    let active = ActiveSegmentIndex::build_async(
        &scan,
        source.index.last_op_number,
        scope.limits.operations,
        limits.file,
    )
    .await?
    .ok_or(Error::InconsistentActiveIndex)?;
    if active.source() != source.index {
        return Err(Error::LineageSegmentMismatch(source.index.segment_id));
    }
    ActiveReadIndex::from_snapshot_async(&active)
        .await
        .map_err(index_error)
}

impl Read {
    /// Detach a fully resident selection with no file access or payload copy.
    /// Releases file protection and the I/O slot; the caller must retain its
    /// own bounded output/transport admission while these payloads stay alive.
    #[expect(
        clippy::result_large_err,
        reason = "return cold capture without per-read allocation"
    )]
    pub fn into_resident(self) -> Result<crate::ResidentRecordRead, Self> {
        if !matches!(
            &self.selection,
            Selection::Indexed {
                resident: Some(_),
                ..
            }
        ) {
            return Err(self);
        }
        let Selection::Indexed {
            source,
            entries,
            resident: Some(records),
        } = self.selection
        else {
            unreachable!("resident indexed selection checked")
        };
        Ok(crate::ResidentRecordRead::from_captured(
            source.index,
            entries,
            records,
            self.limits,
        ))
    }

    async fn select_cold(
        &self,
        sources: &[Source],
    ) -> Result<(Source, CapturedReadEntries), Error> {
        let cached = self
            .cold
            .borrow()
            .covering(sources, self.partition, self.start, self.through);
        if let Some((source, index)) = cached {
            return self.capture_index(source, &index);
        }
        for source in sources
            .iter()
            .rev()
            .filter(|source| source.index.first_op_number <= self.through)
        {
            let index = self.load_index(*source).await?;
            if index.covers(self.partition, self.start)
                && index
                    .end_offset(self.partition)
                    .is_some_and(|end| self.start < end)
            {
                return self.capture_index(*source, &index);
            }
        }
        Err(Error::MissingOffset(self.start))
    }

    fn capture_index(
        &self,
        source: Source,
        index: &ActiveReadIndex,
    ) -> Result<(Source, CapturedReadEntries), Error> {
        let entries = index
            .capture(
                self.partition,
                self.start,
                self.end
                    .min(index.end_offset(self.partition).expect("covered")),
                self.limits.max_records,
                self.through,
            )
            .map_err(index_error)?;
        Ok((source, entries))
    }

    async fn load_index(&self, source: Source) -> Result<Rc<ActiveReadIndex>, Error> {
        if let Some(index) = self.cold.borrow().find(source.index) {
            return Ok(index);
        }
        let loaded = crate::index_builder::asynchronous::open(
            &self.scope.access,
            self.scope
                .root
                .join("indexes")
                .join(crate::segment_index_name(source.index)),
            source.index,
            self.index_limits.file,
            self.scope.limits.io.chunk_bytes,
        )
        .await;
        let index = match loaded {
            Ok(index) => ActiveReadIndex::from_persisted_async(&index)
                .await
                .map_err(index_error)?,
            Err(crate::IndexBuildError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                scan_index(&self.scope, source, self.index_limits).await?
            }
            Err(error) => return Err(DirectoryError::from(error).into()),
        };
        let index = Rc::new(index);
        self.cold.borrow_mut().remember(index.clone());
        Ok(index)
    }

    /// Visit borrowed spans, loading each operation once per read. A callback
    /// may consume fewer records to enforce its own output/part budget.
    /// Returned offset is exclusive. A large first record may exceed
    /// `max_bytes`, matching the journal read contract and avoiding a stuck reader.
    pub async fn visit(
        self,
        mut receive: impl FnMut(&RecordSpan<'_>) -> usize,
    ) -> Result<Offset, Error> {
        if self.start == self.end {
            return Ok(self.start);
        }
        let (source, entries, resident) = match &self.selection {
            Selection::Indexed {
                source,
                entries,
                resident,
            } => (*source, entries.clone(), resident.as_ref()),
            Selection::Cold(sources) => {
                let (source, entries) = self.select_cold(sources).await?;
                (source, entries, None)
            }
        };
        let mut next = self.start;
        let mut bytes = 0usize;
        let mut loaded: Option<(OperationLocation, crate::reader::CachedOperation)> = None;
        for (entry, count) in entries.ranges() {
            let batch = if let Some(resident) = resident {
                resident.select(source.index, entry)?
            } else {
                if loaded
                    .as_ref()
                    .is_none_or(|(location, _)| *location != entry.location.operation)
                {
                    loaded = Some((
                        entry.location.operation,
                        crate::reader::asynchronous::load(
                            &self.scope.access,
                            self.scope
                                .root
                                .join("segments")
                                .join(source.reference.file_name()),
                            source.index,
                            entry,
                            self.scope.limits.decode,
                            self.scope.limits.operations,
                            self.scope.limits.io.chunk_bytes,
                        )
                        .await?,
                    ));
                }
                loaded.as_ref().expect("loaded operation").1.select(entry)?
            };
            let span = batch.span(entry, count)?;
            let mut allowed = 0;
            for record in span.records() {
                let size = record.payload_bytes();
                if (next != self.start || allowed != 0)
                    && size > self.limits.max_bytes.saturating_sub(bytes)
                {
                    break;
                }
                bytes = bytes.saturating_add(size);
                allowed += 1;
            }
            if allowed == 0 {
                break;
            }
            let span = batch.span(entry, allowed)?;
            let consumed = receive(&span);
            if consumed > allowed {
                return Err(IndexedReadError::InvalidSelector.into());
            }
            next = Offset::new(
                next.get()
                    .checked_add(consumed as u64)
                    .ok_or(Error::OffsetExhausted)?,
            );
            if consumed != count {
                break;
            }
        }
        Ok(next)
    }
}
