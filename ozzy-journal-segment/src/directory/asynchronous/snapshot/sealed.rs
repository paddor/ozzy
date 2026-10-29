//! Sealed indexes captured on the owner and read by a detached job, with no
//! active-segment scan.

use super::Journal;
use crate::retention::PreparedSegmentLease;
use crate::{
    DirectoryError, IndexLimits, IndexSource, JournalIndexError, ScheduleIndexEntry,
    async_files::Access, index_catalog::asynchronous::Catalog,
};
use ozzy_journal::operation::{HeldPosition, ReleaseKey};
use ozzy_proto::PartitionIncarnation;
use std::{cell::RefCell, path::PathBuf, rc::Rc};

/// Every sealed index this owner validated, protected until the last clone
/// drops. The first read opens them; clones share that catalog and its
/// bounded cache of index images.
#[derive(Debug, Clone)]
pub struct SealedSchedules {
    captured: Rc<Captured>,
}

#[derive(Debug)]
struct Captured {
    access: Access,
    directory: PathBuf,
    sources: Vec<IndexSource>,
    limits: IndexLimits,
    chunk: usize,
    catalog: RefCell<Option<Catalog>>,
    _leases: Vec<PreparedSegmentLease>,
}

impl Journal {
    /// Capture the sealed indexes, or `None` while one is not validated
    /// here yet. A built snapshot validates it. Returns `kept` while it
    /// still captures exactly the sealed segments.
    pub fn capture_sealed_schedules(
        &self,
        limits: IndexLimits,
        kept: Option<&SealedSchedules>,
    ) -> Result<Option<SealedSchedules>, JournalIndexError> {
        self.healthy()?;
        let sources = self.sealed_sources()?;
        if let Some(kept) = kept
            && kept.captured.sources == sources
        {
            return Ok(Some(kept.clone()));
        }
        let validated = self.validated_indexes.borrow();
        if sources
            .iter()
            .any(|source| validated.get(&source.segment_id) != Some(source))
        {
            return Ok(None);
        }
        let leases = sources
            .iter()
            .map(|source| self.pins.protect_prepared_segment(source.segment_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(DirectoryError::from)?;
        Ok(Some(SealedSchedules {
            captured: Rc::new(Captured {
                access: self.access.clone(),
                directory: self.root().join("indexes"),
                sources,
                limits,
                chunk: self.limits.io.chunk_bytes,
                catalog: RefCell::new(None),
                _leases: leases,
            }),
        }))
    }
}

impl SealedSchedules {
    /// The partition's first `limit` scheduled records after `after`, or
    /// from its first one, in release order. Rows before the carry floor
    /// `from` are skipped. Opening the indexes verifies their checksums.
    pub async fn after(
        &self,
        partition: PartitionIncarnation,
        after: Option<ReleaseKey>,
        from: HeldPosition,
        limit: usize,
    ) -> Result<Vec<ScheduleIndexEntry>, JournalIndexError> {
        let opened = self.captured.catalog.borrow().clone();
        let catalog = if let Some(catalog) = opened {
            catalog
        } else {
            let captured = &self.captured;
            let catalog = Catalog::open(
                captured.access.clone(),
                captured.directory.clone(),
                captured.sources.clone(),
                captured.limits,
                captured.chunk,
            )
            .await?;
            *captured.catalog.borrow_mut() = Some(catalog.clone());
            catalog
        };
        Ok(catalog
            .schedules_after(partition, after, from, limit)
            .await?)
    }
}
