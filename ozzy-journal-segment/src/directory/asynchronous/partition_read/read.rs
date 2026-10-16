//! One detached bounded read: cold source selection, index loading, and the
//! record visit.

use super::{
    ActiveReadIndex, Admission, Cache, CapturedReadEntries, ReadRange, Scope, Selection, Source,
    index_error,
    sources::{extent_reader, scan_index},
};
use crate::JournalIndexError as Error;
use crate::reader::ResidentOperations;
use crate::retention::PreparedSegmentLease;
use crate::{DirectoryError, IndexBuildLimits, IndexedReadError, OperationLocation, RecordSpan};
use ozzy_journal::ReadLimits;
use ozzy_proto::Offset;
use std::cell::RefCell;
use std::rc::Rc;

/// Captured visibility and temporary deletion protection. Execution borrows no
/// journal/index owner. Later writes, rolls, or replacement cannot enlarge it.
#[derive(Debug)]
pub struct Read {
    pub(super) scope: Scope,
    pub(super) selection: Selection,
    pub(super) cold: Rc<RefCell<Cache>>,
    pub(super) index_limits: IndexBuildLimits,
    pub(super) range: ReadRange,
    pub(super) limits: ReadLimits,
    pub(super) _leases: Vec<PreparedSegmentLease>,
    pub(super) _admission: Admission,
}

/// How many leading records of `span` fit the remaining byte budget,
/// charging them to `bytes`. The first record of a read always fits.
fn fitting(span: &RecordSpan<'_>, first: bool, bytes: &mut usize, max_bytes: usize) -> usize {
    let mut allowed = 0;
    for record in span.records() {
        let size = record.payload_bytes();
        if (!first || allowed != 0) && size > max_bytes.saturating_sub(*bytes) {
            break;
        }
        *bytes = bytes.saturating_add(size);
        allowed += 1;
    }
    allowed
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
        let Selection::Indexed {
            source,
            entries,
            resident: Some(records),
        } = self.selection
        else {
            return Err(self);
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
        let cached = self.cold.borrow().covering(sources, self.range);
        if let Some((source, index)) = cached {
            return self.capture_index(source, &index);
        }
        for source in sources
            .iter()
            .rev()
            .filter(|source| source.index.first_op_number <= self.range.through)
        {
            let index = self.load_index(*source).await?;
            if self.range.starts_in(&index) {
                return self.capture_index(*source, &index);
            }
        }
        Err(Error::MissingOffset(self.range.start))
    }

    fn capture_index(
        &self,
        source: Source,
        index: &ActiveReadIndex,
    ) -> Result<(Source, CapturedReadEntries), Error> {
        Ok((source, self.range.capture(index, self.limits.max_records)?))
    }

    /// Captured entries, or those of the cold source holding the range start,
    /// with any resident operations.
    async fn entries(
        &self,
    ) -> Result<(Source, CapturedReadEntries, Option<&ResidentOperations>), Error> {
        match &self.selection {
            Selection::Indexed {
                source,
                entries,
                resident,
            } => Ok((*source, entries.clone(), resident.as_ref())),
            Selection::Cold(sources) => {
                let (source, entries) = self.select_cold(sources).await?;
                Ok((source, entries, None))
            }
        }
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
        if self.range.start == self.range.end {
            return Ok(self.range.start);
        }
        let (source, entries, resident) = self.entries().await?;
        let mut next = self.range.start;
        let mut bytes = 0usize;
        let mut loaded: Option<(OperationLocation, crate::reader::CachedOperation)> = None;
        for (entry, count) in entries.ranges() {
            let batch = if let Some(resident) = resident {
                resident.select(source.index, entry)?
            } else {
                let operation = match loaded.take() {
                    Some(current) if current.0 == entry.location.operation => current,
                    _ => (
                        entry.location.operation,
                        extent_reader(
                            &self.scope.access,
                            self.scope
                                .root
                                .join("segments")
                                .join(source.reference.file_name()),
                            source.index,
                            self.scope.limits,
                        )
                        .load(entry)
                        .await?,
                    ),
                };
                loaded.insert(operation).1.select(entry)?
            };
            let span = batch.span(entry, count)?;
            let first = next == self.range.start;
            let allowed = fitting(&span, first, &mut bytes, self.limits.max_bytes);
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
