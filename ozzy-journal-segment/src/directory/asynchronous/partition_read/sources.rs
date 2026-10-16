//! Journal-side record sources for reads: active and sealed segment sources,
//! index scans, and extent readers.

use super::{ActiveReadIndex, Journal, Scope, Source, index_error, validate_operation_bodies};
use crate::JournalIndexError as Error;
use crate::async_files::Access;
use crate::directory::asynchronous::active_segment;
use crate::reader::asynchronous::ExtentReader;
use crate::{
    ActiveSegmentIndex, DirectoryError, IndexBuildLimits, IndexSource, IndexedRecord,
    JournalLimits, LogPosition, OffsetIndexEntry, SegmentScan, TailState,
};
use ozzy_io::{OpenMode, Operation};
use ozzy_journal::ReadLimits;
use std::path::PathBuf;

impl Journal {
    pub(super) fn record_source(&self) -> Result<Option<Source>, Error> {
        self.healthy()?;
        let reference = *active_segment(&self.manifest.segments)?;
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

    pub(super) fn sealed_record_source(&self, id: u64) -> Result<Source, Error> {
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

    /// The journal's current source for `index`, which must still match it.
    /// `active` says `index` covers the open segment.
    pub(super) fn indexed_source(
        &self,
        index: &ActiveReadIndex,
        active: bool,
    ) -> Result<Source, Error> {
        let source = if active {
            self.record_source()?.ok_or(Error::StaleCatalog)?
        } else {
            self.sealed_record_source(index.source().segment_id)?
        };
        if source.index != index.source() {
            return Err(Error::StaleCatalog);
        }
        Ok(source)
    }

    /// Read the records at `entries`, one extent read per operation. Stops
    /// before the byte limit; a first record over it is an error.
    pub(super) async fn read_entries(
        &self,
        source: Source,
        entries: &[OffsetIndexEntry],
        limits: ReadLimits,
    ) -> Result<Vec<(IndexedRecord, LogPosition)>, Error> {
        let _lease = self
            .pins
            .protect_prepared_segment(source.index.segment_id)
            .map_err(DirectoryError::from)?;
        let path = self
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
            let records = extent_reader(&self.access, path.clone(), source.index, self.limits)
                .records(&entries[first..end])
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
                    return Ok(output);
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
        Ok(output)
    }

    pub(super) fn read_scope(&self) -> Scope {
        Scope {
            access: self.access.clone(),
            root: self.root().to_path_buf(),
            limits: self.limits,
            configuration_epoch: self.manifest.configuration_epoch,
            promised_view: self.manifest.promised_view,
        }
    }
}

pub(super) async fn scan_index(
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
    if !scan_matches(&scan, source) {
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

/// Whether a clean scan is exactly the segment prefix `source` names.
fn scan_matches(scan: &SegmentScan<'_>, source: Source) -> bool {
    scan.header.group_id() == source.index.group_id
        && scan.header.segment_id() == source.index.segment_id
        && scan.header.capacity() == source.reference.capacity
        && scan.valid_bytes == source.index.valid_bytes
        && scan.digest == source.index.segment_digest
        && scan.tail == TailState::Clean
}

/// Extent reader for one segment file under the journal's decode limits.
pub(super) fn extent_reader(
    access: &Access,
    path: PathBuf,
    source: IndexSource,
    limits: JournalLimits,
) -> ExtentReader<'_> {
    ExtentReader {
        access,
        path,
        source,
        decode: limits.decode,
        operations: limits.operations,
        chunk: limits.io.chunk_bytes,
    }
}
