//! Detached reads of one operation at the extent its write returned, with no
//! index snapshot.

use super::{Journal, Scope, Source, sources::extent_reader};
use crate::JournalIndexError as Error;
use crate::retention::PreparedSegmentLease;
use crate::{DecodedOperation, DirectoryError, OperationLocation};
use ozzy_journal::progress::JournalGeneration;

/// Where one operation was written: the writer incarnation, the segment
/// file, and the extent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrittenOperation {
    /// Writer incarnation that wrote it.
    pub generation: JournalGeneration,
    /// File generation of its segment then.
    pub file_generation: u64,
    /// Its extent and identity.
    pub location: OperationLocation,
}

/// One captured operation read. Its segment stays protected until it
/// settles; execution borrows no journal owner.
#[derive(Debug)]
pub struct OperationRead {
    scope: Scope,
    source: Source,
    location: OperationLocation,
    _lease: PreparedSegmentLease,
}

impl Journal {
    /// The active segment's operations at `locations`, as just written.
    pub fn written_operations<'a>(
        &self,
        locations: &'a [OperationLocation],
    ) -> impl Iterator<Item = WrittenOperation> + 'a {
        let generation = self.writer.written_position().generation();
        let file_generation = self
            .manifest
            .segments
            .last()
            .map_or(0, |reference| reference.file_generation);
        locations.iter().map(move |&location| WrittenOperation {
            generation,
            file_generation,
            location,
        })
    }

    /// Capture a read of `written`, or `None` once another writer, file, or
    /// retirement replaced it.
    pub fn prepare_operation_read(
        &self,
        written: WrittenOperation,
    ) -> Result<Option<OperationRead>, Error> {
        self.healthy()?;
        let id = written.location.segment_id;
        if written.generation != self.writer.written_position().generation() {
            return Ok(None);
        }
        let Ok(at) = self
            .manifest
            .segments
            .binary_search_by_key(&id, |reference| reference.segment_id)
        else {
            return Ok(None);
        };
        let reference = self.manifest.segments[at];
        if reference.file_generation != written.file_generation {
            return Ok(None);
        }
        let source = if reference.sealed.is_none() {
            // A roll in progress moves the active source.
            let Ok(Some(source)) = self.record_source() else {
                return Ok(None);
            };
            source
        } else {
            self.sealed_record_source(id)?
        };
        let lease = self
            .pins
            .protect_prepared_segment(id)
            .map_err(DirectoryError::from)?;
        Ok(Some(OperationRead {
            scope: self.read_scope(),
            source,
            location: written.location,
            _lease: lease,
        }))
    }
}

impl OperationRead {
    /// Read and verify the operation's framing, number, and digest.
    pub async fn read(self) -> Result<DecodedOperation<'static>, Error> {
        let path = self
            .scope
            .root
            .join("segments")
            .join(self.source.reference.file_name());
        Ok(extent_reader(
            &self.scope.access,
            path,
            self.source.index,
            self.scope.limits,
        )
        .operation(self.location)
        .await?)
    }
}
