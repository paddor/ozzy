//! Keep the predecessor readable while one worker allocates and publishes its successor.

use super::{
    Arc, BufferedRollPublication, DirectoryError, File, OpenGroupJournal, SegmentHeader,
    SegmentPreparation, SegmentWriter, validate_buffered_roll_boundary,
};
use crate::directory::{RollBoundary, publication, roll_manifest_after, segment_name};
use crate::{CurrentReference, Manifest, MetadataLimits};

#[cfg(test)]
mod tests;

/// Immutable predecessor access while roll I/O owns the publication right.
#[derive(Debug)]
#[must_use = "settle the exact roll before restoring mutation"]
pub struct PendingJournalRoll {
    journal: OpenGroupJournal,
    key: Arc<()>,
}

/// Bounded metadata and file ownership for one detached segment roll.
#[derive(Debug)]
#[must_use = "publish on the physical writer and install its completion"]
pub struct PreparedJournalRoll {
    successor: Successor,
    predecessor: Arc<File>,
    boundary: RollBoundary,
    manifest: Manifest,
    current: CurrentReference,
    limits: MetadataLimits,
    data_sync: bool,
    direct: bool,
    key: Arc<()>,
}

/// Unforgeable publication result, including the initialized successor writer.
#[derive(Debug)]
#[must_use = "install on the matching pending journal"]
pub struct CompletedJournalRoll {
    key: Arc<()>,
    result: Result<Rolled, DirectoryError>,
}

#[derive(Debug)]
enum Successor {
    Allocate(SegmentPreparation),
    Ready(super::PreparedSegment),
}

#[derive(Debug)]
struct Rolled {
    writer: SegmentWriter<Arc<File>>,
    manifest: Manifest,
    current: CurrentReference,
}

impl OpenGroupJournal {
    /// Freeze physical mutation, retaining read access to the complete predecessor.
    /// Allocation, data barriers, and metadata publication all run in `publish`.
    /// The successor keeps the predecessor's write mode; no successor data is
    /// written before publication.
    pub fn begin_owned_roll(
        self,
        capacity: u64,
        max_orphan_probes: usize,
    ) -> Result<(PendingJournalRoll, PreparedJournalRoll), DirectoryError> {
        self.begin_owned_roll_with(capacity, max_orphan_probes, None)
    }

    /// As `begin_owned_roll`, using `prepared` as the successor when it was
    /// captured from this segment and generation with this capacity. A
    /// mismatched successor is dropped; its file is left for orphan cleanup.
    pub fn begin_owned_roll_with(
        self,
        capacity: u64,
        max_orphan_probes: usize,
        prepared: Option<super::PreparedSegment>,
    ) -> Result<(PendingJournalRoll, PreparedJournalRoll), DirectoryError> {
        self.require_roll_published()?;
        let boundary = validate_buffered_roll_boundary(&self.directory, &self.writer)?;
        let successor = match prepared {
            Some(prepared)
                if Arc::ptr_eq(&prepared.scope.lock, &self.directory.lock)
                    && prepared.scope.identity == self.directory.identity
                    && prepared.scope.predecessor == self.writer.header().segment_id()
                    && prepared.scope.generation == self.writer.written_position().generation()
                    && prepared.scope.capacity == capacity =>
            {
                Successor::Ready(prepared)
            }
            _ => Successor::Allocate(self.prepare_next_segment(capacity, max_orphan_probes)?),
        };
        let key = Arc::new(());
        let work = PreparedJournalRoll {
            successor,
            predecessor: self.writer.write_handle(),
            boundary,
            manifest: self.directory.manifest.clone(),
            current: self.directory.current,
            limits: self.directory.limits,
            data_sync: self.writer.data_sync(),
            direct: self.writer.is_direct(),
            key: key.clone(),
        };
        Ok((PendingJournalRoll { journal: self, key }, work))
    }
}

impl PreparedJournalRoll {
    /// Allocate, synchronize, and publish exactly the captured roll.
    pub fn publish(self) -> CompletedJournalRoll {
        let key = self.key.clone();
        CompletedJournalRoll {
            key,
            result: self.execute(),
        }
    }

    fn execute(self) -> Result<Rolled, DirectoryError> {
        let prepared = match self.successor {
            Successor::Allocate(preparation) => preparation.prepare()?,
            Successor::Ready(prepared) => prepared,
        };
        let header = SegmentHeader::new(
            prepared.scope.identity.group_id,
            prepared.segment_id,
            Some(self.boundary.active.segment_id),
            self.boundary.next_chain.previous_digest(),
            prepared.scope.capacity,
        )?;
        let manifest = roll_manifest_after(
            &self.manifest,
            self.boundary,
            prepared.segment_id,
            prepared.scope.capacity,
        )?;
        let (manifest, manifest_bytes, manifest_digest) = publication::prepare_manifest(
            &mut publication::Filesystem(&prepared.scope.root),
            &self.manifest,
            manifest,
            self.limits,
        )?;
        let successor_header = crate::codec::encode_segment_header(&header);
        let successor = prepared.file.try_clone()?;
        let mut writer = SegmentWriter::initialize_allocated_at(
            std::sync::Arc::new(prepared.file),
            header,
            self.boundary.writer_generation,
            self.boundary.next_group_number,
            self.boundary.next_chain,
            false,
        )?;
        if self.data_sync {
            writer.set_write_mode(
                &prepared.scope.root.join(segment_name(prepared.segment_id)),
                crate::SegmentWriteMode::DataSync,
            )?;
        }
        if self.direct {
            writer.set_direct(
                &prepared.scope.root.join(segment_name(prepared.segment_id)),
                true,
            )?;
        }
        let published = BufferedRollPublication {
            root: prepared.scope.root,
            lock: prepared.scope.lock,
            expected_current: self.current,
            next_manifest: manifest.clone(),
            manifest_bytes,
            manifest_digest,
            predecessor: self.predecessor,
            successor,
            successor_header,
        }
        .publish()?;
        Ok(Rolled {
            writer,
            manifest,
            current: published.current,
        })
    }
}

impl CompletedJournalRoll {
    pub fn succeeded(&self) -> bool {
        self.result.is_ok()
    }
}

impl PendingJournalRoll {
    pub const fn journal(&self) -> &OpenGroupJournal {
        &self.journal
    }

    /// Only the matching successful publication replaces readable metadata/indexes.
    pub fn complete(
        mut self,
        completed: CompletedJournalRoll,
    ) -> Result<OpenGroupJournal, DirectoryError> {
        if !Arc::ptr_eq(&self.key, &completed.key) {
            return Err(DirectoryError::RollPublicationMismatch);
        }
        let mut rolled = completed.result?;
        self.journal
            .writer
            .transfer_encode_buffers_to(&mut rolled.writer);
        self.journal.writer = rolled.writer;
        self.journal.directory.manifest = rolled.manifest;
        self.journal.directory.current = rolled.current;
        Ok(self.journal)
    }
}
