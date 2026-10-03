//! Prepared successor allocation and buffered roll publication.

use super::{
    Arc, BufferedRollPublication, DirectoryError, File, GroupIdentity, JournalGeneration,
    OpenGroupJournal, OpenOptions, PathBuf, PublishedBufferedRoll, SegmentHeader, SegmentWriter,
    StoreLock, allocate_segment, encode_manifest_with_limits, io, manifest_digest, roll_manifest,
    segment_name, validate_buffered_roll_boundary,
};

const DEFAULT_ORPHAN_PROBES: usize = 64;

mod detached;
pub use detached::{CompletedJournalRoll, PendingJournalRoll, PreparedJournalRoll};

/// Allocation work captured without disk I/O from one live journal generation.
/// The final successor header is deliberately not constructed until rollover.
#[derive(Debug)]
pub struct SegmentPreparation {
    root: PathBuf,
    lock: Arc<StoreLock>,
    identity: GroupIdentity,
    predecessor: u64,
    generation: JournalGeneration,
    capacity: u64,
    max_orphan_probes: usize,
    pins: Arc<crate::retention::PinRegistry>,
}

/// One freshly created, allocated, unselected successor, owned by its journal.
/// Dropping it leaves an unreferenced file for explicit orphan cleanup; it can
/// never select itself or overwrite another attempt's file.
#[derive(Debug)]
pub struct PreparedSegment {
    scope: SegmentPreparation,
    segment_id: u64,
    file: File,
    _lease: crate::retention::PreparedSegmentLease,
}

impl SegmentPreparation {
    /// Create and allocate one successor on a blocking maintenance worker.
    /// Occupied names are skipped within the explicit probe bound, never reused.
    pub fn prepare(self) -> Result<PreparedSegment, DirectoryError> {
        let mut segment_id = self.predecessor;
        for _ in 0..self.max_orphan_probes {
            segment_id = segment_id
                .checked_add(1)
                .ok_or(DirectoryError::SegmentIdExhausted)?;
            let path = self.root.join(segment_name(segment_id));
            let lease = self.pins.protect_prepared_segment(segment_id)?;
            let file = match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            };
            allocate_segment(&file, self.capacity)?;
            // Make the allocation durable here, off the journal owner, so the
            // roll's publication synchronizes only the successor header.
            file.sync_data()?;
            return Ok(PreparedSegment {
                scope: self,
                segment_id,
                file,
                _lease: lease,
            });
        }
        Err(DirectoryError::RollProbeLimit {
            limit: self.max_orphan_probes,
        })
    }
}

impl PreparedSegment {
    pub const fn capacity(&self) -> u64 {
        self.scope.capacity
    }

    /// Overwrite `[offset, offset + zeros.len())` with zeros and sync the data.
    /// Later `O_DSYNC` writes then land on written extents and skip the
    /// filesystem's unwritten-extent conversion. The range must stay within
    /// the allocation; the header region is rewritten at roll.
    ///
    /// TODO: use `fallocate(FALLOC_FL_WRITE_ZEROES)` (Linux 6.17+) once it is
    /// widely available; the device then zeroes without transferring data.
    pub fn zero_range(&self, offset: u64, zeros: &[u8]) -> Result<(), DirectoryError> {
        use std::os::unix::fs::FileExt;
        if offset
            .checked_add(zeros.len() as u64)
            .is_none_or(|end| end > self.scope.capacity)
        {
            return Err(DirectoryError::PreparedSegmentMismatch);
        }
        debug_assert!(zeros.iter().all(|&byte| byte == 0));
        self.file.write_all_at(zeros, offset)?;
        self.file.sync_data()?;
        Ok(())
    }
}

impl OpenGroupJournal {
    /// Capture one successor allocation request. Later appends are allowed;
    /// using its result after a writer/segment replacement is rejected.
    pub fn prepare_next_segment(
        &self,
        capacity: u64,
        max_orphan_probes: usize,
    ) -> Result<SegmentPreparation, DirectoryError> {
        if self.writer.is_faulted() {
            return Err(super::WriterError::Faulted.into());
        }
        let predecessor = self.writer.header().segment_id();
        predecessor
            .checked_add(1)
            .ok_or(DirectoryError::SegmentIdExhausted)?;
        crate::codec::validate_segment_capacity(capacity)?;
        Ok(SegmentPreparation {
            root: self.directory.root.clone(),
            lock: self.directory.lock.clone(),
            identity: self.directory.identity,
            predecessor,
            generation: self.writer.written_position().generation(),
            capacity,
            max_orphan_probes,
            pins: Arc::clone(&self.pins),
        })
    }

    /// Switch writes to a successor before the roll's durability barriers run.
    ///
    /// The returned publication must run on a blocking I/O thread and its
    /// result must be completed before another roll or metadata publication.
    /// Until then, a crash reopens the predecessor selected by `CURRENT` and
    /// may discard buffered writes in the successor.
    pub fn begin_buffered_roll(
        self,
        new_capacity: u64,
    ) -> Result<(Self, BufferedRollPublication), DirectoryError> {
        self.require_roll_published()?;
        validate_buffered_roll_boundary(&self.directory, &self.writer)?;
        let prepared = self
            .prepare_next_segment(new_capacity, DEFAULT_ORPHAN_PROBES)?
            .prepare()?;
        self.begin_buffered_roll_prepared(prepared)
    }

    /// Finish a prepared successor with the predecessor's final digest, then
    /// switch writes without waiting for the returned durability publication.
    pub fn begin_buffered_roll_prepared(
        self,
        prepared: PreparedSegment,
    ) -> Result<(Self, BufferedRollPublication), DirectoryError> {
        self.require_roll_published()?;
        if !Arc::ptr_eq(&prepared.scope.lock, &self.directory.lock)
            || prepared.scope.identity != self.directory.identity
            || prepared.scope.predecessor != self.writer.header().segment_id()
            || prepared.scope.generation != self.writer.written_position().generation()
        {
            return Err(DirectoryError::PreparedSegmentMismatch);
        }
        let Self {
            mut directory,
            mut writer,
            evidence,
            decode_limits,
            operation_limits,
            pins,
        } = self;
        if directory.current.generation != directory.manifest.generation {
            return Err(DirectoryError::RollPublicationPending);
        }
        let boundary = validate_buffered_roll_boundary(&directory, &writer)?;
        let expected_current = directory.current;

        let next_header = SegmentHeader::new(
            directory.identity.group_id,
            prepared.segment_id,
            Some(boundary.active.segment_id),
            boundary.next_chain.previous_digest(),
            prepared.scope.capacity,
        )?;
        let next_manifest = roll_manifest(
            &directory,
            boundary,
            prepared.segment_id,
            prepared.scope.capacity,
        )?;
        let (next_manifest, manifest_bytes, manifest_digest) =
            directory.prepare_manifest_install(next_manifest)?;
        let successor_header = crate::codec::encode_segment_header(&next_header);
        let successor = prepared.file.try_clone()?;
        let mut next_writer = SegmentWriter::initialize_allocated_at(
            std::sync::Arc::new(prepared.file),
            next_header,
            boundary.writer_generation,
            boundary.next_group_number,
            boundary.next_chain,
            false,
        )?;
        if directory.data_sync {
            next_writer.set_write_mode(
                &directory.root.join(segment_name(prepared.segment_id)),
                crate::SegmentWriteMode::DataSync,
            )?;
        }
        if directory.direct {
            next_writer.set_direct(
                &directory.root.join(segment_name(prepared.segment_id)),
                true,
            )?;
        }
        writer.transfer_encode_buffers_to(&mut next_writer);
        let predecessor = writer.into_inner();
        directory.manifest = next_manifest.clone();
        let publication = BufferedRollPublication {
            root: directory.root.clone(),
            lock: Arc::clone(&directory.lock),
            expected_current,
            next_manifest,
            manifest_bytes,
            manifest_digest,
            predecessor,
            successor,
            successor_header,
        };
        Ok((
            Self {
                directory,
                evidence,
                writer: next_writer,
                decode_limits,
                operation_limits,
                pins,
            },
            publication,
        ))
    }

    /// Install completion evidence returned by the paired buffered publication.
    pub fn complete_buffered_roll(
        mut self,
        published: PublishedBufferedRoll,
    ) -> Result<Self, DirectoryError> {
        if self.directory.current != published.previous
            || self.directory.manifest.generation != published.current.generation
            || self.directory.manifest.identity.group_id != published.current.group_id
            || self.directory.manifest.identity.store_id != published.current.store_id
        {
            return Err(DirectoryError::RollPublicationMismatch);
        }
        let bytes = encode_manifest_with_limits(
            &self.directory.manifest,
            self.directory.metadata_limits(),
        )?;
        if manifest_digest(&bytes, self.directory.metadata_limits())?
            != published.current.manifest_digest
        {
            return Err(DirectoryError::RollPublicationMismatch);
        }
        self.directory.current = published.current;
        Ok(self)
    }
}
