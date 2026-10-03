//! Owned successor preparation and detached roll publication. The live owner
//! stays read-only until its exact completion is installed.

use super::{Journal, Limits};
use crate::directory::{
    RollBoundary, roll_manifest_after, segment_name, validate_buffered_roll_state,
};
use crate::retention::{PinRegistry, PreparedSegmentLease};
use crate::{
    AsyncSegmentStart, AsyncSegmentWriter, CurrentReference, DirectoryError, GroupIdentity,
    JournalGroupEncoding, Manifest, SegmentHeader, WriterError, async_files::Access,
    async_metadata, encode_manifest_with_limits,
};
use ozzy_io::{Handle, OpenMode, Operation};
use ozzy_journal::progress::JournalGeneration;
use std::{io, ops::Range, path::PathBuf, sync::Arc};

/// Captured allocation request. It contains no final predecessor digest, so
/// appends can continue while this request executes on the storage backend.
#[derive(Debug)]
pub struct Preparation {
    access: Access,
    root: PathBuf,
    identity: GroupIdentity,
    predecessor: u64,
    generation: JournalGeneration,
    capacity: u64,
    max_probes: usize,
    chunk_bytes: usize,
    pins: Arc<PinRegistry>,
}

/// Unselected new file, protected from cleanup. Interrupted zeroing fences it.
/// Dropping it leaves an orphan; this object can never select itself.
#[derive(Debug)]
pub struct Segment {
    scope: Preparation,
    id: u64,
    file: Handle,
    _lease: PreparedSegmentLease,
    faulted: bool,
}

#[derive(Debug)]
/// Captured successor identity and bounded physical preparation inputs.
pub struct Pending {
    journal: Journal,
    key: Arc<()>,
}

#[derive(Debug)]
#[must_use = "publish and install the exact roll completion"]
/// Prepared successor and selected metadata awaiting asynchronous publication.
pub struct Prepared {
    key: Arc<()>,
    directory: async_metadata::Directory,
    successor: Successor,
    predecessor: Handle,
    boundary: RollBoundary,
    manifest: Manifest,
    current: CurrentReference,
    configuration: Option<Vec<u8>>,
    limits: Limits,
}

#[derive(Debug)]
enum Successor {
    Allocate(Preparation),
    Ready(Segment),
}

#[derive(Debug)]
#[must_use = "install on the originating pending owner"]
/// Physical roll publication result awaiting matching owner installation.
pub struct Completed {
    key: Arc<()>,
    result: Result<Rolled, DirectoryError>,
}

#[derive(Debug)]
struct Rolled {
    directory: async_metadata::Directory,
    writer: AsyncSegmentWriter,
    manifest: Manifest,
    current: CurrentReference,
}

impl Journal {
    /// Capture successor preparation inputs without performing file work.
    pub fn prepare_next_segment(
        &self,
        capacity: u64,
        max_probes: usize,
    ) -> Result<Preparation, DirectoryError> {
        self.healthy()?;
        crate::codec::validate_segment_capacity(capacity)?;
        if capacity > self.limits.io.max_segment_bytes || max_probes == 0 {
            return Err(DirectoryError::PreparedSegmentMismatch);
        }
        self.writer
            .header()
            .segment_id()
            .checked_add(1)
            .ok_or(DirectoryError::SegmentIdExhausted)?;
        Ok(Preparation {
            access: self.access.clone(),
            root: self.root().to_path_buf(),
            identity: self.manifest.identity,
            predecessor: self.writer.header().segment_id(),
            generation: self.writer.written_position().generation(),
            capacity,
            max_probes,
            chunk_bytes: self.limits.io.chunk_bytes,
            pins: self.pins.clone(),
        })
    }

    /// Freeze physical/metadata mutation while reads and body preparation retain
    /// the old image. Unlike `roll_active`, this may synchronize a buffered tail.
    pub fn begin_owned_roll(
        self,
        capacity: u64,
        max_probes: usize,
    ) -> Result<(Pending, Prepared), DirectoryError> {
        self.begin_owned_roll_with(capacity, max_probes, None)
    }

    /// Reuse a healthy prepared file from this exact owner/segment/generation.
    /// A stale file is abandoned and allocation probes a fresh unoccupied name.
    pub fn begin_owned_roll_with(
        self,
        capacity: u64,
        max_probes: usize,
        prepared: Option<Segment>,
    ) -> Result<(Pending, Prepared), DirectoryError> {
        self.healthy()?;
        let boundary = validate_buffered_roll_state(&self.manifest, self.writer.state())?;
        let preparation = self.prepare_next_segment(capacity, max_probes)?;
        let successor = match prepared {
            Some(file)
                if !file.faulted
                    && Arc::ptr_eq(&file.scope.pins, &self.pins)
                    && file.scope.identity == self.manifest.identity
                    && file.scope.predecessor == self.writer.header().segment_id()
                    && file.scope.generation == self.writer.written_position().generation()
                    && file.scope.capacity == capacity =>
            {
                Successor::Ready(file)
            }
            _ => Successor::Allocate(preparation),
        };
        let key = Arc::new(());
        let work = Prepared {
            key: key.clone(),
            directory: self.directory.detached_publication()?,
            successor,
            predecessor: self.writer.buffered(),
            boundary,
            manifest: self.manifest.clone(),
            current: self.current,
            configuration: self.configuration.clone(),
            limits: self.limits,
        };
        Ok((Pending { journal: self, key }, work))
    }
}

impl Preparation {
    /// Exclusive-create, allocate and sync without selecting or writing a header.
    pub async fn prepare(self) -> Result<Segment, DirectoryError> {
        let mut id = self.predecessor;
        for _ in 0..self.max_probes {
            id = id
                .checked_add(1)
                .ok_or(DirectoryError::SegmentIdExhausted)?;
            let lease = self.pins.protect_prepared_segment(id)?;
            let file = match self
                .access
                .open(
                    self.root.join(segment_name(id)),
                    OpenMode::CreateNew,
                    false,
                    false,
                )
                .await
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            };
            self.access
                .done(Operation::Allocate {
                    handle: file.clone(),
                    offset: 0,
                    length: self.capacity,
                })
                .await?;
            self.access.sync(&file).await?;
            return Ok(Segment {
                scope: self,
                id,
                file,
                _lease: lease,
                faulted: false,
            });
        }
        Err(DirectoryError::RollProbeLimit {
            limit: self.max_probes,
        })
    }
}

impl Segment {
    /// Configured physical segment capacity in bytes.
    pub const fn capacity(&self) -> u64 {
        self.scope.capacity
    }

    /// Zero and sync a bounded range through chunked backend jobs. A canceled
    /// wait can still change this file; it must never subsequently become selected.
    pub async fn zero_range(&mut self, range: Range<u64>) -> Result<(), DirectoryError> {
        if self.faulted {
            return Err(WriterError::Faulted.into());
        }
        if range.start > range.end || range.end > self.scope.capacity {
            return Err(DirectoryError::PreparedSegmentMismatch);
        }
        self.faulted = true;
        self.scope
            .access
            .zero(&self.file, range, self.scope.chunk_bytes)
            .await?;
        self.scope.access.sync(&self.file).await?;
        self.faulted = false;
        Ok(())
    }
}

impl Prepared {
    /// Execute the prepared metadata publication and return its fenced completion.
    pub async fn publish(self) -> Completed {
        let key = self.key.clone();
        Completed {
            key,
            result: self.execute().await,
        }
    }

    async fn execute(mut self) -> Result<Rolled, DirectoryError> {
        let selected = self
            .directory
            .read_selected(
                self.manifest.identity,
                self.limits.metadata,
                self.configuration.as_deref(),
            )
            .await?;
        if selected.current != self.current || selected.manifest != self.manifest {
            return Err(DirectoryError::CurrentMismatch);
        }
        let prepared = match self.successor {
            Successor::Allocate(work) => work.prepare().await?,
            Successor::Ready(file) => file,
        };
        let next = roll_manifest_after(
            &self.manifest,
            self.boundary,
            prepared.id,
            prepared.scope.capacity,
        )?;
        encode_manifest_with_limits(&next, self.limits.metadata)?;
        let header = SegmentHeader::new(
            self.manifest.identity.group_id,
            prepared.id,
            Some(self.boundary.active.segment_id),
            self.boundary.next_chain.previous_digest(),
            prepared.scope.capacity,
        )?;
        let access = self.directory.access();
        let writer = AsyncSegmentWriter::initialize_prepared(
            prepared.scope.root.join(segment_name(prepared.id)),
            access.clone(),
            prepared.file,
            header,
            AsyncSegmentStart {
                generation: self.boundary.writer_generation,
                first_group_number: self.boundary.next_group_number,
                initial_chain: self.boundary.next_chain,
            },
            self.limits.io,
        )
        .await?;
        // The successor header names the complete written predecessor. Even
        // buffered predecessor bytes must be stable before selecting that link.
        access.sync(&self.predecessor).await?;
        access
            .sync_directory(self.directory.root().join("segments"))
            .await?;
        let (manifest, digest) = self
            .directory
            .publish_manifest(&self.manifest, next, self.limits.metadata)
            .await?;
        let current = CurrentReference {
            group_id: manifest.identity.group_id,
            store_id: manifest.identity.store_id,
            generation: manifest.generation,
            manifest_digest: digest,
        };
        self.directory.select_current(current).await?;
        Ok(Rolled {
            directory: self.directory,
            writer,
            manifest,
            current,
        })
    }
}

impl Completed {
    /// Whether physical execution completed successfully; owner installation remains separate.
    pub fn succeeded(&self) -> bool {
        self.result.is_ok()
    }
}

impl Pending {
    /// Borrow the exact journal owner associated with this work.
    pub const fn journal(&self) -> &Journal {
        &self.journal
    }

    /// Freeze old selected files for reads while successor publication runs.
    /// Index jobs touch derived files only; the detached roll still installs
    /// its exact prepared authority and cannot include later application data.
    pub async fn build_index_snapshot(
        &mut self,
        boundary: crate::JournalIndexBoundary,
        limits: crate::IndexBuildLimits,
    ) -> Result<crate::AsyncJournalIndexSnapshot, crate::JournalIndexError> {
        self.journal.build_index_snapshot(boundary, limits).await
    }

    /// Body work is independent of file placement and cannot change authority.
    pub fn begin_group_encoding(&mut self) -> Result<JournalGroupEncoding, DirectoryError> {
        self.journal.begin_group_encoding()
    }

    /// Accept only this pending roll's result. Errors consume ownership and
    /// require reopen to resolve a possibly completed CURRENT publication.
    pub fn complete(mut self, completed: Completed) -> Result<Journal, DirectoryError> {
        if !Arc::ptr_eq(&self.key, &completed.key) {
            return Err(DirectoryError::RollPublicationMismatch);
        }
        let mut rolled = completed.result?;
        self.journal.writer.transfer_buffers_to(&mut rolled.writer);
        self.journal.writer = rolled.writer;
        self.journal.directory = rolled.directory;
        self.journal.manifest = rolled.manifest;
        self.journal.current = rolled.current;
        Ok(self.journal)
    }
}
