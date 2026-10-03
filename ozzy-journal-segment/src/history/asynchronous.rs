//! Frozen history and bounded transfer with backend-owned file operations.

mod validation;
pub use validation::Validation;

use super::{
    HistoryChunk, HistoryError as Error, HistoryState, Source, before, largest_captured_prefix,
    reserve_buffers, validate_segment_capacities,
};
use crate::{
    AsyncJournalLimits, Digest, GroupIdentity, LogPosition, Manifest, SealedSegment,
    SegmentReference, WriterPosition,
    async_files::Access,
    retention::{PinRegistry, PreparedSegmentLease},
};
use ozzy_io::{OpenMode, Operation};
use ozzy_journal::progress::JournalGeneration;
use std::{path::PathBuf, rc::Rc, sync::Arc};

/// One bounded encoded-segment cache plus exact immutable history anchors.
/// Cold reads await file jobs; cached positions and byte validation stay local.
#[derive(Debug)]
pub struct History {
    state: HistoryState<Rc<Captured>>,
    access: Access,
    chunk: usize,
}

#[derive(Debug)]
struct Captured {
    root: PathBuf,
    references: Vec<SegmentReference>,
    _leases: Vec<PreparedSegmentLease>,
}

impl Source for Rc<Captured> {
    fn references(&self) -> &[SegmentReference] {
        &self.references
    }
}
impl Captured {
    fn path(&self, at: usize) -> PathBuf {
        self.root.join(self.references[at].file_name())
    }
}

/// Immutable segment metadata and temporary deletion protection, without
/// payloads or decoding scratch. Clones share the same file lifetime guards.
/// A donor keeps this while a detached reader is canceled or its cache is lost.
#[derive(Debug, Clone)]
pub struct Metadata {
    pin: Rc<Captured>,
    access: Access,
    chunk: usize,
    identity: GroupIdentity,
    generation: JournalGeneration,
    configuration_epoch: u64,
    promised_view: u64,
    through: LogPosition,
    physical_through: LogPosition,
    active_seal: SealedSegment,
    decode: crate::DecodeLimits,
    operations: crate::OperationLimits,
    max_segment_bytes: usize,
}

impl Metadata {
    /// Allocate one bounded reader of the same frozen source, with empty cache.
    /// Owners must separately limit simultaneously executing reader jobs.
    pub fn reader(&self) -> Result<History, Error> {
        let maximum = largest_captured_prefix(&self.pin.references, self.active_seal)?;
        let (bytes, entries, entry_limit) = reserve_buffers(maximum, self.max_segment_bytes)?;
        Ok(History {
            state: HistoryState {
                pin: self.pin.clone(),
                identity: self.identity,
                generation: self.generation,
                configuration_epoch: self.configuration_epoch,
                promised_view: self.promised_view,
                through: self.through,
                physical_through: self.physical_through,
                active_seal: self.active_seal,
                decode: self.decode,
                operations: self.operations,
                bytes,
                entries,
                entry_limit,
                loaded: None,
                #[cfg(test)]
                full_scans: std::cell::Cell::new(0),
            },
            access: self.access.clone(),
            chunk: self.chunk,
        })
    }
}

impl History {
    pub(crate) fn capture(
        access: Access,
        root: PathBuf,
        manifest: &Manifest,
        written: (WriterPosition, Digest),
        pins: &Arc<PinRegistry>,
        limits: AsyncJournalLimits,
        max_segment_bytes: usize,
    ) -> Result<Self, Error> {
        validate_segment_capacities(&manifest.segments, max_segment_bytes)?;
        let active_seal = SealedSegment {
            valid_bytes: written.0.end_offset(),
            digest: written.1,
        };
        let maximum = largest_captured_prefix(&manifest.segments, active_seal)?;
        let (bytes, entries, entry_limit) = reserve_buffers(maximum, max_segment_bytes)?;
        let leases = manifest
            .segments
            .iter()
            .map(|r| pins.protect_prepared_segment(r.segment_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(crate::DirectoryError::from)?;
        let through = before(written.0.next_chain());
        Ok(Self {
            state: HistoryState {
                pin: Rc::new(Captured {
                    root,
                    references: manifest.segments.clone(),
                    _leases: leases,
                }),
                identity: manifest.identity,
                generation: written.0.generation(),
                configuration_epoch: manifest.configuration_epoch,
                promised_view: manifest.promised_view,
                through,
                physical_through: through,
                active_seal,
                decode: limits.decode,
                operations: limits.operations,
                bytes,
                entries,
                entry_limit,
                loaded: None,
                #[cfg(test)]
                full_scans: std::cell::Cell::new(0),
            },
            access,
            chunk: limits.io.chunk_bytes,
        })
    }

    /// Exact group, node, volume, store, and store-generation binding.
    pub const fn identity(&self) -> GroupIdentity {
        self.state.identity()
    }

    /// Capture metadata only. Retaining this does not retain the encoded-segment
    /// arena, and creating another reader cannot extend the captured prefix.
    pub fn metadata(&self) -> Metadata {
        Metadata {
            pin: self.state.pin.clone(),
            access: self.access.clone(),
            chunk: self.chunk,
            identity: self.state.identity,
            generation: self.state.generation,
            configuration_epoch: self.state.configuration_epoch,
            promised_view: self.state.promised_view,
            through: self.state.through,
            physical_through: self.state.physical_through,
            active_seal: self.state.active_seal,
            decode: self.state.decode,
            operations: self.state.operations,
            max_segment_bytes: self.state.bytes.capacity(),
        }
    }

    /// Reuse an already checked physical prefix when a fresh live capture only
    /// extends the same segment. The next read checks every new group and the
    /// new captured seal before exposing any suffix operation.
    pub fn reuse_checked_prefix(&mut self, previous: &mut Self) -> Result<bool, Error> {
        let Some(loaded) = previous.state.loaded.as_ref() else {
            return Ok(false);
        };
        let index = loaded.index;
        let Some(&old) = previous.state.pin.references.get(index) else {
            return Ok(false);
        };
        let Some(&new) = self.state.pin.references.get(index) else {
            return Ok(false);
        };
        if self.state.identity != previous.state.identity
            || self.state.generation != previous.state.generation
            || self.state.configuration_epoch != previous.state.configuration_epoch
            || self.state.promised_view != previous.state.promised_view
            || self.state.decode != previous.state.decode
            || self.state.operations != previous.state.operations
            || (
                old.segment_id,
                old.file_generation,
                old.first_group_number,
                old.first_chain,
                old.capacity,
            ) != (
                new.segment_id,
                new.file_generation,
                new.first_group_number,
                new.first_chain,
                new.capacity,
            )
            || loaded.valid_bytes != previous.state.bytes.len() as u64
            || loaded.digest.finish() != previous.state.seal(index).digest
            || loaded.valid_bytes > self.state.seal(index).valid_bytes
            || previous.state.through.op_number > self.state.through.op_number
            || (previous.state.through.op_number == self.state.through.op_number
                && previous.state.through != self.state.through)
            || (loaded.valid_bytes == self.state.seal(index).valid_bytes
                && loaded.digest.finish() != self.state.seal(index).digest)
        {
            return Ok(false);
        }
        let target =
            usize::try_from(self.state.seal(index).valid_bytes).map_err(|_| Error::Capacity)?;
        let capacity = usize::try_from(new.capacity).map_err(|_| Error::Capacity)?;
        let extra = (capacity - target).min(16 * 1024 * 1024);
        previous
            .state
            .bytes
            .try_reserve_exact(target + extra - previous.state.bytes.len())
            .map_err(|_| Error::Capacity)?;
        previous
            .state
            .entries
            .try_reserve(
                256.min(
                    self.state
                        .entry_limit
                        .saturating_sub(previous.state.entries.len()),
                ),
            )
            .map_err(|_| Error::Capacity)?;
        self.state.bytes = std::mem::take(&mut previous.state.bytes);
        self.state.entries = std::mem::take(&mut previous.state.entries);
        self.state.loaded = previous.state.loaded.take();
        Ok(true)
    }
    /// Journal-owner generation fencing these captured bytes or completions.
    pub const fn generation(&self) -> JournalGeneration {
        self.state.generation()
    }
    /// Exact upper canonical operation prefix visible through this capture.
    pub const fn through(&self) -> LogPosition {
        self.state.through()
    }
    /// Exact canonical prefix immediately before captured history.
    pub fn predecessor(&self) -> LogPosition {
        self.state.predecessor()
    }

    /// Exact history evidence only, never a vote or confirmation decision.
    pub async fn position(&mut self, op: u64) -> Result<Option<LogPosition>, Error> {
        if op > self.predecessor().op_number && op <= self.through().op_number {
            let at = self.state.segment_for(op)?;
            self.load(at).await?;
        }
        self.state.position_loaded(op)
    }

    /// Return a nonempty bounded prefix. A response may stop at a segment
    /// boundary. Wrong anchors and an undersized first-record budget are errors.
    pub async fn read_after(
        &mut self,
        predecessor: LogPosition,
        max_operations: usize,
        max_body_bytes: usize,
    ) -> Result<HistoryChunk<'_>, Error> {
        let at = self
            .state
            .read_index(predecessor, max_operations, max_body_bytes)?;
        self.load(at).await?;
        self.state
            .read_after_loaded(predecessor, max_operations, max_body_bytes)
    }

    pub(crate) async fn restrict(mut self, through: LogPosition) -> Result<Self, Error> {
        if self.position(through.op_number).await? != Some(through) {
            return Err(Error::Source);
        }
        self.state.through = through;
        Ok(self)
    }

    async fn load(&mut self, at: usize) -> Result<(), Error> {
        if let Some(loaded) = self.state.loaded.as_ref()
            && loaded.index == at
        {
            let start = usize::try_from(loaded.valid_bytes).map_err(|_| Error::Capacity)?;
            let seal = self.state.seal(at);
            if loaded.valid_bytes == seal.valid_bytes {
                let expected = self
                    .state
                    .pin
                    .references
                    .get(at + 1)
                    .map_or(self.state.physical_through, |next| before(next.first_chain));
                return if loaded.digest.finish() == seal.digest
                    && before(loaded.next_chain) == expected
                {
                    Ok(())
                } else {
                    Err(Error::Source)
                };
            }
            let length = usize::try_from(seal.valid_bytes).map_err(|_| Error::Capacity)?;
            let reference = self.state.pin.references[at];
            if start > length || seal.valid_bytes > reference.capacity {
                return Err(Error::Source);
            }
            self.state.bytes.truncate(start);
            let file = self
                .access
                .open(self.state.pin.path(at), OpenMode::Read, false, false)
                .await?;
            let actual = self.access.length(&file).await?;
            if actual < seal.valid_bytes || actual > reference.capacity {
                return Err(Error::Source);
            }
            let read = self
                .access
                .read_append(
                    &file,
                    start as u64,
                    length - start,
                    self.chunk,
                    &mut self.state.bytes,
                )
                .await;
            self.access.done(Operation::Close { handle: file }).await?;
            read?;
            if let Err(error) = self.state.extend_loaded(at) {
                self.state.loaded = None;
                self.state.entries.clear();
                self.state.bytes.clear();
                return Err(error);
            }
            return Ok(());
        }
        self.state.loaded = None;
        self.state.entries.clear();
        self.state.bytes.clear();
        let reference = self.state.pin.references[at];
        let seal = self.state.seal(at);
        let length = usize::try_from(seal.valid_bytes).map_err(|_| Error::Capacity)?;
        if seal.valid_bytes > reference.capacity || length > self.state.bytes.capacity() {
            return Err(Error::Source);
        }
        let file = self
            .access
            .open(self.state.pin.path(at), OpenMode::Read, false, false)
            .await?;
        let actual = self.access.length(&file).await?;
        if actual < seal.valid_bytes || actual > reference.capacity {
            return Err(Error::Source);
        }
        self.access
            .read_append(&file, 0, length, self.chunk, &mut self.state.bytes)
            .await?;
        self.access.done(Operation::Close { handle: file }).await?;
        let mut entries = std::mem::take(&mut self.state.entries);
        let result = self.state.index_segment(at, &mut entries);
        self.state.entries = entries;
        self.state.loaded = Some(result?);
        Ok(())
    }
}
