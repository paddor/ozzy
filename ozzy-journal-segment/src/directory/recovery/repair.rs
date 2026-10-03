//! Copy-on-write repair of sealed files under externally acquired group authority.

use std::fs::{File, OpenOptions};
use std::io;
use std::sync::Arc;

use super::super::{
    DirectoryError, GroupDirectory, evidence, position_before, read_limited_file,
    segment_reference_name, sync_directory,
};
use super::{RecoveryPublication, RecoveryPublicationError};
use crate::{
    BodyEncoding, CanonicalOperation, CanonicalRecoveryLimits, DecodeLimits, Digest, LogPosition,
    OpenGroupJournal, OperationLimits, SealedSegment, SegmentHeader, SegmentReference,
    SegmentWriter,
};
#[cfg(test)]
use ozzy_journal::operation::logical_operation_digest;
pub(crate) mod state;
use ozzy_journal::progress::JournalGeneration;

/// A manifest-authenticated logical range, independent of donor file boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairRange {
    pub segment_id: u64,
    pub after: LogPosition,
    pub through: LogPosition,
}

/// Explicit memory, transfer, and replacement-file bounds.
#[derive(Debug, Clone, Copy)]
pub struct SealedRepairLimits {
    pub max_segment_bytes: u64,
    pub max_chunk_operations: usize,
    pub max_chunk_body_bytes: usize,
    pub max_staged_bytes: u64,
    pub max_orphan_probes: usize,
    pub body_encoding: BodyEncoding,
}

/// An exclusively owned, persistently nonvoting physical repair attempt.
///
/// Healthy files and logical metadata remain unchanged. Only replacement files
/// are written. No manifest is published until every range verifies and syncs.
/// Dropping this object leaves the marker and old selected generation intact.
#[derive(Debug)]
pub struct SealedRepair {
    directory: GroupDirectory,
    state: state::State,
    replacements: Vec<SegmentReference>,
    writer: Option<SegmentWriter<Arc<File>>>,
    current_reference: Option<SegmentReference>,
    generation: JournalGeneration,
    staged_bytes: u64,
    faulted: bool,
}

impl GroupDirectory {
    /// Inspect one bounded file at a time. `None` means active-file damage needs
    /// full recovery. Empty ranges mean no sealed repair is needed. Authority
    /// metadata and checkpoints must already have passed metadata-only open.
    pub fn sealed_damage(
        &self,
        decode: DecodeLimits,
        operations: OperationLimits,
        max_segment_bytes: u64,
    ) -> Result<Option<Vec<RepairRange>>, DirectoryError> {
        let mut damaged = Vec::new();
        for (index, reference) in self.manifest.segments.iter().enumerate() {
            if reference.capacity > max_segment_bytes {
                return Err(DirectoryError::SegmentMismatch(reference.segment_id));
            }
            let valid = validate_file(self, index, decode, operations);
            if let Err(DirectoryError::Io(error)) = &valid
                && error.kind() != io::ErrorKind::NotFound
                && error.kind() != io::ErrorKind::UnexpectedEof
            {
                // Permission/device errors do not become payload corruption.
                return valid.map(|()| None);
            }
            if valid.is_err() {
                let Some(next) = self.manifest.segments.get(index + 1) else {
                    return Ok(None);
                };
                damaged.push(RepairRange {
                    segment_id: reference.segment_id,
                    after: position_before(reference.first_chain)?,
                    through: position_before(next.first_chain)?,
                });
            }
        }
        Ok(Some(damaged))
    }

    /// Start repair after fresh recovery authority has selected a pinned donor.
    /// The caller must authenticate every chunk against that donor's exact source
    /// generation and request. Local range checks grant no consensus authority.
    #[expect(
        clippy::too_many_arguments,
        reason = "explicit cold recovery bounds and authority"
    )]
    pub fn begin_sealed_repair(
        self,
        configuration: &[u8],
        generation: JournalGeneration,
        view: u64,
        source_accepted: LogPosition,
        decode: DecodeLimits,
        operations: OperationLimits,
        limits: SealedRepairLimits,
    ) -> Result<SealedRepair, DirectoryError> {
        self.require_recovering(configuration)?;
        state::State::validate_limits(decode, limits)?;
        let ranges = self
            .sealed_damage(decode, operations, limits.max_segment_bytes)?
            .ok_or(DirectoryError::ConfigurationMismatch)?;
        let state = state::State::new(
            &self.manifest,
            ranges,
            view,
            source_accepted,
            decode,
            operations,
            limits,
        )?;
        let mut repair = SealedRepair {
            directory: self,
            state,
            replacements: Vec::new(),
            writer: None,
            current_reference: None,
            generation,
            staged_bytes: 0,
            faulted: false,
        };
        repair.advance_local()?;
        Ok(repair)
    }
}

impl SealedRepair {
    /// Next missing range. The first boundary advances after each complete chunk.
    pub fn pending(&self) -> Result<Option<RepairRange>, DirectoryError> {
        self.require_healthy()?;
        Ok(self.state.pending())
    }

    /// Append canonical operations to a replacement incarnation, never to damaged
    /// bytes. Callers bound requests at `pending().through`, even if donors use
    /// different file boundaries. Every error fences the attempt.
    pub fn append(&mut self, operations: &[CanonicalOperation<'_>]) -> Result<(), DirectoryError> {
        self.require_healthy()?;
        let result = self.append_inner(operations);
        self.faulted |= result.is_err();
        result
    }

    fn append_inner(
        &mut self,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<(), DirectoryError> {
        self.state.append(&self.directory.manifest, operations)?;
        self.advance_local()
    }

    fn advance_local(&mut self) -> Result<(), DirectoryError> {
        while let Some(range) = self.state.ranges.get(self.state.next).copied() {
            if self.state.source.is_empty() {
                self.load_local(range)?;
            }
            self.state.missing += self.state.source[self.state.missing..]
                .iter()
                .take_while(|entry| entry.is_some())
                .count();
            if self.state.missing < self.state.source.len() {
                if self.state.missing >= self.state.missing_end {
                    self.state.missing_end = self.state.source[self.state.missing..]
                        .iter()
                        .position(Option::is_some)
                        .map_or(self.state.source.len(), |end| self.state.missing + end);
                }
                return Ok(());
            }
            self.write_replacement(range)?;
            self.state.source.clear();
            self.state.source_bytes = 0;
            self.state.missing = 0;
            self.state.missing_end = 0;
            self.state.next += 1;
        }
        Ok(())
    }

    fn load_local(&mut self, range: RepairRange) -> Result<(), DirectoryError> {
        let reference = *self
            .directory
            .manifest
            .segments
            .iter()
            .find(|r| r.segment_id == range.segment_id)
            .expect("repair source");
        self.state.prepare(range, reference)?;
        let image = match read_limited_file(
            &self.directory.root.join(segment_reference_name(reference)),
            reference.capacity as usize,
            "repair source",
        ) {
            Ok(image) => image,
            Err(DirectoryError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(DirectoryError::FileLimit { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        self.state
            .load(&self.directory.manifest, range, reference, &image)
    }

    fn write_replacement(&mut self, range: RepairRange) -> Result<(), DirectoryError> {
        self.create_replacement(range.segment_id)?;
        let writer = self.writer.as_mut().expect("replacement writer");
        let mut chunk = Vec::with_capacity(self.state.limits.max_chunk_operations);
        let mut bytes = 0;
        for entry in &self.state.source {
            let entry = entry.as_ref().expect("complete repair source");
            if entry.body.len() > self.state.limits.max_chunk_body_bytes {
                return Err(DirectoryError::SegmentMismatch(range.segment_id));
            }
            if chunk.len() == self.state.limits.max_chunk_operations
                || entry.body.len() > self.state.limits.max_chunk_body_bytes - bytes
            {
                writer.append_with_body_encoding(&chunk, self.state.limits.body_encoding)?;
                chunk.clear();
                bytes = 0;
            }
            bytes += entry.body.len();
            chunk.push(CanonicalOperation {
                group_id: entry.group_id,
                configuration_epoch: entry.configuration_epoch,
                original_view: entry.original_view,
                op_number: entry.op_number,
                previous_digest: entry.previous_digest,
                kind: entry.kind,
                body: &entry.body,
            });
        }
        if !chunk.is_empty() {
            writer.append_with_body_encoding(&chunk, self.state.limits.body_encoding)?;
        }
        if position_before(writer.written_position().next_chain())? != range.through
            || writer.written_position().group_number()
                - (self
                    .current_reference
                    .expect("reference")
                    .first_group_number
                    - 1)
                > self.state.decode.max_groups as u64
        {
            return Err(DirectoryError::SegmentMismatch(range.segment_id));
        }
        writer.sync_through(writer.begin_sync())?;
        let mut reference = self
            .current_reference
            .take()
            .expect("replacement reference");
        reference.sealed = Some(SealedSegment {
            valid_bytes: writer.durable_position().end_offset(),
            digest: writer.structural_digest(),
        });
        self.replacements.push(reference);
        self.writer = None;
        Ok(())
    }

    fn create_replacement(&mut self, id: u64) -> Result<(), DirectoryError> {
        let index = self
            .directory
            .manifest
            .segments
            .iter()
            .position(|reference| reference.segment_id == id)
            .expect("planned segment");
        let mut reference = self.directory.manifest.segments[index];
        let staged = self
            .staged_bytes
            .checked_add(reference.capacity)
            .ok_or(DirectoryError::CurrentMismatch)?;
        if staged > self.state.limits.max_staged_bytes {
            return Err(DirectoryError::SegmentMismatch(id));
        }
        let predecessor = index
            .checked_sub(1)
            .map(|index| self.directory.manifest.segments[index].segment_id);
        let header = SegmentHeader::new(
            self.directory.identity.group_id,
            id,
            predecessor,
            predecessor.map_or(Digest::ZERO, |_| reference.first_chain.previous_digest()),
            reference.capacity,
        )?;
        for _ in 0..self.state.limits.max_orphan_probes {
            reference.file_generation = reference
                .file_generation
                .checked_add(1)
                .ok_or(DirectoryError::SegmentIdExhausted)?;
            let path = self.directory.root.join(segment_reference_name(reference));
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
            super::super::allocate_segment(&file, reference.capacity)?;
            self.writer = Some(SegmentWriter::initialize_allocated_at(
                std::sync::Arc::new(file),
                header.with_file_generation(reference.file_generation),
                self.generation,
                reference.first_group_number,
                reference.first_chain,
                true,
            )?);
            self.current_reference = Some(reference);
            self.staged_bytes = staged;
            return Ok(());
        }
        Err(DirectoryError::RollProbeLimit {
            limit: self.state.limits.max_orphan_probes,
        })
    }

    /// Install all replacements in one manifest publication, privately replay all
    /// retained history, then remove the nonvoting marker. Preserve last-normal
    /// view: restoring bytes does not install the donor's current logical history.
    /// Success permits fenced intact restart only. Uncertain errors consume ownership.
    pub fn finish(
        self,
        configuration: &[u8],
        recovery: CanonicalRecoveryLimits,
    ) -> Result<OpenGroupJournal, RecoveryPublicationError> {
        self.finish_observing(
            configuration,
            recovery,
            &mut super::super::NoopObserver,
            |_| Ok(()),
        )
    }

    fn finish_observing(
        mut self,
        configuration: &[u8],
        recovery: CanonicalRecoveryLimits,
        observer: &mut impl super::super::PersistenceObserver,
        completed: impl FnMut(super::PublicationPhase) -> io::Result<()>,
    ) -> Result<OpenGroupJournal, RecoveryPublicationError> {
        self.require_healthy()?;
        if self.state.next != self.state.ranges.len() || self.writer.is_some() {
            return Err(DirectoryError::CurrentMismatch.into());
        }
        self.directory.require_recovering(configuration)?;
        sync_directory(&self.directory.root.join("segments"))?;
        let mut next = self.directory.manifest.clone();
        next.parent_generation = next.generation;
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(DirectoryError::ManifestGeneration)?;
        next.promised_view = self.state.view;
        for replacement in self.replacements {
            *next
                .segments
                .iter_mut()
                .find(|r| r.segment_id == replacement.segment_id)
                .expect("selected repair segment") = replacement;
        }
        self.directory.install_manifest(next, observer)?;
        let journal =
            self.directory
                .recover(self.generation, self.state.decode, self.state.operations)?;
        let publication = RecoveryPublication {
            current: journal.directory().current(),
            generation: self.generation,
            view: self.state.view,
            accepted: journal.directory().manifest().accepted,
            committed: journal.directory().manifest().committed,
        };
        let (journal, _) = journal.publish_configuration_observing(
            configuration,
            publication,
            recovery,
            true,
            completed,
        )?;
        Ok(journal)
    }

    /// Abandon unpublished work. Orphan cleanup after recovery removes only
    /// unselected incarnations; damaged originals remain selected and nonvoting.
    pub fn abort(self) -> GroupDirectory {
        self.directory
    }

    fn require_healthy(&self) -> Result<(), DirectoryError> {
        if self.faulted {
            Err(crate::WriterError::Faulted.into())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests;

fn validate_file(
    directory: &GroupDirectory,
    index: usize,
    decode: DecodeLimits,
    operations: OperationLimits,
) -> Result<(), DirectoryError> {
    let reference = directory.manifest.segments[index];
    let capacity = usize::try_from(reference.capacity)
        .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
    let image = read_limited_file(
        &directory.root.join(segment_reference_name(reference)),
        capacity,
        "segment",
    )?;
    let protected = if reference.sealed.is_none() {
        evidence::protected(&directory.root, &directory.manifest)?
    } else {
        LogPosition::GENESIS
    };
    state::validate_image(
        &directory.manifest,
        index,
        &image,
        protected,
        decode,
        operations,
    )
}
