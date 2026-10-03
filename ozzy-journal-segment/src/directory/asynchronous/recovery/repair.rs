//! Copy-on-write physical repair. Selection and voting eligibility remain
//! separate publications; all old selected files survive an interrupted repair.

use super::{Journal, RecoveryDirectory, recovery_marker};
use crate::directory::{position_before, recovery::repair::state, segment_reference_name};
use crate::{
    AsyncSegmentStart, AsyncSegmentWriter, CanonicalOperation, CanonicalRecoveryLimits,
    CurrentReference, Digest, DirectoryError, LogPosition, RecoveryPublication,
    RecoveryPublicationError, RepairRange, SealedRepairLimits, SealedSegment, SegmentHeader,
    SegmentReference, WriterError,
};
use ozzy_io::{OpenMode, Operation};
use ozzy_journal::progress::JournalGeneration;
use std::io;

/// Exclusively owned nonvoting repair. Donor requests must be authenticated by
/// the caller against its selected source generation and external authority.
#[derive(Debug)]
pub struct Repair {
    directory: RecoveryDirectory,
    state: state::State,
    replacements: Vec<SegmentReference>,
    generation: JournalGeneration,
    staged_bytes: u64,
    faulted: bool,
}

impl RecoveryDirectory {
    async fn read_segment(&self, reference: SegmentReference) -> Result<Vec<u8>, DirectoryError> {
        let capacity = usize::try_from(reference.capacity)
            .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
        let handle = self
            .access
            .open(
                self.root().join(segment_reference_name(reference)),
                OpenMode::Read,
                false,
                false,
            )
            .await?;
        let result = async {
            let length = self.access.length(&handle).await?;
            if length > reference.capacity {
                return Err(DirectoryError::FileLimit {
                    object: "repair source",
                    actual: length,
                    limit: capacity,
                });
            }
            Ok(self
                .access
                .read_range(&handle, 0, length as usize, self.limits.io.chunk_bytes)
                .await?)
        }
        .await;
        let closed = self.access.done(Operation::Close { handle }).await;
        let image = result?;
        closed?;
        Ok(image)
    }

    /// Inspect bounded source images without modifying them. `None` means active
    /// history is damaged and requires full recovery instead of sealed repair.
    /// Device/permission errors are not classified as corrupt payloads.
    pub async fn sealed_damage(
        &mut self,
        max_segment_bytes: u64,
    ) -> Result<Option<Vec<RepairRange>>, DirectoryError> {
        let configuration = self.configuration.clone();
        self.require_selected(&configuration).await?;
        let protected = self
            .directory
            .load_evidence(&self.manifest)
            .await?
            .protected(&self.manifest)?;
        let mut damaged = Vec::new();
        for (index, reference) in self.manifest.segments.iter().enumerate() {
            if reference.capacity > max_segment_bytes
                || reference.capacity > self.limits.io.max_segment_bytes
            {
                return Err(DirectoryError::SegmentMismatch(reference.segment_id));
            }
            let valid = match self.read_segment(*reference).await {
                Ok(image) => state::validate_image(
                    &self.manifest,
                    index,
                    &image,
                    protected,
                    self.limits.decode,
                    self.limits.operations,
                ),
                Err(error) => Err(error),
            };
            if let Err(DirectoryError::Io(error)) = &valid
                && error.kind() != io::ErrorKind::NotFound
                && error.kind() != io::ErrorKind::UnexpectedEof
            {
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

    /// Start only after external recovery authority has selected a pinned donor.
    /// Healthy local fragments are salvaged using the shared repair validator.
    pub async fn begin_sealed_repair(
        mut self,
        configuration: &[u8],
        generation: JournalGeneration,
        view: u64,
        source_accepted: LogPosition,
        limits: SealedRepairLimits,
    ) -> Result<Repair, DirectoryError> {
        let marker = recovery_marker(configuration)?;
        if self.configuration != marker {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.require_selected(&marker).await?;
        state::State::validate_limits(self.limits.decode, limits)?;
        let ranges = self
            .sealed_damage(limits.max_segment_bytes)
            .await?
            .ok_or(DirectoryError::ConfigurationMismatch)?;
        let state = state::State::new(
            &self.manifest,
            ranges,
            view,
            source_accepted,
            self.limits.decode,
            self.limits.operations,
            limits,
        )?;
        let mut repair = Repair {
            directory: self,
            state,
            replacements: Vec::new(),
            generation,
            staged_bytes: 0,
            faulted: false,
        };
        repair.advance_local().await?;
        Ok(repair)
    }
}

impl Repair {
    fn healthy(&self) -> Result<(), DirectoryError> {
        if self.faulted {
            Err(WriterError::Faulted.into())
        } else {
            Ok(())
        }
    }

    /// Exact missing logical range, independent of donor file/group boundaries.
    pub fn pending(&self) -> Result<Option<RepairRange>, DirectoryError> {
        self.healthy()?;
        Ok(self.state.pending())
    }

    /// Validate a bounded donor chunk, then asynchronously write any completed
    /// replacement. Every error or canceled wait fences this attempt.
    pub async fn append(
        &mut self,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<(), DirectoryError> {
        self.healthy()?;
        self.faulted = true;
        self.state.append(&self.directory.manifest, operations)?;
        self.advance_local().await?;
        self.faulted = false;
        Ok(())
    }

    async fn advance_local(&mut self) -> Result<(), DirectoryError> {
        while let Some(range) = self.state.ranges.get(self.state.next).copied() {
            if self.state.source.is_empty() {
                self.load_local(range).await?;
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
            self.write_replacement(range).await?;
            self.state.source.clear();
            self.state.source_bytes = 0;
            self.state.missing = 0;
            self.state.missing_end = 0;
            self.state.next += 1;
        }
        Ok(())
    }

    async fn load_local(&mut self, range: RepairRange) -> Result<(), DirectoryError> {
        let reference = *self
            .directory
            .manifest
            .segments
            .iter()
            .find(|r| r.segment_id == range.segment_id)
            .expect("repair source");
        self.state.prepare(range, reference)?;
        let image = match self.directory.read_segment(reference).await {
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

    async fn write_replacement(&mut self, range: RepairRange) -> Result<(), DirectoryError> {
        let (mut reference, mut writer) = self.create_replacement(range.segment_id).await?;
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
                writer
                    .append_with_limits(
                        &chunk,
                        self.state.limits.body_encoding,
                        Some(self.state.decode),
                    )
                    .await?;
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
            writer
                .append_with_limits(
                    &chunk,
                    self.state.limits.body_encoding,
                    Some(self.state.decode),
                )
                .await?;
        }
        if position_before(writer.written_position().next_chain())? != range.through
            || writer.written_position().group_number() - (reference.first_group_number - 1)
                > self.state.decode.max_groups as u64
        {
            return Err(DirectoryError::SegmentMismatch(range.segment_id));
        }
        writer.sync_through(writer.written_position()).await?;
        reference.sealed = Some(SealedSegment {
            valid_bytes: writer.durable_position().end_offset(),
            digest: writer.state().structural_digest(),
        });
        writer.close().await?;
        self.replacements.push(reference);
        Ok(())
    }

    async fn create_replacement(
        &mut self,
        id: u64,
    ) -> Result<(SegmentReference, AsyncSegmentWriter), DirectoryError> {
        let index = self
            .directory
            .manifest
            .segments
            .iter()
            .position(|r| r.segment_id == id)
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
            .map(|i| self.directory.manifest.segments[i].segment_id);
        let header = SegmentHeader::new(
            self.directory.manifest.identity.group_id,
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
            let writer = AsyncSegmentWriter::create(
                self.directory
                    .root()
                    .join(segment_reference_name(reference)),
                self.directory.access.io.clone(),
                self.directory.access.protection.clone(),
                header
                    .clone()
                    .with_file_generation(reference.file_generation),
                AsyncSegmentStart {
                    generation: self.generation,
                    first_group_number: reference.first_group_number,
                    initial_chain: reference.first_chain,
                },
                self.directory.limits.io,
            )
            .await;
            match writer {
                Ok(writer) => {
                    self.staged_bytes = staged;
                    return Ok((reference, writer));
                }
                Err(WriterError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(DirectoryError::RollProbeLimit {
            limit: self.state.limits.max_orphan_probes,
        })
    }

    /// Select complete synchronized replacements, replay privately, then restore
    /// configuration. Physical repair preserves the previous last-normal view.
    /// Success permits fenced intact restart, never same-view voting.
    pub async fn finish(
        mut self,
        configuration: &[u8],
        recovery: CanonicalRecoveryLimits,
    ) -> Result<Journal, RecoveryPublicationError> {
        self.healthy()?;
        if self.state.next != self.state.ranges.len() {
            return Err(DirectoryError::CurrentMismatch.into());
        }
        let marker = recovery_marker(configuration)?;
        if self.directory.configuration != marker {
            return Err(DirectoryError::ConfigurationMismatch.into());
        }
        self.directory.require_selected(&marker).await?;
        self.directory
            .access
            .sync_directory(self.directory.root().join("segments"))
            .await?;
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
        let (manifest, manifest_digest) = self
            .directory
            .directory
            .publish_manifest(
                &self.directory.manifest,
                next,
                self.directory.limits.metadata,
            )
            .await?;
        let current = CurrentReference {
            group_id: manifest.identity.group_id,
            store_id: manifest.identity.store_id,
            generation: manifest.generation,
            manifest_digest,
        };
        self.directory.directory.select_current(current).await?;
        let selected = self
            .directory
            .directory
            .read_selected(
                manifest.identity,
                self.directory.limits.metadata,
                Some(&marker),
            )
            .await?;
        let mut journal = Journal::recover_selected(
            self.directory.directory,
            selected,
            self.generation,
            self.directory.limits,
        )
        .await?;
        let publication = RecoveryPublication {
            current: journal.current(),
            generation: self.generation,
            view: self.state.view,
            accepted: journal.manifest.accepted,
            committed: journal.manifest.committed,
        };
        journal
            .publish_configuration(configuration, publication, recovery, true)
            .await?;
        Ok(journal)
    }

    /// Abandon unpublished replacements. Selected damaged originals and the
    /// nonvoting marker remain untouched; interrupted metadata requires reopen.
    pub fn abort(self) -> RecoveryDirectory {
        self.directory
    }
}
