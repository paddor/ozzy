//! Bounded staging with one final selection. Transport transfer and consensus
//! authority stay outside this physical installer.

use super::Journal;
use crate::suffix::{OwnedOperation, enforce_limit, streaming};
use crate::{
    CanonicalOperation, ChainPosition, CodecError, Digest, LogPosition, SEGMENT_HEADER_BYTES,
    SealedSegment, SegmentHeader, SegmentReference, SegmentStart, SegmentWriter, SuffixReplacement,
    SuffixReplacementError as Error, SuffixStreamLimits, WriterError, metadata::position_before,
};
use ozzy_io::Operation;
use std::io;

mod begin;

/// Owns the old selected journal plus bounded, unselected replacement files.
/// Dropping/canceling this object leaves originals and selection intact unless
/// final publication started. Reopen resolves any ambiguous final publication.
#[derive(Debug)]
pub struct Installer {
    checkpoint_position: Option<LogPosition>,
    checkpoint_seen: bool,
    journal: Journal,
    request: SuffixRequest,
    writer: SegmentWriter,
    segments: Vec<SegmentReference>,
    body_digests: Vec<Digest>,
    staged_bytes: u64,
    committed_seen: bool,
    faulted: bool,
}

/// One streamed suffix replacement: the selection it installs, the accepted
/// prefix its staged operations must end at, and its bounds.
#[derive(Debug, Clone, Copy)]
pub struct SuffixRequest {
    /// Consensus authorization and resulting hard state.
    pub replacement: SuffixReplacement,
    /// Exact accepted prefix the staged suffix must reach.
    pub accepted: LogPosition,
    /// Memory, work, and disk bounds for this attempt.
    pub limits: SuffixStreamLimits,
}

/// Original checkpoint and predecessor of its required retained operations.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointRecovery {
    /// Original hash anchor before required retained operations.
    pub predecessor: LogPosition,
    /// Original confirmed prefix represented by canonical state.
    pub position: LogPosition,
}

/// Privately decoded state to publish under destination-local store identity.
#[derive(Debug)]
pub struct CanonicalCheckpointImport<'a> {
    /// Fresh destination-local checkpoint artifact identity.
    pub id: ozzy_proto::CheckpointId,
    /// Exact original history prefix, independently checked against retained bytes.
    pub position: LogPosition,
    /// Complete original image, including its independent identity section.
    pub image: &'a super::CheckpointInbox,
    /// Live canonical metadata bounds for private validation.
    pub state_limits: ozzy_core::state::StateLimits,
    /// Independent byte and name bounds for the core snapshot section.
    pub snapshot_limits: ozzy_core::state::StateSnapshotLimits,
}

#[derive(Clone, Copy)]
struct Start {
    first_id: u64,
    predecessor: Option<u64>,
    predecessor_digest: Digest,
    first_group: u64,
    first_chain: ChainPosition,
}

impl Installer {
    fn healthy(&self) -> Result<(), Error> {
        if self.faulted {
            Err(Error::StagingFaulted)
        } else {
            Ok(())
        }
    }

    async fn copy_retained(&mut self, retained: &[OwnedOperation]) -> Result<(), Error> {
        let mut borrowed = Vec::new();
        borrowed
            .try_reserve_exact(self.request.limits.max_group_operations)
            .map_err(|_| Error::Allocation)?;
        let mut bytes = 0usize;
        for operation in retained {
            let operation = operation.borrowed();
            if operation.body.len() > self.request.limits.max_group_body_bytes {
                return Err(Error::InvalidLimits);
            }
            if borrowed.len() == self.request.limits.max_group_operations
                || operation.body.len() > self.request.limits.max_group_body_bytes - bytes
            {
                self.append_chunk(&borrowed).await?;
                borrowed.clear();
                bytes = 0;
            }
            bytes += operation.body.len();
            borrowed.push(operation);
        }
        if !borrowed.is_empty() {
            self.append_chunk(&borrowed).await?;
        }
        Ok(())
    }

    /// Validate one whole bounded chunk before changing the staged prefix.
    /// Bad input remains retryable. File errors/cancellation fence this installer.
    pub async fn append_chunk(
        &mut self,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<(), Error> {
        self.healthy()?;
        let (bytes, committed_seen) = streaming::validate_chunk(
            &self.journal.suffix_source(),
            &self.request,
            self.writer.written_position().next_chain(),
            &mut self.body_digests,
            operations,
        )?;
        let checkpoint_seen = if let Some(position) = self.checkpoint_position {
            if let Some(operation) = operations
                .iter()
                .find(|operation| operation.op_number == position.op_number)
            {
                if ozzy_journal::operation::logical_operation_digest(operation) != position.digest {
                    return Err(Error::CommitNotSelected);
                }
                true
            } else {
                false
            }
        } else {
            false
        };
        self.faulted = true;
        self.write_chunk(operations, bytes).await?;
        self.committed_seen |= committed_seen;
        self.checkpoint_seen |= checkpoint_seen;
        self.faulted = false;
        Ok(())
    }

    // Iterative depth-first splitting replaces recursive async futures. Stack
    // depth is logarithmic in the already bounded canonical-operation count.
    async fn write_chunk(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        body_bytes: usize,
    ) -> Result<(), Error> {
        let mut parts = Vec::with_capacity(usize::BITS as usize + 1);
        parts.push((0..operations.len(), body_bytes));
        while let Some((range, bytes)) = parts.pop() {
            let reference = super::active_segment(&self.segments)?;
            let groups =
                self.writer.written_position().group_number() - (reference.first_group_number - 1);
            let decoded = self.writer.written_position().decoded_body_bytes();
            if groups >= self.journal.limits.decode.max_groups as u64
                || bytes > self.journal.limits.decode.max_segment_decoded_body_bytes - decoded
            {
                self.roll().await?;
            }
            let result = self
                .writer
                .append_with_limits_and_digests(
                    &operations[range.clone()],
                    &self.body_digests[range.clone()],
                    self.request.replacement.body_encoding,
                    Some(self.journal.limits.decode),
                )
                .await;
            match result {
                Ok(_) => {}
                Err(WriterError::Codec(CodecError::GroupExceedsSegment))
                    if self.writer.written_position().end_offset()
                        > SEGMENT_HEADER_BYTES as u64 =>
                {
                    self.roll().await?;
                    parts.push((range, bytes));
                }
                Err(WriterError::Codec(CodecError::GroupExceedsSegment)) if range.len() > 1 => {
                    let middle = range.start + range.len() / 2;
                    let left = range.start..middle;
                    let left_bytes = operations[left.clone()]
                        .iter()
                        .map(|op| op.body.len())
                        .sum();
                    parts.push((middle..range.end, bytes - left_bytes));
                    parts.push((left, left_bytes));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    async fn roll(&mut self) -> Result<(), Error> {
        enforce_limit(
            "selected manifest segments",
            self.segments.len() + 1,
            self.request.limits.max_segments,
        )?;
        let staged = self
            .staged_bytes
            .checked_add(self.request.replacement.segment_capacity)
            .ok_or(Error::LengthOverflow)?;
        if staged > self.request.limits.max_staged_bytes {
            return Err(Error::StagingQuota {
                actual: staged,
                limit: self.request.limits.max_staged_bytes,
            });
        }
        let position = self.writer.written_position();
        self.writer.sync_through(position).await?;
        let sealed = SealedSegment {
            valid_bytes: position.end_offset(),
            digest: self.writer.state().structural_digest(),
        };
        let start = Start {
            first_id: self
                .writer
                .header()
                .segment_id()
                .checked_add(1)
                .ok_or(Error::SegmentIdExhausted)?,
            predecessor: Some(self.writer.header().segment_id()),
            predecessor_digest: position.next_chain().previous_digest(),
            first_group: position
                .group_number()
                .checked_add(1)
                .ok_or(Error::LengthOverflow)?,
            first_chain: position.next_chain(),
        };
        let mut successor = create_segment(
            &self.journal,
            self.request.replacement,
            self.request.limits,
            start,
        )
        .await?;
        self.writer.transfer_buffers_to(&mut successor);
        self.segments
            .last_mut()
            .ok_or(crate::DirectoryError::MissingActiveSegment)?
            .sealed = Some(sealed);
        self.segments.push(active_reference(&successor, start));
        let old = std::mem::replace(&mut self.writer, successor);
        old.close().await?;
        self.staged_bytes = staged;
        Ok(())
    }

    /// Remove only files created by this healthy attempt. No selected or pinned
    /// old history is touched. Failed/canceled cleanup requires explicit reopen.
    pub async fn abort(self) -> Result<Journal, Error> {
        self.healthy()?;
        let last_old = super::active_segment(&self.journal.manifest.segments)?.segment_id;
        let Self {
            journal,
            writer,
            segments,
            ..
        } = self;
        writer.close().await?;
        let directory = journal.root().join("segments");
        for segment in segments {
            if segment.segment_id > last_old {
                journal
                    .access
                    .done(Operation::RemoveFile {
                        path: directory.join(segment.file_name()),
                    })
                    .await?;
            }
        }
        journal.access.sync_directory(directory).await?;
        Ok(journal)
    }

    /// Sync all staged files, then publish the exact accepted/committed anchors
    /// and hard state together. This consumes ownership even on uncertain errors.
    pub async fn finish(self) -> Result<Journal, Error> {
        if self.checkpoint_position.is_some() {
            return Err(Error::IncompleteSuffix);
        }
        Box::pin(self.finish_selection(None)).await
    }

    /// Publish checkpoint state plus required retained operations in one final
    /// selection. Its source metadata and chunk files remain unselected until
    /// complete. Dropping this future leaves the original nonvoting store selected.
    pub async fn finish_with_checkpoint(
        self,
        import: CanonicalCheckpointImport<'_>,
    ) -> Result<Journal, Error> {
        if !self.checkpoint_seen || self.checkpoint_position != Some(import.position) {
            return Err(Error::IncompleteSuffix);
        }
        import
            .image
            .require_owner(self.journal.root(), self.journal.manifest.identity)?;
        let mut budget = crate::cooperative::Budget::default();
        let mut decoder = ozzy_core::checkpoint::Decoder::new(
            import.image.state_bytes(),
            import.snapshot_limits.max_snapshot_bytes,
        );
        let mut reader = import.image.stream()?;
        while let Some(bytes) = reader.next(64 * 1024).await? {
            decoder.push(&bytes, |_| Ok(()), |_, _| Ok(()))?;
            budget.charge(bytes.len()).await;
        }
        if !reader.is_verified() {
            return Err(Error::IncompleteSuffix);
        }
        let (layout, bytes) = decoder.finish()?;
        let state = ozzy_core::state::CanonicalState::decode_snapshot_cooperative(
            &bytes,
            import.state_limits,
            import.snapshot_limits,
            async |bytes| budget.charge(bytes).await,
        )
        .await?;
        layout.validate_state(&state)?;
        if state.revision() != import.position.op_number {
            return Err(Error::IncompleteSuffix);
        }
        drop(state);
        drop(bytes);
        Box::pin(self.finish_selection(Some(import))).await
    }

    async fn finish_selection(
        mut self,
        checkpoint: Option<CanonicalCheckpointImport<'_>>,
    ) -> Result<Journal, Error> {
        self.healthy()?;
        if position_before(self.writer.written_position().next_chain())? != self.request.accepted
            || !self.committed_seen
        {
            return Err(Error::IncompleteSuffix);
        }
        self.journal.validate_authority_files().await?;
        self.writer
            .sync_through(self.writer.written_position())
            .await?;
        self.journal
            .access
            .sync_directory(self.journal.root().join("segments"))
            .await?;
        let mut next = self.journal.next_manifest()?;
        next.promised_view = self.request.replacement.promised_view;
        next.last_normal_view = self.request.replacement.last_normal_view;
        next.accepted = self.request.accepted;
        next.committed = self.request.replacement.committed;
        next.segments = std::mem::take(&mut self.segments);
        if let Some(checkpoint) = checkpoint {
            next = self.publish_checkpoint_source(next, checkpoint).await?;
        }
        self.journal.install_selected(next).await?;
        let old = std::mem::replace(&mut self.journal.writer, self.writer);
        old.close().await?;
        Ok(self.journal)
    }

    async fn publish_checkpoint_source(
        &mut self,
        mut source: crate::Manifest,
        import: CanonicalCheckpointImport<'_>,
    ) -> Result<crate::Manifest, Error> {
        source.committed = import.position;
        source.checkpoint = None;
        self.journal.interrupted = true;
        let (source, digest) = self
            .journal
            .directory
            .publish_manifest(&self.journal.manifest, source, self.journal.limits.metadata)
            .await?;
        let spec = crate::CheckpointSpec {
            group_id: source.identity.group_id,
            store_id: source.identity.store_id,
            checkpoint_id: import.id,
            position: import.position,
            configuration_epoch: source.configuration_epoch,
            source_manifest_generation: source.generation,
            source_manifest_digest: digest,
            state_schema_digest: ozzy_core::state::canonical_state_schema_digest(),
            chunk_bytes: self.journal.limits.checkpoint.max_chunk_bytes,
        };
        let prepared = crate::checkpoint::asynchronous::PreparedBuild::new(
            crate::checkpoint::asynchronous::BuildConfig {
                access: self.journal.access.clone(),
                root: self.journal.root().to_owned(),
                spec,
                limits: self.journal.limits.checkpoint,
                io_bytes: self.journal.limits.io.chunk_bytes,
                listing: (
                    self.journal.limits.directory_entries,
                    self.journal.limits.directory_name_bytes,
                ),
            },
            import.image.state_bytes(),
        )?;
        let mut builder = prepared.start().await?;
        let mut reader = import.image.stream()?;
        while let Some(bytes) = reader.next(builder.fragment_bytes()).await? {
            builder.push(&bytes).await?;
        }
        if !reader.is_verified() {
            return Err(Error::IncompleteSuffix);
        }
        let checkpoint = builder.finish().await?;
        if checkpoint.manifest.state_digest != import.image.digest() {
            return Err(Error::IncompleteSuffix);
        }
        self.journal.manifest = source;
        let mut selected = self.journal.next_manifest()?;
        selected.committed = self.request.replacement.committed;
        selected.checkpoint = Some(crate::CheckpointReference {
            checkpoint_id: import.id,
            position: import.position,
            manifest_digest: checkpoint.digest,
        });
        self.journal.checkpoint = Some(checkpoint);
        Ok(selected)
    }
}

fn active_reference(writer: &SegmentWriter, start: Start) -> SegmentReference {
    SegmentReference {
        segment_id: writer.header().segment_id(),
        file_generation: 0,
        first_group_number: start.first_group,
        first_chain: start.first_chain,
        capacity: writer.header().capacity(),
        sealed: None,
    }
}

async fn create_segment(
    journal: &Journal,
    replacement: SuffixReplacement,
    limits: SuffixStreamLimits,
    start: Start,
) -> Result<SegmentWriter, Error> {
    let mut id = start.first_id;
    for _ in 0..limits.max_orphan_probes {
        let header = SegmentHeader::new(
            journal.manifest.identity.group_id,
            id,
            start.predecessor,
            start.predecessor_digest,
            replacement.segment_capacity,
        )?;
        let writer = SegmentWriter::create(
            journal
                .access
                .segment_file(journal.root().join(format!("segments/{id}.log"))),
            header,
            SegmentStart {
                generation: replacement.writer_generation,
                first_group_number: start.first_group,
                initial_chain: start.first_chain,
            },
            journal.limits.io,
        )
        .await;
        match writer {
            Ok(writer) => return Ok(writer),
            Err(WriterError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
                id = id.checked_add(1).ok_or(Error::SegmentIdExhausted)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::ReplacementSegmentConflict)
}
