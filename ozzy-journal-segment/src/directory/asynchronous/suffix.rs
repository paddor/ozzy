//! Bounded staging with one final selection. Transport transfer and consensus
//! authority stay outside this physical installer.

use super::Journal;
use crate::directory::segment_name;
use crate::suffix::{OwnedOperation, enforce_limit, streaming};
use crate::{
    CanonicalOperation, ChainPosition, CodecError, Digest, LogPosition, PackLimits,
    SEGMENT_HEADER_BYTES, SealedSegment, SegmentHeader, SegmentReference, SegmentStart,
    SegmentWriter, SuffixReplacement, SuffixReplacementError as Error, SuffixStreamLimits,
    WRITE_GROUP_ALIGNMENT, WriterError, metadata::position_before,
};
use ozzy_io::Operation;
use std::io;

mod begin;
mod checkpoint;
pub use checkpoint::CanonicalCheckpointImport;

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
        let checkpoint_seen = checkpoint::holds_checkpoint(self.checkpoint_position, operations)?;
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
        if self.segments.len() >= self.request.limits.max_segments {
            Box::pin(self.pack()).await?;
        }
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

    /// At the segment limit, pack the sealed run of the new selection that
    /// frees the most segments per byte rewritten: kept history this copy
    /// wrote in small groups, or staged segments of small transfer chunks.
    /// A replaced kept segment stays selected until finish and is cleaned
    /// up after; a replaced staged one is removed at once. Without a run
    /// worth packing, the selection stays as it was.
    async fn pack(&mut self) -> Result<(), Error> {
        let capacity = self.request.replacement.segment_capacity;
        let staged = self
            .staged_bytes
            .checked_add(capacity)
            .ok_or(Error::LengthOverflow)?;
        if staged > self.request.limits.max_staged_bytes {
            return Ok(());
        }
        // Reads and groups as large as staging's own, so a pack holds about
        // what staging does.
        let limits = PackLimits {
            max_group_bytes: self
                .request
                .limits
                .max_group_body_bytes
                .max(WRITE_GROUP_ALIGNMENT),
            read_ahead_bytes: self.journal.limits.io.chunk_bytes,
            max_probes: self.request.limits.max_orphan_probes,
        };
        let generation = self.request.replacement.writer_generation;
        let Some(job) = self
            .journal
            .pack_job(&self.segments, limits, generation, capacity)?
        else {
            return Ok(());
        };
        let mut outcome = job.run().await?;
        let Some(packed) = outcome.take_packed() else {
            return Ok(());
        };
        let replaced = packed.replaced();
        let at = self
            .segments
            .iter()
            .position(|reference| Some(reference) == replaced.first())
            .ok_or(crate::DirectoryError::PackMismatch)?;
        self.segments
            .splice(at..at + replaced.len(), [packed.merged()]);
        let directory = self.journal.root().join("segments");
        let mut removed = 0;
        for reference in replaced {
            if !self.journal.manifest.segments.contains(reference) {
                self.journal
                    .access
                    .done(Operation::RemoveFile {
                        path: directory.join(reference.file_name()),
                    })
                    .await?;
                removed += reference.capacity;
            }
        }
        self.staged_bytes = staged - removed;
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
        let created = super::spare::create_segment_file(
            &journal.access,
            journal.root(),
            id,
            replacement.segment_capacity,
        )
        .await;
        let file = match created {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                id = id.checked_add(1).ok_or(Error::SegmentIdExhausted)?;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let opened = crate::writer::asynchronous::Opened {
            path: journal.root().join(segment_name(id)),
            access: journal.access.clone(),
            buffered: file,
        };
        let start = SegmentStart {
            generation: replacement.writer_generation,
            first_group_number: start.first_group,
            initial_chain: start.first_chain,
        };
        return Ok(
            SegmentWriter::initialize_prepared(opened, header, start, journal.limits.io).await?,
        );
    }
    Err(Error::ReplacementSegmentConflict)
}
