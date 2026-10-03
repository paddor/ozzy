//! Bounded staging with one final selection. Transport transfer and consensus
//! authority stay outside this physical installer.

use super::Journal;
use crate::directory::recovery::recovery_marker;
use crate::suffix::{
    OwnedOperation, enforce_limit, position_before, retained_image, rewrite_segment_index,
    state::Source, streaming, validate_common_request,
};
use crate::{
    AsyncSegmentStart, AsyncSegmentWriter, CanonicalOperation, ChainPosition, CodecError, Digest,
    ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, LogPosition, SEGMENT_HEADER_BYTES, SealedSegment,
    SegmentHeader, SegmentReference, SuffixReplacement, SuffixReplacementError as Error,
    SuffixReplacementLimits, SuffixStreamLimits, WRITE_GROUP_ALIGNMENT, WriterError,
};
use ozzy_io::Operation;
use std::io;

/// Owns the old selected journal plus bounded, unselected replacement files.
/// Dropping/canceling this object leaves originals and selection intact unless
/// final publication started. Reopen resolves any ambiguous final publication.
#[derive(Debug)]
pub struct Installer {
    journal: Journal,
    replacement: SuffixReplacement,
    accepted: LogPosition,
    limits: SuffixStreamLimits,
    writer: AsyncSegmentWriter,
    segments: Vec<SegmentReference>,
    body_digests: Vec<Digest>,
    staged_bytes: u64,
    committed_seen: bool,
    faulted: bool,
}

#[derive(Clone, Copy)]
struct Start {
    first_id: u64,
    predecessor: Option<u64>,
    predecessor_digest: Digest,
    first_group: u64,
    first_chain: ChainPosition,
}

impl Journal {
    fn suffix_source(&self) -> Source<'_> {
        Source {
            manifest: &self.manifest,
            current: self.current,
            header: self.writer.header(),
            written: self.writer.written_position(),
            durable: self.writer.durable_position(),
            healthy: !self.is_faulted(),
            decode: self.limits.decode,
            operations: self.limits.operations,
            max_segments: self.limits.metadata.max_segments,
        }
    }

    /// Preserve the exact protected prefix and stage a selected suffix. Copies
    /// at most one bounded source segment; older sealed predecessors stay shared.
    pub async fn begin_suffix_replacement(
        mut self,
        replacement: SuffixReplacement,
        accepted: LogPosition,
        limits: SuffixStreamLimits,
    ) -> Result<Installer, Error> {
        streaming::validate_stream_request(&self.suffix_source(), replacement, accepted, limits)?;
        self.validate_authority_files().await?;
        let index = rewrite_segment_index(&self.manifest, replacement.protected_committed)?;
        let reference = self.manifest.segments[index];
        if reference.capacity > limits.max_source_segment_bytes {
            return Err(Error::InvalidLimits);
        }
        let image = self.segment_image(reference).await?;
        let retained = retained_image(
            &self.suffix_source(),
            reference,
            replacement.protected_committed,
            SuffixReplacementLimits {
                max_operations: usize::try_from(reference.capacity / ENTRY_HEADER_BYTES as u64)
                    .map_err(|_| Error::LengthOverflow)?,
                max_body_bytes: self.limits.decode.max_segment_decoded_body_bytes,
                max_segment_bytes: limits.max_source_segment_bytes,
            },
            &image,
        )?;
        drop(image);
        Box::pin(self.start_suffix(replacement, accepted, limits, index, reference, &retained))
            .await
    }

    /// Replace an unfinished nonvoting store from genesis under fresh external
    /// authority. Final selection leaves its marker intact. Private replay and
    /// configuration publication remain separate required gates.
    pub async fn begin_recovery_replacement(
        mut self,
        configuration: &[u8],
        replacement: SuffixReplacement,
        accepted: LogPosition,
        limits: SuffixStreamLimits,
    ) -> Result<Installer, Error> {
        let marker = recovery_marker(configuration)?;
        if self.configuration.as_deref() != Some(marker.as_slice()) {
            return Err(crate::DirectoryError::ConfigurationMismatch.into());
        }
        validate_common_request(
            &self.suffix_source(),
            replacement,
            SuffixReplacementLimits {
                max_operations: limits.max_group_operations,
                max_body_bytes: limits.max_group_body_bytes,
                max_segment_bytes: limits.max_staged_bytes,
            },
        )?;
        if replacement.protected_committed != LogPosition::GENESIS
            || self.manifest.checkpoint.is_some()
            || self.manifest.segments[0].first_chain != ChainPosition::GENESIS
        {
            return Err(Error::ProtectedCommitMismatch);
        }
        if replacement.promised_view != replacement.last_normal_view {
            return Err(Error::InvalidView);
        }
        streaming::validate_stream_limits(&self.suffix_source(), replacement, accepted, limits)?;
        self.validate_authority_files().await?;
        let source = SegmentReference {
            first_group_number: 1,
            first_chain: ChainPosition::GENESIS,
            ..self.manifest.segments[0]
        };
        Box::pin(self.start_suffix(replacement, accepted, limits, 0, source, &[])).await
    }

    async fn start_suffix(
        self,
        replacement: SuffixReplacement,
        accepted: LogPosition,
        limits: SuffixStreamLimits,
        rewrite_index: usize,
        source: SegmentReference,
        retained: &[OwnedOperation],
    ) -> Result<Installer, Error> {
        enforce_limit(
            "selected manifest segments",
            rewrite_index + 1,
            limits.max_segments,
        )?;
        if replacement.segment_capacity > self.limits.io.max_segment_bytes {
            return Err(Error::InvalidLimits);
        }
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(limits.max_segments)
            .map_err(|_| Error::Allocation)?;
        segments.extend_from_slice(&self.manifest.segments[..rewrite_index]);
        let (predecessor, predecessor_digest) =
            segments.last().map_or((None, Digest::ZERO), |segment| {
                (
                    Some(segment.segment_id),
                    source.first_chain.previous_digest(),
                )
            });
        let start = Start {
            first_id: self
                .manifest
                .segments
                .last()
                .expect("active segment")
                .segment_id
                .checked_add(1)
                .ok_or(Error::SegmentIdExhausted)?,
            predecessor,
            predecessor_digest,
            first_group: source.first_group_number,
            first_chain: source.first_chain,
        };
        let mut body_digests = Vec::new();
        body_digests
            .try_reserve_exact(limits.max_group_operations)
            .map_err(|_| Error::Allocation)?;
        let reserve = limits
            .max_group_operations
            .checked_mul(ENTRY_HEADER_BYTES + 7)
            .and_then(|overhead| overhead.checked_add(limits.max_group_body_bytes))
            .and_then(|bytes| bytes.checked_add(GROUP_SEAL_BYTES + WRITE_GROUP_ALIGNMENT))
            .ok_or(Error::LengthOverflow)?;
        let mut writer = create_segment(&self, replacement, limits, start).await?;
        writer.reserve_encode_buffer(reserve, replacement.body_encoding)?;
        segments.push(active_reference(&writer, start));
        let mut installer = Installer {
            journal: self,
            replacement,
            accepted,
            limits,
            writer,
            segments,
            body_digests,
            staged_bytes: replacement.segment_capacity,
            committed_seen: replacement.committed == replacement.protected_committed,
            faulted: false,
        };
        installer.copy_retained(retained).await?;
        debug_assert_eq!(
            position_before(installer.writer.written_position().next_chain())?,
            replacement.protected_committed
        );
        Ok(installer)
    }
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
            .try_reserve_exact(self.limits.max_group_operations)
            .map_err(|_| Error::Allocation)?;
        let mut bytes = 0usize;
        for operation in retained {
            let operation = operation.borrowed();
            if operation.body.len() > self.limits.max_group_body_bytes {
                return Err(Error::InvalidLimits);
            }
            if borrowed.len() == self.limits.max_group_operations
                || operation.body.len() > self.limits.max_group_body_bytes - bytes
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
            self.replacement,
            self.accepted,
            self.limits,
            self.writer.written_position().next_chain(),
            &mut self.body_digests,
            operations,
        )?;
        self.faulted = true;
        self.write_chunk(operations, bytes).await?;
        self.committed_seen |= committed_seen;
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
            let reference = self.segments.last().expect("active reference");
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
                    self.replacement.body_encoding,
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
            self.limits.max_segments,
        )?;
        let staged = self
            .staged_bytes
            .checked_add(self.replacement.segment_capacity)
            .ok_or(Error::LengthOverflow)?;
        if staged > self.limits.max_staged_bytes {
            return Err(Error::StagingQuota {
                actual: staged,
                limit: self.limits.max_staged_bytes,
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
        let mut successor =
            create_segment(&self.journal, self.replacement, self.limits, start).await?;
        self.writer.transfer_buffers_to(&mut successor);
        self.segments.last_mut().expect("active reference").sealed = Some(sealed);
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
        let last_old = self
            .journal
            .manifest
            .segments
            .last()
            .expect("selected active segment")
            .segment_id;
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
    pub async fn finish(mut self) -> Result<Journal, Error> {
        self.healthy()?;
        if position_before(self.writer.written_position().next_chain())? != self.accepted
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
        next.promised_view = self.replacement.promised_view;
        next.last_normal_view = self.replacement.last_normal_view;
        next.accepted = self.accepted;
        next.committed = self.replacement.committed;
        next.segments = self.segments;
        self.journal.install_selected(next).await?;
        let old = std::mem::replace(&mut self.journal.writer, self.writer);
        old.close().await?;
        Ok(self.journal)
    }
}

fn active_reference(writer: &AsyncSegmentWriter, start: Start) -> SegmentReference {
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
) -> Result<AsyncSegmentWriter, Error> {
    let mut id = start.first_id;
    for _ in 0..limits.max_orphan_probes {
        let header = SegmentHeader::new(
            journal.manifest.identity.group_id,
            id,
            start.predecessor,
            start.predecessor_digest,
            replacement.segment_capacity,
        )?;
        let writer = AsyncSegmentWriter::create(
            journal.root().join(format!("segments/{id}.log")),
            journal.access.io.clone(),
            journal.access.protection.clone(),
            header,
            AsyncSegmentStart {
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
