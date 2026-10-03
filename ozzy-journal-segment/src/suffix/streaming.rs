//! Bounded selected-history staging with one final manifest publication.

use std::fs::{File, OpenOptions};
use std::io;
use std::sync::Arc;

use ozzy_journal::operation::{
    canonical_body_digest, logical_operation_digest_with_body_digest, validate_operation_body,
};

use super::state::Source;
use super::{
    FollowingChain, SuffixReplacement, SuffixReplacementError as Error, SuffixReplacementLimits,
    allocate_segment, enforce_limit, position_before, retained_fragment, rewrite_segment_index,
    sync_directory, validate_request,
};
use crate::LogPosition;
use crate::{
    CanonicalOperation, ChainPosition, CodecError, Digest, ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES,
    OpenGroupJournal, SEGMENT_HEADER_BYTES, SealedSegment, SegmentHeader, SegmentReference,
    SegmentWriter, WRITE_GROUP_ALIGNMENT, WriterError,
};

/// Memory/work and disk bounds for one streamed selected generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuffixStreamLimits {
    /// Maximum complete operations in each submitted/staged physical group.
    pub max_group_operations: usize,
    /// Maximum decoded body bytes in each submitted/staged physical group.
    pub max_group_body_bytes: usize,
    /// Maximum references in the final manifest, including retained predecessors.
    pub max_segments: usize,
    /// Sum of allocated capacities of new segments in this attempt, not old pins.
    pub max_staged_bytes: u64,
    /// Maximum physical size of the one old segment containing the protected floor.
    /// Its decoded-memory limit still comes from the journal's `DecodeLimits`.
    pub max_source_segment_bytes: u64,
    /// Maximum exclusive-create attempts per segment, including occupied names.
    pub max_orphan_probes: usize,
}

impl Default for SuffixStreamLimits {
    fn default() -> Self {
        Self {
            max_group_operations: 64,
            max_group_body_bytes: 1024 * 1024,
            max_segments: 1024,
            max_staged_bytes: 16 * 1024 * 1024 * 1024,
            max_source_segment_bytes: 2 * 1024 * 1024 * 1024,
            max_orphan_probes: 64,
        }
    }
}

/// Dedicated-worker-only staged suffix. The old journal remains selected.
///
/// This object exclusively owns the old journal and the new physical writer.
/// Complete chunks may span many files, but no intermediate manifest is published.
/// Drop/error leaves unselected files for explicit pin-aware cleanup after reopen.
/// Storage validates canonical bytes, not consensus or application authority.
#[derive(Debug)]
pub struct SuffixInstaller {
    journal: OpenGroupJournal,
    replacement: SuffixReplacement,
    accepted: LogPosition,
    limits: SuffixStreamLimits,
    writer: SegmentWriter<Arc<File>>,
    segments: Vec<SegmentReference>,
    body_digests: Vec<Digest>,
    staged_bytes: u64,
    committed_seen: bool,
    faulted: bool,
}

#[derive(Debug, Clone, Copy)]
struct SegmentStart {
    first_id: u64,
    predecessor_id: Option<u64>,
    predecessor_digest: Digest,
    first_group: u64,
    first_chain: ChainPosition,
}

impl OpenGroupJournal {
    /// Begin worker-side streaming replacement with an exact selected tail anchor.
    ///
    /// Validates current authority and copies the protected fragment from at most
    /// one bounded source segment. Older sealed predecessors stay referenced.
    /// Transfer chunks start strictly after `protected_committed`. Call `finish`
    /// only after all selected bytes arrive; it synchronizes and publishes once.
    /// The caller must pin/validate selected lineage and supply consensus authority.
    pub fn begin_suffix_replacement(
        self,
        replacement: SuffixReplacement,
        accepted: LogPosition,
        limits: SuffixStreamLimits,
    ) -> Result<SuffixInstaller, Error> {
        validate_stream_request(&Source::new(&self), replacement, accepted, limits)?;
        let rewrite_index =
            rewrite_segment_index(&self.directory.manifest, replacement.protected_committed)?;
        let source = self.directory.manifest.segments[rewrite_index];
        if source.capacity > limits.max_source_segment_bytes {
            return Err(Error::InvalidLimits);
        }
        // Existing scanner/copy is bounded by one physical source segment and
        // its aggregate decoded-byte limit, never the length of the selected tail.
        let retained = retained_fragment(
            &self,
            source,
            replacement.protected_committed,
            SuffixReplacementLimits {
                max_operations: usize::try_from(source.capacity / ENTRY_HEADER_BYTES as u64)
                    .map_err(|_| Error::LengthOverflow)?,
                max_body_bytes: self.decode_limits.max_segment_decoded_body_bytes,
                max_segment_bytes: limits.max_source_segment_bytes,
            },
        )?;
        self.start_stream(
            replacement,
            accepted,
            limits,
            rewrite_index,
            source,
            &retained,
        )
    }

    /// Replace an unfinished nonvoting store from genesis under fresh recovery authority.
    ///
    /// Requires the exact recovery marker in memory and on disk. Previous attempt
    /// metadata grants no authority and contributes no protected prefix. Ordinary
    /// configured stores cannot use this path. The caller must obtain fresh quorum
    /// evidence and supply its full accepted tail and known commit floor. Publication
    /// here leaves the marker intact; private semantic replay and configuration
    /// publication are separate mandatory gates before fenced intact restart.
    pub fn begin_recovery_replacement(
        self,
        configuration: &[u8],
        replacement: SuffixReplacement,
        accepted: LogPosition,
        limits: SuffixStreamLimits,
    ) -> Result<SuffixInstaller, Error> {
        self.directory.require_recovering(configuration)?;
        super::validate_common_request(
            &Source::new(&self),
            replacement,
            SuffixReplacementLimits {
                max_operations: limits.max_group_operations,
                max_body_bytes: limits.max_group_body_bytes,
                max_segment_bytes: limits.max_staged_bytes,
            },
        )?;
        if replacement.protected_committed != LogPosition::GENESIS
            || self.directory.manifest.checkpoint.is_some()
            || self.directory.manifest.segments[0].first_chain != ChainPosition::GENESIS
        {
            return Err(Error::ProtectedCommitMismatch);
        }
        if replacement.promised_view != replacement.last_normal_view {
            return Err(Error::InvalidView);
        }
        validate_stream_limits(&Source::new(&self), replacement, accepted, limits)?;
        let source = SegmentReference {
            first_group_number: 1,
            first_chain: ChainPosition::GENESIS,
            ..self.directory.manifest.segments[0]
        };
        self.start_stream(replacement, accepted, limits, 0, source, &[])
    }

    fn start_stream(
        self,
        replacement: SuffixReplacement,
        accepted: LogPosition,
        limits: SuffixStreamLimits,
        rewrite_index: usize,
        source: SegmentReference,
        retained: &[super::OwnedOperation],
    ) -> Result<SuffixInstaller, Error> {
        enforce_limit(
            "selected manifest segments",
            rewrite_index + 1,
            limits.max_segments,
        )?;
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(limits.max_segments)
            .map_err(|_| Error::Allocation)?;
        segments.extend_from_slice(&self.directory.manifest.segments[..rewrite_index]);
        let (predecessor_id, predecessor_digest) =
            segments.last().map_or((None, Digest::ZERO), |segment| {
                (
                    Some(segment.segment_id),
                    source.first_chain.previous_digest(),
                )
            });
        let start = SegmentStart {
            first_id: self
                .directory
                .manifest
                .segments
                .last()
                .expect("active segment")
                .segment_id
                .checked_add(1)
                .ok_or(Error::SegmentIdExhausted)?,
            predecessor_id,
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
        let mut writer = create_segment(&self, replacement, limits, start)?;
        writer.reserve_encode_buffer(reserve, replacement.body_encoding)?;
        segments.push(active_reference(&writer, start));
        let mut installer = SuffixInstaller {
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
        installer.copy_retained(retained)?;
        debug_assert_eq!(
            position_before(installer.writer.written_position().next_chain())?,
            replacement.protected_committed
        );
        Ok(installer)
    }
}

impl SuffixInstaller {
    fn copy_retained(&mut self, retained: &[super::OwnedOperation]) -> Result<(), Error> {
        let mut borrowed = Vec::new();
        borrowed
            .try_reserve_exact(self.limits.max_group_operations)
            .map_err(|_| Error::Allocation)?;
        let mut bytes = 0usize;
        for operation in retained {
            if operation.body.len() > self.limits.max_group_body_bytes {
                return Err(Error::InvalidLimits);
            }
            if borrowed.len() == self.limits.max_group_operations
                || operation.body.len() > self.limits.max_group_body_bytes - bytes
            {
                self.append_validated(&borrowed)?;
                borrowed.clear();
                bytes = 0;
            }
            bytes += operation.body.len();
            borrowed.push(operation.borrowed());
        }
        if !borrowed.is_empty() {
            self.append_validated(&borrowed)?;
        }
        Ok(())
    }

    /// Stage one complete bounded transfer chunk, without publishing authority.
    ///
    /// Bad scopes, bodies, gaps, digests, and chunk bounds reject before changing
    /// the staged prefix. A storage/roll failure after validation faults the entire
    /// installer. Retry then requires reopen, not a duplicate write into this state.
    pub fn append_chunk(&mut self, operations: &[CanonicalOperation<'_>]) -> Result<(), Error> {
        self.require_healthy()?;
        self.append_validated(operations)
    }

    fn append_validated(&mut self, operations: &[CanonicalOperation<'_>]) -> Result<(), Error> {
        let (body_bytes, committed_seen) = self.validate_chunk(operations)?;
        let result = self.write_chunk(operations, body_bytes);
        if result.is_err() {
            self.faulted = true;
        } else {
            self.committed_seen |= committed_seen;
        }
        result
    }

    fn validate_chunk(
        &mut self,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<(usize, bool), Error> {
        validate_chunk(
            &Source::new(&self.journal),
            self.replacement,
            self.accepted,
            self.limits,
            self.writer.written_position().next_chain(),
            &mut self.body_digests,
            operations,
        )
    }

    fn write_chunk(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        body_bytes: usize,
    ) -> Result<(), Error> {
        self.write_part(operations, body_bytes, 0)
    }

    // Split only between canonical operations. Recursion depth is logarithmic
    // in the already bounded chunk count, plus at most one roll per level.
    fn write_part(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        body_bytes: usize,
        first_digest: usize,
    ) -> Result<(), Error> {
        let reference = self.segments.last().expect("active reference");
        let groups =
            self.writer.written_position().group_number() - (reference.first_group_number - 1);
        let decoded = self.writer.written_position().decoded_body_bytes();
        if groups >= self.journal.decode_limits.max_groups as u64
            || body_bytes > self.journal.decode_limits.max_segment_decoded_body_bytes - decoded
        {
            self.roll()?;
        }
        match self.writer.append_with_body_encoding_and_digests(
            operations,
            &self.body_digests[first_digest..first_digest + operations.len()],
            self.replacement.body_encoding,
        ) {
            Ok(_) => Ok(()),
            Err(WriterError::Codec(CodecError::GroupExceedsSegment))
                if self.writer.written_position().end_offset() > SEGMENT_HEADER_BYTES as u64 =>
            {
                self.roll()?;
                self.write_part(operations, body_bytes, first_digest)
            }
            Err(WriterError::Codec(CodecError::GroupExceedsSegment)) if operations.len() > 1 => {
                let middle = operations.len() / 2;
                let (left, right) = operations.split_at(middle);
                let left_bytes: usize = left.iter().map(|operation| operation.body.len()).sum();
                self.write_part(left, left_bytes, first_digest)?;
                self.write_part(right, body_bytes - left_bytes, first_digest + middle)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn roll(&mut self) -> Result<(), Error> {
        enforce_limit(
            "selected manifest segments",
            self.segments.len() + 1,
            self.limits.max_segments,
        )?;
        let staged_bytes = self
            .staged_bytes
            .checked_add(self.replacement.segment_capacity)
            .ok_or(Error::LengthOverflow)?;
        if staged_bytes > self.limits.max_staged_bytes {
            return Err(Error::StagingQuota {
                actual: staged_bytes,
                limit: self.limits.max_staged_bytes,
            });
        }
        let position = self.writer.begin_sync();
        self.writer.sync_through(position)?;
        let sealed = SealedSegment {
            valid_bytes: position.end_offset(),
            digest: self.writer.structural_digest(),
        };
        let start = SegmentStart {
            first_id: self
                .writer
                .header()
                .segment_id()
                .checked_add(1)
                .ok_or(Error::SegmentIdExhausted)?,
            predecessor_id: Some(self.writer.header().segment_id()),
            predecessor_digest: position.next_chain().previous_digest(),
            first_group: position
                .group_number()
                .checked_add(1)
                .ok_or(Error::LengthOverflow)?,
            first_chain: position.next_chain(),
        };
        let mut successor = create_segment(&self.journal, self.replacement, self.limits, start)?;
        self.writer.transfer_encode_buffers_to(&mut successor);
        self.segments.last_mut().expect("active reference").sealed = Some(sealed);
        self.segments.push(active_reference(&successor, start));
        self.writer = successor;
        self.staged_bytes = staged_bytes;
        Ok(())
    }

    /// Abandon healthy, unpublished staging and return the still-selected old journal.
    ///
    /// Call only on the exclusive storage worker after earlier chunk calls settle.
    /// This cannot undo `finish`, which consumes ownership before publication.
    /// Deletes only files created by this attempt, never old or pinned history.
    /// Cleanup failure consumes ownership and requires reopen; no uncertain I/O
    /// may be retried through this path. Disk work is bounded by `max_segments`.
    pub fn abort(self) -> Result<OpenGroupJournal, Error> {
        self.require_healthy()?;
        let last_old = self
            .journal
            .directory
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
        drop(writer);
        let directory = journal.directory.root.join("segments");
        for segment in segments {
            if segment.segment_id > last_old {
                std::fs::remove_file(directory.join(format!("{}.log", segment.segment_id)))?;
            }
        }
        sync_directory(&directory)?;
        Ok(journal)
    }

    /// Synchronize all staged files and publish the selected view/log atomically.
    ///
    /// Exact tail and commit anchors are mandatory. Ownership is consumed even on
    /// error, since a publication failure can leave either complete generation
    /// selected. Reopen resolves `CURRENT`; old pinned files are not deleted here.
    pub fn finish(mut self) -> Result<OpenGroupJournal, Error> {
        self.require_healthy()?;
        if position_before(self.writer.written_position().next_chain())? != self.accepted
            || !self.committed_seen
        {
            return Err(Error::IncompleteSuffix);
        }
        self.writer.sync_through(self.writer.begin_sync())?;
        sync_directory(&self.journal.directory.root.join("segments"))?;
        let mut next = self.journal.directory.manifest.clone();
        next.parent_generation = next.generation;
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(Error::ManifestGeneration)?;
        next.promised_view = self.replacement.promised_view;
        next.last_normal_view = self.replacement.last_normal_view;
        next.accepted = self.accepted;
        next.committed = self.replacement.committed;
        next.segments = self.segments;
        self.journal.directory.install_selected_manifest(next)?;
        self.journal.writer = self.writer;
        if self.journal.directory.data_sync {
            self.journal
                .set_write_mode(crate::SegmentWriteMode::DataSync)?;
        }
        Ok(self.journal)
    }

    fn require_healthy(&self) -> Result<(), Error> {
        if self.faulted {
            Err(Error::StagingFaulted)
        } else {
            Ok(())
        }
    }
}

fn active_reference(writer: &SegmentWriter<Arc<File>>, start: SegmentStart) -> SegmentReference {
    SegmentReference {
        segment_id: writer.header().segment_id(),
        file_generation: 0,
        first_group_number: start.first_group,
        first_chain: start.first_chain,
        capacity: writer.header().capacity(),
        sealed: None,
    }
}

fn create_segment(
    journal: &OpenGroupJournal,
    replacement: SuffixReplacement,
    limits: SuffixStreamLimits,
    start: SegmentStart,
) -> Result<SegmentWriter<Arc<File>>, Error> {
    let mut id = start.first_id;
    for _ in 0..limits.max_orphan_probes {
        let header = SegmentHeader::new(
            journal.directory.identity.group_id,
            id,
            start.predecessor_id,
            start.predecessor_digest,
            replacement.segment_capacity,
        )?;
        let path = journal
            .directory
            .root
            .join("segments")
            .join(format!("{id}.log"));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(file) => {
                allocate_segment(&file, replacement.segment_capacity)?;
                let writer = SegmentWriter::initialize_allocated_at(
                    std::sync::Arc::new(file),
                    header,
                    replacement.writer_generation,
                    start.first_group,
                    start.first_chain,
                    true,
                )?;
                return Ok(writer);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                id = id.checked_add(1).ok_or(Error::SegmentIdExhausted)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::ReplacementSegmentConflict)
}

pub(crate) fn validate_stream_request(
    source: &Source<'_>,
    replacement: SuffixReplacement,
    accepted: LogPosition,
    limits: SuffixStreamLimits,
) -> Result<(), Error> {
    validate_request(
        source,
        replacement,
        &[],
        SuffixReplacementLimits {
            max_operations: limits.max_group_operations,
            max_body_bytes: limits.max_group_body_bytes,
            max_segment_bytes: limits.max_staged_bytes,
        },
    )?;
    validate_stream_limits(source, replacement, accepted, limits)
}

pub(crate) fn validate_stream_limits(
    source: &Source<'_>,
    replacement: SuffixReplacement,
    accepted: LogPosition,
    limits: SuffixStreamLimits,
) -> Result<(), Error> {
    accepted.following_chain()?;
    replacement.committed.following_chain()?;
    if replacement.writer_generation == source.written.generation() {
        return Err(Error::ReusedWriterGeneration);
    }
    if limits.max_segments == 0
        || limits.max_segments > source.max_segments
        || limits.max_orphan_probes == 0
        || limits.max_group_operations > source.decode.max_entries
        || limits.max_group_body_bytes > source.decode.max_group_decoded_body_bytes
        || limits.max_group_body_bytes > source.decode.max_segment_decoded_body_bytes
        || source.decode.max_groups == 0
    {
        return Err(Error::InvalidLimits);
    }
    let protected = replacement.protected_committed;
    if accepted.op_number < protected.op_number
        || (accepted.op_number == protected.op_number && accepted != protected)
        || replacement.committed.op_number < protected.op_number
        || replacement.committed.op_number > accepted.op_number
        || (replacement.committed.op_number == protected.op_number
            && replacement.committed != protected)
        || (replacement.committed.op_number == accepted.op_number
            && replacement.committed != accepted)
    {
        return Err(Error::CommitNotSelected);
    }
    Ok(())
}

pub(crate) fn validate_chunk(
    source: &Source<'_>,
    replacement: SuffixReplacement,
    accepted: LogPosition,
    limits: SuffixStreamLimits,
    mut expected: ChainPosition,
    body_digests: &mut Vec<Digest>,
    operations: &[CanonicalOperation<'_>],
) -> Result<(usize, bool), Error> {
    if operations.is_empty() {
        return Err(CodecError::EmptyGroup.into());
    }
    enforce_limit(
        "suffix chunk operations",
        operations.len(),
        limits.max_group_operations,
    )?;
    body_digests.clear();
    let mut body_bytes = 0usize;
    let mut committed_seen = false;
    for operation in operations {
        if operation.group_id != source.manifest.identity.group_id
            || operation.configuration_epoch != source.manifest.configuration_epoch
            || operation.original_view > replacement.promised_view
            || operation.op_number != expected.next_op_number()
            || operation.previous_digest != expected.previous_digest()
        {
            return Err(Error::SelectedSuffixMismatch);
        }
        body_bytes = body_bytes
            .checked_add(operation.body.len())
            .ok_or(Error::LengthOverflow)?;
        enforce_limit(
            "suffix chunk body bytes",
            body_bytes,
            limits.max_group_body_bytes,
        )?;
        enforce_limit(
            "suffix entry decoded bytes",
            operation.body.len(),
            source.decode.max_decoded_body_bytes,
        )?;
        let raw_entry = operation
            .body
            .len()
            .checked_add(ENTRY_HEADER_BYTES + 7)
            .ok_or(Error::LengthOverflow)?
            & !7;
        enforce_limit(
            "suffix entry bytes",
            raw_entry,
            source.decode.max_entry_bytes,
        )?;
        validate_operation_body(operation.kind, operation.body, source.operations)?;
        let body_digest = canonical_body_digest(operation.body);
        body_digests.push(body_digest);
        let digest = logical_operation_digest_with_body_digest(operation, body_digest);
        expected = ChainPosition::new(
            operation
                .op_number
                .checked_add(1)
                .ok_or(Error::LengthOverflow)?,
            digest,
        );
        if operation.op_number == replacement.committed.op_number {
            if digest != replacement.committed.digest {
                return Err(Error::CommitNotSelected);
            }
            committed_seen = true;
        }
    }
    let reached = position_before(expected)?;
    if reached.op_number > accepted.op_number
        || (reached.op_number == accepted.op_number && reached != accepted)
    {
        return Err(Error::SelectedSuffixMismatch);
    }
    Ok((body_bytes, committed_seen))
}
