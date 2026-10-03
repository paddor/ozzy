//! Atomic installation of one consensus-selected journal suffix.

pub(crate) mod state;
pub(crate) mod streaming;
use state::Source;
pub use streaming::{SuffixInstaller, SuffixStreamLimits};

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use ozzy_journal::operation::{
    CanonicalOperation, ChainPosition, OperationKind, logical_operation_digest,
    validate_operation_body,
};
use ozzy_journal::progress::JournalGeneration;
use thiserror::Error;

use crate::{
    BodyEncoding, CanonicalRecoveryRequirements, CodecError, CommitMode, CurrentReference, Digest,
    DirectoryError, LogPosition, Manifest, OpenGroupJournal, SEGMENT_HEADER_BYTES, SegmentHeader,
    SegmentReference, SegmentWriter, TailState, WriterError, encode_group_with_body_encoding,
    encode_segment_header, scan_segment,
};

/// Consensus authorization and resulting hard state for one suffix install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuffixReplacement {
    /// Exact CURRENT selection required before suffix installation.
    pub expected_current: CurrentReference,
    /// Confirmed canonical prefix that replacement must preserve.
    pub protected_committed: LogPosition,
    /// Promised election view constraining retained history and authority.
    pub promised_view: u64,
    /// Last installed normal election view.
    pub last_normal_view: u64,
    /// Exact confirmed canonical operation prefix.
    pub committed: LogPosition,
    /// New journal-owner generation fencing old physical completions.
    pub writer_generation: JournalGeneration,
    /// Physical byte capacity of replacement segments.
    pub segment_capacity: u64,
    /// Physical body compression policy; canonical bytes remain unchanged.
    pub body_encoding: BodyEncoding,
}

/// Explicit memory, operation, and output bounds for suffix replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuffixReplacementLimits {
    /// Maximum retained canonical operations during suffix installation.
    pub max_operations: usize,
    /// Maximum retained canonical body bytes during suffix installation.
    pub max_body_bytes: usize,
    /// Maximum physical segment bytes.
    pub max_segment_bytes: u64,
}

impl Default for SuffixReplacementLimits {
    fn default() -> Self {
        Self {
            max_operations: 1_048_576,
            max_body_bytes: 1024 * 1024 * 1024,
            max_segment_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedOperation {
    group_id: ozzy_proto::GroupId,
    configuration_epoch: u64,
    original_view: u64,
    op_number: u64,
    previous_digest: Digest,
    kind: OperationKind,
    body: Vec<u8>,
}

impl OwnedOperation {
    pub(crate) fn borrowed(&self) -> CanonicalOperation<'_> {
        CanonicalOperation {
            group_id: self.group_id,
            configuration_epoch: self.configuration_epoch,
            original_view: self.original_view,
            op_number: self.op_number,
            previous_digest: self.previous_digest,
            kind: self.kind,
            body: &self.body,
        }
    }
}

impl OpenGroupJournal {
    /// Rewrite the physical segment containing the protected commit point and
    /// atomically install one selected suffix.
    ///
    /// Caller supplies consensus authorization. This method validates only its
    /// frozen storage consequences. One bounded replacement segment is emitted;
    /// callers must choose capacity for the retained fragment plus selected
    /// suffix. Publication consumes ownership so ambiguous errors require reopen.
    #[expect(
        clippy::too_many_lines,
        reason = "linear validation and publication sequence is easier to audit"
    )]
    pub fn replace_suffix(
        self,
        replacement: SuffixReplacement,
        selected_suffix: &[CanonicalOperation<'_>],
        limits: SuffixReplacementLimits,
    ) -> Result<Self, SuffixReplacementError> {
        validate_request(&Source::new(&self), replacement, selected_suffix, limits)?;
        let rewrite_index =
            rewrite_segment_index(&self.directory.manifest, replacement.protected_committed)?;
        let rewrite = self.directory.manifest.segments[rewrite_index];
        let retained = retained_fragment(&self, rewrite, replacement.protected_committed, limits)?;
        let retained_body_bytes = retained.iter().try_fold(0_usize, |total, operation| {
            total
                .checked_add(operation.body.len())
                .ok_or(SuffixReplacementError::LengthOverflow)
        })?;
        let selected_body_bytes =
            selected_suffix
                .iter()
                .try_fold(0_usize, |total, operation| {
                    total
                        .checked_add(operation.body.len())
                        .ok_or(SuffixReplacementError::LengthOverflow)
                })?;
        enforce_limit(
            "replacement operations",
            retained.len().saturating_add(selected_suffix.len()),
            limits.max_operations,
        )?;
        enforce_limit(
            "replacement body bytes",
            retained_body_bytes
                .checked_add(selected_body_bytes)
                .ok_or(SuffixReplacementError::LengthOverflow)?,
            limits.max_body_bytes,
        )?;

        let next_chain = validate_selected_suffix(
            &Source::new(&self),
            replacement,
            selected_suffix,
            replacement.protected_committed.following_chain()?,
        )?;
        let accepted = position_before(next_chain)?;
        validate_new_commit(
            replacement.protected_committed,
            replacement.committed,
            accepted,
            selected_suffix,
        )?;

        let first_candidate_segment_id = self
            .directory
            .manifest
            .segments
            .last()
            .expect("validated manifest has an active segment")
            .segment_id
            .checked_add(1)
            .ok_or(SuffixReplacementError::SegmentIdExhausted)?;
        let (predecessor_id, predecessor_digest) = self.directory.manifest.segments
            [..rewrite_index]
            .last()
            .map_or((None, Digest::ZERO), |reference| {
                (
                    Some(reference.segment_id),
                    rewrite.first_chain.previous_digest(),
                )
            });
        let retained_borrowed = retained
            .iter()
            .map(OwnedOperation::borrowed)
            .collect::<Vec<_>>();
        let operations = retained_borrowed
            .iter()
            .copied()
            .chain(selected_suffix.iter().copied())
            .collect::<Vec<_>>();
        let mut next_segment_id = first_candidate_segment_id;
        let path = loop {
            let header = SegmentHeader::new(
                self.directory.identity.group_id,
                next_segment_id,
                predecessor_id,
                predecessor_digest,
                replacement.segment_capacity,
            )?;
            let segment_bytes = encode_replacement_segment(
                &header,
                rewrite.first_group_number,
                rewrite.first_chain,
                &operations,
                replacement.body_encoding,
            )?;
            if segment_bytes.len() as u64 > limits.max_segment_bytes {
                return Err(SuffixReplacementError::LimitExceeded {
                    kind: "replacement segment bytes",
                    actual: segment_bytes.len(),
                    limit: usize::try_from(limits.max_segment_bytes).unwrap_or(usize::MAX),
                });
            }
            let path = self
                .directory
                .root
                .join("segments")
                .join(format!("{next_segment_id}.log"));
            match write_or_resume_segment(&path, &segment_bytes, replacement.segment_capacity) {
                Ok(()) => break path,
                Err(SuffixReplacementError::ReplacementSegmentConflict) => {
                    next_segment_id = next_segment_id
                        .checked_add(1)
                        .ok_or(SuffixReplacementError::SegmentIdExhausted)?;
                }
                Err(error) => return Err(error),
            }
        };
        sync_directory(&self.directory.root.join("segments"))?;

        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut writer = SegmentWriter::recover_canonical(
            std::sync::Arc::new(file),
            replacement.writer_generation,
            rewrite.first_group_number,
            rewrite.first_chain,
            self.decode_limits,
            replacement.segment_capacity,
            CanonicalRecoveryRequirements {
                operation_limits: self.operation_limits,
                protected: [
                    Some(accepted.following_chain()?),
                    Some(replacement.committed.following_chain()?),
                ],
                configuration_epoch: Some(self.directory.manifest.configuration_epoch),
                promised_view: Some(replacement.promised_view),
                discard_damaged_tail: false,
            },
        )?;

        let OpenGroupJournal {
            mut directory,
            writer: old_writer,
            evidence,
            decode_limits,
            operation_limits,
            pins,
        } = self;
        let mut next = Manifest {
            generation: directory
                .manifest
                .generation
                .checked_add(1)
                .ok_or(SuffixReplacementError::ManifestGeneration)?,
            parent_generation: directory.manifest.generation,
            promised_view: replacement.promised_view,
            last_normal_view: replacement.last_normal_view,
            accepted,
            committed: replacement.committed,
            segments: directory.manifest.segments[..rewrite_index].to_vec(),
            ..directory.manifest.clone()
        };
        next.segments.push(SegmentReference {
            segment_id: next_segment_id,
            file_generation: 0,
            first_group_number: rewrite.first_group_number,
            first_chain: rewrite.first_chain,
            capacity: replacement.segment_capacity,
            sealed: None,
        });
        drop(old_writer);
        directory.install_selected_manifest(next)?;
        if directory.data_sync {
            writer.set_write_mode(
                &directory.segment_path(next_segment_id)?,
                crate::SegmentWriteMode::DataSync,
            )?;
        }
        Ok(Self {
            directory,
            writer,
            evidence,
            decode_limits,
            operation_limits,
            pins,
        })
    }
}

pub(crate) fn validate_request(
    source: &Source<'_>,
    replacement: SuffixReplacement,
    selected_suffix: &[CanonicalOperation<'_>],
    limits: SuffixReplacementLimits,
) -> Result<(), SuffixReplacementError> {
    validate_common_request(source, replacement, limits)?;
    if source.manifest.committed != replacement.protected_committed {
        return Err(SuffixReplacementError::ProtectedCommitMismatch);
    }
    if replacement.promised_view < source.manifest.promised_view
        || replacement.last_normal_view < source.manifest.last_normal_view
        || replacement.last_normal_view > replacement.promised_view
    {
        return Err(SuffixReplacementError::InvalidView);
    }
    enforce_limit(
        "selected suffix operations",
        selected_suffix.len(),
        limits.max_operations,
    )
}

pub(crate) fn validate_common_request(
    source: &Source<'_>,
    replacement: SuffixReplacement,
    limits: SuffixReplacementLimits,
) -> Result<(), SuffixReplacementError> {
    if limits.max_operations == 0
        || limits.max_body_bytes == 0
        || limits.max_segment_bytes < (SEGMENT_HEADER_BYTES + crate::WRITE_GROUP_ALIGNMENT) as u64
        || replacement.segment_capacity > limits.max_segment_bytes
    {
        return Err(SuffixReplacementError::InvalidLimits);
    }
    if source.manifest.commit_mode != CommitMode::External {
        return Err(SuffixReplacementError::LocalDurableJournal);
    }
    if source.current != replacement.expected_current {
        return Err(SuffixReplacementError::CurrentMismatch);
    }
    if !source.healthy || source.written != source.durable {
        return Err(SuffixReplacementError::SourceNotDurable);
    }
    Ok(())
}

pub(crate) fn rewrite_segment_index(
    manifest: &Manifest,
    protected: LogPosition,
) -> Result<usize, SuffixReplacementError> {
    for (index, reference) in manifest.segments.iter().enumerate() {
        let start = position_before(reference.first_chain)?;
        if start.op_number > protected.op_number
            || (start.op_number == protected.op_number && start.digest != protected.digest)
        {
            return Err(SuffixReplacementError::ProtectedCommitMismatch);
        }
        let Some(next) = manifest.segments.get(index + 1) else {
            return Ok(index);
        };
        let end = position_before(next.first_chain)?;
        if end.op_number < protected.op_number {
            continue;
        }
        if end.op_number == protected.op_number {
            if end.digest != protected.digest {
                return Err(SuffixReplacementError::ProtectedCommitMismatch);
            }
            return Ok(index + 1);
        }
        return Ok(index);
    }
    Err(SuffixReplacementError::ProtectedCommitMismatch)
}

fn retained_fragment(
    journal: &OpenGroupJournal,
    reference: SegmentReference,
    protected: LogPosition,
    limits: SuffixReplacementLimits,
) -> Result<Vec<OwnedOperation>, SuffixReplacementError> {
    let capacity =
        usize::try_from(reference.capacity).map_err(|_| SuffixReplacementError::LengthOverflow)?;
    let path = journal
        .directory
        .root
        .join("segments")
        .join(reference.file_name());
    require_regular_file(&path)?;
    let file = File::open(path)?;
    let length = usize::try_from(file.metadata()?.len())
        .map_err(|_| SuffixReplacementError::LengthOverflow)?;
    if length > capacity {
        return Err(SuffixReplacementError::SourceMismatch);
    }
    let mut bytes = Vec::with_capacity(length);
    file.take(capacity.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > capacity {
        return Err(SuffixReplacementError::SourceMismatch);
    }
    retained_image(&Source::new(journal), reference, protected, limits, &bytes)
}

pub(crate) fn retained_image(
    source: &Source<'_>,
    reference: SegmentReference,
    protected: LogPosition,
    limits: SuffixReplacementLimits,
    bytes: &[u8],
) -> Result<Vec<OwnedOperation>, SuffixReplacementError> {
    let scan = scan_segment(
        bytes,
        reference.first_group_number,
        reference.first_chain,
        source.decode,
    )?;
    if scan.header.segment_id() != reference.segment_id
        || scan.header.group_id() != source.manifest.identity.group_id
    {
        return Err(SuffixReplacementError::SourceMismatch);
    }
    if let Some(sealed) = reference.sealed {
        if scan.valid_bytes != sealed.valid_bytes
            || scan.digest != sealed.digest
            || matches!(scan.tail, TailState::Truncated { .. })
        {
            return Err(SuffixReplacementError::SourceMismatch);
        }
    } else if scan.valid_bytes != source.written.end_offset()
        || scan.next_chain != source.written.next_chain()
        || scan.header != *source.header
        || matches!(scan.tail, TailState::Truncated { .. })
    {
        return Err(SuffixReplacementError::SourceMismatch);
    }
    let mut result = Vec::new();
    let mut body_bytes = 0_usize;
    for operation in scan.groups.iter().flat_map(|group| &group.operations) {
        if operation.op_number > protected.op_number {
            break;
        }
        validate_operation_body(operation.kind, operation.body.as_ref(), source.operations)?;
        body_bytes = body_bytes
            .checked_add(operation.body.len())
            .ok_or(SuffixReplacementError::LengthOverflow)?;
        enforce_limit(
            "retained replacement body bytes",
            body_bytes,
            limits.max_body_bytes,
        )?;
        enforce_limit(
            "retained replacement operations",
            result.len().saturating_add(1),
            limits.max_operations,
        )?;
        result.push(OwnedOperation {
            group_id: operation.group_id,
            configuration_epoch: operation.configuration_epoch,
            original_view: operation.original_view,
            op_number: operation.op_number,
            previous_digest: operation.previous_digest,
            kind: operation.kind,
            body: operation.body.to_vec(),
        });
    }
    let reached = result
        .last()
        .map_or(position_before(reference.first_chain)?, |operation| {
            LogPosition {
                op_number: operation.op_number,
                digest: logical_operation_digest(&operation.borrowed()),
            }
        });
    if reached != protected {
        return Err(SuffixReplacementError::ProtectedCommitMismatch);
    }
    Ok(result)
}

pub(crate) fn validate_selected_suffix(
    source: &Source<'_>,
    replacement: SuffixReplacement,
    selected: &[CanonicalOperation<'_>],
    mut expected: ChainPosition,
) -> Result<ChainPosition, SuffixReplacementError> {
    for operation in selected {
        if operation.group_id != source.manifest.identity.group_id
            || operation.configuration_epoch != source.manifest.configuration_epoch
            || operation.original_view > replacement.promised_view
            || operation.op_number != expected.next_op_number()
            || operation.previous_digest != expected.previous_digest()
        {
            return Err(SuffixReplacementError::SelectedSuffixMismatch);
        }
        validate_operation_body(operation.kind, operation.body, source.operations)?;
        expected = ChainPosition::new(
            operation
                .op_number
                .checked_add(1)
                .ok_or(SuffixReplacementError::LengthOverflow)?,
            logical_operation_digest(operation),
        );
    }
    Ok(expected)
}

pub(crate) fn validate_new_commit(
    protected: LogPosition,
    committed: LogPosition,
    accepted: LogPosition,
    selected: &[CanonicalOperation<'_>],
) -> Result<(), SuffixReplacementError> {
    if committed.op_number < protected.op_number || committed.op_number > accepted.op_number {
        return Err(SuffixReplacementError::CommitNotSelected);
    }
    if committed.op_number == protected.op_number {
        return if committed.digest == protected.digest {
            Ok(())
        } else {
            Err(SuffixReplacementError::CommitNotSelected)
        };
    }
    if selected.iter().any(|operation| {
        operation.op_number == committed.op_number
            && logical_operation_digest(operation) == committed.digest
    }) {
        Ok(())
    } else {
        Err(SuffixReplacementError::CommitNotSelected)
    }
}

fn encode_replacement_segment(
    header: &SegmentHeader,
    first_group_number: u64,
    first_chain: ChainPosition,
    operations: &[CanonicalOperation<'_>],
    encoding: BodyEncoding,
) -> Result<Vec<u8>, SuffixReplacementError> {
    let mut bytes = encode_segment_header(header).to_vec();
    if !operations.is_empty() {
        let group = encode_group_with_body_encoding(
            header,
            first_group_number,
            SEGMENT_HEADER_BYTES as u64,
            first_chain,
            operations,
            encoding,
        )?;
        bytes.extend_from_slice(group.as_bytes());
    }
    Ok(bytes)
}

fn write_or_resume_segment(
    path: &Path,
    expected: &[u8],
    capacity: u64,
) -> Result<(), SuffixReplacementError> {
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => {
            allocate_segment(&file, capacity)?;
            file
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            require_regular_file(path)?;
            let mut file = OpenOptions::new().read(true).write(true).open(path)?;
            let length = usize::try_from(file.metadata()?.len())
                .map_err(|_| SuffixReplacementError::LengthOverflow)?;
            if length as u64 != capacity && length > expected.len() {
                return Err(SuffixReplacementError::ReplacementSegmentConflict);
            }
            let prefix = length.min(expected.len());
            let mut buffer = [0; 16 * 1024];
            for chunk in expected[..prefix].chunks(buffer.len()) {
                file.read_exact(&mut buffer[..chunk.len()])?;
                if chunk != &buffer[..chunk.len()] {
                    return Err(SuffixReplacementError::ReplacementSegmentConflict);
                }
            }
            if length >= expected.len() {
                if !crate::directory::remaining_file_is_zero(&mut file)? {
                    return Err(SuffixReplacementError::ReplacementSegmentConflict);
                }
                allocate_segment(&file, capacity)?;
                file.sync_all()?;
                return Ok(());
            }
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            allocate_segment(&file, capacity)?;
            file
        }
        Err(error) => return Err(error.into()),
    };
    file.write_all(expected)?;
    file.sync_all()?;
    Ok(())
}

pub(crate) fn position_before(chain: ChainPosition) -> Result<LogPosition, SuffixReplacementError> {
    Ok(LogPosition {
        op_number: chain
            .next_op_number()
            .checked_sub(1)
            .ok_or(SuffixReplacementError::InvalidPosition)?,
        digest: chain.previous_digest(),
    })
}

pub(crate) fn enforce_limit(
    kind: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), SuffixReplacementError> {
    if actual > limit {
        Err(SuffixReplacementError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn require_regular_file(path: &Path) -> Result<(), SuffixReplacementError> {
    if fs::symlink_metadata(path)?.file_type().is_file() {
        Ok(())
    } else {
        Err(SuffixReplacementError::ReplacementSegmentConflict)
    }
}

fn sync_directory(path: &Path) -> Result<(), SuffixReplacementError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn allocate_segment(file: &File, capacity: u64) -> Result<(), SuffixReplacementError> {
    rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, capacity)
        .map_err(io::Error::from)?;
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn allocate_segment(_file: &File, _capacity: u64) -> Result<(), SuffixReplacementError> {
    Err(SuffixReplacementError::AllocationUnsupported)
}

pub(crate) trait FollowingChain {
    fn following_chain(self) -> Result<ChainPosition, SuffixReplacementError>;
}

#[cfg(test)]
mod allocation_tests {
    use super::*;

    #[test]
    fn fixed_size_replacement_retry_reuses_exact_bytes_and_rejects_dirty_padding() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("replacement.log");
        let expected = vec![7; 8 * 1024];
        let capacity = 128 * 1024;
        write_or_resume_segment(&path, &expected, capacity).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), capacity);
        let original = fs::read(&path).unwrap();
        write_or_resume_segment(&path, &expected, capacity).unwrap();
        assert_eq!(fs::read(&path).unwrap(), original);

        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(capacity - 1)).unwrap();
        file.write_all(&[1]).unwrap();
        file.sync_all().unwrap();
        let damaged = fs::read(&path).unwrap();
        assert!(matches!(
            write_or_resume_segment(&path, &expected, capacity),
            Err(SuffixReplacementError::ReplacementSegmentConflict)
        ));
        assert_eq!(fs::read(&path).unwrap(), damaged);
    }
}

impl FollowingChain for LogPosition {
    fn following_chain(self) -> Result<ChainPosition, SuffixReplacementError> {
        if self.op_number != u64::MAX && (self.op_number == 0) == (self.digest == Digest::ZERO) {
            Ok(ChainPosition::new(self.op_number + 1, self.digest))
        } else {
            Err(SuffixReplacementError::InvalidPosition)
        }
    }
}

/// Suffix authorization, bounded rewrite, or publication failure.
#[derive(Debug, Error)]
pub enum SuffixReplacementError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error(transparent)]
    /// Journal directory validation or publication failed.
    Directory(#[from] DirectoryError),
    #[error(transparent)]
    /// Physical segment framing or integrity validation failed.
    Codec(#[from] CodecError),
    #[error(transparent)]
    /// The segment writer rejected this transition.
    Writer(#[from] WriterError),
    #[error(transparent)]
    /// Canonical operation-body validation failed.
    Operation(#[from] ozzy_journal::operation::OperationCodecError),
    #[error("suffix replacement limits are invalid")]
    /// Suffix replacement limits are invalid.
    InvalidLimits,
    #[error("suffix replacement is not valid for a local-durable journal")]
    /// Suffix replacement is not valid for a local-durable journal.
    LocalDurableJournal,
    #[error("suffix replacement does not name the selected manifest generation")]
    /// Suffix replacement does not name the selected manifest generation.
    CurrentMismatch,
    #[error("suffix replacement does not preserve the exact committed prefix")]
    /// Suffix replacement does not preserve the exact committed prefix.
    ProtectedCommitMismatch,
    #[error("suffix replacement source has unsynchronized or faulted writes")]
    /// Suffix replacement source has unsynchronized or faulted writes.
    SourceNotDurable,
    #[error("suffix replacement view state regresses or is inconsistent")]
    /// Suffix replacement view state regresses or is inconsistent.
    InvalidView,
    #[error("selected suffix does not continue the protected journal chain")]
    /// Selected suffix does not continue the protected journal chain.
    SelectedSuffixMismatch,
    #[error("new commit position is not present in the selected suffix")]
    /// New commit position is not present in the selected suffix.
    CommitNotSelected,
    #[error("replacement source segment does not match selected storage")]
    /// Replacement source segment does not match selected storage.
    SourceMismatch,
    #[error("replacement segment ID space is exhausted")]
    /// Replacement segment ID space is exhausted.
    SegmentIdExhausted,
    #[error("replacement manifest generation is exhausted")]
    /// Replacement manifest generation is exhausted.
    ManifestGeneration,
    #[error("replacement position is invalid")]
    /// Replacement position is invalid.
    InvalidPosition,
    #[error("replacement segment conflicts with an interrupted prior attempt")]
    /// Replacement segment conflicts with an interrupted prior attempt.
    ReplacementSegmentConflict,
    #[error("physical segment allocation is unsupported on this platform")]
    /// Physical segment allocation is unsupported on this platform.
    AllocationUnsupported,
    #[error("suffix replacement length arithmetic overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
    #[error("suffix installer faulted; reopen the selected journal")]
    /// Suffix installer faulted; reopen the selected journal.
    StagingFaulted,
    #[error("selected suffix has not reached its exact accepted and committed anchors")]
    /// Selected suffix has not reached its exact accepted and committed anchors.
    IncompleteSuffix,
    #[error("suffix installation reused the source writer generation")]
    /// Suffix installation reused the source writer generation.
    ReusedWriterGeneration,
    #[error("suffix staging allocation failed")]
    /// Suffix staging allocation failed.
    Allocation,
    #[error("suffix staging disk quota exceeded: {actual} > {limit}")]
    /// Suffix staging disk quota exceeded.
    StagingQuota {
        #[doc = "Observed size, count, or fenced field value."]
        actual: u64,
        #[doc = "Configured maximum for the reported resource."]
        limit: u64,
    },
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    /// The named resource exceeds its configured bound.
    LimitExceeded {
        /// Resource bound that rejected the operation.
        kind: &'static str,
        /// Observed size, count, or fenced field value.
        actual: usize,
        /// Configured maximum for the reported resource.
        limit: usize,
    },
}
