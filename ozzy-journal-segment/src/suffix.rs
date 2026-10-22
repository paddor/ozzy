//! Atomic installation of one consensus-selected journal suffix.

pub(crate) mod state;
pub(crate) mod streaming;
use state::Source;
pub use streaming::SuffixStreamLimits;

use std::io;

use ozzy_journal::operation::{
    CanonicalOperation, OperationKind, logical_operation_digest, validate_operation_body,
};
use ozzy_journal::progress::JournalGeneration;
use thiserror::Error;

use crate::{
    BodyEncoding, CodecError, CommitMode, CurrentReference, Digest, DirectoryError, LogPosition,
    Manifest, MetadataError, SEGMENT_HEADER_BYTES, SegmentReference, SegmentScan, TailState,
    WriterError, metadata::position_before, scan_segment,
};

/// Consensus authorization and resulting hard state for one suffix install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuffixReplacement {
    /// Exact CURRENT selection required before suffix installation.
    pub expected_current: CurrentReference,
    /// Confirmed canonical prefix that replacement must preserve.
    pub protected_committed: LogPosition,
    /// Local prefix, from `protected_committed` through `committed`, that
    /// already lies in the selected lineage. Replacement keeps it and stages
    /// only what follows.
    pub retained: LogPosition,
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
    if !scan_matches(source, reference, &scan) {
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

/// Whether a scanned segment is the referenced one: the same segment and
/// group, untruncated, and matching its seal, or the written position and
/// header when it is the active segment.
fn scan_matches(source: &Source<'_>, reference: SegmentReference, scan: &SegmentScan<'_>) -> bool {
    if scan.header.segment_id() != reference.segment_id
        || scan.header.group_id() != source.manifest.identity.group_id
        || matches!(scan.tail, TailState::Truncated { .. })
    {
        return false;
    }
    match reference.sealed {
        Some(sealed) => scan.valid_bytes == sealed.valid_bytes && scan.digest == sealed.digest,
        None => {
            scan.valid_bytes == source.written.end_offset()
                && scan.next_chain == source.written.next_chain()
                && scan.header == *source.header
        }
    }
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

/// Suffix authorization, bounded rewrite, or publication failure.
#[derive(Debug, Error)]
pub enum SuffixReplacementError {
    #[error(transparent)]
    /// Private checkpoint receive or immutable artifact verification failed.
    CheckpointBytes(#[from] crate::CheckpointError),
    #[error(transparent)]
    /// Canonical checkpoint metadata or identity section validation failed.
    CheckpointSections(#[from] ozzy_core::checkpoint::Error),
    #[error(transparent)]
    /// Imported canonical checkpoint state could not be encoded.
    Snapshot(#[from] ozzy_core::state::StateSnapshotError),
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
    #[error(transparent)]
    /// A replacement position is not a valid canonical prefix.
    Metadata(#[from] MetadataError),
    #[error("replacement segment conflicts with an interrupted prior attempt")]
    /// Replacement segment conflicts with an interrupted prior attempt.
    ReplacementSegmentConflict,
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
