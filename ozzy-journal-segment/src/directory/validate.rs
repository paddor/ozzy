//! Pure checks on segments, operations, checkpoints, and metadata successors.

use ozzy_journal::operation::{OperationLimits, validate_operation_body};

use super::DirectoryError;
use crate::{
    ChainPosition, GroupIdentity, LogPosition, Manifest, SegmentHeader, SegmentReference,
    WriterError,
};

/// Maximum immutable adapter configuration bytes stored beside a group identity.
pub const GROUP_CONFIGURATION_MAX_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy)]
pub(crate) struct OperationEnvelope<'a> {
    pub(crate) kind: crate::OperationKind,
    pub(crate) body: &'a [u8],
    pub(crate) op_number: u64,
    pub(crate) configuration_epoch: u64,
    pub(crate) original_view: u64,
}

pub(crate) fn validate_initial_segment(
    identity: GroupIdentity,
    segment: &SegmentHeader,
) -> Result<(), DirectoryError> {
    if segment.group_id() != identity.group_id
        || segment.file_generation() != 0
        || segment.predecessor_segment_id().is_some()
        || segment.predecessor_digest() != crate::Digest::ZERO
    {
        return Err(DirectoryError::InvalidInitialSegment);
    }
    Ok(())
}

pub(crate) fn validate_segment_header(
    manifest: &Manifest,
    previous: Option<&SegmentReference>,
    reference: &SegmentReference,
    header: &SegmentHeader,
    length: u64,
) -> Result<(), DirectoryError> {
    if header.group_id() != manifest.identity.group_id
        || header.segment_id() != reference.segment_id
        || header.file_generation() != reference.file_generation
        || header.capacity() != reference.capacity
    {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    if let Some(predecessor) = previous
        && (header.predecessor_segment_id() != Some(predecessor.segment_id)
            || header.predecessor_digest() != reference.first_chain.previous_digest())
    {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    if length < crate::SEGMENT_HEADER_BYTES as u64 || length > reference.capacity {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    if let Some(sealed) = reference.sealed
        && length < sealed.valid_bytes
    {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    Ok(())
}

pub(crate) fn validate_checkpoint_successor(
    current: &Manifest,
    checkpoint: &crate::CheckpointManifest,
) -> Result<(), DirectoryError> {
    if checkpoint.group_id != current.identity.group_id
        || checkpoint.store_id != current.identity.store_id
        || checkpoint.configuration_epoch != current.configuration_epoch
        || checkpoint.source_manifest_generation > current.generation
        || checkpoint.position.op_number > current.committed.op_number
    {
        return Err(DirectoryError::CheckpointMismatch);
    }
    if current
        .checkpoint
        .is_some_and(|previous| checkpoint.position.op_number <= previous.position.op_number)
    {
        return Err(DirectoryError::CheckpointRegression);
    }
    Ok(())
}

pub(crate) fn validate_checkpoint_source(
    identity: GroupIdentity,
    checkpoint: &crate::CheckpointManifest,
    source: &Manifest,
    digest: crate::Digest,
) -> Result<(), DirectoryError> {
    if digest != checkpoint.source_manifest_digest
        || source.identity != identity
        || source.generation != checkpoint.source_manifest_generation
        || source.configuration_epoch != checkpoint.configuration_epoch
        || source.committed != checkpoint.position
    {
        return Err(DirectoryError::CheckpointMismatch);
    }
    Ok(())
}

pub(crate) fn scan_contains_position(
    scan: &crate::SegmentScan<'_>,
    initial: ChainPosition,
    target: ChainPosition,
) -> bool {
    initial == target
        || scan
            .groups
            .iter()
            .flat_map(|group| group.operations.iter())
            .any(|operation| {
                operation.op_number.checked_add(1) == Some(target.next_op_number())
                    && operation.digest == target.previous_digest()
            })
}

pub(crate) fn validate_operation_bodies(
    scan: &crate::SegmentScan<'_>,
    limits: OperationLimits,
    configuration_epoch: u64,
    promised_view: u64,
) -> Result<(), DirectoryError> {
    for operation in scan.groups.iter().flat_map(|group| group.operations.iter()) {
        validate_operation(
            OperationEnvelope {
                kind: operation.kind,
                body: operation.body.as_ref(),
                op_number: operation.op_number,
                configuration_epoch: operation.configuration_epoch,
                original_view: operation.original_view,
            },
            limits,
            configuration_epoch,
            promised_view,
        )?;
    }
    Ok(())
}

pub(crate) fn validate_operation(
    operation: OperationEnvelope<'_>,
    limits: OperationLimits,
    expected_configuration_epoch: u64,
    promised_view: u64,
) -> Result<(), DirectoryError> {
    validate_operation_body(operation.kind, operation.body, limits)?;
    validate_operation_metadata(operation, expected_configuration_epoch, promised_view)
}

pub(crate) fn validate_operation_metadata(
    operation: OperationEnvelope<'_>,
    expected_configuration_epoch: u64,
    promised_view: u64,
) -> Result<(), DirectoryError> {
    if operation.configuration_epoch != expected_configuration_epoch {
        return Err(WriterError::ConfigurationMismatch {
            op_number: operation.op_number,
            actual: operation.configuration_epoch,
            expected: expected_configuration_epoch,
        }
        .into());
    }
    if operation.original_view > promised_view {
        return Err(WriterError::ViewBeyondPromise {
            op_number: operation.op_number,
            actual: operation.original_view,
            promised: promised_view,
        }
        .into());
    }
    Ok(())
}

pub(crate) fn validate_metadata_successor_fields(
    current: &Manifest,
    next: &Manifest,
) -> Result<(), DirectoryError> {
    if next.configuration_epoch != current.configuration_epoch {
        return Err(DirectoryError::ConfigurationChangeRequiresInstall);
    }
    if next.durable_evidence != current.durable_evidence {
        return Err(DirectoryError::ConfigurationChangeRequiresInstall);
    }
    if next.commit_mode != current.commit_mode {
        return Err(DirectoryError::CommitModeChange);
    }
    if next.checkpoint != current.checkpoint {
        return Err(DirectoryError::CheckpointChangeRequiresInstall);
    }
    if next.promised_view < current.promised_view
        || next.last_normal_view < current.last_normal_view
        || position_regresses(current.accepted, next.accepted)
        || position_regresses(current.committed, next.committed)
    {
        return Err(DirectoryError::HardStateRegression);
    }
    Ok(())
}

pub(crate) fn position_regresses(current: LogPosition, next: LogPosition) -> bool {
    next.op_number < current.op_number
        || (next.op_number == current.op_number && next.digest != current.digest)
}

pub(crate) fn validate_configuration_length(length: usize) -> Result<(), DirectoryError> {
    if length == 0 || length > GROUP_CONFIGURATION_MAX_BYTES {
        return Err(DirectoryError::ConfigurationLength);
    }
    Ok(())
}
