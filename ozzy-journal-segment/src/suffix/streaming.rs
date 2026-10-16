//! Bounded selected-history staging with one final manifest publication.

use ozzy_journal::operation::{
    canonical_body_digest, logical_operation_digest_with_body_digest, validate_operation_body,
};

use super::state::Source;
use super::{
    SuffixReplacement, SuffixReplacementError as Error, SuffixReplacementLimits, enforce_limit,
    validate_request,
};
use crate::{CanonicalOperation, ChainPosition, CodecError, Digest, ENTRY_HEADER_BYTES};
use crate::{SuffixRequest, metadata::position_before};

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

pub(crate) fn validate_stream_request(
    source: &Source<'_>,
    request: &SuffixRequest,
) -> Result<(), Error> {
    let limits = request.limits;
    validate_request(
        source,
        request.replacement,
        &[],
        SuffixReplacementLimits {
            max_operations: limits.max_group_operations,
            max_body_bytes: limits.max_group_body_bytes,
            max_segment_bytes: limits.max_staged_bytes,
        },
    )?;
    validate_stream_limits(source, request)
}

pub(crate) fn validate_stream_limits(
    source: &Source<'_>,
    request: &SuffixRequest,
) -> Result<(), Error> {
    let SuffixRequest {
        replacement,
        accepted,
        limits,
    } = *request;
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
    request: &SuffixRequest,
    mut expected: ChainPosition,
    body_digests: &mut Vec<Digest>,
    operations: &[CanonicalOperation<'_>],
) -> Result<(usize, bool), Error> {
    let SuffixRequest {
        replacement,
        accepted,
        limits,
    } = *request;
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
        if !continues(source, replacement, expected, operation) {
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
        check_entry(source, operation)?;
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

/// Whether an operation belongs to this group and epoch, stays within the
/// promised view, and follows the expected chain position.
fn continues(
    source: &Source<'_>,
    replacement: SuffixReplacement,
    expected: ChainPosition,
    operation: &CanonicalOperation<'_>,
) -> bool {
    operation.group_id == source.manifest.identity.group_id
        && operation.configuration_epoch == source.manifest.configuration_epoch
        && operation.original_view <= replacement.promised_view
        && operation.op_number == expected.next_op_number()
        && operation.previous_digest == expected.previous_digest()
}

/// One operation's decoded and aligned entry sizes stay within the decode
/// limits, and its body is valid for its kind.
fn check_entry(source: &Source<'_>, operation: &CanonicalOperation<'_>) -> Result<(), Error> {
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
    Ok(())
}
