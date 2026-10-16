//! Manifest encoding, decoding, and validation, with its segment references.

use super::{
    CheckpointReference, CommitMode, GroupIdentity, LogPosition, MANIFEST_HEADER_BYTES,
    METADATA_VERSION, Manifest, MetadataError, MetadataLimits, SEGMENT_REFERENCE_BYTES,
    SealedSegment, SegmentReference, all_zero, enforce_limit, require_len, require_nonzero_id,
    require_version, usize_from_u64, verify_hash,
};
use crate::{SEGMENT_HEADER_BYTES, WRITE_GROUP_ALIGNMENT};
use ozzy_journal::integrity::hash_without_digest;
use ozzy_journal::operation::{ChainPosition, Digest};
use ozzy_proto::layout::{self, Fixed};
use ozzy_proto::{CheckpointId, GroupId, NodeId, StoreId, VolumeId};
use std::ops::Range;

const MANIFEST_MAGIC: &[u8; 8] = b"OZYMAN\0\0";
const MANIFEST_HASH_CONTEXT: &str = "ozzy journal manifest v1";
const MANIFEST_DIGEST_RANGE: Range<usize> = 312..344;
const MANIFEST_CHECKPOINT: u32 = 1 << 0;
const MANIFEST_LOCAL_COMMIT: u32 = 1 << 1;
const MANIFEST_DURABLE_EVIDENCE: u32 = 1 << 2;
const SEGMENT_ACTIVE: u32 = 1 << 0;

/// Validate and encode selected journal metadata with its integrity fields.
pub fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, MetadataError> {
    encode_manifest_with_limits(manifest, MetadataLimits::default())
}

/// Encode one manifest after checking caller-selected allocation bounds.
pub fn encode_manifest_with_limits(
    manifest: &Manifest,
    limits: MetadataLimits,
) -> Result<Vec<u8>, MetadataError> {
    validate_manifest(manifest)?;
    enforce_limit(
        "manifest segment count",
        manifest.segments.len(),
        limits.max_segments,
    )?;
    let segment_count =
        u32::try_from(manifest.segments.len()).map_err(|_| MetadataError::LengthOverflow)?;
    let body_bytes = manifest
        .segments
        .len()
        .checked_mul(SEGMENT_REFERENCE_BYTES)
        .ok_or(MetadataError::LengthOverflow)?;
    let total_bytes = MANIFEST_HEADER_BYTES
        .checked_add(body_bytes)
        .ok_or(MetadataError::LengthOverflow)?;
    enforce_limit("manifest bytes", total_bytes, limits.max_manifest_bytes)?;
    let mut output = vec![0_u8; total_bytes];
    let (header, references) = output
        .split_first_chunk_mut::<MANIFEST_HEADER_BYTES>()
        .ok_or(MetadataError::LengthOverflow)?;
    header.put::<0, 8>(*MANIFEST_MAGIC);
    header.put_u16::<8>(METADATA_VERSION);
    header.put_u16::<10>(MANIFEST_HEADER_BYTES as u16);
    let flags = (u32::from(manifest.checkpoint.is_some()) * MANIFEST_CHECKPOINT)
        | (u32::from(manifest.commit_mode == CommitMode::LocalDurable) * MANIFEST_LOCAL_COMMIT)
        | (u32::from(manifest.durable_evidence) * MANIFEST_DURABLE_EVIDENCE);
    header.put_u32::<12>(flags);
    header.put_u64::<16>(total_bytes as u64);
    header.put_u64::<24>(manifest.generation);
    header.put_u64::<32>(manifest.parent_generation);
    header.put::<40, 16>(*manifest.identity.group_id.as_bytes());
    header.put::<56, 16>(*manifest.identity.replica_node_id.as_bytes());
    header.put::<72, 16>(*manifest.identity.volume_id.as_bytes());
    header.put::<88, 16>(*manifest.identity.store_id.as_bytes());
    header.put_u64::<104>(manifest.configuration_epoch);
    header.put_u64::<112>(manifest.promised_view);
    header.put_u64::<120>(manifest.last_normal_view);
    header.put::<128, 40>(position_bytes(manifest.accepted));
    header.put::<168, 40>(position_bytes(manifest.committed));
    if let Some(checkpoint) = manifest.checkpoint {
        header.put::<208, 16>(*checkpoint.checkpoint_id.as_bytes());
        header.put::<224, 40>(position_bytes(checkpoint.position));
        header.put::<264, 32>(*checkpoint.manifest_digest.as_bytes());
    }
    header.put_u32::<296>(segment_count);
    header.put_u64::<304>(body_bytes as u64);
    header.put_u64::<344>(manifest.identity.store_generation);
    let references = references.as_chunks_mut::<SEGMENT_REFERENCE_BYTES>().0;
    for (output, segment) in references.iter_mut().zip(&manifest.segments) {
        encode_segment_reference(*segment, output);
    }
    let digest = hash_without_digest(MANIFEST_HASH_CONTEXT, &output, MANIFEST_DIGEST_RANGE.start);
    layout::patch(&mut output, MANIFEST_DIGEST_RANGE.start, *digest.as_bytes())?;
    Ok(output)
}

/// Validate and decode bounded selected journal metadata.
pub fn decode_manifest(input: &[u8], limits: MetadataLimits) -> Result<Manifest, MetadataError> {
    require_len(input, MANIFEST_HEADER_BYTES, "manifest header")?;
    enforce_limit("manifest bytes", input.len(), limits.max_manifest_bytes)?;
    let (header, references) = input
        .split_first_chunk::<MANIFEST_HEADER_BYTES>()
        .ok_or(MetadataError::LengthOverflow)?;
    let segment_count = check_manifest_framing(input, header, limits)?;
    let identity = GroupIdentity {
        group_id: GroupId::from_bytes(header.array::<40, 16>()),
        replica_node_id: NodeId::from_bytes(header.array::<56, 16>()),
        volume_id: VolumeId::from_bytes(header.array::<72, 16>()),
        store_id: StoreId::from_bytes(header.array::<88, 16>()),
        store_generation: header.u64_at::<344>(),
    };
    identity.validate()?;
    let flags = header.u32_at::<12>();
    let commit_mode = if flags & MANIFEST_LOCAL_COMMIT == 0 {
        CommitMode::External
    } else {
        CommitMode::LocalDurable
    };
    let checkpoint = decode_checkpoint_reference(header, flags)?;
    let mut segments = Vec::with_capacity(segment_count);
    for reference in references.as_chunks::<SEGMENT_REFERENCE_BYTES>().0 {
        segments.push(decode_segment_reference(reference)?);
    }
    let manifest = Manifest {
        generation: header.u64_at::<24>(),
        parent_generation: header.u64_at::<32>(),
        identity,
        configuration_epoch: header.u64_at::<104>(),
        commit_mode,
        durable_evidence: flags & MANIFEST_DURABLE_EVIDENCE != 0,
        promised_view: header.u64_at::<112>(),
        last_normal_view: header.u64_at::<120>(),
        accepted: read_position(header.array::<128, 40>()),
        committed: read_position(header.array::<168, 40>()),
        checkpoint,
        segments,
    };
    validate_manifest(&manifest)?;
    Ok(manifest)
}

/// Check the manifest header's magic, version, flags, reserved fields, and
/// lengths, then its digest. Returns the segment count.
fn check_manifest_framing(
    input: &[u8],
    header: &[u8; MANIFEST_HEADER_BYTES],
    limits: MetadataLimits,
) -> Result<usize, MetadataError> {
    if header.array::<0, 8>() != *MANIFEST_MAGIC {
        return Err(MetadataError::WrongMagic("manifest"));
    }
    require_version(header.u16_at::<8>())?;
    let unknown_flags = header.u32_at::<12>()
        & !(MANIFEST_CHECKPOINT | MANIFEST_LOCAL_COMMIT | MANIFEST_DURABLE_EVIDENCE);
    if header.u16_at::<10>() != MANIFEST_HEADER_BYTES as u16
        || unknown_flags != 0
        || header.u32_at::<300>() != 0
        || !all_zero(header.slice::<352, { MANIFEST_HEADER_BYTES - 352 }>())
    {
        return Err(MetadataError::UnsupportedFields("manifest"));
    }
    let total_bytes = usize_from_u64(header.u64_at::<16>())?;
    let body_bytes = usize_from_u64(header.u64_at::<304>())?;
    let segment_count = header.u32_at::<296>() as usize;
    enforce_limit("manifest segment count", segment_count, limits.max_segments)?;
    let expected_body = segment_count
        .checked_mul(SEGMENT_REFERENCE_BYTES)
        .ok_or(MetadataError::LengthOverflow)?;
    let expected_total = MANIFEST_HEADER_BYTES
        .checked_add(expected_body)
        .ok_or(MetadataError::LengthOverflow)?;
    if body_bytes != expected_body || total_bytes != expected_total {
        return Err(MetadataError::UnsupportedFields("manifest lengths"));
    }
    require_len(input, total_bytes, "manifest")?;
    if input.len() != total_bytes {
        return Err(MetadataError::TrailingBytes(input.len() - total_bytes));
    }
    verify_hash(
        MANIFEST_HASH_CONTEXT,
        input,
        MANIFEST_DIGEST_RANGE,
        "manifest",
    )?;
    Ok(segment_count)
}

/// The checkpoint reference the flags select; unselected fields stay zero.
fn decode_checkpoint_reference(
    header: &[u8; MANIFEST_HEADER_BYTES],
    flags: u32,
) -> Result<Option<CheckpointReference>, MetadataError> {
    if flags & MANIFEST_CHECKPOINT == 0 {
        if !all_zero(header.slice::<208, 88>()) {
            return Err(MetadataError::InvalidCheckpoint);
        }
        return Ok(None);
    }
    Ok(Some(CheckpointReference {
        checkpoint_id: CheckpointId::from_bytes(header.array::<208, 16>()),
        position: read_position(header.array::<224, 40>()),
        manifest_digest: Digest::from_bytes(header.array::<264, 32>()),
    }))
}

/// Validate encoded manifest bytes and return digest named by `CURRENT`.
pub fn manifest_digest(input: &[u8], limits: MetadataLimits) -> Result<Digest, MetadataError> {
    decode_manifest(input, limits)?;
    Ok(Digest::from_bytes(*layout::record(
        input,
        MANIFEST_DIGEST_RANGE.start,
    )?))
}

fn validate_manifest(manifest: &Manifest) -> Result<(), MetadataError> {
    manifest.identity.validate()?;
    if manifest.generation == 0 || manifest.parent_generation >= manifest.generation {
        return Err(MetadataError::InvalidGeneration);
    }
    if manifest.last_normal_view > manifest.promised_view {
        return Err(MetadataError::InvalidView);
    }
    manifest.accepted.validate()?;
    manifest.committed.validate()?;
    if manifest.committed.op_number > manifest.accepted.op_number {
        return Err(MetadataError::CommitBeyondAccepted);
    }
    if let Some(checkpoint) = manifest.checkpoint {
        require_nonzero_id("checkpoint", checkpoint.checkpoint_id.as_bytes())?;
        checkpoint.position.validate()?;
        if checkpoint.position.op_number == 0
            || checkpoint.position.op_number > manifest.committed.op_number
            || checkpoint.manifest_digest == Digest::ZERO
        {
            return Err(MetadataError::InvalidCheckpoint);
        }
    }
    validate_segments(&manifest.segments)
}

fn validate_segments(segments: &[SegmentReference]) -> Result<(), MetadataError> {
    if segments.is_empty() {
        return Err(MetadataError::InvalidActiveSegment);
    }
    let mut previous_id = 0;
    let mut previous_group = 0;
    let mut previous_op = 0;
    for (index, segment) in segments.iter().enumerate() {
        if segment.segment_id <= previous_id
            || segment.first_group_number == 0
            || segment.first_group_number <= previous_group
            || segment.first_chain.next_op_number() == 0
            || segment.first_chain.next_op_number() <= previous_op
            || segment.capacity < (SEGMENT_HEADER_BYTES + WRITE_GROUP_ALIGNMENT) as u64
            || !segment
                .capacity
                .is_multiple_of(WRITE_GROUP_ALIGNMENT as u64)
        {
            return Err(MetadataError::InvalidSegmentReference);
        }
        match segment.sealed {
            None if index + 1 == segments.len() => {}
            None => return Err(MetadataError::InvalidActiveSegment),
            Some(_) if index + 1 == segments.len() => {
                return Err(MetadataError::InvalidActiveSegment);
            }
            Some(sealed)
                if sealed.valid_bytes < SEGMENT_HEADER_BYTES as u64
                    || sealed.valid_bytes > segment.capacity
                    || !sealed
                        .valid_bytes
                        .is_multiple_of(WRITE_GROUP_ALIGNMENT as u64)
                    || sealed.digest == Digest::ZERO =>
            {
                return Err(MetadataError::InvalidSegmentReference);
            }
            Some(_) => {}
        }
        previous_id = segment.segment_id;
        previous_group = segment.first_group_number;
        previous_op = segment.first_chain.next_op_number();
    }
    Ok(())
}

fn encode_segment_reference(segment: SegmentReference, output: &mut [u8; SEGMENT_REFERENCE_BYTES]) {
    output.put_u64::<0>(segment.segment_id);
    output.put_u64::<8>(segment.first_group_number);
    output.put_u64::<16>(segment.first_chain.next_op_number());
    output.put::<24, 32>(*segment.first_chain.previous_digest().as_bytes());
    output.put_u64::<56>(segment.capacity);
    if let Some(sealed) = segment.sealed {
        output.put_u64::<64>(sealed.valid_bytes);
        output.put::<72, 32>(*sealed.digest.as_bytes());
    } else {
        output.put_u32::<104>(SEGMENT_ACTIVE);
    }
    output.put_u64::<108>(segment.file_generation);
}

fn decode_segment_reference(
    input: &[u8; SEGMENT_REFERENCE_BYTES],
) -> Result<SegmentReference, MetadataError> {
    let flags = input.u32_at::<104>();
    if flags & !SEGMENT_ACTIVE != 0
        || !all_zero(input.slice::<116, { SEGMENT_REFERENCE_BYTES - 116 }>())
    {
        return Err(MetadataError::UnsupportedFields("segment reference"));
    }
    let valid_bytes = input.u64_at::<64>();
    let digest = Digest::from_bytes(input.array::<72, 32>());
    let sealed = if flags & SEGMENT_ACTIVE != 0 {
        if valid_bytes != 0 || digest != Digest::ZERO {
            return Err(MetadataError::InvalidSegmentReference);
        }
        None
    } else {
        Some(SealedSegment {
            valid_bytes,
            digest,
        })
    };
    Ok(SegmentReference {
        segment_id: input.u64_at::<0>(),
        file_generation: input.u64_at::<108>(),
        first_group_number: input.u64_at::<8>(),
        first_chain: ChainPosition::new(
            input.u64_at::<16>(),
            Digest::from_bytes(input.array::<24, 32>()),
        ),
        capacity: input.u64_at::<56>(),
        sealed,
    })
}

fn position_bytes(position: LogPosition) -> [u8; 40] {
    let mut bytes = [0; 40];
    bytes.put_u64::<0>(position.op_number);
    bytes.put::<8, 32>(*position.digest.as_bytes());
    bytes
}

fn read_position(bytes: [u8; 40]) -> LogPosition {
    LogPosition {
        op_number: bytes.u64_at::<0>(),
        digest: Digest::from_bytes(bytes.array::<8, 32>()),
    }
}
