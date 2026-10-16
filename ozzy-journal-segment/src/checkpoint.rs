//! Immutable, chunked checkpoint artifacts.

#![cfg_attr(not(test), warn(clippy::indexing_slicing))]

pub(crate) mod asynchronous;
mod manifest_builder;
pub(crate) use manifest_builder::ManifestBuilder;

use std::io;

use ozzy_journal::integrity::{IntegrityHasher as Hasher, hash_without_digest};
use ozzy_proto::layout::{self, Fixed, LayoutError};
use ozzy_proto::{CheckpointId, GroupId, StoreId};
use thiserror::Error;

use crate::{Digest, LogPosition};

/// Exact encoded checkpoint manifest header length.
pub const CHECKPOINT_HEADER_BYTES: usize = 4096;
/// Exact encoded checkpoint chunk descriptor length.
pub const CHECKPOINT_CHUNK_ENTRY_BYTES: usize = 64;

const CHECKPOINT_MAGIC: &[u8; 8] = b"OZYCHK01";
const CHECKPOINT_VERSION: u16 = 2;
const CHECKPOINT_MANIFEST_FILE: &str = "manifest";
const CHECKPOINT_MANIFEST_HASH_CONTEXT: &str = "ozzy journal checkpoint manifest v1";
const CHECKPOINT_CHUNK_HASH_CONTEXT: &str = "ozzy journal checkpoint chunk v1";
pub(crate) const CHECKPOINT_STATE_HASH_CONTEXT: &str = "ozzy journal checkpoint state v1";
const CHECKPOINT_DIGEST_START: usize = 256;

/// Bounds for checkpoint metadata, chunks, and optional materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointLimits {
    /// Maximum encoded manifest bytes, including its header.
    pub max_manifest_bytes: usize,
    /// Maximum state chunks described by one checkpoint.
    pub max_chunks: usize,
    /// Maximum bytes in one checkpoint chunk.
    pub max_chunk_bytes: usize,
    /// Maximum total checkpoint state bytes.
    pub max_state_bytes: u64,
}

impl Default for CheckpointLimits {
    fn default() -> Self {
        Self {
            max_manifest_bytes: 16 * 1024 * 1024,
            max_chunks: 65_536,
            max_chunk_bytes: 64 * 1024 * 1024,
            max_state_bytes: 64 * 1024 * 1024 * 1024,
        }
    }
}

/// Caller-frozen identity and lineage for one checkpoint build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointSpec {
    /// Persistent partition replication-group identity.
    pub group_id: GroupId,
    /// Persistent local journal-store identity.
    pub store_id: StoreId,
    /// Exact checkpoint artifact identity.
    pub checkpoint_id: CheckpointId,
    /// Canonical operation number and digest represented by this checkpoint.
    pub position: LogPosition,
    /// Selected membership/configuration epoch.
    pub configuration_epoch: u64,
    /// Exact manifest generation captured for checkpoint construction.
    pub source_manifest_generation: u64,
    /// Integrity digest of the exact captured source manifest.
    pub source_manifest_digest: Digest,
    /// Digest identifying the canonical state schema, not the state bytes.
    pub state_schema_digest: Digest,
    /// Physical chunk target. This is recorded in the immutable manifest.
    pub chunk_bytes: usize,
}

/// One exact state chunk named by ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointChunk {
    /// Zero-based chunk position in canonical state order.
    pub ordinal: u32,
    /// First byte offset within the logical checkpoint state.
    pub logical_offset: u64,
    /// Physical byte count of this chunk or unvalidated tail.
    pub bytes: u64,
    /// Integrity digest bound to this exact object or canonical prefix.
    pub digest: Digest,
}

/// Complete immutable checkpoint manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointManifest {
    /// Persistent partition replication-group identity.
    pub group_id: GroupId,
    /// Persistent local journal-store identity.
    pub store_id: StoreId,
    /// Exact checkpoint artifact identity.
    pub checkpoint_id: CheckpointId,
    /// Canonical operation number and digest represented by this checkpoint.
    pub position: LogPosition,
    /// Selected membership/configuration epoch.
    pub configuration_epoch: u64,
    /// Exact manifest generation captured for checkpoint construction.
    pub source_manifest_generation: u64,
    /// Integrity digest of the exact captured source manifest.
    pub source_manifest_digest: Digest,
    /// Exact application-state schema binding.
    pub state_schema_digest: Digest,
    /// Combined logical state bytes across all chunks.
    pub state_bytes: u64,
    /// Maximum logical bytes per checkpoint chunk.
    pub chunk_bytes: u32,
    /// Integrity digest over the complete logical state.
    pub state_digest: Digest,
    /// Chunk descriptors in logical state order.
    pub chunks: Vec<CheckpointChunk>,
}

/// Validate and encode bounded checkpoint metadata with its integrity fields.
pub fn encode_checkpoint_manifest(
    manifest: &CheckpointManifest,
    limits: CheckpointLimits,
) -> Result<Vec<u8>, CheckpointError> {
    validate_manifest(manifest, limits)?;
    let entries_bytes = manifest
        .chunks
        .len()
        .checked_mul(CHECKPOINT_CHUNK_ENTRY_BYTES)
        .ok_or(CheckpointError::LengthOverflow)?;
    let total_bytes = CHECKPOINT_HEADER_BYTES
        .checked_add(entries_bytes)
        .ok_or(CheckpointError::LengthOverflow)?;
    enforce_usize(
        "checkpoint manifest bytes",
        total_bytes,
        limits.max_manifest_bytes,
    )?;
    let mut output = vec![0_u8; total_bytes];
    let (header, entries) = output
        .split_first_chunk_mut::<CHECKPOINT_HEADER_BYTES>()
        .ok_or(CheckpointError::LengthOverflow)?;
    header.put::<0, 8>(*CHECKPOINT_MAGIC);
    header.put_u16::<8>(CHECKPOINT_VERSION);
    header.put_u16::<10>(CHECKPOINT_HEADER_BYTES as u16);
    header.put_u64::<16>(usize_to_u64(total_bytes)?);
    header.put::<24, 16>(*manifest.group_id.as_bytes());
    header.put::<40, 16>(*manifest.store_id.as_bytes());
    header.put::<56, 16>(*manifest.checkpoint_id.as_bytes());
    header.put::<72, 40>(position_bytes(manifest.position));
    header.put_u64::<112>(manifest.configuration_epoch);
    header.put_u64::<120>(manifest.source_manifest_generation);
    header.put::<128, 32>(*manifest.source_manifest_digest.as_bytes());
    header.put::<160, 32>(*manifest.state_schema_digest.as_bytes());
    header.put_u64::<192>(manifest.state_bytes);
    header.put_u32::<200>(manifest.chunk_bytes);
    header.put_u32::<204>(CHECKPOINT_CHUNK_ENTRY_BYTES as u32);
    header.put_u32::<208>(
        u32::try_from(manifest.chunks.len()).map_err(|_| CheckpointError::LengthOverflow)?,
    );
    header.put_u64::<216>(usize_to_u64(entries_bytes)?);
    header.put::<224, 32>(*manifest.state_digest.as_bytes());
    let targets = entries.as_chunks_mut::<CHECKPOINT_CHUNK_ENTRY_BYTES>().0;
    for (chunk, target) in manifest.chunks.iter().zip(targets) {
        encode_chunk(*chunk, target);
    }
    let digest = manifest_digest(&output);
    layout::patch(&mut output, CHECKPOINT_DIGEST_START, *digest.as_bytes())?;
    Ok(output)
}

/// Validate checkpoint framing, fields, and bounds without reading its chunks.
pub fn decode_checkpoint_manifest(
    input: &[u8],
    limits: CheckpointLimits,
) -> Result<CheckpointManifest, CheckpointError> {
    validate_limits(limits)?;
    enforce_usize(
        "checkpoint manifest bytes",
        input.len(),
        limits.max_manifest_bytes,
    )?;
    let (header, entries) = input
        .split_first_chunk::<CHECKPOINT_HEADER_BYTES>()
        .ok_or(CheckpointError::Truncated)?;
    check_header_fields(header)?;
    let chunk_count = header.u32_at::<208>() as usize;
    enforce_usize("checkpoint chunk count", chunk_count, limits.max_chunks)?;
    let entries_bytes = chunk_count
        .checked_mul(CHECKPOINT_CHUNK_ENTRY_BYTES)
        .ok_or(CheckpointError::LengthOverflow)?;
    let total_bytes = CHECKPOINT_HEADER_BYTES
        .checked_add(entries_bytes)
        .ok_or(CheckpointError::LengthOverflow)?;
    if header.u64_at::<16>() != usize_to_u64(total_bytes)?
        || header.u64_at::<216>() != usize_to_u64(entries_bytes)?
        || input.len() != total_bytes
    {
        return Err(CheckpointError::LengthMismatch);
    }
    if manifest_digest(input).as_bytes() != header.slice::<CHECKPOINT_DIGEST_START, 32>() {
        return Err(CheckpointError::DigestMismatch("checkpoint manifest"));
    }
    let chunks = entries
        .as_chunks::<CHECKPOINT_CHUNK_ENTRY_BYTES>()
        .0
        .iter()
        .map(decode_chunk)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = decode_header(header, chunks);
    validate_manifest(&manifest, limits)?;
    Ok(manifest)
}

/// Magic, version, fixed sizes, and zeroed reserved fields.
fn check_header_fields(header: &[u8; CHECKPOINT_HEADER_BYTES]) -> Result<(), CheckpointError> {
    if header.array::<0, 8>() != *CHECKPOINT_MAGIC {
        return Err(CheckpointError::WrongMagic);
    }
    let version = header.u16_at::<8>();
    if version != CHECKPOINT_VERSION {
        return Err(CheckpointError::UnsupportedVersion(version));
    }
    let reserved_zero = header.u32_at::<12>() == 0
        && header.u32_at::<212>() == 0
        && header
            .slice::<288, { CHECKPOINT_HEADER_BYTES - 288 }>()
            .iter()
            .all(|byte| *byte == 0);
    if header.u16_at::<10>() != CHECKPOINT_HEADER_BYTES as u16
        || header.u32_at::<204>() != CHECKPOINT_CHUNK_ENTRY_BYTES as u32
        || !reserved_zero
    {
        return Err(CheckpointError::UnsupportedFields);
    }
    Ok(())
}

/// The manifest fields of a framed header; validation stays with the caller.
fn decode_header(
    header: &[u8; CHECKPOINT_HEADER_BYTES],
    chunks: Vec<CheckpointChunk>,
) -> CheckpointManifest {
    CheckpointManifest {
        group_id: GroupId::from_bytes(header.array::<24, 16>()),
        store_id: StoreId::from_bytes(header.array::<40, 16>()),
        checkpoint_id: CheckpointId::from_bytes(header.array::<56, 16>()),
        position: read_position(header.array::<72, 40>()),
        configuration_epoch: header.u64_at::<112>(),
        source_manifest_generation: header.u64_at::<120>(),
        source_manifest_digest: Digest::from_bytes(header.array::<128, 32>()),
        state_schema_digest: Digest::from_bytes(header.array::<160, 32>()),
        state_bytes: header.u64_at::<192>(),
        chunk_bytes: header.u32_at::<200>(),
        state_digest: Digest::from_bytes(header.array::<224, 32>()),
        chunks,
    }
}

/// Validate encoded checkpoint metadata and compute its integrity digest.
pub fn checkpoint_manifest_digest(
    input: &[u8],
    limits: CheckpointLimits,
) -> Result<Digest, CheckpointError> {
    decode_checkpoint_manifest(input, limits)?;
    Ok(Digest::from_bytes(*layout::record(
        input,
        CHECKPOINT_DIGEST_START,
    )?))
}

/// Canonical hexadecimal directory name for an exact checkpoint identity.
pub fn checkpoint_name(checkpoint_id: CheckpointId) -> String {
    crate::directory::hex(checkpoint_id.as_bytes())
}

fn validate_spec(spec: CheckpointSpec, limits: CheckpointLimits) -> Result<(), CheckpointError> {
    validate_limits(limits)?;
    require_nonzero("group", spec.group_id.as_bytes())?;
    require_nonzero("store", spec.store_id.as_bytes())?;
    require_nonzero("checkpoint", spec.checkpoint_id.as_bytes())?;
    spec.position
        .validate()
        .map_err(|_| CheckpointError::InvalidPosition)?;
    if spec.position.op_number == 0
        || spec.configuration_epoch == 0
        || spec.source_manifest_generation == 0
        || spec.source_manifest_digest == Digest::ZERO
        || spec.state_schema_digest == Digest::ZERO
    {
        return Err(CheckpointError::InvalidSpec);
    }
    if spec.chunk_bytes == 0 || spec.chunk_bytes > limits.max_chunk_bytes {
        return Err(CheckpointError::InvalidChunkBytes);
    }
    Ok(())
}

fn validate_manifest(
    manifest: &CheckpointManifest,
    limits: CheckpointLimits,
) -> Result<(), CheckpointError> {
    validate_spec(
        CheckpointSpec {
            group_id: manifest.group_id,
            store_id: manifest.store_id,
            checkpoint_id: manifest.checkpoint_id,
            position: manifest.position,
            configuration_epoch: manifest.configuration_epoch,
            source_manifest_generation: manifest.source_manifest_generation,
            source_manifest_digest: manifest.source_manifest_digest,
            state_schema_digest: manifest.state_schema_digest,
            chunk_bytes: manifest.chunk_bytes as usize,
        },
        limits,
    )?;
    enforce_u64(
        "checkpoint state bytes",
        manifest.state_bytes,
        limits.max_state_bytes,
    )?;
    enforce_usize(
        "checkpoint chunk count",
        manifest.chunks.len(),
        limits.max_chunks,
    )?;
    if manifest.state_bytes == 0
        || manifest.state_digest == Digest::ZERO
        || manifest.chunks.is_empty()
    {
        return Err(CheckpointError::EmptyState);
    }
    let mut next_offset = 0_u64;
    for (expected_ordinal, chunk) in manifest.chunks.iter().enumerate() {
        if chunk.ordinal as usize != expected_ordinal
            || chunk.logical_offset != next_offset
            || chunk.bytes == 0
            || chunk.bytes > u64::from(manifest.chunk_bytes)
            || chunk.digest == Digest::ZERO
        {
            return Err(CheckpointError::InvalidChunk);
        }
        if expected_ordinal + 1 < manifest.chunks.len()
            && chunk.bytes != u64::from(manifest.chunk_bytes)
        {
            return Err(CheckpointError::InvalidChunk);
        }
        next_offset = next_offset
            .checked_add(chunk.bytes)
            .ok_or(CheckpointError::LengthOverflow)?;
    }
    if next_offset != manifest.state_bytes {
        return Err(CheckpointError::LengthMismatch);
    }
    Ok(())
}

fn encode_chunk(chunk: CheckpointChunk, output: &mut [u8; CHECKPOINT_CHUNK_ENTRY_BYTES]) {
    output.put_u32::<0>(chunk.ordinal);
    output.put_u64::<8>(chunk.logical_offset);
    output.put_u64::<16>(chunk.bytes);
    output.put::<24, 32>(*chunk.digest.as_bytes());
}

fn decode_chunk(
    input: &[u8; CHECKPOINT_CHUNK_ENTRY_BYTES],
) -> Result<CheckpointChunk, CheckpointError> {
    if input.u32_at::<4>() != 0 || input.u64_at::<56>() != 0 {
        return Err(CheckpointError::UnsupportedFields);
    }
    Ok(CheckpointChunk {
        ordinal: input.u32_at::<0>(),
        logical_offset: input.u64_at::<8>(),
        bytes: input.u64_at::<16>(),
        digest: Digest::from_bytes(input.array::<24, 32>()),
    })
}

fn manifest_digest(input: &[u8]) -> Digest {
    hash_without_digest(
        CHECKPOINT_MANIFEST_HASH_CONTEXT,
        input,
        CHECKPOINT_DIGEST_START,
    )
}

pub(crate) fn chunk_name(ordinal: u32) -> String {
    format!("CHUNK.{ordinal:08x}")
}

fn parse_chunk_name(name: &std::ffi::OsStr) -> Option<u32> {
    let name = name.to_str()?;
    let suffix = name.strip_prefix("CHUNK.")?;
    if suffix.len() != 8 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    u32::from_str_radix(suffix, 16).ok()
}

fn require_nonzero(kind: &'static str, bytes: &[u8]) -> Result<(), CheckpointError> {
    if bytes.iter().all(|byte| *byte == 0) {
        Err(CheckpointError::ZeroIdentity(kind))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_limits(limits: CheckpointLimits) -> Result<(), CheckpointError> {
    if limits.max_manifest_bytes < CHECKPOINT_HEADER_BYTES
        || limits.max_chunks == 0
        || limits.max_chunk_bytes == 0
        || limits.max_state_bytes == 0
    {
        Err(CheckpointError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn enforce_usize(kind: &'static str, actual: usize, limit: usize) -> Result<(), CheckpointError> {
    if actual > limit {
        Err(CheckpointError::LimitExceeded {
            kind,
            actual: actual as u64,
            limit: limit as u64,
        })
    } else {
        Ok(())
    }
}

pub(crate) fn enforce_u64(
    kind: &'static str,
    actual: u64,
    limit: u64,
) -> Result<(), CheckpointError> {
    if actual > limit {
        Err(CheckpointError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn usize_to_u64(value: usize) -> Result<u64, CheckpointError> {
    u64::try_from(value).map_err(|_| CheckpointError::LengthOverflow)
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

impl From<LayoutError> for CheckpointError {
    fn from(error: LayoutError) -> Self {
        match error {
            LayoutError::Truncated { .. } => Self::Truncated,
            LayoutError::Trailing(_) => Self::LengthMismatch,
            LayoutError::Overflow { .. } => Self::LengthOverflow,
        }
    }
}

/// Checkpoint format, publication, or content failure.
#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error("wrong checkpoint manifest magic")]
    /// The named object has an incorrect format magic.
    WrongMagic,
    #[error("unsupported checkpoint version {0}")]
    /// The encoded format version is unsupported.
    UnsupportedVersion(u16),
    #[error("unsupported checkpoint flags, widths, or reserved fields")]
    /// Unsupported checkpoint flags, widths, or reserved fields.
    UnsupportedFields,
    #[error("checkpoint metadata is truncated")]
    /// Input bytes end before a complete encoded object.
    Truncated,
    #[error("checkpoint lengths do not agree")]
    /// Checkpoint lengths do not agree.
    LengthMismatch,
    #[error("{0} identity is zero")]
    /// A required persistent identity is zero.
    ZeroIdentity(&'static str),
    #[error("checkpoint operation position is invalid")]
    /// Checkpoint operation position is invalid.
    InvalidPosition,
    #[error("checkpoint specification is invalid")]
    /// Checkpoint specification is invalid.
    InvalidSpec,
    #[error("checkpoint state is empty")]
    /// Checkpoint state is empty.
    EmptyState,
    #[error("checkpoint chunk size is invalid")]
    /// Checkpoint chunk size is invalid.
    InvalidChunkBytes,
    #[error("checkpoint resource limits are invalid")]
    /// Checkpoint resource limits are invalid.
    InvalidLimits,
    #[error("checkpoint chunk sequence or fields are invalid")]
    /// Checkpoint chunk sequence or fields are invalid.
    InvalidChunk,
    #[error("{0} digest mismatch")]
    /// The named object does not match its expected integrity digest.
    DigestMismatch(&'static str),
    #[error("checkpoint belongs to another group or store")]
    /// Checkpoint belongs to another group or store.
    IdentityMismatch,
    #[error("checkpoint is missing a required chunk")]
    /// Checkpoint is missing a required chunk.
    MissingChunk,
    #[error("checkpoint directory contains an unexpected file")]
    /// Checkpoint directory contains an unexpected file.
    UnexpectedFile,
    #[error("checkpoint staging namespace is exhausted")]
    /// Checkpoint staging namespace is exhausted.
    StagingConflict,
    #[error("immutable checkpoint path contains different content")]
    /// Immutable checkpoint path contains different content.
    ImmutableConflict,
    #[error("{0} is not a regular file")]
    /// The named artifact is not a regular file.
    NotRegularFile(&'static str),
    #[error("{0} is not a directory")]
    /// The named artifact is not a directory.
    NotDirectory(&'static str),
    #[error("checkpoint integer or length arithmetic overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    /// The named resource exceeds its configured bound.
    LimitExceeded {
        /// Resource bound that rejected the operation.
        kind: &'static str,
        /// Observed size, count, or fenced field value.
        actual: u64,
        /// Configured maximum for the reported resource.
        limit: u64,
    },
}

fn validate_checkpoint_names(
    names: impl Iterator<Item = io::Result<std::ffi::OsString>>,
    manifest: &CheckpointManifest,
) -> Result<(), CheckpointError> {
    let mut seen = vec![false; manifest.chunks.len()];
    let mut entries = 0_usize;
    for name in names {
        let file_name = name?;
        entries = entries
            .checked_add(1)
            .ok_or(CheckpointError::LengthOverflow)?;
        if file_name == CHECKPOINT_MANIFEST_FILE {
            continue;
        }
        let Some(ordinal) = parse_chunk_name(&file_name) else {
            return Err(CheckpointError::UnexpectedFile);
        };
        let index = usize::try_from(ordinal).map_err(|_| CheckpointError::LengthOverflow)?;
        // One flag per manifest chunk; each chunk file may appear once.
        let seen = seen.get_mut(index).ok_or(CheckpointError::UnexpectedFile)?;
        if std::mem::replace(seen, true) {
            return Err(CheckpointError::UnexpectedFile);
        }
    }
    if entries != manifest.chunks.len() + 1 || seen.iter().any(|value| !value) {
        return Err(CheckpointError::MissingChunk);
    }
    Ok(())
}
