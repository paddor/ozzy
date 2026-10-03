//! Immutable, chunked checkpoint artifacts.

pub(crate) mod asynchronous;

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_proto::{CheckpointId, GroupId, StoreId};
use thiserror::Error;

use crate::retention::CheckpointLease;
use crate::{Digest, LogPosition};

pub const CHECKPOINT_HEADER_BYTES: usize = 4096;
pub const CHECKPOINT_CHUNK_ENTRY_BYTES: usize = 64;

const CHECKPOINT_MAGIC: &[u8; 8] = b"OZYCHK01";
const CHECKPOINT_VERSION: u16 = 2;
const CHECKPOINT_MANIFEST_FILE: &str = "manifest";
const CHECKPOINT_MANIFEST_HASH_CONTEXT: &str = "ozzy journal checkpoint manifest v1";
const CHECKPOINT_CHUNK_HASH_CONTEXT: &str = "ozzy journal checkpoint chunk v1";
const CHECKPOINT_STATE_HASH_CONTEXT: &str = "ozzy journal checkpoint state v1";
const CHECKPOINT_DIGEST_START: usize = 256;
const CHECKPOINT_DIGEST_END: usize = 288;

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Bounds for checkpoint metadata, chunks, and optional materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointLimits {
    pub max_manifest_bytes: usize,
    pub max_chunks: usize,
    pub max_chunk_bytes: usize,
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
    pub group_id: GroupId,
    pub store_id: StoreId,
    pub checkpoint_id: CheckpointId,
    pub position: LogPosition,
    pub configuration_epoch: u64,
    pub source_manifest_generation: u64,
    pub source_manifest_digest: Digest,
    /// Digest identifying the canonical state schema, not the state bytes.
    pub state_schema_digest: Digest,
    /// Physical chunk target. This is recorded in the immutable manifest.
    pub chunk_bytes: usize,
}

/// Frozen background-build target captured from one durable manifest.
#[derive(Debug)]
pub struct CheckpointPlan {
    checkpoints: PathBuf,
    staging: PathBuf,
    spec: CheckpointSpec,
    lease: CheckpointLease,
}

impl CheckpointPlan {
    pub(crate) fn new(
        checkpoints: PathBuf,
        staging: PathBuf,
        spec: CheckpointSpec,
        lease: CheckpointLease,
    ) -> Self {
        Self {
            checkpoints,
            staging,
            spec,
            lease,
        }
    }

    pub const fn spec(&self) -> CheckpointSpec {
        self.spec
    }

    /// Perform blocking chunk writes and durable publication off the owner path.
    pub fn build(
        self,
        state: &[u8],
        limits: CheckpointLimits,
    ) -> Result<CheckpointImage, CheckpointError> {
        let Self {
            checkpoints,
            staging,
            spec,
            lease,
        } = self;
        let mut image = build_checkpoint(checkpoints, staging, spec, state, limits)?;
        image.attach_lease(lease);
        Ok(image)
    }
}

/// One exact state chunk named by ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointChunk {
    pub ordinal: u32,
    pub logical_offset: u64,
    pub bytes: u64,
    pub digest: Digest,
}

/// Complete immutable checkpoint manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointManifest {
    pub group_id: GroupId,
    pub store_id: StoreId,
    pub checkpoint_id: CheckpointId,
    pub position: LogPosition,
    pub configuration_epoch: u64,
    pub source_manifest_generation: u64,
    pub source_manifest_digest: Digest,
    pub state_schema_digest: Digest,
    pub state_bytes: u64,
    pub chunk_bytes: u32,
    pub state_digest: Digest,
    pub chunks: Vec<CheckpointChunk>,
}

/// Fully validated checkpoint directory. State remains chunked on disk.
#[derive(Debug)]
pub struct CheckpointImage {
    root: PathBuf,
    manifest: CheckpointManifest,
    manifest_digest: Digest,
    lease: Option<CheckpointLease>,
}

impl PartialEq for CheckpointImage {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
            && self.manifest == other.manifest
            && self.manifest_digest == other.manifest_digest
    }
}

impl Eq for CheckpointImage {}

impl CheckpointImage {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn manifest(&self) -> &CheckpointManifest {
        &self.manifest
    }

    pub const fn manifest_digest(&self) -> Digest {
        self.manifest_digest
    }

    pub(crate) fn attach_lease(&mut self, lease: CheckpointLease) {
        debug_assert!(self.lease.is_none());
        self.lease = Some(lease);
    }

    /// Materialize the canonical state under the caller's existing limits.
    pub fn read_state(&self, limits: CheckpointLimits) -> Result<Vec<u8>, CheckpointError> {
        validate_limits(limits)?;
        enforce_u64(
            "checkpoint state bytes",
            self.manifest.state_bytes,
            limits.max_state_bytes,
        )?;
        let capacity = usize::try_from(self.manifest.state_bytes)
            .map_err(|_| CheckpointError::LengthOverflow)?;
        let mut state = Vec::with_capacity(capacity);
        for chunk in &self.manifest.chunks {
            let bytes =
                read_exact_chunk(&self.root.join(chunk_name(chunk.ordinal)), *chunk, limits)?;
            state.extend_from_slice(&bytes);
        }
        if state_digest(&state) != self.manifest.state_digest {
            return Err(CheckpointError::DigestMismatch("checkpoint state"));
        }
        Ok(state)
    }
}

/// Build, synchronize, and publish one immutable checkpoint directory.
///
/// This is blocking maintenance work. `checkpoints` and `staging` must already
/// exist on the same group volume. Publication never mutates a prior artifact.
pub fn build_checkpoint(
    checkpoints: impl AsRef<Path>,
    staging: impl AsRef<Path>,
    spec: CheckpointSpec,
    state: &[u8],
    limits: CheckpointLimits,
) -> Result<CheckpointImage, CheckpointError> {
    let checkpoints = checkpoints.as_ref();
    let staging = staging.as_ref();
    require_directory(checkpoints, "checkpoints")?;
    require_directory(staging, "staging")?;
    let manifest = manifest_for_state(spec, state, limits)?;
    let manifest_bytes = encode_checkpoint_manifest(&manifest, limits)?;
    let manifest_digest = checkpoint_manifest_digest(&manifest_bytes, limits)?;
    let final_root = checkpoints.join(checkpoint_name(spec.checkpoint_id));
    match fs::symlink_metadata(&final_root) {
        Ok(_) => {
            let existing = open_checkpoint(&final_root, spec.group_id, spec.store_id, limits)?;
            if existing.manifest != manifest || existing.manifest_digest != manifest_digest {
                return Err(CheckpointError::ImmutableConflict);
            }
            sync_directory(checkpoints)?;
            sync_directory(staging)?;
            return Ok(existing);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let temporary = create_staging_directory(staging, spec.checkpoint_id)?;

    for chunk in &manifest.chunks {
        let start =
            usize::try_from(chunk.logical_offset).map_err(|_| CheckpointError::LengthOverflow)?;
        let length = usize::try_from(chunk.bytes).map_err(|_| CheckpointError::LengthOverflow)?;
        let end = start
            .checked_add(length)
            .ok_or(CheckpointError::LengthOverflow)?;
        write_new_synced(
            &temporary.join(chunk_name(chunk.ordinal)),
            &state[start..end],
        )?;
    }
    write_new_synced(&temporary.join(CHECKPOINT_MANIFEST_FILE), &manifest_bytes)?;
    sync_directory(&temporary)?;

    match fs::rename(&temporary, &final_root) {
        Ok(()) => {}
        Err(rename_error) => {
            if path_exists(&final_root)? {
                let existing = open_checkpoint(&final_root, spec.group_id, spec.store_id, limits)?;
                if existing.manifest != manifest || existing.manifest_digest != manifest_digest {
                    return Err(CheckpointError::ImmutableConflict);
                }
                remove_owned_staging(&temporary)?;
            } else {
                return Err(rename_error.into());
            }
        }
    }
    sync_directory(checkpoints)?;
    sync_directory(staging)?;
    open_checkpoint(final_root, spec.group_id, spec.store_id, limits)
}

fn create_staging_directory(
    staging: &Path,
    checkpoint_id: CheckpointId,
) -> Result<PathBuf, CheckpointError> {
    let checkpoint = checkpoint_name(checkpoint_id);
    for _ in 0..1024 {
        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = staging.join(format!(
            ".checkpoint-{checkpoint}-{}-{sequence}.tmp",
            std::process::id()
        ));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(CheckpointError::StagingConflict)
}

/// Validate one checkpoint without materializing its complete state image.
pub fn open_checkpoint(
    root: impl AsRef<Path>,
    expected_group_id: GroupId,
    expected_store_id: StoreId,
    limits: CheckpointLimits,
) -> Result<CheckpointImage, CheckpointError> {
    let root = root.as_ref();
    let (manifest, manifest_digest) =
        read_checkpoint_manifest(root, expected_group_id, expected_store_id, limits)?;
    validate_checkpoint_files(root, &manifest, limits)?;
    Ok(CheckpointImage {
        root: root.to_path_buf(),
        manifest,
        manifest_digest,
        lease: None,
    })
}

/// Read authenticated metadata only. This does not validate the state chunks.
pub(crate) fn read_checkpoint_manifest(
    root: &Path,
    expected_group_id: GroupId,
    expected_store_id: StoreId,
    limits: CheckpointLimits,
) -> Result<(CheckpointManifest, Digest), CheckpointError> {
    validate_limits(limits)?;
    require_directory(root, "checkpoint root")?;
    let bytes = read_limited_file(
        &root.join(CHECKPOINT_MANIFEST_FILE),
        limits.max_manifest_bytes,
    )?;
    let digest = checkpoint_manifest_digest(&bytes, limits)?;
    let manifest = decode_checkpoint_manifest(&bytes, limits)?;
    if manifest.group_id != expected_group_id || manifest.store_id != expected_store_id {
        return Err(CheckpointError::IdentityMismatch);
    }
    Ok((manifest, digest))
}

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
    output[..8].copy_from_slice(CHECKPOINT_MAGIC);
    put_u16(&mut output, 8, CHECKPOINT_VERSION);
    put_u16(&mut output, 10, CHECKPOINT_HEADER_BYTES as u16);
    put_u64(&mut output, 16, usize_to_u64(total_bytes)?);
    output[24..40].copy_from_slice(manifest.group_id.as_bytes());
    output[40..56].copy_from_slice(manifest.store_id.as_bytes());
    output[56..72].copy_from_slice(manifest.checkpoint_id.as_bytes());
    put_position(&mut output, 72, manifest.position);
    put_u64(&mut output, 112, manifest.configuration_epoch);
    put_u64(&mut output, 120, manifest.source_manifest_generation);
    output[128..160].copy_from_slice(manifest.source_manifest_digest.as_bytes());
    output[160..192].copy_from_slice(manifest.state_schema_digest.as_bytes());
    put_u64(&mut output, 192, manifest.state_bytes);
    put_u32(&mut output, 200, manifest.chunk_bytes);
    put_u32(&mut output, 204, CHECKPOINT_CHUNK_ENTRY_BYTES as u32);
    put_u32(
        &mut output,
        208,
        u32::try_from(manifest.chunks.len()).map_err(|_| CheckpointError::LengthOverflow)?,
    );
    put_u64(&mut output, 216, usize_to_u64(entries_bytes)?);
    output[224..256].copy_from_slice(manifest.state_digest.as_bytes());
    for (chunk, target) in manifest.chunks.iter().zip(
        output[CHECKPOINT_HEADER_BYTES..]
            .as_chunks_mut::<CHECKPOINT_CHUNK_ENTRY_BYTES>()
            .0,
    ) {
        encode_chunk(*chunk, target);
    }
    let digest = manifest_digest(&output);
    output[CHECKPOINT_DIGEST_START..CHECKPOINT_DIGEST_END].copy_from_slice(digest.as_bytes());
    Ok(output)
}

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
    if input.len() < CHECKPOINT_HEADER_BYTES {
        return Err(CheckpointError::Truncated);
    }
    if &input[..8] != CHECKPOINT_MAGIC {
        return Err(CheckpointError::WrongMagic);
    }
    let version = read_u16(input, 8);
    if version != CHECKPOINT_VERSION {
        return Err(CheckpointError::UnsupportedVersion(version));
    }
    if read_u16(input, 10) != CHECKPOINT_HEADER_BYTES as u16
        || read_u32(input, 12) != 0
        || read_u32(input, 204) != CHECKPOINT_CHUNK_ENTRY_BYTES as u32
        || read_u32(input, 212) != 0
        || input[288..CHECKPOINT_HEADER_BYTES]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(CheckpointError::UnsupportedFields);
    }
    let chunk_count = read_u32(input, 208) as usize;
    enforce_usize("checkpoint chunk count", chunk_count, limits.max_chunks)?;
    let entries_bytes = chunk_count
        .checked_mul(CHECKPOINT_CHUNK_ENTRY_BYTES)
        .ok_or(CheckpointError::LengthOverflow)?;
    let total_bytes = CHECKPOINT_HEADER_BYTES
        .checked_add(entries_bytes)
        .ok_or(CheckpointError::LengthOverflow)?;
    if read_u64(input, 16) != usize_to_u64(total_bytes)?
        || read_u64(input, 216) != usize_to_u64(entries_bytes)?
        || input.len() != total_bytes
    {
        return Err(CheckpointError::LengthMismatch);
    }
    if manifest_digest(input).as_bytes() != &input[CHECKPOINT_DIGEST_START..CHECKPOINT_DIGEST_END] {
        return Err(CheckpointError::DigestMismatch("checkpoint manifest"));
    }
    let chunks = input[CHECKPOINT_HEADER_BYTES..]
        .as_chunks::<CHECKPOINT_CHUNK_ENTRY_BYTES>()
        .0
        .iter()
        .map(decode_chunk)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = CheckpointManifest {
        group_id: GroupId::from_bytes(array_16(input, 24)),
        store_id: StoreId::from_bytes(array_16(input, 40)),
        checkpoint_id: CheckpointId::from_bytes(array_16(input, 56)),
        position: read_position(input, 72),
        configuration_epoch: read_u64(input, 112),
        source_manifest_generation: read_u64(input, 120),
        source_manifest_digest: digest_at(input, 128),
        state_schema_digest: digest_at(input, 160),
        state_bytes: read_u64(input, 192),
        chunk_bytes: read_u32(input, 200),
        state_digest: digest_at(input, 224),
        chunks,
    };
    validate_manifest(&manifest, limits)?;
    Ok(manifest)
}

pub fn checkpoint_manifest_digest(
    input: &[u8],
    limits: CheckpointLimits,
) -> Result<Digest, CheckpointError> {
    decode_checkpoint_manifest(input, limits)?;
    Ok(digest_at(input, CHECKPOINT_DIGEST_START))
}

pub fn checkpoint_name(checkpoint_id: CheckpointId) -> String {
    id_name(checkpoint_id.as_bytes())
}

fn manifest_for_state(
    spec: CheckpointSpec,
    state: &[u8],
    limits: CheckpointLimits,
) -> Result<CheckpointManifest, CheckpointError> {
    validate_spec(spec, limits)?;
    let state_bytes = usize_to_u64(state.len())?;
    enforce_u64(
        "checkpoint state bytes",
        state_bytes,
        limits.max_state_bytes,
    )?;
    if state.is_empty() {
        return Err(CheckpointError::EmptyState);
    }
    let count = state.len().div_ceil(spec.chunk_bytes);
    enforce_usize("checkpoint chunk count", count, limits.max_chunks)?;
    let manifest_bytes = count
        .checked_mul(CHECKPOINT_CHUNK_ENTRY_BYTES)
        .and_then(|bytes| bytes.checked_add(CHECKPOINT_HEADER_BYTES))
        .ok_or(CheckpointError::LengthOverflow)?;
    enforce_usize(
        "checkpoint manifest bytes",
        manifest_bytes,
        limits.max_manifest_bytes,
    )?;
    let chunks = state
        .chunks(spec.chunk_bytes)
        .enumerate()
        .map(|(ordinal, bytes)| {
            let ordinal = u32::try_from(ordinal).map_err(|_| CheckpointError::LengthOverflow)?;
            let logical_offset = u64::from(ordinal)
                .checked_mul(usize_to_u64(spec.chunk_bytes)?)
                .ok_or(CheckpointError::LengthOverflow)?;
            Ok(CheckpointChunk {
                ordinal,
                logical_offset,
                bytes: usize_to_u64(bytes.len())?,
                digest: chunk_digest(bytes),
            })
        })
        .collect::<Result<Vec<_>, CheckpointError>>()?;
    let manifest = CheckpointManifest {
        group_id: spec.group_id,
        store_id: spec.store_id,
        checkpoint_id: spec.checkpoint_id,
        position: spec.position,
        configuration_epoch: spec.configuration_epoch,
        source_manifest_generation: spec.source_manifest_generation,
        source_manifest_digest: spec.source_manifest_digest,
        state_schema_digest: spec.state_schema_digest,
        state_bytes,
        chunk_bytes: u32::try_from(spec.chunk_bytes)
            .map_err(|_| CheckpointError::LengthOverflow)?,
        state_digest: state_digest(state),
        chunks,
    };
    validate_manifest(&manifest, limits)?;
    Ok(manifest)
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

fn validate_checkpoint_files(
    root: &Path,
    manifest: &CheckpointManifest,
    limits: CheckpointLimits,
) -> Result<(), CheckpointError> {
    validate_checkpoint_names(
        fs::read_dir(root)?.map(|entry| entry.map(|entry| entry.file_name())),
        manifest,
    )?;
    let mut state_hasher = Hasher::new(CHECKPOINT_STATE_HASH_CONTEXT);
    for chunk in &manifest.chunks {
        validate_chunk_file(
            &root.join(chunk_name(chunk.ordinal)),
            *chunk,
            limits,
            &mut state_hasher,
        )?;
    }
    if state_hasher.finish() != manifest.state_digest {
        return Err(CheckpointError::DigestMismatch("checkpoint state"));
    }
    Ok(())
}

fn validate_chunk_file(
    path: &Path,
    chunk: CheckpointChunk,
    limits: CheckpointLimits,
    state_hasher: &mut Hasher,
) -> Result<(), CheckpointError> {
    require_regular_file(path, "checkpoint chunk")?;
    enforce_u64(
        "checkpoint chunk bytes",
        chunk.bytes,
        limits.max_chunk_bytes as u64,
    )?;
    let mut file = File::open(path)?;
    if file.metadata()?.len() != chunk.bytes {
        return Err(CheckpointError::LengthMismatch);
    }
    let mut hasher = Hasher::new(CHECKPOINT_CHUNK_HASH_CONTEXT);
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut remaining = chunk.bytes;
    while remaining != 0 {
        let wanted = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| CheckpointError::LengthOverflow)?;
        file.read_exact(&mut buffer[..wanted])?;
        hasher.update(&buffer[..wanted]);
        state_hasher.update(&buffer[..wanted]);
        remaining -= wanted as u64;
    }
    if hasher.finish() != chunk.digest {
        return Err(CheckpointError::DigestMismatch("checkpoint chunk"));
    }
    Ok(())
}

fn read_exact_chunk(
    path: &Path,
    chunk: CheckpointChunk,
    limits: CheckpointLimits,
) -> Result<Vec<u8>, CheckpointError> {
    require_regular_file(path, "checkpoint chunk")?;
    enforce_u64(
        "checkpoint chunk bytes",
        chunk.bytes,
        limits.max_chunk_bytes as u64,
    )?;
    let length = usize::try_from(chunk.bytes).map_err(|_| CheckpointError::LengthOverflow)?;
    let mut bytes = vec![0_u8; length];
    let mut file = File::open(path)?;
    if file.metadata()?.len() != chunk.bytes {
        return Err(CheckpointError::LengthMismatch);
    }
    file.read_exact(&mut bytes)?;
    if chunk_digest(&bytes) != chunk.digest {
        return Err(CheckpointError::DigestMismatch("checkpoint chunk"));
    }
    Ok(bytes)
}

fn encode_chunk(chunk: CheckpointChunk, output: &mut [u8; CHECKPOINT_CHUNK_ENTRY_BYTES]) {
    put_u32(output, 0, chunk.ordinal);
    put_u64(output, 8, chunk.logical_offset);
    put_u64(output, 16, chunk.bytes);
    output[24..56].copy_from_slice(chunk.digest.as_bytes());
}

fn decode_chunk(
    input: &[u8; CHECKPOINT_CHUNK_ENTRY_BYTES],
) -> Result<CheckpointChunk, CheckpointError> {
    if read_u32(input, 4) != 0 || read_u64(input, 56) != 0 {
        return Err(CheckpointError::UnsupportedFields);
    }
    Ok(CheckpointChunk {
        ordinal: read_u32(input, 0),
        logical_offset: read_u64(input, 8),
        bytes: read_u64(input, 16),
        digest: digest_at(input, 24),
    })
}

fn manifest_digest(input: &[u8]) -> Digest {
    hash_zeroed(
        CHECKPOINT_MANIFEST_HASH_CONTEXT,
        input,
        CHECKPOINT_DIGEST_START,
        CHECKPOINT_DIGEST_END,
    )
}

fn chunk_digest(input: &[u8]) -> Digest {
    let mut hasher = Hasher::new(CHECKPOINT_CHUNK_HASH_CONTEXT);
    hasher.update(input);
    hasher.finish()
}

fn state_digest(input: &[u8]) -> Digest {
    let mut hasher = Hasher::new(CHECKPOINT_STATE_HASH_CONTEXT);
    hasher.update(input);
    hasher.finish()
}

fn hash_zeroed(context: &str, input: &[u8], start: usize, end: usize) -> Digest {
    debug_assert_eq!(end - start, 32);
    let mut hasher = Hasher::new(context);
    hasher.update(&input[..start]);
    hasher.update(&[0_u8; 32]);
    hasher.update(&input[end..]);
    hasher.finish()
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), CheckpointError> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn read_limited_file(path: &Path, limit: usize) -> Result<Vec<u8>, CheckpointError> {
    require_regular_file(path, "checkpoint manifest")?;
    let file = File::open(path)?;
    let length =
        usize::try_from(file.metadata()?.len()).map_err(|_| CheckpointError::LengthOverflow)?;
    enforce_usize("checkpoint manifest bytes", length, limit)?;
    let mut bytes = Vec::with_capacity(length);
    file.take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    enforce_usize("checkpoint manifest bytes", bytes.len(), limit)?;
    Ok(bytes)
}

fn remove_owned_staging(path: &Path) -> Result<(), CheckpointError> {
    require_directory(path, "checkpoint staging")?;
    fs::remove_dir_all(path)?;
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool, CheckpointError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn require_regular_file(path: &Path, object: &'static str) -> Result<(), CheckpointError> {
    if fs::symlink_metadata(path)?.file_type().is_file() {
        Ok(())
    } else {
        Err(CheckpointError::NotRegularFile(object))
    }
}

fn require_directory(path: &Path, object: &'static str) -> Result<(), CheckpointError> {
    if fs::symlink_metadata(path)?.file_type().is_dir() {
        Ok(())
    } else {
        Err(CheckpointError::NotDirectory(object))
    }
}

fn sync_directory(path: &Path) -> Result<(), CheckpointError> {
    File::open(path)?.sync_all()?;
    Ok(())
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

fn id_name(bytes: &[u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing into String is infallible");
    }
    output
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

fn enforce_u64(kind: &'static str, actual: u64, limit: u64) -> Result<(), CheckpointError> {
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

fn put_position(output: &mut [u8], offset: usize, position: LogPosition) {
    put_u64(output, offset, position.op_number);
    output[offset + 8..offset + 40].copy_from_slice(position.digest.as_bytes());
}

fn read_position(input: &[u8], offset: usize) -> LogPosition {
    LogPosition {
        op_number: read_u64(input, offset),
        digest: digest_at(input, offset + 8),
    }
}

fn array_16(input: &[u8], offset: usize) -> [u8; 16] {
    input[offset..offset + 16]
        .try_into()
        .expect("validated fixed-width field")
}

fn digest_at(input: &[u8], offset: usize) -> Digest {
    Digest::from_bytes(
        input[offset..offset + 32]
            .try_into()
            .expect("validated fixed-width digest"),
    )
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(
        input[offset..offset + 2]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        input[offset..offset + 4]
            .try_into()
            .expect("fixed-width field"),
    )
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("fixed-width field"),
    )
}

/// Checkpoint format, publication, or content failure.
#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("wrong checkpoint manifest magic")]
    WrongMagic,
    #[error("unsupported checkpoint version {0}")]
    UnsupportedVersion(u16),
    #[error("unsupported checkpoint flags, widths, or reserved fields")]
    UnsupportedFields,
    #[error("checkpoint metadata is truncated")]
    Truncated,
    #[error("checkpoint lengths do not agree")]
    LengthMismatch,
    #[error("{0} identity is zero")]
    ZeroIdentity(&'static str),
    #[error("checkpoint operation position is invalid")]
    InvalidPosition,
    #[error("checkpoint specification is invalid")]
    InvalidSpec,
    #[error("checkpoint state is empty")]
    EmptyState,
    #[error("checkpoint chunk size is invalid")]
    InvalidChunkBytes,
    #[error("checkpoint resource limits are invalid")]
    InvalidLimits,
    #[error("checkpoint chunk sequence or fields are invalid")]
    InvalidChunk,
    #[error("{0} digest mismatch")]
    DigestMismatch(&'static str),
    #[error("checkpoint belongs to another group or store")]
    IdentityMismatch,
    #[error("checkpoint is missing a required chunk")]
    MissingChunk,
    #[error("checkpoint directory contains an unexpected file")]
    UnexpectedFile,
    #[error("checkpoint staging namespace is exhausted")]
    StagingConflict,
    #[error("immutable checkpoint path contains different content")]
    ImmutableConflict,
    #[error("{0} is not a regular file")]
    NotRegularFile(&'static str),
    #[error("{0} is not a directory")]
    NotDirectory(&'static str),
    #[error("checkpoint integer or length arithmetic overflow")]
    LengthOverflow,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    LimitExceeded {
        kind: &'static str,
        actual: u64,
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
        manifest
            .chunks
            .get(index)
            .ok_or(CheckpointError::UnexpectedFile)?;
        if seen[index] {
            return Err(CheckpointError::UnexpectedFile);
        }
        seen[index] = true;
    }
    if entries != manifest.chunks.len() + 1 || seen.iter().any(|value| !value) {
        return Err(CheckpointError::MissingChunk);
    }
    Ok(())
}
