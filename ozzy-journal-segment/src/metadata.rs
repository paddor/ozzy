use std::ops::Range;

use ozzy_journal::integrity::IntegrityHasher as Hasher;
use ozzy_journal::operation::{ChainPosition, Digest};
use ozzy_proto::{CheckpointId, GroupId, NodeId, StoreId, VolumeId};
use thiserror::Error;

use crate::{SEGMENT_HEADER_BYTES, WRITE_GROUP_ALIGNMENT};

/// Exact encoded persistent group/store identity length.
pub const GROUP_IDENTITY_BYTES: usize = 4 * 1024;
/// Exact encoded manifest-selection reference length.
pub const CURRENT_BYTES: usize = 128;
/// Exact encoded selected-manifest header length.
pub const MANIFEST_HEADER_BYTES: usize = 384;
/// Exact encoded manifest segment-reference length.
pub const SEGMENT_REFERENCE_BYTES: usize = 120;

const METADATA_VERSION: u16 = 2;
const IDENTITY_MAGIC: &[u8; 8] = b"OZYIDENT";
const CURRENT_MAGIC: &[u8; 8] = b"OZYCUR\0\0";
const MANIFEST_MAGIC: &[u8; 8] = b"OZYMAN\0\0";
const IDENTITY_HASH_CONTEXT: &str = "ozzy journal group identity v1";
const CURRENT_HASH_CONTEXT: &str = "ozzy journal current reference v1";
const MANIFEST_HASH_CONTEXT: &str = "ozzy journal manifest v1";
const IDENTITY_DIGEST_RANGE: Range<usize> = 80..112;
const CURRENT_DIGEST_RANGE: Range<usize> = 88..120;
const MANIFEST_DIGEST_RANGE: Range<usize> = 312..344;
const MANIFEST_CHECKPOINT: u32 = 1 << 0;
const MANIFEST_LOCAL_COMMIT: u32 = 1 << 1;
const MANIFEST_DURABLE_EVIDENCE: u32 = 1 << 2;
const SEGMENT_ACTIVE: u32 = 1 << 0;

/// Authority deciding whether a complete local group is committed on recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// Consensus/hard state supplies accepted and committed boundaries.
    External,
    /// Every complete synchronized local group is eligible to commit.
    LocalDurable,
}

/// Immutable group replica identity stored below one volume root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupIdentity {
    /// Persistent partition replication-group identity.
    pub group_id: GroupId,
    /// Persistent broker routing identity owning this store.
    pub replica_node_id: NodeId,
    /// Persistent containing physical volume identity.
    pub volume_id: VolumeId,
    /// Persistent local journal-store identity.
    pub store_id: StoreId,
    /// Local relocation/configuration fence. Independent of manifest generation.
    pub store_generation: u64,
}

impl GroupIdentity {
    /// Check structural identity or prefix invariants and return the unchanged value.
    pub fn validate(self) -> Result<Self, MetadataError> {
        for (kind, bytes) in [
            ("group", self.group_id.as_bytes()),
            ("replica node", self.replica_node_id.as_bytes()),
            ("volume", self.volume_id.as_bytes()),
            ("store", self.store_id.as_bytes()),
        ] {
            require_nonzero_id(kind, bytes)?;
        }
        if self.store_generation == 0 {
            return Err(MetadataError::InvalidStoreGeneration);
        }
        Ok(self)
    }
}

/// Logical operation position and its canonical digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPosition {
    /// Partition-local canonical operation number.
    pub op_number: u64,
    /// Integrity digest bound to this exact object or canonical prefix.
    pub digest: Digest,
}

impl LogPosition {
    /// Empty canonical operation prefix with the zero digest.
    pub const GENESIS: Self = Self {
        op_number: 0,
        digest: Digest::ZERO,
    };

    /// Check structural identity or prefix invariants and return the unchanged value.
    pub fn validate(self) -> Result<Self, MetadataError> {
        if self.op_number != u64::MAX && (self.op_number == 0) == (self.digest == Digest::ZERO) {
            Ok(self)
        } else {
            Err(MetadataError::InvalidLogPosition)
        }
    }
}

/// Immutable checkpoint generation selected by one manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointReference {
    /// Exact checkpoint artifact identity.
    pub checkpoint_id: CheckpointId,
    /// Canonical operation number and digest represented by this checkpoint.
    pub position: LogPosition,
    /// Integrity digest binding the selected manifest bytes.
    pub manifest_digest: Digest,
}

/// Final byte boundary and digest of an immutable segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedSegment {
    /// Physical bytes covered by the exact validated segment prefix.
    pub valid_bytes: u64,
    /// Integrity digest bound to this exact object or canonical prefix.
    pub digest: Digest,
}

/// Manifest reference needed to scan one physical segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReference {
    /// Physical segment identity.
    pub segment_id: u64,
    /// Physical replacement incarnation. Zero selects the original file.
    pub file_generation: u64,
    /// First expected physical write-group number in this segment.
    pub first_group_number: u64,
    /// Exact canonical operation chain at this segment start.
    pub first_chain: ChainPosition,
    /// Configured physical segment capacity in bytes.
    pub capacity: u64,
    /// None identifies the one active final segment.
    pub sealed: Option<SealedSegment>,
}

impl SegmentReference {
    pub(crate) fn file_name(self) -> String {
        if self.file_generation == 0 {
            format!("{}.log", self.segment_id)
        } else {
            format!("{}.{}.log", self.segment_id, self.file_generation)
        }
    }
}

/// One immutable metadata generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// Exact metadata-manifest generation.
    pub generation: u64,
    /// Exact predecessor manifest generation.
    pub parent_generation: u64,
    /// Exact group, node, volume, store, and store-generation binding.
    pub identity: GroupIdentity,
    /// Selected membership/configuration epoch.
    pub configuration_epoch: u64,
    /// Explicit journal durability and metadata-acceptance mode.
    pub commit_mode: CommitMode,
    /// A fixed durable accepted-history record is mandatory for recovery.
    pub durable_evidence: bool,
    /// Promised election view constraining retained history and authority.
    pub promised_view: u64,
    /// Last installed normal election view.
    pub last_normal_view: u64,
    /// Exact canonical operation prefix accepted by the selected metadata.
    pub accepted: LogPosition,
    /// Exact confirmed canonical operation prefix.
    pub committed: LogPosition,
    /// Optional exact checkpoint selected as a recovery source.
    pub checkpoint: Option<CheckpointReference>,
    /// Selected physical segment chain in journal order.
    pub segments: Vec<SegmentReference>,
}

/// Small replaceable pointer to one immutable manifest generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentReference {
    /// Persistent partition replication-group identity.
    pub group_id: GroupId,
    /// Persistent local journal-store identity.
    pub store_id: StoreId,
    /// Exact metadata-manifest generation.
    pub generation: u64,
    /// Integrity digest binding the selected manifest bytes.
    pub manifest_digest: Digest,
}

/// Bounds enforced before allocating manifest references.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataLimits {
    /// Maximum encoded manifest bytes, including its header.
    pub max_manifest_bytes: usize,
    /// Maximum selected or scanned physical segments.
    pub max_segments: usize,
}

impl Default for MetadataLimits {
    fn default() -> Self {
        Self {
            max_manifest_bytes: 16 * 1024 * 1024,
            max_segments: 65_536,
        }
    }
}

/// Immutable metadata format or invariant failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataError {
    #[error("truncated {object}: need {needed} bytes, have {available}")]
    /// Input bytes end before a complete encoded object.
    Truncated {
        /// Named physical or encoded object that failed validation.
        object: &'static str,
        /// Required bytes for a complete encoded field.
        needed: usize,
        /// Available bytes, capacity, or canonical prefix at failure.
        available: usize,
    },
    #[error("wrong {0} magic")]
    /// The named object has an incorrect format magic.
    WrongMagic(&'static str),
    #[error("unsupported metadata version {0}")]
    /// The encoded format version is unsupported.
    UnsupportedVersion(u16),
    #[error("unsupported nonzero flags or reserved bytes in {0}")]
    /// Unsupported nonzero flags or reserved bytes in.
    UnsupportedFields(&'static str),
    #[error("{0} digest mismatch")]
    /// The named object does not match its expected integrity digest.
    DigestMismatch(&'static str),
    #[error("{0} identity is zero")]
    /// A required persistent identity is zero.
    ZeroIdentity(&'static str),
    #[error("store generation is zero")]
    /// Store generation is zero.
    InvalidStoreGeneration,
    #[error("manifest generation or parent is invalid")]
    /// Manifest generation or parent is invalid.
    InvalidGeneration,
    #[error("operation position and digest do not agree")]
    /// Operation position and digest do not agree.
    InvalidLogPosition,
    #[error("manifest accepted position precedes committed position")]
    /// Manifest accepted position precedes committed position.
    CommitBeyondAccepted,
    #[error("last installed normal view exceeds promised view")]
    /// Last installed normal view exceeds promised view.
    InvalidView,
    #[error("checkpoint reference is invalid")]
    /// Checkpoint reference is invalid.
    InvalidCheckpoint,
    #[error("segment reference or ordering is invalid")]
    /// Segment reference or ordering is invalid.
    InvalidSegmentReference,
    #[error("manifest requires exactly one final active segment")]
    /// Manifest requires exactly one final active segment.
    InvalidActiveSegment,
    #[error("CURRENT reference is invalid")]
    /// CURRENT reference is invalid.
    InvalidCurrent,
    #[error("integer or length arithmetic overflow")]
    /// Integer or byte-count arithmetic overflows the supported range.
    LengthOverflow,
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
    #[error("metadata file has {0} trailing bytes")]
    /// Metadata file has trailing bytes.
    TrailingBytes(usize),
}

/// Validate and encode the fixed-size persistent group/store binding.
pub fn encode_group_identity(
    identity: GroupIdentity,
) -> Result<[u8; GROUP_IDENTITY_BYTES], MetadataError> {
    identity.validate()?;
    let mut output = [0_u8; GROUP_IDENTITY_BYTES];
    output[0..8].copy_from_slice(IDENTITY_MAGIC);
    put_u16(&mut output, 8, METADATA_VERSION);
    put_u16(&mut output, 10, GROUP_IDENTITY_BYTES as u16);
    output[16..32].copy_from_slice(identity.group_id.as_bytes());
    output[32..48].copy_from_slice(identity.replica_node_id.as_bytes());
    output[48..64].copy_from_slice(identity.volume_id.as_bytes());
    output[64..80].copy_from_slice(identity.store_id.as_bytes());
    put_u64(&mut output, 112, identity.store_generation);
    let digest = hash_zeroed(IDENTITY_HASH_CONTEXT, &output, IDENTITY_DIGEST_RANGE);
    output[IDENTITY_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
    Ok(output)
}

/// Validate and decode the fixed-size persistent group/store binding.
pub fn decode_group_identity(input: &[u8]) -> Result<GroupIdentity, MetadataError> {
    require_len(input, GROUP_IDENTITY_BYTES, "group identity")?;
    if input.len() != GROUP_IDENTITY_BYTES {
        return Err(MetadataError::TrailingBytes(
            input.len() - GROUP_IDENTITY_BYTES,
        ));
    }
    if &input[0..8] != IDENTITY_MAGIC {
        return Err(MetadataError::WrongMagic("group identity"));
    }
    require_version(read_u16(input, 8))?;
    if read_u16(input, 10) != GROUP_IDENTITY_BYTES as u16
        || read_u32(input, 12) != 0
        || !all_zero(&input[120..])
    {
        return Err(MetadataError::UnsupportedFields("group identity"));
    }
    verify_hash(
        IDENTITY_HASH_CONTEXT,
        input,
        IDENTITY_DIGEST_RANGE,
        "group identity",
    )?;
    GroupIdentity {
        group_id: GroupId::from_bytes(array_16(input, 16)),
        replica_node_id: NodeId::from_bytes(array_16(input, 32)),
        volume_id: VolumeId::from_bytes(array_16(input, 48)),
        store_id: StoreId::from_bytes(array_16(input, 64)),
        store_generation: read_u64(input, 112),
    }
    .validate()
}

/// Validate and encode an exact manifest-selection reference.
pub fn encode_current(current: CurrentReference) -> Result<[u8; CURRENT_BYTES], MetadataError> {
    validate_current(current)?;
    let mut output = [0_u8; CURRENT_BYTES];
    output[0..8].copy_from_slice(CURRENT_MAGIC);
    put_u16(&mut output, 8, METADATA_VERSION);
    put_u16(&mut output, 10, CURRENT_BYTES as u16);
    output[16..32].copy_from_slice(current.group_id.as_bytes());
    output[32..48].copy_from_slice(current.store_id.as_bytes());
    put_u64(&mut output, 48, current.generation);
    output[56..88].copy_from_slice(current.manifest_digest.as_bytes());
    let digest = hash_zeroed(CURRENT_HASH_CONTEXT, &output, CURRENT_DIGEST_RANGE);
    output[CURRENT_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
    Ok(output)
}

/// Validate and decode an exact manifest-selection reference.
pub fn decode_current(input: &[u8]) -> Result<CurrentReference, MetadataError> {
    require_len(input, CURRENT_BYTES, "CURRENT")?;
    if input.len() != CURRENT_BYTES {
        return Err(MetadataError::TrailingBytes(input.len() - CURRENT_BYTES));
    }
    if &input[0..8] != CURRENT_MAGIC {
        return Err(MetadataError::WrongMagic("CURRENT"));
    }
    require_version(read_u16(input, 8))?;
    if read_u16(input, 10) != CURRENT_BYTES as u16
        || read_u32(input, 12) != 0
        || !all_zero(&input[120..])
    {
        return Err(MetadataError::UnsupportedFields("CURRENT"));
    }
    verify_hash(CURRENT_HASH_CONTEXT, input, CURRENT_DIGEST_RANGE, "CURRENT")?;
    let current = CurrentReference {
        group_id: GroupId::from_bytes(array_16(input, 16)),
        store_id: StoreId::from_bytes(array_16(input, 32)),
        generation: read_u64(input, 48),
        manifest_digest: digest_at(input, 56),
    };
    validate_current(current)?;
    Ok(current)
}

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
    output[0..8].copy_from_slice(MANIFEST_MAGIC);
    put_u16(&mut output, 8, METADATA_VERSION);
    put_u16(&mut output, 10, MANIFEST_HEADER_BYTES as u16);
    let flags = (u32::from(manifest.checkpoint.is_some()) * MANIFEST_CHECKPOINT)
        | (u32::from(manifest.commit_mode == CommitMode::LocalDurable) * MANIFEST_LOCAL_COMMIT)
        | (u32::from(manifest.durable_evidence) * MANIFEST_DURABLE_EVIDENCE);
    put_u32(&mut output, 12, flags);
    put_u64(&mut output, 16, total_bytes as u64);
    put_u64(&mut output, 24, manifest.generation);
    put_u64(&mut output, 32, manifest.parent_generation);
    output[40..56].copy_from_slice(manifest.identity.group_id.as_bytes());
    output[56..72].copy_from_slice(manifest.identity.replica_node_id.as_bytes());
    output[72..88].copy_from_slice(manifest.identity.volume_id.as_bytes());
    output[88..104].copy_from_slice(manifest.identity.store_id.as_bytes());
    put_u64(&mut output, 104, manifest.configuration_epoch);
    put_u64(&mut output, 112, manifest.promised_view);
    put_u64(&mut output, 120, manifest.last_normal_view);
    put_position(&mut output, 128, manifest.accepted);
    put_position(&mut output, 168, manifest.committed);
    if let Some(checkpoint) = manifest.checkpoint {
        output[208..224].copy_from_slice(checkpoint.checkpoint_id.as_bytes());
        put_position(&mut output, 224, checkpoint.position);
        output[264..296].copy_from_slice(checkpoint.manifest_digest.as_bytes());
    }
    put_u32(&mut output, 296, segment_count);
    put_u64(&mut output, 304, body_bytes as u64);
    for (index, segment) in manifest.segments.iter().enumerate() {
        let start = MANIFEST_HEADER_BYTES + index * SEGMENT_REFERENCE_BYTES;
        encode_segment_reference(
            *segment,
            &mut output[start..start + SEGMENT_REFERENCE_BYTES],
        );
    }
    put_u64(&mut output, 344, manifest.identity.store_generation);
    let digest = hash_zeroed(MANIFEST_HASH_CONTEXT, &output, MANIFEST_DIGEST_RANGE);
    output[MANIFEST_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
    Ok(output)
}

/// Validate and decode bounded selected journal metadata.
pub fn decode_manifest(input: &[u8], limits: MetadataLimits) -> Result<Manifest, MetadataError> {
    require_len(input, MANIFEST_HEADER_BYTES, "manifest header")?;
    enforce_limit("manifest bytes", input.len(), limits.max_manifest_bytes)?;
    if &input[0..8] != MANIFEST_MAGIC {
        return Err(MetadataError::WrongMagic("manifest"));
    }
    require_version(read_u16(input, 8))?;
    if read_u16(input, 10) != MANIFEST_HEADER_BYTES as u16
        || read_u32(input, 12)
            & !(MANIFEST_CHECKPOINT | MANIFEST_LOCAL_COMMIT | MANIFEST_DURABLE_EVIDENCE)
            != 0
        || read_u32(input, 300) != 0
        || !all_zero(&input[352..MANIFEST_HEADER_BYTES])
    {
        return Err(MetadataError::UnsupportedFields("manifest"));
    }
    let total_bytes = usize_from_u64(read_u64(input, 16))?;
    let body_bytes = usize_from_u64(read_u64(input, 304))?;
    let segment_count = read_u32(input, 296) as usize;
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

    let identity = GroupIdentity {
        group_id: GroupId::from_bytes(array_16(input, 40)),
        replica_node_id: NodeId::from_bytes(array_16(input, 56)),
        volume_id: VolumeId::from_bytes(array_16(input, 72)),
        store_id: StoreId::from_bytes(array_16(input, 88)),
        store_generation: read_u64(input, 344),
    };
    identity.validate()?;
    let flags = read_u32(input, 12);
    let commit_mode = if flags & MANIFEST_LOCAL_COMMIT == 0 {
        CommitMode::External
    } else {
        CommitMode::LocalDurable
    };
    let checkpoint = if flags & MANIFEST_CHECKPOINT == 0 {
        if !all_zero(&input[208..296]) {
            return Err(MetadataError::InvalidCheckpoint);
        }
        None
    } else {
        Some(CheckpointReference {
            checkpoint_id: CheckpointId::from_bytes(array_16(input, 208)),
            position: read_position(input, 224),
            manifest_digest: digest_at(input, 264),
        })
    };
    let mut segments = Vec::with_capacity(segment_count);
    for index in 0..segment_count {
        let start = MANIFEST_HEADER_BYTES + index * SEGMENT_REFERENCE_BYTES;
        segments.push(decode_segment_reference(
            &input[start..start + SEGMENT_REFERENCE_BYTES],
        )?);
    }
    let manifest = Manifest {
        generation: read_u64(input, 24),
        parent_generation: read_u64(input, 32),
        identity,
        configuration_epoch: read_u64(input, 104),
        commit_mode,
        durable_evidence: flags & MANIFEST_DURABLE_EVIDENCE != 0,
        promised_view: read_u64(input, 112),
        last_normal_view: read_u64(input, 120),
        accepted: read_position(input, 128),
        committed: read_position(input, 168),
        checkpoint,
        segments,
    };
    validate_manifest(&manifest)?;
    Ok(manifest)
}

/// Validate encoded manifest bytes and return digest named by `CURRENT`.
pub fn manifest_digest(input: &[u8], limits: MetadataLimits) -> Result<Digest, MetadataError> {
    decode_manifest(input, limits)?;
    Ok(digest_at(input, MANIFEST_DIGEST_RANGE.start))
}

fn validate_current(current: CurrentReference) -> Result<(), MetadataError> {
    require_nonzero_id("group", current.group_id.as_bytes())?;
    require_nonzero_id("store", current.store_id.as_bytes())?;
    if current.generation == 0 || current.manifest_digest == Digest::ZERO {
        return Err(MetadataError::InvalidCurrent);
    }
    Ok(())
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

fn encode_segment_reference(segment: SegmentReference, output: &mut [u8]) {
    put_u64(output, 0, segment.segment_id);
    put_u64(output, 8, segment.first_group_number);
    put_u64(output, 16, segment.first_chain.next_op_number());
    output[24..56].copy_from_slice(segment.first_chain.previous_digest().as_bytes());
    put_u64(output, 56, segment.capacity);
    if let Some(sealed) = segment.sealed {
        put_u64(output, 64, sealed.valid_bytes);
        output[72..104].copy_from_slice(sealed.digest.as_bytes());
    } else {
        put_u32(output, 104, SEGMENT_ACTIVE);
    }
    put_u64(output, 108, segment.file_generation);
}

fn decode_segment_reference(input: &[u8]) -> Result<SegmentReference, MetadataError> {
    let flags = read_u32(input, 104);
    if flags & !SEGMENT_ACTIVE != 0 || !all_zero(&input[116..]) {
        return Err(MetadataError::UnsupportedFields("segment reference"));
    }
    let valid_bytes = read_u64(input, 64);
    let digest = digest_at(input, 72);
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
        segment_id: read_u64(input, 0),
        file_generation: read_u64(input, 108),
        first_group_number: read_u64(input, 8),
        first_chain: ChainPosition::new(read_u64(input, 16), digest_at(input, 24)),
        capacity: read_u64(input, 56),
        sealed,
    })
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

fn verify_hash(
    context: &'static str,
    input: &[u8],
    zero: Range<usize>,
    object: &'static str,
) -> Result<(), MetadataError> {
    let expected = digest_at(input, zero.start);
    let actual = hash_zeroed(context, input, zero);
    if expected == actual {
        Ok(())
    } else {
        Err(MetadataError::DigestMismatch(object))
    }
}

fn hash_zeroed(context: &'static str, input: &[u8], zero: Range<usize>) -> Digest {
    let mut hasher = Hasher::new(context);
    hasher.update(&input[..zero.start]);
    hasher.update(&[0; 32]);
    hasher.update(&input[zero.end..]);
    hasher.finish()
}

fn require_nonzero_id(kind: &'static str, bytes: &[u8; 16]) -> Result<(), MetadataError> {
    if all_zero(bytes) {
        Err(MetadataError::ZeroIdentity(kind))
    } else {
        Ok(())
    }
}

fn require_version(version: u16) -> Result<(), MetadataError> {
    if version == METADATA_VERSION {
        Ok(())
    } else {
        Err(MetadataError::UnsupportedVersion(version))
    }
}

fn require_len(input: &[u8], needed: usize, object: &'static str) -> Result<(), MetadataError> {
    if input.len() < needed {
        Err(MetadataError::Truncated {
            object,
            needed,
            available: input.len(),
        })
    } else {
        Ok(())
    }
}

fn enforce_limit(kind: &'static str, actual: usize, limit: usize) -> Result<(), MetadataError> {
    if limit == 0 || actual > limit {
        Err(MetadataError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn usize_from_u64(value: u64) -> Result<usize, MetadataError> {
    usize::try_from(value).map_err(|_| MetadataError::LengthOverflow)
}

fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
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
    u16::from_be_bytes([input[offset], input[offset + 1]])
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
    ])
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes([
        input[offset],
        input[offset + 1],
        input[offset + 2],
        input[offset + 3],
        input[offset + 4],
        input[offset + 5],
        input[offset + 6],
        input[offset + 7],
    ])
}

fn array_16(input: &[u8], offset: usize) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&input[offset..offset + 16]);
    bytes
}

fn digest_at(input: &[u8], offset: usize) -> Digest {
    let mut bytes = [0; 32];
    bytes.copy_from_slice(&input[offset..offset + 32]);
    Digest::from_bytes(bytes)
}
