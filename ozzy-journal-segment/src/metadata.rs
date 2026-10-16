#![cfg_attr(not(test), warn(clippy::indexing_slicing))]

use std::ops::Range;

use ozzy_journal::integrity::hash_without_digest;
use ozzy_journal::operation::{ChainPosition, Digest};
use ozzy_proto::layout;
use ozzy_proto::{CheckpointId, GroupId, NodeId, StoreId, VolumeId};

mod current;
mod error;
mod identity;
mod manifest;

pub use current::{decode_current, encode_current};
pub use error::MetadataError;
pub use identity::{decode_group_identity, encode_group_identity};
pub use manifest::{
    decode_manifest, encode_manifest, encode_manifest_with_limits, manifest_digest,
};

/// Exact encoded persistent group/store identity length.
pub const GROUP_IDENTITY_BYTES: usize = 4 * 1024;
/// Exact encoded manifest-selection reference length.
pub const CURRENT_BYTES: usize = 128;
/// Exact encoded selected-manifest header length.
pub const MANIFEST_HEADER_BYTES: usize = 384;
/// Exact encoded manifest segment-reference length.
pub const SEGMENT_REFERENCE_BYTES: usize = 120;

const METADATA_VERSION: u16 = 2;

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

    /// Chain position of the operation that follows this prefix.
    pub(crate) fn following_chain(self) -> Result<ChainPosition, MetadataError> {
        let position = self.validate()?;
        Ok(ChainPosition::new(position.op_number + 1, position.digest))
    }
}

/// Exact prefix that ends immediately before `chain`.
pub(crate) fn position_before(chain: ChainPosition) -> Result<LogPosition, MetadataError> {
    LogPosition {
        op_number: chain
            .next_op_number()
            .checked_sub(1)
            .ok_or(MetadataError::InvalidLogPosition)?,
        digest: chain.previous_digest(),
    }
    .validate()
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

fn verify_hash(
    context: &'static str,
    input: &[u8],
    zero: Range<usize>,
    object: &'static str,
) -> Result<(), MetadataError> {
    let expected = Digest::from_bytes(*layout::record(input, zero.start)?);
    let actual = hash_without_digest(context, input, zero.start);
    if expected == actual {
        Ok(())
    } else {
        Err(MetadataError::DigestMismatch(object))
    }
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
