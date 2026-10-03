//! Canonical identities, operation bodies, and admission metadata.

use super::{AppendRecordList, OperationCodecError};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, GroupId, MessageId, Offset, OperationId, OwnerEpoch,
    PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};
use smallvec::SmallVec;
use std::num::NonZeroU64;

/// A fixed-width digest or externally supplied identity fingerprint.
/// Computed integrity digests contain XXH3-128 followed by 16 zero bytes;
/// see [`crate::integrity`]. External principal fingerprints remain opaque.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    /// Digest used by the configured genesis operation prefix.
    pub const ZERO: Self = Self([0; 32]);

    /// Construct a digest from its canonical bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return canonical digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// One operation kind in the canonical journal schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum OperationKind {
    /// Create an incarnation and bind its logical address and owner fence.
    CreatePartition = 1,
    /// Open or fence one partition-local producer session.
    OpenProducer = 2,
    /// Assign consecutive records to producer sequences and partition offsets.
    Append = 3,
    /// Declare exclusive individual or consumer-group progress.
    Progress = 4,
    /// Install or revoke a fenced consumer-group partition assignment.
    Assign = 5,
    /// Advance the partition retention floor.
    Trim = 6,
    /// Install a newer retention-policy revision.
    PartitionPolicy = 7,
    /// Record a control-operation identity without changing record state.
    Barrier = 8,
    /// Advance the retained producer retry-result floor.
    ProducerResultFloor = 9,
}

impl TryFrom<u16> for OperationKind {
    type Error = OperationCodecError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::CreatePartition),
            2 => Ok(Self::OpenProducer),
            3 => Ok(Self::Append),
            4 => Ok(Self::Progress),
            5 => Ok(Self::Assign),
            6 => Ok(Self::Trim),
            7 => Ok(Self::PartitionPolicy),
            8 => Ok(Self::Barrier),
            9 => Ok(Self::ProducerResultFloor),
            other => Err(OperationCodecError::UnsupportedOperationKind(other)),
        }
    }
}

/// Expected logical chain position before an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainPosition {
    next_op_number: u64,
    previous_digest: Digest,
}

impl ChainPosition {
    /// Configured genesis position. Journal operations begin at one.
    pub const GENESIS: Self = Self {
        next_op_number: 1,
        previous_digest: Digest::ZERO,
    };

    /// Bind the next canonical operation number to its predecessor digest.
    pub const fn new(next_op_number: u64, previous_digest: Digest) -> Self {
        Self {
            next_op_number,
            previous_digest,
        }
    }

    /// Next expected canonical operation number.
    pub const fn next_op_number(self) -> u64 {
        self.next_op_number
    }

    /// Canonical chain digest immediately before the next operation.
    pub const fn previous_digest(self) -> Digest {
        self.previous_digest
    }
}

/// One canonical operation ready for wire or physical-disk framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalOperation<'a> {
    /// Persistent partition replication-group identity.
    pub group_id: GroupId,
    /// Membership/configuration fence at admission.
    pub configuration_epoch: u64,
    /// Original admission view, preserved through replication and repair.
    pub original_view: u64,
    /// Partition-local canonical operation number.
    pub op_number: u64,
    /// Canonical chain digest immediately before this operation.
    pub previous_digest: Digest,
    /// Canonical operation discriminator.
    pub kind: OperationKind,
    /// Exact canonical body bytes covered by logical integrity.
    pub body: &'a [u8],
}

/// Logical identity without any assumption about body storage or contiguity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationHeader {
    /// Persistent partition replication-group identity.
    pub group_id: GroupId,
    /// Membership/configuration fence at admission.
    pub configuration_epoch: u64,
    /// Original admission view, preserved through replication and repair.
    pub original_view: u64,
    /// Partition-local canonical operation number.
    pub op_number: u64,
    /// Canonical chain digest immediately before this operation.
    pub previous_digest: Digest,
    /// Canonical operation discriminator.
    pub kind: OperationKind,
}

impl CanonicalOperation<'_> {
    /// Borrow-independent canonical metadata for this exact operation.
    pub const fn header(&self) -> OperationHeader {
        OperationHeader {
            group_id: self.group_id,
            configuration_epoch: self.configuration_epoch,
            original_view: self.original_view,
            op_number: self.op_number,
            previous_digest: self.previous_digest,
            kind: self.kind,
        }
    }
}

/// Independent age/size retention targets. Neither target means keep forever.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Optional maximum retained record age in milliseconds.
    pub max_age_millis: Option<NonZeroU64>,
    /// Optional maximum retained record bytes.
    pub max_bytes: Option<NonZeroU64>,
}

/// Partition creation. Policy revision starts at one implicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartition<'a> {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Logical stream namespace.
    pub stream: &'a str,
    /// Logical topic name.
    pub topic: &'a str,
    /// Partition number within its topic.
    pub partition_id: PartitionId,
    /// Expected partition ownership fence.
    pub owner_epoch: OwnerEpoch,
    /// Retention policy stored in canonical state.
    pub retention: RetentionPolicy,
}

/// Idempotent producer-session open or fencing transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenProducer {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Producer identity scoped to this partition.
    pub producer_id: ProducerId,
    /// Expected existing session epoch; none requires no session.
    pub expected_epoch: Option<ProducerEpoch>,
    /// New producer fence, strictly newer than an existing session.
    pub new_epoch: ProducerEpoch,
    /// Exact control-operation retry identity.
    pub operation_id: OperationId,
}

/// One record within a canonical append sub-batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRecord<'a> {
    /// Payload representation, preserved through persistence and replay.
    pub encoding: ozzy_proto::data::Encoding,
    /// Application record identity preserved through retry and replay.
    pub message_id: MessageId,
    /// Opaque payload parts in record order.
    pub parts: SmallVec<[&'a [u8]; 2]>,
}

/// One partition-contiguous append within an atomic group operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBatch<'a> {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Expected partition ownership fence.
    pub owner_epoch: OwnerEpoch,
    /// Producer identity scoped to this partition.
    pub producer_id: ProducerId,
    /// Expected producer-session fence.
    pub producer_epoch: ProducerEpoch,
    /// First contiguous producer-local sequence.
    pub first_sequence: ProducerSequence,
    /// First assigned partition-global record offset.
    pub first_offset: Offset,
    /// Primary-resolved Unix timestamp in milliseconds.
    pub append_timestamp_millis: u64,
    /// Consecutive records in this partition batch.
    pub records: AppendRecordList<'a>,
}

/// One journal append operation containing independently addressed partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Append<'a> {
    /// Independently addressed batches in canonical operation order.
    pub batches: Vec<AppendBatch<'a>>,
}

/// Metadata needed to validate one append's state transition, without records.
/// This is not a hash proof or authorization to skip canonical body validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendBatchSummary {
    /// Destination partition incarnation.
    pub partition: PartitionIncarnation,
    /// Expected partition ownership epoch.
    pub owner_epoch: OwnerEpoch,
    /// Producer bound to this partition.
    pub producer_id: ProducerId,
    /// Expected producer session epoch.
    pub producer_epoch: ProducerEpoch,
    /// First contiguous producer sequence in this batch.
    pub first_sequence: ProducerSequence,
    /// First assigned partition offset in this batch.
    pub first_offset: Offset,
    /// Nonzero record count checked against schema bounds.
    pub record_count: usize,
    /// Whether every descriptor contains a nonzero message identity.
    /// State admission must reject false; syntax validation alone permits it.
    pub nonzero_message_ids: bool,
}

/// One record's byte position inside its batch, recorded while the schema
/// walker validates descriptors. Offsets are relative to the batch's
/// descriptor table and payload start; the final entry per batch is an end
/// checkpoint with zero parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordPosition {
    /// Byte offset relative to the batch descriptor table.
    pub descriptor: u32,
    /// Byte offset relative to the batch payload start.
    pub payload: u32,
    /// Part count; zero marks the batch end checkpoint.
    pub parts: u32,
}

/// Schema-validated append metadata. Up to four partitions stay on the stack.
/// Record descriptors and all part lengths use the ordinary schema walker;
/// application state must still check epochs, positions, and nonzero IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSummary {
    pub(super) batches: SmallVec<[AppendBatchSummary; 4]>,
}

impl AppendSummary {
    /// Construct a one-batch summary from metadata carried by a validated wire APPEND.
    pub fn single(batch: AppendBatchSummary) -> Self {
        Self {
            batches: smallvec::smallvec![batch],
        }
    }

    /// Partition batches in canonical order, without materialized record tables.
    pub fn batches(&self) -> &[AppendBatchSummary] {
        &self.batches
    }
}

/// Stable owner of one processing-progress cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressOwner {
    /// Progress declared by one standalone subscription.
    Subscription(SubscriptionId),
    /// Progress declared by a fenced consumer-group assignment.
    ConsumerGroup(ConsumerGroupId),
}

/// Durable contiguous consumer-processing progress transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    /// Standalone subscription or fenced consumer group declaring progress.
    pub owner: ProgressOwner,
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Assignment fence required for consumer-group progress.
    pub assignment_epoch: Option<u64>,
    /// Expected prior exclusive offset; none requires no progress entry.
    pub expected_progress: Option<Offset>,
    /// New exclusive progress offset within confirmed record state.
    pub new_progress: Offset,
    /// Exact control-operation retry identity.
    pub operation_id: OperationId,
}

/// Consumer-group partition assignment transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Assign {
    /// Consumer group whose assignment changes.
    pub consumer_group_id: ConsumerGroupId,
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Expected current assignment fence.
    pub expected_assignment_epoch: u64,
    /// New assignment fence, strictly newer than the current one.
    pub new_assignment_epoch: u64,
    /// New assigned member; none revokes ownership.
    pub new_member: Option<ConsumerMemberId>,
    /// Exact control-operation retry identity.
    pub operation_id: OperationId,
}

/// Advance one partition's committed earliest-retained offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trim {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Expected current retention floor.
    pub expected_floor: Offset,
    /// New monotonic retention floor.
    pub new_floor: Offset,
    /// Exact control-operation retry identity.
    pub operation_id: OperationId,
}

/// Replace one partition's retention policy by revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionPolicy {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Expected current retention-policy revision.
    pub expected_revision: u64,
    /// New retention-policy revision.
    pub new_revision: u64,
    /// Retention policy stored in canonical state.
    pub retention: RetentionPolicy,
    /// Exact control-operation retry identity.
    pub operation_id: OperationId,
}

/// Ordered no-op used to establish an authority or read boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Barrier {
    /// Exact control-operation retry identity.
    pub operation_id: OperationId,
}

/// Advance the exclusive floor below which producer retry results expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerResultFloor {
    /// Exact partition record incarnation.
    pub partition: PartitionIncarnation,
    /// Producer identity scoped to this partition.
    pub producer_id: ProducerId,
    /// Expected producer-session fence.
    pub producer_epoch: ProducerEpoch,
    /// Expected retained producer retry-result floor.
    pub expected_floor: ProducerSequence,
    /// New monotonic producer retry-result floor.
    pub new_floor: ProducerSequence,
    /// Exact control-operation retry identity.
    pub operation_id: OperationId,
}

/// One typed canonical operation body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationBody<'a> {
    /// Create an incarnation and bind its logical address and owner fence.
    CreatePartition(CreatePartition<'a>),
    /// Open or fence one partition-local producer session.
    OpenProducer(OpenProducer),
    /// Assign consecutive records to producer sequences and partition offsets.
    Append(Append<'a>),
    /// Declare exclusive individual or consumer-group progress.
    Progress(Progress),
    /// Install or revoke a fenced consumer-group partition assignment.
    Assign(Assign),
    /// Advance the partition retention floor.
    Trim(Trim),
    /// Install a newer retention-policy revision.
    PartitionPolicy(PartitionPolicy),
    /// Record a control-operation identity without changing record state.
    Barrier(Barrier),
    /// Advance the retained producer retry-result floor.
    ProducerResultFloor(ProducerResultFloor),
}

impl OperationBody<'_> {
    /// Explicit retry identity for control operations. APPEND identity belongs
    /// to its producer sequences; partition creation uses its incarnation.
    pub const fn operation_id(&self) -> Option<OperationId> {
        match self {
            Self::CreatePartition(_) | Self::Append(_) => None,
            Self::OpenProducer(value) => Some(value.operation_id),
            Self::Progress(value) => Some(value.operation_id),
            Self::Assign(value) => Some(value.operation_id),
            Self::Trim(value) => Some(value.operation_id),
            Self::PartitionPolicy(value) => Some(value.operation_id),
            Self::Barrier(value) => Some(value.operation_id),
            Self::ProducerResultFloor(value) => Some(value.operation_id),
        }
    }

    /// Return the canonical operation discriminator.
    pub const fn kind(&self) -> OperationKind {
        match self {
            Self::CreatePartition(_) => OperationKind::CreatePartition,
            Self::OpenProducer(_) => OperationKind::OpenProducer,
            Self::Append(_) => OperationKind::Append,
            Self::Progress(_) => OperationKind::Progress,
            Self::Assign(_) => OperationKind::Assign,
            Self::Trim(_) => OperationKind::Trim,
            Self::PartitionPolicy(_) => OperationKind::PartitionPolicy,
            Self::Barrier(_) => OperationKind::Barrier,
            Self::ProducerResultFloor(_) => OperationKind::ProducerResultFloor,
        }
    }
}

/// Resource bounds applied to canonical operation bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationLimits {
    /// Maximum encoded canonical operation-body bytes.
    pub max_body_bytes: usize,
    /// Maximum UTF-8 bytes per stream or topic name.
    pub max_name_bytes: usize,
    /// Maximum independently addressed batches per APPEND.
    pub max_append_batches: usize,
    /// Maximum combined records per APPEND.
    pub max_records: usize,
    /// Maximum combined payload parts per APPEND.
    pub max_parts: usize,
    /// Maximum combined APPEND payload bytes.
    pub max_payload_bytes: usize,
}

impl Default for OperationLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: 64 * 1024 * 1024,
            max_name_bytes: 255,
            max_append_batches: 1_024,
            max_records: 1_024,
            max_parts: 65_536,
            max_payload_bytes: 64 * 1024 * 1024,
        }
    }
}
