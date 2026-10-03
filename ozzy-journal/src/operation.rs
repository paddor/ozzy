//! Canonical replicated journal operations and their bounded body codec.

use std::num::NonZeroU64;
use std::ops::Range;

use crate::integrity::IntegrityHasher as Hasher;
use bytes::Bytes;
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, GroupId, MessageId, Offset, OperationId, OwnerEpoch,
    PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};
use smallvec::SmallVec;
use thiserror::Error;

mod append_encoder;
mod append_pack;
mod output;
mod records;
mod view;
pub use append_encoder::{
    AppendBodyEncoder, AppendHeader, DescriptorLayout, ValidatedWireAppend,
    append_prepared_record_batch, append_record_batches, append_wire_record_batch,
};
pub use append_pack::{
    AppendPackResult, AppendPackScratch, pack_append_payload, pack_validated_wire_append_payload,
};
pub use output::OperationOutput;
pub use records::{AppendRecordList, RecordIter, RecordParts, RecordRef};
pub use view::{
    AppendBatchView, AppendBatches, AppendPartLengths, AppendParts, AppendRecordDescriptor,
    AppendRecordDescriptors, AppendRecordView, AppendRecords, AppendView, PreparedAppendPayload,
    decode_append_batches, decode_append_batches_indexed,
    decode_append_batches_with_validated_payload, decode_append_records, decode_append_view,
    decode_packed_append_records,
};

const TINY_RECORDS: u32 = 1 << 31;
const PREPARED_PAYLOAD: u32 = 1 << 30;
const RECORD_COUNT_MASK: u32 = !(TINY_RECORDS | PREPARED_PAYLOAD);

const BODY_HASH_CONTEXT: &str = "ozzy journal canonical body v1";
const OPERATION_HASH_CONTEXT: &str = "ozzy journal logical operation v1";
const RETENTION_VERSION: u8 = 1;
const RETENTION_MAX_AGE: u8 = 1 << 0;
const RETENTION_MAX_BYTES: u8 = 1 << 1;
const RETENTION_KNOWN_FLAGS: u8 = RETENTION_MAX_AGE | RETENTION_MAX_BYTES;

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
    CreatePartition = 1,
    OpenProducer = 2,
    Append = 3,
    Progress = 4,
    Assign = 5,
    Trim = 6,
    PartitionPolicy = 7,
    Barrier = 8,
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

    pub const fn new(next_op_number: u64, previous_digest: Digest) -> Self {
        Self {
            next_op_number,
            previous_digest,
        }
    }

    pub const fn next_op_number(self) -> u64 {
        self.next_op_number
    }

    pub const fn previous_digest(self) -> Digest {
        self.previous_digest
    }
}

/// One canonical operation ready for wire or physical-disk framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalOperation<'a> {
    pub group_id: GroupId,
    pub configuration_epoch: u64,
    pub original_view: u64,
    pub op_number: u64,
    pub previous_digest: Digest,
    pub kind: OperationKind,
    pub body: &'a [u8],
}

/// Logical identity without any assumption about body storage or contiguity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationHeader {
    pub group_id: GroupId,
    pub configuration_epoch: u64,
    pub original_view: u64,
    pub op_number: u64,
    pub previous_digest: Digest,
    pub kind: OperationKind,
}

impl CanonicalOperation<'_> {
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
    pub max_age_millis: Option<NonZeroU64>,
    pub max_bytes: Option<NonZeroU64>,
}

/// Partition creation. Policy revision starts at one implicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartition<'a> {
    pub partition: PartitionIncarnation,
    pub stream: &'a str,
    pub topic: &'a str,
    pub partition_id: PartitionId,
    pub owner_epoch: OwnerEpoch,
    pub retention: RetentionPolicy,
}

/// Idempotent producer-session open or fencing transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenProducer {
    pub partition: PartitionIncarnation,
    pub producer_id: ProducerId,
    pub expected_epoch: Option<ProducerEpoch>,
    pub new_epoch: ProducerEpoch,
    pub operation_id: OperationId,
}

/// One record within a canonical append sub-batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRecord<'a> {
    /// Payload representation, preserved through persistence and replay.
    pub encoding: ozzy_proto::data::Encoding,
    pub message_id: MessageId,
    pub parts: SmallVec<[&'a [u8]; 2]>,
}

/// One partition-contiguous append within an atomic group operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBatch<'a> {
    pub partition: PartitionIncarnation,
    pub owner_epoch: OwnerEpoch,
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
    pub first_sequence: ProducerSequence,
    pub first_offset: Offset,
    /// Primary-resolved Unix timestamp in milliseconds.
    pub append_timestamp_millis: u64,
    pub records: AppendRecordList<'a>,
}

/// One journal append operation containing independently addressed partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Append<'a> {
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
    pub descriptor: u32,
    pub payload: u32,
    pub parts: u32,
}

/// Schema-validated append metadata. Up to four partitions stay on the stack.
/// Record descriptors and all part lengths use the ordinary schema walker;
/// application state must still check epochs, positions, and nonzero IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendSummary {
    batches: SmallVec<[AppendBatchSummary; 4]>,
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
    Subscription(SubscriptionId),
    ConsumerGroup(ConsumerGroupId),
}

/// Durable contiguous consumer-processing progress transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub owner: ProgressOwner,
    pub partition: PartitionIncarnation,
    pub assignment_epoch: Option<u64>,
    pub expected_progress: Option<Offset>,
    pub new_progress: Offset,
    pub operation_id: OperationId,
}

/// Consumer-group partition assignment transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Assign {
    pub consumer_group_id: ConsumerGroupId,
    pub partition: PartitionIncarnation,
    pub expected_assignment_epoch: u64,
    pub new_assignment_epoch: u64,
    pub new_member: Option<ConsumerMemberId>,
    pub operation_id: OperationId,
}

/// Advance one partition's committed earliest-retained offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trim {
    pub partition: PartitionIncarnation,
    pub expected_floor: Offset,
    pub new_floor: Offset,
    pub operation_id: OperationId,
}

/// Replace one partition's retention policy by revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionPolicy {
    pub partition: PartitionIncarnation,
    pub expected_revision: u64,
    pub new_revision: u64,
    pub retention: RetentionPolicy,
    pub operation_id: OperationId,
}

/// Ordered no-op used to establish an authority or read boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Barrier {
    pub operation_id: OperationId,
}

/// Advance the exclusive floor below which producer retry results expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerResultFloor {
    pub partition: PartitionIncarnation,
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
    pub expected_floor: ProducerSequence,
    pub new_floor: ProducerSequence,
    pub operation_id: OperationId,
}

/// One typed canonical operation body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationBody<'a> {
    CreatePartition(CreatePartition<'a>),
    OpenProducer(OpenProducer),
    Append(Append<'a>),
    Progress(Progress),
    Assign(Assign),
    Trim(Trim),
    PartitionPolicy(PartitionPolicy),
    Barrier(Barrier),
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
    pub max_body_bytes: usize,
    pub max_name_bytes: usize,
    pub max_append_batches: usize,
    pub max_records: usize,
    pub max_parts: usize,
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

/// Canonical operation body codec failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OperationCodecError {
    #[error("unsupported operation kind {0}")]
    UnsupportedOperationKind(u16),
    #[error("truncated operation body: need {needed} bytes, have {available}")]
    Truncated { needed: usize, available: usize },
    #[error("operation body has {0} trailing bytes")]
    TrailingBytes(usize),
    #[error("invalid UTF-8 in {0}")]
    InvalidUtf8(&'static str),
    #[error("{0} must not be empty")]
    EmptyValue(&'static str),
    #[error("invalid optional-value tag {0}")]
    InvalidOptionTag(u8),
    #[error("invalid progress-owner tag {0}")]
    InvalidProgressOwnerTag(u8),
    #[error("retention policy version or flags are unsupported")]
    InvalidRetention,
    #[error("append operation must contain a batch and each batch a record")]
    EmptyAppend,
    #[error("append record must contain at least one message part")]
    EmptyRecordParts,
    #[error("prepared append payload is invalid or does not match its raw record view")]
    InvalidPreparedPayload,
    #[error("APPEND payload compression failed")]
    AppendCompression,
    #[error("append sequence or offset range overflows")]
    AppendPositionOverflow,
    #[error("consumer-group progress requires an assignment epoch")]
    MissingAssignmentEpoch,
    #[error("standalone subscription progress cannot carry an assignment epoch")]
    UnexpectedAssignmentEpoch,
    #[error("integer or length arithmetic overflow")]
    LengthOverflow,
    #[error("operation output allocation failed")]
    OutputAllocation,
    #[error("{kind} limit exceeded: {actual} > {limit}")]
    LimitExceeded {
        kind: &'static str,
        actual: usize,
        limit: usize,
    },
}

/// Hash exact canonical body bytes independently of their disk representation.
pub fn canonical_body_digest(body: &[u8]) -> Digest {
    crate::integrity::hash(BODY_HASH_CONTEXT, body)
}

/// Start the canonical body's exact checksum domain for incremental processing.
/// Callers can bound each update and yield between chunks without changing bytes
/// or digest identity. An unfinished checksum is not validation evidence.
pub fn canonical_body_hasher() -> crate::integrity::IntegrityHasher {
    Hasher::new(BODY_HASH_CONTEXT)
}

/// Hash a body split across storage buffers, without concatenating it.
pub fn canonical_body_digest_parts<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> Digest {
    let mut hasher = canonical_body_hasher();
    for part in parts {
        hasher.update(part);
    }
    hasher.finish()
}

/// Hash one logical operation independently of disk padding and compression.
pub fn logical_operation_digest(operation: &CanonicalOperation<'_>) -> Digest {
    logical_operation_digest_with_body_digest(operation, canonical_body_digest(operation.body))
}

/// Hash one logical operation using an already verified canonical body digest.
pub fn logical_operation_digest_with_body_digest(
    operation: &CanonicalOperation<'_>,
    body_digest: Digest,
) -> Digest {
    operation.header().digest(body_digest)
}

impl OperationHeader {
    /// Hash logical identity and an already verified body digest.
    pub fn digest(&self, body_digest: Digest) -> Digest {
        let operation = self;
        let mut hasher = Hasher::new(OPERATION_HASH_CONTEXT);
        hasher.update(operation.group_id.as_bytes());
        hasher.update(&operation.configuration_epoch.to_be_bytes());
        hasher.update(&operation.original_view.to_be_bytes());
        hasher.update(&operation.op_number.to_be_bytes());
        hasher.update(&(operation.kind as u16).to_be_bytes());
        hasher.update(operation.previous_digest.as_bytes());
        hasher.update(body_digest.as_bytes());
        hasher.finish()
    }
}

/// Encode one typed body into its canonical network-byte-order representation.
pub fn encode_operation_body(
    body: &OperationBody<'_>,
    limits: OperationLimits,
) -> Result<Vec<u8>, OperationCodecError> {
    let mut output = Vec::new();
    append_operation_body(&mut output, body, limits)?;
    Ok(output)
}

/// Append one typed body in canonical network-byte order.
///
/// The returned range identifies this body in `output`. Bytes already present
/// in `output` are preserved. A failed encode restores its original length so
/// callers can safely reuse one arena for a complete operation group.
pub fn append_operation_body(
    output: &mut Vec<u8>,
    body: &OperationBody<'_>,
    limits: OperationLimits,
) -> Result<Range<usize>, OperationCodecError> {
    append_operation_body_to(output, body, limits)
}

/// Append through a reusable contiguous or segmented output. Failure restores
/// the original length. The codec preserves descriptor and payload ordering.
pub fn append_operation_body_to(
    output: &mut impl OperationOutput,
    body: &OperationBody<'_>,
    limits: OperationLimits,
) -> Result<Range<usize>, OperationCodecError> {
    validate_limits(limits)?;
    let start = output.len();
    let mut encoder = Encoder::new(output, start, limits.max_body_bytes);
    let result = (|| {
        match body {
            OperationBody::CreatePartition(value) => encode_create(value, limits, &mut encoder)?,
            OperationBody::OpenProducer(value) => encode_open_producer(*value, &mut encoder)?,
            OperationBody::Append(value) => encode_append(value, limits, &mut encoder)?,
            OperationBody::Progress(value) => encode_progress(*value, &mut encoder)?,
            OperationBody::Assign(value) => encode_assign(*value, &mut encoder)?,
            OperationBody::Trim(value) => encode_trim(*value, &mut encoder)?,
            OperationBody::PartitionPolicy(value) => encode_policy(*value, &mut encoder)?,
            OperationBody::Barrier(value) => encoder.id(value.operation_id.as_bytes())?,
            OperationBody::ProducerResultFloor(value) => {
                encode_producer_result_floor(*value, &mut encoder)?;
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        encoder.restore_start();
        return Err(error);
    }
    Ok(start..encoder.finish())
}

/// Validate exactly one canonical body and its configured schema limits.
///
/// Uses the same schema walker as [`decode_operation_body`] without allocating
/// record or part vectors. Text and opaque payload bytes stay borrowed.
/// This checks syntax, ranges, and framing, not canonical state transitions or
/// integrity digests. Callers remain responsible for those independent checks.
pub fn validate_operation_body(
    kind: OperationKind,
    input: &[u8],
    limits: OperationLimits,
) -> Result<(), OperationCodecError> {
    decode_body::<false, true>(kind, input, limits).map(|_| ())
}

/// Like [`validate_operation_body`], but skip whole-payload LZ4 blocks that the
/// caller already validated for these exact bytes, or produced itself.
/// Descriptors, decoded lengths, and every limit are still checked.
pub fn validate_operation_body_with_validated_payload(
    kind: OperationKind,
    input: &[u8],
    limits: OperationLimits,
) -> Result<(), OperationCodecError> {
    decode_body::<false, false>(kind, input, limits).map(|_| ())
}

/// Decode exactly one typed body without copying text or application payloads.
///
/// Append decoding allocates output batch/record vectors. Records with at most
/// two parts keep their part slices inline; larger multipart records also
/// allocate an output part vector. No per-record descriptor scratch is allocated.
pub fn decode_operation_body(
    kind: OperationKind,
    input: &[u8],
    limits: OperationLimits,
) -> Result<OperationBody<'_>, OperationCodecError> {
    decode_body::<true, true>(kind, input, limits)
}

/// Validate an append and return only metadata needed for state admission.
///
/// Uses the same bounded schema walker as full decoding, including every record
/// and part descriptor, cumulative limits, overflow, and trailing-byte checks.
/// No record/part vectors or payload copies are made. More than four partition
/// batches allocate summary storage; the ordinary single-partition path does not.
pub fn decode_append_summary(
    input: &[u8],
    limits: OperationLimits,
) -> Result<AppendSummary, OperationCodecError> {
    decode_append_summary_and_view(input, limits).map(|(summary, _)| summary)
}

/// Validate append metadata once and retain a borrowed view for derived indexes.
/// Both outputs describe the same completely validated immutable operation.
pub fn decode_append_summary_and_view(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(AppendSummary, AppendView<'_>), OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Decoder::new(input);
    let count = decoder.count("append batch count", limits.max_append_batches)?;
    if count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    let mut batches = SmallVec::with_capacity(count);
    let mut totals = AppendTotals::default();
    for _ in 0..count {
        let (_, view) =
            decode_append_batch::<false, true>(&mut decoder, limits, &mut totals, None)?;
        batches.push(view.summary);
    }
    decoder.finish()?;
    Ok((
        AppendSummary { batches },
        AppendView::validated(input, limits),
    ))
}

/// Validate once and retain batch boundaries for immediate index construction.
/// Both outputs describe the same immutable bytes. Up to four batches allocate
/// no heap storage; record tables and payloads remain borrowed.
pub fn decode_append_summary_and_batches(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(AppendSummary, SmallVec<[AppendBatchView<'_>; 4]>), OperationCodecError> {
    let views = decode_append_batches(input, limits)?;
    let summary = AppendSummary {
        batches: views.iter().map(|view| view.summary).collect(),
    };
    Ok((summary, views))
}

/// Validate once, retaining batch boundaries and every record's byte position
/// for an owner's read index, so admission never walks the records again.
/// Positions are appended per batch in canonical order, each batch ending
/// with an end checkpoint.
pub fn decode_append_summary_and_batches_indexed<'a>(
    input: &'a [u8],
    limits: OperationLimits,
    positions: &mut Vec<RecordPosition>,
) -> Result<(AppendSummary, SmallVec<[AppendBatchView<'a>; 4]>), OperationCodecError> {
    let views = decode_append_batches_indexed(input, limits, positions)?;
    let summary = AppendSummary {
        batches: views.iter().map(|view| view.summary).collect(),
    };
    Ok((summary, views))
}

/// Validate descriptors and retain batch boundaries without revalidating a
/// whole-payload codec already checked before canonical body construction.
pub fn decode_append_summary_and_batches_with_validated_payload(
    input: &[u8],
    limits: OperationLimits,
) -> Result<(AppendSummary, SmallVec<[AppendBatchView<'_>; 4]>), OperationCodecError> {
    let views = decode_append_batches_with_validated_payload(input, limits)?;
    let summary = AppendSummary {
        batches: views.iter().map(|view| view.summary).collect(),
    };
    Ok((summary, views))
}

// Validation and materialization share every schema check. The false variant
// never exposes its empty append vectors outside this module.
fn decode_body<const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    kind: OperationKind,
    input: &[u8],
    limits: OperationLimits,
) -> Result<OperationBody<'_>, OperationCodecError> {
    validate_limits(limits)?;
    enforce_limit("operation body bytes", input.len(), limits.max_body_bytes)?;
    let mut decoder = Decoder::new(input);
    let body = match kind {
        OperationKind::CreatePartition => {
            OperationBody::CreatePartition(decode_create(&mut decoder, limits)?)
        }
        OperationKind::OpenProducer => {
            OperationBody::OpenProducer(decode_open_producer(&mut decoder)?)
        }
        OperationKind::Append => OperationBody::Append(decode_append::<
            MATERIALIZE,
            VALIDATE_PREPARED,
        >(&mut decoder, limits)?),
        OperationKind::Progress => OperationBody::Progress(decode_progress(&mut decoder)?),
        OperationKind::Assign => OperationBody::Assign(decode_assign(&mut decoder)?),
        OperationKind::Trim => OperationBody::Trim(decode_trim(&mut decoder)?),
        OperationKind::PartitionPolicy => {
            OperationBody::PartitionPolicy(decode_policy(&mut decoder)?)
        }
        OperationKind::Barrier => OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes(decoder.id()?),
        }),
        OperationKind::ProducerResultFloor => {
            OperationBody::ProducerResultFloor(decode_producer_result_floor(&mut decoder)?)
        }
    };
    decoder.finish()?;
    Ok(body)
}

fn encode_create(
    value: &CreatePartition<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    validate_name("stream", value.stream, limits.max_name_bytes)?;
    validate_name("topic", value.topic, limits.max_name_bytes)?;
    encoder.id(value.partition.as_bytes())?;
    encoder.text(value.stream)?;
    encoder.text(value.topic)?;
    encoder.u32(value.partition_id.get())?;
    encoder.u64(value.owner_epoch.get())?;
    encode_retention(value.retention, encoder)
}

fn decode_create<'a>(
    decoder: &mut Decoder<'a>,
    limits: OperationLimits,
) -> Result<CreatePartition<'a>, OperationCodecError> {
    let partition = PartitionIncarnation::from_bytes(decoder.id()?);
    let stream = decoder.text("stream", limits.max_name_bytes)?;
    let topic = decoder.text("topic", limits.max_name_bytes)?;
    Ok(CreatePartition {
        partition,
        stream,
        topic,
        partition_id: PartitionId::new(decoder.u32()?),
        owner_epoch: OwnerEpoch::new(decoder.u64()?),
        retention: decode_retention(decoder)?,
    })
}

fn encode_open_producer(
    value: OpenProducer,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.id(value.producer_id.as_bytes())?;
    encoder.optional_u64(value.expected_epoch.map(ProducerEpoch::get))?;
    encoder.u64(value.new_epoch.get())?;
    encoder.id(value.operation_id.as_bytes())
}

fn decode_open_producer(decoder: &mut Decoder<'_>) -> Result<OpenProducer, OperationCodecError> {
    Ok(OpenProducer {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        producer_id: ProducerId::from_bytes(decoder.id()?),
        expected_epoch: decoder.optional_u64()?.map(ProducerEpoch::new),
        new_epoch: ProducerEpoch::new(decoder.u64()?),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

fn encode_append(
    value: &Append<'_>,
    limits: OperationLimits,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    if value.batches.is_empty() {
        return Err(OperationCodecError::EmptyAppend);
    }
    enforce_limit(
        "append batch count",
        value.batches.len(),
        limits.max_append_batches,
    )?;
    encoder.count(value.batches.len())?;
    let mut totals = AppendTotals::default();
    for batch in &value.batches {
        encode_append_batch(batch, limits, &mut totals, encoder)?;
    }
    Ok(())
}

fn encode_append_batch(
    batch: &AppendBatch<'_>,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    if batch.records.is_packed() {
        AppendHeader::from(batch).encode(
            batch.records.len(),
            true,
            false,
            limits,
            totals,
            encoder,
        )?;
        totals.add_parts(batch.records.len(), limits)?;
        for (ids, _, _) in batch.records.packed_slices().expect("packed list") {
            for id in ids {
                encoder.id(id.as_bytes())?;
            }
        }
        for (_, lengths, payload) in batch.records.packed_slices().expect("packed list") {
            totals.add_payload(payload.len(), limits)?;
            encoder.bytes(lengths)?;
        }
        for (_, _, payload) in batch.records.packed_slices().expect("packed list") {
            encoder.payload(payload)?;
        }
        return Ok(());
    }
    let records = batch
        .records
        .iter()
        .map(|r| (r.message_id, r.encoding, r.parts.iter()));
    let tiny = records_are_tiny(records.clone());
    AppendHeader::from(batch).encode(batch.records.len(), tiny, false, limits, totals, encoder)?;
    if !tiny && let Some(chunks) = batch.records.encoded_slices() {
        // Native and canonical general descriptors have identical bytes.
        // Frames are immutable and validated; enforce this operation's possibly
        // tighter limits without rebuilding descriptors or per-record owners.
        for chunk in chunks.clone() {
            for record in chunk.iter() {
                if !record.encoding.validate(
                    record.parts.len(),
                    limits.max_parts,
                    limits.max_payload_bytes,
                ) {
                    return Err(OperationCodecError::EmptyRecordParts);
                }
                totals.add_parts(record.parts.len(), limits)?;
            }
            totals.add_payload(chunk.payload_bytes(), limits)?;
            encoder.bytes(chunk.encoded().0)?;
        }
        for chunk in chunks {
            encoder.payload(chunk.encoded().1)?;
        }
        return Ok(());
    }
    encode_append_records(records, tiny, limits, totals, encoder)
}

fn records_are_tiny<'a, P: ExactSizeIterator<Item = &'a [u8]>>(
    records: impl Iterator<Item = (MessageId, ozzy_proto::data::Encoding, P)>,
) -> bool {
    records.into_iter().all(|(_, encoding, mut parts)| {
        encoding == ozzy_proto::data::Encoding::Raw
            && parts.len() == 1
            && u8::try_from(parts.next().unwrap().len()).is_ok()
    })
}

fn encode_append_records<'a, P: ExactSizeIterator<Item = &'a [u8]>>(
    records: impl Iterator<Item = (MessageId, ozzy_proto::data::Encoding, P)> + Clone,
    tiny: bool,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    for (id, encoding, mut parts) in records.clone() {
        if !encoding.validate(parts.len(), limits.max_parts, limits.max_payload_bytes) {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        totals.add_parts(parts.len(), limits)?;
        if tiny {
            encoder.id(id.as_bytes())?;
        } else {
            let mut descriptor = [0; 24];
            descriptor[..16].copy_from_slice(id.as_bytes());
            let count =
                u32::try_from(parts.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
            descriptor[16..20].copy_from_slice(&(count | encoding.tag()).to_be_bytes());
            if parts.len() == 1 && encoding == ozzy_proto::data::Encoding::Raw {
                let part = parts.next().unwrap();
                totals.add_payload(part.len(), limits)?;
                let len =
                    u32::try_from(part.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
                descriptor[20..].copy_from_slice(&len.to_be_bytes());
                encoder.bytes(&descriptor)?;
            } else {
                encoder.bytes(&descriptor[..20])?;
                if let ozzy_proto::data::Encoding::Lz4 { decoded_bytes } = encoding {
                    encoder.u32(decoded_bytes)?;
                }
                for part in parts {
                    totals.add_payload(part.len(), limits)?;
                    encoder.length(part.len())?;
                }
            }
        }
    }
    if tiny {
        for (_, _, mut parts) in records.clone() {
            let part = parts.next().expect("single-part compact record");
            totals.add_payload(part.len(), limits)?;
            encoder
                .u8(u8::try_from(part.len()).map_err(|_| OperationCodecError::LengthOverflow)?)?;
        }
    }
    for (_, _, parts) in records {
        for part in parts {
            encoder.payload(part)?;
        }
    }
    Ok(())
}

fn decode_append<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    decoder: &mut Decoder<'a>,
    limits: OperationLimits,
) -> Result<Append<'a>, OperationCodecError> {
    let batch_count = decoder.count("append batch count", limits.max_append_batches)?;
    if batch_count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    let mut batches = if MATERIALIZE {
        Vec::with_capacity(batch_count)
    } else {
        Vec::new()
    };
    let mut totals = AppendTotals::default();
    for _ in 0..batch_count {
        let (batch, _) = decode_append_batch::<MATERIALIZE, VALIDATE_PREPARED>(
            decoder,
            limits,
            &mut totals,
            None,
        )?;
        if MATERIALIZE {
            batches.push(batch);
        }
    }
    Ok(Append { batches })
}

fn decode_append_batch<'a, const MATERIALIZE: bool, const VALIDATE_PREPARED: bool>(
    decoder: &mut Decoder<'a>,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(AppendBatch<'a>, AppendBatchView<'a>), OperationCodecError> {
    let partition = PartitionIncarnation::from_bytes(decoder.id()?);
    let owner_epoch = OwnerEpoch::new(decoder.u64()?);
    let producer_id = ProducerId::from_bytes(decoder.id()?);
    let producer_epoch = ProducerEpoch::new(decoder.u64()?);
    let first_sequence = ProducerSequence::new(decoder.u64()?);
    let first_offset = Offset::new(decoder.u64()?);
    let append_timestamp_millis = decoder.u64()?;
    let count_and_layout = decoder.u32()?;
    let tiny = count_and_layout & TINY_RECORDS != 0;
    let prepared = count_and_layout & PREPARED_PAYLOAD != 0;
    if tiny && prepared {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let record_count = (count_and_layout & RECORD_COUNT_MASK) as usize;
    enforce_limit("append record count", record_count, limits.max_records)?;
    if record_count == 0 {
        return Err(OperationCodecError::EmptyAppend);
    }
    totals.add_records(record_count, limits)?;
    validate_position_count(first_sequence.get(), record_count)?;
    validate_position_count(first_offset.get(), record_count)?;

    // The canonical descriptor table already stores every ID and part length.
    // Validate it once, then reread those immutable bytes only when constructing
    // output records. This avoids a temporary Vec for every record's lengths.
    let descriptors = *decoder;
    let (payload_bytes, nonzero_message_ids, all_raw, lengths) = if tiny {
        let ids = decoder.take(
            record_count
                .checked_mul(16)
                .ok_or(OperationCodecError::LengthOverflow)?,
        )?;
        let lengths = decoder.take(record_count)?;
        let (bytes, nonzero) =
            validate_tiny_descriptors(ids, lengths, record_count, limits, totals, positions)?;
        (bytes, nonzero, true, Some(lengths))
    } else {
        let (bytes, nonzero, all_raw) =
            validate_append_descriptors(decoder, record_count, limits, totals, positions)?;
        (bytes, nonzero, all_raw, None)
    };
    let descriptor_end = decoder.offset - lengths.map_or(0, <[u8]>::len);
    let descriptor_bytes = &decoder.input[descriptors.offset..descriptor_end];
    if prepared && !all_raw {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    let payload = if prepared {
        None
    } else {
        Some(decoder.take(payload_bytes)?)
    };
    let prepared_payload = prepared
        .then(|| decode_prepared_payload::<VALIDATE_PREPARED>(decoder, payload_bytes))
        .transpose()?;
    let summary = AppendBatchSummary {
        partition,
        owner_epoch,
        producer_id,
        producer_epoch,
        first_sequence,
        first_offset,
        record_count,
        nonzero_message_ids,
    };
    let view = AppendBatchView {
        summary,
        append_timestamp_millis,
        prepared_payload,
        records: payload.map(|payload| {
            AppendRecords::with_lengths(descriptor_bytes, lengths, payload, record_count)
        }),
        descriptors: AppendRecordDescriptors::new(descriptor_bytes, lengths, record_count),
    };
    let records = if MATERIALIZE {
        if let Some(prepared) = view.prepared_payload {
            materialize_prepared_records(view.descriptors(), prepared)?
        } else {
            view.records()
                .map(|record| AppendRecord {
                    encoding: record.encoding,
                    message_id: record.message_id,
                    parts: record.parts.collect(),
                })
                .collect()
        }
    } else {
        AppendRecordList::from(Vec::new())
    };
    Ok((
        AppendBatch {
            partition,
            owner_epoch,
            producer_id,
            producer_epoch,
            first_sequence,
            first_offset,
            append_timestamp_millis,
            records,
        },
        view,
    ))
}

fn decode_prepared_payload<'a, const VALIDATE: bool>(
    decoder: &mut Decoder<'a>,
    expected_decoded_bytes: usize,
) -> Result<PreparedAppendPayload<'a>, OperationCodecError> {
    let encoding = ozzy_proto::append::PayloadEncoding::try_from(decoder.u8()?)
        .map_err(|_| OperationCodecError::InvalidPreparedPayload)?;
    let decoded_bytes = decoder.u32()? as usize;
    let encoded_bytes = decoder.u32()? as usize;
    let encoded = decoder.take(encoded_bytes)?;
    if encoding != ozzy_proto::append::PayloadEncoding::Lz4
        || decoded_bytes != expected_decoded_bytes
        || encoded.is_empty()
    {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    if VALIDATE {
        lz4rip::block::validate_block(encoded, decoded_bytes)
            .map_err(|_| OperationCodecError::InvalidPreparedPayload)?;
    }
    Ok(PreparedAppendPayload {
        encoding,
        decoded_bytes,
        encoded,
    })
}

fn materialize_prepared_records<'a>(
    descriptors: AppendRecordDescriptors<'_>,
    prepared: PreparedAppendPayload<'_>,
) -> Result<AppendRecordList<'a>, OperationCodecError> {
    let payload = Bytes::from(
        lz4rip::block::decompress(prepared.encoded, prepared.decoded_bytes)
            .map_err(|_| OperationCodecError::InvalidPreparedPayload)?,
    );
    let mut offset = 0_usize;
    let mut records = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        if descriptor.encoding != ozzy_proto::data::Encoding::Raw {
            return Err(OperationCodecError::InvalidPreparedPayload);
        }
        let mut parts = SmallVec::new();
        for length in descriptor.part_lengths {
            let end = offset
                .checked_add(length)
                .filter(|&end| end <= payload.len())
                .ok_or(OperationCodecError::InvalidPreparedPayload)?;
            parts.push(payload.slice(offset..end));
            offset = end;
        }
        records.push(ozzy_proto::data::OwnedRecord {
            encoding: descriptor.encoding,
            message_id: descriptor.message_id,
            payload: parts,
        });
    }
    if offset != payload.len() {
        return Err(OperationCodecError::InvalidPreparedPayload);
    }
    Ok(AppendRecordList::owned(records))
}

fn validate_tiny_descriptors(
    ids: &[u8],
    lengths: &[u8],
    count: usize,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    mut positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(usize, bool), OperationCodecError> {
    if ids.len()
        != count
            .checked_mul(16)
            .ok_or(OperationCodecError::LengthOverflow)?
        || lengths.len() != count
    {
        return Err(OperationCodecError::LengthOverflow);
    }
    totals.add_parts(count, limits)?;
    let mut bytes = 0_usize;
    for (index, &len) in lengths.iter().enumerate() {
        if let Some(positions) = positions.as_deref_mut() {
            positions.push(position(index * 16, bytes, 1)?);
        }
        bytes = bytes
            .checked_add(len as usize)
            .ok_or(OperationCodecError::LengthOverflow)?;
    }
    if let Some(positions) = positions {
        positions.push(position(ids.len(), bytes, 0)?);
    }
    totals.add_payload(bytes, limits)?;
    Ok((
        bytes,
        ids.as_chunks::<16>().0.iter().all(|id| id != &[0; 16]),
    ))
}

fn position(
    descriptor: usize,
    payload: usize,
    parts: usize,
) -> Result<RecordPosition, OperationCodecError> {
    Ok(RecordPosition {
        descriptor: u32::try_from(descriptor).map_err(|_| OperationCodecError::LengthOverflow)?,
        payload: u32::try_from(payload).map_err(|_| OperationCodecError::LengthOverflow)?,
        parts: u32::try_from(parts).map_err(|_| OperationCodecError::LengthOverflow)?,
    })
}

fn validate_append_descriptors(
    decoder: &mut Decoder<'_>,
    record_count: usize,
    limits: OperationLimits,
    totals: &mut AppendTotals,
    mut positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(usize, bool, bool), OperationCodecError> {
    let mut batch_payload_bytes = 0_usize;
    let mut nonzero_message_ids = true;
    let mut all_raw = true;
    let table_start = decoder.offset;
    for _ in 0..record_count {
        let descriptor_start = decoder.offset - table_start;
        let payload_start = batch_payload_bytes;
        nonzero_message_ids &= decoder.id()? != [0; 16];
        let tagged = decoder.u32()?;
        let part_count = (tagged & 0x00ff_ffff) as usize;
        let encoding = match tagged >> 24 {
            0 => ozzy_proto::data::Encoding::Raw,
            1 => ozzy_proto::data::Encoding::Lz4 {
                decoded_bytes: decoder.u32()?,
            },
            _ => return Err(OperationCodecError::EmptyRecordParts),
        };
        all_raw &= encoding == ozzy_proto::data::Encoding::Raw;
        if !encoding.validate(part_count, limits.max_parts, limits.max_payload_bytes) {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        if part_count == 0 {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        totals.add_parts(part_count, limits)?;
        for _ in 0..part_count {
            let length = decoder.u32()? as usize;
            totals.add_payload(length, limits)?;
            batch_payload_bytes = batch_payload_bytes
                .checked_add(length)
                .ok_or(OperationCodecError::LengthOverflow)?;
        }
        if let Some(positions) = positions.as_deref_mut() {
            positions.push(position(descriptor_start, payload_start, part_count)?);
        }
    }
    if let Some(positions) = positions {
        positions.push(position(
            decoder.offset - table_start,
            batch_payload_bytes,
            0,
        )?);
    }
    Ok((batch_payload_bytes, nonzero_message_ids, all_raw))
}

fn encode_progress(
    value: Progress,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    match value.owner {
        ProgressOwner::Subscription(id) => {
            if value.assignment_epoch.is_some() {
                return Err(OperationCodecError::UnexpectedAssignmentEpoch);
            }
            encoder.u8(1)?;
            encoder.id(id.as_bytes())?;
        }
        ProgressOwner::ConsumerGroup(id) => {
            if value.assignment_epoch.is_none() {
                return Err(OperationCodecError::MissingAssignmentEpoch);
            }
            encoder.u8(2)?;
            encoder.id(id.as_bytes())?;
        }
    }
    encoder.id(value.partition.as_bytes())?;
    encoder.optional_u64(value.assignment_epoch)?;
    encoder.optional_u64(value.expected_progress.map(Offset::get))?;
    encoder.u64(value.new_progress.get())?;
    encoder.id(value.operation_id.as_bytes())
}

fn decode_progress(decoder: &mut Decoder<'_>) -> Result<Progress, OperationCodecError> {
    let owner = match decoder.u8()? {
        1 => ProgressOwner::Subscription(SubscriptionId::from_bytes(decoder.id()?)),
        2 => ProgressOwner::ConsumerGroup(ConsumerGroupId::from_bytes(decoder.id()?)),
        tag => return Err(OperationCodecError::InvalidProgressOwnerTag(tag)),
    };
    let partition = PartitionIncarnation::from_bytes(decoder.id()?);
    let assignment_epoch = decoder.optional_u64()?;
    match owner {
        ProgressOwner::Subscription(_) if assignment_epoch.is_some() => {
            return Err(OperationCodecError::UnexpectedAssignmentEpoch);
        }
        ProgressOwner::ConsumerGroup(_) if assignment_epoch.is_none() => {
            return Err(OperationCodecError::MissingAssignmentEpoch);
        }
        _ => {}
    }
    Ok(Progress {
        owner,
        partition,
        assignment_epoch,
        expected_progress: decoder.optional_u64()?.map(Offset::new),
        new_progress: Offset::new(decoder.u64()?),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

fn encode_assign(
    value: Assign,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.consumer_group_id.as_bytes())?;
    encoder.id(value.partition.as_bytes())?;
    encoder.u64(value.expected_assignment_epoch)?;
    encoder.u64(value.new_assignment_epoch)?;
    encoder.optional_id(value.new_member.map(|id| *id.as_bytes()))?;
    encoder.id(value.operation_id.as_bytes())
}

fn decode_assign(decoder: &mut Decoder<'_>) -> Result<Assign, OperationCodecError> {
    Ok(Assign {
        consumer_group_id: ConsumerGroupId::from_bytes(decoder.id()?),
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        expected_assignment_epoch: decoder.u64()?,
        new_assignment_epoch: decoder.u64()?,
        new_member: decoder.optional_id()?.map(ConsumerMemberId::from_bytes),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

fn encode_trim(
    value: Trim,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.u64(value.expected_floor.get())?;
    encoder.u64(value.new_floor.get())?;
    encoder.id(value.operation_id.as_bytes())
}

fn decode_trim(decoder: &mut Decoder<'_>) -> Result<Trim, OperationCodecError> {
    Ok(Trim {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        expected_floor: Offset::new(decoder.u64()?),
        new_floor: Offset::new(decoder.u64()?),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

fn encode_policy(
    value: PartitionPolicy,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.u64(value.expected_revision)?;
    encoder.u64(value.new_revision)?;
    encode_retention(value.retention, encoder)?;
    encoder.id(value.operation_id.as_bytes())
}

fn decode_policy(decoder: &mut Decoder<'_>) -> Result<PartitionPolicy, OperationCodecError> {
    Ok(PartitionPolicy {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        expected_revision: decoder.u64()?,
        new_revision: decoder.u64()?,
        retention: decode_retention(decoder)?,
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

fn encode_producer_result_floor(
    value: ProducerResultFloor,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    encoder.id(value.partition.as_bytes())?;
    encoder.id(value.producer_id.as_bytes())?;
    encoder.u64(value.producer_epoch.get())?;
    encoder.u64(value.expected_floor.get())?;
    encoder.u64(value.new_floor.get())?;
    encoder.id(value.operation_id.as_bytes())
}

fn decode_producer_result_floor(
    decoder: &mut Decoder<'_>,
) -> Result<ProducerResultFloor, OperationCodecError> {
    Ok(ProducerResultFloor {
        partition: PartitionIncarnation::from_bytes(decoder.id()?),
        producer_id: ProducerId::from_bytes(decoder.id()?),
        producer_epoch: ProducerEpoch::new(decoder.u64()?),
        expected_floor: ProducerSequence::new(decoder.u64()?),
        new_floor: ProducerSequence::new(decoder.u64()?),
        operation_id: OperationId::from_bytes(decoder.id()?),
    })
}

fn encode_retention(
    policy: RetentionPolicy,
    encoder: &mut Encoder<'_, impl OperationOutput>,
) -> Result<(), OperationCodecError> {
    let mut flags = 0_u8;
    if policy.max_age_millis.is_some() {
        flags |= RETENTION_MAX_AGE;
    }
    if policy.max_bytes.is_some() {
        flags |= RETENTION_MAX_BYTES;
    }
    encoder.u8(RETENTION_VERSION)?;
    encoder.u8(flags)?;
    encoder.u16(0)?;
    encoder.u64(policy.max_age_millis.map_or(0, NonZeroU64::get))?;
    encoder.u64(policy.max_bytes.map_or(0, NonZeroU64::get))
}

fn decode_retention(decoder: &mut Decoder<'_>) -> Result<RetentionPolicy, OperationCodecError> {
    let version = decoder.u8()?;
    let flags = decoder.u8()?;
    let reserved = decoder.u16()?;
    let age = decoder.u64()?;
    let bytes = decoder.u64()?;
    if version != RETENTION_VERSION || flags & !RETENTION_KNOWN_FLAGS != 0 || reserved != 0 {
        return Err(OperationCodecError::InvalidRetention);
    }
    let max_age_millis = retention_value(flags, RETENTION_MAX_AGE, age)?;
    let max_bytes = retention_value(flags, RETENTION_MAX_BYTES, bytes)?;
    Ok(RetentionPolicy {
        max_age_millis,
        max_bytes,
    })
}

fn retention_value(
    flags: u8,
    flag: u8,
    value: u64,
) -> Result<Option<NonZeroU64>, OperationCodecError> {
    match (flags & flag != 0, NonZeroU64::new(value)) {
        (false, None) => Ok(None),
        (true, Some(value)) => Ok(Some(value)),
        _ => Err(OperationCodecError::InvalidRetention),
    }
}

fn validate_position_count(first: u64, count: usize) -> Result<(), OperationCodecError> {
    let delta = u64::try_from(count - 1).map_err(|_| OperationCodecError::LengthOverflow)?;
    first
        .checked_add(delta)
        .ok_or(OperationCodecError::AppendPositionOverflow)?;
    Ok(())
}

fn validate_name(kind: &'static str, value: &str, max: usize) -> Result<(), OperationCodecError> {
    if value.is_empty() {
        return Err(OperationCodecError::EmptyValue(kind));
    }
    enforce_limit(kind, value.len(), max)
}

fn validate_limits(limits: OperationLimits) -> Result<(), OperationCodecError> {
    for (kind, value) in [
        ("operation body bytes", limits.max_body_bytes),
        ("name bytes", limits.max_name_bytes),
        ("append batch count", limits.max_append_batches),
        ("append record count", limits.max_records),
        ("append part count", limits.max_parts),
        ("append payload bytes", limits.max_payload_bytes),
    ] {
        if value == 0 {
            return Err(OperationCodecError::LimitExceeded {
                kind,
                actual: 0,
                limit: 0,
            });
        }
    }
    Ok(())
}

fn enforce_limit(
    kind: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), OperationCodecError> {
    if actual > limit {
        Err(OperationCodecError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct AppendTotals {
    records: usize,
    parts: usize,
    payload_bytes: usize,
}

impl AppendTotals {
    fn add_records(
        &mut self,
        count: usize,
        limits: OperationLimits,
    ) -> Result<(), OperationCodecError> {
        self.records = self
            .records
            .checked_add(count)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit("append record count", self.records, limits.max_records)
    }

    fn add_parts(
        &mut self,
        count: usize,
        limits: OperationLimits,
    ) -> Result<(), OperationCodecError> {
        self.parts = self
            .parts
            .checked_add(count)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit("append part count", self.parts, limits.max_parts)
    }

    fn add_payload(
        &mut self,
        bytes: usize,
        limits: OperationLimits,
    ) -> Result<(), OperationCodecError> {
        self.payload_bytes = self
            .payload_bytes
            .checked_add(bytes)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit(
            "append payload bytes",
            self.payload_bytes,
            limits.max_payload_bytes,
        )
    }
}

#[derive(Debug)]
struct Encoder<'a, O: OperationOutput> {
    output: &'a mut O,
    start: usize,
    limit: usize,
}

impl<'a, O: OperationOutput> Encoder<'a, O> {
    fn new(output: &'a mut O, start: usize, limit: usize) -> Self {
        Self {
            output,
            start,
            limit,
        }
    }

    fn finish(self) -> usize {
        self.output.len()
    }

    fn restore_start(&mut self) {
        self.output.truncate(self.start);
    }

    // Metadata fields have compile-time lengths. Keep those lengths visible to
    // the output fast path instead of making an out-of-line memcpy per field.
    #[inline]
    fn bytes(&mut self, bytes: &[u8]) -> Result<(), OperationCodecError> {
        self.check_length(bytes.len())?;
        self.output.extend(bytes)
    }

    fn payload(&mut self, bytes: &[u8]) -> Result<(), OperationCodecError> {
        self.check_length(bytes.len())?;
        self.output.payload(bytes)
    }

    fn check_length(&self, additional: usize) -> Result<(), OperationCodecError> {
        let new_len = self
            .output
            .len()
            .checked_add(additional)
            .ok_or(OperationCodecError::LengthOverflow)?;
        let body_len = new_len
            .checked_sub(self.start)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit("operation body bytes", body_len, self.limit)?;
        Ok(())
    }

    fn u8(&mut self, value: u8) -> Result<(), OperationCodecError> {
        self.bytes(&[value])
    }

    fn u16(&mut self, value: u16) -> Result<(), OperationCodecError> {
        self.bytes(&value.to_be_bytes())
    }

    fn u32(&mut self, value: u32) -> Result<(), OperationCodecError> {
        self.bytes(&value.to_be_bytes())
    }

    fn u64(&mut self, value: u64) -> Result<(), OperationCodecError> {
        self.bytes(&value.to_be_bytes())
    }

    fn count(&mut self, value: usize) -> Result<(), OperationCodecError> {
        self.u32(u32::try_from(value).map_err(|_| OperationCodecError::LengthOverflow)?)
    }

    fn length(&mut self, value: usize) -> Result<(), OperationCodecError> {
        self.count(value)
    }

    fn id(&mut self, value: &[u8; 16]) -> Result<(), OperationCodecError> {
        self.bytes(value)
    }

    fn text(&mut self, value: &str) -> Result<(), OperationCodecError> {
        self.length(value.len())?;
        self.bytes(value.as_bytes())
    }

    fn optional_u64(&mut self, value: Option<u64>) -> Result<(), OperationCodecError> {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1)?;
                self.u64(value)
            }
        }
    }

    fn optional_id(&mut self, value: Option<[u8; 16]>) -> Result<(), OperationCodecError> {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1)?;
                self.id(&value)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn finish(self) -> Result<(), OperationCodecError> {
        let remaining = self.input.len() - self.offset;
        if remaining == 0 {
            Ok(())
        } else {
            Err(OperationCodecError::TrailingBytes(remaining))
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], OperationCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(OperationCodecError::LengthOverflow)?;
        let Some(bytes) = self.input.get(self.offset..end) else {
            return Err(OperationCodecError::Truncated {
                needed: end,
                available: self.input.len(),
            });
        };
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, OperationCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, OperationCodecError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, OperationCodecError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, OperationCodecError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn id(&mut self) -> Result<[u8; 16], OperationCodecError> {
        let mut id = [0; 16];
        id.copy_from_slice(self.take(16)?);
        Ok(id)
    }

    fn count(&mut self, kind: &'static str, limit: usize) -> Result<usize, OperationCodecError> {
        let count = self.u32()? as usize;
        enforce_limit(kind, count, limit)?;
        Ok(count)
    }

    fn text(&mut self, kind: &'static str, limit: usize) -> Result<&'a str, OperationCodecError> {
        let length = self.count(kind, limit)?;
        let bytes = self.take(length)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| OperationCodecError::InvalidUtf8(kind))?;
        validate_name(kind, value, limit)?;
        Ok(value)
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, OperationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.u64().map(Some),
            tag => Err(OperationCodecError::InvalidOptionTag(tag)),
        }
    }

    fn optional_id(&mut self) -> Result<Option<[u8; 16]>, OperationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.id().map(Some),
            tag => Err(OperationCodecError::InvalidOptionTag(tag)),
        }
    }
}
