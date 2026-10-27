//! Deterministic canonical-operation state transitions.

use ahash::AHashMap as HashMap;
use smallvec::SmallVec;

use ozzy_journal::operation::{ProgressOwner, RetentionPolicy};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};
use thiserror::Error;

mod admission;
mod groups;
pub use groups::{CoordinatorGroup, group_targets};
mod append;
mod delay;
pub use delay::{DelayState, PendingSchedule};
mod identity;
mod images;
mod installation;
pub use identity::{
    IdentityClaim, IdentityIndex, IdentityIndexError, IdentityKey, MemoryIdentityIndex,
};
mod producer;
mod snapshot;

use producer::PreparedProducerResults;
pub use producer::{
    CanonicalProducer, ProducerResultSource, ProducerResultSpan, ProducerTransition,
};

pub use images::{
    CanonicalImages, CanonicalImagesError, CanonicalRecovery, PreparedCanonicalGroup,
    RecoveredImages,
};
pub use snapshot::{
    PENDING_SCHEDULE_SNAPSHOT_BYTES, STATE_SNAPSHOT_HEADER_BYTES, StateSnapshotError,
    StateSnapshotLimits, canonical_state_schema_digest,
};

/// Bounds for live application metadata. Historical identities live in indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateLimits {
    /// Maximum live partition incarnations.
    pub max_partitions: usize,
    /// Maximum live producer sessions across partitions.
    pub max_producers: usize,
    /// Maximum retained producer sequence-to-offset spans.
    pub max_retry_spans: usize,
    /// Maximum individual or group progress entries.
    pub max_progress: usize,
    /// Maximum consumer-group partition assignments.
    pub max_assignments: usize,
    /// Maximum durable coordinator group incarnations.
    pub max_groups: usize,
    /// Maximum members across coordinator groups.
    pub max_group_members: usize,
    /// Maximum source intents across coordinator groups.
    pub max_group_partitions: usize,
    /// Maximum held scheduled records per partition.
    pub max_pending_schedules: usize,
}

impl Default for StateLimits {
    fn default() -> Self {
        Self {
            max_partitions: 65_536,
            max_producers: 1_048_576,
            max_retry_spans: 1_048_576,
            max_progress: 1_048_576,
            max_assignments: 1_048_576,
            max_groups: 65_536,
            max_group_members: 1_048_576,
            max_group_partitions: 1_048_576,
            max_pending_schedules: 1_048_576,
        }
    }
}

/// Immutable logical address bound to one partition incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionAddress {
    /// Logical stream namespace.
    pub stream: String,
    /// Logical topic name.
    pub topic: String,
    /// Partition number within the topic.
    pub partition_id: PartitionId,
}

/// Compact live state reconstructed for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalPartition {
    /// Immutable logical address for this incarnation.
    pub address: PartitionAddress,
    /// Current owner fence for this partition.
    pub owner_epoch: OwnerEpoch,
    producers: HashMap<ProducerId, CanonicalProducer>,
    /// Next partition-global record offset.
    pub next_offset: Offset,
    /// Earliest offset retained by the current policy.
    pub retained_from: Offset,
    /// Current retention-policy revision.
    pub policy_revision: u64,
    /// Installed retention policy.
    pub retention: RetentionPolicy,
    /// Immutable first-wins event window; absent means deduplication is disabled.
    pub dedup_window_millis: Option<std::num::NonZeroU64>,
    /// Confirmed or speculative replicated deduplication clock watermark.
    pub dedup_time: u64,
    /// Highest epoch of any retired producer session, zero before the first.
    /// A recreated session starts above it, so no retired epoch returns.
    pub retired_epoch: u64,
    /// Held scheduled records; absent unless the partition enables delay.
    pub delay: Option<DelayState>,
}

impl CanonicalPartition {
    /// Look up one partition-local producer session.
    pub fn producer(&self, producer: ProducerId) -> Option<&CanonicalProducer> {
        self.producers.get(&producer)
    }

    /// The open producer session a validated plan names.
    fn producer_mut(&mut self, producer: ProducerId) -> Result<&mut CanonicalProducer, StateError> {
        self.producers
            .get_mut(&producer)
            .ok_or(StateError::ProducerMismatch)
    }

    /// Iterate partition-local producer sessions in unspecified order.
    pub fn producers(&self) -> impl ExactSizeIterator<Item = (ProducerId, &CanonicalProducer)> {
        self.producers.iter().map(|(id, state)| (*id, state))
    }

    /// Earliest offset whose producer retry result remains queryable.
    pub fn producer_result_offset_floor(&self) -> Option<Offset> {
        Some(
            self.producers
                .values()
                .filter_map(CanonicalProducer::result_offset_floor)
                .min()
                .unwrap_or(self.next_offset),
        )
    }
}

/// Current consumer-group assignment for one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssignmentState {
    /// Current assignment fence.
    pub epoch: u64,
    /// Latest coordinator plan applied to this source partition.
    pub plan_revision: u64,
    /// Exclusive cursor captured with this assignment, unchanged by later progress.
    pub resume_cursor: Offset,
    /// Assigned member, or no owner when revoked.
    pub member: Option<ConsumerMemberId>,
}

/// Infallible state mutation prepared against one exact state revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionPlan {
    expected_revision: u64,
    op_number: u64,
    mutation: Mutation,
    claims: Vec<IdentityClaim>,
}

impl TransitionPlan {
    /// Canonical operation number bound to this prepared transition.
    pub const fn op_number(&self) -> u64 {
        self.op_number
    }

    /// Control identities reserved by this prepared transition.
    pub fn identity_claims(&self) -> &[IdentityClaim] {
        &self.claims
    }
}

/// One committed or speculative application-state image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalState {
    limits: StateLimits,
    revision: u64,
    partitions: HashMap<PartitionIncarnation, CanonicalPartition>,
    addresses: HashMap<PartitionAddress, PartitionIncarnation>,
    progress: HashMap<ProgressKey, Offset>,
    assignments: HashMap<AssignmentKey, AssignmentState>,
    groups: HashMap<ConsumerGroupId, std::sync::Arc<CoordinatorGroup>>,
    group_member_count: usize,
    group_target_count: usize,
    producer_count: usize,
    retry_span_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ProgressKey {
    Subscription(SubscriptionId, PartitionIncarnation),
    ConsumerGroup(ConsumerGroupId, PartitionIncarnation),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AssignmentKey(ConsumerGroupId, PartitionIncarnation);

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mutation {
    GroupPlan {
        value: std::sync::Arc<CoordinatorGroup>,
        members: usize,
        sources: usize,
    },
    GroupDelete {
        group: ConsumerGroupId,
        members: usize,
        sources: usize,
    },
    Create {
        partition: PartitionIncarnation,
        state: CanonicalPartition,
    },
    OpenProducer {
        partition: PartitionIncarnation,
        producer: ProducerId,
        new_epoch: ProducerEpoch,
        transition: ProducerTransition,
    },
    Append(SmallVec<[AppendCursor; 4]>),
    ResolvedAppend {
        partition: PartitionIncarnation,
        producer: ProducerId,
        results: PreparedProducerResults,
        time: u64,
    },
    Progress {
        key: ProgressKey,
        value: Offset,
    },
    Assign {
        key: AssignmentKey,
        value: AssignmentState,
    },
    Trim {
        partition: PartitionIncarnation,
        retained_from: Offset,
    },
    Policy {
        partition: PartitionIncarnation,
        revision: u64,
        retention: RetentionPolicy,
    },
    ProducerResultFloor {
        partition: PartitionIncarnation,
        producer: ProducerId,
        floor: ProducerSequence,
    },
    RetireProducer {
        partition: PartitionIncarnation,
        producer: ProducerId,
    },
    DedupTime {
        partition: PartitionIncarnation,
        time: u64,
    },
    Delay(delay::PreparedDelay),
    Barrier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppendCursor {
    partition: PartitionIncarnation,
    producer: ProducerId,
    first_sequence: ProducerSequence,
    first_offset: Offset,
    records: u64,
    new_span: bool,
    next_sequence: ProducerSequence,
    next_offset: Offset,
}

impl CanonicalState {
    /// Create empty application state with explicit metadata bounds.
    pub fn new(limits: StateLimits) -> Self {
        Self {
            limits,
            revision: 0,
            partitions: HashMap::new(),
            addresses: HashMap::new(),
            progress: HashMap::new(),
            assignments: HashMap::new(),
            groups: HashMap::new(),
            group_member_count: 0,
            group_target_count: 0,
            producer_count: 0,
            retry_span_count: 0,
        }
    }

    fn require_next_operation(&self, op_number: u64) -> Result<(), StateError> {
        let expected = self
            .revision
            .checked_add(1)
            .ok_or(StateError::RevisionExhausted)?;
        if expected != op_number {
            return Err(StateError::OperationNumberMismatch);
        }
        Ok(())
    }

    /// Current mutation revision used to fence prepared transitions.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Look up live state for one exact partition incarnation.
    pub fn partition(&self, partition: PartitionIncarnation) -> Option<&CanonicalPartition> {
        self.partitions.get(&partition)
    }

    /// Resolve one live partition address to its immutable incarnation.
    pub fn partition_by_address(&self, address: &PartitionAddress) -> Option<PartitionIncarnation> {
        self.addresses.get(address).copied()
    }

    /// Iterate live partitions for retention, diagnostics, and adapters.
    pub fn partitions(
        &self,
    ) -> impl ExactSizeIterator<Item = (PartitionIncarnation, &CanonicalPartition)> {
        self.partitions
            .iter()
            .map(|(partition, state)| (*partition, state))
    }

    /// Look up the current consumer-group assignment for a partition.
    pub fn assignment(
        &self,
        group: ConsumerGroupId,
        partition: PartitionIncarnation,
    ) -> Option<AssignmentState> {
        self.assignments
            .get(&AssignmentKey(group, partition))
            .copied()
    }

    /// Look up the declared exclusive progress offset for an owner and partition.
    pub fn progress(
        &self,
        owner: ProgressOwner,
        partition: PartitionIncarnation,
    ) -> Option<Offset> {
        self.progress.get(&progress_key(owner, partition)).copied()
    }
}

fn operation_claim(
    operation_id: OperationId,
    op_number: u64,
    identities: &impl IdentityIndex,
) -> Result<Vec<IdentityClaim>, StateError> {
    require_id("operation", operation_id.as_bytes())?;
    let claim = IdentityClaim {
        operation_id,
        op_number,
    };
    if identities.lookup(claim.key())?.is_some() {
        return Err(StateError::IdentityConflict);
    }
    Ok(vec![claim])
}

fn progress_key(owner: ProgressOwner, partition: PartitionIncarnation) -> ProgressKey {
    match owner {
        ProgressOwner::Subscription(id) => ProgressKey::Subscription(id, partition),
        ProgressOwner::ConsumerGroup(id) => ProgressKey::ConsumerGroup(id, partition),
    }
}

fn require_progress_owner(owner: ProgressOwner) -> Result<(), StateError> {
    match owner {
        ProgressOwner::Subscription(id) => require_id("subscription", id.as_bytes()),
        ProgressOwner::ConsumerGroup(id) => require_id("consumer group", id.as_bytes()),
    }
}

fn require_record_parts(parts: usize) -> Result<(), StateError> {
    if parts == 0 {
        Err(StateError::MalformedBody(
            "append record must contain at least one part",
        ))
    } else {
        Ok(())
    }
}

fn require_id(kind: &'static str, bytes: &[u8; 16]) -> Result<(), StateError> {
    if bytes.iter().all(|byte| *byte == 0) {
        Err(StateError::ZeroValue(kind))
    } else {
        Ok(())
    }
}

/// Deterministic canonical application-state validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum StateError {
    /// A coordinator image has invalid members, source epochs, or balance.
    #[error("invalid consumer group plan")]
    InvalidGroupPlan,
    /// A membership transition no longer matches the coordinator revision.
    #[error("consumer group revision mismatch")]
    GroupRevisionMismatch,
    /// A group name, incarnation, coordinator, or topology changed.
    #[error("consumer group identity mismatch")]
    GroupIdentityMismatch,
    #[error(transparent)]
    /// The exact identity index rejected lookup or reservation.
    IdentityIndex(#[from] IdentityIndexError),
    #[error("state transition plan is stale")]
    /// The prepared transition belongs to an obsolete state revision.
    StalePlan,
    #[error("state revision exhausted")]
    /// The state mutation revision cannot advance.
    RevisionExhausted,
    #[error("operation number is not the next state revision")]
    /// Canonical operation numbers are not consecutive.
    OperationNumberMismatch,
    #[error("malformed canonical operation body: {0}")]
    /// The decoded operation body violates its contract.
    MalformedBody(&'static str),
    #[error("{0} must be nonzero")]
    /// A required nonzero field is zero.
    ZeroValue(&'static str),
    #[error("{0} limit exceeded")]
    /// The operation or snapshot exceeds a configured resource bound.
    LimitExceeded(&'static str),
    #[error("partition incarnation already exists")]
    /// The partition incarnation already exists.
    PartitionExists,
    #[error("partition logical address already exists")]
    /// Another incarnation already owns this logical address.
    PartitionAddressExists,
    #[error("partition does not exist")]
    /// The referenced partition incarnation does not exist.
    UnknownPartition,
    #[error("producer session is not open in this partition")]
    /// The operation names a different producer session.
    ProducerMismatch,
    #[error("partition owner epoch does not match")]
    /// The operation carries an obsolete or foreign owner fence.
    OwnerEpochMismatch,
    #[error("producer epoch does not match")]
    /// The operation carries a different producer-session fence.
    ProducerEpochMismatch,
    #[error("producer epoch did not advance")]
    /// A producer-session transition does not advance its fence.
    ProducerEpochNotAdvanced,
    #[error("producer sequence does not match")]
    /// Producer sequences are not consecutive with session state.
    ProducerSequenceMismatch,
    #[error("partition offset does not match")]
    /// Assigned offsets are not consecutive with partition state.
    OffsetMismatch,
    #[error("append contains more than one batch for a partition")]
    /// An APPEND repeats the same partition incarnation.
    DuplicateAppendPartition,
    #[error("producer sequence or partition offset exhausted")]
    /// A record offset or sequence cannot advance.
    PositionExhausted,
    #[error("message or operation identity already exists")]
    /// Retry identity conflicts with a previously recorded result.
    IdentityConflict,
    #[error("consumer progress expectation does not match")]
    /// The expected progress offset differs from current state.
    ProgressMismatch,
    #[error("consumer progress did not advance")]
    /// The declared progress does not advance.
    ProgressNotAdvanced,
    #[error("consumer progress exceeds the partition's records")]
    /// Consumer progress would pass confirmed application state.
    ProgressBeyondRecords,
    #[error("consumer-group assignment does not exist")]
    /// The consumer group has no assignment for this partition.
    MissingAssignment,
    #[error("consumer-group assignment epoch does not match")]
    /// The assignment fence differs from current state.
    AssignmentEpochMismatch,
    #[error("consumer-group member does not own the assignment")]
    /// The assignment is revoked or belongs to a different member.
    AssignmentMemberMismatch,
    #[error("consumer-group assignment epoch did not advance by one")]
    /// The assignment transition does not advance its fence.
    AssignmentEpochNotAdvanced,
    #[error("consumer-group assignment plan did not advance")]
    /// A delayed coordinator plan cannot supersede a newer source assignment.
    AssignmentPlanNotAdvanced,
    #[error("consumer-group assignment cursor differs from existing progress")]
    /// A handoff must preserve the exact accepted processing prefix.
    AssignmentProgressMismatch,
    #[error("initial consumer-group cursor is outside confirmed retained history")]
    /// A first assignment cannot silently skip unavailable or unconfirmed history.
    InvalidAssignmentCursor,
    #[error("trim floor expectation does not match")]
    /// The expected retention floor differs from current state.
    TrimFloorMismatch,
    #[error("trim floor did not advance within the partition range")]
    /// The proposed retention floor is outside the valid record range.
    InvalidTrimFloor,
    #[error("trim floor exceeds expired producer retry results")]
    /// Retention would remove still-required producer retry results.
    TrimBeyondProducerResults,
    #[error("partition policy revision does not match")]
    /// The expected policy revision differs from current state.
    PolicyRevisionMismatch,
    #[error("partition policy revision did not advance by one")]
    /// The policy transition does not advance its revision.
    PolicyRevisionNotAdvanced,
    #[error("producer retry-result floor expectation does not match")]
    /// The expected producer retry floor differs from session state.
    ProducerResultFloorMismatch,
    #[error("producer retry-result floor did not advance within the current session")]
    /// The proposed retry floor is outside the producer result range.
    InvalidProducerResultFloor,
    #[error("deduplication logical time did not advance")]
    /// An idle watermark must strictly advance the current logical time.
    DedupTimeNotAdvanced,
    #[error("partition does not hold scheduled records")]
    /// A schedule or release names a partition without delay.
    DelayDisabled,
    #[error("partition cannot enable both delay and deduplication")]
    /// Released records would bypass deduplication.
    DelayWithDeduplication,
    #[error("schedule sequence does not match")]
    /// Schedule sequences are not consecutive with partition state.
    ScheduleMismatch,
    #[error("schedule or release time went backward")]
    /// A timestamp is below the latest schedule or release, or a release
    /// precedes its records' due time.
    DelayTimeRegressed,
    #[error("due time exceeds the maximum delay")]
    /// A due time lies past acceptance plus the maximum delay.
    DueBeyondMaximum,
    #[error("release does not match the held records")]
    /// A release is not exactly the held prefix through its key.
    ReleaseMismatch,
}
