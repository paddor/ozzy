//! Deterministic canonical-operation state transitions.

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use smallvec::SmallVec;

use ozzy_journal::operation::{
    Append, AppendBatchSummary, AppendSummary, Assign, Barrier, CreatePartition, OpenProducer,
    OperationBody, PartitionPolicy, ProducerResultFloor, Progress, ProgressOwner, RetentionPolicy,
    Trim,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};
use thiserror::Error;

mod images;
mod producer;
mod snapshot;

pub use producer::{CanonicalProducer, ProducerResultSpan};

pub use images::{
    CanonicalImages, CanonicalImagesError, CanonicalRecovery, PreparedCanonicalGroup,
};
pub use snapshot::{
    STATE_SNAPSHOT_HEADER_BYTES, StateSnapshotError, StateSnapshotLimits,
    canonical_state_schema_digest,
};

/// Bounds for live application metadata. Historical identities live in indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateLimits {
    pub max_partitions: usize,
    pub max_producers: usize,
    pub max_retry_spans: usize,
    pub max_progress: usize,
    pub max_assignments: usize,
}

impl Default for StateLimits {
    fn default() -> Self {
        Self {
            max_partitions: 65_536,
            max_producers: 1_048_576,
            max_retry_spans: 1_048_576,
            max_progress: 1_048_576,
            max_assignments: 1_048_576,
        }
    }
}

/// Immutable logical address bound to one partition incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionAddress {
    pub stream: String,
    pub topic: String,
    pub partition_id: PartitionId,
}

/// Compact live state reconstructed for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalPartition {
    pub address: PartitionAddress,
    pub owner_epoch: OwnerEpoch,
    producers: HashMap<ProducerId, CanonicalProducer>,
    pub next_offset: Offset,
    pub retained_from: Offset,
    pub policy_revision: u64,
    pub retention: RetentionPolicy,
}

impl CanonicalPartition {
    pub fn producer(&self, producer: ProducerId) -> Option<&CanonicalProducer> {
        self.producers.get(&producer)
    }

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
    pub epoch: u64,
    pub member: Option<ConsumerMemberId>,
}

/// Exact control-operation identity checked against persistent indexes and
/// bounded overlays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IdentityKey(OperationId);

impl IdentityKey {
    pub const fn operation(operation_id: OperationId) -> Self {
        Self(operation_id)
    }

    pub const fn operation_id(self) -> OperationId {
        self.0
    }
}

/// Control-operation identity plus its deterministic result coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentityClaim {
    pub operation_id: OperationId,
    pub op_number: u64,
}

impl IdentityClaim {
    pub const fn key(self) -> IdentityKey {
        IdentityKey::operation(self.operation_id)
    }
}

/// Exact committed index plus bounded unindexed/speculative overlay.
///
/// `reserve` is atomic: an error inserts no claim. It performs no blocking I/O;
/// storage adapters resolve disk lookup and capacity before entering the core.
pub trait IdentityIndex {
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError>;

    /// Check whether this many additional unique claims fit.
    fn check_capacity(&self, additional: usize) -> Result<(), IdentityIndexError>;

    /// Validate an atomic reservation without changing the index.
    fn check_reserve(&self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError>;

    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError>;

    /// Install claims already validated against this exact index generation.
    ///
    /// Canonical transition plans are opaque and revision-bound. The state
    /// engine uses this after `prepare` has checked every identity and the
    /// index cannot have changed independently. Implementations may skip
    /// duplicate lookups, but must still preserve capacity and atomicity.
    fn reserve_validated(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.reserve(claims)
    }
}

/// Bounded exact in-memory identity index for tests, simulation, and overlays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryIdentityIndex {
    capacity: usize,
    claims: HashMap<OperationId, u64>,
}

impl MemoryIdentityIndex {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            claims: HashMap::new(),
        }
    }

    pub fn get(&self, key: IdentityKey) -> Option<IdentityClaim> {
        self.claims
            .get(&key.operation_id())
            .map(|op_number| IdentityClaim {
                operation_id: key.operation_id(),
                op_number: *op_number,
            })
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.claims.len()
    }

    pub fn is_empty(&self) -> bool {
        self.claims.is_empty()
    }

    pub fn clear(&mut self) {
        self.claims.clear();
    }

    pub fn claims(&self) -> impl ExactSizeIterator<Item = IdentityClaim> + '_ {
        self.claims
            .iter()
            .map(|(operation_id, op_number)| IdentityClaim {
                operation_id: *operation_id,
                op_number: *op_number,
            })
    }
}

impl IdentityIndex for MemoryIdentityIndex {
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError> {
        Ok(self.get(key))
    }

    fn check_capacity(&self, additional: usize) -> Result<(), IdentityIndexError> {
        if self.claims.len().saturating_add(additional) > self.capacity {
            return Err(IdentityIndexError::Capacity);
        }
        Ok(())
    }

    fn check_reserve(&self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        let keys = claims
            .iter()
            .map(|claim| claim.key())
            .collect::<HashSet<_>>();
        if keys.len() != claims.len()
            || keys
                .iter()
                .any(|key| self.claims.contains_key(&key.operation_id()))
        {
            return Err(IdentityIndexError::Conflict);
        }
        self.check_capacity(keys.len())
    }

    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_reserve(claims)?;
        self.claims.extend(
            claims
                .iter()
                .map(|claim| (claim.operation_id, claim.op_number)),
        );
        Ok(())
    }

    fn reserve_validated(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_capacity(claims.len())?;
        self.claims.extend(
            claims
                .iter()
                .map(|claim| (claim.operation_id, claim.op_number)),
        );
        Ok(())
    }
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
    pub const fn op_number(&self) -> u64 {
        self.op_number
    }

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
    Create {
        partition: PartitionIncarnation,
        state: CanonicalPartition,
    },
    OpenProducer {
        partition: PartitionIncarnation,
        producer: ProducerId,
        new_epoch: ProducerEpoch,
    },
    Append(SmallVec<[AppendCursor; 4]>),
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

// Four or fewer partition cursors stay inline. Larger grouped appends switch
// to a set so hostile/many-partition input cannot create a quadratic scan.
fn insert_append_partition(
    cursors: &[AppendCursor],
    partitions: &mut HashSet<PartitionIncarnation>,
    partition: PartitionIncarnation,
) -> bool {
    if cursors.len() < 4 {
        return !cursors.iter().any(|cursor| cursor.partition == partition);
    }
    if partitions.is_empty() {
        partitions.extend(cursors.iter().map(|cursor| cursor.partition));
    }
    partitions.insert(partition)
}

impl CanonicalState {
    pub fn new(limits: StateLimits) -> Self {
        Self {
            limits,
            revision: 0,
            partitions: HashMap::new(),
            addresses: HashMap::new(),
            progress: HashMap::new(),
            assignments: HashMap::new(),
            producer_count: 0,
            retry_span_count: 0,
        }
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

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

    pub fn assignment(
        &self,
        group: ConsumerGroupId,
        partition: PartitionIncarnation,
    ) -> Option<AssignmentState> {
        self.assignments
            .get(&AssignmentKey(group, partition))
            .copied()
    }

    pub fn progress(
        &self,
        owner: ProgressOwner,
        partition: PartitionIncarnation,
    ) -> Option<Offset> {
        self.progress.get(&progress_key(owner, partition)).copied()
    }

    /// Validate one decoded operation and freeze its deterministic mutation.
    ///
    /// `committed` supplies the visibility bound for consumer progress. It may
    /// equal `self` when preparing a committed transition.
    pub fn prepare(
        &self,
        op_number: u64,
        body: &OperationBody<'_>,
        identities: &impl IdentityIndex,
        committed: &Self,
    ) -> Result<TransitionPlan, StateError> {
        let expected_op_number = self
            .revision
            .checked_add(1)
            .ok_or(StateError::RevisionExhausted)?;
        if op_number != expected_op_number {
            return Err(StateError::OperationNumberMismatch);
        }
        let (mutation, claims) = match body {
            OperationBody::CreatePartition(value) => self.prepare_create(value)?,
            OperationBody::OpenProducer(value) => {
                self.prepare_open(*value, op_number, identities)?
            }
            OperationBody::Append(value) => self.prepare_append(value)?,
            OperationBody::Progress(value) => {
                self.prepare_progress(*value, op_number, identities, committed)?
            }
            OperationBody::Assign(value) => self.prepare_assign(*value, op_number, identities)?,
            OperationBody::Trim(value) => self.prepare_trim(*value, op_number, identities)?,
            OperationBody::PartitionPolicy(value) => {
                self.prepare_policy(*value, op_number, identities)?
            }
            OperationBody::Barrier(value) => Self::prepare_barrier(*value, op_number, identities)?,
            OperationBody::ProducerResultFloor(value) => {
                self.prepare_producer_result_floor(*value, op_number, identities)?
            }
        };
        Ok(TransitionPlan {
            expected_revision: self.revision,
            op_number,
            mutation,
            claims,
        })
    }

    /// Atomically reserve identities, then apply a previously prepared plan.
    pub fn apply(
        &mut self,
        plan: TransitionPlan,
        identities: &mut impl IdentityIndex,
    ) -> Result<(), StateError> {
        if plan.expected_revision != self.revision {
            return Err(StateError::StalePlan);
        }
        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or(StateError::RevisionExhausted)?;
        identities.reserve_validated(&plan.claims)?;
        self.apply_mutation(plan.mutation);
        self.revision = next_revision;
        Ok(())
    }

    pub(super) fn install_prepared(&mut self, plan: TransitionPlan) {
        debug_assert_eq!(plan.expected_revision, self.revision);
        debug_assert_eq!(plan.op_number, self.revision + 1);
        self.apply_mutation(plan.mutation);
        self.revision = plan.op_number;
    }

    fn prepare_create(
        &self,
        value: &CreatePartition<'_>,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        require_id("partition", value.partition.as_bytes())?;
        if value.stream.is_empty() || value.topic.is_empty() {
            return Err(StateError::MalformedBody(
                "partition names must not be empty",
            ));
        }
        if value.owner_epoch.get() == 0 {
            return Err(StateError::ZeroValue("owner epoch"));
        }
        if self.partitions.contains_key(&value.partition) {
            return Err(StateError::PartitionExists);
        }
        if self.partitions.len() >= self.limits.max_partitions {
            return Err(StateError::LimitExceeded("partitions"));
        }
        let address = PartitionAddress {
            stream: value.stream.to_owned(),
            topic: value.topic.to_owned(),
            partition_id: value.partition_id,
        };
        if self.addresses.contains_key(&address) {
            return Err(StateError::PartitionAddressExists);
        }
        Ok((
            Mutation::Create {
                partition: value.partition,
                state: CanonicalPartition {
                    address,
                    owner_epoch: value.owner_epoch,
                    producers: HashMap::new(),
                    next_offset: Offset::ZERO,
                    retained_from: Offset::ZERO,
                    policy_revision: 1,
                    retention: value.retention,
                },
            },
            Vec::new(),
        ))
    }

    fn prepare_open(
        &self,
        value: OpenProducer,
        op_number: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let partition = self.require_partition(value.partition)?;
        require_id("producer", value.producer_id.as_bytes())?;
        let producer = partition.producer(value.producer_id);
        if producer.is_none() && self.producer_count >= self.limits.max_producers {
            return Err(StateError::LimitExceeded("producers"));
        }
        let current_epoch = producer.map(|producer| producer.producer_epoch);
        if value.expected_epoch != current_epoch {
            return Err(StateError::ProducerEpochMismatch);
        }
        if value.new_epoch.get() == 0 || current_epoch.is_some_and(|epoch| value.new_epoch <= epoch)
        {
            return Err(StateError::ProducerEpochNotAdvanced);
        }
        let claims = operation_claim(value.operation_id, op_number, identities)?;
        Ok((
            Mutation::OpenProducer {
                partition: value.partition,
                producer: value.producer_id,
                new_epoch: value.new_epoch,
            },
            claims,
        ))
    }

    fn prepare_append(
        &self,
        value: &Append<'_>,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        if value.batches.is_empty() {
            return Err(StateError::MalformedBody(
                "append must contain at least one batch",
            ));
        }
        let mut cursors = SmallVec::with_capacity(value.batches.len());
        let claims = Vec::new();
        let mut partitions = HashSet::new();
        for batch in &value.batches {
            if batch.records.is_empty() {
                return Err(StateError::MalformedBody(
                    "append batch must contain at least one record",
                ));
            }
            if !insert_append_partition(&cursors, &mut partitions, batch.partition) {
                return Err(StateError::DuplicateAppendPartition);
            }
            let cursor = self.append_cursor(AppendBatchSummary {
                partition: batch.partition,
                owner_epoch: batch.owner_epoch,
                producer_id: batch.producer_id,
                producer_epoch: batch.producer_epoch,
                first_sequence: batch.first_sequence,
                first_offset: batch.first_offset,
                record_count: batch.records.len(),
                nonzero_message_ids: true,
            })?;
            for record in &batch.records {
                require_id("message", record.message_id.as_bytes())?;
                if record.parts.is_empty() {
                    return Err(StateError::MalformedBody(
                        "append record must contain at least one part",
                    ));
                }
            }
            cursors.push(cursor);
        }
        self.check_retry_span_budget(&cursors)?;
        Ok((Mutation::Append(cursors), claims))
    }

    /// Validate schema-checked append metadata without materializing records.
    /// Same epoch, ownership, position, ID, and duplicate-partition checks as
    /// [`Self::prepare`]. The summary is constructed only by the body decoder.
    pub fn prepare_append_summary(
        &self,
        op_number: u64,
        summary: &AppendSummary,
    ) -> Result<TransitionPlan, StateError> {
        if self
            .revision
            .checked_add(1)
            .ok_or(StateError::RevisionExhausted)?
            != op_number
        {
            return Err(StateError::OperationNumberMismatch);
        }
        let mut cursors = SmallVec::with_capacity(summary.batches().len());
        let mut partitions = HashSet::new();
        for &batch in summary.batches() {
            if !insert_append_partition(&cursors, &mut partitions, batch.partition) {
                return Err(StateError::DuplicateAppendPartition);
            }
            cursors.push(self.append_cursor(batch)?);
        }
        self.check_retry_span_budget(&cursors)?;
        Ok(TransitionPlan {
            expected_revision: self.revision,
            op_number,
            mutation: Mutation::Append(cursors),
            claims: Vec::new(),
        })
    }

    fn append_cursor(&self, batch: AppendBatchSummary) -> Result<AppendCursor, StateError> {
        let partition = self.require_partition(batch.partition)?;
        if batch.owner_epoch != partition.owner_epoch {
            return Err(StateError::OwnerEpochMismatch);
        }
        let producer = partition
            .producer(batch.producer_id)
            .ok_or(StateError::ProducerMismatch)?;
        if batch.producer_epoch != producer.producer_epoch {
            return Err(StateError::ProducerEpochMismatch);
        }
        if batch.first_sequence != producer.next_producer_sequence {
            return Err(StateError::ProducerSequenceMismatch);
        }
        if batch.first_offset != partition.next_offset {
            return Err(StateError::OffsetMismatch);
        }
        let count = u64::try_from(batch.record_count).map_err(|_| StateError::PositionExhausted)?;
        let next_sequence = batch
            .first_sequence
            .get()
            .checked_add(count)
            .map(ProducerSequence::new)
            .ok_or(StateError::PositionExhausted)?;
        let next_offset = batch
            .first_offset
            .get()
            .checked_add(count)
            .map(Offset::new)
            .ok_or(StateError::PositionExhausted)?;
        if !batch.nonzero_message_ids {
            return Err(StateError::ZeroValue("message"));
        }
        Ok(AppendCursor {
            partition: batch.partition,
            producer: batch.producer_id,
            first_sequence: batch.first_sequence,
            first_offset: batch.first_offset,
            records: count,
            new_span: producer.needs_result_span(batch.first_offset),
            next_sequence,
            next_offset,
        })
    }

    fn check_retry_span_budget(&self, cursors: &[AppendCursor]) -> Result<(), StateError> {
        let additional = cursors.iter().filter(|cursor| cursor.new_span).count();
        if self
            .retry_span_count
            .checked_add(additional)
            .is_none_or(|total| total > self.limits.max_retry_spans)
        {
            return Err(StateError::LimitExceeded("producer retry spans"));
        }
        Ok(())
    }

    fn prepare_progress(
        &self,
        value: Progress,
        op_number: u64,
        identities: &impl IdentityIndex,
        committed: &Self,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        self.require_partition(value.partition)?;
        let key = progress_key(value.owner, value.partition);
        require_progress_owner(value.owner)?;
        match (value.owner, value.assignment_epoch) {
            (ProgressOwner::Subscription(_), None) | (ProgressOwner::ConsumerGroup(_), Some(_)) => {
            }
            (ProgressOwner::Subscription(_), Some(_)) => {
                return Err(StateError::MalformedBody(
                    "subscription progress cannot carry an assignment epoch",
                ));
            }
            (ProgressOwner::ConsumerGroup(_), None) => {
                return Err(StateError::MalformedBody(
                    "consumer-group progress requires an assignment epoch",
                ));
            }
        }
        let current = self.progress.get(&key).copied();
        if current != value.expected_progress {
            return Err(StateError::ProgressMismatch);
        }
        if current.is_some_and(|offset| value.new_progress <= offset) {
            return Err(StateError::ProgressNotAdvanced);
        }
        let committed_partition = committed.require_partition(value.partition)?;
        if value.new_progress >= committed_partition.next_offset {
            return Err(StateError::ProgressBeyondCommit);
        }
        if let ProgressOwner::ConsumerGroup(group) = value.owner {
            let assignment = self
                .assignments
                .get(&AssignmentKey(group, value.partition))
                .ok_or(StateError::MissingAssignment)?;
            if value.assignment_epoch != Some(assignment.epoch) {
                return Err(StateError::AssignmentEpochMismatch);
            }
        }
        if current.is_none() && self.progress.len() >= self.limits.max_progress {
            return Err(StateError::LimitExceeded("progress owners"));
        }
        let claims = operation_claim(value.operation_id, op_number, identities)?;
        Ok((
            Mutation::Progress {
                key,
                value: value.new_progress,
            },
            claims,
        ))
    }

    fn prepare_assign(
        &self,
        value: Assign,
        op_number: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        self.require_partition(value.partition)?;
        require_id("consumer group", value.consumer_group_id.as_bytes())?;
        if let Some(member) = value.new_member {
            require_id("consumer member", member.as_bytes())?;
        }
        let key = AssignmentKey(value.consumer_group_id, value.partition);
        let current = self
            .assignments
            .get(&key)
            .copied()
            .unwrap_or(AssignmentState {
                epoch: 0,
                member: None,
            });
        if value.expected_assignment_epoch != current.epoch {
            return Err(StateError::AssignmentEpochMismatch);
        }
        if value.expected_assignment_epoch.checked_add(1) != Some(value.new_assignment_epoch) {
            return Err(StateError::AssignmentEpochNotAdvanced);
        }
        if !self.assignments.contains_key(&key)
            && self.assignments.len() >= self.limits.max_assignments
        {
            return Err(StateError::LimitExceeded("assignments"));
        }
        let claims = operation_claim(value.operation_id, op_number, identities)?;
        Ok((
            Mutation::Assign {
                key,
                value: AssignmentState {
                    epoch: value.new_assignment_epoch,
                    member: value.new_member,
                },
            },
            claims,
        ))
    }

    fn prepare_trim(
        &self,
        value: Trim,
        op_number: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let partition = self.require_partition(value.partition)?;
        if value.expected_floor != partition.retained_from {
            return Err(StateError::TrimFloorMismatch);
        }
        if value.new_floor <= value.expected_floor || value.new_floor > partition.next_offset {
            return Err(StateError::InvalidTrimFloor);
        }
        if partition
            .producer_result_offset_floor()
            .is_none_or(|retry_floor| value.new_floor > retry_floor)
        {
            return Err(StateError::TrimBeyondProducerResults);
        }
        let claims = operation_claim(value.operation_id, op_number, identities)?;
        Ok((
            Mutation::Trim {
                partition: value.partition,
                retained_from: value.new_floor,
            },
            claims,
        ))
    }

    fn prepare_policy(
        &self,
        value: PartitionPolicy,
        op_number: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let partition = self.require_partition(value.partition)?;
        if value.expected_revision != partition.policy_revision {
            return Err(StateError::PolicyRevisionMismatch);
        }
        if value.expected_revision.checked_add(1) != Some(value.new_revision) {
            return Err(StateError::PolicyRevisionNotAdvanced);
        }
        let claims = operation_claim(value.operation_id, op_number, identities)?;
        Ok((
            Mutation::Policy {
                partition: value.partition,
                revision: value.new_revision,
                retention: value.retention,
            },
            claims,
        ))
    }

    fn prepare_producer_result_floor(
        &self,
        value: ProducerResultFloor,
        op_number: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let partition = self.require_partition(value.partition)?;
        let producer = partition
            .producer(value.producer_id)
            .ok_or(StateError::ProducerMismatch)?;
        if value.producer_epoch != producer.producer_epoch {
            return Err(StateError::ProducerEpochMismatch);
        }
        if value.expected_floor != producer.producer_result_floor {
            return Err(StateError::ProducerResultFloorMismatch);
        }
        if value.new_floor <= value.expected_floor
            || value.new_floor > producer.next_producer_sequence
        {
            return Err(StateError::InvalidProducerResultFloor);
        }
        let claims = operation_claim(value.operation_id, op_number, identities)?;
        Ok((
            Mutation::ProducerResultFloor {
                partition: value.partition,
                producer: value.producer_id,
                floor: value.new_floor,
            },
            claims,
        ))
    }

    fn prepare_barrier(
        value: Barrier,
        op_number: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        Ok((
            Mutation::Barrier,
            operation_claim(value.operation_id, op_number, identities)?,
        ))
    }

    fn require_partition(
        &self,
        partition: PartitionIncarnation,
    ) -> Result<&CanonicalPartition, StateError> {
        self.partitions
            .get(&partition)
            .ok_or(StateError::UnknownPartition)
    }

    fn apply_mutation(&mut self, mutation: Mutation) {
        match mutation {
            Mutation::Create { partition, state } => {
                self.addresses.insert(state.address.clone(), partition);
                self.partitions.insert(partition, state);
            }
            Mutation::OpenProducer {
                partition,
                producer,
                new_epoch,
            } => {
                let state = self.partitions.get_mut(&partition).expect("validated plan");
                if let Some(previous) = state
                    .producers
                    .insert(producer, CanonicalProducer::new(new_epoch))
                {
                    self.retry_span_count -= previous.result_spans().len();
                } else {
                    self.producer_count += 1;
                }
            }
            Mutation::Append(cursors) => {
                for cursor in cursors {
                    let state = self
                        .partitions
                        .get_mut(&cursor.partition)
                        .expect("validated plan");
                    state
                        .producers
                        .get_mut(&cursor.producer)
                        .expect("validated producer")
                        .record_assignment(
                            cursor.first_sequence,
                            cursor.first_offset,
                            cursor.records,
                            self.limits.max_retry_spans,
                        )
                        .expect("validated assignment");
                    self.retry_span_count += usize::from(cursor.new_span);
                    state.next_offset = cursor.next_offset;
                }
            }
            Mutation::Progress { key, value } => {
                self.progress.insert(key, value);
            }
            Mutation::Assign { key, value } => {
                self.assignments.insert(key, value);
            }
            Mutation::Trim {
                partition,
                retained_from,
            } => {
                self.partitions
                    .get_mut(&partition)
                    .expect("validated plan")
                    .retained_from = retained_from;
            }
            Mutation::Policy {
                partition,
                revision,
                retention,
            } => {
                let state = self.partitions.get_mut(&partition).expect("validated plan");
                state.policy_revision = revision;
                state.retention = retention;
            }
            Mutation::ProducerResultFloor {
                partition,
                producer,
                floor,
            } => {
                let producer = self
                    .partitions
                    .get_mut(&partition)
                    .expect("validated plan")
                    .producers
                    .get_mut(&producer)
                    .expect("validated producer");
                let previous = producer.result_spans().len();
                producer.expire_results(floor);
                self.retry_span_count -= previous - producer.result_spans().len();
            }
            Mutation::Barrier => {}
        }
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

fn require_id(kind: &'static str, bytes: &[u8; 16]) -> Result<(), StateError> {
    if bytes.iter().all(|byte| *byte == 0) {
        Err(StateError::ZeroValue(kind))
    } else {
        Ok(())
    }
}

/// Atomic identity-overlay reservation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum IdentityIndexError {
    #[error("identity already exists")]
    Conflict,
    #[error("identity overlay capacity exhausted")]
    Capacity,
    #[error("persistent identity lookup is unavailable")]
    LookupUnavailable,
}

/// Deterministic canonical application-state validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum StateError {
    #[error(transparent)]
    IdentityIndex(#[from] IdentityIndexError),
    #[error("state transition plan is stale")]
    StalePlan,
    #[error("state revision exhausted")]
    RevisionExhausted,
    #[error("operation number is not the next state revision")]
    OperationNumberMismatch,
    #[error("malformed canonical operation body: {0}")]
    MalformedBody(&'static str),
    #[error("{0} must be nonzero")]
    ZeroValue(&'static str),
    #[error("{0} limit exceeded")]
    LimitExceeded(&'static str),
    #[error("partition incarnation already exists")]
    PartitionExists,
    #[error("partition logical address already exists")]
    PartitionAddressExists,
    #[error("partition does not exist")]
    UnknownPartition,
    #[error("producer session is not open in this partition")]
    ProducerMismatch,
    #[error("partition owner epoch does not match")]
    OwnerEpochMismatch,
    #[error("producer epoch does not match")]
    ProducerEpochMismatch,
    #[error("producer epoch did not advance")]
    ProducerEpochNotAdvanced,
    #[error("producer sequence does not match")]
    ProducerSequenceMismatch,
    #[error("partition offset does not match")]
    OffsetMismatch,
    #[error("append contains more than one batch for a partition")]
    DuplicateAppendPartition,
    #[error("producer sequence or partition offset exhausted")]
    PositionExhausted,
    #[error("message or operation identity already exists")]
    IdentityConflict,
    #[error("consumer progress expectation does not match")]
    ProgressMismatch,
    #[error("consumer progress did not advance")]
    ProgressNotAdvanced,
    #[error("consumer progress exceeds committed records")]
    ProgressBeyondCommit,
    #[error("consumer-group assignment does not exist")]
    MissingAssignment,
    #[error("consumer-group assignment epoch does not match")]
    AssignmentEpochMismatch,
    #[error("consumer-group assignment epoch did not advance by one")]
    AssignmentEpochNotAdvanced,
    #[error("trim floor expectation does not match")]
    TrimFloorMismatch,
    #[error("trim floor did not advance within the partition range")]
    InvalidTrimFloor,
    #[error("trim floor exceeds expired producer retry results")]
    TrimBeyondProducerResults,
    #[error("partition policy revision does not match")]
    PolicyRevisionMismatch,
    #[error("partition policy revision did not advance by one")]
    PolicyRevisionNotAdvanced,
    #[error("producer retry-result floor expectation does not match")]
    ProducerResultFloorMismatch,
    #[error("producer retry-result floor did not advance within the current session")]
    InvalidProducerResultFloor,
}
