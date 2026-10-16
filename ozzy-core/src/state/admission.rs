//! Validate control operations against current fences, in operation order.

use super::{
    AssignmentKey, AssignmentState, CanonicalPartition, CanonicalState, IdentityClaim,
    IdentityIndex, Mutation, PartitionAddress, PartitionIncarnation, StateError, TransitionPlan,
    operation_claim, progress_key, require_id, require_progress_owner,
};
use ahash::AHashMap as HashMap;
use ozzy_journal::operation::{
    Assign, Barrier, CreatePartition, DedupWatermark, OpenProducer, OperationBody, PartitionPolicy,
    ProducerResultFloor, Progress, ProgressOwner, Trim,
};
use ozzy_proto::Offset;

impl CanonicalState {
    /// Validate one decoded operation and freeze its deterministic mutation.
    ///
    /// Every replica validates against the same ordered prefix. Consumer
    /// progress may cover records accepted before it: it commits only after
    /// them. The leader separately admits only cursors over confirmed records.
    pub fn prepare(
        &self,
        op_number: u64,
        body: &OperationBody<'_>,
        identities: &impl IdentityIndex,
    ) -> Result<TransitionPlan, StateError> {
        self.require_next_operation(op_number)?;
        let (mutation, claims) = match body {
            OperationBody::CreatePartition(value) => self.prepare_create(value)?,
            OperationBody::OpenProducer(value) => {
                self.prepare_open(*value, op_number, identities)?
            }
            OperationBody::Append(value) => self.prepare_append(value)?,
            OperationBody::ResolvedAppend(value) => {
                return self.prepare_resolved_append(op_number, value);
            }
            OperationBody::Progress(value) => self.prepare_progress(*value)?,
            OperationBody::Assign(value) => self.prepare_assign(*value, op_number, identities)?,
            OperationBody::Trim(value) => self.prepare_trim(*value, op_number, identities)?,
            OperationBody::PartitionPolicy(value) => {
                self.prepare_policy(*value, op_number, identities)?
            }
            OperationBody::Barrier(value) => Self::prepare_barrier(*value, op_number, identities)?,
            OperationBody::ProducerResultFloor(value) => {
                self.prepare_producer_result_floor(*value, op_number, identities)?
            }
            OperationBody::DedupWatermark(value) => self.prepare_dedup_watermark(*value)?,
            OperationBody::GroupPlan(value) => {
                self.prepare_group_plan(value, op_number, identities)?
            }
            OperationBody::GroupDelete(value) => {
                self.prepare_group_delete(*value, op_number, identities)?
            }
        };
        Ok(TransitionPlan {
            expected_revision: self.revision,
            op_number,
            mutation,
            claims,
        })
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
                    dedup_window_millis: value.dedup_window_millis,
                    dedup_time: 0,
                },
            },
            Vec::new(),
        ))
    }

    fn prepare_dedup_watermark(
        &self,
        value: DedupWatermark,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let partition = self.require_partition(value.partition)?;
        if value.owner_epoch != partition.owner_epoch {
            return Err(StateError::OwnerEpochMismatch);
        }
        if value.logical_time <= partition.dedup_time {
            return Err(StateError::DedupTimeNotAdvanced);
        }
        Ok((
            Mutation::DedupTime {
                partition: value.partition,
                time: value.logical_time,
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
                transition: super::ProducerTransition {
                    operation_id: value.operation_id,
                    expected_epoch: value.expected_epoch,
                },
            },
            claims,
        ))
    }

    /// Progress claims no control identity. A retry either finds its cursor
    /// already covered or fails the expected-progress and assignment fences.
    fn prepare_progress(
        &self,
        value: Progress,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let partition = self.require_partition(value.partition)?;
        let key = progress_key(value.owner, value.partition);
        require_progress_owner(value.owner)?;
        match (value.owner, value.assignment) {
            (ProgressOwner::Subscription(_), None) => {}
            (ProgressOwner::ConsumerGroup(group), Some(fence)) => {
                require_id("consumer member", fence.member.as_bytes())?;
                let assignment = self
                    .assignments
                    .get(&AssignmentKey(group, value.partition))
                    .ok_or(StateError::MissingAssignment)?;
                if fence.epoch != assignment.epoch {
                    return Err(StateError::AssignmentEpochMismatch);
                }
                if assignment.member != Some(fence.member) {
                    return Err(StateError::AssignmentMemberMismatch);
                }
            }
            (ProgressOwner::Subscription(_), Some(_)) => {
                return Err(StateError::MalformedBody(
                    "subscription progress cannot carry an assignment epoch",
                ));
            }
            (ProgressOwner::ConsumerGroup(_), None) => {
                return Err(StateError::MalformedBody(
                    "consumer-group progress requires a member and assignment epoch",
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
        if value.new_progress > partition.next_offset {
            return Err(StateError::ProgressBeyondRecords);
        }
        if current.is_none() && self.progress.len() >= self.limits.max_progress {
            return Err(StateError::LimitExceeded("progress owners"));
        }
        Ok((
            Mutation::Progress {
                key,
                value: value.new_progress,
            },
            Vec::new(),
        ))
    }

    fn prepare_assign(
        &self,
        value: Assign,
        op_number: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        let partition = self.require_partition(value.partition)?;
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
                plan_revision: 0,
                resume_cursor: Offset::ZERO,
                member: None,
            });
        if value.expected_assignment_epoch != current.epoch {
            return Err(StateError::AssignmentEpochMismatch);
        }
        if value.expected_assignment_epoch.checked_add(1) != Some(value.new_assignment_epoch) {
            return Err(StateError::AssignmentEpochNotAdvanced);
        }
        if value.plan_revision <= current.plan_revision {
            return Err(StateError::AssignmentPlanNotAdvanced);
        }
        self.check_resume_cursor(&value, partition)?;
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
                    plan_revision: value.plan_revision,
                    resume_cursor: value.resume_cursor,
                    member: value.new_member,
                },
            },
            claims,
        ))
    }

    /// A group's resume cursor must equal its committed progress, or start a
    /// new progress owner inside the partition's retained records.
    fn check_resume_cursor(
        &self,
        value: &Assign,
        partition: &CanonicalPartition,
    ) -> Result<(), StateError> {
        let progress = progress_key(
            ProgressOwner::ConsumerGroup(value.consumer_group_id),
            value.partition,
        );
        if let Some(prior) = self.progress.get(&progress) {
            if value.resume_cursor != *prior {
                return Err(StateError::AssignmentProgressMismatch);
            }
        } else {
            if value.resume_cursor < partition.retained_from
                || value.resume_cursor > partition.next_offset
            {
                return Err(StateError::InvalidAssignmentCursor);
            }
            if self.progress.len() >= self.limits.max_progress {
                return Err(StateError::LimitExceeded("progress owners"));
            }
        }
        Ok(())
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

    pub(super) fn require_partition(
        &self,
        partition: PartitionIncarnation,
    ) -> Result<&CanonicalPartition, StateError> {
        self.partitions
            .get(&partition)
            .ok_or(StateError::UnknownPartition)
    }
}
