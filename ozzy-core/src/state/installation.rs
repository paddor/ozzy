//! Install one fully validated state mutation and its identity reservations.

use ozzy_journal::operation::RetentionPolicy;
use ozzy_proto::{PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence};
use smallvec::SmallVec;

use super::{
    AppendCursor, AssignmentKey, AssignmentState, CanonicalPartition, CanonicalProducer,
    CanonicalState, IdentityIndex, Mutation, PreparedProducerResults, ProgressKey, StateError,
    TransitionPlan,
};

impl CanonicalState {
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
        self.apply_mutation(plan.mutation)?;
        self.revision = next_revision;
        Ok(())
    }

    pub(super) fn install_prepared(&mut self, plan: TransitionPlan) -> Result<(), StateError> {
        debug_assert_eq!(plan.expected_revision, self.revision);
        debug_assert_eq!(plan.op_number, self.revision + 1);
        self.apply_mutation(plan.mutation)?;
        self.revision = plan.op_number;
        Ok(())
    }

    /// Install a plan another image keeps, copying its mutation but not its
    /// identity claims.
    pub(super) fn install_prepared_copy(
        &mut self,
        plan: &TransitionPlan,
    ) -> Result<(), StateError> {
        debug_assert_eq!(plan.expected_revision, self.revision);
        debug_assert_eq!(plan.op_number, self.revision + 1);
        self.apply_mutation(plan.mutation.clone())?;
        self.revision = plan.op_number;
        Ok(())
    }

    /// A plan validated at this revision names only state the image holds;
    /// a missing partition or producer is rejected, never invented.
    fn apply_mutation(&mut self, mutation: Mutation) -> Result<(), StateError> {
        match mutation {
            Mutation::GroupPlan {
                value,
                members,
                sources,
            } => {
                self.groups.insert(value.group, value);
                self.set_group_counts(members, sources);
            }
            Mutation::GroupDelete {
                group,
                members,
                sources,
            } => {
                self.groups.remove(&group);
                self.set_group_counts(members, sources);
            }
            Mutation::Create { partition, state } => self.install_partition(partition, state),
            Mutation::OpenProducer {
                partition,
                producer,
                new_epoch,
                transition,
            } => self.install_producer(partition, producer, new_epoch, transition)?,
            Mutation::Append(cursors) => self.install_append(cursors)?,
            Mutation::ResolvedAppend {
                partition,
                producer,
                results,
                time,
            } => self.install_results(partition, producer, results, time)?,
            Mutation::Progress { key, value } => {
                self.progress.insert(key, value);
            }
            Mutation::Assign { key, value } => self.install_assignment(key, value),
            Mutation::Trim {
                partition,
                retained_from,
            } => self.partition_mut(partition)?.retained_from = retained_from,
            Mutation::Policy {
                partition,
                revision,
                retention,
            } => self.install_policy(partition, revision, retention)?,
            Mutation::ProducerResultFloor {
                partition,
                producer,
                floor,
            } => self.expire_producer_results(partition, producer, floor)?,
            Mutation::RetireProducer {
                partition,
                producer,
            } => self.retire_producer(partition, producer)?,
            Mutation::DedupTime { partition, time } => {
                self.partition_mut(partition)?.dedup_time = time;
            }
            Mutation::Barrier => {}
        }
        Ok(())
    }

    fn install_partition(&mut self, partition: PartitionIncarnation, state: CanonicalPartition) {
        self.addresses.insert(state.address.clone(), partition);
        self.partitions.insert(partition, state);
    }

    fn set_group_counts(&mut self, members: usize, sources: usize) {
        self.group_member_count = members;
        self.group_target_count = sources;
    }

    fn install_results(
        &mut self,
        partition: PartitionIncarnation,
        producer: ProducerId,
        results: PreparedProducerResults,
        time: u64,
    ) -> Result<(), StateError> {
        let additional = results.additional;
        let state = self.partition_mut(partition)?;
        state.next_offset = results.next_offset;
        state.dedup_time = time;
        state.producer_mut(producer)?.install_results(results);
        self.count_retry_spans(additional, 0)
    }

    fn install_assignment(&mut self, key: AssignmentKey, value: AssignmentState) {
        self.progress.insert(
            ProgressKey::ConsumerGroup(key.0, key.1),
            value.resume_cursor,
        );
        self.assignments.insert(key, value);
    }

    fn install_policy(
        &mut self,
        partition: PartitionIncarnation,
        revision: u64,
        retention: RetentionPolicy,
    ) -> Result<(), StateError> {
        let state = self.partition_mut(partition)?;
        state.policy_revision = revision;
        state.retention = retention;
        Ok(())
    }

    /// A partition the validated plan names.
    fn partition_mut(
        &mut self,
        partition: PartitionIncarnation,
    ) -> Result<&mut super::CanonicalPartition, StateError> {
        self.partitions
            .get_mut(&partition)
            .ok_or(StateError::UnknownPartition)
    }

    fn install_producer(
        &mut self,
        partition: PartitionIncarnation,
        producer: ProducerId,
        new_epoch: ProducerEpoch,
        transition: super::ProducerTransition,
    ) -> Result<(), StateError> {
        let state = self.partition_mut(partition)?;
        let mut opened = CanonicalProducer::new(new_epoch);
        opened.transition = Some(transition);
        if let Some(previous) = state.producers.insert(producer, opened) {
            self.count_retry_spans(0, previous.result_spans().len())?;
        } else {
            self.producer_count = self
                .producer_count
                .checked_add(1)
                .ok_or(StateError::LimitExceeded("producer count"))?;
        }
        Ok(())
    }

    /// Account result spans one applied mutation added and released. The
    /// plan charged them against the image limits before apply.
    fn count_retry_spans(&mut self, added: usize, released: usize) -> Result<(), StateError> {
        self.retry_span_count = self
            .retry_span_count
            .checked_add(added)
            .and_then(|count| count.checked_sub(released))
            .ok_or(StateError::LimitExceeded("retry span count"))?;
        Ok(())
    }

    fn install_append(&mut self, cursors: SmallVec<[AppendCursor; 4]>) -> Result<(), StateError> {
        for cursor in cursors {
            let state = self
                .partitions
                .get_mut(&cursor.partition)
                .ok_or(StateError::UnknownPartition)?;
            state.producer_mut(cursor.producer)?.record_assignment(
                cursor.first_sequence,
                cursor.first_offset,
                cursor.records,
                self.limits.max_retry_spans,
            )?;
            state.next_offset = cursor.next_offset;
            self.count_retry_spans(usize::from(cursor.new_span), 0)?;
        }
        Ok(())
    }

    fn retire_producer(
        &mut self,
        partition: PartitionIncarnation,
        producer: ProducerId,
    ) -> Result<(), StateError> {
        let state = self.partition_mut(partition)?;
        let retired = state
            .producers
            .remove(&producer)
            .ok_or(StateError::ProducerMismatch)?;
        state.retired_epoch = state.retired_epoch.max(retired.producer_epoch.get());
        self.producer_count = self
            .producer_count
            .checked_sub(1)
            .ok_or(StateError::LimitExceeded("producer count"))?;
        self.count_retry_spans(0, retired.result_spans().len())
    }

    fn expire_producer_results(
        &mut self,
        partition: PartitionIncarnation,
        producer: ProducerId,
        floor: ProducerSequence,
    ) -> Result<(), StateError> {
        let producer = self
            .partitions
            .get_mut(&partition)
            .ok_or(StateError::UnknownPartition)?
            .producer_mut(producer)?;
        let previous = producer.result_spans().len();
        producer.expire_results(floor);
        let remaining = producer.result_spans().len();
        self.count_retry_spans(remaining, previous)
    }
}
