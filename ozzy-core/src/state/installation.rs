//! Install one fully validated state mutation and its identity reservations.

use ozzy_proto::{PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence};
use smallvec::SmallVec;

use super::{
    AppendCursor, CanonicalProducer, CanonicalState, IdentityIndex, Mutation, StateError,
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
                self.install_producer(partition, producer, new_epoch);
            }
            Mutation::Append(cursors) => {
                self.install_append(cursors);
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
                self.expire_producer_results(partition, producer, floor);
            }
            Mutation::Barrier => {}
        }
    }

    fn install_producer(
        &mut self,
        partition: PartitionIncarnation,
        producer: ProducerId,
        new_epoch: ProducerEpoch,
    ) {
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

    fn install_append(&mut self, cursors: SmallVec<[AppendCursor; 4]>) {
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

    fn expire_producer_results(
        &mut self,
        partition: PartitionIncarnation,
        producer: ProducerId,
        floor: ProducerSequence,
    ) {
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
}
