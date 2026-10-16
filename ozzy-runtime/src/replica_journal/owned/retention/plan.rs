//! Retention planning from sealed-segment summaries, and the retry-floor and
//! trim proposal that advances the record floor.

use super::super::{JournalError, OwnedJournal};
use crate::replica_journal::ProposalBuffer;
use ozzy_core::retention::{Plan, Segment};
use ozzy_core::state::CanonicalProducer;
use ozzy_journal::operation::{OperationBody, ProducerResultFloor, Trim, encode_operation_body};
use ozzy_proto::{Offset, OperationId, PartitionIncarnation};
use ozzy_replication::driver::ValidationTicket;

/// The retained partition and the bounds its plan starts from.
struct Retained {
    partition: PartitionIncarnation,
    policy: ozzy_journal::operation::RetentionPolicy,
    floor: Offset,
    end: Offset,
}

impl OwnedJournal {
    /// Read at most `maximum_segments` immutable summaries; cache each exact
    /// generation once. Selected capacities include unexamined and active space.
    pub async fn plan_retention(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
        maximum_segments: usize,
    ) -> Result<Option<(PartitionIncarnation, Plan)>, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        let Some(retained) = self.retained_partition()? else {
            return Ok(None);
        };
        let _scratch = self.charge_retention()?;
        let segments = self
            .retention_segments(ticket, &retained, maximum_segments)
            .await?;
        let plan = Plan::select(
            &segments,
            retained.policy,
            now_millis,
            ticket.applied().op.0,
            maximum_segments,
        )?;
        Ok(Some((retained.partition, plan)))
    }

    /// The journal's only partition, when its policy limits retention.
    fn retained_partition(&self) -> Result<Option<Retained>, JournalError> {
        let mut partitions = self.images()?.committed().partitions();
        let Some((partition, state)) = partitions.next() else {
            return Ok(None);
        };
        if partitions.next().is_some() {
            return Err(JournalError::Configuration);
        }
        let policy = state.retention;
        if policy.max_age_millis.is_none() && policy.max_bytes.is_none() {
            return Ok(None);
        }
        Ok(Some(Retained {
            partition,
            policy,
            floor: state.retained_from,
            end: state.next_offset,
        }))
    }

    /// Summaries of every selected segment. Reads at most
    /// `maximum_segments` sealed summaries not cached yet.
    async fn retention_segments(
        &mut self,
        ticket: ValidationTicket,
        retained: &Retained,
        maximum_segments: usize,
    ) -> Result<Vec<Segment>, JournalError> {
        let journal = self.journal.ready()?;
        let references = journal.manifest().segments.clone();
        self.retention
            .retain_segments(|id| references.iter().any(|entry| entry.segment_id == id));
        let active_time = self
            .reader
            .as_ref()
            .and_then(|reader| reader.newest_active_append_millis(retained.partition));
        let mut segments = Vec::with_capacity(references.len());
        let mut record_end = retained.floor;
        for (index, reference) in references.iter().enumerate() {
            if reference.sealed.is_some()
                && index < maximum_segments
                && !self.retention.sealed.contains_key(&reference.segment_id)
            {
                let summary = journal
                    .retention_segment(reference.segment_id, retained.partition)
                    .await?;
                self.retention.sealed.insert(reference.segment_id, summary);
            }
            let last_operation = references
                .get(index + 1)
                .map_or(ticket.accepted().op.0, |next| {
                    next.first_chain.next_op_number() - 1
                });
            let mut segment = self
                .retention
                .sealed
                .get(&reference.segment_id)
                .copied()
                .unwrap_or(Segment {
                    id: reference.segment_id,
                    capacity: reference.capacity,
                    sealed: reference.sealed.is_some(),
                    last_operation,
                    record_end,
                    newest_append_millis: active_time.filter(|_| reference.sealed.is_none()),
                });
            record_end = record_end.max(segment.record_end);
            segment.record_end = if segment.sealed {
                record_end
            } else {
                retained.end
            };
            segments.push(segment);
        }
        Ok(segments)
    }

    /// Expire bounded producer retry ranges before advancing the record floor.
    /// The caller submits this through the ordinary mode's proposal/commit path.
    pub fn retention_proposal(
        &self,
        partition: PartitionIncarnation,
        floor: Offset,
        operation_floor: u64,
        seed: OperationId,
    ) -> Result<Option<ProposalBuffer>, JournalError> {
        let state = self
            .images()?
            .committed()
            .partition(partition)
            .ok_or(JournalError::Configuration)?;
        let floor = floor.max(state.retained_from);
        let mut buffer = self
            .lease_proposal_buffer()
            .map_err(|_| JournalError::AppendCapacity)?;
        let mut producers: Vec<_> = state
            .producers()
            .filter_map(|(id, producer)| {
                raised_retry_floor(producer, floor, operation_floor)
                    .map(|new_floor| (id, producer, new_floor))
            })
            .take(self.append_limits.max_operations.saturating_add(1))
            .collect();
        producers.sort_unstable_by_key(|(id, _, _)| *id);
        let mut complete = true;
        for (producer_id, producer, new_floor) in producers {
            if buffer.len() == self.append_limits.max_operations {
                complete = false;
                break;
            }
            let id = operation_id(seed, buffer.len());
            push(
                &mut buffer,
                &OperationBody::ProducerResultFloor(ProducerResultFloor {
                    partition,
                    producer_id,
                    producer_epoch: producer.producer_epoch,
                    expected_floor: producer.producer_result_floor,
                    new_floor,
                    operation_id: id,
                }),
                self.limits.operations,
            )?;
        }
        if complete
            && floor > state.retained_from
            && buffer.len() < self.append_limits.max_operations
        {
            let id = operation_id(seed, buffer.len());
            push(
                &mut buffer,
                &OperationBody::Trim(Trim {
                    partition,
                    expected_floor: state.retained_from,
                    new_floor: floor,
                    operation_id: id,
                }),
                self.limits.operations,
            )?;
        }
        Ok((!buffer.is_empty()).then_some(buffer))
    }
}

fn push(
    buffer: &mut ProposalBuffer,
    body: &OperationBody<'_>,
    limits: ozzy_journal::operation::OperationLimits,
) -> Result<(), JournalError> {
    buffer.push(body.kind(), &encode_operation_body(body, limits)?)
}

fn operation_id(seed: OperationId, ordinal: usize) -> OperationId {
    let mut bytes = *seed.as_bytes();
    for (target, value) in bytes[8..].iter_mut().zip((ordinal as u64).to_be_bytes()) {
        *target ^= value;
    }
    OperationId::from_bytes(bytes)
}

/// The producer's retry-result floor after retention to `floor`, when that
/// raises it.
pub(super) fn raised_retry_floor(
    producer: &CanonicalProducer,
    floor: Offset,
    operation_floor: u64,
) -> Option<ozzy_proto::ProducerSequence> {
    let raised = producer.result_floor_for_retention(floor, operation_floor);
    (raised > producer.producer_result_floor).then_some(raised)
}
