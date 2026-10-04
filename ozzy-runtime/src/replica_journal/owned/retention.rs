//! Confirmed floors precede checkpoint selection and physical retirement.

use super::{JournalError, OwnedJournal};
use crate::replica_journal::{ProposalBuffer, authority::position};
use ozzy_core::retention::{Plan, Segment};
use ozzy_journal::operation::{OperationBody, ProducerResultFloor, Trim, encode_operation_body};
use ozzy_journal_segment::{AsyncRetirementBudget, RetentionFloors, RetiredPrefix};
use ozzy_proto::{CheckpointId, Offset, OperationId, PartitionIncarnation};
use ozzy_replication::driver::ValidationTicket;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(super) struct Retention {
    sealed: BTreeMap<u64, Segment>,
}

impl OwnedJournal {
    fn charge_retention(&self) -> Result<Option<crate::memory::Charge>, JournalError> {
        let Some(owner) = &self.append_memory else {
            return Ok(None);
        };
        let bytes = self.journal.ready()?.retention_scratch_bytes()?;
        owner.try_charge(bytes).map(Some).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                JournalError::AppendCapacity
            } else {
                error.into()
            }
        })
    }

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
        let mut partitions = self.images()?.committed().partitions();
        let Some((partition, state)) = partitions.next() else {
            return Ok(None);
        };
        if partitions.next().is_some() {
            return Err(JournalError::Configuration);
        }
        let (policy, floor, end) = (state.retention, state.retained_from, state.next_offset);
        drop(partitions);
        if policy.max_age_millis.is_none() && policy.max_bytes.is_none() {
            return Ok(None);
        }
        let _scratch = self.charge_retention()?;
        let journal = self.journal.ready()?;
        let references = journal.manifest().segments.clone();
        self.retention
            .sealed
            .retain(|id, _| references.iter().any(|entry| entry.segment_id == *id));
        let active_time = self
            .reader
            .as_ref()
            .and_then(|reader| reader.newest_active_append_millis(partition));
        let mut segments = Vec::with_capacity(references.len());
        let mut record_end = floor;
        for (index, reference) in references.iter().enumerate() {
            if reference.sealed.is_some()
                && index < maximum_segments
                && !self.retention.sealed.contains_key(&reference.segment_id)
            {
                let summary = journal
                    .retention_segment(reference.segment_id, partition)
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
                    newest_append_millis: if reference.sealed.is_none() {
                        active_time
                    } else {
                        None
                    },
                });
            record_end = record_end.max(segment.record_end);
            segment.record_end = if segment.sealed { record_end } else { end };
            segments.push(segment);
        }
        Ok(Some((
            partition,
            Plan::select(
                &segments,
                policy,
                now_millis,
                ticket.applied().op.0,
                maximum_segments,
            )?,
        )))
    }

    /// Expire bounded producer retry ranges before advancing the record floor.
    /// The caller submits this through the ordinary mode's proposal/commit path.
    pub fn retention_proposal(
        &self,
        partition: PartitionIncarnation,
        floor: Offset,
        seed: OperationId,
    ) -> Result<Option<ProposalBuffer>, JournalError> {
        let state = self
            .images()?
            .committed()
            .partition(partition)
            .ok_or(JournalError::Configuration)?;
        if floor <= state.retained_from {
            return Ok(None);
        }
        let mut buffer = self
            .lease_proposal_buffer()
            .map_err(|_| JournalError::AppendCapacity)?;
        let mut producers: Vec<_> = state
            .producers()
            .filter(|(_, producer)| {
                producer.result_floor_for_offset(floor) > producer.producer_result_floor
            })
            .take(self.append_limits.max_operations.saturating_add(1))
            .collect();
        producers.sort_unstable_by_key(|(id, _)| *id);
        let mut complete = true;
        for (producer_id, producer) in producers {
            let new_floor = producer.result_floor_for_offset(floor);
            if new_floor <= producer.producer_result_floor {
                continue;
            }
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
        if complete && buffer.len() < self.append_limits.max_operations {
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

    /// Select a durable checkpoint of exactly the settled confirmed image, then
    /// unreference only a validated sealed prefix below those committed floors.
    /// Cancellation fences this owner. Captured readers keep their file pins.
    pub async fn retire_confirmed_history(
        &mut self,
        ticket: ValidationTicket,
        id: CheckpointId,
        budget: AsyncRetirementBudget,
    ) -> Result<RetiredPrefix, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        self.writeback.require_idle()?;
        if !self.pending.is_empty()
            || ticket.accepted() != self.applied
            || ticket.committed() != self.applied
        {
            return Err(JournalError::CompletionMismatch);
        }
        let _scratch = self.charge_retention()?;
        let state = self.images()?.committed().clone();
        let floors = RetentionFloors::from_canonical_state(&state)?;
        self.faulted = true;
        let journal = self.journal.ready_mut()?;
        journal
            .sync_through(journal.writer().written_position())
            .await?;
        // A bounded scan may need several retirement turns without an append.
        // Reuse only the exact selected operation/digest, never a newer or
        // different checkpoint. Retirement still revalidates its authority,
        // checkpoint files, segment bodies, and committed floors below.
        let files = if journal
            .manifest()
            .checkpoint
            .is_none_or(|selected| selected.position != position(self.applied))
        {
            let mut next = journal.manifest().clone();
            next.parent_generation = next.generation;
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(JournalError::Configuration)?;
            next.accepted = position(self.applied);
            next.committed = position(self.applied);
            journal.install_metadata(next).await?;
            let files = journal
                .build_canonical_checkpoint(
                    id,
                    self.limits.checkpoint.max_chunk_bytes.min(64 * 1024),
                    &state,
                    self.recovery.snapshot,
                )
                .await?;
            journal
                .install_canonical_checkpoint(
                    id,
                    self.recovery.state,
                    self.recovery.snapshot,
                    self.recovery.checkpoint,
                )
                .await?;
            Some(files)
        } else {
            None
        };
        let retired = journal.retire_sealed_prefix(&floors, budget).await?;
        drop(files);
        if let Some(reader) = &mut self.reader {
            reader.retired(journal, &retired.unreferenced_segment_ids)?;
        }
        self.images = Some(journal.recover_canonical_images(self.recovery).await?);
        self.replay.clear();
        self.storage_validation.clear();
        self.retention
            .sealed
            .retain(|id, _| !retired.unreferenced_segment_ids.contains(id));
        self.faulted = false;
        Ok(retired)
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

impl OwnedJournal {
    pub(in crate::replica_journal) async fn retention_turn(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
        seed: OperationId,
        leader: bool,
    ) -> Result<super::super::RetentionTurn, JournalError> {
        if ticket.accepted() != ticket.applied() || ticket.committed() != ticket.applied() {
            return Ok(super::super::RetentionTurn {
                enabled: true,
                proposal: None,
                released: None,
            });
        }
        let planned = match self.plan_retention(ticket, now_millis, 1).await {
            Err(JournalError::AppendCapacity) => return Ok(deferred()),
            result => result?,
        };
        let Some((partition, plan)) = planned else {
            return Ok(super::super::RetentionTurn {
                enabled: false,
                proposal: None,
                released: None,
            });
        };
        // Recovery can leave unselected generations even when the selected log
        // fits its retention limit. Use the same selection and pin checks on
        // every settled turn; each class has a fixed removal budget.
        for kind in [
            super::StorageCleanup::Segments,
            super::StorageCleanup::Checkpoints,
            super::StorageCleanup::Indexes,
            super::StorageCleanup::Metadata,
        ] {
            self.cleanup_storage(ticket, kind, 8).await?;
        }
        let state = self
            .images()?
            .committed()
            .partition(partition)
            .ok_or(JournalError::Configuration)?;
        if plan.record_floor > state.retained_from {
            let proposal = if leader {
                match self.retention_proposal(partition, plan.record_floor, seed) {
                    Err(JournalError::AppendCapacity) => return Ok(deferred()),
                    result => result?,
                }
            } else {
                None
            };
            return Ok(super::super::RetentionTurn {
                enabled: true,
                proposal,
                released: None,
            });
        }
        let mut released = None;
        if !plan.retire.is_empty() {
            let previous = self.history.take();
            released = previous.as_ref().map(super::history::source);
            let max_read_bytes = self.limits.io.max_segment_bytes as usize;
            let retirement = self
                .retire_confirmed_history(
                    ticket,
                    CheckpointId::from_bytes(*seed.as_bytes()),
                    AsyncRetirementBudget {
                        max_segments: 1,
                        max_read_bytes,
                    },
                )
                .await;
            if matches!(retirement, Err(JournalError::AppendCapacity)) {
                self.history = previous;
                return Ok(deferred());
            }
            retirement?;
        } else if leader && plan.roll_active {
            let work = self.begin_roll(16)?;
            let done = work.publish().await;
            self.complete_roll(done)?;
        }
        Ok(super::super::RetentionTurn {
            enabled: true,
            proposal: None,
            released,
        })
    }
}

fn deferred() -> super::super::RetentionTurn {
    super::super::RetentionTurn {
        enabled: true,
        proposal: None,
        released: None,
    }
}
