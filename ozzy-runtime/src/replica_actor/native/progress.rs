//! Durable consumer progress: cursor updates held while one proposal is in
//! flight share the next proposal and each get its outcome.

use super::control::{PROGRESS, operation};
use super::{
    BatchState, Batched, NativeIntake, NativeIntakeError, NativeReceive, PROGRESS_BATCH, Pending,
    ProposalSubmitError, Reply, Request,
};
use crate::replica_actor::{ProposalOutcome, response};
use crate::replica_journal::{GroupProgressRequest, ProposalBuffer};
use crate::{frontend::Link, profiling::Event};
use ozzy_proto::{
    NodeId, Packet,
    append::Policy,
    nack::{AuthorityHint, RetryClass},
    progress,
};

/// Slots of the updates joined to one progress proposal.
type Riders = smallvec::SmallVec<[usize; PROGRESS_BATCH]>;

impl NativeIntake {
    pub(super) const fn progress_start(&self) -> usize {
        self.progress_start
    }

    pub(super) fn progress_work(&self, node: NodeId) -> usize {
        self.slots[self.progress_start()..]
            .iter()
            .filter(|slot| {
                slot.pending
                    .as_ref()
                    .is_some_and(|pending| pending.request.envelope.sender == node)
                    || slot
                        .reply
                        .as_ref()
                        .is_some_and(|reply| reply.request.envelope.sender == node)
                    || slot
                        .batch
                        .as_ref()
                        .is_some_and(|batched| batched.request.envelope.sender == node)
            })
            .count()
    }

    pub(super) fn receive_progress(
        &mut self,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let control = match self.admit_control(&packet, link, hint, &PROGRESS)? {
            Ok(control) => control,
            Err(received) => return Ok(received),
        };
        let Ok(progress) = progress::decode_request(packet, self.config.limits.envelope) else {
            return self.refuse(&control, (1, RetryClass::Permanent));
        };
        crate::profiling::event(Event::ProcessingReceived);
        crate::profiling::event(if progress.cursor.is_some() {
            Event::ProgressUpdateRequest
        } else {
            Event::ProgressQueryRequest
        });
        if let Some(rejection) = self.control_target(
            progress.assignment.partition,
            progress.authority,
            progress.policy,
            hint,
        ) {
            return self.refuse(&control, rejection);
        }
        let Some(operation) = operation(&packet) else {
            return self.refuse(&control, (1, RetryClass::Permanent));
        };
        let request = GroupProgressRequest {
            authority: progress.authority,
            assignment: progress.assignment,
            cursor: progress.cursor,
            operation,
        };
        let prepared = self.prepare_control(&control, |buffer| {
            buffer.prepare_group_progress(request, progress.policy)
        });
        match prepared {
            Ok(()) if progress.cursor.is_none() => {
                self.submit_prepared(control.slot, control.request, hint)
            }
            Ok(()) => self.submit_progress(control.slot, control.request, hint),
            Err(failure) => self.control_failure(&control, &PROGRESS, failure),
        }
    }

    /// Submit a cursor update now when no progress proposal is in flight.
    /// Otherwise hold it: the next proposal carries every update held by then,
    /// so one validation and replication round confirms them together.
    fn submit_progress(
        &mut self,
        slot: usize,
        request: Request,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        if self.progress_flight.is_some() {
            self.slots[slot].batch = Some(Batched {
                request,
                started: crate::profiling::start(),
                state: BatchState::Held { solo: false },
            });
            self.busy += 1;
            self.progress_held += 1;
            return Ok(NativeReceive::Accepted);
        }
        let received = self.submit_prepared(slot, request, hint)?;
        if self.slots[slot].pending.is_some() {
            self.progress_flight = Some(slot);
            #[cfg(test)]
            {
                self.progress_proposals += 1;
            }
        }
        Ok(received)
    }

    /// With no progress proposal in flight, send the first held update and
    /// join the other held ones it can carry.
    pub(super) fn dispatch_progress(
        &mut self,
        hint: AuthorityHint,
        current: &mut impl FnMut(NodeId) -> Option<Link>,
    ) -> Result<bool, NativeIntakeError> {
        if self.progress_flight.is_some() || self.progress_held == 0 {
            return Ok(false);
        }
        let Some(carrier) = self.first_live_held(current) else {
            return Ok(false);
        };
        let carrier_slot = &mut self.slots[carrier];
        let (Some(batched), Some(mut buffer)) =
            (carrier_slot.batch.take(), carrier_slot.buffer.take())
        else {
            return Err(NativeIntakeError::Slot);
        };
        self.progress_held -= 1;
        let original = buffer
            .group_progress_request()
            .ok_or(NativeIntakeError::History)?;
        let riders = if batched.state == (BatchState::Held { solo: false }) {
            self.join_riders(carrier, &mut buffer)?
        } else {
            Riders::new()
        };
        self.remaining = self.slots.len();
        match self.submitter.try_submit(buffer) {
            Ok(proposal) => {
                crate::profiling::event(crate::profiling::Event::NativeProposalSubmitted);
                #[cfg(test)]
                {
                    self.progress_proposals += 1;
                }
                self.slots[carrier].pending = Some(Pending {
                    request: batched.request,
                    proposal: Box::pin(proposal),
                    started: batched.started,
                });
                self.track(carrier);
                self.ride(carrier, &riders);
                Ok(true)
            }
            Err(mut rejected) => {
                crate::profiling::event(crate::profiling::Event::NativeProposalRefusal);
                rejected.buffer.clear();
                if rejected.reason == ProposalSubmitError::Full {
                    return self.hold_carrier(carrier, batched, rejected.buffer, original);
                }
                // Any other refusal rejects the carrier's own request.
                self.slots[carrier].buffer = Some(rejected.buffer);
                self.busy -= 1;
                self.reject(
                    carrier,
                    batched.request,
                    (10, RetryClass::AfterBackoff),
                    hint,
                )?;
                Ok(true)
            }
        }
    }

    /// Join held updates after `carrier` into its arena, up to the batch size.
    fn join_riders(
        &self,
        carrier: usize,
        buffer: &mut ProposalBuffer,
    ) -> Result<Riders, NativeIntakeError> {
        let mut riders = Riders::new();
        for index in carrier + 1..self.slots.len() {
            if riders.len() + 1 == self.config.progress_batch {
                break;
            }
            if self.slots[index]
                .batch
                .as_ref()
                .is_some_and(|rider| rider.state == BatchState::Held { solo: false })
                && let Some(held) = self.slots[index].buffer.as_ref()
                && buffer
                    .join_group_progress(held)
                    .map_err(|_| NativeIntakeError::History)?
            {
                riders.push(index);
            }
        }
        Ok(riders)
    }

    /// Mark the joined updates as riding in the carrier's proposal.
    fn ride(&mut self, carrier: usize, riders: &Riders) {
        self.progress_flight = Some(carrier);
        self.progress_riders = riders.len();
        self.progress_held -= riders.len();
        for (joined, &rider) in riders.iter().enumerate() {
            if let Some(batch) = self.slots[rider].batch.as_mut() {
                batch.state = BatchState::Riding {
                    carrier,
                    index: joined,
                };
            }
        }
    }

    /// A full lane holds a lone update again; joined ones stay held.
    fn hold_carrier(
        &mut self,
        carrier: usize,
        batched: Batched,
        mut buffer: ProposalBuffer,
        (original, policy): (GroupProgressRequest, Policy),
    ) -> Result<bool, NativeIntakeError> {
        buffer
            .prepare_group_progress(original, policy)
            .map_err(|_| NativeIntakeError::History)?;
        self.slots[carrier].buffer = Some(buffer);
        self.slots[carrier].batch = Some(batched);
        self.progress_held += 1;
        // A completion that frees the lane polls this intake again.
        Ok(false)
    }

    /// Drop held updates whose client link changed and return the first
    /// live one. Nothing of a dropped update was admitted.
    fn first_live_held(
        &mut self,
        current: &mut impl FnMut(NodeId) -> Option<Link>,
    ) -> Option<usize> {
        let mut carrier = None;
        for index in self.progress_start()..self.slots.len() {
            let Some(batched) = &self.slots[index].batch else {
                continue;
            };
            let envelope = batched.request.envelope;
            if !response::current_client(current(envelope.sender), &envelope) {
                self.slots[index].batch = None;
                if let Some(buffer) = self.slots[index].buffer.as_mut() {
                    buffer.clear();
                }
                self.busy -= 1;
                self.progress_held -= 1;
                continue;
            }
            carrier = carrier.or(Some(index));
        }
        carrier
    }

    /// The progress proposal in `carrier` finished. Its joined updates get
    /// the same outcome; a committed one adopts the coordinates validated for
    /// it. An invalid batch splits: every update validates alone for its own
    /// result, so the carrier is held again and `None` is returned.
    pub(super) fn complete_progress(
        &mut self,
        carrier: usize,
        pending: &Pending,
        outcome: ProposalOutcome,
        buffer: ProposalBuffer,
    ) -> Result<Option<(ProposalOutcome, ProposalBuffer)>, NativeIntakeError> {
        self.progress_flight = None;
        if std::mem::take(&mut self.progress_riders) == 0 {
            return Ok(Some((outcome, buffer)));
        }
        let riders = (0..self.slots.len())
            .filter_map(|slot| match self.slots[slot].batch {
                Some(Batched {
                    state: BatchState::Riding { carrier: of, index },
                    ..
                }) if of == carrier => Some((slot, index)),
                _ => None,
            })
            .collect::<smallvec::SmallVec<[_; PROGRESS_BATCH]>>();
        self.remaining = self.slots.len();
        if matches!(outcome, ProposalOutcome::Invalid(_)) {
            let slots = riders.iter().map(|&(slot, _)| slot).collect();
            self.split_batch(carrier, pending, &slots, buffer)?;
            return Ok(None);
        }
        for (slot, index) in riders {
            self.complete_rider(slot, index, &outcome, &buffer)?;
        }
        Ok(Some((outcome, buffer)))
    }

    /// Hold the carrier and its riders again, each to validate alone.
    fn split_batch(
        &mut self,
        carrier: usize,
        pending: &Pending,
        riders: &Riders,
        mut buffer: ProposalBuffer,
    ) -> Result<(), NativeIntakeError> {
        self.progress_held += riders.len() + 1;
        for &slot in riders {
            if let Some(batch) = self.slots[slot].batch.as_mut() {
                batch.state = BatchState::Held { solo: true };
            }
        }
        let (request, policy) = buffer
            .group_progress_request()
            .ok_or(NativeIntakeError::History)?;
        buffer.clear();
        buffer
            .prepare_group_progress(request, policy)
            .map_err(|_| NativeIntakeError::History)?;
        self.slots[carrier].buffer = Some(buffer);
        self.slots[carrier].batch = Some(Batched {
            request: pending.request,
            started: pending.started,
            state: BatchState::Held { solo: true },
        });
        Ok(())
    }

    /// Give the `index`th joined update the carrier's outcome.
    fn complete_rider(
        &mut self,
        slot: usize,
        index: usize,
        outcome: &ProposalOutcome,
        buffer: &ProposalBuffer,
    ) -> Result<(), NativeIntakeError> {
        use crate::profiling::{Event, Stage, event, finish};
        let committed = matches!(outcome, ProposalOutcome::Committed { .. });
        if committed {
            let state = buffer
                .group_progress_rider_state(index)
                .ok_or(NativeIntakeError::History)?;
            self.slots[slot]
                .buffer
                .as_mut()
                .ok_or(NativeIntakeError::Slot)?
                .adopt_group_progress_state(state)
                .map_err(|_| NativeIntakeError::History)?;
        }
        let rider = self.slots[slot]
            .batch
            .take()
            .ok_or(NativeIntakeError::Slot)?;
        finish(Stage::NativeProposal, rider.started);
        event(Event::ProcessingCompleted);
        event(if committed {
            Event::NativeProposalCommitted
        } else {
            Event::NativeProposalUnconfirmed
        });
        self.slots[slot].reply = Some(Reply {
            request: rider.request,
            outcome: match outcome {
                ProposalOutcome::Committed { scope, through } => ProposalOutcome::Committed {
                    scope: *scope,
                    through: *through,
                },
                ProposalOutcome::Unknown => ProposalOutcome::Unknown,
                _ => ProposalOutcome::NotAdmitted,
            },
            next: 0,
            frame: None,
        });
        self.track(slot);
        Ok(())
    }

    pub(super) fn submit_prepared(
        &mut self,
        slot: usize,
        request: Request,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let buffer = self.slots[slot]
            .buffer
            .take()
            .ok_or(NativeIntakeError::Slot)?;
        match self.submitter.try_submit(buffer) {
            Ok(proposal) => {
                crate::profiling::event(crate::profiling::Event::NativeProposalSubmitted);
                self.busy += 1;
                self.slots[slot].pending = Some(Pending {
                    request,
                    proposal: Box::pin(proposal),
                    started: crate::profiling::start(),
                });
                self.track(slot);
            }
            Err(mut rejected) => {
                crate::profiling::event(crate::profiling::Event::NativeProposalRefusal);
                let full = rejected.reason == ProposalSubmitError::Full;
                rejected.buffer.clear();
                self.slots[slot].buffer = Some(rejected.buffer);
                if full {
                    return Ok(NativeReceive::Busy);
                }
                self.reject(slot, request, (10, RetryClass::AfterBackoff), hint)?;
            }
        }
        Ok(NativeReceive::Accepted)
    }
}
