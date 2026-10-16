//! Durable consumer progress reuses native proposal and reply ownership.

use super::{
    BatchState, Batched, Command, NativeIntake, NativeIntakeError, NativeReceive, PROGRESS_BATCH,
    Pending, ProposalSubmitError, Reply, Request,
};
use crate::replica_actor::ProposalOutcome;
use crate::replica_journal::ProposalBuffer;
use crate::{
    frontend::Link,
    replica_journal::{GroupAssignRequest, GroupProgressRequest, JournalError},
};
use ozzy_proto::{
    NodeId, OperationId, Packet, group, handshake,
    nack::{AuthorityHint, RetryClass},
    progress,
};

impl NativeIntake {
    fn progress_start(&self) -> usize {
        self.config
            .access
            .required_buffers(self.config.requests_per_writer)
            .expect("validated native slots")
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
        let Some(slot) =
            (self.progress_start()..self.slots.len()).find(|&index| self.slot_free(index))
        else {
            if self.config.progress_requests != 0 {
                crate::profiling::event(crate::profiling::Event::ProcessingSlotRefusal);
            }
            return Ok(if self.config.progress_requests == 0 {
                NativeReceive::Ignored
            } else {
                NativeReceive::Busy
            });
        };
        let request = Request {
            envelope: packet.envelope,
            send: link.send,
            command: Command::Progress,
        };
        if link.send.envelope.max_metadata_bytes < progress::CONFIRMED_BYTES {
            return Ok(NativeReceive::Ignored);
        }
        self.remaining = self.slots.len();
        if link.remote.roles & handshake::CONSUMER == 0
            || link.remote.capabilities & handshake::OWNER_PROGRESS == 0
        {
            self.reject(slot, request, 3, RetryClass::Permanent, hint)?;
            return Ok(NativeReceive::Accepted);
        }
        let Ok(progress) = progress::decode_request(packet, self.config.limits.envelope) else {
            self.reject(slot, request, 1, RetryClass::Permanent, hint)?;
            return Ok(NativeReceive::Accepted);
        };
        crate::profiling::event(crate::profiling::Event::ProcessingReceived);
        #[cfg(feature = "storage-metrics")]
        crate::storage_metrics::add(
            if progress.cursor.is_some() {
                &crate::storage_metrics::PROGRESS_UPDATES
            } else {
                &crate::storage_metrics::PROGRESS_QUERIES
            },
            1,
        );
        if progress.assignment.partition != self.config.partition
            || progress.authority.group_id != self.config.group
            || progress.authority != hint.authority
            || hint.primary != self.config.local
        {
            self.reject(slot, request, 5, RetryClass::AfterAuthorityRefresh, hint)?;
            return Ok(NativeReceive::Accepted);
        }
        if progress.policy != self.config.policy {
            self.reject(slot, request, 2, RetryClass::Permanent, hint)?;
            return Ok(NativeReceive::Accepted);
        }
        let buffer = self.slots[slot]
            .buffer
            .as_mut()
            .expect("free progress arena");
        buffer.clear();
        let result = buffer.prepare_group_progress(
            GroupProgressRequest {
                authority: progress.authority,
                assignment: progress.assignment,
                cursor: progress.cursor,
                operation: OperationId::from_bytes(
                    *packet
                        .envelope
                        .request_id
                        .expect("correlated progress")
                        .as_bytes(),
                ),
            },
            progress.policy,
        );
        match result {
            Ok(()) if progress.cursor.is_none() => self.submit_prepared(slot, request, hint),
            Ok(()) => self.submit_progress(slot, request, hint),
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                crate::profiling::event(crate::profiling::Event::ProcessingArenaRefusal);
                self.slots[slot].buffer.as_mut().unwrap().clear();
                Ok(NativeReceive::Busy)
            }
            Err(error) => {
                self.slots[slot].buffer.as_mut().unwrap().clear();
                let (code, retry) = super::super::response::rejection(&error);
                self.reject(slot, request, code, retry, hint)?;
                Ok(NativeReceive::Accepted)
            }
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
        let batched = self.slots[carrier].batch.take().expect("held update");
        self.progress_held -= 1;
        let mut buffer = self.slots[carrier].buffer.take().expect("held arena");
        let (original, policy) = buffer
            .group_progress_request()
            .ok_or(NativeIntakeError::History)?;
        let mut riders = smallvec::SmallVec::<[usize; PROGRESS_BATCH]>::new();
        if batched.state == (BatchState::Held { solo: false }) {
            for index in carrier + 1..self.slots.len() {
                if riders.len() + 1 == self.config.progress_batch {
                    break;
                }
                if self.slots[index]
                    .batch
                    .as_ref()
                    .is_some_and(|rider| rider.state == BatchState::Held { solo: false })
                    && buffer
                        .join_group_progress(self.slots[index].buffer.as_ref().expect("held arena"))
                        .map_err(|_| NativeIntakeError::History)?
                {
                    riders.push(index);
                }
            }
        }
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
                self.progress_flight = Some(carrier);
                self.progress_riders = riders.len();
                self.progress_held -= riders.len();
                for (joined, &rider) in riders.iter().enumerate() {
                    self.slots[rider]
                        .batch
                        .as_mut()
                        .expect("joined update")
                        .state = BatchState::Riding {
                        carrier,
                        index: joined,
                    };
                }
            }
            Err(mut rejected) => {
                crate::profiling::event(crate::profiling::Event::NativeProposalRefusal);
                // Restore a lone held update; joined ones stay held.
                rejected.buffer.clear();
                if rejected.reason == ProposalSubmitError::Full {
                    rejected
                        .buffer
                        .prepare_group_progress(original, policy)
                        .map_err(|_| NativeIntakeError::History)?;
                    self.slots[carrier].buffer = Some(rejected.buffer);
                    self.slots[carrier].batch = Some(batched);
                    self.progress_held += 1;
                    // A completion that frees the lane polls this intake again.
                    return Ok(false);
                }
                self.slots[carrier].buffer = Some(rejected.buffer);
                self.busy -= 1;
                self.reject(carrier, batched.request, 10, RetryClass::AfterBackoff, hint)?;
            }
        }
        Ok(true)
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
            if !current(envelope.sender).is_some_and(|link| {
                link.binding.kind == crate::frontend::Kind::Client
                    && Some(link.binding.session) == envelope.session
                    && link.binding.peer == envelope.sender
            }) {
                self.slots[index].batch = None;
                self.slots[index]
                    .buffer
                    .as_mut()
                    .expect("held arena")
                    .clear();
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
        mut buffer: ProposalBuffer,
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
        let shared = match &outcome {
            ProposalOutcome::Committed { scope, through } => ProposalOutcome::Committed {
                scope: *scope,
                through: *through,
            },
            ProposalOutcome::NotAdmitted => ProposalOutcome::NotAdmitted,
            ProposalOutcome::Unknown => ProposalOutcome::Unknown,
            ProposalOutcome::Invalid(_) => {
                self.progress_held += riders.len() + 1;
                for (slot, _) in riders {
                    self.slots[slot].batch.as_mut().expect("rider").state =
                        BatchState::Held { solo: true };
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
                return Ok(None);
            }
        };
        for (slot, index) in riders {
            if matches!(shared, ProposalOutcome::Committed { .. }) {
                let state = buffer
                    .group_progress_rider_state(index)
                    .ok_or(NativeIntakeError::History)?;
                self.slots[slot]
                    .buffer
                    .as_mut()
                    .expect("rider arena")
                    .adopt_group_progress_state(state)
                    .map_err(|_| NativeIntakeError::History)?;
            }
            let rider = self.slots[slot].batch.take().expect("rider");
            crate::profiling::finish(crate::profiling::Stage::NativeProposal, rider.started);
            crate::profiling::event(crate::profiling::Event::ProcessingCompleted);
            crate::profiling::event(if matches!(shared, ProposalOutcome::Committed { .. }) {
                crate::profiling::Event::NativeProposalCommitted
            } else {
                crate::profiling::Event::NativeProposalUnconfirmed
            });
            self.slots[slot].reply = Some(Reply {
                request: rider.request,
                outcome: match &shared {
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
        }
        Ok(Some((outcome, buffer)))
    }

    /// Relay one committed coordinator target to this source. The source
    /// allocates the epoch and captures the cursor; preparation proves nothing.
    pub(super) fn receive_assign(
        &mut self,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let Some(slot) =
            (self.progress_start()..self.slots.len()).find(|&index| self.slot_free(index))
        else {
            if self.config.progress_requests != 0 {
                crate::profiling::event(crate::profiling::Event::AssignmentSlotRefusal);
            }
            return Ok(if self.config.progress_requests == 0 {
                NativeReceive::Ignored
            } else {
                NativeReceive::Busy
            });
        };
        let request = Request {
            envelope: packet.envelope,
            send: link.send,
            command: Command::Assign,
        };
        if link.send.envelope.max_metadata_bytes < group::ASSIGNED_BYTES {
            return Ok(NativeReceive::Ignored);
        }
        self.remaining = self.slots.len();
        if link.remote.roles & handshake::CONSUMER == 0
            || link.remote.capabilities & handshake::OWNER_GROUPS == 0
        {
            self.reject(slot, request, 3, RetryClass::Permanent, hint)?;
            return Ok(NativeReceive::Accepted);
        }
        let Ok(assign) = group::decode_assign(packet, self.config.limits.envelope) else {
            self.reject(slot, request, 1, RetryClass::Permanent, hint)?;
            return Ok(NativeReceive::Accepted);
        };
        if assign.partition != self.config.partition
            || assign.authority.group_id != self.config.group
            || assign.authority != hint.authority
            || hint.primary != self.config.local
        {
            self.reject(slot, request, 5, RetryClass::AfterAuthorityRefresh, hint)?;
            return Ok(NativeReceive::Accepted);
        }
        if assign.policy != self.config.policy {
            self.reject(slot, request, 2, RetryClass::Permanent, hint)?;
            return Ok(NativeReceive::Accepted);
        }
        let buffer = self.slots[slot]
            .buffer
            .as_mut()
            .expect("free consumer control arena");
        buffer.clear();
        let result = buffer.prepare_group_assignment(
            GroupAssignRequest {
                authority: assign.authority,
                group: assign.group,
                partition: assign.partition,
                plan_revision: assign.plan_revision,
                member: assign.member,
                initial: assign.initial,
                operation: OperationId::from_bytes(
                    *packet
                        .envelope
                        .request_id
                        .expect("correlated activation")
                        .as_bytes(),
                ),
            },
            assign.policy,
        );
        match result {
            Ok(()) => self.submit_prepared(slot, request, hint),
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                crate::profiling::event(crate::profiling::Event::AssignmentArenaRefusal);
                self.slots[slot].buffer.as_mut().unwrap().clear();
                Ok(NativeReceive::Busy)
            }
            Err(error) => {
                self.slots[slot].buffer.as_mut().unwrap().clear();
                let (code, retry) = super::super::response::rejection(&error);
                self.reject(slot, request, code, retry, hint)?;
                Ok(NativeReceive::Accepted)
            }
        }
    }

    pub(super) fn submit_prepared(
        &mut self,
        slot: usize,
        request: Request,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let buffer = self.slots[slot].buffer.take().expect("free native arena");
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
                self.reject(slot, request, 10, RetryClass::AfterBackoff, hint)?;
            }
        }
        Ok(NativeReceive::Accepted)
    }
}
