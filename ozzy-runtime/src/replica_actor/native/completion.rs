use super::{Command, NativeIntake, NativeIntakeError, Reply, Request};
use crate::{
    frontend::{Kind, Link},
    replica_actor::{ProposalOutcome, ProposalReply},
    replica_journal::{GroupAssignError, GroupProgressError, JournalError, ProposalBuffer},
};
use bytes::Bytes;
use omq_tokio::{Message, TrySendError};
use ozzy_journal::operation::{OperationKind, OperationLimits, decode_append_summary};
use ozzy_proto::{
    Envelope, NodeId, Opcode, append,
    data::DataLimits,
    group,
    nack::{self, AuthorityHint, RetryClass},
    producer, progress,
};
use std::{
    future::Future,
    task::{Context, Poll},
};

impl NativeIntake {
    pub(super) fn poll_slot(
        &mut self,
        index: usize,
        cx: &mut Context<'_>,
        hint: AuthorityHint,
        current: &mut impl FnMut(NodeId) -> Option<Link>,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, NativeIntakeError> {
        let mut changed = false;
        // A progress proposal that carries other clients' updates outlives
        // its own client's link; the shared result still completes them.
        let carries = self.progress_flight == Some(index) && self.progress_riders != 0;
        if let Some(pending) = &self.slots[index].pending
            && !carries
            && !current(pending.request.envelope.sender).is_some_and(|link| {
                link.binding.kind == Kind::Client
                    && Some(link.binding.session) == pending.request.envelope.session
                    && link.binding.peer == pending.request.envelope.sender
            })
        {
            pending.proposal.fence_unadmitted();
        }
        if let Some(pending) = &mut self.slots[index].pending
            && let Poll::Ready(result) = pending.proposal.as_mut().poll(cx)
        {
            let pending = self.slots[index]
                .pending
                .take()
                .expect("completed proposal");
            crate::profiling::finish(crate::profiling::Stage::NativeProposal, pending.started);
            let ProposalReply { outcome, buffer } = result?;
            let (outcome, buffer) = if self.progress_flight == Some(index) {
                match self.complete_progress(index, &pending, outcome, buffer)? {
                    Some(completed) => completed,
                    None => return Ok(true),
                }
            } else {
                (outcome, buffer)
            };
            if matches!(outcome, ProposalOutcome::Unknown) {
                crate::profiling::log(
                    crate::profiling::Level::Warn,
                    "native proposal completed with an unknown outcome",
                );
            }
            crate::profiling::event(if matches!(outcome, ProposalOutcome::Committed { .. }) {
                crate::profiling::Event::NativeProposalCommitted
            } else {
                crate::profiling::Event::NativeProposalUnconfirmed
            });
            match pending.request.command {
                Command::Progress => {
                    crate::profiling::event(crate::profiling::Event::ProcessingCompleted);
                }
                Command::Assign => {
                    crate::profiling::event(crate::profiling::Event::AssignmentCompleted);
                }
                Command::Open | Command::Append(_) => {}
            }
            self.slots[index].buffer = Some(buffer);
            self.slots[index].reply = Some(Reply {
                request: pending.request,
                outcome,
                next: 0,
                frame: None,
            });
            changed = true;
        }
        let Some(reply) = &self.slots[index].reply else {
            return Ok(changed);
        };
        let envelope = reply.request.envelope;
        if !current(envelope.sender).is_some_and(|link| {
            link.binding.kind == Kind::Client
                && Some(link.binding.session) == envelope.session
                && link.binding.peer == envelope.sender
        }) {
            finish(&mut self.slots[index]);
            self.busy -= 1;
            return Ok(true);
        }
        if let (Command::Append(confirmed), ProposalOutcome::Committed { .. }) =
            (reply.request.command, &reply.outcome)
            && self.earlier_append_pending(index, confirmed)
        {
            // The SDK advances a contiguous writer prefix. Rotating slots
            // must not turn a later durable result into a needless retry.
            return Ok(changed);
        }
        if self.slots[index].reply.as_ref().unwrap().frame.is_none() {
            self.encode_result(index, hint)?;
        }
        let Some(reply) = &mut self.slots[index].reply else {
            return Ok(true);
        };
        let frame = reply.frame.take().expect("encoded reply");
        match try_send(cx, frame) {
            Ok(()) => {
                reply.next += 1;
                changed = true;
                // Prepare the next reply now, or release the request when
                // this was its last one. The shard can then settle it before
                // the reply leaves.
                self.encode_result(index, hint)?;
            }
            Err(TrySendError::Full(frame)) => reply.frame = Some(frame),
            Err(TrySendError::Closed) => return Err(omq_tokio::Error::Closed.into()),
            Err(TrySendError::Error(error)) => return Err(error.into()),
        }
        Ok(changed)
    }

    fn earlier_append_pending(&self, index: usize, confirmed: append::stream::Confirmed) -> bool {
        let stride = self.config.requests_per_writer + 1;
        let first = index / stride * stride;
        self.slots[first + 1..first + stride].iter().any(|slot| {
            let request = slot
                .pending
                .as_ref()
                .map(|pending| pending.request)
                .or_else(|| slot.reply.as_ref().map(|reply| reply.request));
            matches!(request.map(|request| request.command), Some(Command::Append(earlier))
                if earlier.key.producer_epoch == confirmed.key.producer_epoch
                    && earlier.key.first_sequence < confirmed.key.first_sequence)
        })
    }

    pub(super) fn reject(
        &mut self,
        index: usize,
        request: Request,
        code: u16,
        retry: RetryClass,
        hint: AuthorityHint,
    ) -> Result<(), NativeIntakeError> {
        let frame = self.nack_frame(request, code, retry, hint, None)?;
        if self.slots[index].pending.is_none() && self.slots[index].reply.is_none() {
            self.busy += 1;
        }
        self.slots[index].reply = Some(Reply {
            request,
            outcome: ProposalOutcome::NotAdmitted,
            next: 0,
            frame: Some(frame),
        });
        self.track(index);
        Ok(())
    }

    fn nack_frame(
        &mut self,
        request: Request,
        code: u16,
        retry: RetryClass,
        hint: AuthorityHint,
        typed: Option<TypedDetail>,
    ) -> Result<Message, NativeIntakeError> {
        if retry == RetryClass::AfterBackoff {
            crate::profiling::event(crate::profiling::Event::NativeAdmissionRefusal);
        }
        let detail = if matches!(code, 5 | 12 | 13) {
            Some(hint.encode()?)
        } else {
            None
        };
        let typed = typed.map(TypedDetail::encode).transpose()?;
        let header = nack::encode(
            Envelope {
                opcode: Opcode::Nack,
                response: true,
                sender: self.config.local,
                ..request.envelope
            },
            nack::Nack {
                code,
                retry,
                detail: detail
                    .as_ref()
                    .map(<[u8; 48]>::as_slice)
                    .or(typed.as_deref())
                    .unwrap_or_default(),
                diagnostic: "",
            },
            &mut self.metadata,
            request.send.envelope,
        )?;
        Ok(crate::native_frames::message(
            request.envelope.sender.as_bytes(),
            header,
            &self.metadata,
            Bytes::new(),
        ))
    }

    fn encode_result(
        &mut self,
        index: usize,
        hint: AuthorityHint,
    ) -> Result<(), NativeIntakeError> {
        let reply = self.slots[index].reply.as_ref().expect("ready reply");
        let request = reply.request;
        if let ProposalOutcome::Committed { scope, .. } = reply.outcome {
            let authority = append::Authority {
                group_id: scope.group_id,
                config_epoch: scope.configuration_epoch,
                view: scope.view,
            };
            self.encode_committed(index, request, authority)?;
        } else {
            if reply.next != 0 {
                finish(&mut self.slots[index]);
                self.busy -= 1;
                return Ok(());
            }
            let (code, retry) = match &reply.outcome {
                ProposalOutcome::NotAdmitted => (12, RetryClass::AfterAuthorityRefresh),
                ProposalOutcome::Unknown => (13, RetryClass::UnknownOutcome),
                ProposalOutcome::Invalid(error) => super::super::response::rejection(error),
                ProposalOutcome::Committed { .. } => unreachable!(),
            };
            let typed = match &reply.outcome {
                ProposalOutcome::Invalid(JournalError::GroupProgress(
                    GroupProgressError::HistoryExpired {
                        cursor,
                        retained_from,
                    },
                )) => Some(TypedDetail::Expired(progress::HistoryExpired {
                    cursor: *cursor,
                    retained_from: *retained_from,
                })),
                ProposalOutcome::Invalid(JournalError::GroupAssign(
                    GroupAssignError::InitialPosition {
                        cursor,
                        retained_from,
                        end,
                    },
                )) => Some(TypedDetail::Start(group::InitialPosition {
                    cursor: *cursor,
                    retained_from: *retained_from,
                    end: *end,
                })),
                _ => None,
            };
            let frame = self.nack_frame(request, code, retry, hint, typed)?;
            self.slots[index].reply.as_mut().unwrap().frame = Some(frame);
        }
        Ok(())
    }

    fn encode_committed(
        &mut self,
        index: usize,
        request: Request,
        authority: append::Authority,
    ) -> Result<(), NativeIntakeError> {
        let slot = &mut self.slots[index];
        let reply = slot.reply.as_mut().expect("ready reply");
        let buffer = slot.buffer.as_ref().expect("returned arena");
        let envelope = Envelope {
            response: true,
            sender: self.config.local,
            ..request.envelope
        };
        if reply.next != 0 && !matches!(request.command, Command::Append(_)) {
            finish(slot);
            self.busy -= 1;
            return Ok(());
        }
        let header = match request.command {
            Command::Open => {
                let opened = buffer.producer_opened().ok_or(NativeIntakeError::History)?;
                if opened.partition != self.config.partition
                    || opened.policy != self.config.policy
                    || Some(opened.producer)
                        != self
                            .writers
                            .producer(index / (self.config.requests_per_writer + 1))
                    || opened.authority.group_id != authority.group_id
                    || opened.authority.config_epoch != authority.config_epoch
                {
                    return Err(NativeIntakeError::History);
                }
                producer::encode_opened(
                    Envelope {
                        opcode: Opcode::ProducerOpened,
                        ..envelope
                    },
                    producer::Opened {
                        authority,
                        ..opened
                    },
                    &mut self.metadata,
                    request.send.envelope,
                )?
            }
            Command::Append(mut confirmed) => {
                let range = result_range(buffer, reply.next, self.config.limits)?;
                let Some(range) = range else {
                    finish(slot);
                    self.busy -= 1;
                    return Ok(());
                };
                let sequence = range.first_sequence.get();
                let count = range.records;
                if sequence < confirmed.key.first_sequence
                    || sequence
                        .checked_add(count)
                        .is_none_or(|end| end > confirmed.end_sequence)
                {
                    return Err(NativeIntakeError::History);
                }
                confirmed.authority = authority;
                confirmed.key.first_sequence = sequence;
                confirmed.end_sequence = sequence + count;
                confirmed.first_offset = range.first_offset.get();
                confirmed.disposition = range.disposition();
                append::stream::encode_confirmed(
                    Envelope {
                        opcode: Opcode::Appended,
                        ..envelope
                    },
                    confirmed,
                    &mut self.metadata,
                    request.send.envelope,
                )?
            }
            Command::Assign => assignment_header(
                buffer,
                &self.config,
                envelope,
                authority,
                request.send.envelope,
                &mut self.metadata,
            )?,
            Command::Progress => progress_header(
                buffer,
                &self.config,
                envelope,
                authority,
                request.send.envelope,
                &mut self.metadata,
            )?,
        };
        reply.frame = Some(crate::native_frames::message(
            request.envelope.sender.as_bytes(),
            header,
            &self.metadata,
            Bytes::new(),
        ));
        Ok(())
    }
}

fn finish(slot: &mut super::Slot) {
    slot.reply = None;
    // Completed/stale responses retain no application payload. Journal and
    // transport aliases keep their own charge until their actual release.
    slot.buffer.as_mut().expect("returned arena").clear();
}

fn result_range(
    buffer: &ProposalBuffer,
    index: usize,
    limits: DataLimits,
) -> Result<Option<ozzy_core::state::ProducerResultSpan>, NativeIntakeError> {
    let retained = buffer.producer_retry_results();
    if !retained.is_empty() {
        return Ok(retained.get(index).copied());
    }
    if index != 0 {
        return Ok(None);
    }
    let batch = if let Some(batch) = buffer.validated_producer_summary(0) {
        batch
    } else {
        let Some((OperationKind::Append, body)) = buffer.bodies().next() else {
            return Err(NativeIntakeError::History);
        };
        let summary = decode_append_summary(
            body,
            OperationLimits {
                max_body_bytes: buffer.limits().max_body_bytes,
                max_append_batches: 1,
                max_records: limits.max_records,
                max_parts: limits.max_parts,
                max_payload_bytes: limits.envelope.max_payload_bytes,
                ..OperationLimits::default()
            },
        )
        .map_err(|_| NativeIntakeError::History)?;
        let [batch] = summary.batches() else {
            return Err(NativeIntakeError::History);
        };
        *batch
    };
    Ok(Some(ozzy_core::state::ProducerResultSpan {
        first_sequence: batch.first_sequence,
        first_offset: batch.first_offset,
        records: batch.record_count as u64,
        source: ozzy_core::state::ProducerResultSource::Stored,
    }))
}

/// Code-specific NACK details carried beside authority hints.
#[derive(Clone, Copy, Debug)]
pub(super) enum TypedDetail {
    Expired(progress::HistoryExpired),
    Start(group::InitialPosition),
}

impl TypedDetail {
    fn encode(self) -> Result<Vec<u8>, NativeIntakeError> {
        Ok(match self {
            Self::Expired(expired) => expired.encode()?.to_vec(),
            Self::Start(start) => start.encode()?.to_vec(),
        })
    }
}

fn assignment_header(
    buffer: &ProposalBuffer,
    config: &super::NativeIntakeConfig,
    envelope: Envelope,
    authority: append::Authority,
    send: ozzy_proto::EnvelopeLimits,
    metadata: &mut Vec<u8>,
) -> Result<[u8; ozzy_proto::ENVELOPE_BYTES], NativeIntakeError> {
    let state = buffer
        .group_assignment_state()
        .ok_or(NativeIntakeError::History)?;
    if state.partition != config.partition || state.policy != config.policy {
        return Err(NativeIntakeError::History);
    }
    Ok(group::encode_assigned(
        Envelope {
            opcode: Opcode::GroupAssigned,
            ..envelope
        },
        group::AssignReply {
            authority,
            partition: state.partition,
            group: state.group,
            member: state.member,
            epoch: state.epoch,
            plan_revision: state.plan_revision,
            cursor: state.cursor,
            retained_from: state.retained_from,
            end: state.end,
            policy: state.policy,
        },
        metadata,
        send,
    )?)
}

fn progress_header(
    buffer: &ProposalBuffer,
    config: &super::NativeIntakeConfig,
    envelope: Envelope,
    authority: append::Authority,
    send: ozzy_proto::EnvelopeLimits,
    metadata: &mut Vec<u8>,
) -> Result<[u8; ozzy_proto::ENVELOPE_BYTES], NativeIntakeError> {
    let state = buffer
        .group_progress_state()
        .ok_or(NativeIntakeError::History)?;
    if state.assignment.partition != config.partition || state.policy != config.policy {
        return Err(NativeIntakeError::History);
    }
    Ok(progress::encode_confirmed(
        Envelope {
            opcode: Opcode::ProgressCommitted,
            ..envelope
        },
        progress::Confirmed {
            authority,
            assignment: state.assignment,
            policy: state.policy,
            cursor: state.cursor,
            retained_from: state.retained_from,
            end: state.end,
        },
        metadata,
        send,
    )?)
}
