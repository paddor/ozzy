use super::{Command, NativeIntake, NativeIntakeError, Reply, Request};
use crate::{
    frontend::Link,
    replica_actor::{ProposalOutcome, ProposalReply, response},
    replica_journal::{GroupAssignError, GroupProgressError, JournalError, ProposalBuffer},
};
use bytes::Bytes;
use omq_tokio::{Message, TrySendError};
use ozzy_journal::operation::{OperationKind, OperationLimits, decode_append_summary};
use ozzy_proto::{
    Envelope, EnvelopeLimits, NodeId, Opcode, ProducerId, append,
    data::DataLimits,
    group,
    nack::{AuthorityHint, RetryClass},
    producer, progress,
};
use std::{
    future::Future,
    task::{Context, Poll},
};

/// What polling a slot's proposal did.
enum Completion {
    Waiting,
    Replied,
    /// An invalid progress batch was held again to validate each update alone.
    Split,
}

/// What a slot visit leaves for its reply. Each carries whether the slot
/// changed.
pub(super) enum Visit {
    /// Nothing to send this turn.
    Done(bool),
    /// The reply may go now.
    Reply(bool),
}

impl NativeIntake {
    /// Settle the slot's proposal and drop a reply whose client left. The
    /// reply may go unless an earlier append of its writer is unanswered.
    pub(super) fn poll_slot(
        &mut self,
        index: usize,
        cx: &mut Context<'_>,
        current: &mut impl FnMut(NodeId) -> Option<Link>,
    ) -> Result<Visit, NativeIntakeError> {
        self.fence_stale(index, current);
        let changed = match self.complete_pending(index, cx)? {
            Completion::Waiting => false,
            Completion::Replied => true,
            Completion::Split => return Ok(Visit::Done(true)),
        };
        let Some(reply) = &self.slots[index].reply else {
            return Ok(Visit::Done(changed));
        };
        let envelope = reply.request.envelope;
        if !response::current_client(current(envelope.sender), &envelope) {
            finish(&mut self.slots[index]);
            self.busy -= 1;
            return Ok(Visit::Done(true));
        }
        if let (Command::Append(confirmed), ProposalOutcome::Committed { .. }) =
            (reply.request.command, &reply.outcome)
            && self.earlier_append_pending(index, confirmed)
        {
            // The SDK advances a contiguous writer prefix. Rotating slots
            // must not turn a later durable result into a needless retry.
            return Ok(Visit::Done(changed));
        }
        Ok(Visit::Reply(changed))
    }

    /// Fence a proposal whose client link changed before it was admitted.
    fn fence_stale(&self, index: usize, current: &mut impl FnMut(NodeId) -> Option<Link>) {
        // A progress proposal that carries other clients' updates outlives
        // its own client's link; the shared result still completes them.
        let carries = self.progress_flight == Some(index) && self.progress_riders != 0;
        if let Some(pending) = &self.slots[index].pending
            && !carries
            && !response::current_client(
                current(pending.request.envelope.sender),
                &pending.request.envelope,
            )
        {
            pending.proposal.fence_unadmitted();
        }
    }

    /// Turn a finished proposal into the slot's reply.
    fn complete_pending(
        &mut self,
        index: usize,
        cx: &mut Context<'_>,
    ) -> Result<Completion, NativeIntakeError> {
        let Some(pending) = &mut self.slots[index].pending else {
            return Ok(Completion::Waiting);
        };
        let Poll::Ready(result) = pending.proposal.as_mut().poll(cx) else {
            return Ok(Completion::Waiting);
        };
        let Some(pending) = self.slots[index].pending.take() else {
            return Ok(Completion::Waiting);
        };
        crate::profiling::finish(crate::profiling::Stage::NativeProposal, pending.started);
        let ProposalReply { outcome, buffer } = result?;
        let (outcome, buffer) = if self.progress_flight == Some(index) {
            match self.complete_progress(index, &pending, outcome, buffer)? {
                Some(completed) => completed,
                None => return Ok(Completion::Split),
            }
        } else {
            (outcome, buffer)
        };
        record_outcome(pending.request.command, &outcome);
        self.slots[index].buffer = Some(buffer);
        self.slots[index].reply = Some(Reply {
            request: pending.request,
            outcome,
            next: 0,
            frame: None,
        });
        Ok(Completion::Replied)
    }

    /// Send the slot's next reply frame, encoding it first if needed.
    pub(super) fn send_reply(
        &mut self,
        index: usize,
        cx: &mut Context<'_>,
        hint: AuthorityHint,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, NativeIntakeError> {
        if self.slots[index]
            .reply
            .as_ref()
            .is_some_and(|reply| reply.frame.is_none())
        {
            self.encode_result(index, hint)?;
        }
        let Some(reply) = &mut self.slots[index].reply else {
            return Ok(true);
        };
        let frame = reply.frame.take().ok_or(NativeIntakeError::Slot)?;
        match try_send(cx, frame) {
            Ok(()) => {
                reply.next += 1;
                // Prepare the next reply now, or release the request when
                // this was its last one. The shard can then settle it before
                // the reply leaves.
                self.encode_result(index, hint)?;
                Ok(true)
            }
            Err(TrySendError::Full(frame)) => {
                reply.frame = Some(frame);
                Ok(false)
            }
            Err(TrySendError::Closed) => Err(omq_tokio::Error::Closed.into()),
            Err(TrySendError::Error(error)) => Err(error.into()),
        }
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
        rejection: (u16, RetryClass),
        hint: AuthorityHint,
    ) -> Result<(), NativeIntakeError> {
        let frame = self.nack_frame(request, rejection, hint, None)?;
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
        rejection: (u16, RetryClass),
        hint: AuthorityHint,
        typed: Option<TypedDetail>,
    ) -> Result<Message, NativeIntakeError> {
        if rejection.1 == RetryClass::AfterBackoff {
            crate::profiling::event(crate::profiling::Event::NativeAdmissionRefusal);
        }
        let typed = typed.map(TypedDetail::encode).transpose()?;
        let nack = response::Nack {
            request: request.envelope,
            send: request.send.envelope,
            rejection,
            hint,
            detail: typed.as_deref().unwrap_or_default(),
        };
        Ok(response::nack_frame(
            self.config.local,
            &nack,
            &mut self.metadata,
        )?)
    }

    fn encode_result(
        &mut self,
        index: usize,
        hint: AuthorityHint,
    ) -> Result<(), NativeIntakeError> {
        let reply = self.slots[index]
            .reply
            .as_ref()
            .ok_or(NativeIntakeError::Slot)?;
        let request = reply.request;
        let rejection = match &reply.outcome {
            ProposalOutcome::Committed { scope, .. } => {
                let authority = response::authority(*scope);
                return self.encode_committed(index, request, authority);
            }
            _ if reply.next != 0 => {
                finish(&mut self.slots[index]);
                self.busy -= 1;
                return Ok(());
            }
            ProposalOutcome::NotAdmitted => (12, RetryClass::AfterAuthorityRefresh),
            ProposalOutcome::Unknown => (13, RetryClass::UnknownOutcome),
            ProposalOutcome::Invalid(error) => response::rejection(error),
        };
        let typed = TypedDetail::of(&reply.outcome);
        let frame = self.nack_frame(request, rejection, hint, typed)?;
        self.slots[index]
            .reply
            .as_mut()
            .ok_or(NativeIntakeError::Slot)?
            .frame = Some(frame);
        Ok(())
    }

    fn encode_committed(
        &mut self,
        index: usize,
        request: Request,
        authority: append::Authority,
    ) -> Result<(), NativeIntakeError> {
        let writer = self
            .writers
            .producer(index / (self.config.requests_per_writer + 1));
        let slot = &mut self.slots[index];
        let (Some(reply), Some(buffer)) = (slot.reply.as_mut(), slot.buffer.as_ref()) else {
            return Err(NativeIntakeError::Slot);
        };
        if reply.next != 0 && !matches!(request.command, Command::Append(_)) {
            finish(slot);
            self.busy -= 1;
            return Ok(());
        }
        let header = ReplyHeader {
            request: request.envelope,
            local: self.config.local,
            authority,
            send: request.send.envelope,
        };
        let metadata = &mut self.metadata;
        let encoded = match request.command {
            Command::Open => opened_header(buffer, &self.config, writer, header, metadata)?,
            Command::Append(confirmed) => {
                let Some(range) = result_range(buffer, reply.next, self.config.limits)? else {
                    finish(slot);
                    self.busy -= 1;
                    return Ok(());
                };
                appended_header(range, confirmed, header, metadata)?
            }
            Command::Assign => assignment_header(buffer, &self.config, header, metadata)?,
            Command::Progress => progress_header(buffer, &self.config, header, metadata)?,
        };
        reply.frame = Some(crate::native_frames::message(
            request.envelope.sender.as_bytes(),
            encoded,
            &self.metadata,
            Bytes::new(),
        ));
        Ok(())
    }
}

fn record_outcome(command: Command, outcome: &ProposalOutcome) {
    use crate::profiling::{Event, Level, event, log};
    if matches!(outcome, ProposalOutcome::Unknown) {
        log(
            Level::Warn,
            "native proposal completed with an unknown outcome",
        );
    }
    event(if matches!(outcome, ProposalOutcome::Committed { .. }) {
        Event::NativeProposalCommitted
    } else {
        Event::NativeProposalUnconfirmed
    });
    match command {
        Command::Progress => event(Event::ProcessingCompleted),
        Command::Assign => event(Event::AssignmentCompleted),
        Command::Open | Command::Append(_) => {}
    }
}

fn finish(slot: &mut super::Slot) {
    slot.reply = None;
    // Completed/stale responses retain no application payload. Journal and
    // transport aliases keep their own charge until their actual release.
    if let Some(buffer) = slot.buffer.as_mut() {
        buffer.clear();
    }
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
    fn of(outcome: &ProposalOutcome) -> Option<Self> {
        match outcome {
            ProposalOutcome::Invalid(JournalError::GroupProgress(
                GroupProgressError::HistoryExpired {
                    cursor,
                    retained_from,
                },
            )) => Some(Self::Expired(progress::HistoryExpired {
                cursor: *cursor,
                retained_from: *retained_from,
            })),
            ProposalOutcome::Invalid(JournalError::GroupAssign(
                GroupAssignError::InitialPosition {
                    cursor,
                    retained_from,
                    end,
                },
            )) => Some(Self::Start(group::InitialPosition {
                cursor: *cursor,
                retained_from: *retained_from,
                end: *end,
            })),
            _ => None,
        }
    }

    fn encode(self) -> Result<Vec<u8>, NativeIntakeError> {
        Ok(match self {
            Self::Expired(expired) => expired.encode()?.to_vec(),
            Self::Start(start) => start.encode()?.to_vec(),
        })
    }
}

/// The request, authority, and bounds a committed reply header answers.
#[derive(Clone, Copy)]
struct ReplyHeader {
    request: Envelope,
    local: NodeId,
    authority: append::Authority,
    send: EnvelopeLimits,
}

type Header = [u8; ozzy_proto::ENVELOPE_BYTES];

impl ReplyHeader {
    fn envelope(self, opcode: Opcode) -> Envelope {
        response::reply_envelope(self.request, self.local, opcode)
    }
}

fn opened_header(
    buffer: &ProposalBuffer,
    config: &super::NativeIntakeConfig,
    writer: Option<ProducerId>,
    header: ReplyHeader,
    metadata: &mut Vec<u8>,
) -> Result<Header, NativeIntakeError> {
    let opened = buffer.producer_opened().ok_or(NativeIntakeError::History)?;
    if opened.partition != config.partition
        || opened.policy != config.policy
        || Some(opened.producer) != writer
        || opened.authority.group_id != header.authority.group_id
        || opened.authority.config_epoch != header.authority.config_epoch
    {
        return Err(NativeIntakeError::History);
    }
    let opened = producer::Opened {
        authority: header.authority,
        ..opened
    };
    Ok(producer::encode_opened(
        header.envelope(Opcode::ProducerOpened),
        opened,
        metadata,
        header.send,
    )?)
}

fn appended_header(
    range: ozzy_core::state::ProducerResultSpan,
    mut confirmed: append::stream::Confirmed,
    header: ReplyHeader,
    metadata: &mut Vec<u8>,
) -> Result<Header, NativeIntakeError> {
    let sequence = range.first_sequence.get();
    let count = range.records;
    if sequence < confirmed.key.first_sequence
        || sequence
            .checked_add(count)
            .is_none_or(|end| end > confirmed.end_sequence)
    {
        return Err(NativeIntakeError::History);
    }
    confirmed.authority = header.authority;
    confirmed.key.first_sequence = sequence;
    confirmed.end_sequence = sequence + count;
    confirmed.first_offset = range.first_offset.get();
    confirmed.disposition = range.disposition();
    Ok(append::stream::encode_confirmed(
        header.envelope(Opcode::Appended),
        confirmed,
        metadata,
        header.send,
    )?)
}

fn assignment_header(
    buffer: &ProposalBuffer,
    config: &super::NativeIntakeConfig,
    header: ReplyHeader,
    metadata: &mut Vec<u8>,
) -> Result<Header, NativeIntakeError> {
    let state = buffer
        .group_assignment_state()
        .ok_or(NativeIntakeError::History)?;
    if state.partition != config.partition || state.policy != config.policy {
        return Err(NativeIntakeError::History);
    }
    let assigned = group::AssignReply {
        authority: header.authority,
        partition: state.partition,
        group: state.group,
        member: state.member,
        epoch: state.epoch,
        plan_revision: state.plan_revision,
        cursor: state.cursor,
        retained_from: state.retained_from,
        end: state.end,
        policy: state.policy,
    };
    Ok(group::encode_assigned(
        header.envelope(Opcode::GroupAssigned),
        assigned,
        metadata,
        header.send,
    )?)
}

fn progress_header(
    buffer: &ProposalBuffer,
    config: &super::NativeIntakeConfig,
    header: ReplyHeader,
    metadata: &mut Vec<u8>,
) -> Result<Header, NativeIntakeError> {
    let state = buffer
        .group_progress_state()
        .ok_or(NativeIntakeError::History)?;
    if state.assignment.partition != config.partition || state.policy != config.policy {
        return Err(NativeIntakeError::History);
    }
    let confirmed = progress::Confirmed {
        authority: header.authority,
        assignment: state.assignment,
        policy: state.policy,
        cursor: state.cursor,
        retained_from: state.retained_from,
        end: state.end,
    };
    Ok(progress::encode_confirmed(
        header.envelope(Opcode::ProgressCommitted),
        confirmed,
        metadata,
        header.send,
    )?)
}
