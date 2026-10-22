//! Proposal completion and the replies it leaves in a slot.

mod headers;

use super::{Command, NativeIntake, NativeIntakeError, Reply, Request};
use crate::{
    frontend::Link,
    replica_actor::{ProposalOutcome, ProposalReply, response},
    replica_journal::{GroupAssignError, GroupProgressError, JournalError},
};
use bytes::Bytes;
use headers::{
    ReplyHeader, appended_header, assignment_header, opened_header, progress_header, result_range,
};
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{
    NodeId, append, group,
    nack::{AuthorityHint, RetryClass},
    progress,
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
        let stride = self.stride();
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
        if !self.slots[index].active() {
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
        let writer = self.writers.producer(index / self.stride());
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
