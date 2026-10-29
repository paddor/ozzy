//! The actor's proposal admission: the waiting submission with its deferral,
//! producers parked behind an unconfirmed original, and maintenance work.

use super::{Ingress, ProposalOutcome, Reply, Submission};
use crate::replica_journal::ProposalBuffer;
use std::collections::VecDeque;

/// Owner-local wait for confirmation or identity capacity. The existing
/// submission permit and arena stay held; no shared state is added.
#[derive(Debug, Clone, Copy)]
pub(in crate::replica_actor) struct ProposalWait {
    pub(in crate::replica_actor) validation: ozzy_replication::driver::ValidationTicket,
    pub(in crate::replica_actor) reason: crate::replica_journal::ProposalDeferral,
}

impl ProposalWait {
    pub(in crate::replica_actor) fn current(
        self,
        ticket: ozzy_replication::driver::ValidationTicket,
    ) -> bool {
        self.validation.scope() == ticket.scope()
            && self.validation.generation() == ticket.generation()
    }

    pub(in crate::replica_actor) fn ready(
        self,
        ticket: ozzy_replication::driver::ValidationTicket,
    ) -> bool {
        self.current(ticket)
            && match self.reason {
                crate::replica_journal::ProposalDeferral::Original(operation) => {
                    ticket.applied().op >= operation
                }
                crate::replica_journal::ProposalDeferral::IdentityCapacity => {
                    ticket.applied().op > self.validation.applied().op
                }
                crate::replica_journal::ProposalDeferral::MemoryCapacity => true,
            }
    }

    pub(in crate::replica_actor) fn capacity(self) -> bool {
        self.reason == crate::replica_journal::ProposalDeferral::IdentityCapacity
    }
}

/// Owner-local proposal admission shared by the local and replicated actors:
/// the one waiting submission with its deferral, producers parked behind an
/// unconfirmed original, the proposal in validation, and a pending
/// journal-built maintenance proposal. Each actor keeps its own window and
/// journal capacity checks between `next` and `start`.
#[derive(Debug, Default)]
pub(in crate::replica_actor) struct Proposals {
    waiting: Option<Submission>,
    wait: Option<ProposalWait>,
    parked: VecDeque<ParkedProducer>,
    maintenance: Option<ProposalBuffer>,
    validating: Option<Reply>,
}

/// One producer's proposals deferred behind its unconfirmed original. Other
/// producers keep admitting meanwhile. Its later proposals queue behind the
/// parked one, so its sequences reach validation in arrival order. Entries
/// keep their submission permits; ingress capacity bounds them.
#[derive(Debug)]
struct ParkedProducer {
    producer: ozzy_proto::ProducerId,
    /// The front's deferral; `None` lets the front validate now.
    wait: Option<ProposalWait>,
    queue: VecDeque<Submission>,
}

/// What the waiting slot offers this scheduling round.
#[derive(Debug)]
pub(in crate::replica_actor) enum NextProposal<'a> {
    /// Nothing waits or is queued.
    Idle,
    /// A fenced or stale proposal finished unadmitted.
    Finished,
    /// The waiting proposal's deferral has not cleared.
    Blocked,
    /// Validate when capacity allows, then `start` it.
    Ready(&'a Submission),
}

impl Proposals {
    /// Move the next queued submission into an empty waiting slot, behind
    /// nothing. Parked producers are not consulted; callers that cannot
    /// validate reject every held proposal afterwards.
    pub(in crate::replica_actor) fn fill(&mut self, ingress: &mut Ingress) {
        if self.waiting.is_none() {
            self.waiting = ingress.pop();
        }
    }

    /// Fill the slot from the oldest ready parked producer, else from ingress,
    /// finish a fenced or stale proposal, and check a deferral. The ticket is
    /// requested only while a parked or deferred proposal compares images.
    pub(in crate::replica_actor) fn next<E>(
        &mut self,
        ingress: &mut Ingress,
        ticket: impl Fn() -> Result<ozzy_replication::driver::ValidationTicket, E>,
    ) -> Result<NextProposal<'_>, E> {
        if self.waiting.is_none() && !self.parked.is_empty() && self.unpark(ticket()?) {
            return Ok(NextProposal::Finished);
        }
        if self.waiting.is_none() {
            self.pull(ingress);
        }
        let Some(waiting) = &self.waiting else {
            return Ok(NextProposal::Idle);
        };
        if waiting.reply.fenced() {
            self.reject_waiting();
            return Ok(NextProposal::Finished);
        }
        if let Some(wait) = self.wait {
            let ticket = ticket()?;
            if !wait.current(ticket) {
                self.reject_waiting();
                return Ok(NextProposal::Finished);
            }
            if !wait.ready(ticket) {
                return Ok(NextProposal::Blocked);
            }
            self.wait = None;
        }
        Ok(match &self.waiting {
            Some(waiting) => NextProposal::Ready(waiting),
            None => NextProposal::Idle,
        })
    }

    /// Move the oldest ready parked front into the empty waiting slot. A
    /// fenced or stale front finishes unadmitted instead; true reports that.
    fn unpark(&mut self, ticket: ozzy_replication::driver::ValidationTicket) -> bool {
        let mut index = 0;
        while let Some(parked) = self.parked.get_mut(index) {
            let Some(submission) = parked.queue.pop_front() else {
                // An emptied producer queue leaves the parked list.
                self.parked.remove(index);
                continue;
            };
            let stale = parked.wait.is_some_and(|wait| !wait.current(ticket));
            let ready = parked.wait.is_none_or(|wait| wait.ready(ticket));
            if !(stale || ready || submission.reply.fenced()) {
                parked.queue.push_front(submission);
                index += 1;
                continue;
            }
            parked.wait = None;
            if parked.queue.is_empty() {
                self.parked.remove(index);
            }
            if stale || submission.reply.fenced() {
                submission
                    .reply
                    .finish(submission.buffer, ProposalOutcome::NotAdmitted);
                return true;
            }
            self.waiting = Some(submission);
            return false;
        }
        false
    }

    /// Take the next ingress submission whose producer has nothing parked;
    /// one that does queues behind its producer's parked proposals.
    fn pull(&mut self, ingress: &mut Ingress) {
        while let Some(submission) = ingress.pop() {
            let producer = submission.buffer.producer();
            if let Some(parked) = self
                .parked
                .iter_mut()
                .find(|parked| Some(parked.producer) == producer)
            {
                parked.queue.push_back(submission);
            } else {
                self.waiting = Some(submission);
                return;
            }
        }
    }

    /// Take the ready waiting proposal into validation and return its arena.
    pub(in crate::replica_actor) fn start(&mut self) -> Option<ProposalBuffer> {
        assert!(self.wait.is_none());
        let mut submission = self.waiting.take()?;
        submission.reply.scheduled();
        assert!(self.validating.replace(submission.reply).is_none());
        Some(submission.buffer)
    }

    /// The pending maintenance proposal, before it starts.
    pub(in crate::replica_actor) fn maintenance(&self) -> Option<&ProposalBuffer> {
        self.maintenance.as_ref()
    }

    /// Take the pending maintenance proposal into validation, if any.
    pub(in crate::replica_actor) fn start_maintenance(&mut self) -> Option<ProposalBuffer> {
        let buffer = self.maintenance.take()?;
        assert!(self.validating.replace(Reply::maintenance()).is_none());
        Some(buffer)
    }

    /// The reply of the proposal whose validation just completed.
    pub(in crate::replica_actor) fn validated(&mut self) -> Option<Reply> {
        self.validating.take()
    }

    /// Hold a validated proposal until its deferral clears. A wait for one
    /// producer's unconfirmed original parks that producer, ahead of its
    /// queued proposals; a capacity wait holds the partition's slot.
    pub(in crate::replica_actor) fn defer(&mut self, submission: Submission, wait: ProposalWait) {
        assert!(self.waiting.is_none());
        let original = matches!(
            wait.reason,
            crate::replica_journal::ProposalDeferral::Original(_)
        );
        let Some(producer) = submission.buffer.producer().filter(|_| original) else {
            self.waiting = Some(submission);
            self.wait = Some(wait);
            return;
        };
        if let Some(parked) = self
            .parked
            .iter_mut()
            .find(|parked| parked.producer == producer)
        {
            parked.queue.push_front(submission);
            parked.wait = Some(wait);
        } else {
            self.parked.push_back(ParkedProducer {
                producer,
                wait: Some(wait),
                queue: VecDeque::from([submission]),
            });
        }
    }

    /// Hold a proposal whose validation went stale for the next image.
    pub(in crate::replica_actor) fn requeue(&mut self, submission: Submission) {
        assert!(self.wait.is_none());
        assert!(self.waiting.replace(submission).is_none());
    }

    /// Queue a journal-built proposal ahead of the waiting slot.
    pub(in crate::replica_actor) fn set_maintenance(&mut self, buffer: ProposalBuffer) {
        assert!(self.maintenance.replace(buffer).is_none());
    }

    /// Finish the waiting and parked proposals unadmitted and drop maintenance
    /// work. Returns how many proposals finished.
    pub(in crate::replica_actor) fn reject_all(&mut self) -> usize {
        self.maintenance = None;
        usize::from(self.reject_waiting()) + self.reject_parked()
    }

    /// After an authority change: drop maintenance work and finish deferred
    /// proposals, whose waits name the old image. An undeferred waiting
    /// proposal validates again under the new authority.
    pub(in crate::replica_actor) fn reject_deferred(&mut self) {
        self.maintenance = None;
        if self.wait.is_some() {
            self.reject_waiting();
        }
        self.reject_parked();
    }

    fn reject_parked(&mut self) -> usize {
        let mut finished = 0;
        for submission in self.parked.drain(..).flat_map(|parked| parked.queue) {
            submission
                .reply
                .finish(submission.buffer, ProposalOutcome::NotAdmitted);
            finished += 1;
        }
        finished
    }

    fn reject_waiting(&mut self) -> bool {
        self.wait = None;
        let Some(waiting) = self.waiting.take() else {
            return false;
        };
        waiting
            .reply
            .finish(waiting.buffer, ProposalOutcome::NotAdmitted);
        true
    }

    /// Expired identities made room: an identity-capacity wait retries.
    pub(in crate::replica_actor) fn release_capacity(&mut self) {
        if self.wait.is_some_and(ProposalWait::capacity) {
            self.wait = None;
        }
    }

    /// A journal-built proposal is queued or validating. The next retention
    /// turn waits for it, so its own proposal never overlaps this one.
    pub(in crate::replica_actor) fn holds_maintenance(&self) -> bool {
        self.maintenance.is_some() || self.validating.as_ref().is_some_and(Reply::is_maintenance)
    }

    /// A waiting proposal defers maintenance, unless it waits for identity
    /// capacity that only maintenance creates. Parked proposals wait for
    /// application, which maintenance never delays.
    pub(in crate::replica_actor) fn blocks_maintenance(&self) -> bool {
        self.waiting.is_some() && !self.wait.is_some_and(ProposalWait::capacity)
    }

    /// The slot is empty, so ingress may deliver the next submission.
    pub(in crate::replica_actor) fn is_idle(&self) -> bool {
        self.waiting.is_none()
    }
}
