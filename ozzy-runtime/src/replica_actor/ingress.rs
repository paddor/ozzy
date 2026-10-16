//! Startup-registered fanring lanes with a global outstanding-request bound.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use ozzy_replication::{JournalGeneration, PipelineLimits, Prefix, Scope};
use pin_project_lite::pin_project;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use crate::command_channel as mpsc;
use crate::replica_journal::{JournalError, ProposalBuffer};
use crate::signal::{CloseSignal, Closed};

/// Owner-local wait for confirmation or identity capacity. The existing
/// submission permit and arena stay held; no shared state is added.
#[derive(Debug, Clone, Copy)]
pub(super) struct ProposalWait {
    pub(super) validation: ozzy_replication::driver::ValidationTicket,
    pub(super) reason: crate::replica_journal::ProposalDeferral,
}

impl ProposalWait {
    pub(super) fn current(self, ticket: ozzy_replication::driver::ValidationTicket) -> bool {
        self.validation.scope() == ticket.scope()
            && self.validation.generation() == ticket.generation()
    }

    pub(super) fn ready(self, ticket: ozzy_replication::driver::ValidationTicket) -> bool {
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

    pub(super) fn capacity(self) -> bool {
        self.reason == crate::replica_journal::ProposalDeferral::IdentityCapacity
    }
}

/// Owner-local proposal admission shared by the local and replicated actors:
/// the one waiting submission with its deferral, producers parked behind an
/// unconfirmed original, the proposal in validation, and a pending
/// journal-built maintenance proposal. Each actor keeps its own window and
/// journal capacity checks between `next` and `start`.
#[derive(Debug, Default)]
pub(super) struct Proposals {
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
pub(super) enum NextProposal<'a> {
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
    pub(super) fn fill(&mut self, ingress: &mut Ingress) {
        if self.waiting.is_none() {
            self.waiting = ingress.pop();
        }
    }

    /// Fill the slot from the oldest ready parked producer, else from ingress,
    /// finish a fenced or stale proposal, and check a deferral. The ticket is
    /// requested only while a parked or deferred proposal compares images.
    pub(super) fn next<E>(
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
        Ok(NextProposal::Ready(
            self.waiting.as_ref().expect("waiting proposal"),
        ))
    }

    /// Move the oldest ready parked front into the empty waiting slot. A
    /// fenced or stale front finishes unadmitted instead; true reports that.
    fn unpark(&mut self, ticket: ozzy_replication::driver::ValidationTicket) -> bool {
        let mut index = 0;
        while let Some(parked) = self.parked.get_mut(index) {
            let front = parked.queue.front().expect("nonempty parked queue");
            let stale = parked.wait.is_some_and(|wait| !wait.current(ticket));
            let ready = parked.wait.is_none_or(|wait| wait.ready(ticket));
            if !(stale || ready || front.reply.fenced()) {
                index += 1;
                continue;
            }
            parked.wait = None;
            let submission = parked.queue.pop_front().expect("checked front");
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
    pub(super) fn start(&mut self) -> ProposalBuffer {
        assert!(self.wait.is_none());
        let mut submission = self.waiting.take().expect("ready proposal");
        submission.reply.scheduled();
        assert!(self.validating.replace(submission.reply).is_none());
        submission.buffer
    }

    /// Take the pending maintenance proposal into validation, if any.
    pub(super) fn start_maintenance(&mut self) -> Option<ProposalBuffer> {
        let buffer = self.maintenance.take()?;
        assert!(self.validating.replace(Reply::maintenance()).is_none());
        Some(buffer)
    }

    /// The reply of the proposal whose validation just completed.
    pub(super) fn validated(&mut self) -> Option<Reply> {
        self.validating.take()
    }

    /// Hold a validated proposal until its deferral clears. A wait for one
    /// producer's unconfirmed original parks that producer, ahead of its
    /// queued proposals; a capacity wait holds the partition's slot.
    pub(super) fn defer(&mut self, submission: Submission, wait: ProposalWait) {
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
    pub(super) fn requeue(&mut self, submission: Submission) {
        assert!(self.wait.is_none());
        assert!(self.waiting.replace(submission).is_none());
    }

    /// Queue a journal-built proposal ahead of the waiting slot.
    pub(super) fn set_maintenance(&mut self, buffer: ProposalBuffer) {
        assert!(self.maintenance.replace(buffer).is_none());
    }

    /// Finish the waiting and parked proposals unadmitted and drop maintenance
    /// work. Returns how many proposals finished.
    pub(super) fn reject_all(&mut self) -> usize {
        self.maintenance = None;
        usize::from(self.reject_waiting()) + self.reject_parked()
    }

    /// After an authority change: drop maintenance work and finish deferred
    /// proposals, whose waits name the old image. An undeferred waiting
    /// proposal validates again under the new authority.
    pub(super) fn reject_deferred(&mut self) {
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
    pub(super) fn release_capacity(&mut self) {
        if self.wait.is_some_and(ProposalWait::capacity) {
            self.wait = None;
        }
    }

    /// A waiting proposal defers maintenance, unless it waits for identity
    /// capacity that only maintenance creates. Parked proposals wait for
    /// application, which maintenance never delays.
    pub(super) fn blocks_maintenance(&self) -> bool {
        self.waiting.is_some() && !self.wait.is_some_and(ProposalWait::capacity)
    }

    /// The slot is empty, so ingress may deliver the next submission.
    pub(super) fn is_idle(&self) -> bool {
        self.waiting.is_none()
    }
}

/// Completion of this proposal, not a consumer's external processing transaction.
#[derive(Debug)]
pub enum ProposalOutcome {
    /// Every operation through this exact prefix satisfies the configured group
    /// confirmation policy and is applied. RAM confirmation is not disk durability.
    Committed {
        /// Group, configuration, and view that authorized this reply.
        scope: Scope,
        /// Last operation covered by the configured group policy and application.
        through: Prefix,
    },
    /// This attempt was not admitted. This says nothing about earlier attempts.
    NotAdmitted,
    /// Read-only validation rejected this attempt before admission.
    Invalid(JournalError),
    /// Admission occurred, but authority changed before the reply. Reconcile retries.
    Unknown,
}

/// Returned arena remains charged to its original worker until reused or dropped.
#[derive(Debug)]
pub struct ProposalReply {
    /// Exact completion boundary or an explicitly uncertain outcome.
    pub outcome: ProposalOutcome,
    /// Original bodies; clear before filling with unrelated work.
    pub buffer: ProposalBuffer,
}

/// Closed completion channel. The actor may already have persisted the proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("replica proposal outcome unknown: actor stopped")]
pub struct ProposalStopped;

pin_project! {
    /// Dropping this future cancels waiting, never an already admitted disk action.
    #[derive(Debug)]
    pub struct PendingProposal {
        #[pin]
        reply: oneshot::Receiver<ProposalReply>,
        #[pin]
        closed: Closed,
        admission: CloseSignal,
    }
}

impl PendingProposal {
    /// Fence this attachment's work before core admission. Already admitted
    /// operations and physical jobs retain their normal completion obligations.
    pub(crate) fn fence_unadmitted(&self) {
        self.admission.close();
    }
}

impl Future for PendingProposal {
    type Output = Result<ProposalReply, ProposalStopped>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(result) = this.reply.poll(cx) {
            return Poll::Ready(result.map_err(|_| ProposalStopped));
        }
        if this.closed.poll(cx).is_ready() {
            return Poll::Ready(Err(ProposalStopped));
        }
        Poll::Pending
    }
}

/// Local queue rejection, before this submission reaches the actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposalSubmitError {
    /// Global request budget or this lane is full.
    Full,
    /// Actor is no longer accepting requests.
    Stopped,
    /// Empty, foreign-worker, or larger than this actor's operation/body limits.
    Buffer,
}

/// An unsubmitted proposal returned without surrendering its arena.
#[derive(Debug)]
pub struct UnsubmittedProposal {
    /// Local admission failure, not a persistence result.
    pub reason: ProposalSubmitError,
    /// Unchanged proposal for reuse or retry.
    pub buffer: ProposalBuffer,
}

/// One preallocated, non-clonable MPSC lane. Move it to its submitting thread.
#[derive(Debug)]
pub struct ProposalSubmitter {
    sender: mpsc::NotifiedSender<Submission>,
    closed: CloseSignal,
    capacity: Arc<Semaphore>,
    generation: JournalGeneration,
    limits: PipelineLimits,
    untaken: Arc<AtomicUsize>,
}

impl ProposalSubmitter {
    /// Enqueue only. Syntax, hashing, and disk work run on the journal worker.
    #[expect(
        clippy::result_large_err,
        reason = "return arena ownership on backpressure"
    )]
    pub fn try_submit(
        &mut self,
        buffer: ProposalBuffer,
    ) -> Result<PendingProposal, UnsubmittedProposal> {
        let reason = if self.closed.is_closed() || self.sender.is_disconnected() {
            Some(ProposalSubmitError::Stopped)
        } else if buffer.owner_generation() != self.generation
            || buffer.is_empty()
            || buffer.len() > self.limits.max_operations
            || buffer.body_bytes() > self.limits.max_body_bytes
        {
            Some(ProposalSubmitError::Buffer)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(UnsubmittedProposal { reason, buffer });
        }
        let Ok(permit) = self.capacity.clone().try_acquire_owned() else {
            return Err(UnsubmittedProposal {
                reason: ProposalSubmitError::Full,
                buffer,
            });
        };
        let (done, pending) = oneshot::channel();
        let admission = CloseSignal::default();
        self.untaken.fetch_add(1, Ordering::Relaxed);
        let submission = Submission {
            buffer,
            reply: Reply {
                done,
                permit: Some(permit),
                untaken: Untaken(Some(self.untaken.clone())),
                queued_at: crate::profiling::start(),
                admitted_at: None,
                admission: admission.clone(),
            },
        };
        if let Err(error) = self.sender.try_send(submission) {
            let (reason, submission) = match error {
                mpsc::TrySendError::Full(value) => (ProposalSubmitError::Full, value),
                mpsc::TrySendError::Disconnected(value) => (ProposalSubmitError::Stopped, value),
            };
            return Err(UnsubmittedProposal {
                reason,
                buffer: submission.buffer,
            });
        }
        Ok(PendingProposal {
            reply: pending,
            closed: self.closed.closed(),
            admission,
        })
    }
}

/// Counts a submission as untaken until the actor schedules or finishes it.
#[derive(Debug)]
struct Untaken(Option<Arc<AtomicUsize>>);

impl Untaken {
    fn taken(&mut self) {
        if let Some(count) = self.0.take() {
            count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for Untaken {
    fn drop(&mut self) {
        self.taken();
    }
}

#[derive(Debug)]
pub(super) struct Reply {
    done: oneshot::Sender<ProposalReply>,
    permit: Option<OwnedSemaphorePermit>,
    untaken: Untaken,
    queued_at: Option<std::time::Instant>,
    admitted_at: Option<std::time::Instant>,
    admission: CloseSignal,
}

impl Reply {
    /// One owner-local maintenance proposal. Its caller observes journal state.
    pub(super) fn maintenance() -> Self {
        let (done, observer) = oneshot::channel();
        drop(observer);
        Self {
            done,
            permit: None,
            untaken: Untaken(None),
            queued_at: None,
            admitted_at: None,
            admission: CloseSignal::default(),
        }
    }

    pub(super) fn fenced(&self) -> bool {
        self.admission.is_closed()
    }
    pub(super) fn is_maintenance(&self) -> bool {
        self.permit.is_none()
    }
    pub(super) fn scheduled(&mut self) {
        self.untaken.taken();
        crate::profiling::finish(crate::profiling::Stage::ReplicaQueue, self.queued_at.take());
    }

    pub(super) fn admitted(&mut self) {
        self.admitted_at = crate::profiling::start();
    }

    pub(super) fn finish(self, buffer: ProposalBuffer, outcome: ProposalOutcome) {
        let Self {
            done,
            permit,
            untaken,
            admitted_at,
            ..
        } = self;
        drop(untaken);
        if matches!(outcome, ProposalOutcome::Committed { .. }) {
            crate::profiling::finish(crate::profiling::Stage::ReplicaConfirmation, admitted_at);
        }
        drop(permit);
        let _ = done.send(ProposalReply { outcome, buffer });
    }
}

#[derive(Debug)]
pub(super) struct Submission {
    pub buffer: ProposalBuffer,
    pub reply: Reply,
}

#[derive(Debug)]
pub(super) struct Ingress {
    receiver: mpsc::NotifiedReceiver<Submission>,
    closed: CloseSignal,
    pollable: bool,
}

impl Ingress {
    pub(super) fn close(&self) {
        self.closed.close();
    }

    pub(super) fn begin_round(&mut self) {
        self.pollable = false;
    }

    pub(super) fn resume(&mut self) {
        self.pollable = true;
    }

    pub(super) async fn ready(&self) {
        // Queued work is actionable only when another scheduling round can
        // inspect ingress. Election, full sync groups and admission pressure
        // resume on their own completions, traffic or timers, not queue backlog.
        if !self.pollable {
            std::future::pending::<()>().await;
        }
        self.receiver.ready().await;
    }

    pub(super) fn new(
        lanes: usize,
        slots: usize,
        generation: JournalGeneration,
        limits: PipelineLimits,
    ) -> (Self, Vec<ProposalSubmitter>) {
        let (sender, mut receiver) = mpsc::notified_channel(slots);
        let closed = CloseSignal::default();
        let capacity = Arc::new(Semaphore::new(slots));
        let mut senders = Vec::with_capacity(lanes);
        for _ in 1..lanes {
            senders.push(sender.try_clone().expect("new receiver alive"));
        }
        senders.push(sender);
        // Register all lanes and their receiver scratch before accepting traffic.
        assert!(receiver.try_recv().is_err());
        let submitters = senders
            .into_iter()
            .map(|sender| ProposalSubmitter {
                sender,
                closed: closed.clone(),
                capacity: capacity.clone(),
                generation,
                limits,
                untaken: Arc::new(AtomicUsize::new(0)),
            })
            .collect();
        (
            Self {
                receiver,
                closed,
                pollable: false,
            },
            submitters,
        )
    }

    pub(super) fn pop(&mut self) -> Option<Submission> {
        self.receiver.try_recv().ok()
    }
}

impl Drop for Ingress {
    fn drop(&mut self) {
        // Admission closes before worker draining. Coordinated teardown also
        // permits late cleanup by an overlapping sender, so notify independently.
        self.close();
    }
}
