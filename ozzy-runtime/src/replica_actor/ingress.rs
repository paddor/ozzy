//! Owner-local proposal queue with an outstanding-request bound. The actor's
//! admission state above it lives in `admission`.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use ozzy_replication::{JournalGeneration, PipelineLimits, Prefix, Scope};
use pin_project_lite::pin_project;
use tokio::sync::oneshot;

use crate::replica_journal::{JournalError, ProposalBuffer};
use crate::signal::{CloseSignal, Closed};

mod admission;
pub(super) use admission::{NextProposal, ProposalWait, Proposals};

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

/// One submission lane into an actor on the same thread. Submitting wakes the
/// actor's partition.
#[derive(Debug)]
pub struct ProposalSubmitter {
    queue: Rc<Queue>,
    generation: JournalGeneration,
    limits: PipelineLimits,
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
        let queue = &self.queue;
        let reason = if queue.closed.is_closed() {
            Some(ProposalSubmitError::Stopped)
        } else if buffer.owner_generation() != self.generation
            || buffer.is_empty()
            || buffer.len() > self.limits.max_operations
            || buffer.body_bytes() > self.limits.max_body_bytes
        {
            Some(ProposalSubmitError::Buffer)
        } else if queue.outstanding.get() == queue.slots {
            Some(ProposalSubmitError::Full)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(UnsubmittedProposal { reason, buffer });
        }
        let (done, pending) = oneshot::channel();
        let admission = CloseSignal::default();
        queue.push(Submission {
            buffer,
            reply: Reply {
                done,
                slot: Some(Slot::take(queue)),
                queued_at: crate::profiling::start(),
                admitted_at: None,
                admission: admission.clone(),
            },
        });
        Ok(PendingProposal {
            reply: pending,
            closed: queue.closed.closed(),
            admission,
        })
    }
}

/// Submissions not yet taken by the actor. The actor and its submitters share
/// it on one thread.
#[derive(Debug)]
struct Queue {
    submissions: RefCell<VecDeque<Submission>>,
    /// Submissions whose reply has not finished, queued or taken.
    outstanding: Cell<usize>,
    slots: usize,
    closed: CloseSignal,
    /// The actor's partition, from its last readiness poll. The first
    /// submission into an empty queue wakes it.
    waker: RefCell<Option<Waker>>,
}

impl Queue {
    fn push(&self, submission: Submission) {
        let mut submissions = self.submissions.borrow_mut();
        let first = submissions.is_empty();
        submissions.push_back(submission);
        drop(submissions);
        if first && let Some(waker) = &*self.waker.borrow() {
            waker.wake_by_ref();
        }
    }
}

/// One unit of the outstanding-request bound, held until its reply finishes.
#[derive(Debug)]
struct Slot(Rc<Queue>);

impl Slot {
    fn take(queue: &Rc<Queue>) -> Self {
        queue.outstanding.set(queue.outstanding.get() + 1);
        Self(queue.clone())
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.outstanding.set(self.0.outstanding.get() - 1);
    }
}

#[derive(Debug)]
pub(super) struct Reply {
    done: oneshot::Sender<ProposalReply>,
    slot: Option<Slot>,
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
            slot: None,
            queued_at: None,
            admitted_at: None,
            admission: CloseSignal::default(),
        }
    }

    pub(super) fn fenced(&self) -> bool {
        self.admission.is_closed()
    }
    pub(super) fn is_maintenance(&self) -> bool {
        self.slot.is_none()
    }
    pub(super) fn scheduled(&mut self) {
        crate::profiling::finish(crate::profiling::Stage::ReplicaQueue, self.queued_at.take());
    }

    pub(super) fn admitted(&mut self) {
        self.admitted_at = crate::profiling::start();
    }

    pub(super) fn finish(self, buffer: ProposalBuffer, outcome: ProposalOutcome) {
        let Self {
            done,
            slot,
            admitted_at,
            ..
        } = self;
        if matches!(outcome, ProposalOutcome::Committed { .. }) {
            crate::profiling::finish(crate::profiling::Stage::ReplicaConfirmation, admitted_at);
        }
        drop(slot);
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
    queue: Rc<Queue>,
    pollable: bool,
}

impl Ingress {
    pub(super) fn new(
        lanes: usize,
        slots: usize,
        generation: JournalGeneration,
        limits: PipelineLimits,
    ) -> (Self, Vec<ProposalSubmitter>) {
        let queue = Rc::new(Queue {
            submissions: RefCell::new(VecDeque::with_capacity(slots)),
            outstanding: Cell::new(0),
            slots,
            closed: CloseSignal::default(),
            waker: RefCell::new(None),
        });
        let submitters = (0..lanes)
            .map(|_| ProposalSubmitter {
                queue: queue.clone(),
                generation,
                limits,
            })
            .collect();
        let ingress = Self {
            queue,
            pollable: false,
        };
        (ingress, submitters)
    }

    pub(super) fn close(&self) {
        self.queue.closed.close();
    }

    pub(super) fn begin_round(&mut self) {
        self.pollable = false;
    }

    pub(super) fn resume(&mut self) {
        self.pollable = true;
    }

    /// Ready once queued work is actionable, when another scheduling round can
    /// inspect ingress. Election, full sync groups and admission pressure
    /// resume on their own completions, traffic or timers, not queue backlog.
    /// The registration outlives this poll: a later submission still wakes it.
    pub(super) fn poll_ready(&self, cx: &Context<'_>) -> Poll<()> {
        let mut registered = self.queue.waker.borrow_mut();
        if !registered
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()))
        {
            *registered = Some(cx.waker().clone());
        }
        drop(registered);
        if self.pollable && !self.queue.submissions.borrow().is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    pub(super) async fn ready(&self) {
        std::future::poll_fn(|cx| self.poll_ready(cx)).await;
    }

    pub(super) fn pop(&mut self) -> Option<Submission> {
        self.queue.submissions.borrow_mut().pop_front()
    }
}

impl Drop for Ingress {
    fn drop(&mut self) {
        // Stop admission, and finish queued submissions now rather than when
        // the last submitter drops.
        self.close();
        self.queue.submissions.borrow_mut().clear();
    }
}
