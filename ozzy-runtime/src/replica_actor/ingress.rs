//! Startup-registered fanring lanes with a global outstanding-request bound.

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
        self.untaken.fetch_add(1, Ordering::Relaxed);
        let submission = Submission {
            buffer,
            reply: Reply {
                done,
                _permit: permit,
                untaken: Untaken(Some(self.untaken.clone())),
                queued_at: crate::profiling::start(),
                admitted_at: None,
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
    _permit: OwnedSemaphorePermit,
    untaken: Untaken,
    queued_at: Option<std::time::Instant>,
    admitted_at: Option<std::time::Instant>,
}

impl Reply {
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
            _permit: permit,
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
