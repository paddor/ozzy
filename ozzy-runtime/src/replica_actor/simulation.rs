//! Explicit event driving of the production actor, enabled by `simulation`.
//!
//! Callers control protocol time and callback observation. The journal execution
//! type selects legacy real workers or shard-local futures over an injected file
//! backend. `ReplicaActor::new_with_ids` also controls election and receive-flow
//! identities. Neither selection by itself proves power-loss behavior.

use std::time::Duration;

use super::{ActorError, ActorStatus, ReplicaActor};
use crate::replica_transport::FlushProgress;
use omq_tokio::{Message, TrySendError};

pub use super::recovery::simulation::{ControlledRecovery, RecoveryTransition};

/// Own an actor while controlling its event schedule instead of running OMQ I/O.
///
/// Take proposal lanes and buffer leases before transferring the actor here.
/// Time is monotonic from actor startup and is never derived from disk latency.
/// After a terminal actor error, only observation and shutdown are allowed.
#[derive(Debug)]
pub struct ControlledReplica<E = crate::replica_journal::ShardJournal> {
    actor: Box<ReplicaActor<E>>,
    now: Duration,
    active: bool,
}

impl<E: crate::replica_journal::JournalExecution> ControlledReplica<E> {
    /// Take a newly constructed actor. No work runs until explicitly advanced.
    pub fn new(actor: ReplicaActor<E>) -> Self {
        Self {
            actor: Box::new(actor),
            now: Duration::ZERO,
            active: true,
        }
    }

    /// Run one production scheduling round, including timers and journal admission.
    /// This never waits for a disk completion or grants durability by itself.
    pub fn advance(&mut self, now: Duration) -> Result<(), SimulationError> {
        self.set_time(now)?;
        let result = self.actor.advance(now);
        self.finish_event(result)
    }

    /// Deliver an OMQ-shaped message through production decoding and peer checks.
    /// The first frame is the independently supplied sender routing identity,
    /// followed by the three Ozzy protocol frames. No scheduling round runs here.
    pub fn receive(&mut self, message: &Message, now: Duration) -> Result<(), SimulationError> {
        self.set_time(now)?;
        let result = self.actor.receive(message, now);
        self.finish_event(result)
    }

    /// Attempt one production outbox round against a controlled transport.
    /// Messages include their destination routing frame. Return `Full(message)`
    /// to preserve exact ownership under backpressure; `Ok(())` transfers it.
    /// Submission supplies no receipt or quorum evidence. The caller owns any
    /// admitted message until delivery or an explicitly modeled transport loss.
    pub fn flush(
        &mut self,
        try_send: impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<FlushProgress, SimulationError> {
        self.set_time(self.now)?;
        let result = self
            .actor
            .outbox
            .flush_with(try_send)
            .map_err(ActorError::Transport);
        self.finish_event(result)
    }

    /// Observe one actual journal completion at the supplied protocol time.
    /// Returns false if no command is outstanding. While awaiting the worker,
    /// no other actor events run; schedule other voters independently.
    /// Cancellation retains the outstanding command inside this actor.
    /// Sync readiness and owner installation are separate observable events.
    /// Ready sync events precede ready write/image events, as in the live loop.
    pub async fn complete_disk(&mut self, now: Duration) -> Result<bool, SimulationError> {
        self.set_time(now)?;
        let result = self.actor.observe_journal(now, false).await;
        self.finish_event(result)
    }

    /// Latest coalesced observation. This is not permission to vote or acknowledge.
    pub fn status(&self) -> ActorStatus {
        *self.actor.status.borrow()
    }

    /// Close admission and join admitted journal work through production shutdown.
    /// Does not convert unobserved physical completion into a client ACK.
    pub async fn shutdown(mut self) -> Result<(), ActorError> {
        self.actor.shutdown().await
    }

    fn set_time(&mut self, now: Duration) -> Result<(), SimulationError> {
        if !self.active {
            return Err(SimulationError::Stopped);
        }
        if now < self.now {
            return Err(SimulationError::TimeReversed);
        }
        self.now = now;
        Ok(())
    }

    fn finish_event<T>(&mut self, result: Result<T, ActorError>) -> Result<T, SimulationError> {
        if result.is_err() {
            self.active = false;
            self.actor.ingress.close();
        }
        self.actor.publish_status(self.active);
        result.map_err(SimulationError::Actor)
    }
}

/// Rejected event schedule or terminal production actor failure.
#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    /// A recovery transition must be consumed before delivering further events.
    #[error("controlled recovery requires its pending transition")]
    TransitionPending,
    /// Recovery has not reached a retry or durable-publication boundary.
    #[error("controlled recovery has no transition ready")]
    TransitionNotReady,
    /// An earlier actor error ended this controlled run.
    #[error("controlled replica is stopped")]
    Stopped,
    /// No event was delivered because its time precedes an earlier event.
    #[error("controlled replica time moved backward")]
    TimeReversed,
    /// The same error would terminate the production actor loop.
    #[error(transparent)]
    Actor(#[from] ActorError),
}
