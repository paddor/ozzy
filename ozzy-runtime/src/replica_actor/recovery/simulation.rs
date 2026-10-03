//! Explicit events through the production nonvoting recovery actor.

use super::{
    ActorError, ActorStatus, Duration, Outcome, RecoveryActor, RecoveryStorage, ReplicaActor,
    ShardRecoveringJournal, Transition,
};
use crate::replica_actor::simulation::SimulationError;
use crate::replica_transport::FlushProgress;
use omq_tokio::{Message, TrySendError};

/// Controlled production recovery with explicit storage execution and callbacks.
/// Times are relative to this attempt's start; a returned retry starts at zero.
/// No event can bypass full-history publication or grant same-view voting.
#[derive(Debug)]
pub struct ControlledRecovery<J: RecoveryStorage = ShardRecoveringJournal> {
    actor: Box<RecoveryActor<J>>,
    outcome: Option<Outcome>,
    now: Duration,
    active: bool,
}

/// Result of the same owned lifecycle transition used by the OMQ actor loop.
#[derive(Debug)]
pub enum RecoveryTransition<J: RecoveryStorage = ShardRecoveringJournal> {
    /// Fresh nonce/worker attempt. Relative protocol time starts again at zero.
    Retry(Box<ControlledRecovery<J>>),
    /// Fully published replica, still fenced and requiring election/activation.
    /// Configure client admission before placing it in `ControlledReplica` or OMQ.
    Normal(Box<ReplicaActor>),
}

impl<J: RecoveryStorage> ControlledRecovery<J> {
    /// Take a prepared nonvoting actor without running timers or network I/O.
    pub fn new(actor: RecoveryActor<J>) -> Self {
        Self {
            actor: Box::new(actor),
            outcome: None,
            now: Duration::ZERO,
            active: true,
        }
    }

    /// Run one production scheduling round without observing disk completion.
    pub fn advance(&mut self, now: Duration) -> Result<(), SimulationError> {
        self.set_time(now)?;
        let result = self.actor.advance(now);
        self.outcome = self.finish_event(result)?;
        Ok(())
    }

    /// Deliver an independently routed OMQ-shaped message through production checks.
    pub fn receive(&mut self, message: &Message, now: Duration) -> Result<(), SimulationError> {
        self.set_time(now)?;
        let result = self.actor.receive(message, now);
        self.finish_event(result)
    }

    /// Flush bounded outboxes through a transport that may delay, drop, or reject sends.
    /// A successful send supplies neither recovery authority nor durability.
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

    /// Observe one storage callback. Cancellation retains the pending command.
    /// Other events and virtual time must be advanced explicitly by the caller.
    pub async fn complete_disk(&mut self, now: Duration) -> Result<bool, SimulationError> {
        self.set_time(now)?;
        let Some(pending) = &mut self.actor.pending else {
            return Ok(false);
        };
        let result = tokio::select! {
            biased;
            result = pending.wait() => result,
            error = std::future::poll_fn(|cx| self.actor.journal.as_mut().expect("recovery owner").poll_stopped(cx)) => Err(ActorError::Journal(error)),
        };
        self.actor.pending = None;
        let result = result.and_then(|completed| self.actor.complete(completed, now));
        self.outcome = self.finish_event(result)?;
        Ok(true)
    }

    /// Latest coalesced observation. Always nonvoting before owned handoff.
    pub fn status(&self) -> ActorStatus {
        *self.actor.status.borrow()
    }

    /// Whether publication or abandonment has reached an owned lifecycle boundary.
    pub const fn transition_pending(&self) -> bool {
        self.outcome.is_some()
    }

    /// Consume a completed boundary through production shutdown/reopen/handoff.
    /// Cancellation drops the owned actor while worker I/O remains owned until
    /// exit, never leaving a half-transitioned controller that can resume votes.
    pub async fn transition(mut self) -> Result<RecoveryTransition<J>, SimulationError> {
        if !self.active {
            return Err(SimulationError::Stopped);
        }
        let outcome = self
            .outcome
            .take()
            .ok_or(SimulationError::TransitionNotReady)?;
        match Box::pin(self.actor.transition(outcome)).await? {
            Transition::Retry(actor) => Ok(RecoveryTransition::Retry(Box::new(Self::new(*actor)))),
            Transition::Normal(actor) => Ok(RecoveryTransition::Normal(actor)),
        }
    }

    /// Settle and join worker-owned I/O without granting normal authority.
    pub async fn shutdown(self) -> Result<(), ActorError> {
        self.actor.shutdown().await
    }

    fn set_time(&mut self, now: Duration) -> Result<(), SimulationError> {
        if !self.active {
            return Err(SimulationError::Stopped);
        }
        if self.outcome.is_some() {
            return Err(SimulationError::TransitionPending);
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
        }
        self.actor.publish(self.active);
        result.map_err(SimulationError::Actor)
    }
}
