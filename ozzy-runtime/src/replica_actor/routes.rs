//! Bounded, coalesced observations of the actors' current election state.

use std::{
    collections::BTreeSet,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use ozzy_proto::{GroupId, NodeId, PartitionIncarnation, directory::RouteState};

use super::PartitionActors;
use crate::{
    dispatch::{Error as AdmissionError, SendFailure},
    frontend::{Pending, Port, PortError, RouteError, RouteResult},
};

type Observation = (u64, Option<NodeId>);

#[derive(Debug)]
struct Publication {
    observation: Observation,
    pending: Pending<RouteResult>,
}

#[derive(Debug)]
struct Slot {
    latest: RouteState,
    delivered: Option<Observation>,
    pending: Option<Publication>,
}

impl Slot {
    fn observe_pending(&mut self, cx: &mut Context<'_>) -> Result<bool, RoutePublicationError> {
        let Some(publication) = &mut self.pending else {
            return Ok(false);
        };
        let Poll::Ready(result) = Pin::new(&mut publication.pending).poll(cx) else {
            return Ok(false);
        };
        result??;
        self.delivered = Some(publication.observation);
        self.pending = None;
        Ok(true)
    }
}

/// Poll beside the shard's actors with its one shared frontend port. At most one
/// publication per partition is awaiting dispatch; newer observations coalesce.
/// Full watch or port queues cannot hold up elections or journal progress.
pub struct RoutePublisher {
    slots: Vec<Slot>,
    turn: usize,
    next: usize,
    remaining: usize,
    capacity: Option<Pin<Box<dyn Future<Output = ()>>>>,
}

impl std::fmt::Debug for RoutePublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutePublisher")
            .field("slots", &self.slots)
            .field("turn", &self.turn)
            .field("waiting_capacity", &self.capacity.is_some())
            .finish_non_exhaustive()
    }
}

impl RoutePublisher {
    /// Exactly one checked incarnation per shard actor. Initial member order
    /// and configuration epochs come from the actors, not caller leader guesses.
    pub fn new(
        actors: &PartitionActors,
        identities: &[(GroupId, PartitionIncarnation)],
        turn: usize,
    ) -> Result<Self, RoutePublicationError> {
        if identities.len() != actors.len() || turn == 0 || turn > 1024 {
            return Err(RoutePublicationError::Configuration);
        }
        let mut groups = BTreeSet::new();
        let mut incarnations = BTreeSet::new();
        let mut slots = Vec::with_capacity(identities.len());
        for &(group, partition) in identities {
            let latest = actors
                .route_state(group, partition)
                .filter(RouteState::valid)
                .ok_or(RoutePublicationError::Configuration)?;
            if !groups.insert(group) || !incarnations.insert(partition) {
                return Err(RoutePublicationError::Configuration);
            }
            slots.push(Slot {
                latest,
                delivered: None,
                pending: None,
            });
        }
        Ok(Self {
            remaining: slots.len(),
            slots,
            turn,
            next: 0,
            capacity: None,
        })
    }

    /// Poll after actor progress in the same scheduler turn. Pending port
    /// completions and capacity waits register wakeups without a retry timer.
    /// Canceling this poll preserves every queued observation and scan position.
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        actors: &PartitionActors,
        port: &mut Port,
    ) -> Poll<Result<(), RoutePublicationError>> {
        if let Some(wait) = &mut self.capacity
            && wait.as_mut().poll(cx).is_ready()
        {
            self.capacity = None;
        }
        if self.remaining == 0 {
            self.remaining = self.slots.len();
        }
        let mut progressed = false;
        let slots = self.slots.len();
        for _ in 0..self.turn.min(self.remaining) {
            let slot = &mut self.slots[self.next];
            self.next = (self.next + 1) % slots;
            self.remaining -= 1;
            if !actors.observe_route(&mut slot.latest) {
                return Poll::Ready(Err(RoutePublicationError::Configuration));
            }
            progressed |= slot.observe_pending(cx)?;
            let observation = (slot.latest.view, slot.latest.leader);
            if slot.pending.is_some()
                || slot.delivered == Some(observation)
                || self.capacity.is_some()
            {
                continue;
            }
            let generation = port.generation();
            match port.try_route(slot.latest.clone()) {
                Ok(pending) => {
                    slot.pending = Some(Publication {
                        observation,
                        pending,
                    });
                    // Poll now so an immediate dispatcher completion cannot be
                    // stranded behind a previously visited scheduler position.
                    slot.observe_pending(cx)?;
                    progressed = true;
                }
                Err((PortError::Admission(SendFailure::Admission(AdmissionError::Full)), _)) => {
                    let mut wait = Box::pin(port.changed_after(generation));
                    if wait.as_mut().poll(cx).is_ready() {
                        progressed = true;
                    } else {
                        self.capacity = Some(wait);
                    }
                }
                Err((error, _)) => return Poll::Ready(Err(error.into())),
            }
        }
        if progressed || self.remaining > 0 {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

/// Publication failed independently of partition confirmation or elections.
#[derive(Debug, thiserror::Error)]
pub enum RoutePublicationError {
    /// Missing, repeated, or changed partition/configuration identity.
    #[error("invalid actor routing publication configuration")]
    Configuration,
    /// Shared shard control lane ended or refused the command.
    #[error(transparent)]
    Port(#[from] PortError),
    /// Dispatcher rejected the shard placement, watch identity, or view.
    #[error(transparent)]
    Route(#[from] RouteError),
}
