//! The owner's trim: check the floor it expects, propose the `Trim` on the
//! log's lane, and publish the new floor once it commits.

use super::{LogService, Term};
use crate::log::{LogRole, LogStatus, TrimError, TrimRequest};
use crate::replica_actor::{PartitionActor, PendingProposal, ProposalOutcome};
use crate::replica_journal::{AppendAdmissionError, JournalError};
use ozzy_core::state::{CanonicalImagesError, StateError};
use ozzy_journal::operation::Trim;
use ozzy_proto::{Offset, OperationId};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// The submitted trim.
#[derive(Debug)]
pub(super) struct TrimFlight {
    pending: Pin<Box<PendingProposal>>,
    reply: oneshot::Sender<Result<(), TrimError>>,
    floor: Offset,
}

impl TrimFlight {
    /// The actor went away with this trim admitted or not.
    pub(super) fn unknown(self) {
        let _ = self.reply.send(Err(TrimError::Unknown));
    }
}

/// A control-operation identity no other trim of this log shares: floors
/// only move forward.
fn operation_id(floor: Offset) -> OperationId {
    let mut bytes = *b"ownrtrim\0\0\0\0\0\0\0\0";
    bytes[8..].copy_from_slice(&floor.get().to_be_bytes());
    OperationId::from_bytes(bytes)
}

impl LogService {
    /// The `Trim` for `request`, why it is refused, or `Pending` while a
    /// trim runs or the journal is busy.
    fn plan_trim(
        &self,
        actor: &PartitionActor,
        request: &TrimRequest,
    ) -> Poll<Result<(Term, Trim), TrimError>> {
        let term = match self.term {
            Some(term) if term.scope.view == request.view => term,
            Some(term) => {
                return Poll::Ready(Err(TrimError::View {
                    named: request.view,
                    current: term.scope.view,
                }));
            }
            None => return Poll::Ready(Err(TrimError::NotLeader)),
        };
        if self.stepping.is_some() || self.port.published().role == LogRole::Doubt {
            return Poll::Ready(Err(TrimError::NotLeader));
        }
        if self.trim.is_some() {
            return Poll::Pending;
        }
        let Some(facts) = actor.facts(self.port.partition) else {
            return Poll::Pending;
        };
        if facts.retained_from != request.expected {
            return Poll::Ready(Err(TrimError::Floor {
                current: facts.retained_from,
            }));
        }
        if request.floor <= request.expected || request.floor > facts.next_offset {
            return Poll::Ready(Err(TrimError::Invalid));
        }
        Poll::Ready(Ok((
            term,
            Trim {
                partition: self.port.partition,
                expected_floor: request.expected,
                new_floor: request.floor,
                expected_operation_floor: facts.operation_floor,
                new_operation_floor: facts.operation_floor,
                operation_id: operation_id(request.floor),
            },
        )))
    }

    /// Submit `request`, or hand it back while a trim runs, the journal is
    /// busy, or every arena is in flight.
    pub(super) fn start_trim(
        &mut self,
        actor: &PartitionActor,
        request: TrimRequest,
    ) -> Option<TrimRequest> {
        let refuse = |request: TrimRequest, error| {
            let _ = request.reply.send(Err(error));
            None
        };
        let (term, trim) = match self.plan_trim(actor, &request) {
            Poll::Ready(Ok(planned)) => planned,
            Poll::Ready(Err(error)) => return refuse(request, error),
            Poll::Pending => return Some(request),
        };
        let Some(lane) = &mut self.lane else {
            return refuse(request, TrimError::NotLeader);
        };
        let Some(mut buffer) = lane.arenas.pop() else {
            return Some(request);
        };
        if buffer
            .prepare_owner_trim(term.authority(), trim, lane.operations)
            .is_err()
        {
            buffer.clear();
            lane.arenas.push(buffer);
            return refuse(request, TrimError::Invalid);
        }
        match lane.submitter.try_submit(buffer) {
            Ok(pending) => {
                self.trim = Some(TrimFlight {
                    pending: Box::pin(pending),
                    reply: request.reply,
                    floor: request.floor,
                });
                None
            }
            Err(unsubmitted) => {
                let mut buffer = unsubmitted.buffer;
                buffer.clear();
                lane.arenas.push(buffer);
                refuse(request, TrimError::NotLeader)
            }
        }
    }

    /// Answer the trim once its outcome is known and reuse its arena.
    pub(super) fn finish_trim(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(flight) = &mut self.trim else {
            return false;
        };
        let Poll::Ready(result) = flight.pending.as_mut().poll(cx) else {
            return false;
        };
        let Some(flight) = self.trim.take() else {
            return false;
        };
        let Ok(reply) = result else {
            flight.unknown();
            return true;
        };
        let answer = match reply.outcome {
            ProposalOutcome::Committed { .. } => Ok(()),
            ProposalOutcome::NotAdmitted => Err(TrimError::NotLeader),
            ProposalOutcome::Unknown => Err(TrimError::Unknown),
            ProposalOutcome::Invalid(error) => Err(refusal(&error)),
        };
        if answer.is_ok() {
            let published = self.port.published();
            if published.lead.is_some() && flight.floor > published.retained_from {
                self.port.publish(LogStatus {
                    retained_from: flight.floor,
                    ..published
                });
            }
        }
        let _ = flight.reply.send(answer);
        if let Some(lane) = &mut self.lane {
            let mut buffer = reply.buffer;
            buffer.clear();
            lane.arenas.push(buffer);
        }
        true
    }
}

/// The trim error a validation refusal stands for.
fn refusal(error: &JournalError) -> TrimError {
    match error {
        JournalError::Images(CanonicalImagesError::State(StateError::TrimFloorMismatch)) => {
            TrimError::Retry
        }
        JournalError::Images(CanonicalImagesError::State(
            StateError::InvalidTrimFloor | StateError::TrimBeyondProducerResults,
        )) => TrimError::Invalid,
        JournalError::ProducerAppend(AppendAdmissionError::Authority)
        | JournalError::AppendMismatch => TrimError::NotLeader,
        _ => TrimError::Stopped,
    }
}
