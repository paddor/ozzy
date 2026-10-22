//! Peer messages a recovering copy accepts: recovery state, view evidence,
//! and the operation and checkpoint chunks it fetched.

use super::{Pending, RecoveryActor};
use crate::replica_actor::{
    ActorError, Bytes, Duration, HistoryReason, Message, NodeId, ReplicaMessage, routed, wire,
};
use crate::replica_journal::RecoveryStorage;
use ozzy_replication::wire::{Control, RecoveryMessage};

impl<J: RecoveryStorage> RecoveryActor<J> {
    pub(super) fn receive(&mut self, message: &Message, now: Duration) -> Result<(), ActorError> {
        let Some((index, binding, frames)) = routed(self.configuration, &self.bindings, message)
        else {
            return Ok(());
        };
        let from = self.configuration.voters()[index];
        let Ok(decoded) = wire::decode(&frames, binding, self.wire_limits) else {
            return Ok(());
        };
        match decoded {
            ReplicaMessage::Recovery(RecoveryMessage::State(state)) => {
                self.receive_state(index, &state, now)?;
            }
            ReplicaMessage::Control(control) => {
                if self.authoritative(from, control) {
                    self.observe(from, control.scope())?;
                }
            }
            ReplicaMessage::Checkpoint(wire::CheckpointMessage::Chunk { request, .. }) => {
                self.receive_checkpoint(
                    request,
                    message
                        .part_bytes(3)
                        .ok_or_else(|| ActorError::history(HistoryReason::Recovery))?,
                )?;
            }
            ReplicaMessage::Ops(batch) => self.receive_ops(&batch)?,
            _ => {} // Never answer normal votes, flow credits, or recovery requests.
        }
        Ok(())
    }

    fn receive_state(
        &mut self,
        index: usize,
        state: &wire::RecoveryState,
        now: Duration,
    ) -> Result<(), ActorError> {
        if state.validate_response(self.requests[index]).is_err() {
            return Ok(());
        }
        let from = self.configuration.voters()[index];
        self.recovery.receive(from, state.response)?;
        self.observe(from, state.response.scope)?;
        if self.replies[index].is_none_or(|view| state.response.scope.view > view) {
            self.progress_at = now;
            self.replies[index] = Some(state.response.scope.view);
        }
        Ok(())
    }

    /// Whether `control` from `from` proves its view for this copy.
    fn authoritative(&self, from: NodeId, control: Control) -> bool {
        let primary = self.configuration.primary(control.scope().view);
        match control {
            // Volatile suspicion cannot supersede a recovery attempt.
            Control::ExitView(_) => false,
            Control::Commit(_) | Control::StartView(_) => from == primary,
            Control::PrepareOk { .. } | Control::DoViewChange(_) => self.local == primary,
            Control::StartViewChange(_) => true,
        }
    }

    /// Stage a fetched chunk for the journal, or keep the fetch for a retry
    /// when the arena has no room.
    fn receive_ops(&mut self, batch: &wire::OpsBatch<'_>) -> Result<(), ActorError> {
        let Some((request, _)) = self.fetch else {
            return Ok(());
        };
        if self.abandoning || self.pending.is_some() || batch.validate_response(request).is_err() {
            return Ok(());
        }
        let ticket = self
            .ticket
            .ok_or_else(|| ActorError::history(HistoryReason::Recovery))?;
        let bytes = batch.operations().map(|op| op.canonical().body.len()).sum();
        if !self
            .buffer
            .as_mut()
            .ok_or_else(|| ActorError::history(HistoryReason::Recovery))?
            .reserve_incoming(bytes)?
        {
            return Ok(()); // Preserve fetch correlation and retry without partial staging.
        }
        let mut buffer = self
            .buffer
            .take()
            .ok_or_else(|| ActorError::history(HistoryReason::BufferUnavailable))?;
        for operation in batch.operations() {
            buffer.push(operation.canonical())?;
        }
        self.fetch = None;
        self.pending = Some(Pending::Chunk(
            self.journal
                .receive_chunk(ticket, buffer)
                .map_err(|rejected| rejected.reason)?,
        ));
        Ok(())
    }

    fn receive_checkpoint(
        &mut self,
        request: wire::CheckpointRequest,
        payload: Bytes,
    ) -> Result<(), ActorError> {
        if self.abandoning
            || self.pending.is_some()
            || self
                .checkpoint_fetch
                .is_none_or(|(outstanding, _)| outstanding != request)
        {
            return Ok(());
        }
        let ticket = self
            .ticket
            .ok_or_else(|| ActorError::history(HistoryReason::Recovery))?;
        let completion = match self
            .journal
            .receive_checkpoint(ticket, request.offset, payload)
        {
            Ok(completion) => completion,
            Err(crate::replica_journal::SubmitError::Full) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        self.pending = Some(Pending::Checkpoint(completion));
        self.checkpoint_fetch = None;
        Ok(())
    }
}
