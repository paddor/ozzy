//! What a recovery attempt fetches next from its donors, and the paced
//! metadata, operation and checkpoint requests that carry it.

use super::{Pending, RecoveryActor, Retry};
use crate::replica_actor::{ActorError, Duration, Prefix, SendClass, control_message, offer, wire};
use crate::replica_journal::{RecoveryPlan, RecoveryStorage};
use ozzy_replication::recovery::RecoveryTicket;
use ozzy_replication::wire::FetchOps;

impl<J: RecoveryStorage> RecoveryActor<J> {
    /// Fetch the checkpoint, then operations after `staged`, and finish
    /// once the plan's history is staged.
    pub(super) fn transfer(
        &mut self,
        ticket: &RecoveryTicket,
        staged: Prefix,
        now: Duration,
    ) -> Result<(), ActorError> {
        if let Some(anchor) = ticket.checkpoint().filter(|_| !self.checkpoint_ready) {
            if self.checkpoint_fetch.is_none() {
                let request = wire::CheckpointRequest {
                    scope: ticket.scope(),
                    request_id: self.ids.request()?,
                    nonce: ticket.nonce(),
                    source: ticket.source(),
                    offset: self.checkpoint_through,
                    max_bytes: self
                        .config
                        .transfer
                        .max_body_bytes
                        .min(anchor.chunk_bytes as usize) as u32,
                };
                self.checkpoint_fetch = Some((request, Retry::new(self.timing)));
            }
            return self.request_checkpoint(now);
        }
        if matches!(self.plan, RecoveryPlan::Repair(None))
            || (self.plan == RecoveryPlan::Full && staged == ticket.source().accepted)
        {
            self.pending = Some(Pending::Finish(self.journal.finish_recovery(*ticket)?));
            return Ok(());
        }
        if self.fetch.is_none() {
            let max_operations = match self.plan {
                RecoveryPlan::Repair(Some(range)) => self
                    .config
                    .transfer
                    .max_operations
                    .min((range.through.op_number - range.after.op_number) as usize)
                    as u32,
                _ => self.config.transfer.max_operations as u32,
            };
            let request = FetchOps {
                scope: ticket.scope(),
                request_id: self.ids.request()?,
                source: ticket.source(),
                predecessor: staged,
                max_operations,
                max_body_bytes: self.config.transfer.max_body_bytes as u32,
            };
            self.fetch = Some((request, Retry::new(self.timing)));
        }
        self.request_chunk(now)
    }

    pub(super) fn request_quorum(&mut self, now: Duration) -> Result<(), ActorError> {
        for index in 0..3 {
            let to = self.configuration.voters()[index];
            if to != self.local
                && self.bindings[index].is_some()
                && self.replies[index] != Some(self.scope.view)
                && now >= self.retries[index].at
            {
                let encoded = wire::encode_recovery(
                    self.local,
                    self.config.sessions[index],
                    self.requests[index],
                    &mut self.metadata,
                    self.wire_limits,
                )?;
                let message = control_message(encoded, &self.metadata);
                offer(&mut self.outbox, to, SendClass::Control, message)?;
                self.retries[index].sent(now, self.timing);
            }
        }
        Ok(())
    }

    fn request_chunk(&mut self, now: Duration) -> Result<(), ActorError> {
        let Some((request, retry)) = self.fetch.as_mut() else {
            return Ok(());
        };
        if now < retry.at {
            return Ok(());
        }
        let request = *request;
        retry.sent(now, self.timing);
        let index = self
            .configuration
            .voters()
            .iter()
            .position(|voter| *voter == request.source.voter)
            .ok_or(ozzy_replication::recovery::RecoveryError::InvalidResponse)?;
        if self.bindings[index].is_none() {
            return Ok(());
        }
        let encoded = wire::encode_fetch(
            self.local,
            self.config.sessions[index],
            request,
            &mut self.metadata,
            self.wire_limits,
        )?;
        let message = control_message(encoded, &self.metadata);
        offer(
            &mut self.outbox,
            request.source.voter,
            SendClass::Data,
            message,
        )
    }

    fn request_checkpoint(&mut self, now: Duration) -> Result<(), ActorError> {
        let Some((request, retry)) = self.checkpoint_fetch.as_mut() else {
            return Ok(());
        };
        if now < retry.at {
            return Ok(());
        }
        let request = *request;
        retry.sent(now, self.timing);
        let index = self
            .configuration
            .voters()
            .iter()
            .position(|&voter| voter == request.source.voter)
            .ok_or(ozzy_replication::recovery::RecoveryError::InvalidResponse)?;
        if self.bindings[index].is_none() {
            return Ok(());
        }
        let encoded = wire::encode_checkpoint(
            self.local,
            self.config.sessions[index],
            wire::CheckpointMessage::Request(request),
            &mut self.metadata,
            self.wire_limits,
        )?;
        let message = control_message(encoded, &self.metadata);
        offer(
            &mut self.outbox,
            request.source.voter,
            SendClass::Control,
            message,
        )
    }
}
