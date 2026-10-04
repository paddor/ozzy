//! Full-WAL validation and exact publication evidence before fenced restart.

use crate::{FrozenLog, Prefix, PreparedOperation, RecoveredState, ReplicationError};

use super::{Recovery, RecoveryError, RecoveryTicket, Transfer};

impl Recovery {
    /// Observe privately decoded canonical state at the pinned original anchor.
    /// The adapter must verify schema, bytes, digest, and revision before calling.
    /// This grants no vote or publication authority.
    pub fn complete_checkpoint(
        &mut self,
        ticket: RecoveryTicket,
        anchor: super::CheckpointAnchor,
        state_revision: u64,
    ) -> Result<(), RecoveryError> {
        self.require_ticket(&ticket)?;
        if ticket.checkpoint() != Some(anchor) {
            return Err(RecoveryError::StaleTransfer);
        }
        if state_revision != anchor.position.op.0 {
            return Err(RecoveryError::ApplicationPending);
        }
        self.pending
            .as_mut()
            .expect("checked transfer")
            .checkpoint_ready = true;
        Ok(())
    }

    /// Validate one count/byte-bounded chunk starting at the current full-WAL cursor.
    ///
    /// No descriptors or payloads are retained. The adapter must independently
    /// validate canonical bodies/application transitions, authenticate and bind
    /// the source/nonce, and stage these same bytes. Rejection leaves the cursor
    /// unchanged. Current-view prepares are included, not discarded as uncertain.
    pub fn validate_chunk(
        &mut self,
        ticket: RecoveryTicket,
        operations: &[PreparedOperation],
    ) -> Result<(), RecoveryError> {
        let pending = self.require_ticket(&ticket)?;
        if !pending.checkpoint_ready {
            return Err(RecoveryError::ApplicationPending);
        }
        if operations.is_empty() {
            return Err(ReplicationError::HistoryGap.into());
        }
        if operations.len() > self.limits.max_operations {
            return Err(ReplicationError::Capacity.into());
        }
        let mut through = pending.through;
        let mut bytes = 0_usize;
        for operation in operations {
            bytes = bytes
                .checked_add(operation.body_bytes)
                .ok_or(ReplicationError::Capacity)?;
            if bytes > self.limits.max_body_bytes {
                return Err(ReplicationError::Capacity.into());
            }
            if operation.group_id != ticket.scope.group_id
                || operation.configuration_epoch != ticket.scope.configuration_epoch
                || operation.original_view > ticket.scope.view
            {
                return Err(ReplicationError::ScopeMismatch.into());
            }
            if through.op.0.checked_add(1) != Some(operation.prefix.op.0) {
                return Err(ReplicationError::HistoryGap.into());
            }
            if operation.previous_digest != through.digest
                || operation.prefix.op > ticket.source.accepted.op
                || (operation.prefix.op == ticket.source.accepted.op
                    && operation.prefix != ticket.source.accepted)
                || (operation.prefix.op == ticket.committed.op
                    && operation.prefix != ticket.committed)
                || ticket.checkpoint.is_some_and(|anchor| {
                    operation.prefix.op == anchor.position.op && operation.prefix != anchor.position
                })
            {
                return Err(ReplicationError::ConflictingHistory.into());
            }
            through = operation.prefix;
        }
        self.pending
            .as_mut()
            .expect("checked pending transfer")
            .through = through;
        Ok(())
    }

    /// Consume actual durable full-store publication and private application validation.
    ///
    /// The worker must atomically publish exact configuration, selected history,
    /// and `promised_view = last_normal_view = ticket.scope().view` before reporting
    /// `durable`. `applied` asserts privately rebuilt canonical state, not consumer
    /// processing. Submission, received bytes, and page-cache writes do not qualify.
    ///
    /// This returns only validated disk state for `ViewChange::recover_intact`,
    /// never a same-view normal role. The existing fenced election/install gates
    /// must still run. Missing-store formatting and runtime orchestration remain
    /// separate adapters; an incomplete replacement must reopen nonvoting.
    pub fn complete(
        &mut self,
        ticket: RecoveryTicket,
        durable: Prefix,
        applied: Prefix,
    ) -> Result<RecoveredState, RecoveryError> {
        let pending = self.require_ticket(&ticket)?;
        if !pending.checkpoint_ready {
            return Err(RecoveryError::ApplicationPending);
        }
        if pending.through != ticket.source.accepted {
            return Err(RecoveryError::HistoryMissing);
        }
        if durable != ticket.source.accepted {
            return Err(RecoveryError::StoragePending);
        }
        if applied != ticket.committed {
            return Err(RecoveryError::ApplicationPending);
        }
        self.pending = None;
        self.completed = true;
        Ok(RecoveredState {
            scope: ticket.scope,
            log: FrozenLog {
                last_normal_view: ticket.scope.view,
                accepted: ticket.source.accepted,
                committed: ticket.committed,
            },
        })
    }

    /// Fence after an actual uncertain storage error, including an older-view action.
    /// Dropping a waiter is not storage failure. Stale callbacks cannot fault a
    /// different recovery attempt; no faulted instance may return recovered state.
    pub fn fail(&mut self, ticket: RecoveryTicket) -> Result<(), RecoveryError> {
        self.pending_ticket(&ticket)?;
        self.faulted = true;
        Ok(())
    }

    /// Retire fresh recovery authority after physical repair and full private
    /// replay of the original local history. Unlike full replacement, this must
    /// preserve the original last-normal view. The caller supplies actual durable
    /// publication evidence. Success permits only fenced intact restart.
    pub fn complete_repair(
        &mut self,
        ticket: RecoveryTicket,
        repaired: RecoveredState,
    ) -> Result<RecoveredState, RecoveryError> {
        self.require_ticket(&ticket)?;
        if repaired.scope
            != (crate::Scope {
                view: repaired.scope.view,
                ..ticket.scope
            })
            || repaired.scope.view < ticket.scope.view
            || repaired.log.last_normal_view > repaired.scope.view
            || [repaired.log.accepted, repaired.log.committed]
                .iter()
                .any(|prefix| (prefix.op.0 == 0) != (prefix.digest == crate::Digest::ZERO))
            || repaired.log.committed.op > repaired.log.accepted.op
            || (repaired.log.committed.op == repaired.log.accepted.op
                && repaired.log.committed != repaired.log.accepted)
        {
            return Err(RecoveryError::StoragePending);
        }
        self.pending = None;
        self.completed = true;
        Ok(repaired)
    }

    fn pending_ticket(&self, ticket: &RecoveryTicket) -> Result<Transfer, RecoveryError> {
        self.require_active()?;
        self.pending
            .filter(|pending| pending.ticket == *ticket)
            .ok_or(RecoveryError::StaleTransfer)
    }

    fn require_ticket(&self, ticket: &RecoveryTicket) -> Result<Transfer, RecoveryError> {
        let pending = self.pending_ticket(ticket)?;
        if self.view > ticket.scope.view {
            return Err(RecoveryError::StaleView);
        }
        Ok(pending)
    }
}
