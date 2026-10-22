//! Selected-history installation into a fenced normal core. Only
//! `InstallingView` activates the result.

use super::{NormalReplica, Status};
use crate::{OpNumber, Prefix, PreparedOperation, ReplicationError, Voters};
use ozzy_journal::progress::JournalProgress;

impl NormalReplica {
    /// Prepare private post-install state without exposing durability or votes.
    /// Reuses the original preallocated descriptor ring. Only `InstallingView` can
    /// activate it after exact publication and application completion.
    pub(crate) fn validate_install(
        &self,
        ticket: &crate::InstallTicket,
        operations: &[PreparedOperation],
    ) -> Result<usize, ReplicationError> {
        let (previous, body_bytes) =
            self.validate_install_chunk(ticket, ticket.committed(), operations)?;
        if previous != ticket.accepted() {
            return Err(ReplicationError::ConflictingHistory);
        }
        Ok(body_bytes)
    }

    pub(crate) fn validate_install_chunk(
        &self,
        ticket: &crate::InstallTicket,
        mut previous: Prefix,
        operations: &[PreparedOperation],
    ) -> Result<(Prefix, usize), ReplicationError> {
        if self.status != Status::Fenced {
            return Err(ReplicationError::NotNormal);
        }
        if operations.len() > self.limits.max_operations {
            return Err(ReplicationError::Capacity);
        }
        let mut body_bytes = 0usize;
        for operation in operations {
            if operation.group_id != ticket.scope().group_id
                || operation.configuration_epoch != ticket.scope().configuration_epoch
                || operation.original_view >= ticket.scope().view
            {
                return Err(ReplicationError::ScopeMismatch);
            }
            if previous.op.0.checked_add(1) != Some(operation.prefix.op.0) {
                return Err(ReplicationError::HistoryGap);
            }
            if operation.previous_digest != previous.digest {
                return Err(ReplicationError::ConflictingHistory);
            }
            body_bytes = body_bytes
                .checked_add(operation.body_bytes)
                .ok_or(ReplicationError::Capacity)?;
            if body_bytes > self.limits.max_body_bytes {
                return Err(ReplicationError::Capacity);
            }
            previous = operation.prefix;
        }
        if previous.op > ticket.accepted().op
            || (previous.op == ticket.accepted().op && previous != ticket.accepted())
        {
            return Err(ReplicationError::ConflictingHistory);
        }
        Ok((previous, body_bytes))
    }

    pub(crate) fn stage_install(
        mut self,
        ticket: &crate::InstallTicket,
        operations: &[PreparedOperation],
        start: crate::StartView,
        body_bytes: usize,
    ) -> Self {
        self.operations.clear();
        self.operations.extend(operations);
        self.body_bytes = body_bytes;
        self.configuration.scope = ticket.scope();
        self.journal = JournalProgress::recover(ticket.generation(), ticket.accepted().op);
        self.accepted = ticket.accepted();
        self.committed = ticket.committed();
        self.applied = ticket.committed();
        self.retained_base = ticket.committed();
        self.known_commit = ticket.committed().op;
        self.acknowledged = Voters::splat(OpNumber(0));
        self.activation_tail = ticket.accepted().op;
        self.installed_view = Some(start);
        self.journal_tail = None;
        self
    }

    pub(crate) fn retain_installed_tail_on_disk(&mut self) {
        debug_assert!(self.operations.is_empty());
        self.journal_tail = (self.accepted.op > self.applied.op).then_some(self.accepted);
    }

    pub(crate) fn activate_installed(&mut self) {
        debug_assert_eq!(self.status, Status::Fenced);
        self.status = Status::Normal;
    }
}
