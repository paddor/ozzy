//! Prepare admission: chain checks against the accepted history and
//! retention capacity for the new operations.

use super::{Admission, NormalReplica};
use crate::{OpNumber, Prefix, PreparedOperation, ReplicationError, Scope};
use ozzy_proto::NodeId;

/// Checked prepare slice: its chain end and the operations past the accepted prefix.
#[derive(Debug, Clone, Copy)]
struct Slice {
    end: Prefix,
    first_new: usize,
    new_body_bytes: usize,
}

impl NormalReplica {
    /// Admit a validated local-primary proposal or authenticated backup prepare.
    ///
    /// The full slice is checked before any admission. Retransmissions may
    /// overlap retained operations; identical entries do not consume capacity.
    /// Missing earlier operations request catch-up; conflicting current history
    /// faults this core. One call is bounded by the configured pipeline size.
    pub fn prepare(
        &mut self,
        from: NodeId,
        scope: Scope,
        operations: &[PreparedOperation],
    ) -> Result<Admission, ReplicationError> {
        self.require_scope(scope)?;
        self.require_primary(from)?;
        if operations.len() > self.limits.max_operations {
            return Err(ReplicationError::Capacity);
        }
        let slice = self.check_slice(scope, operations)?;
        let new = operations.get(slice.first_new..).unwrap_or_default();
        if new.is_empty() {
            return Ok(Admission::Duplicate);
        }
        if !self.ready_for_appends() {
            return Err(ReplicationError::ActivationPending);
        }
        let body_bytes = self.check_capacity(new.len(), slice)?;
        let ticket = self.journal.admit(new.len() as u64)?;
        self.operations.extend(new);
        self.body_bytes = body_bytes;
        self.accepted = slice.end;
        Ok(Admission::Write {
            ticket,
            first_new: slice.first_new,
        })
    }

    /// Check that a prepared slice continues this view's chain and matches the
    /// retained history it overlaps. A mismatch faults this core.
    fn check_slice(
        &mut self,
        scope: Scope,
        operations: &[PreparedOperation],
    ) -> Result<Slice, ReplicationError> {
        let Some(first) = operations.first() else {
            return Err(ReplicationError::EmptyPrepare);
        };
        let mut predecessor = first
            .prefix
            .op
            .0
            .checked_sub(1)
            .ok_or(ReplicationError::HistoryGap)
            .and_then(|op| self.prefix_at(OpNumber(op)))?;
        let mut first_new = operations.len();
        let mut new_body_bytes = 0usize;
        for (index, operation) in operations.iter().enumerate() {
            if operation.group_id != scope.group_id
                || operation.configuration_epoch != scope.configuration_epoch
                || operation.original_view > scope.view
                || (operation.prefix.op > self.accepted.op && operation.original_view != scope.view)
            {
                return Err(ReplicationError::ScopeMismatch);
            }
            if predecessor.op.0.checked_add(1) != Some(operation.prefix.op.0) {
                return Err(ReplicationError::HistoryGap);
            }
            if operation.previous_digest != predecessor.digest {
                return Err(self.conflict());
            }
            if operation.prefix.op <= self.accepted.op {
                if self.prefix_at(operation.prefix.op)? != operation.prefix {
                    return Err(self.conflict());
                }
            } else {
                first_new = first_new.min(index);
                new_body_bytes = new_body_bytes
                    .checked_add(operation.body_bytes)
                    .ok_or(ReplicationError::Capacity)?;
            }
            predecessor = operation.prefix;
        }
        Ok(Slice {
            end: predecessor,
            first_new,
            new_body_bytes,
        })
    }

    /// Bound new operations by the retained window and return the retained
    /// body bytes after admitting them.
    fn check_capacity(&self, new_count: usize, slice: Slice) -> Result<usize, ReplicationError> {
        let retained = self.retained_limits();
        let body_bytes = self
            .body_bytes
            .checked_add(slice.new_body_bytes)
            .ok_or(ReplicationError::Capacity)?;
        if new_count
            > retained
                .max_operations
                .saturating_sub(self.operations.len())
            || body_bytes > retained.max_body_bytes
        {
            return Err(ReplicationError::Capacity);
        }
        // A backup admits only what the primary can keep in flight beyond the
        // commit it announced; the retained bound above covers its own release
        // trailing that commit.
        if !self.is_primary()
            && slice.end.op.0.saturating_sub(self.live_base().0) > self.limits.max_operations as u64
        {
            return Err(ReplicationError::Capacity);
        }
        Ok(body_bytes)
    }
}
