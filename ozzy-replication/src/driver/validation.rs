use std::time::Duration;

use ozzy_proto::NodeId;

use super::{DriverError, ReplicaDriver};
use crate::{
    Admission, JournalGeneration, Prefix, PreparedOperation, ReplicationError, Scope, Status,
};

/// Image/authority fence captured before asynchronous application validation.
///
/// This is not proof of valid operation bodies, a write reservation, or a vote.
/// The adapter must validate the exact submitted operations against the matching
/// application image and retain the resulting plans until admission. Changed
/// accepted/applied images or authority require fresh validation. A higher commit
/// boundary alone changes neither image until application advances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationTicket {
    local: NodeId,
    scope: Scope,
    generation: JournalGeneration,
    accepted: Prefix,
    committed: Prefix,
    applied: Prefix,
}

impl ValidationTicket {
    pub(crate) const fn from_local_image(
        local: NodeId,
        scope: Scope,
        generation: JournalGeneration,
        accepted: Prefix,
        committed: Prefix,
        applied: Prefix,
    ) -> Self {
        Self {
            local,
            scope,
            generation,
            accepted,
            committed,
            applied,
        }
    }
    /// Group, configuration, and view against which validation started.
    pub const fn scope(self) -> Scope {
        self.scope
    }

    /// Exact local writer incarnation. Reopen or installation invalidates it.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }

    /// Speculative journal/application prefix used for validation.
    pub const fn accepted(self) -> Prefix {
        self.accepted
    }

    /// Established commit boundary when validation started.
    pub const fn committed(self) -> Prefix {
        self.committed
    }

    /// Completed application boundary when validation started.
    pub const fn applied(self) -> Prefix {
        self.applied
    }

    pub(crate) fn same_images_as(self, current: Self) -> bool {
        self.local == current.local
            && self.scope == current.scope
            && self.generation == current.generation
            && self.accepted == current.accepted
            && self.applied == current.applied
            // Both immutable tickets came from this exact accepted hash chain.
            // Commit progress cannot alter speculative or applied state, but a
            // regression or a conflicting digest at the same position is stale.
            && (self.committed == current.committed || self.committed.op < current.committed.op)
    }
}

impl ReplicaDriver {
    /// Capture the exact live image and authority before worker-side validation.
    ///
    /// Validation itself may perform blocking identity-index reads on a worker.
    /// Capturing this ticket neither reserves pipeline capacity nor delays timers.
    /// Adapters bind speculative and applied images to their exact prefixes.
    /// The captured commit boundary separately bounds read/application work.
    pub fn begin_validation(&self) -> Result<ValidationTicket, DriverError> {
        let normal = self.normal().ok_or(ReplicationError::NotNormal)?;
        let snapshot = normal.snapshot();
        if snapshot.status != Status::Normal {
            return Err(ReplicationError::NotNormal.into());
        }
        if !snapshot.ready_for_appends {
            return Err(ReplicationError::ActivationPending.into());
        }
        Ok(ValidationTicket {
            local: self.local,
            scope: snapshot.scope,
            generation: snapshot.journal.generation,
            accepted: snapshot.accepted,
            committed: snapshot.committed,
            applied: snapshot.applied,
        })
    }

    /// Capture a ticket for validation that the worker runs right after an
    /// application through `applying.committed()`, where `applying` was
    /// captured on the current image. The ticket names that prefix as applied,
    /// the image the worker will hold. The core must reach it through
    /// `apply_through` before admission, or admission reports stale validation.
    pub fn begin_validation_after_apply(
        &self,
        applying: ValidationTicket,
    ) -> Result<ValidationTicket, DriverError> {
        let current = self.begin_validation()?;
        let through = applying.committed;
        if applying.local != current.local
            || applying.scope != current.scope
            || applying.generation != current.generation
            || applying.accepted != current.accepted
            || applying.applied != current.applied
            || through.op < current.applied.op
            || (through.op == current.applied.op && through != current.applied)
            || through.op > current.committed.op
            || (through.op == current.committed.op && through != current.committed)
        {
            return Err(DriverError::StaleValidation);
        }
        Ok(ValidationTicket {
            applied: through,
            ..current
        })
    }

    /// Admit worker-validated operations only if their image and authority remain live.
    ///
    /// Call before installing speculative plans, writing, or sending PREPARE.
    /// Stale validation is rejected without admitting operations or touching disk
    /// evidence. The adapter discards its plans and retries against a fresh image.
    /// A matching ticket still needs normal membership, lineage, and capacity checks.
    /// Commit may advance on the unchanged accepted chain while validation runs;
    /// this does not permit any change to the speculative or applied images.
    pub fn prepare_validated(
        &mut self,
        from: NodeId,
        ticket: ValidationTicket,
        operations: &[PreparedOperation],
        now: Duration,
    ) -> Result<Admission, DriverError> {
        self.observe_time(now)?;
        if !self
            .begin_validation()
            .is_ok_and(|current| ticket.same_images_as(current))
        {
            return Err(DriverError::StaleValidation);
        }
        self.prepare(from, ticket.scope, operations, now)
    }
}
