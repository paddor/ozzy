//! Re-entering an election after restart or an abandoned installation,
//! without granting any same-view authority.

use super::{
    DoViewChange, Promise, PromiseTicket, RecoveredState, ViewChange, ViewChangeError,
    validate_report,
};
use crate::{
    Configuration, JournalGeneration, NormalReplica, PipelineLimits, ReplicationError, Scope,
};
use ozzy_proto::NodeId;

impl ViewChange {
    pub(crate) fn after_abandon(
        mut normal: NormalReplica,
        previous: PromiseTicket,
        requested_view: u64,
    ) -> Result<Self, ViewChangeError> {
        normal.restore_fenced(previous.generation(), previous.log());
        let mut changing = normal.into_view_change(requested_view)?;
        changing.promise = Promise::Completed(previous);
        Ok(changing)
    }

    /// Restart an intact durable voter without granting same-view authority.
    ///
    /// Restart from an installed normal view requests its successor, fencing a
    /// primary that sent prepares before its local write completed. An already
    /// promised but never installed election resumes that same view: it could
    /// not have assigned normal operations there. Repeated restarts cannot
    /// ratchet that unfinished election. Quorum selection and durable selected
    /// installation/activation remain mandatory before any normal authority.
    ///
    /// `generation` must be a fresh writer incarnation. Journal recovery and
    /// validation run on the storage worker before calling this sans-I/O method.
    /// Retained operations stay on disk; only the live pipeline is preallocated.
    /// Canonical application recovery is deferred to selected-view installation.
    pub fn recover_intact(
        configuration: Configuration,
        local: NodeId,
        generation: JournalGeneration,
        recovered: RecoveredState,
        limits: PipelineLimits,
    ) -> Result<Self, ViewChangeError> {
        if configuration.policy() == crate::QuorumPolicy::Replicated {
            return Err(ViewChangeError::VolatileRestart);
        }
        Self::recover_verified(configuration, local, generation, recovered, limits)
    }

    /// Restart a memory-voting broker from independently proven drained history.
    ///
    /// The adapter must verify a durable shutdown marker bound to the exact
    /// configuration, store, selected manifest, and accepted prefix, or a fresh
    /// completed group recovery. Before returning any driver it must replace
    /// that marker durably with running state. Intact bytes alone are insufficient:
    /// they cannot prove that an unpersisted RAM vote never existed.
    pub fn recover_drained(
        configuration: Configuration,
        local: NodeId,
        generation: JournalGeneration,
        recovered: RecoveredState,
        limits: PipelineLimits,
    ) -> Result<Self, ViewChangeError> {
        if configuration.policy() != crate::QuorumPolicy::Replicated {
            return Err(ReplicationError::PolicyMismatch.into());
        }
        Self::recover_verified(configuration, local, generation, recovered, limits)
    }

    fn recover_verified(
        configuration: Configuration,
        local: NodeId,
        generation: JournalGeneration,
        recovered: RecoveredState,
        limits: PipelineLimits,
    ) -> Result<Self, ViewChangeError> {
        if recovered.scope
            != (Scope {
                view: recovered.scope.view,
                ..configuration.scope()
            })
        {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        if recovered.log.last_normal_view > recovered.scope.view {
            return Err(ViewChangeError::InvalidReport);
        }
        let view = if recovered.scope.view > recovered.log.last_normal_view {
            recovered.scope.view
        } else {
            recovered
                .scope
                .view
                .checked_add(1)
                .ok_or(ViewChangeError::ViewExhausted)?
        };
        validate_report(DoViewChange {
            scope: Scope {
                view,
                ..recovered.scope
            },
            generation,
            log: recovered.log,
        })?;
        let normal =
            NormalReplica::recover_fenced(configuration, local, generation, recovered.log, limits)?;
        let mut changing = normal.into_view_change(view)?;
        changing.promise = if recovered.scope.view == view {
            changing.starts.insert(changing.local); // Independently recovered durable promise.
            Promise::Completed(PromiseTicket {
                scope: changing.scope(),
                generation,
                log: recovered.log,
            })
        } else {
            Promise::Prior(recovered.scope.view)
        };
        Ok(changing)
    }
}
