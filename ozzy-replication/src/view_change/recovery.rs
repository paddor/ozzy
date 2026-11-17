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
    /// primary that sent prepares before its local write completed. A backup
    /// of that view may instead rejoin it through `begin_rejoin_install`, and
    /// so may a primary with evidence of a clean stop (`resume_primary`). An already
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
            // The view's primary may have sent prepares it never stored, so
            // it must leave the view. A backup acknowledged only what its
            // disk holds: its restart is a pause, and it may rejoin.
            if configuration.primary(recovered.scope.view) != local {
                changing.rejoin = Some(PromiseTicket {
                    scope: recovered.scope,
                    generation,
                    log: recovered.log,
                });
            }
            Promise::Prior(recovered.scope.view)
        };
        Ok(changing)
    }

    /// Let the restarted primary of the last installed view rejoin it, as a
    /// backup may. The caller asserts a clean stop: this primary stored
    /// every operation it ever sent, the exact log `recovered` names, and
    /// has sent nothing since. Its restart is then a pause too. Without
    /// that evidence it must leave the view.
    ///
    /// `committed` is what a quorum held of that log when the primary
    /// stopped. The primary resumes with that much committed, however far
    /// the commit its journal recorded trails it.
    ///
    /// Returns whether the right was granted. It is not when the copy is
    /// not that primary, already promised a later view, or `committed`
    /// does not lie between the recorded commit and the end of the log.
    pub fn resume_primary(
        &mut self,
        generation: JournalGeneration,
        recovered: RecoveredState,
        committed: crate::Prefix,
    ) -> bool {
        let installed = recovered.scope.view;
        let mut log = recovered.log;
        if self.promise != Promise::Prior(installed)
            || log.last_normal_view != installed
            || self.configuration.primary(installed) != self.configuration.voter(self.local)
            || committed.op < log.committed.op
            || committed.op > log.accepted.op
            || (committed.op == log.committed.op && committed != log.committed)
            || (committed.op == log.accepted.op && committed != log.accepted)
        {
            return false;
        }
        log.committed = committed;
        self.rejoin = Some(PromiseTicket {
            scope: recovered.scope,
            generation,
            log,
        });
        true
    }
}
