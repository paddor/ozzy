//! Selected-view publication, activation, and late-view fencing.

mod stage;

use ozzy_proto::NodeId;

use crate::{
    Configuration, JournalGeneration, LogSource, NormalReplica, Prefix, PreparedOperation,
    PromiseTicket, ReplicationError, Scope, ViewChange, ViewChangeError,
};

/// Immutable `START_VIEW` sent only after the primary durably installs this log.
/// Sender identity comes from the authenticated configured primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartView {
    /// Installed group/configuration/view.
    pub scope: Scope,
    /// Primary's frozen installed writer incarnation for transfer/retry binding.
    pub generation: JournalGeneration,
    /// Entire selected accepted prefix, including uncertain old-view operations.
    pub accepted: Prefix,
    /// Primary's known commit floor at installation time.
    pub committed: Prefix,
}

/// A `START_VIEW` with the authenticated configured voter that sent it.
/// Backup installation still checks that this voter is the view's primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceivedStart {
    /// Authenticated sender of `start`.
    pub from: NodeId,
    /// Installed descriptor as received.
    pub start: StartView,
}

/// Exact selected-log publication action. Not evidence of successful I/O.
///
/// Persist the selected log with `promised_view = last_normal_view = scope.view`,
/// preserving `protected_committed` and installing `committed`. Only then report
/// completion, with canonical application state rebuilt through `committed`.
/// Local history through `retained` lies in the selected lineage and is kept;
/// only what follows is staged. A streamed backup stage proves that lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstallTicket {
    scope: Scope,
    previous_generation: JournalGeneration,
    generation: JournalGeneration,
    source: LogSource,
    protected_committed: Prefix,
    retained: Prefix,
    committed: Prefix,
}

impl InstallTicket {
    /// Exact group/configuration and installed view.
    pub const fn scope(self) -> Scope {
        self.scope
    }
    /// Replaced writer incarnation. Old callbacks lose authority on installation.
    pub const fn previous_generation(self) -> JournalGeneration {
        self.previous_generation
    }
    /// Fresh local writer incarnation for the selected generation.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }
    /// Frozen source from which canonical installation bytes are obtained.
    pub const fn source(self) -> LogSource {
        self.source
    }
    /// Highest old local commit; replacement must never overwrite it.
    pub const fn protected_committed(self) -> Prefix {
        self.protected_committed
    }
    /// Local prefix in the selected lineage, from `protected_committed` through
    /// `committed`. Staging starts after it. Election verifies it, except a
    /// streamed backup's, whose stage chains from it to the exact tail.
    pub const fn retained(self) -> Prefix {
        self.retained
    }
    /// Complete selected accepted prefix to install durably.
    pub const fn accepted(self) -> Prefix {
        self.source.accepted
    }
    /// Effective local committed prefix, never lower than prior local knowledge.
    pub const fn committed(self) -> Prefix {
        self.committed
    }
}

/// Exactly one owned protocol role after installation.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "rare ownership transition moves preallocated state without boxing"
)]
pub enum InstallOutcome {
    /// Installed view is still current. Primary admission additionally waits for
    /// a new-view backup ACK and ordered application through the selected tail.
    Normal(NormalReplica),
    /// A newer view arrived during I/O. No intermediate normal authority escapes.
    ViewChanging(ViewChange),
}

/// Rejected admission together with the original fenced role.
///
/// Recoverable errors leave it available for retransmission/history fetching.
/// Protected-history conflicts return the same role faulted for recovery.
#[derive(Debug)]
pub struct RejectedInstallation {
    view_change: ViewChange,
    error: ViewChangeError,
}

impl RejectedInstallation {
    /// Why installation could not be admitted.
    pub const fn error(&self) -> ViewChangeError {
        self.error
    }
    /// Recover protocol ownership; rejection never resurrects normal voting.
    pub fn into_view_change(self) -> ViewChange {
        self.view_change
    }
}

impl std::fmt::Display for RejectedInstallation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, formatter)
    }
}

impl std::error::Error for RejectedInstallation {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// In-flight atomic installation. Neither old nor new normal voting is exposed.
#[derive(Debug)]
pub struct InstallingView {
    ticket: InstallTicket,
    previous_promise: PromiseTicket,
    state: InstallState,
    configuration: Configuration,
    requested_view: u64,
    validated_through: Prefix,
}

/// Ownership of the staged role while publication is outstanding.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the staged role moves without boxing"
)]
enum InstallState {
    Pending(NormalReplica),
    /// Ambiguous publication failure; only reopen and recovery remain.
    Failed,
    /// Completion transferred the role to its next owner.
    Completed,
}

impl InstallState {
    fn pending(&self) -> Result<&NormalReplica, ViewChangeError> {
        match self {
            Self::Pending(normal) => Ok(normal),
            Self::Failed => Err(ViewChangeError::Faulted),
            Self::Completed => Err(ViewChangeError::InstallationCompleted),
        }
    }

    /// Transfer the staged role out exactly once.
    fn take(&mut self) -> Result<NormalReplica, ViewChangeError> {
        match std::mem::replace(self, Self::Completed) {
            Self::Pending(normal) => Ok(normal),
            Self::Failed => {
                *self = Self::Failed;
                Err(ViewChangeError::Faulted)
            }
            Self::Completed => Err(ViewChangeError::InstallationCompleted),
        }
    }
}

impl InstallingView {
    pub(crate) fn requested_scope(&self) -> Scope {
        Scope {
            view: self.requested_view,
            ..self.ticket.scope()
        }
    }

    /// Submit this exact action to a dedicated storage worker.
    pub const fn ticket(&self) -> InstallTicket {
        self.ticket
    }

    /// Validate the next bounded chunk of a streamed selected suffix.
    ///
    /// Operations start strictly after the effective committed floor and arrive
    /// in canonical order. Each call has the normal pipeline's count/byte limits;
    /// no descriptors or payloads are retained. The caller independently validates
    /// bodies/application transitions and stages the same bytes on the disk worker.
    /// Rejected chunks leave this cursor unchanged. This is not an I/O completion.
    pub fn validate_suffix(
        &mut self,
        operations: &[PreparedOperation],
    ) -> Result<(), ViewChangeError> {
        let (through, _) = self.state.pending()?.validate_install_chunk(
            &self.ticket,
            self.validated_through,
            operations,
        )?;
        self.validated_through = through;
        Ok(())
    }

    /// Fence the installing view when a configured peer advertises a later one.
    /// Retains only the highest view; no unbounded message backlog is needed.
    pub fn observe_view(&mut self, from: NodeId, scope: Scope) -> Result<(), ViewChangeError> {
        self.require_pending()?;
        self.configuration.voter_index(from)?;
        if (Scope {
            view: scope.view,
            ..self.ticket.scope
        }) != scope
        {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        self.requested_view = self.requested_view.max(scope.view);
        Ok(())
    }

    /// Local progress timeout can request a later view while disk work settles.
    pub fn request_view(&mut self, view: u64) -> Result<(), ViewChangeError> {
        self.require_pending()?;
        if view <= self.requested_view {
            return Err(ViewChangeError::ViewNotHigher);
        }
        self.requested_view = view;
        Ok(())
    }

    /// Complete actual durable publication and canonical application recovery.
    ///
    /// `applied` asserts that deterministic committed state, identities, and
    /// results have been rebuilt through this exact prefix. Wrong/stale callbacks
    /// leave ownership in this object. Successful completion transfers it once.
    pub fn complete(
        &mut self,
        ticket: InstallTicket,
        applied: Prefix,
    ) -> Result<InstallOutcome, ViewChangeError> {
        self.require_ticket(&ticket)?;
        if self.validated_through != ticket.accepted() {
            return Err(ViewChangeError::HistoryMissing);
        }
        if applied != ticket.committed {
            return Err(ViewChangeError::ApplicationPending);
        }
        let mut normal = self.state.take()?;
        normal.activate_installed();
        if self.requested_view > ticket.scope.view {
            Ok(InstallOutcome::ViewChanging(
                normal.into_view_change(self.requested_view)?,
            ))
        } else {
            Ok(InstallOutcome::Normal(normal))
        }
    }

    /// Complete a successful worker abort before publication was submitted.
    ///
    /// The adapter must first drain all admitted staging I/O and confirm the old
    /// journal remains selected. An ambiguous write/publication error cannot use
    /// this path. A later view is mandatory: never retry a different selection in
    /// the abandoned view. Only old durable history is restored, always fenced.
    pub fn complete_abandon(
        &mut self,
        ticket: InstallTicket,
    ) -> Result<ViewChange, ViewChangeError> {
        self.require_ticket(&ticket)?;
        if self.requested_view <= ticket.scope.view {
            return Err(ViewChangeError::ViewNotHigher);
        }
        ViewChange::after_abandon(
            self.state.take()?,
            self.previous_promise,
            self.requested_view,
        )
    }

    /// Ambiguous publication failure requires reopen/recovery, never old-role reuse.
    pub fn fail(&mut self, ticket: InstallTicket) -> Result<(), ViewChangeError> {
        self.require_ticket(&ticket)?;
        self.state = InstallState::Failed;
        Ok(())
    }

    pub(crate) fn require_pending(&self) -> Result<(), ViewChangeError> {
        self.state.pending().map(|_| ())
    }

    fn require_ticket(&self, ticket: &InstallTicket) -> Result<(), ViewChangeError> {
        self.require_pending()?;
        if *ticket != self.ticket {
            return Err(ViewChangeError::StaleInstallation);
        }
        Ok(())
    }
}
