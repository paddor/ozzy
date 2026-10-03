//! Selected-view publication, activation, and late-view fencing.

use ozzy_proto::NodeId;

use crate::{
    Configuration, Digest, JournalGeneration, LogSource, NormalReplica, OpNumber, Prefix,
    PreparedOperation, ReplicationError, Scope, ViewChange, ViewChangeError,
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

/// Exact selected-log publication action. Not evidence of successful I/O.
///
/// Persist the selected log with `promised_view = last_normal_view = scope.view`,
/// preserving `protected_committed` and installing `committed`. Only then report
/// completion, with canonical application state rebuilt through `committed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstallTicket {
    scope: Scope,
    previous_generation: JournalGeneration,
    generation: JournalGeneration,
    source: LogSource,
    protected_committed: Prefix,
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
    previous_promise: crate::PromiseTicket,
    normal: Option<NormalReplica>,
    configuration: Configuration,
    requested_view: u64,
    failed: bool,
    validated_through: Prefix,
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
        self.require_pending()?;
        let (through, _) = self
            .normal
            .as_ref()
            .expect("pending checked")
            .validate_install_chunk(self.ticket, self.validated_through, operations)?;
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
        self.require_ticket(ticket)?;
        if self.validated_through != ticket.accepted() {
            return Err(ViewChangeError::HistoryMissing);
        }
        if applied != ticket.committed {
            return Err(ViewChangeError::ApplicationPending);
        }
        let mut normal = self.normal.take().expect("pending checked");
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
        self.require_ticket(ticket)?;
        if self.requested_view <= ticket.scope.view {
            return Err(ViewChangeError::ViewNotHigher);
        }
        ViewChange::after_abandon(
            self.normal.take().expect("pending checked"),
            self.previous_promise,
            self.requested_view,
        )
    }

    /// Ambiguous publication failure requires reopen/recovery, never old-role reuse.
    pub fn fail(&mut self, ticket: InstallTicket) -> Result<(), ViewChangeError> {
        self.require_ticket(ticket)?;
        self.failed = true;
        Ok(())
    }

    pub(crate) fn require_pending(&self) -> Result<(), ViewChangeError> {
        if self.failed {
            return Err(ViewChangeError::Faulted);
        }
        if self.normal.is_none() {
            return Err(ViewChangeError::InstallationCompleted);
        }
        Ok(())
    }

    fn require_ticket(&self, ticket: InstallTicket) -> Result<(), ViewChangeError> {
        self.require_pending()?;
        if ticket != self.ticket {
            return Err(ViewChangeError::StaleInstallation);
        }
        Ok(())
    }
}

impl ViewChange {
    /// Stage primary installation whose selected tail remains in the journal.
    ///
    /// Feed `InstallingView::validate_suffix` bounded chunks before completion.
    /// Durable publication still installs the complete selected log atomically.
    /// After activation, a whole-tail new-view ACK and ordered application are
    /// required before fresh appends; old operations consume no live ring slots.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns owned fenced role"
    )]
    pub fn begin_primary_install(
        self,
        generation: JournalGeneration,
    ) -> Result<InstallingView, RejectedInstallation> {
        let validated = self.primary_install_ticket(generation);
        self.stage_streaming(&validated)
    }

    /// Stage bounded-stream backup installation after authenticated `START_VIEW`.
    ///
    /// Same promise, primary, and protected-prefix checks as `install_backup`.
    /// Every selected byte must be validated and durably published before the
    /// backup can ACK. Fresh prepares also wait for whole-tail commit/application.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns owned fenced role"
    )]
    pub fn begin_backup_install(
        mut self,
        from: NodeId,
        start: StartView,
        generation: JournalGeneration,
        mut lookup: impl FnMut(LogSource, OpNumber) -> Option<Digest>,
    ) -> Result<InstallingView, RejectedInstallation> {
        let validated = self.backup_install_ticket(from, start, generation, &mut lookup);
        self.stage_streaming(&validated)
    }

    /// Consume the selected primary's old role and stage atomic installation.
    ///
    /// `suffix` is the verified selected tail strictly after `ticket.committed`,
    /// not the full retained WAL. Transfer/application/storage independently
    /// validate earlier history and preserve the protected old committed prefix.
    /// Staging checks operation/byte bounds and reuses the old descriptor arena.
    /// Rejection returns the fenced role so missing data can be retried.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns protocol ownership without allocation"
    )]
    pub fn install_primary(
        self,
        generation: JournalGeneration,
        suffix: &[PreparedOperation],
    ) -> Result<InstallingView, RejectedInstallation> {
        let validated = (|| {
            let (ticket, start) = self.primary_install_ticket(generation)?;
            let bytes = self.normal.validate_install(ticket, suffix)?;
            Ok((ticket, start, bytes))
        })();
        self.stage_install(&validated, suffix)
    }

    /// Consume a backup's fenced role after an authenticated `START_VIEW`.
    ///
    /// Unlike primary selection, this needs no local first/second-phase quorum:
    /// the current primary's installed descriptor supplies protocol authority.
    /// It still requires this backup's completed view promise and verified
    /// compatibility with its own possibly higher committed prefix.
    /// `lookup` has the same pinned, nonblocking lineage contract as `select`.
    /// `suffix` starts after the greater of local and announced commit knowledge.
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns protocol ownership without allocation"
    )]
    pub fn install_backup(
        mut self,
        from: NodeId,
        start: StartView,
        generation: JournalGeneration,
        suffix: &[PreparedOperation],
        mut lookup: impl FnMut(LogSource, OpNumber) -> Option<Digest>,
    ) -> Result<InstallingView, RejectedInstallation> {
        let validated = (|| {
            let (ticket, start) =
                self.backup_install_ticket(from, start, generation, &mut lookup)?;
            let bytes = self.normal.validate_install(ticket, suffix)?;
            Ok((ticket, start, bytes))
        })();
        self.stage_install(&validated, suffix)
    }

    fn primary_install_ticket(
        &self,
        generation: JournalGeneration,
    ) -> Result<(InstallTicket, StartView), ViewChangeError> {
        self.require_promised()?;
        self.require_primary()?;
        let selected = self.selected.ok_or(ViewChangeError::SelectionRequired)?;
        let start = StartView {
            scope: selected.scope(),
            generation,
            accepted: selected.source().accepted,
            committed: selected.committed(),
        };
        Ok((
            self.install_ticket(generation, selected.source(), selected.committed())?,
            start,
        ))
    }

    fn backup_install_ticket(
        &mut self,
        from: NodeId,
        start: StartView,
        generation: JournalGeneration,
        lookup: &mut impl FnMut(LogSource, OpNumber) -> Option<Digest>,
    ) -> Result<(InstallTicket, StartView), ViewChangeError> {
        self.require_scope(start.scope)?;
        self.require_promised()?;
        self.configuration.voter_index(from)?;
        if from != self.configuration.primary(start.scope.view)
            || from == self.configuration.voters()[self.local]
        {
            return Err(ReplicationError::WrongRole.into());
        }
        if !crate::view_change::valid_prefix(start.accepted)
            || !crate::view_change::valid_prefix(start.committed)
            || start.committed.op > start.accepted.op
        {
            return Err(ViewChangeError::InvalidReport);
        }
        let source = LogSource {
            voter: from,
            generation: start.generation,
            accepted: start.accepted,
        };
        let local_commit = self.normal.snapshot().committed;
        self.verify_prefix(source, local_commit, lookup)?;
        self.verify_prefix(source, start.committed, lookup)?;
        let committed = if local_commit.op > start.committed.op {
            local_commit
        } else {
            start.committed
        };
        Ok((self.install_ticket(generation, source, committed)?, start))
    }

    #[expect(
        clippy::result_large_err,
        reason = "rejection returns owned fenced role"
    )]
    fn stage_streaming(
        self,
        validated: &Result<(InstallTicket, StartView), ViewChangeError>,
    ) -> Result<InstallingView, RejectedInstallation> {
        let mut installing =
            self.stage_install(&validated.map(|(ticket, start)| (ticket, start, 0)), &[])?;
        installing.validated_through = installing.ticket.committed();
        installing
            .normal
            .as_mut()
            .expect("staged role")
            .retain_installed_tail_on_disk();
        Ok(installing)
    }

    fn install_ticket(
        &self,
        generation: JournalGeneration,
        source: LogSource,
        committed: Prefix,
    ) -> Result<InstallTicket, ViewChangeError> {
        let previous = self.normal.snapshot();
        if generation == previous.journal.generation {
            return Err(ViewChangeError::ReusedGeneration);
        }
        Ok(InstallTicket {
            scope: self.scope(),
            previous_generation: previous.journal.generation,
            generation,
            source,
            protected_committed: previous.committed,
            committed,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "rejection returns protocol ownership without allocation"
    )]
    fn stage_install(
        self,
        validated: &Result<(InstallTicket, StartView, usize), ViewChangeError>,
        suffix: &[PreparedOperation],
    ) -> Result<InstallingView, RejectedInstallation> {
        let (ticket, start, bytes) = match *validated {
            Ok(values) => values,
            Err(error) => {
                return Err(RejectedInstallation {
                    view_change: self,
                    error,
                });
            }
        };
        let previous_promise = self
            .completed
            .expect("installation requires durable promise");
        let normal = self.normal.stage_install(ticket, suffix, start, bytes);
        Ok(InstallingView {
            normal: Some(normal),
            ticket,
            previous_promise,
            configuration: self.configuration,
            requested_view: ticket.scope.view,
            failed: false,
            validated_through: ticket.accepted(),
        })
    }
}
