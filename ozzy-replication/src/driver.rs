//! Deterministic normal-to-election scheduling with explicit durable completions.
//!
//! The adapter supplies monotonic process-relative time and authenticated,
//! wire-validated controls. One poll emits at most one action; broadcast has
//! exactly two destinations. Keep outbound work bounded and independently retry
//! sends without blocking receive, timers, or disk completion. Poll again after
//! each action/completion, subject to the adapter's count/byte scheduling budget.
//! Selection and pinned history lookup remain explicit adapter work. The driver
//! owns in-flight installation while the adapter stages and publishes on its disk
//! worker. This driver does not itself enable networked process failover.

mod exit_view;
mod installation;
mod validation;

pub use installation::ActivationTicket;
pub use validation::ValidationTicket;

use std::time::Duration;

use ozzy_proto::NodeId;

use crate::wire::Control;
use crate::{
    Admission, Configuration, Digest, InstallingView, JournalGeneration, LogSource, NormalReplica,
    OpNumber, Prefix, PreparedOperation, PromiseTicket, ReplicaSnapshot, ReplicationError, Scope,
    SelectedView, StartView, Status, SyncTicket, ViewChange, ViewChangeError, WriteTicket,
};

/// Independent heartbeat, progress, and election retry intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// Period between primary idle commit announcements.
    pub heartbeat: Duration,
    /// Maximum primary silence, missing commit progress, or stalled local persistence.
    pub primary_timeout: Duration,
    /// Minimum interval between repeated first/second-phase election sends.
    pub retransmit: Duration,
    /// Initial deadline for election, selected-log publication, or tail activation.
    pub election_timeout: Duration,
    /// Ceiling when doubling the deadline after unsuccessful election timeouts.
    pub max_election_timeout: Duration,
}

impl Timing {
    fn validate(self, now: Duration) -> Result<(), DriverError> {
        if self.heartbeat.is_zero()
            || self.retransmit.is_zero()
            || self.heartbeat >= self.primary_timeout
            || self.retransmit >= self.election_timeout
            || self.election_timeout > self.max_election_timeout
        {
            return Err(DriverError::InvalidTiming);
        }
        if now
            .checked_add(self.max_election_timeout.max(self.primary_timeout))
            .is_none()
        {
            return Err(DriverError::TimeExhausted);
        }
        Ok(())
    }
}

/// One bounded network or storage request. Not evidence of its completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Send to both other configured voters, never to the local voter.
    Broadcast(Control),
    /// Send to one configured voter.
    Send {
        /// Configured destination identity, not a socket routing identity.
        to: NodeId,
        /// Control message to encode in that peer's established session.
        message: Control,
    },
    /// Persist the exact frozen history and view promise on the disk worker.
    PersistPromise(PromiseTicket),
}

/// Owned protocol phase; extracting it does not establish installation authority.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "move preallocated roles without boxing"
)]
pub enum Role {
    /// Existing normal state, including any fault/fence status.
    Normal(NormalReplica),
    /// Fenced old state and bounded election evidence.
    ViewChanging(ViewChange),
    /// Owned selected-log publication; no normal authority until exact completion.
    Installing(InstallingView),
}

/// Timer and election-message driver for one fixed three-voter group.
///
/// No clocks, allocations, I/O, or automatic durability assertions on transitions.
/// Old writes must settle before a promise; a pending promise survives later views.
#[derive(Debug)]
pub struct ReplicaDriver {
    role: Option<Role>,
    configuration: Configuration,
    local: NodeId,
    timing: Timing,
    now: Duration,
    contact_deadline: Duration,
    progress_deadline: Duration,
    persistence_deadline: Duration,
    election_deadline: Duration,
    election_delay: Duration,
    retry_at: Duration,
    start_pending: bool,
    report_pending: bool,
    commit_pending: bool,
    // At most one selected-tail activation response per remote voter. A late
    // installed voter cannot receive fresh payload until that tail is applied.
    activation_pending: u8,
    /// Current-view timeout suspicions. Never durable or history-selection votes.
    exit_votes: u8,
    /// `None` means local suspicion is inactive or was withdrawn after progress.
    exit_retry_at: Option<Duration>,
    /// An older view peers still ask to leave, and which voters asked. This
    /// voter already left it.
    exit_echo: Option<(Scope, u8)>,
    /// Earliest next answer to each voter's request to leave an older view.
    exit_echo_at: [Duration; 3],
}

impl ReplicaDriver {
    /// Attach scheduling to a bootstrapped or completely installed normal core.
    /// Intact restart must instead enter the existing nonvoting recovery path.
    pub fn from_normal(
        normal: NormalReplica,
        now: Duration,
        timing: Timing,
    ) -> Result<Self, DriverError> {
        let (configuration, local) = normal.driver_identity();
        Self::new(Role::Normal(normal), configuration, local, now, timing)
    }

    /// Resume scheduling after intact-disk recovery or a superseded installation.
    /// The supplied core is already fenced and cannot restore old normal authority.
    pub fn from_view_change(
        changing: ViewChange,
        now: Duration,
        timing: Timing,
    ) -> Result<Self, DriverError> {
        let configuration = changing.configuration;
        let local = configuration.voters()[changing.local];
        Self::new(
            Role::ViewChanging(changing),
            configuration,
            local,
            now,
            timing,
        )
    }

    fn new(
        role: Role,
        configuration: Configuration,
        local: NodeId,
        now: Duration,
        timing: Timing,
    ) -> Result<Self, DriverError> {
        timing.validate(now)?;
        let changing = matches!(role, Role::ViewChanging(_));
        let activating =
            matches!(&role, Role::Normal(normal) if !normal.snapshot().ready_for_appends);
        Ok(Self {
            role: Some(role),
            configuration,
            local,
            timing,
            now,
            contact_deadline: now + timing.primary_timeout,
            persistence_deadline: now + timing.primary_timeout,
            progress_deadline: now
                + if activating {
                    timing.election_timeout
                } else {
                    timing.primary_timeout
                },
            election_deadline: now + timing.election_timeout,
            election_delay: timing.election_timeout,
            retry_at: now
                + if changing {
                    timing.retransmit
                } else {
                    timing.heartbeat
                },
            start_pending: changing,
            report_pending: changing,
            commit_pending: false,
            activation_pending: 0,
            exit_votes: 0,
            exit_retry_at: None,
            exit_echo: None,
            exit_echo_at: [Duration::ZERO; 3],
        })
    }

    /// Inspect normal evidence. No mutable access can bypass scheduling/fencing.
    pub fn normal(&self) -> Option<&NormalReplica> {
        match self.role.as_ref().expect("owned role") {
            Role::Normal(normal) => Some(normal),
            Role::ViewChanging(_) | Role::Installing(_) => None,
        }
    }

    /// A fenced old view needs an actual barrier before publishing its promise.
    pub fn election_needs_sync(&self) -> bool {
        let Some(Role::ViewChanging(changing)) = &self.role else {
            return false;
        };
        let journal = changing.normal_snapshot().journal;
        journal.written == journal.accepted && journal.written > journal.durable
    }

    /// Current normal or proposed election scope, not necessarily durable.
    pub fn scope(&self) -> Scope {
        match self.role.as_ref().expect("owned role") {
            Role::Normal(normal) => normal.snapshot().scope,
            Role::ViewChanging(changing) => changing.scope(),
            Role::Installing(installing) => installing.requested_scope(),
        }
    }

    /// Extract phase ownership. Pending I/O still must settle; a replacement driver
    /// must preserve newer-view fencing rather than resume an old normal role.
    pub fn into_role(self) -> Role {
        self.role.expect("owned role")
    }

    /// Advance deadlines and emit at most one action, without waiting for I/O.
    /// Repeated polls at one instant do not create unbounded retransmission.
    pub fn poll(&mut self, now: Duration) -> Result<Option<Action>, DriverError> {
        self.observe_time(now)?;
        if let Some(Role::Installing(installing)) = &self.role {
            installing.require_pending()?;
            if let Some(action) = self.exit_echo_action() {
                return Ok(Some(action));
            }
            if now >= self.election_deadline {
                self.request_exit()?;
            }
            // Suspicion can travel, but publication must settle before a promise.
            return Ok(self.exit_action());
        }
        if let Some(Role::ViewChanging(changing)) = &self.role
            && changing.normal_snapshot().status == Status::Faulted
        {
            return Err(ViewChangeError::Faulted.into());
        }
        if let Some(action) = self.exit_echo_action() {
            return Ok(Some(action));
        }
        if let Some(normal) = self.normal() {
            let snapshot = normal.snapshot();
            if snapshot.status != Status::Normal {
                return Err(ReplicationError::NotNormal.into());
            }
            let primary = self.configuration.primary(snapshot.scope.view) == self.local;
            let outstanding =
                snapshot.accepted != snapshot.committed || !snapshot.ready_for_appends;
            if self.normal_timed_out(&snapshot) {
                self.request_exit()?;
                if self.normal().is_some() {
                    return Ok(self.exit_action());
                }
            } else {
                self.withdraw_exit();
                if !outstanding {
                    self.progress_deadline = now + self.timing.primary_timeout;
                }
                if primary && self.commit_pending {
                    self.commit_pending = false;
                    let message = self.normal().expect("normal role").announcement()?;
                    return Ok(Some(Action::Broadcast(Control::Commit(message))));
                }
                if primary && let Some(action) = self.activation_action()? {
                    return Ok(Some(action));
                }
                if primary && now >= self.retry_at {
                    self.retry_at = now + self.timing.heartbeat;
                    if let Some(start) = self.normal().expect("normal role").start_view()? {
                        self.commit_pending = true;
                        return Ok(Some(Action::Broadcast(Control::StartView(start))));
                    }
                    let message = self.normal().expect("normal role").announcement()?;
                    return Ok(Some(Action::Broadcast(Control::Commit(message))));
                }
                return Ok(None);
            }
        } else if now >= self.election_deadline {
            self.request_exit()?;
        }
        if let Some(action) = self.exit_action() {
            return Ok(Some(action));
        }
        if now >= self.retry_at {
            self.start_pending = true;
            self.report_pending = true;
            self.retry_at = now + self.timing.retransmit;
        }
        let Role::ViewChanging(changing) = self.role.as_mut().expect("owned role") else {
            unreachable!("normal path returned or entered election");
        };
        if changing.promised_view() != changing.scope().view {
            return match changing.begin_promise() {
                Ok(ticket) => Ok(Some(Action::PersistPromise(ticket))),
                Err(ViewChangeError::StoragePending | ViewChangeError::PromisePending) => Ok(None),
                Err(error) => Err(error.into()),
            };
        }
        if self.start_pending {
            let message = changing.start_message()?;
            self.start_pending = false;
            return Ok(Some(Action::Broadcast(Control::StartViewChange(message))));
        }
        if self.report_pending {
            match changing.report() {
                Ok(report) => {
                    self.report_pending = false;
                    let to = self.configuration.primary(report.scope.view);
                    if to != self.local {
                        return Ok(Some(Action::Send {
                            to,
                            message: Control::DoViewChange(report),
                        }));
                    }
                }
                Err(ViewChangeError::StartQuorumMissing) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(None)
    }

    /// Receive one authenticated, wire-validated control. Stale views are ignored.
    /// A timeout only sends volatile `EXIT_VIEW` suspicion until a quorum agrees.
    /// A durably promised `START_VIEW_CHANGE` instead fences older authority,
    /// including an intact voter's restart request. These distinct messages must
    /// not be interchanged. Senders must obey the crash-fault protocol, not merely
    /// supply a claimed node identity.
    ///
    /// A returned `START_VIEW` needs bounded retention by the adapter until its
    /// local promise completes, then verified selected installation. It never
    /// activates a normal role here. Exact duplicate installed descriptors return
    /// `None` without reinstalling and permit replay of the normal durable ACK.
    /// Normal ACK grants remain adapter-owned.
    pub fn receive(
        &mut self,
        from: NodeId,
        message: Control,
        now: Duration,
    ) -> Result<Option<StartView>, DriverError> {
        self.observe_time(now)?;
        let voter = self.configuration.voter_index(from)?;
        if from == self.local {
            return Err(ReplicationError::WrongRole.into());
        }
        let scope = message.scope();
        if scope
            != (Scope {
                view: scope.view,
                ..self.configuration.scope()
            })
        {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        if let Control::ExitView(_) = message {
            self.receive_exit(voter, scope)?;
            return Ok(None);
        }
        if scope.view < self.scope().view {
            return Ok(None);
        }
        let primary = self.configuration.primary(scope.view);
        match message {
            Control::Commit(_) | Control::StartView(_) if from != primary => {
                return Err(ReplicationError::WrongRole.into());
            }
            Control::PrepareOk { .. }
            | Control::PrepareRetained { .. }
            | Control::DoViewChange(_)
                if self.local != primary =>
            {
                return Err(ReplicationError::WrongRole.into());
            }
            _ => {}
        }
        if let Control::StartViewChange(_) = message {
            match self.role.as_ref().expect("owned role") {
                Role::Normal(normal) if normal.snapshot().status == Status::Faulted => {
                    return Err(ViewChangeError::Faulted.into());
                }
                Role::ViewChanging(changing)
                    if changing.normal_snapshot().status == Status::Faulted =>
                {
                    return Err(ViewChangeError::Faulted.into());
                }
                Role::Installing(installing) => installing.require_pending()?,
                Role::Normal(_) | Role::ViewChanging(_) => {}
            }
            // A first-phase sender has durably fenced its previous authority.
            // Timeout-only suspicion uses EXIT_VIEW and cannot reach this path.
            if scope.view > self.scope().view {
                self.change_view(scope.view)?;
            }
            if scope.view != self.scope().view {
                return Ok(None);
            }
        }
        if scope.view > self.scope().view {
            self.change_view(scope.view)?;
        }
        match self.role.as_mut().expect("owned role") {
            Role::Normal(normal) => {
                let committed = normal.snapshot().committed;
                match message {
                    Control::Commit(commit) => {
                        normal.receive_commit(from, commit)?;
                        self.contact_deadline = now + self.timing.primary_timeout;
                    }
                    Control::PrepareOk { ack, .. } => normal.receive_ack(from, ack)?,
                    Control::PrepareRetained { ack, .. } => {
                        normal.receive_retained_ack(from, ack)?;
                    }
                    Control::StartView(start) => {
                        normal.observe_installed_start(from, start)?;
                        self.contact_deadline = now + self.timing.primary_timeout;
                    }
                    _ => return Ok(None),
                }
                if let Some(prefix) = message.acknowledged()
                    && let Some(start) = normal.start_view()?
                    && prefix == start.accepted
                    && start.accepted.op < normal.snapshot().committed.op
                {
                    // Selected history plus an established quorum authorizes
                    // replay. An old vote alone cannot create commit authority.
                    self.activation_pending |= 1 << voter;
                }
                if normal.snapshot().committed != committed {
                    self.progress_deadline = now + self.progress_timeout();
                }
            }
            Role::ViewChanging(changing) => match message {
                Control::StartViewChange(start) => changing.receive_start(from, start)?,
                Control::DoViewChange(report) => changing.receive_report(from, report)?,
                Control::StartView(start) => return Ok(Some(start)),
                Control::Commit(_)
                | Control::PrepareOk { .. }
                | Control::PrepareRetained { .. }
                | Control::ExitView(_) => {}
            },
            Role::Installing(installing) => installing.observe_view(from, scope)?,
        }
        Ok(None)
    }

    /// Observe actual durable publication of the exact outstanding promise.
    pub fn complete_promise(&mut self, ticket: PromiseTicket) -> Result<(), DriverError> {
        self.changing()?.complete_promise(ticket)?;
        Ok(())
    }

    /// Report an uncertain metadata publication. No election retry can reuse it.
    pub fn fail_promise(&mut self, ticket: PromiseTicket) -> Result<(), DriverError> {
        self.changing()?.fail_promise(ticket)?;
        Ok(())
    }

    /// Admit an already body/application-validated canonical group. The returned
    /// ticket schedules disk work; transmission may overlap that work. Later
    /// views require selected installation, never implicit normal activation.
    pub fn prepare(
        &mut self,
        from: NodeId,
        scope: Scope,
        operations: &[PreparedOperation],
        now: Duration,
    ) -> Result<Admission, DriverError> {
        self.observe_time(now)?;
        self.configuration.voter_index(from)?;
        if scope
            != (Scope {
                view: scope.view,
                ..self.configuration.scope()
            })
        {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        if from != self.configuration.primary(scope.view) {
            return Err(ReplicationError::WrongRole.into());
        }
        if scope.view > self.scope().view {
            self.change_view(scope.view)?;
        }
        let Role::Normal(normal) = self.role.as_mut().expect("owned role") else {
            return Err(ReplicationError::NotNormal.into());
        };
        let previous = normal.snapshot();
        let admitted = normal.prepare(from, scope, operations)?;
        if normal.snapshot().accepted != previous.accepted
            && previous.accepted == previous.committed
            && previous.ready_for_appends
        {
            self.progress_deadline = now + self.timing.primary_timeout;
        }
        if normal.snapshot().accepted != previous.accepted
            && previous.accepted.op == self.storage_progress(&previous)
        {
            self.persistence_deadline = now + self.timing.primary_timeout;
        }
        self.contact_deadline = now + self.timing.primary_timeout;
        Ok(admitted)
    }

    /// Settle actual complete writes, including writes admitted before a timeout.
    /// Completion while changing view never restores normal voting or commit.
    pub fn complete_write(&mut self, ticket: WriteTicket) -> Result<(), DriverError> {
        match self.role.as_mut().expect("owned role") {
            Role::Normal(normal) => normal.complete_write(ticket)?,
            Role::ViewChanging(changing) => changing.complete_write(ticket)?,
            Role::Installing(_) => return Err(ReplicationError::WrongRole.into()),
        }
        Ok(())
    }

    /// Observe buffered background I/O without asserting stable-storage evidence.
    /// Only RAM-confirmed groups may release capacity at this boundary.
    pub fn complete_buffered_write(
        &mut self,
        ticket: WriteTicket,
        now: Duration,
    ) -> Result<(), DriverError> {
        if self.configuration.policy() != crate::QuorumPolicy::Replicated {
            return Err(ReplicationError::PolicyMismatch.into());
        }
        self.observe_time(now)?;
        let previous = self.normal().map(NormalReplica::snapshot);
        self.complete_write(ticket)?;
        if let Some(previous) = previous {
            self.observe_normal_progress(&previous, now);
        }
        Ok(())
    }

    /// Install exact `O_DSYNC` completion independently of the group's confirmation policy.
    pub fn complete_durable_write(
        &mut self,
        ticket: WriteTicket,
        now: Duration,
    ) -> Result<(), DriverError> {
        self.observe_time(now)?;
        match self.role.as_mut().expect("owned role") {
            Role::Normal(normal) => {
                let previous = normal.snapshot();
                normal.complete_durable_write(ticket)?;
                self.observe_normal_progress(&previous, now);
            }
            Role::ViewChanging(changing) => changing.normal.complete_durable_write(ticket)?,
            Role::Installing(_) => return Err(ReplicationError::WrongRole.into()),
        }
        Ok(())
    }

    /// Complete deterministic ordered application through an established commit.
    /// Application replay/identity-index I/O stays outside this driver.
    pub fn apply_through(&mut self, through: Prefix) -> Result<(), DriverError> {
        let Role::Normal(normal) = self.role.as_mut().expect("owned role") else {
            return Err(ReplicationError::NotNormal.into());
        };
        let was_ready = normal.snapshot().ready_for_appends;
        normal.apply_through(through)?;
        if !was_ready && normal.snapshot().ready_for_appends {
            self.progress_deadline = self.now + self.timing.primary_timeout;
            self.election_delay = self.timing.election_timeout;
        }
        Ok(())
    }

    /// Fence on uncertain storage failure. A stale writer cannot fault a new one.
    pub fn fail_io(&mut self, generation: JournalGeneration) -> Result<(), DriverError> {
        match self.role.as_mut().expect("owned role") {
            Role::Normal(normal) => normal.fail_io(generation)?,
            Role::ViewChanging(changing) => changing.fail_io(generation)?,
            Role::Installing(installing) => {
                let ticket = installing.ticket();
                if generation != ticket.generation() {
                    return Err(ViewChangeError::StaleInstallation.into());
                }
                installing.fail(ticket)?;
            }
        }
        Ok(())
    }

    /// Capture the written prefix for a real disk barrier, never a simulated ACK.
    pub fn begin_sync(&self) -> Result<SyncTicket, DriverError> {
        match self.role.as_ref().expect("owned role") {
            Role::Normal(normal) => Ok(normal.begin_sync()?),
            Role::ViewChanging(changing) => Ok(changing.begin_sync()?),
            Role::Installing(_) => Err(ReplicationError::WrongRole.into()),
        }
    }

    /// Observe a completed barrier. Commit and persistence deadlines advance
    /// independently; a late old-view completion only settles the frozen log.
    pub fn complete_sync(&mut self, ticket: SyncTicket, now: Duration) -> Result<(), DriverError> {
        self.observe_time(now)?;
        match self.role.as_mut().expect("owned role") {
            Role::Normal(normal) => {
                let previous = normal.snapshot();
                normal.complete_sync(ticket)?;
                self.observe_normal_progress(&previous, now);
            }
            Role::ViewChanging(changing) => changing.complete_sync(ticket)?,
            Role::Installing(_) => return Err(ReplicationError::WrongRole.into()),
        }
        Ok(())
    }

    /// Select after collecting reports, using only nonblocking pinned-history lookup.
    /// Missing history/quorum is recoverable. Success is not installed authority.
    pub fn select(
        &mut self,
        lookup: impl FnMut(LogSource, OpNumber) -> Option<Digest>,
    ) -> Result<SelectedView, DriverError> {
        Ok(self.changing()?.select(lookup)?)
    }

    fn changing(&mut self) -> Result<&mut ViewChange, DriverError> {
        match self.role.as_mut().expect("owned role") {
            Role::ViewChanging(changing) => Ok(changing),
            Role::Normal(_) | Role::Installing(_) => Err(ReplicationError::WrongRole.into()),
        }
    }

    fn observe_time(&mut self, now: Duration) -> Result<(), DriverError> {
        if now < self.now {
            return Err(DriverError::ClockRegressed);
        }
        self.timing.validate(now)?;
        self.now = now;
        Ok(())
    }

    fn progress_timeout(&self) -> Duration {
        if self
            .normal()
            .is_some_and(|normal| normal.snapshot().ready_for_appends)
        {
            self.timing.primary_timeout
        } else {
            self.election_delay
        }
    }

    fn observe_normal_progress(&mut self, previous: &ReplicaSnapshot, now: Duration) {
        let current = self.normal().expect("normal completion").snapshot();
        if current.committed != previous.committed {
            self.progress_deadline = now + self.progress_timeout();
        }
        if self.storage_progress(&current) > self.storage_progress(previous) {
            self.persistence_deadline = now + self.timing.primary_timeout;
        }
    }

    fn normal_timed_out(&self, snapshot: &ReplicaSnapshot) -> bool {
        let primary = self.configuration.primary(snapshot.scope.view) == self.local;
        let outstanding = snapshot.accepted != snapshot.committed || !snapshot.ready_for_appends;
        (!primary && self.now >= self.contact_deadline)
            || (outstanding && self.now >= self.progress_deadline)
            || (snapshot.accepted.op > self.storage_progress(snapshot)
                && self.now >= self.persistence_deadline)
    }

    fn storage_progress(&self, snapshot: &ReplicaSnapshot) -> OpNumber {
        match self.configuration.policy() {
            crate::QuorumPolicy::Durable => snapshot.journal.durable,
            crate::QuorumPolicy::Replicated => snapshot.journal.written,
        }
    }

    fn next_view(&mut self) -> Result<(), DriverError> {
        let view = self
            .scope()
            .view
            .checked_add(1)
            .ok_or(ViewChangeError::ViewExhausted)?;
        self.change_view(view)
    }

    fn change_view(&mut self, view: u64) -> Result<(), DriverError> {
        if let Some(Role::ViewChanging(changing)) = self.role.as_mut() {
            changing.advance_view(view)?;
        } else if let Some(Role::Installing(installing)) = self.role.as_mut() {
            installing.request_view(view)?;
        } else {
            if self.normal().expect("normal role").snapshot().status == Status::Faulted {
                return Err(ViewChangeError::Faulted.into());
            }
            let Role::Normal(normal) = self.role.take().expect("owned role") else {
                unreachable!("normal branch");
            };
            self.role = Some(Role::ViewChanging(
                normal
                    .into_view_change(view)
                    .expect("healthy role and higher view"),
            ));
        }
        self.exit_votes = 0;
        self.exit_retry_at = None;
        self.election_deadline = self.now + self.election_delay;
        self.retry_at = self.now + self.timing.retransmit;
        self.start_pending = true;
        self.report_pending = true;
        self.commit_pending = false;
        self.activation_pending = 0;
        Ok(())
    }
}

/// Rejected scheduler event. No clock or packet can bypass core authority checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DriverError {
    /// Intervals must be positive and retries shorter than their failure deadlines.
    #[error("invalid replica timing intervals")]
    InvalidTiming,
    /// The adapter's process-relative monotonic clock moved backward.
    #[error("replica clock regressed")]
    ClockRegressed,
    /// A deadline cannot be represented without wrapping.
    #[error("replica clock exhausted")]
    TimeExhausted,
    /// Worker-side validation used a different voter, writer, role, or application prefix.
    #[error("stale replica application validation")]
    StaleValidation,
    /// Normal membership, lineage, status, or journal rejection.
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    /// View-change promise, selection, or storage rejection.
    #[error(transparent)]
    ViewChange(#[from] ViewChangeError),
}
