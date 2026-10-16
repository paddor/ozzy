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

mod deadline;
mod exit_view;
mod installation;
mod poll;
mod receive;
mod storage;
mod validation;

pub use installation::ActivationTicket;
pub use validation::ValidationTicket;

use std::time::Duration;

use ozzy_proto::NodeId;

use crate::wire::Control;
use crate::{
    Configuration, Digest, InstallingView, LogSource, NormalReplica, OpNumber, PromiseTicket,
    ReplicaSnapshot, ReplicationError, Scope, SelectedView, Status, ViewChange, ViewChangeError,
    VoterIndex, VoterSet, Voters,
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
    /// A consuming transition failed and took the previous phase with it.
    /// Every event fails closed; only restart recovery resumes this voter.
    Fenced(Scope),
}

/// Timer and election-message driver for one fixed three-voter group.
///
/// No clocks, allocations, I/O, or automatic durability assertions on transitions.
/// Old writes must settle before a promise; a pending promise survives later views.
#[derive(Debug)]
pub struct ReplicaDriver {
    role: Role,
    configuration: Configuration,
    local: VoterIndex,
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
    activation_pending: VoterSet,
    /// Current-view timeout suspicions. Never durable or history-selection votes.
    exit_votes: VoterSet,
    /// `None` means local suspicion is inactive or was withdrawn after progress.
    exit_retry_at: Option<Duration>,
    /// An older view peers still ask to leave, and which voters asked. This
    /// voter already left it.
    exit_echo: Option<(Scope, VoterSet)>,
    /// Earliest next answer to each voter's request to leave an older view.
    exit_echo_at: Voters<Duration>,
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
        let local = changing.local;
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
        local: VoterIndex,
        now: Duration,
        timing: Timing,
    ) -> Result<Self, DriverError> {
        timing.validate(now)?;
        let changing = matches!(role, Role::ViewChanging(_));
        let activating =
            matches!(&role, Role::Normal(normal) if !normal.snapshot().ready_for_appends);
        Ok(Self {
            role,
            configuration,
            local,
            timing,
            now,
            contact_deadline: now.saturating_add(timing.primary_timeout),
            persistence_deadline: now.saturating_add(timing.primary_timeout),
            progress_deadline: now
                + if activating {
                    timing.election_timeout
                } else {
                    timing.primary_timeout
                },
            election_deadline: now.saturating_add(timing.election_timeout),
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
            activation_pending: VoterSet::EMPTY,
            exit_votes: VoterSet::EMPTY,
            exit_retry_at: None,
            exit_echo: None,
            exit_echo_at: Voters::splat(Duration::ZERO),
        })
    }

    /// Inspect normal evidence. No mutable access can bypass scheduling/fencing.
    pub fn normal(&self) -> Option<&NormalReplica> {
        match &self.role {
            Role::Normal(normal) => Some(normal),
            Role::ViewChanging(_) | Role::Installing(_) | Role::Fenced(_) => None,
        }
    }

    /// A fenced old view needs an actual barrier before publishing its promise.
    pub fn election_needs_sync(&self) -> bool {
        let Role::ViewChanging(changing) = &self.role else {
            return false;
        };
        let journal = changing.normal_snapshot().journal;
        journal.written == journal.accepted && journal.written > journal.durable
    }

    /// Current normal or proposed election scope, not necessarily durable.
    pub fn scope(&self) -> Scope {
        match &self.role {
            Role::Normal(normal) => normal.snapshot().scope,
            Role::ViewChanging(changing) => changing.scope(),
            Role::Installing(installing) => installing.requested_scope(),
            Role::Fenced(scope) => *scope,
        }
    }

    /// Extract phase ownership. Pending I/O still must settle; a replacement driver
    /// must preserve newer-view fencing rather than resume an old normal role.
    pub fn into_role(self) -> Role {
        self.role
    }

    /// Select after collecting reports, using only nonblocking pinned-history lookup.
    /// Missing history/quorum is recoverable. Success is not installed authority.
    pub fn select(
        &mut self,
        lookup: impl FnMut(LogSource, OpNumber) -> Option<Digest>,
    ) -> Result<SelectedView, DriverError> {
        Ok(self.changing()?.select(lookup)?)
    }

    /// Frozen election sources; their bytes still require exact-tail verification.
    pub fn history_sources(&self) -> [Option<LogSource>; 3] {
        match &self.role {
            Role::ViewChanging(changing) => changing.history_sources(),
            Role::Normal(_) | Role::Installing(_) | Role::Fenced(_) => [None; 3],
        }
    }

    fn changing(&mut self) -> Result<&mut ViewChange, DriverError> {
        match &mut self.role {
            Role::ViewChanging(changing) => Ok(changing),
            Role::Normal(_) | Role::Installing(_) => Err(ReplicationError::WrongRole.into()),
            Role::Fenced(_) => Err(ViewChangeError::Faulted.into()),
        }
    }

    fn require_normal(&self) -> Result<&NormalReplica, DriverError> {
        Ok(self.normal().ok_or(ReplicationError::NotNormal)?)
    }

    /// Reject events once storage, lineage, or a failed transition faulted this voter.
    fn require_healthy(&self) -> Result<(), DriverError> {
        match &self.role {
            Role::Normal(normal) if normal.snapshot().status == Status::Faulted => {
                Err(ViewChangeError::Faulted.into())
            }
            Role::ViewChanging(changing)
                if changing.normal_snapshot().status == Status::Faulted =>
            {
                Err(ViewChangeError::Faulted.into())
            }
            Role::Installing(installing) => Ok(installing.require_pending()?),
            Role::Fenced(_) => Err(ViewChangeError::Faulted.into()),
            Role::Normal(_) | Role::ViewChanging(_) => Ok(()),
        }
    }

    /// Take the role for a consuming transition. The driver stays fenced until
    /// the caller installs the next role; a failed transition leaves it fenced.
    fn take_role(&mut self) -> Role {
        let fenced = Role::Fenced(self.scope());
        std::mem::replace(&mut self.role, fenced)
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

    fn observe_normal_progress(
        &mut self,
        previous: &ReplicaSnapshot,
        current: &ReplicaSnapshot,
        now: Duration,
    ) {
        if current.committed != previous.committed {
            self.progress_deadline = now.saturating_add(self.progress_timeout());
        }
        if current.journal.durable > previous.journal.durable {
            self.persistence_deadline = now.saturating_add(self.timing.primary_timeout);
        }
    }

    fn normal_timed_out(&self, snapshot: &ReplicaSnapshot) -> bool {
        let primary = VoterIndex::primary(snapshot.scope.view) == self.local;
        let outstanding = snapshot.accepted != snapshot.committed || !snapshot.ready_for_appends;
        (!primary && self.now >= self.contact_deadline)
            || (outstanding && self.now >= self.progress_deadline)
            || (snapshot.accepted.op > snapshot.journal.durable
                && self.now >= self.persistence_deadline)
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
        match &mut self.role {
            Role::ViewChanging(changing) => changing.advance_view(view)?,
            Role::Installing(installing) => installing.request_view(view)?,
            Role::Normal(normal) if normal.snapshot().status == Status::Faulted => {
                return Err(ViewChangeError::Faulted.into());
            }
            Role::Normal(_) => {
                let Role::Normal(normal) = self.take_role() else {
                    return Err(ViewChangeError::Faulted.into());
                };
                self.role = Role::ViewChanging(normal.into_view_change(view)?);
            }
            Role::Fenced(_) => return Err(ViewChangeError::Faulted.into()),
        }
        self.exit_votes = VoterSet::EMPTY;
        self.exit_retry_at = None;
        self.election_deadline = self.now.saturating_add(self.election_delay);
        self.retry_at = self.now.saturating_add(self.timing.retransmit);
        self.start_pending = true;
        self.report_pending = true;
        self.commit_pending = false;
        self.activation_pending = VoterSet::EMPTY;
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
