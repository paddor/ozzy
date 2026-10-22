//! Durable view-change initiation and selection. Installation is a separate gate.

mod recovery;
mod selection;

use ozzy_proto::NodeId;

use crate::{
    Configuration, Digest, JournalGeneration, NormalReplica, OpNumber, Prefix, ReplicaSnapshot,
    ReplicationError, Scope, Status, SyncTicket, VoterIndex, VoterSet, Voters, WriteTicket,
};

/// One immutable, synchronized log description for a view-change report.
///
/// The adapter must pin the corresponding canonical history until the report is
/// invalidated. Retired predecessors require independently verified overlapping
/// lineage or checkpoint evidence from the adapter; the core performs no I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrozenLog {
    /// Last durably installed normal view, not the highest entry's original view.
    pub last_normal_view: u64,
    /// Complete synchronized accepted prefix, including uncertain operations.
    pub accepted: Prefix,
    /// Known committed prefix that any selected history must preserve.
    pub committed: Prefix,
}

/// Intact durable state independently validated and synchronized on startup.
///
/// This descriptor is an adapter assertion, not proof of disk integrity. The
/// caller must verify immutable voter/configuration identity, the complete
/// selected canonical lineage and commit floor, and durable hard state. Empty,
/// rolled-back, lost, or ambiguously corrupt stores must not use this path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveredState {
    /// Exact durable configuration and highest promised view, not last normal view.
    pub scope: Scope,
    /// Recovered accepted bytes and known commit floor. No ACK sets are restored.
    pub log: FrozenLog,
}

/// Storage action that persists a view promise without installing a normal view.
///
/// Fields are private so completions can identify only actions this core issued.
/// The adapter atomically persists this exact log and promised view, retaining
/// the prior last-normal-view. Submission or an ordinary file flush is not a
/// successful completion. Storage failure must call `ViewChange::fail_promise`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromiseTicket {
    scope: Scope,
    generation: JournalGeneration,
    log: FrozenLog,
}

impl PromiseTicket {
    /// Exact group/configuration and new promised view to persist.
    pub const fn scope(self) -> Scope {
        self.scope
    }
    /// Unique writer incarnation whose frozen history the promise covers.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }
    /// Log positions and prior normal view that must survive publication.
    pub const fn log(self) -> FrozenLog {
        self.log
    }
}

/// First view-change phase. Sender identity comes from the authenticated peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartViewChange {
    /// Exact proposed group/configuration/view.
    pub scope: Scope,
}

/// Second view-change phase. A bounded descriptor, never an unbounded log frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DoViewChange {
    /// Exact proposed group/configuration/view.
    pub scope: Scope,
    /// Frozen source incarnation; transferred chunks must not mix generations.
    pub generation: JournalGeneration,
    /// Immutable synchronized history and protected commit floor.
    pub log: FrozenLog,
}

/// Exact source against which transferred history must be verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogSource {
    /// Configured voter supplying the selected bytes.
    pub voter: NodeId,
    /// Frozen source writer incarnation from its report.
    pub generation: JournalGeneration,
    /// Tail hash anchoring the complete selected canonical lineage.
    pub accepted: Prefix,
}

/// Verified view-change selection, not installed leadership authority.
///
/// A future installation adapter must durably install these bytes and hard state
/// before `START_VIEW` or normal voting. This value cannot construct a normal core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedView {
    scope: Scope,
    source: LogSource,
    committed: Prefix,
    retained: Prefix,
    voters: VoterSet,
}

impl SelectedView {
    /// New view whose quorum selected this lineage.
    pub const fn scope(self) -> Scope {
        self.scope
    }
    /// Frozen history to install. Uncertain accepted operations remain included.
    pub const fn source(self) -> LogSource {
        self.source
    }
    /// Largest compatible known commit floor in the participating reports.
    pub const fn committed(self) -> Prefix {
        self.committed
    }
    /// This voter's longest prefix that selection verified in the source,
    /// at most `committed`.
    pub(crate) const fn retained(self) -> Prefix {
        self.retained
    }
    /// Bitset of participating voters in configured order, including the primary.
    pub const fn voter_mask(self) -> u8 {
        self.voters.mask()
    }
}

/// Fixed-three-voter durable view-change initiation and log selection.
///
/// Owns the fenced former normal core. No normal-message entry point is exposed.
/// Retains at most three reports and one pending metadata action. It has no I/O,
/// timers, or allocations on live election transitions. Intact restart allocates
/// only the bounded live pipeline and stays above its last installed normal view. Installation
/// consumes this role and uses a separate completion/activation gate.
#[derive(Debug)]
pub struct ViewChange {
    pub(crate) normal: NormalReplica,
    pub(crate) configuration: Configuration,
    pub(crate) local: VoterIndex,
    view: u64,
    promise: Promise,
    pending: Option<PromiseTicket>,
    starts: VoterSet,
    reports: Voters<Option<DoViewChange>>,
    pub(crate) selected: Option<SelectedView>,
}

/// Highest view whose durable promise this core has observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Promise {
    /// Promised before this core began: the installed view, or recovered
    /// metadata without a promise for the requested view.
    Prior(u64),
    /// Completed here, carried across an abandoned installation, or restored
    /// from durable restart evidence.
    Completed(PromiseTicket),
}

impl Promise {
    const fn view(self) -> u64 {
        match self {
            Self::Prior(view) => view,
            Self::Completed(ticket) => ticket.scope.view,
        }
    }
}

impl ViewChange {
    /// Frozen reported sources available for bounded, externally verified history repair.
    /// A report alone supplies no ancestry or installation authority.
    pub fn history_sources(&self) -> [Option<LogSource>; 3] {
        VoterIndex::ALL.map(|voter| {
            self.reports[voter].map(|report| LogSource {
                voter: self.configuration.voter(voter),
                generation: report.generation,
                accepted: report.log.accepted,
            })
        })
    }

    /// Operations whose prefixes this election may look up in its chosen
    /// source: each report's commit and accept, and this voter's own.
    pub fn lookup_ops(&self) -> [Option<OpNumber>; 8] {
        let local = self.normal.snapshot();
        let mut ops = [None; 8];
        ops[6] = Some(local.committed.op);
        ops[7] = Some(local.accepted.op);
        let (slots, _) = ops.as_chunks_mut::<2>();
        for ([committed, accepted], report) in slots.iter_mut().zip(self.reports.values()) {
            if let Some(report) = report {
                *committed = Some(report.log.committed.op);
                *accepted = Some(report.log.accepted.op);
            }
        }
        ops
    }

    pub(crate) fn begin(
        mut normal: NormalReplica,
        configuration: Configuration,
        local: VoterIndex,
        view: u64,
    ) -> Result<Self, ViewChangeError> {
        normal.fence();
        if normal.snapshot().status == Status::Faulted {
            return Err(ViewChangeError::Faulted);
        }
        if view <= configuration.scope().view {
            return Err(ViewChangeError::ViewNotHigher);
        }
        Ok(Self {
            normal,
            configuration,
            local,
            view,
            promise: Promise::Prior(configuration.scope().view),
            pending: None,
            starts: VoterSet::EMPTY,
            reports: Voters::splat(None),
            selected: None,
        })
    }

    /// Read-only old-role journal/application evidence for storage scheduling.
    pub fn normal_snapshot(&self) -> ReplicaSnapshot {
        self.normal.snapshot()
    }

    /// Requested view; it may not yet have a completed durable promise.
    pub fn scope(&self) -> Scope {
        Scope {
            view: self.view,
            ..self.configuration.scope()
        }
    }

    /// Highest view whose promise completion this core has observed.
    pub const fn promised_view(&self) -> u64 {
        self.promise.view()
    }

    /// Move toward a later view without discarding an outstanding disk action.
    ///
    /// Old reports/selection immediately lose authority. A pending earlier
    /// promise must settle before the next can be issued. Its late completion
    /// never authorizes messages for either the old or the unpersisted new view.
    pub fn advance_view(&mut self, view: u64) -> Result<(), ViewChangeError> {
        self.require_active()?;
        if view <= self.view {
            return Err(ViewChangeError::ViewNotHigher);
        }
        self.view = view;
        self.starts = VoterSet::EMPTY;
        self.reports = Voters::splat(None);
        self.selected = None;
        Ok(())
    }

    /// Settle a previously admitted complete write without reviving old votes.
    pub fn complete_write(&mut self, ticket: WriteTicket) -> Result<(), ViewChangeError> {
        Ok(self.normal.complete_write(ticket)?)
    }

    /// Capture the written prefix for a real barrier on the storage worker.
    pub fn begin_sync(&self) -> Result<SyncTicket, ViewChangeError> {
        Ok(self.normal.begin_sync()?)
    }

    /// Settle a real disk barrier. It cannot advance old-view commit.
    pub fn complete_sync(&mut self, ticket: SyncTicket) -> Result<(), ViewChangeError> {
        Ok(self.normal.complete_sync(ticket)?)
    }

    /// Report uncertain journal I/O and permanently fault this transition.
    pub fn fail_io(&mut self, generation: JournalGeneration) -> Result<(), ViewChangeError> {
        Ok(self.normal.fail_io(generation)?)
    }

    /// Issue one metadata promise after every accepted operation is synchronized.
    ///
    /// Finishing all old writes makes the report immutable, including operations
    /// another replica may already have acknowledged before local I/O finished.
    pub fn begin_promise(&mut self) -> Result<PromiseTicket, ViewChangeError> {
        self.require_active()?;
        if self.pending.is_some() {
            return Err(ViewChangeError::PromisePending);
        }
        if self.promise.view() == self.view {
            return Err(ViewChangeError::AlreadyPromised);
        }
        let snapshot = self.normal.snapshot();
        if snapshot.journal.durable != snapshot.accepted.op {
            return Err(ViewChangeError::StoragePending);
        }
        let ticket = PromiseTicket {
            scope: self.scope(),
            generation: snapshot.journal.generation,
            log: FrozenLog {
                last_normal_view: snapshot.scope.view,
                accepted: snapshot.accepted,
                committed: snapshot.committed,
            },
        };
        self.pending = Some(ticket);
        Ok(ticket)
    }

    /// Observe successful durable publication of the exact issued promise.
    pub fn complete_promise(&mut self, ticket: PromiseTicket) -> Result<(), ViewChangeError> {
        self.require_active()?;
        if self.promise == Promise::Completed(ticket) {
            return Ok(());
        }
        self.require_ticket(ticket)?;
        self.pending = None;
        self.promise = Promise::Completed(ticket);
        if ticket.scope.view == self.view {
            self.starts.insert(self.local);
        }
        Ok(())
    }

    /// An ambiguous promise failure fences all election messages and selection.
    pub fn fail_promise(&mut self, ticket: PromiseTicket) -> Result<(), ViewChangeError> {
        self.require_active()?;
        self.require_ticket(ticket)?;
        self.fail_io(ticket.generation)
    }

    /// Broadcast only after the requested view promise is durably complete.
    pub fn start_message(&self) -> Result<StartViewChange, ViewChangeError> {
        self.require_promised()?;
        Ok(StartViewChange {
            scope: self.scope(),
        })
    }

    /// Collect an authenticated other voter's first-phase message.
    /// Duplicate messages cannot create extra voters or replace local persistence.
    pub fn receive_start(
        &mut self,
        from: NodeId,
        message: StartViewChange,
    ) -> Result<(), ViewChangeError> {
        self.require_scope(message.scope)?;
        let voter = self.configuration.voter_index(from)?;
        if voter == self.local {
            return Err(ReplicationError::WrongRole.into());
        }
        self.starts.insert(voter);
        Ok(())
    }

    /// Freeze the local second-phase report after a first-phase majority.
    /// Send it to the configured primary; the primary retains its own report too.
    pub fn report(&mut self) -> Result<DoViewChange, ViewChangeError> {
        let promise = self.require_promised()?;
        if self.starts.len() < 2 {
            return Err(ViewChangeError::StartQuorumMissing);
        }
        let report = DoViewChange {
            scope: self.scope(),
            generation: promise.generation,
            log: promise.log,
        };
        self.reports[self.local] = Some(report);
        Ok(report)
    }

    /// Primary collects immutable, authenticated second-phase reports.
    ///
    /// Later views are explicit `advance_view` events. A new report cannot alter
    /// an already selected quorum; changed frozen logs from one voter are faults.
    /// Intact restart may repeat the identical log with a fresh writer generation.
    /// Keep the first source descriptor unchanged, including after selection:
    /// this is one vote, not permission to mix or redirect history chunks.
    pub fn receive_report(
        &mut self,
        from: NodeId,
        report: DoViewChange,
    ) -> Result<(), ViewChangeError> {
        self.require_scope(report.scope)?;
        self.require_primary()?;
        let voter = self.configuration.voter_index(from)?;
        if voter == self.local {
            return Err(ReplicationError::WrongRole.into());
        }
        validate_report(report)?;
        if let Some(previous) = self.reports[voter] {
            if previous.log != report.log {
                return Err(self.conflict());
            }
            // An unfinished durable promise survives restart, but its process's
            // writer incarnation does not. Equivalent history is not a fault.
            // A vanished first source may require another view; never replace a
            // pinned or selected source merely because a different ID arrived.
            return Ok(());
        }
        if self.selected.is_some() {
            return Err(ViewChangeError::SelectionFrozen);
        }
        self.reports[voter] = Some(report);
        Ok(())
    }

    fn require_active(&self) -> Result<(), ViewChangeError> {
        if self.normal.snapshot().status == Status::Faulted {
            return Err(ViewChangeError::Faulted);
        }
        Ok(())
    }

    pub(crate) fn require_scope(&self, scope: Scope) -> Result<(), ViewChangeError> {
        self.require_active()?;
        if scope != self.scope() {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        Ok(())
    }

    pub(crate) fn require_primary(&self) -> Result<(), ViewChangeError> {
        if VoterIndex::primary(self.view) != self.local {
            return Err(ReplicationError::WrongRole.into());
        }
        Ok(())
    }

    /// The completed promise for the requested view.
    pub(crate) fn require_promised(&self) -> Result<PromiseTicket, ViewChangeError> {
        self.require_active()?;
        match self.promise {
            Promise::Completed(ticket) if ticket.scope.view == self.view => Ok(ticket),
            Promise::Prior(_) | Promise::Completed(_) => Err(ViewChangeError::PromiseRequired),
        }
    }

    fn require_ticket(&self, ticket: PromiseTicket) -> Result<(), ViewChangeError> {
        if self.pending != Some(ticket) {
            return Err(ViewChangeError::StalePromise);
        }
        Ok(())
    }

    pub(crate) fn conflict(&mut self) -> ViewChangeError {
        self.normal.fault();
        ViewChangeError::ConflictingHistory
    }
}

pub(crate) fn valid_prefix(prefix: Prefix) -> bool {
    prefix.op.0 != u64::MAX && ((prefix.op.0 == 0) == (prefix.digest == Digest::ZERO))
}

fn validate_report(report: DoViewChange) -> Result<(), ViewChangeError> {
    let log = report.log;
    if log.last_normal_view >= report.scope.view
        || !valid_prefix(log.accepted)
        || !valid_prefix(log.committed)
        || log.committed.op > log.accepted.op
        || (log.committed.op == log.accepted.op && log.committed != log.accepted)
    {
        return Err(ViewChangeError::InvalidReport);
    }
    Ok(())
}

/// Rejected election event. Missing history/quorums wait; conflicts fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ViewChangeError {
    /// Existing journal/normal scope, role, or membership check failed.
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    /// Timers/messages cannot regress or reuse the target view.
    #[error("view must increase")]
    ViewNotHigher,
    /// A restart cannot safely wrap its highest durable view back to zero.
    #[error("durable view number exhausted")]
    ViewExhausted,
    /// All accepted old-view operations must finish synchronization first.
    #[error("old-view journal work has not settled")]
    StoragePending,
    /// Only one metadata promise action can be in flight.
    #[error("a view promise is already pending")]
    PromisePending,
    /// No new metadata write is necessary for this requested view.
    #[error("requested view already promised")]
    AlreadyPromised,
    /// Completion is not the exact outstanding metadata action.
    #[error("stale or foreign view-promise completion")]
    StalePromise,
    /// No election message before successful durable promise completion.
    #[error("requested view promise has not completed")]
    PromiseRequired,
    /// Need own durable promise plus another distinct first-phase voter.
    #[error("start-view-change quorum missing")]
    StartQuorumMissing,
    /// Selection needs the primary's own report and another distinct voter.
    #[error("do-view-change quorum missing")]
    ReportQuorumMissing,
    /// Fetch and validate the frozen selected history before selecting it.
    #[error("selected lineage evidence missing")]
    HistoryMissing,
    /// Malformed positions or last-normal-view cannot become evidence.
    #[error("invalid frozen view-change report")]
    InvalidReport,
    /// A protected prefix or immutable report conflicts with this lineage.
    #[error("conflicting view-change history")]
    ConflictingHistory,
    /// Do not change the selected quorum while installation may be in flight.
    #[error("view selection already frozen")]
    SelectionFrozen,
    /// Primary cannot install without its completed quorum selection.
    #[error("view has not been selected")]
    SelectionRequired,
    /// A replacement writer needs a fresh, globally unique incarnation.
    #[error("installation reused the old writer generation")]
    ReusedGeneration,
    /// Completion does not identify this exact pending installation.
    #[error("stale or foreign installation completion")]
    StaleInstallation,
    /// Successful completion has already transferred ownership to a new role.
    #[error("installation already completed")]
    InstallationCompleted,
    /// Committed canonical state must be rebuilt before role activation.
    #[error("installed application state is not at the required committed prefix")]
    ApplicationPending,
    /// Storage or lineage fault requires recovery, not another local election.
    #[error("view change faulted")]
    Faulted,
}
