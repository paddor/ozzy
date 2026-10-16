mod admission;
mod commit;
mod installation;

use std::collections::VecDeque;

use ozzy_journal::progress::{JournalProgress, JournalSnapshot};
use ozzy_proto::NodeId;

use crate::{
    Configuration, JournalGeneration, OpNumber, PipelineLimits, Prefix, PreparedOperation,
    QuorumPolicy, ReplicationError, Scope, SyncTicket, VoterIndex, Voters, WriteTicket,
};

/// Whether the normal protocol may emit votes, commit, or apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Exact installed view permits normal operation.
    Normal,
    /// Old-view decisions stopped; outstanding disk work may still settle.
    Fenced,
    /// Conflicting lineage or uncertain storage requires recovery/quarantine.
    Faulted,
}

/// Result of atomically validating a complete prepare group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Every operation was already accepted with identical metadata.
    Duplicate,
    /// Persist only this newly admitted suffix of the submitted operation slice.
    Write {
        /// Generation-scoped exact write range, not durability evidence.
        ticket: WriteTicket,
        /// Index of the first new operation in the supplied slice.
        first_new: usize,
    },
}

/// Read-only normal state, suitable for adapter scheduling and invariant checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaSnapshot {
    /// Current group/configuration/view identity.
    pub scope: Scope,
    /// Whether normal protocol actions are still allowed.
    pub status: Status,
    /// Independent local write and synchronization evidence.
    pub journal: JournalSnapshot,
    /// Highest validated, contiguous canonical operation.
    pub accepted: Prefix,
    /// Highest contiguous quorum-committed operation under the configured policy.
    pub committed: Prefix,
    /// Highest operation reported applied by the deterministic state adapter.
    pub applied: Prefix,
    /// Retained live descriptors awaiting application or local persistence.
    /// Excludes a disk-backed `journal_tail`.
    pub pending_operations: usize,
    /// Live canonical body bytes reserved in the external payload adapter.
    pub pending_body_bytes: usize,
    /// Operations above this number occupy the primary's live window: the
    /// commit it announced or the released prefix, whichever is later.
    pub live_base: OpNumber,
    /// Descriptor and body capacity this role retains beyond its released prefix.
    pub retained_limits: PipelineLimits,
    /// Installed/recovered suffix retained only in the journal, not the live
    /// descriptor ring. Fresh work waits until this entire tail is applied.
    pub journal_tail: Option<Prefix>,
    /// Primary only: the highest operation each backup acknowledged.
    pub backups: Option<[(NodeId, OpNumber); 2]>,
    /// Highest commit the primary announced, as this replica knows it.
    pub known_commit: OpNumber,
    /// Whether new operations may be admitted. Duplicates can still be retried
    /// while an installed primary reestablishes its selected-tail quorum. Entries
    /// inside a disk-backed tail require journal lookup, not descriptor retries.
    pub ready_for_appends: bool,
}

/// Fixed-three-voter normal VSR path with a bounded pipelined prepare window.
///
/// This is one component of VSR, not a complete election or recovery protocol.
/// The caller authenticates sender IDs and validates canonical operations plus
/// application transitions before admission. Returned write tickets schedule
/// disk work; the primary may concurrently send the same prepares to backups.
/// Producer success additionally requires ordered application of committed ops.
#[derive(Debug)]
pub struct NormalReplica {
    configuration: Configuration,
    local: VoterIndex,
    status: Status,
    journal: JournalProgress,
    limits: PipelineLimits,
    operations: VecDeque<PreparedOperation>,
    body_bytes: usize,
    accepted: Prefix,
    committed: Prefix,
    applied: Prefix,
    // Descriptors start after the applied prefix stored under the group policy.
    retained_base: Prefix,
    known_commit: OpNumber,
    acknowledged: Voters<OpNumber>,
    activation_tail: OpNumber,
    installed_view: Option<crate::StartView>,
    journal_tail: Option<Prefix>,
}

impl NormalReplica {
    /// Exact prefix released from the live window after application and persistence.
    pub const fn reclaimed_prefix(&self) -> Prefix {
        self.retained_base
    }
    pub(crate) const fn driver_identity(&self) -> (Configuration, VoterIndex) {
        (self.configuration, self.local)
    }

    /// Consume normal-role ownership and immediately fence its decisions.
    ///
    /// The new view must exceed the installed view. Outstanding journal work
    /// moves into the view-change core and must settle before a view promise.
    /// This transition cannot resume the old normal role, even on failure.
    pub fn into_view_change(self, view: u64) -> Result<crate::ViewChange, crate::ViewChangeError> {
        let configuration = self.configuration;
        let local = self.local;
        crate::ViewChange::begin(self, configuration, local, view)
    }

    /// Start a newly formatted, empty group at view zero.
    ///
    /// All three durable stores must have been explicitly initialized with this
    /// exact configuration. Never use this constructor on restart, lost storage,
    /// or a previously used voter identity. It cannot recover existing history.
    /// `generation` must be unique across all writer instances/completions.
    pub fn bootstrap(
        configuration: Configuration,
        local: NodeId,
        generation: JournalGeneration,
        limits: PipelineLimits,
    ) -> Result<Self, ReplicationError> {
        let local = configuration.voter_index(local)?;
        if limits.max_operations == 0
            || limits.max_body_bytes == 0
            || u64::try_from(limits.max_operations).is_err()
        {
            return Err(ReplicationError::InvalidLimits);
        }
        // A later installed view can change the role; the ring then grows once.
        let retained =
            if configuration.primary(configuration.scope.view) == configuration.voter(local) {
                limits
            } else {
                limits.backup()
            };
        let mut operations = VecDeque::new();
        operations
            .try_reserve_exact(retained.max_operations)
            .map_err(|_| ReplicationError::Capacity)?;
        Ok(Self {
            configuration,
            local,
            status: Status::Normal,
            journal: JournalProgress::recover(generation, OpNumber(0)),
            limits,
            operations,
            body_bytes: 0,
            accepted: Prefix::GENESIS,
            committed: Prefix::GENESIS,
            applied: Prefix::GENESIS,
            retained_base: Prefix::GENESIS,
            known_commit: OpNumber(0),
            acknowledged: Voters::splat(OpNumber(0)),
            activation_tail: OpNumber(0),
            installed_view: None,
            journal_tail: None,
        })
    }

    /// Reconstruct only fenced disk evidence, not application or normal authority.
    /// Complete retained history stays in the journal, outside the live pipeline.
    pub(crate) fn recover_fenced(
        mut configuration: Configuration,
        local: NodeId,
        generation: JournalGeneration,
        log: crate::FrozenLog,
        limits: PipelineLimits,
    ) -> Result<Self, ReplicationError> {
        configuration.scope.view = log.last_normal_view;
        let mut normal = Self::bootstrap(configuration, local, generation, limits)?;
        normal.restore_fenced(generation, log);
        Ok(normal)
    }

    /// Reuse the bounded ring after storage confirms unpublished staging was aborted.
    /// Restore only the old durable history. Never restore votes or application authority.
    pub(crate) fn restore_fenced(&mut self, generation: JournalGeneration, log: crate::FrozenLog) {
        self.configuration.scope.view = log.last_normal_view;
        self.status = Status::Fenced;
        self.journal = JournalProgress::recover(generation, log.accepted.op);
        self.operations.clear();
        self.body_bytes = 0;
        self.accepted = log.accepted;
        self.committed = log.committed;
        self.applied = Prefix::GENESIS;
        self.retained_base = Prefix::GENESIS;
        self.known_commit = log.committed.op;
        self.acknowledged = Voters::splat(OpNumber(0));
        self.activation_tail = log.accepted.op;
        self.installed_view = None;
        self.journal_tail = (log.accepted != Prefix::GENESIS).then_some(log.accepted);
    }

    /// Inspect state without obtaining mutable journal or quorum access.
    pub fn snapshot(&self) -> ReplicaSnapshot {
        ReplicaSnapshot {
            scope: self.configuration.scope(),
            status: self.status,
            journal: self.journal.snapshot(),
            accepted: self.accepted,
            committed: self.committed,
            applied: self.applied,
            pending_operations: self.operations.len(),
            pending_body_bytes: self.body_bytes,
            live_base: self.live_base(),
            retained_limits: self.retained_limits(),
            journal_tail: self.journal_tail,
            backups: self.is_primary().then(|| {
                self.local
                    .others()
                    .map(|voter| (self.configuration.voter(voter), self.acknowledged[voter]))
            }),
            known_commit: self.known_commit,
            ready_for_appends: self.ready_for_appends(),
        }
    }

    /// Operations above this number occupy the primary's live window.
    fn live_base(&self) -> OpNumber {
        self.known_commit.max(self.retained_base.op)
    }

    /// Descriptor and body capacity retained beyond the released prefix.
    ///
    /// The primary keeps one window in flight beyond its released prefix. A
    /// backup releases only what the primary has announced committed, and its
    /// own persistence can trail that announcement by a whole window while the
    /// other backup confirms the primary's writes, so it retains two.
    fn retained_limits(&self) -> PipelineLimits {
        if self.is_primary() {
            self.limits
        } else {
            self.limits.backup()
        }
    }

    /// Observe a complete local write. Page-cache completion casts no vote.
    pub fn complete_write(&mut self, ticket: WriteTicket) -> Result<(), ReplicationError> {
        self.journal.complete_write(ticket)?;
        self.reclaim()
    }

    /// Local prefix available for replay and releasing applied live capacity.
    /// RAM-confirmed groups may use completed page-cache writes; disk groups
    /// require synchronized writes. Neither boundary alone establishes a quorum.
    pub fn stored_through(&self) -> OpNumber {
        match self.configuration.policy() {
            QuorumPolicy::Durable => self.journal.snapshot().durable,
            QuorumPolicy::Replicated => self.journal.snapshot().written,
        }
    }

    /// Capture the complete written prefix before submitting a disk barrier.
    pub fn begin_sync(&self) -> Result<SyncTicket, ReplicationError> {
        Ok(self.journal.begin_sync()?)
    }

    /// Observe an actual successful barrier for its captured prefix only.
    /// Fenced completions update disk evidence but cannot advance commit.
    pub fn complete_sync(&mut self, ticket: SyncTicket) -> Result<(), ReplicationError> {
        self.journal.complete_sync(ticket)?;
        self.advance_commit()?;
        self.reclaim()
    }

    /// Observe an adapter write that also synchronized its complete prefix.
    pub fn complete_durable_write(&mut self, ticket: WriteTicket) -> Result<(), ReplicationError> {
        self.journal.complete_durable_write(ticket)?;
        self.advance_commit()?;
        self.reclaim()
    }

    /// Report ordered application. Release capacity only through `stored_through`.
    ///
    /// The caller asserts every operation through this exact prefix was applied
    /// in order, not only the last one. This is not consumer processing. Payloads
    /// needed for retries, replay, or lagging replicas remain in the journal.
    pub fn apply_through(&mut self, through: Prefix) -> Result<(), ReplicationError> {
        self.require_normal()?;
        if through.op < self.applied.op {
            return Err(ReplicationError::HistoryUnavailable);
        }
        if through.op > self.committed.op {
            return Err(ReplicationError::ApplyBeyondCommit);
        }
        if self.prefix_at(through.op)? != through {
            return Err(self.conflict());
        }
        self.applied = through;
        self.reclaim()?;
        if self.journal_tail.is_some_and(|tail| through.op >= tail.op) {
            self.journal_tail = None;
        }
        Ok(())
    }

    /// Immediately stop old-view ACKs, commit, admission, and application.
    /// Disk completions can still settle for a subsequent view-change report.
    /// There is deliberately no un-fence or same-view restart method.
    pub fn fence(&mut self) {
        if self.status == Status::Normal {
            self.status = Status::Fenced;
        }
    }

    /// Report uncertain storage failure. Stale generations cannot fault this core.
    pub fn fail_io(&mut self, generation: JournalGeneration) -> Result<(), ReplicationError> {
        self.journal.fail(generation)?;
        self.fault();
        Ok(())
    }

    /// Fault the current generation after this core found conflicting history.
    pub(crate) fn fault(&mut self) {
        self.journal.fault();
        self.status = Status::Faulted;
    }

    fn is_primary(&self) -> bool {
        self.configuration.voter(self.local)
            == self.configuration.primary(self.configuration.scope.view)
    }

    /// Highest matching evidence from either distinct backup.
    fn backup_acknowledged(&self) -> OpNumber {
        let [left, right] = self.local.others();
        self.acknowledged[left].max(self.acknowledged[right])
    }

    fn ready_for_appends(&self) -> bool {
        self.status == Status::Normal
            && self.journal_tail.is_none()
            && (!self.is_primary()
                || (self.applied.op >= self.activation_tail
                    && self.journal.snapshot().durable >= self.activation_tail
                    && self.backup_acknowledged() >= self.activation_tail))
    }

    fn require_normal(&self) -> Result<(), ReplicationError> {
        if self.status != Status::Normal {
            return Err(ReplicationError::NotNormal);
        }
        Ok(())
    }

    fn require_scope(&self, scope: Scope) -> Result<(), ReplicationError> {
        self.require_normal()?;
        if scope != self.configuration.scope() {
            return Err(ReplicationError::ScopeMismatch);
        }
        Ok(())
    }

    fn require_primary(&self, from: NodeId) -> Result<(), ReplicationError> {
        self.configuration.voter_index(from)?;
        if from != self.configuration.primary(self.configuration.scope.view) {
            return Err(ReplicationError::WrongRole);
        }
        Ok(())
    }

    fn prefix_at(&self, op: OpNumber) -> Result<Prefix, ReplicationError> {
        if op == self.applied.op {
            return Ok(self.applied);
        }
        if op == self.retained_base.op {
            return Ok(self.retained_base);
        }
        if op < self.retained_base.op {
            return Err(ReplicationError::HistoryUnavailable);
        }
        if op > self.accepted.op {
            return Err(ReplicationError::HistoryGap);
        }
        if let Some(tail) = self.journal_tail {
            if op == tail.op {
                return Ok(tail);
            }
            if op == self.committed.op {
                return Ok(self.committed);
            }
            // Intermediate prefixes need pinned journal evidence. Do not index
            // the empty live ring or promote partial, unverified tail evidence.
            return Err(ReplicationError::HistoryUnavailable);
        }
        // The first retained operation follows the retained base.
        op.0.checked_sub(self.retained_base.op.0)
            .and_then(|distance| distance.checked_sub(1))
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| self.operations.get(index))
            .map(|operation| operation.prefix)
            .ok_or(ReplicationError::HistoryUnavailable)
    }

    fn reclaim(&mut self) -> Result<(), ReplicationError> {
        let through = self.applied.op.min(self.stored_through());
        if through <= self.retained_base.op {
            return Ok(());
        }
        let prefix = self.prefix_at(through)?;
        self.body_bytes -= crate::release_through(&mut self.operations, through);
        self.retained_base = prefix;
        Ok(())
    }

    fn advance_commit(&mut self) -> Result<(), ReplicationError> {
        if self.status != Status::Normal {
            return Ok(());
        }
        let available = match self.configuration.policy() {
            QuorumPolicy::Durable => self.journal.snapshot().durable,
            QuorumPolicy::Replicated => self.accepted.op,
        };
        let eligible = if self.is_primary() {
            // Own matching evidence AND either distinct backup's matching evidence.
            available.min(self.backup_acknowledged())
        } else {
            available.min(self.known_commit)
        };
        if eligible > self.committed.op {
            self.committed = self.prefix_at(eligible)?;
        }
        Ok(())
    }

    fn conflict(&mut self) -> ReplicationError {
        self.fault();
        ReplicationError::ConflictingHistory
    }
}
