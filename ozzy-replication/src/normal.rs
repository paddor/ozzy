use std::collections::VecDeque;

use ozzy_journal::progress::{JournalProgress, JournalSnapshot};
use ozzy_proto::NodeId;

use crate::{
    Commit, Configuration, JournalGeneration, OpNumber, PipelineLimits, Prefix, PrepareOk,
    PreparedOperation, QuorumPolicy, ReplicationError, RetainedPrepareOk, Scope, SyncTicket,
    WriteTicket,
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
    /// Installed/recovered suffix retained only in the journal, not the live
    /// descriptor ring. Fresh work waits until this entire tail is applied.
    pub journal_tail: Option<Prefix>,
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
    local: usize,
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
    acknowledged: [OpNumber; 3],
    activation_tail: OpNumber,
    installed_view: Option<crate::StartView>,
    journal_tail: Option<Prefix>,
}

impl NormalReplica {
    /// Exact prefix released from the live window after application and persistence.
    pub const fn reclaimed_prefix(&self) -> Prefix {
        self.retained_base
    }
    pub(crate) fn driver_identity(&self) -> (Configuration, NodeId) {
        (self.configuration, self.configuration.voters[self.local])
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
        let mut operations = VecDeque::new();
        operations
            .try_reserve_exact(limits.max_operations)
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
            acknowledged: [OpNumber(0); 3],
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
        self.acknowledged = [OpNumber(0); 3];
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
            journal_tail: self.journal_tail,
            ready_for_appends: self.ready_for_appends(),
        }
    }

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
        if operations.is_empty() {
            return Err(ReplicationError::EmptyPrepare);
        }
        if operations.len() > self.limits.max_operations {
            return Err(ReplicationError::Capacity);
        }

        let mut predecessor = operations[0]
            .prefix
            .op
            .0
            .checked_sub(1)
            .ok_or(ReplicationError::HistoryGap)
            .and_then(|op| self.prefix_at(OpNumber(op)))?;
        let mut first_new = operations.len();
        let mut additional_bytes = 0usize;
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
                additional_bytes = additional_bytes
                    .checked_add(operation.body_bytes)
                    .ok_or(ReplicationError::Capacity)?;
            }
            predecessor = operation.prefix;
        }
        if first_new == operations.len() {
            return Ok(Admission::Duplicate);
        }
        if !self.ready_for_appends() {
            return Err(ReplicationError::ActivationPending);
        }
        let new_count = operations.len() - first_new;
        if new_count > self.limits.max_operations - self.operations.len()
            || additional_bytes > self.limits.max_body_bytes - self.body_bytes
        {
            return Err(ReplicationError::Capacity);
        }
        let ticket = self.journal.admit(new_count as u64)?;
        self.operations.extend(&operations[first_new..]);
        self.body_bytes += additional_bytes;
        self.accepted = predecessor;
        Ok(Admission::Write { ticket, first_new })
    }

    /// Observe a complete local write. Page-cache completion casts no vote.
    pub fn complete_write(&mut self, ticket: WriteTicket) -> Result<(), ReplicationError> {
        self.journal.complete_write(ticket)?;
        self.reclaim();
        Ok(())
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
        self.advance_commit();
        self.reclaim();
        Ok(())
    }

    /// Observe an adapter write that also synchronized its complete prefix.
    pub fn complete_durable_write(&mut self, ticket: WriteTicket) -> Result<(), ReplicationError> {
        self.journal.complete_durable_write(ticket)?;
        self.advance_commit();
        self.reclaim();
        Ok(())
    }

    /// Construct a backup's cumulative durable vote, never a transport receipt.
    pub fn acknowledgment(&self) -> Result<PrepareOk, ReplicationError> {
        self.require_normal()?;
        if self.configuration.policy() != QuorumPolicy::Durable {
            return Err(ReplicationError::PolicyMismatch);
        }
        if self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        Ok(PrepareOk {
            scope: self.configuration.scope(),
            durable: self.prefix_at(self.journal.snapshot().durable)?,
        })
    }

    /// Count one authenticated backup's cumulative matching durable prefix.
    /// Duplicate deliveries/reconnections do not create extra voters.
    pub fn receive_ack(&mut self, from: NodeId, ack: PrepareOk) -> Result<(), ReplicationError> {
        if self.configuration.policy() != QuorumPolicy::Durable {
            return Err(ReplicationError::PolicyMismatch);
        }
        self.receive_prefix(from, ack.scope, ack.durable)
    }

    /// Vote for the complete validated prefix retained by the payload adapter.
    /// Physical writes are independent and cannot reclaim this prefix early.
    pub fn retained_acknowledgment(&self) -> Result<RetainedPrepareOk, ReplicationError> {
        self.require_normal()?;
        if self.configuration.policy() != QuorumPolicy::Replicated {
            return Err(ReplicationError::PolicyMismatch);
        }
        if self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        Ok(RetainedPrepareOk {
            scope: self.configuration.scope(),
            retained: self.accepted,
        })
    }

    /// Count one distinct backup's exact retained-memory prefix.
    pub fn receive_retained_ack(
        &mut self,
        from: NodeId,
        ack: RetainedPrepareOk,
    ) -> Result<(), ReplicationError> {
        if self.configuration.policy() != QuorumPolicy::Replicated {
            return Err(ReplicationError::PolicyMismatch);
        }
        self.receive_prefix(from, ack.scope, ack.retained)
    }

    fn receive_prefix(
        &mut self,
        from: NodeId,
        scope: Scope,
        prefix: Prefix,
    ) -> Result<(), ReplicationError> {
        self.require_scope(scope)?;
        let voter = self.configuration.voter_index(from)?;
        if !self.is_primary() || voter == self.local {
            return Err(ReplicationError::WrongRole);
        }
        if prefix.op < self.applied.op {
            return Ok(());
        }
        if self.prefix_at(prefix.op)? != prefix {
            return Err(self.conflict());
        }
        self.acknowledged[voter] = self.acknowledged[voter].max(prefix.op);
        self.advance_commit();
        Ok(())
    }

    /// Construct the primary's current commit announcement or idle heartbeat.
    pub fn announcement(&self) -> Result<Commit, ReplicationError> {
        self.require_normal()?;
        if !self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        Ok(Commit {
            scope: self.configuration.scope(),
            committed: self.committed,
        })
    }

    /// Retransmit the immutable installed-view descriptor after publication.
    /// Bootstrap has no descriptor. Later appends do not change this snapshot;
    /// lagging backups install it first, then catch up through normal prepares.
    pub fn start_view(&self) -> Result<Option<crate::StartView>, ReplicationError> {
        self.require_normal()?;
        if !self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        Ok(self.installed_view)
    }

    pub(crate) fn activation_prefix(&self) -> Result<Prefix, ReplicationError> {
        self.require_normal()?;
        let snapshot = self.snapshot();
        if self
            .installed_view
            .is_none_or(|installed| installed.accepted != snapshot.accepted)
            || snapshot.committed != snapshot.accepted
            || snapshot.journal.durable < snapshot.accepted.op
            || (self.is_primary()
                && self.acknowledged[(self.local + 1) % 3]
                    .max(self.acknowledged[(self.local + 2) % 3])
                    < snapshot.accepted.op)
        {
            return Err(ReplicationError::ActivationPending);
        }
        Ok(snapshot.accepted)
    }

    pub(crate) fn observe_installed_start(
        &mut self,
        from: NodeId,
        start: crate::StartView,
    ) -> Result<(), ReplicationError> {
        self.require_scope(start.scope)?;
        self.require_primary(from)?;
        if self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        if self.installed_view != Some(start) {
            return Err(self.conflict());
        }
        Ok(())
    }

    /// Observe an authenticated primary's commit, including piggybacked commit.
    /// Visibility waits for this backup's configured local evidence. Missing
    /// payload returns `HistoryGap`; retry the announcement after catch-up.
    pub fn receive_commit(&mut self, from: NodeId, commit: Commit) -> Result<(), ReplicationError> {
        self.require_scope(commit.scope)?;
        self.require_primary(from)?;
        if self.is_primary() {
            return Err(ReplicationError::WrongRole);
        }
        if commit.committed.op < self.applied.op {
            return Ok(());
        }
        if self.prefix_at(commit.committed.op)? != commit.committed {
            return Err(self.conflict());
        }
        self.known_commit = self.known_commit.max(commit.committed.op);
        self.advance_commit();
        Ok(())
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
        self.reclaim();
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
        self.status = Status::Faulted;
        Ok(())
    }

    fn is_primary(&self) -> bool {
        self.configuration.voters[self.local]
            == self.configuration.primary(self.configuration.scope.view)
    }

    fn ready_for_appends(&self) -> bool {
        self.status == Status::Normal
            && self.journal_tail.is_none()
            && (!self.is_primary()
                || (self.applied.op >= self.activation_tail
                    && self.journal.snapshot().durable >= self.activation_tail
                    && self.acknowledged[(self.local + 1) % 3]
                        .max(self.acknowledged[(self.local + 2) % 3])
                        >= self.activation_tail))
    }

    /// Prepare private post-install state without exposing durability or votes.
    /// Reuses the original preallocated descriptor ring. Only `InstallingView` can
    /// activate it after exact publication and application completion.
    pub(crate) fn validate_install(
        &self,
        ticket: crate::InstallTicket,
        operations: &[PreparedOperation],
    ) -> Result<usize, ReplicationError> {
        let (previous, body_bytes) =
            self.validate_install_chunk(ticket, ticket.committed(), operations)?;
        if previous != ticket.accepted() {
            return Err(ReplicationError::ConflictingHistory);
        }
        Ok(body_bytes)
    }

    pub(crate) fn validate_install_chunk(
        &self,
        ticket: crate::InstallTicket,
        mut previous: Prefix,
        operations: &[PreparedOperation],
    ) -> Result<(Prefix, usize), ReplicationError> {
        if self.status != Status::Fenced {
            return Err(ReplicationError::NotNormal);
        }
        if operations.len() > self.limits.max_operations {
            return Err(ReplicationError::Capacity);
        }
        let mut body_bytes = 0usize;
        for operation in operations {
            if operation.group_id != ticket.scope().group_id
                || operation.configuration_epoch != ticket.scope().configuration_epoch
                || operation.original_view >= ticket.scope().view
            {
                return Err(ReplicationError::ScopeMismatch);
            }
            if previous.op.0.checked_add(1) != Some(operation.prefix.op.0) {
                return Err(ReplicationError::HistoryGap);
            }
            if operation.previous_digest != previous.digest {
                return Err(ReplicationError::ConflictingHistory);
            }
            body_bytes = body_bytes
                .checked_add(operation.body_bytes)
                .ok_or(ReplicationError::Capacity)?;
            if body_bytes > self.limits.max_body_bytes {
                return Err(ReplicationError::Capacity);
            }
            previous = operation.prefix;
        }
        if previous.op > ticket.accepted().op
            || (previous.op == ticket.accepted().op && previous != ticket.accepted())
        {
            return Err(ReplicationError::ConflictingHistory);
        }
        Ok((previous, body_bytes))
    }

    pub(crate) fn stage_install(
        mut self,
        ticket: crate::InstallTicket,
        operations: &[PreparedOperation],
        start: crate::StartView,
        body_bytes: usize,
    ) -> Self {
        self.operations.clear();
        self.operations.extend(operations);
        self.body_bytes = body_bytes;
        self.configuration.scope = ticket.scope();
        self.journal = JournalProgress::recover(ticket.generation(), ticket.accepted().op);
        self.accepted = ticket.accepted();
        self.committed = ticket.committed();
        self.applied = ticket.committed();
        self.retained_base = ticket.committed();
        self.known_commit = ticket.committed().op;
        self.acknowledged = [OpNumber(0); 3];
        self.activation_tail = ticket.accepted().op;
        self.installed_view = Some(start);
        self.journal_tail = None;
        self
    }

    pub(crate) fn retain_installed_tail_on_disk(&mut self) {
        debug_assert!(self.operations.is_empty());
        self.journal_tail = (self.accepted.op > self.applied.op).then_some(self.accepted);
    }

    pub(crate) fn activate_installed(&mut self) {
        debug_assert_eq!(self.status, Status::Fenced);
        self.status = Status::Normal;
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
        // Difference is bounded by max_operations, checked before admission.
        let index = (op.0 - self.retained_base.op.0 - 1) as usize;
        Ok(self.operations[index].prefix)
    }

    fn reclaim(&mut self) {
        let through = self.applied.op.min(self.stored_through());
        if through <= self.retained_base.op {
            return;
        }
        let prefix = self.prefix_at(through).expect("applied persisted prefix");
        while self
            .operations
            .front()
            .is_some_and(|operation| operation.prefix.op <= through)
        {
            let operation = self.operations.pop_front().expect("checked front");
            self.body_bytes -= operation.body_bytes;
        }
        self.retained_base = prefix;
    }

    fn advance_commit(&mut self) {
        if self.status != Status::Normal {
            return;
        }
        let available = match self.configuration.policy() {
            QuorumPolicy::Durable => self.journal.snapshot().durable,
            QuorumPolicy::Replicated => self.accepted.op,
        };
        let eligible = if self.is_primary() {
            // Own matching evidence AND either distinct backup's matching evidence.
            let backup = self.acknowledged[(self.local + 1) % 3]
                .max(self.acknowledged[(self.local + 2) % 3]);
            available.min(backup)
        } else {
            available.min(self.known_commit)
        };
        if eligible > self.committed.op {
            self.committed = self.prefix_at(eligible).expect("verified retained prefix");
        }
    }

    fn conflict(&mut self) -> ReplicationError {
        self.journal
            .fail(self.journal.snapshot().generation)
            .expect("current generation");
        self.status = Status::Faulted;
        ReplicationError::ConflictingHistory
    }
}
