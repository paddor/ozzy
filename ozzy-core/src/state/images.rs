//! Committed/speculative state composition and fail-closed recovery.

mod group;

use std::collections::VecDeque;

use ozzy_journal::operation::{AppendSummary, OperationBody};
use thiserror::Error;

use super::{
    CanonicalState, IdentityIndex, MemoryIdentityIndex, StateError, StateLimits, TransitionPlan,
};

/// Bounded committed state plus one accepted-only suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalImages<I = MemoryIdentityIndex> {
    committed: CanonicalState,
    speculative: CanonicalState,
    committed_identities: I,
    /// Full speculative identity image, allocated only while a suffix exists.
    speculative_identities: Option<I>,
    pending: VecDeque<TransitionPlan>,
    pending_capacity: usize,
}

/// Committed and speculative images rebuilt by replay, with the accepted
/// transition plans that lead from one to the other.
#[derive(Debug)]
pub struct RecoveredImages<I> {
    /// Application state through the confirmed canonical prefix.
    pub committed: CanonicalState,
    /// Identity index matching `committed`.
    pub committed_identities: I,
    /// Application state through accepted, possibly unconfirmed operations.
    pub speculative: CanonicalState,
    /// Identity index matching `speculative`.
    pub speculative_identities: I,
    /// Accepted plans from `committed` to `speculative`, in order.
    pub pending: Vec<TransitionPlan>,
}

/// State transitions validated atomically against one canonical image revision.
#[derive(Debug)]
pub struct PreparedCanonicalGroup {
    expected_revision: u64,
    plans: Vec<TransitionPlan>,
}

/// Fail-closed candidate used while replaying one selected journal lineage.
#[derive(Debug)]
pub struct CanonicalRecovery {
    candidate: Option<CanonicalImages>,
    accepted_only_seen: bool,
}

impl CanonicalImages {
    /// Create an empty bounded committed/speculative pair.
    pub fn new(
        state_limits: StateLimits,
        identity_capacity: usize,
        pending_capacity: usize,
    ) -> Self {
        let committed = CanonicalState::new(state_limits);
        let committed_identities = MemoryIdentityIndex::new(identity_capacity);
        Self::from_checkpoint(committed, committed_identities, pending_capacity)
    }

    /// Start at one already validated committed checkpoint.
    pub fn from_checkpoint(
        committed: CanonicalState,
        committed_identities: MemoryIdentityIndex,
        pending_capacity: usize,
    ) -> Self {
        Self {
            speculative: committed.clone(),
            speculative_identities: None,
            committed,
            committed_identities,
            pending: VecDeque::new(),
            pending_capacity,
        }
    }
}

impl<I> CanonicalImages<I>
where
    I: IdentityIndex + Clone,
{
    /// Restore exact committed/speculative images and accepted transition plans.
    pub fn from_recovered(
        recovered: RecoveredImages<I>,
        pending_capacity: usize,
    ) -> Result<Self, CanonicalImagesError> {
        let RecoveredImages {
            committed,
            committed_identities,
            speculative,
            speculative_identities,
            pending,
        } = recovered;
        if pending.len() > pending_capacity
            || speculative.revision() < committed.revision()
            || speculative.revision() - committed.revision() != pending.len() as u64
            || (pending.is_empty() && speculative != committed)
        {
            return Err(CanonicalImagesError::RecoveredStateMismatch);
        }
        let mut expected_revision = committed.revision();
        for plan in &pending {
            if plan.expected_revision != expected_revision
                || plan.op_number != expected_revision + 1
            {
                return Err(CanonicalImagesError::RecoveredStateMismatch);
            }
            expected_revision = plan.op_number;
        }
        let speculative_identities = (!pending.is_empty()).then_some(speculative_identities);
        Ok(Self {
            committed,
            speculative,
            committed_identities,
            speculative_identities,
            pending: pending.into(),
            pending_capacity,
        })
    }

    /// Application state through the confirmed canonical prefix.
    pub const fn committed(&self) -> &CanonicalState {
        &self.committed
    }

    /// Application state through accepted, possibly unconfirmed operations.
    pub const fn speculative(&self) -> &CanonicalState {
        &self.speculative
    }

    /// Identity index associated with confirmed application state.
    pub const fn committed_identities(&self) -> &I {
        &self.committed_identities
    }

    /// Identity index associated with accepted application state.
    pub const fn speculative_identities(&self) -> &I {
        match &self.speculative_identities {
            Some(identities) => identities,
            None => &self.committed_identities,
        }
    }

    /// Accepted transitions still awaiting commitment.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Replace identity storage at one fully committed image without replaying
    /// application state. The caller must verify that the replacement preserves
    /// every old claim and has no claims beyond this exact revision. This method
    /// supplies no storage or replication authority.
    pub fn replace_settled_identity_index(
        &mut self,
        revision: u64,
        identities: I,
    ) -> Result<(), CanonicalImagesError> {
        if !self.pending.is_empty()
            || self.speculative_identities.is_some()
            || self.committed.revision() != revision
            || self.speculative.revision() != revision
        {
            return Err(CanonicalImagesError::RecoveredStateMismatch);
        }
        self.committed_identities = identities;
        Ok(())
    }

    /// Replace the committed and any speculative identity index with ones
    /// derived from them, such as coverage narrowed to retained history. Both
    /// change or neither does. The caller must keep every claim a later lookup
    /// can reach; this supplies no storage or replication authority.
    pub fn map_identity_indexes<E>(
        &mut self,
        mut map: impl FnMut(&I) -> Result<I, E>,
    ) -> Result<(), E> {
        let committed = map(&self.committed_identities)?;
        let speculative = self
            .speculative_identities
            .as_ref()
            .map(&mut map)
            .transpose()?;
        self.committed_identities = committed;
        if speculative.is_some() {
            self.speculative_identities = speculative;
        }
        Ok(())
    }

    /// Validate and install one operation only into the accepted image.
    pub fn admit(
        &mut self,
        op_number: u64,
        body: &OperationBody<'_>,
    ) -> Result<(), CanonicalImagesError> {
        if self.pending.len() >= self.pending_capacity {
            return Err(CanonicalImagesError::PendingCapacity);
        }
        let plan = self
            .speculative
            .prepare(op_number, body, self.speculative_identities())?;
        self.admit_plan(plan)
    }

    /// Admit a schema-validated append without allocating record descriptions.
    /// Pending capacity and application-state checks are identical to [`Self::admit`].
    pub fn admit_append_summary(
        &mut self,
        op_number: u64,
        summary: &AppendSummary,
    ) -> Result<(), CanonicalImagesError> {
        if self.pending.len() >= self.pending_capacity {
            return Err(CanonicalImagesError::PendingCapacity);
        }
        let plan = self
            .speculative
            .prepare_append_summary(op_number, summary)?;
        self.admit_plan(plan)
    }

    fn admit_plan(&mut self, plan: TransitionPlan) -> Result<(), CanonicalImagesError> {
        self.speculative.apply(
            plan.clone(),
            self.speculative_identities
                .get_or_insert_with(|| self.committed_identities.clone()),
        )?;
        self.pending.push_back(plan);
        Ok(())
    }

    /// Apply retained transitions to committed state in operation order.
    pub fn commit_through(&mut self, op_number: u64) -> Result<(), CanonicalImagesError> {
        if op_number < self.committed.revision() {
            return Err(CanonicalImagesError::CommitRegression);
        }
        if op_number > self.speculative.revision() {
            return Err(CanonicalImagesError::CommitBeyondAccepted);
        }
        let count = usize::try_from(op_number - self.committed.revision())
            .map_err(|_| CanonicalImagesError::MissingTransition)?;
        if count > self.pending.len() {
            return Err(CanonicalImagesError::MissingTransition);
        }
        let mut expected = self.committed.revision();
        for plan in self.pending.iter().take(count) {
            expected = expected
                .checked_add(1)
                .ok_or(CanonicalImagesError::MissingTransition)?;
            if plan.op_number() != expected {
                return Err(CanonicalImagesError::MissingTransition);
            }
        }
        let claims = self
            .pending
            .iter()
            .take(count)
            .flat_map(|plan| plan.identity_claims().iter().copied())
            .collect::<Vec<_>>();
        self.committed_identities
            .reserve_validated(&claims)
            .map_err(StateError::from)?;
        for plan in self.pending.drain(..count) {
            self.committed.install_prepared(plan)?;
        }
        if self.pending.is_empty() {
            self.speculative_identities = None;
        }
        Ok(())
    }

    /// Discard every accepted-only transition and return to committed state.
    pub fn discard_suffix(&mut self) {
        self.speculative.clone_from(&self.committed);
        self.speculative_identities = None;
        self.pending.clear();
    }

    /// Atomically replace the accepted-only suffix after view selection.
    pub fn replace_suffix(
        &mut self,
        operations: &[(u64, OperationBody<'_>)],
    ) -> Result<(), CanonicalImagesError> {
        let mut replacement = self.clone();
        replacement.discard_suffix();
        for (op_number, body) in operations {
            replacement.admit(*op_number, body)?;
        }
        *self = replacement;
        Ok(())
    }
}

impl CanonicalRecovery {
    /// Create a bounded fail-closed candidate for canonical history replay.
    pub fn new(
        state_limits: StateLimits,
        identity_capacity: usize,
        pending_capacity: usize,
    ) -> Self {
        Self {
            candidate: Some(CanonicalImages::new(
                state_limits,
                identity_capacity,
                pending_capacity,
            )),
            accepted_only_seen: false,
        }
    }

    /// Start after one canonical checkpoint and retained identity set.
    pub fn from_checkpoint(
        state: CanonicalState,
        identities: MemoryIdentityIndex,
        pending_capacity: usize,
    ) -> Self {
        Self {
            candidate: Some(CanonicalImages::from_checkpoint(
                state,
                identities,
                pending_capacity,
            )),
            accepted_only_seen: false,
        }
    }

    /// Apply one replayed operation. Committed operations must form a prefix.
    pub fn apply(
        &mut self,
        op_number: u64,
        body: &OperationBody<'_>,
        committed: bool,
    ) -> Result<(), CanonicalImagesError> {
        let Some(candidate) = self.candidate.as_mut() else {
            return Err(CanonicalImagesError::RecoveryFaulted);
        };
        if committed && self.accepted_only_seen {
            self.candidate = None;
            return Err(CanonicalImagesError::CommittedAfterAccepted);
        }
        let result = if committed {
            candidate
                .prepare_consecutive_group(op_number, std::slice::from_ref(body))
                .and_then(|prepared| candidate.install_committed_prepared_group(prepared))
        } else {
            candidate.admit(op_number, body)
        };
        if result.is_err() {
            self.candidate = None;
            return result;
        }
        self.accepted_only_seen |= !committed;
        Ok(())
    }

    /// Install complete candidate. Any earlier error permanently faults it.
    pub fn finish(self) -> Result<CanonicalImages, CanonicalImagesError> {
        self.candidate.ok_or(CanonicalImagesError::RecoveryFaulted)
    }
}

/// Invalid accepted/committed composition or recovery transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CanonicalImagesError {
    #[error(transparent)]
    /// A canonical application-state transition was rejected.
    State(#[from] StateError),
    #[error("accepted transition capacity exhausted")]
    /// Accepted transition retention is full.
    PendingCapacity,
    #[error("commit position regressed")]
    /// Commitment would move backward.
    CommitRegression,
    #[error("commit position exceeds accepted state")]
    /// Commitment would pass accepted history.
    CommitBeyondAccepted,
    #[error("accepted transition prefix is incomplete")]
    /// A canonical transition required by the prefix is missing.
    MissingTransition,
    #[error("committed replay operation follows an accepted-only operation")]
    /// Recovery presents committed history after an accepted-only suffix.
    CommittedAfterAccepted,
    #[error("canonical recovery candidate is faulted")]
    /// An earlier recovery failure fenced this candidate.
    RecoveryFaulted,
    #[error("recovered state, identities, and pending plans do not share one boundary")]
    /// Recovered application state does not match its claimed prefix.
    RecoveredStateMismatch,
    #[error("prepared canonical group no longer matches the live image")]
    /// The prepared group belongs to obsolete canonical state.
    StalePreparedGroup,
}
