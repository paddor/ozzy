//! Committed/speculative state composition and fail-closed recovery.

use std::{borrow::Cow, collections::VecDeque};

use ahash::AHashSet as HashSet;

use ozzy_journal::operation::{AppendSummary, OperationBody};
use thiserror::Error;

use super::{
    CanonicalState, IdentityClaim, IdentityIndex, IdentityIndexError, IdentityKey,
    MemoryIdentityIndex, StateError, StateLimits, TransitionPlan,
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

/// State transitions validated atomically against one canonical image revision.
#[derive(Debug)]
pub struct PreparedCanonicalGroup {
    expected_revision: u64,
    plans: Vec<TransitionPlan>,
}

#[derive(Debug)]
struct StagedIdentityIndex<'a, I> {
    base: &'a I,
    claims: MemoryIdentityIndex,
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
        committed: CanonicalState,
        speculative: CanonicalState,
        committed_identities: I,
        speculative_identities: I,
        pending: Vec<TransitionPlan>,
        pending_capacity: usize,
    ) -> Result<Self, CanonicalImagesError> {
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

    /// Validate a group without cloning retained identity history.
    ///
    /// The returned plans remain valid only while this image stays unchanged.
    /// Installation can still fail if the backing identity index becomes
    /// unavailable after validation. Callers that already made an external
    /// effect durable must fence and recover after such an error.
    pub fn prepare_group(
        &self,
        operations: &[(u64, OperationBody<'_>)],
    ) -> Result<PreparedCanonicalGroup, CanonicalImagesError> {
        self.prepare_group_with(operations.len(), |index, candidate, identities| {
            let (number, body) = &operations[index];
            candidate.prepare(*number, body, identities, &self.committed)
        })
    }

    /// Validate schema-checked append groups without materializing record tables.
    /// The same revision, capacity, and atomic-install rules apply as for typed bodies.
    pub fn prepare_append_group(
        &self,
        operations: &[(u64, AppendSummary)],
    ) -> Result<PreparedCanonicalGroup, CanonicalImagesError> {
        self.prepare_group_with(operations.len(), |index, candidate, _| {
            let (number, summary) = &operations[index];
            candidate.prepare_append_summary(*number, summary)
        })
    }

    /// Validate consecutive typed bodies without an encode/decode round trip.
    pub fn prepare_consecutive_group(
        &self,
        first_op_number: u64,
        bodies: &[OperationBody<'_>],
    ) -> Result<PreparedCanonicalGroup, CanonicalImagesError> {
        self.prepare_group_with(bodies.len(), |index, candidate, identities| {
            let delta = u64::try_from(index).map_err(|_| StateError::RevisionExhausted)?;
            let number = first_op_number
                .checked_add(delta)
                .ok_or(StateError::RevisionExhausted)?;
            candidate.prepare(number, &bodies[index], identities, &self.committed)
        })
    }

    fn prepare_group_with(
        &self,
        count: usize,
        mut prepare: impl FnMut(
            usize,
            &CanonicalState,
            &StagedIdentityIndex<'_, I>,
        ) -> Result<TransitionPlan, StateError>,
    ) -> Result<PreparedCanonicalGroup, CanonicalImagesError> {
        if self.pending.len().saturating_add(count) > self.pending_capacity {
            return Err(CanonicalImagesError::PendingCapacity);
        }
        let mut candidate = Cow::Borrowed(&self.speculative);
        let mut identities = StagedIdentityIndex::new(self.speculative_identities());
        let mut plans = Vec::with_capacity(count);
        for index in 0..count {
            let plan = prepare(index, &candidate, &identities)?;
            if index + 1 == count {
                identities
                    .check_capacity(plan.identity_claims().len())
                    .map_err(StateError::from)?;
            } else {
                candidate.to_mut().apply(plan.clone(), &mut identities)?;
            }
            plans.push(plan);
        }
        Ok(PreparedCanonicalGroup {
            expected_revision: self.speculative.revision(),
            plans,
        })
    }

    /// Install one previously validated group into the speculative image.
    pub fn install_prepared_group(
        &mut self,
        prepared: PreparedCanonicalGroup,
    ) -> Result<(), CanonicalImagesError> {
        if self.speculative.revision() != prepared.expected_revision
            || self.pending.len().saturating_add(prepared.plans.len()) > self.pending_capacity
        {
            return Err(CanonicalImagesError::StalePreparedGroup);
        }
        let claims = prepared
            .plans
            .iter()
            .flat_map(|plan| plan.identity_claims().iter().copied())
            .collect::<Vec<_>>();
        if self.speculative_identities.is_none() {
            self.speculative_identities = Some(self.committed_identities.clone());
        }
        self.speculative_identities
            .as_mut()
            .expect("speculative identities initialized")
            .reserve_validated(&claims)
            .map_err(StateError::from)?;
        for plan in prepared.plans {
            self.speculative.install_prepared(plan.clone());
            self.pending.push_back(plan);
        }
        Ok(())
    }

    /// Install one prepared group directly at the committed boundary.
    ///
    /// Local journals have no accepted-only suffix: their write or sync is the
    /// commit decision. One identity image therefore represents both views.
    pub fn install_committed_prepared_group(
        &mut self,
        prepared: PreparedCanonicalGroup,
    ) -> Result<(), CanonicalImagesError> {
        if self.speculative.revision() != prepared.expected_revision
            || self.committed.revision() != prepared.expected_revision
            || !self.pending.is_empty()
            || self.speculative_identities.is_some()
        {
            return Err(CanonicalImagesError::StalePreparedGroup);
        }
        let claims = prepared
            .plans
            .iter()
            .flat_map(|plan| plan.identity_claims().iter().copied())
            .collect::<Vec<_>>();
        self.committed_identities
            .reserve_validated(&claims)
            .map_err(StateError::from)?;
        for plan in prepared.plans {
            self.speculative.install_prepared(plan.clone());
            self.committed.install_prepared(plan);
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
        let plan = self.speculative.prepare(
            op_number,
            body,
            self.speculative_identities(),
            &self.committed,
        )?;
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
        if self.speculative_identities.is_none() {
            self.speculative_identities = Some(self.committed_identities.clone());
        }
        self.speculative.apply(
            plan.clone(),
            self.speculative_identities
                .as_mut()
                .expect("speculative identities initialized"),
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
        for _ in 0..count {
            let plan = self.pending.pop_front().expect("validated pending count");
            self.committed.install_prepared(plan);
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

impl<'a, I> StagedIdentityIndex<'a, I> {
    fn new(base: &'a I) -> Self {
        Self {
            base,
            claims: MemoryIdentityIndex::new(usize::MAX),
        }
    }
}

impl<I: IdentityIndex> IdentityIndex for StagedIdentityIndex<'_, I> {
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError> {
        if let Some(claim) = self.claims.get(key) {
            return Ok(Some(claim));
        }
        self.base.lookup(key)
    }

    fn check_capacity(&self, additional: usize) -> Result<(), IdentityIndexError> {
        self.base
            .check_capacity(self.claims.len().saturating_add(additional))
    }

    fn check_reserve(&self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        let keys = claims
            .iter()
            .map(|claim| claim.key())
            .collect::<HashSet<_>>();
        if keys.len() != claims.len() {
            return Err(IdentityIndexError::Conflict);
        }
        for key in keys {
            if self.lookup(key)?.is_some() {
                return Err(IdentityIndexError::Conflict);
            }
        }
        self.check_capacity(claims.len())
    }

    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_reserve(claims)?;
        self.claims.reserve_validated(claims)
    }

    fn reserve_validated(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_capacity(claims.len())?;
        self.claims.reserve_validated(claims)
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
