//! Group validation against staged identities, and installation of the result.

use std::borrow::Cow;

use ozzy_journal::operation::{AppendBatchSummary, AppendResults, AppendSummary, OperationBody};

use super::super::{
    CanonicalState, IdentityClaim, IdentityIndex, IdentityIndexError, IdentityKey,
    MemoryIdentityIndex, StateError, TransitionPlan,
};
use super::{CanonicalImages, CanonicalImagesError, PreparedCanonicalGroup};

impl<I> CanonicalImages<I>
where
    I: IdentityIndex + Clone,
{
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
        self.prepare_group_with(
            operations.iter(),
            |(number, body), candidate, identities| candidate.prepare(*number, body, identities),
        )
    }

    /// Validate schema-checked append groups without materializing record tables.
    /// The same revision, capacity, and atomic-install rules apply as for typed bodies.
    pub fn prepare_append_group(
        &self,
        operations: &[(u64, AppendSummary)],
    ) -> Result<PreparedCanonicalGroup, CanonicalImagesError> {
        self.prepare_group_with(operations.iter(), |(number, summary), candidate, _| {
            candidate.prepare_append_summary(*number, summary)
        })
    }

    /// Validate resolved attempt metadata without materializing stored payloads.
    /// The caller must validate every summary and result against its exact body.
    pub fn prepare_resolved_append_group(
        &self,
        bodies: &[(u64, AppendBatchSummary, AppendResults<'_>)],
    ) -> Result<PreparedCanonicalGroup, CanonicalImagesError> {
        self.prepare_group_with(bodies.iter(), |&(number, batch, results), candidate, _| {
            candidate.prepare_resolved_append_summary(number, batch, results)
        })
    }

    /// Validate consecutive typed bodies without an encode/decode round trip.
    pub fn prepare_consecutive_group(
        &self,
        first_op_number: u64,
        bodies: &[OperationBody<'_>],
    ) -> Result<PreparedCanonicalGroup, CanonicalImagesError> {
        let bodies = bodies.iter().enumerate();
        self.prepare_group_with(bodies, |(index, body), candidate, identities| {
            let delta = u64::try_from(index).map_err(|_| StateError::RevisionExhausted)?;
            let number = first_op_number
                .checked_add(delta)
                .ok_or(StateError::RevisionExhausted)?;
            candidate.prepare(number, body, identities)
        })
    }

    fn prepare_group_with<T>(
        &self,
        items: impl ExactSizeIterator<Item = T>,
        mut prepare: impl FnMut(
            T,
            &CanonicalState,
            &StagedIdentityIndex<'_, I>,
        ) -> Result<TransitionPlan, StateError>,
    ) -> Result<PreparedCanonicalGroup, CanonicalImagesError> {
        let count = items.len();
        if self.pending.len().saturating_add(count) > self.pending_capacity {
            return Err(CanonicalImagesError::PendingCapacity);
        }
        let mut candidate = Cow::Borrowed(&self.speculative);
        let mut identities = StagedIdentityIndex::new(self.speculative_identities());
        let mut plans = Vec::with_capacity(count);
        for (index, item) in items.enumerate() {
            let plan = prepare(item, &candidate, &identities)?;
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
        self.speculative_identities
            .get_or_insert_with(|| self.committed_identities.clone())
            .reserve_validated(&claims)
            .map_err(StateError::from)?;
        for plan in prepared.plans {
            self.speculative.install_prepared_copy(&plan)?;
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
            self.speculative.install_prepared_copy(&plan)?;
            self.committed.install_prepared(plan)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct StagedIdentityIndex<'a, I> {
    base: &'a I,
    claims: MemoryIdentityIndex,
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

    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_reserve(claims)?;
        self.claims.reserve_validated(claims)
    }

    fn reserve_validated(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_capacity(claims.len())?;
        self.claims.reserve_validated(claims)
    }
}
