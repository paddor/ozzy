use super::Journal;
use crate::{
    AsyncJournalIdentityIndex, AsyncJournalIndexSnapshot, CanonicalRecoveryLimits,
    CanonicalStateRecoveryError as Error, ReplayError,
};
use ozzy_core::state::{
    CanonicalImages, CanonicalImagesError, CanonicalState, IdentityClaim, IdentityIndex,
    IdentityIndexError, IdentityKey,
};
use ozzy_journal::operation::{OperationBody, decode_operation_body};

impl Journal {
    /// Recover bounded committed/speculative state without blocking identity
    /// lookups inside the state core. Historical identity memory stays constant;
    /// only accepted transition plans consume the configured live tail budget.
    pub async fn recover_canonical_images(
        &mut self,
        limits: CanonicalRecoveryLimits,
    ) -> Result<CanonicalImages<AsyncJournalIdentityIndex>, Error> {
        let initial = self
            .selected_canonical_checkpoint(limits.state, limits.snapshot, limits.checkpoint)
            .await?
            .unwrap_or_else(|| CanonicalState::new(limits.state));
        let (committed_snapshot, accepted_snapshot) =
            self.build_recovery_index_snapshots(limits.index).await?;
        let mut committed = initial.clone();
        let mut speculative = initial;
        let mut pending = Vec::new();
        let mut accepted_only_seen = false;
        finish_replay(
            self.replay_accepted_async(async |item| {
                let body = decode_operation_body(
                    item.operation.kind,
                    &item.operation.body,
                    self.limits.operations,
                )?;
                let mut replay =
                    Resolved::new(&accepted_snapshot, &body, speculative.revision()).await?;
                if item.committed {
                    if accepted_only_seen {
                        return Err(CanonicalImagesError::CommittedAfterAccepted.into());
                    }
                    let plan = committed
                        .prepare(item.operation.op_number, &body, &replay, &committed)
                        .map_err(CanonicalImagesError::from)?;
                    committed
                        .apply(plan.clone(), &mut replay)
                        .map_err(CanonicalImagesError::from)?;
                    speculative
                        .apply(plan, &mut replay)
                        .map_err(CanonicalImagesError::from)?;
                } else {
                    accepted_only_seen = true;
                    if pending.len() >= limits.accepted_transitions {
                        return Err(CanonicalImagesError::PendingCapacity.into());
                    }
                    let plan = speculative
                        .prepare(item.operation.op_number, &body, &replay, &committed)
                        .map_err(CanonicalImagesError::from)?;
                    speculative
                        .apply(plan.clone(), &mut replay)
                        .map_err(CanonicalImagesError::from)?;
                    pending.push(plan);
                }
                Ok(())
            })
            .await,
        )?;
        if committed.revision() != self.committed_position()?.op_number
            || speculative.revision() != self.accepted_position()?.op_number
        {
            return Err(Error::PositionMismatch);
        }
        Ok(CanonicalImages::from_recovered(
            committed,
            speculative,
            AsyncJournalIdentityIndex::new(
                committed_snapshot,
                limits.retained_identities,
                limits.retained_identities,
            ),
            AsyncJournalIdentityIndex::new(
                accepted_snapshot,
                limits.retained_identities,
                limits.retained_identities,
            ),
            pending,
            limits.accepted_transitions,
        )?)
    }
}

/// One operation's exact pre-resolved identity. No history-sized overlay and no
/// file access inside `IdentityIndex`. APPEND uses canonical producer state.
pub(super) struct Resolved(Option<(IdentityKey, Option<IdentityClaim>)>);

impl Resolved {
    pub(super) async fn new(
        snapshot: &AsyncJournalIndexSnapshot,
        body: &OperationBody<'_>,
        through: u64,
    ) -> Result<Self, Error> {
        let Some(id) = crate::index::operation_id(body) else {
            return Ok(Self(None));
        };
        let key = IdentityKey::operation(id);
        Ok(Self(Some((
            key,
            snapshot.identity_claim_through(key, through).await?,
        ))))
    }
}

impl IdentityIndex for Resolved {
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError> {
        match self.0 {
            Some((resolved, claim)) if key == resolved => Ok(claim),
            _ => Err(IdentityIndexError::LookupUnavailable),
        }
    }
    fn check_capacity(&self, _additional: usize) -> Result<(), IdentityIndexError> {
        Ok(())
    }
    fn check_reserve(&self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        // Canonical control operations claim one key, APPEND claims none.
        if claims.len() > 1 {
            return Err(IdentityIndexError::Conflict);
        }
        for claim in claims {
            if self.lookup(claim.key())?.is_some() {
                return Err(IdentityIndexError::Conflict);
            }
        }
        Ok(())
    }
    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_reserve(claims)
    }
}

pub(super) fn finish_replay(result: Result<(), ReplayError<Error>>) -> Result<(), Error> {
    match result {
        Ok(()) => Ok(()),
        Err(ReplayError::Journal(error)) => Err(error.into()),
        Err(ReplayError::Visitor(error)) => Err(error),
    }
}
