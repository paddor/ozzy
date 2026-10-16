use super::Journal;
use crate::{
    CanonicalRecoveryLimits, CanonicalStateRecoveryError as Error, JournalIdentityIndex,
    JournalIndexSnapshot, ReplayError,
};
use ozzy_core::state::{
    CanonicalImages, CanonicalImagesError, CanonicalState, IdentityClaim, IdentityIndex,
    IdentityIndexError, IdentityKey, RecoveredImages, TransitionPlan,
};
use ozzy_journal::operation::{OperationBody, decode_operation_body};

impl Journal {
    /// Recover bounded committed/speculative state without blocking identity
    /// lookups inside the state core. Historical identity memory stays constant;
    /// only accepted transition plans consume the configured live tail budget.
    pub async fn recover_canonical_images(
        &mut self,
        limits: CanonicalRecoveryLimits,
    ) -> Result<CanonicalImages<JournalIdentityIndex>, Error> {
        let initial = self.initial_canonical_state(limits).await?;
        let (committed_snapshot, accepted_snapshot) =
            self.build_recovery_index_snapshots(limits.index).await?;
        let mut images = Replayed {
            committed: initial.clone(),
            speculative: initial,
            pending: Vec::new(),
            accepted_only_seen: false,
            max_pending: limits.accepted_transitions,
        };
        finish_replay(
            self.replay_accepted_async(async |item| {
                let op = item.operation;
                let body = decode_operation_body(op.kind, &op.body, self.limits.operations)?;
                let mut replay =
                    Resolved::new(&accepted_snapshot, &body, images.speculative.revision()).await?;
                if item.committed {
                    images.commit(op.op_number, &body, &mut replay)
                } else {
                    images.accept(op.op_number, &body, &mut replay)
                }
            })
            .await,
        )?;
        if images.committed.revision() != self.committed_position()?.op_number
            || images.speculative.revision() != self.accepted_position()?.op_number
        {
            return Err(Error::PositionMismatch);
        }
        Ok(CanonicalImages::from_recovered(
            RecoveredImages {
                committed: images.committed,
                committed_identities: identities(committed_snapshot, limits),
                speculative: images.speculative,
                speculative_identities: identities(accepted_snapshot, limits),
                pending: images.pending,
            },
            limits.accepted_transitions,
        )?)
    }

    /// The selected checkpoint's canonical state, or empty state without one.
    pub(super) async fn initial_canonical_state(
        &self,
        limits: CanonicalRecoveryLimits,
    ) -> Result<CanonicalState, Error> {
        Ok(self
            .selected_canonical_checkpoint(limits.state, limits.snapshot, limits.checkpoint)
            .await?
            .unwrap_or_else(|| CanonicalState::new(limits.state)))
    }
}

/// Committed and speculative images replayed together. Accepted-only
/// operations follow every committed one and stay pending plans.
struct Replayed {
    committed: CanonicalState,
    speculative: CanonicalState,
    pending: Vec<TransitionPlan>,
    accepted_only_seen: bool,
    max_pending: usize,
}

impl Replayed {
    /// Apply a committed operation to both images.
    fn commit(
        &mut self,
        op_number: u64,
        body: &OperationBody<'_>,
        replay: &mut Resolved,
    ) -> Result<(), Error> {
        if self.accepted_only_seen {
            return Err(CanonicalImagesError::CommittedAfterAccepted.into());
        }
        let plan = self
            .committed
            .prepare(op_number, body, &*replay)
            .map_err(CanonicalImagesError::from)?;
        self.committed
            .apply(plan.clone(), replay)
            .map_err(CanonicalImagesError::from)?;
        self.speculative
            .apply(plan, replay)
            .map_err(CanonicalImagesError::from)?;
        Ok(())
    }

    /// Apply an accepted-only operation to the speculative image and keep
    /// its plan pending.
    fn accept(
        &mut self,
        op_number: u64,
        body: &OperationBody<'_>,
        replay: &mut Resolved,
    ) -> Result<(), Error> {
        self.accepted_only_seen = true;
        if self.pending.len() >= self.max_pending {
            return Err(CanonicalImagesError::PendingCapacity.into());
        }
        let plan = self
            .speculative
            .prepare(op_number, body, &*replay)
            .map_err(CanonicalImagesError::from)?;
        self.speculative
            .apply(plan.clone(), replay)
            .map_err(CanonicalImagesError::from)?;
        self.pending.push(plan);
        Ok(())
    }
}

/// Identity index over `snapshot`, retaining the configured identities.
pub(super) fn identities(
    snapshot: JournalIndexSnapshot,
    limits: CanonicalRecoveryLimits,
) -> JournalIdentityIndex {
    JournalIdentityIndex::new(
        snapshot,
        limits.retained_identities,
        limits.retained_identities,
    )
}

/// Images with no pending transitions: committed and speculative are both
/// `state` with `identities`.
pub(super) fn settled_images(
    state: CanonicalState,
    identities: JournalIdentityIndex,
    pending_capacity: usize,
) -> Result<CanonicalImages<JournalIdentityIndex>, CanonicalImagesError> {
    CanonicalImages::from_recovered(
        RecoveredImages {
            committed: state.clone(),
            committed_identities: identities.clone(),
            speculative: state,
            speculative_identities: identities,
            pending: Vec::new(),
        },
        pending_capacity,
    )
}

/// One operation's exact pre-resolved identity. No history-sized overlay and no
/// file access inside `IdentityIndex`. APPEND uses canonical producer state.
pub(super) struct Resolved(Option<(IdentityKey, Option<IdentityClaim>)>);

impl Resolved {
    pub(super) async fn new(
        snapshot: &JournalIndexSnapshot,
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
