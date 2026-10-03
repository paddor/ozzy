//! Checkpoint-backed canonical state reconstruction from selected journal bytes.

mod selected;

pub use selected::CanonicalRecoveryCandidate;

use std::collections::HashSet;

use ozzy_core::state::{
    CanonicalImages, CanonicalImagesError, CanonicalState, IdentityClaim, IdentityIndex,
    IdentityIndexError, IdentityKey, StateLimits, StateSnapshotLimits,
};
use ozzy_journal::operation::{OperationCodecError, decode_operation_body};
use thiserror::Error;

use crate::{
    CanonicalCheckpointError, CheckpointLimits, DirectoryError, IndexBuildLimits,
    JournalIdentityIndex, JournalIndexError, JournalIndexSnapshot, OpenGroupJournal, ReplayError,
};

/// Explicit bounds for semantic state and retained retry reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalRecoveryLimits {
    /// Bounds for reconstructed canonical application state.
    pub state: StateLimits,
    /// Bounds for canonical application-state snapshot decoding.
    pub snapshot: StateSnapshotLimits,
    /// Checkpoint construction and decoding bounds.
    pub checkpoint: CheckpointLimits,
    /// Derived-index construction and selector bounds.
    pub index: IndexBuildLimits,
    /// Live identity claims allowed after the recovered index boundary.
    pub retained_identities: usize,
    /// Maximum live pending plans. Selected-view candidates do not charge
    /// historical operations to this limit; activation preserves it for new work.
    pub accepted_transitions: usize,
}

impl Default for CanonicalRecoveryLimits {
    fn default() -> Self {
        Self {
            state: StateLimits::default(),
            snapshot: StateSnapshotLimits::default(),
            checkpoint: CheckpointLimits::default(),
            index: IndexBuildLimits::default(),
            retained_identities: 16 * 1024 * 1024,
            accepted_transitions: 1_048_576,
        }
    }
}

impl OpenGroupJournal {
    /// Restore committed/speculative canonical images from checkpoint plus log.
    ///
    /// Exact identity checks use source-bound disk indexes while replay advances
    /// their visible operation boundary. RAM grows with accepted transition
    /// plans and the configured live overlay, not retained record count.
    pub fn recover_canonical_images(
        &self,
        limits: CanonicalRecoveryLimits,
    ) -> Result<CanonicalImages<JournalIdentityIndex>, CanonicalStateRecoveryError> {
        let checkpoint =
            self.selected_canonical_checkpoint(limits.state, limits.snapshot, limits.checkpoint)?;
        let checkpoint_position = checkpoint
            .as_ref()
            .map_or(0, ozzy_core::state::CanonicalState::revision);
        let (committed_snapshot, accepted_snapshot) =
            self.build_recovery_index_snapshots(limits.index)?;
        let initial = checkpoint.unwrap_or_else(|| CanonicalState::new(limits.state));
        let mut committed = initial.clone();
        let mut speculative = initial;
        let mut committed_replay =
            ReplayIdentityIndex::new(&committed_snapshot, checkpoint_position);
        let mut speculative_replay =
            ReplayIdentityIndex::new(&accepted_snapshot, checkpoint_position);
        let mut pending = Vec::new();
        let mut accepted_only_seen = false;
        flatten_replay(self.replay_accepted(|item| {
            let body = decode_operation_body(
                item.operation.kind,
                item.operation.body.as_ref(),
                self.operation_limits(),
            )?;
            if item.committed {
                if accepted_only_seen {
                    return Err(CanonicalImagesError::CommittedAfterAccepted.into());
                }
                let plan = committed.prepare(
                    item.operation.op_number,
                    &body,
                    &committed_replay,
                    &committed,
                )?;
                committed.apply(plan.clone(), &mut committed_replay)?;
                committed_replay.advance(item.operation.op_number);
                speculative.apply(plan, &mut speculative_replay)?;
                speculative_replay.advance(item.operation.op_number);
            } else {
                accepted_only_seen = true;
                if pending.len() >= limits.accepted_transitions {
                    return Err(CanonicalImagesError::PendingCapacity.into());
                }
                let plan = speculative.prepare(
                    item.operation.op_number,
                    &body,
                    &speculative_replay,
                    &committed,
                )?;
                speculative.apply(plan.clone(), &mut speculative_replay)?;
                speculative_replay.advance(item.operation.op_number);
                pending.push(plan);
            }
            Ok::<_, CanonicalReplayError>(())
        }))?;
        if committed.revision() != self.committed_position()?.op_number
            || speculative.revision() != self.accepted_position()?.op_number
        {
            return Err(CanonicalStateRecoveryError::PositionMismatch);
        }
        Ok(CanonicalImages::from_recovered(
            committed,
            speculative,
            JournalIdentityIndex::new(committed_snapshot, limits.retained_identities),
            JournalIdentityIndex::new(accepted_snapshot, limits.retained_identities),
            pending,
            limits.accepted_transitions,
        )?)
    }
}

#[derive(Debug)]
struct ReplayIdentityIndex<'a> {
    snapshot: &'a JournalIndexSnapshot,
    through: u64,
}

impl ReplayIdentityIndex<'_> {
    const fn new(snapshot: &JournalIndexSnapshot, through: u64) -> ReplayIdentityIndex<'_> {
        ReplayIdentityIndex { snapshot, through }
    }

    fn advance(&mut self, through: u64) {
        self.through = through;
    }
}

impl IdentityIndex for ReplayIdentityIndex<'_> {
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError> {
        self.snapshot
            .identity_claim_through(key, self.through)
            .map_err(|_| IdentityIndexError::LookupUnavailable)
    }

    fn check_capacity(&self, _additional: usize) -> Result<(), IdentityIndexError> {
        Ok(())
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
        Ok(())
    }

    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_reserve(claims)
    }
}

fn flatten_replay(
    result: Result<(), ReplayError<CanonicalReplayError>>,
) -> Result<(), CanonicalStateRecoveryError> {
    match result {
        Ok(()) => Ok(()),
        Err(ReplayError::Journal(error)) => Err(error.into()),
        Err(ReplayError::Visitor(error)) => Err(error.into()),
    }
}

#[derive(Debug, Error)]
enum CanonicalReplayError {
    #[error(transparent)]
    Operation(#[from] OperationCodecError),
    #[error(transparent)]
    Images(#[from] CanonicalImagesError),
    #[error(transparent)]
    State(#[from] ozzy_core::state::StateError),
}

/// Typed checkpoint, journal replay, bound, or state-position failure.
#[derive(Debug, Error)]
pub enum CanonicalStateRecoveryError {
    #[error(transparent)]
    /// Journal directory validation or publication failed.
    Directory(#[from] DirectoryError),
    #[error(transparent)]
    /// Checkpoint construction or validation failed.
    Checkpoint(#[from] CanonicalCheckpointError),
    #[error(transparent)]
    /// Canonical operation-body validation failed.
    Operation(#[from] OperationCodecError),
    #[error(transparent)]
    /// Exact operation-identity lookup or reservation failed.
    Identity(#[from] IdentityIndexError),
    #[error(transparent)]
    /// Canonical committed/speculative state reconstruction failed.
    Images(#[from] CanonicalImagesError),
    #[error(transparent)]
    /// Derived index construction or validation failed.
    Index(#[from] JournalIndexError),
    #[error("recovered canonical revisions do not match journal hard state")]
    /// Recovered canonical revisions do not match journal hard state.
    PositionMismatch,
    #[error("canonical recovery selection or writer changed before activation")]
    /// Canonical recovery selection or writer changed before activation.
    SelectionChanged,
    #[error("the entire selected tail must have a published commit floor before activation")]
    /// The entire selected tail must have a published commit floor before activation.
    CommitNotPublished,
}

impl From<CanonicalReplayError> for CanonicalStateRecoveryError {
    fn from(error: CanonicalReplayError) -> Self {
        match error {
            CanonicalReplayError::Operation(error) => Self::Operation(error),
            CanonicalReplayError::Images(error) => Self::Images(error),
            CanonicalReplayError::State(error) => Self::Images(CanonicalImagesError::State(error)),
        }
    }
}
