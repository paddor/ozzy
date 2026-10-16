//! Checkpoint-backed canonical state reconstruction from selected journal bytes.

use ozzy_core::state::{
    CanonicalImagesError, IdentityIndexError, StateLimits, StateSnapshotLimits,
};
use ozzy_journal::operation::OperationCodecError;
use thiserror::Error;

use crate::{
    CanonicalCheckpointError, CheckpointLimits, DirectoryError, IndexBuildLimits, JournalIndexError,
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
