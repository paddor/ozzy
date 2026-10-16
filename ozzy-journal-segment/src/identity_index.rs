//! Exact persistent identity lookup plus bounded unindexed overlay.

pub(crate) mod asynchronous;

use thiserror::Error;

use crate::JournalIndexError;

/// Persistent snapshot handoff failed.
#[derive(Debug, Error)]
pub enum JournalIdentityHandoffError {
    #[error(transparent)]
    /// Exact identity lookup or overlay reservation failed.
    Lookup(#[from] JournalIndexError),
    #[error("replacement persistent index is missing an overlay identity")]
    /// Replacement persistent index is missing an overlay identity.
    ClaimMissing,
    #[error("replacement persistent index disagrees with an overlay identity")]
    /// Replacement persistent index disagrees with an overlay identity.
    ClaimMismatch,
    #[error("replacement persistent index does not extend the same store lineage")]
    /// Replacement persistent index does not extend the same store lineage.
    SnapshotMismatch,
}
