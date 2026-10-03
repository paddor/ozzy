//! Exact persistent identity lookup plus bounded unindexed overlay.

pub(crate) mod asynchronous;

use std::rc::Rc;

use ahash::AHashSet as HashSet;
use ozzy_core::state::{
    IdentityClaim, IdentityIndex, IdentityIndexError, IdentityKey, MemoryIdentityIndex,
};
use thiserror::Error;

use crate::{JournalIndexError, JournalIndexSnapshot};

/// One pinned persistent snapshot plus identities admitted after its boundary.
#[derive(Debug, Clone)]
pub struct JournalIdentityIndex {
    snapshot: Rc<JournalIndexSnapshot>,
    overlay: MemoryIdentityIndex,
}

impl JournalIdentityIndex {
    /// Bind an exact captured journal index to a bounded in-memory identity overlay.
    pub fn new(snapshot: JournalIndexSnapshot, overlay_capacity: usize) -> Self {
        Self {
            snapshot: Rc::new(snapshot),
            overlay: MemoryIdentityIndex::new(overlay_capacity),
        }
    }

    /// Borrow the exact captured journal-index source.
    pub fn snapshot(&self) -> &JournalIndexSnapshot {
        &self.snapshot
    }

    /// Borrow bounded identity claims not yet represented by the captured disk index.
    pub const fn overlay(&self) -> &MemoryIdentityIndex {
        &self.overlay
    }

    /// Replace persistent coverage only after every overlay claim is present.
    pub fn handoff(
        self,
        replacement: JournalIndexSnapshot,
    ) -> Result<Self, JournalIdentityHandoffError> {
        if replacement.identity() != self.snapshot.identity()
            || replacement.through().op_number < self.snapshot.through().op_number
            || !replacement.contains_position(self.snapshot.through())?
        {
            return Err(JournalIdentityHandoffError::SnapshotMismatch);
        }
        for claim in self.overlay.claims() {
            match replacement.identity_claim(claim.key())? {
                Some(persisted) if persisted == claim => {}
                Some(_) => return Err(JournalIdentityHandoffError::ClaimMismatch),
                None => return Err(JournalIdentityHandoffError::ClaimMissing),
            }
        }
        Ok(Self {
            snapshot: Rc::new(replacement),
            overlay: MemoryIdentityIndex::new(self.overlay.capacity()),
        })
    }

    fn lookup_exact(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, JournalIndexError> {
        if let Some(claim) = self
            .overlay
            .lookup(key)
            .expect("memory identity lookup is infallible")
        {
            return Ok(Some(claim));
        }
        self.snapshot.identity_claim(key)
    }
}

impl IdentityIndex for JournalIdentityIndex {
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError> {
        self.lookup_exact(key)
            .map_err(|_| IdentityIndexError::LookupUnavailable)
    }

    fn check_capacity(&self, additional: usize) -> Result<(), IdentityIndexError> {
        self.overlay.check_capacity(additional)
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
        self.check_capacity(claims.len())?;
        for claim in claims {
            if self
                .snapshot
                .identity_claim(claim.key())
                .map_err(|_| IdentityIndexError::LookupUnavailable)?
                .is_some()
            {
                return Err(IdentityIndexError::Conflict);
            }
        }
        self.overlay.reserve(claims)
    }

    fn reserve_validated(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.overlay.reserve_validated(claims)
    }
}

impl JournalIndexSnapshot {
    /// Resolve an exact idempotency claim through authoritative record bytes.
    pub fn identity_claim(
        &self,
        key: IdentityKey,
    ) -> Result<Option<IdentityClaim>, JournalIndexError> {
        self.identity_claim_through(key, self.through().op_number)
    }

    pub(crate) fn identity_claim_through(
        &self,
        key: IdentityKey,
        through: u64,
    ) -> Result<Option<IdentityClaim>, JournalIndexError> {
        let operation_id = key.operation_id();
        Ok(self
            .find_operation_through(operation_id, through)?
            .map(|location| IdentityClaim {
                operation_id,
                op_number: location.entry.location.op_number,
            }))
    }
}

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
