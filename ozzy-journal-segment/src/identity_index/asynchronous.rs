//! Resolve bounded cold identities before entering the synchronous state core.

use std::{cell::RefCell, rc::Rc};

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use ozzy_core::state::{
    IdentityClaim, IdentityIndex, IdentityIndexError, IdentityKey, MemoryIdentityIndex,
};
use thiserror::Error;

use crate::{AsyncJournalIndexSnapshot, JournalIdentityHandoffError, JournalIndexError};

/// One immutable persistent prefix, bounded resolved lookups, and bounded new
/// claims. Clones share immutable files, not overlays or resolved-key sets.
#[derive(Debug, Clone)]
pub struct Index {
    snapshot: Rc<AsyncJournalIndexSnapshot>,
    overlay: MemoryIdentityIndex,
    resolved: RefCell<HashMap<IdentityKey, Option<IdentityClaim>>>,
    lookup_capacity: usize,
}

impl Index {
    pub fn new(
        snapshot: AsyncJournalIndexSnapshot,
        overlay_capacity: usize,
        lookup_capacity: usize,
    ) -> Self {
        Self {
            snapshot: Rc::new(snapshot),
            overlay: MemoryIdentityIndex::new(overlay_capacity),
            resolved: RefCell::new(HashMap::new()),
            lookup_capacity,
        }
    }

    pub fn snapshot(&self) -> &AsyncJournalIndexSnapshot {
        &self.snapshot
    }
    pub const fn overlay(&self) -> &MemoryIdentityIndex {
        &self.overlay
    }

    /// Replace the resolved-key set atomically. Failure or cancellation cannot
    /// turn an unresolved key into absence. Each subsequent synchronous lookup
    /// must hit this set or the live overlay; a cold miss is `LookupUnavailable`.
    ///
    /// APPEND retry identity lives in canonical producer state, not this index.
    /// Control operations need at most one key each. Resolve every key in a
    /// group before calling the synchronous core's prepare/check methods.
    pub async fn resolve(&self, keys: &[IdentityKey]) -> Result<(), ResolveError> {
        self.resolved.borrow_mut().clear();
        if keys.len() > self.lookup_capacity {
            return Err(IdentityIndexError::Capacity.into());
        }
        let mut resolved = HashMap::with_capacity(keys.len());
        for &key in keys {
            if resolved.contains_key(&key) {
                continue;
            }
            let claim = if let Some(claim) = self.overlay.get(key) {
                Some(claim)
            } else {
                self.snapshot.identity_claim(key).await?
            };
            resolved.insert(key, claim);
        }
        *self.resolved.borrow_mut() = resolved;
        Ok(())
    }

    /// Replace persistent coverage only after exact lineage and every overlay
    /// claim have been checked. Resolved negatives never cross the handoff.
    pub async fn handoff(
        self,
        replacement: AsyncJournalIndexSnapshot,
    ) -> Result<Self, JournalIdentityHandoffError> {
        if replacement.identity() != self.snapshot.identity()
            || replacement.through().op_number < self.snapshot.through().op_number
            || !replacement
                .contains_position(self.snapshot.through())
                .await?
        {
            return Err(JournalIdentityHandoffError::SnapshotMismatch);
        }
        for claim in self.overlay.claims() {
            match replacement.identity_claim(claim.key()).await? {
                Some(persisted) if persisted == claim => {}
                Some(_) => return Err(JournalIdentityHandoffError::ClaimMismatch),
                None => return Err(JournalIdentityHandoffError::ClaimMissing),
            }
        }
        Ok(Self::new(
            replacement,
            self.overlay.capacity(),
            self.lookup_capacity,
        ))
    }
}

impl IdentityIndex for Index {
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError> {
        if let Some(claim) = self.overlay.get(key) {
            return Ok(Some(claim));
        }
        self.resolved
            .borrow()
            .get(&key)
            .copied()
            .ok_or(IdentityIndexError::LookupUnavailable)
    }

    fn check_capacity(&self, additional: usize) -> Result<(), IdentityIndexError> {
        self.overlay.check_capacity(additional)
    }

    fn check_reserve(&self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_capacity(claims.len())?;
        let mut keys = HashSet::with_capacity(claims.len());
        for claim in claims {
            if !keys.insert(claim.key()) || self.lookup(claim.key())?.is_some() {
                return Err(IdentityIndexError::Conflict);
            }
        }
        Ok(())
    }

    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_reserve(claims)?;
        self.overlay.reserve(claims)
    }

    fn reserve_validated(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.overlay.reserve_validated(claims)
    }
}

#[derive(Debug, Error)]
pub enum ResolveError {
    #[error(transparent)]
    Lookup(#[from] JournalIndexError),
    #[error(transparent)]
    Identity(#[from] IdentityIndexError),
}
