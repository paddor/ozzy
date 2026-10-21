//! Resolve bounded cold identities before entering the synchronous state core.

use std::{cell::RefCell, rc::Rc};

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use ozzy_core::state::{
    IdentityClaim, IdentityIndex, IdentityIndexError, IdentityKey, MemoryIdentityIndex,
};
use thiserror::Error;

use crate::{
    JournalIdentityHandoffError, JournalIndexError, JournalIndexSnapshot, SegmentReference,
};

/// One immutable persistent prefix, bounded resolved lookups, and bounded new
/// claims. Clones share immutable files, not overlays or resolved-key sets.
#[derive(Debug, Clone)]
pub struct Index {
    snapshot: Rc<JournalIndexSnapshot>,
    overlay: MemoryIdentityIndex,
    resolved: RefCell<HashMap<IdentityKey, Option<IdentityClaim>>>,
    lookup_capacity: usize,
}

impl Index {
    /// Bind an exact captured journal index to a bounded in-memory identity overlay.
    pub fn new(
        snapshot: JournalIndexSnapshot,
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

    /// Borrow the exact captured journal-index source.
    pub fn snapshot(&self) -> &JournalIndexSnapshot {
        &self.snapshot
    }
    /// Borrow bounded identity claims not yet represented by the captured disk index.
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

    /// Drop coverage of history that retirement unreferenced, without reading
    /// files. `first` is the first segment the journal still selects. Sources,
    /// their deletion protection, and overlay claims before it leave, so the
    /// result covers what a snapshot rebuilt over the retained history would.
    pub fn retire_prefix(&self, first: SegmentReference) -> Result<Self, JournalIndexError> {
        let snapshot = self.snapshot.retain_from(first)?;
        let mut overlay = self.overlay.clone();
        overlay.retain_from(first.first_chain.next_op_number());
        Ok(Self {
            snapshot: Rc::new(snapshot),
            overlay,
            resolved: RefCell::new(HashMap::new()),
            lookup_capacity: self.lookup_capacity,
        })
    }

    /// Replace persistent coverage only after exact lineage and every overlay
    /// claim have been checked. Resolved negatives never cross the handoff.
    pub async fn handoff(
        self,
        replacement: JournalIndexSnapshot,
    ) -> Result<Self, JournalIdentityHandoffError> {
        if replacement.identity() != self.snapshot.identity()
            || replacement.through().op_number < self.snapshot.through().op_number
            || !replacement
                .contains_position(self.snapshot.through())
                .await?
        {
            return Err(JournalIdentityHandoffError::SnapshotMismatch);
        }
        // In op order: index reads stay sequential, and the schedule does not
        // depend on hash order.
        let mut claims: Vec<_> = self.overlay.claims().collect();
        claims.sort_unstable_by_key(|claim| claim.op_number);
        for claim in claims {
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
/// Persistent identity resolution or bounded overlay reservation failed.
pub enum ResolveError {
    #[error(transparent)]
    /// Exact identity lookup or overlay reservation failed.
    Lookup(#[from] JournalIndexError),
    #[error(transparent)]
    /// Exact operation-identity lookup or reservation failed.
    Identity(#[from] IdentityIndexError),
}
