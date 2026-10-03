//! Exact control identities and bounded owner-local reservation overlays.

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use ozzy_proto::OperationId;
use thiserror::Error;

/// Exact control-operation identity checked against persistent indexes and
/// bounded overlays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IdentityKey(OperationId);

impl IdentityKey {
    /// Create an exact control-operation identity key.
    pub const fn operation(operation_id: OperationId) -> Self {
        Self(operation_id)
    }

    /// Return the underlying control-operation identity.
    pub const fn operation_id(self) -> OperationId {
        self.0
    }
}

/// Control-operation identity plus its deterministic result coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentityClaim {
    /// Exact control-operation retry identity.
    pub operation_id: OperationId,
    /// Canonical operation number that owns the result.
    pub op_number: u64,
}

impl IdentityClaim {
    /// Return the exact identity key for this claim.
    pub const fn key(self) -> IdentityKey {
        IdentityKey::operation(self.operation_id)
    }
}

/// Exact committed index plus bounded unindexed/speculative overlay.
///
/// `reserve` is atomic: an error inserts no claim. It performs no blocking I/O;
/// storage adapters resolve disk lookup and capacity before entering the core.
pub trait IdentityIndex {
    /// Resolve an exact identity without changing the index or performing I/O.
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError>;

    /// Check whether this many additional unique claims fit.
    fn check_capacity(&self, additional: usize) -> Result<(), IdentityIndexError>;

    /// Validate an atomic reservation without changing the index.
    fn check_reserve(&self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError>;

    /// Atomically reserve identity claims; an error inserts nothing.
    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError>;

    /// Install claims already validated against this exact index generation.
    ///
    /// Canonical transition plans are opaque and revision-bound. The state
    /// engine uses this after `prepare` has checked every identity and the
    /// index cannot have changed independently. Implementations may skip
    /// duplicate lookups, but must still preserve capacity and atomicity.
    fn reserve_validated(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.reserve(claims)
    }
}

/// Bounded exact in-memory identity index for tests, simulation, and overlays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryIdentityIndex {
    capacity: usize,
    claims: HashMap<OperationId, u64>,
}

impl MemoryIdentityIndex {
    /// Create an empty exact identity overlay with this claim bound.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            claims: HashMap::new(),
        }
    }

    /// Look up one retained identity claim.
    pub fn get(&self, key: IdentityKey) -> Option<IdentityClaim> {
        self.claims
            .get(&key.operation_id())
            .map(|op_number| IdentityClaim {
                operation_id: key.operation_id(),
                op_number: *op_number,
            })
    }

    /// Maximum retained unique identity claims.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current retained unique identity count.
    pub fn len(&self) -> usize {
        self.claims.len()
    }

    /// Whether no identity claims remain.
    pub fn is_empty(&self) -> bool {
        self.claims.is_empty()
    }

    /// Remove all retained identity claims.
    pub fn clear(&mut self) {
        self.claims.clear();
    }

    /// Iterate retained claims in unspecified order.
    pub fn claims(&self) -> impl ExactSizeIterator<Item = IdentityClaim> + '_ {
        self.claims
            .iter()
            .map(|(operation_id, op_number)| IdentityClaim {
                operation_id: *operation_id,
                op_number: *op_number,
            })
    }
}

impl IdentityIndex for MemoryIdentityIndex {
    fn lookup(&self, key: IdentityKey) -> Result<Option<IdentityClaim>, IdentityIndexError> {
        Ok(self.get(key))
    }

    fn check_capacity(&self, additional: usize) -> Result<(), IdentityIndexError> {
        if self.claims.len().saturating_add(additional) > self.capacity {
            return Err(IdentityIndexError::Capacity);
        }
        Ok(())
    }

    fn check_reserve(&self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        let keys = claims
            .iter()
            .map(|claim| claim.key())
            .collect::<HashSet<_>>();
        if keys.len() != claims.len()
            || keys
                .iter()
                .any(|key| self.claims.contains_key(&key.operation_id()))
        {
            return Err(IdentityIndexError::Conflict);
        }
        self.check_capacity(keys.len())
    }

    fn reserve(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_reserve(claims)?;
        self.claims.extend(
            claims
                .iter()
                .map(|claim| (claim.operation_id, claim.op_number)),
        );
        Ok(())
    }

    fn reserve_validated(&mut self, claims: &[IdentityClaim]) -> Result<(), IdentityIndexError> {
        self.check_capacity(claims.len())?;
        self.claims.extend(
            claims
                .iter()
                .map(|claim| (claim.operation_id, claim.op_number)),
        );
        Ok(())
    }
}

/// Atomic identity-overlay reservation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum IdentityIndexError {
    #[error("identity already exists")]
    /// An identity already belongs to a different canonical operation.
    Conflict,
    #[error("identity overlay capacity exhausted")]
    /// The identity index cannot retain another claim.
    Capacity,
    #[error("persistent identity lookup is unavailable")]
    /// Required persistent identity evidence is unavailable.
    LookupUnavailable,
}
