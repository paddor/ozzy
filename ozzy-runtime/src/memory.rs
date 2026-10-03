//! Bounded Ozzy payload allocation domains and owner-local caches.
//!
//! Construct owners after applying thread placement. Buffers allocate and first
//! initialize there. Immutable references may cross threads; final release uses
//! a bounded return queue. OMQ-owned receive buffers have separate allocation.

mod owner;
#[cfg(test)]
mod tests;

pub(crate) use owner::{Allocator, Arena};
pub(crate) use owner::{Allowance as ForeignAllowance, Charge as ForeignCharge};
pub use owner::{Buffer, Capacity, Owner, Quota};

/// Journal construction retains allocation authority through recovery handoff.
#[derive(Clone, Debug)]
pub(crate) enum AllocationSource {
    Shared(Owner),
    Reserved(Capacity),
}

impl AllocationSource {
    pub(crate) fn allocator(&self) -> Allocator {
        match self {
            Self::Shared(owner) => owner.allocator(),
            Self::Reserved(capacity) => capacity.allocator(),
        }
    }
}
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// Payload budget for one NUMA node, or the unplaced allocation domain.
#[derive(Clone, Debug)]
pub struct Domain(Arc<Shared>);

#[derive(Debug)]
struct Shared {
    node: Option<u32>,
    bytes: usize,
    reserved: AtomicUsize,
}

/// Fixed owner reservation. Data and control use different owners so data
/// cannot consume control capacity. Cache and live buffers share this budget.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Payload bytes, including cached allocations and outstanding shared bytes.
    pub bytes: usize,
    /// Maximum physical buffers and return-queue entries.
    pub buffers: usize,
    /// Maximum payload bytes kept in this owner's reuse cache.
    pub cache_bytes: usize,
}

impl Domain {
    /// Budget construction allocates no payload. CPU/NUMA placement belongs to
    /// the owner thread's startup, before calling `owner` or allocating buffers.
    pub fn new(node: Option<u32>, bytes: usize) -> io::Result<Self> {
        if bytes == 0 || bytes > isize::MAX as usize {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(Self(Arc::new(Shared {
            node,
            bytes,
            reserved: AtomicUsize::new(0),
        })))
    }

    /// Reserve an owner's full budget, including unused capacity. Returns a
    /// !Send owner; cloned owners share their local cache and reservation.
    pub fn owner(&self, limits: Limits) -> io::Result<Owner> {
        if limits.bytes == 0
            || limits.bytes > self.0.bytes
            || !(1..=65_536).contains(&limits.buffers)
            || limits.cache_bytes > limits.bytes
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.0
            .reserved
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                reserved
                    .checked_add(limits.bytes)
                    .filter(|&total| total <= self.0.bytes)
            })
            .map_err(|_| io::ErrorKind::WouldBlock)?;
        Ok(Owner::new(
            Reservation {
                domain: self.clone(),
                bytes: limits.bytes,
            },
            limits,
        ))
    }

    /// Requested allocation node. `None` makes no NUMA locality claim.
    pub fn node(&self) -> Option<u32> {
        self.0.node
    }

    /// Includes unused owner grants. A dropped owner with live shared payloads
    /// retains its reservation until those payloads have also been released.
    pub fn reserved_bytes(&self) -> usize {
        self.0.reserved.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct Reservation {
    domain: Domain,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.domain
            .0
            .reserved
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
