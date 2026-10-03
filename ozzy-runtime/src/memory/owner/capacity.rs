//! Reserve unused allocation capacity before advertising intake credit.
//! Reservations convert into physical buffer charges on the same owner.

use super::{Allocator, Local, Owner, Usage};
pub(crate) mod external;
use std::{
    cell::RefCell,
    io,
    marker::PhantomData,
    rc::{Rc, Weak as LocalWeak},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, ThreadId},
};

/// Independent allocation bounds. Buffer slots count physical allocations,
/// not records, APPEND requests, or dispatcher queue entries.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct Quota {
    /// Allocation capacity, including backing bytes beyond a visible slice.
    pub bytes: usize,
    /// Physical allocations, independently bounded from their byte capacity.
    pub buffers: usize,
}

impl Quota {
    pub(super) fn fits(self, addition: Self, limits: super::Limits) -> bool {
        addition.bytes <= limits.bytes
            && addition.buffers <= limits.buffers
            && self.bytes <= limits.bytes - addition.bytes
            && self.buffers <= limits.buffers - addition.buffers
    }
}

/// Shard-local authority to spend a pre-reserved allocation allowance. Clones
/// share one allowance. Dropping its last owner releases only unspent capacity;
/// physical buffers and their aliases remain charged to the shared memory pool.
#[derive(Clone, Debug)]
pub struct Capacity {
    reserved: Arc<Reserved>,
    local: LocalWeak<RefCell<Local>>,
    thread: ThreadId,
    local_only: PhantomData<Rc<()>>,
}

#[derive(Debug)]
pub(super) struct Reserved {
    remaining_bytes: AtomicUsize,
    remaining_buffers: AtomicUsize,
    usage: Arc<Usage>,
}

impl Owner {
    /// Create an empty allowance for an actor/service on this owner. This
    /// reserves metadata only. The deployment bounds the number of services.
    pub fn capacity(&self) -> Capacity {
        Capacity {
            reserved: Arc::new(Reserved {
                remaining_bytes: AtomicUsize::new(0),
                remaining_buffers: AtomicUsize::new(0),
                usage: self.0.borrow().shared.usage.clone(),
            }),
            local: Rc::downgrade(&self.0),
            thread: thread::current().id(),
            local_only: PhantomData,
        }
    }

    /// Reserve unused bytes and allocation slots against the same bounds as
    /// physical buffers. Counts and bytes can be granted independently. Cache
    /// eviction happens on the owner; live aliases can never be evicted.
    pub fn reserve(&self, capacity: &Capacity, quota: Quota) -> io::Result<()> {
        if !capacity.local.ptr_eq(&Rc::downgrade(&self.0)) || quota == Quota::default() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let mut local = self.0.borrow_mut();
        if quota.bytes > local.limits.bytes || quota.buffers > local.limits.buffers {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        local.collect();
        loop {
            if local.shared.usage.claimed().fits(quota, local.limits) {
                break;
            }
            let Some(block) = local.cache.pop() else {
                return Err(io::ErrorKind::WouldBlock.into());
            };
            local.cache_bytes -= block.capacity;
            drop(block);
        }
        capacity.reserved.add(quota);
        Ok(())
    }

    /// Unspent allocation grants across this owner. Physical allocations,
    /// caches, and queued returns are reported separately by `allocated_bytes`.
    pub fn reserved_capacity(&self) -> Quota {
        let local = self.0.borrow();
        let usage = &local.shared.usage;
        Quota {
            bytes: usage.reserved_bytes.load(Ordering::Acquire),
            buffers: usage.reserved_buffers.load(Ordering::Acquire),
        }
    }
}

impl Capacity {
    pub(crate) fn same_allowance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.reserved, &other.reserved)
    }

    /// Remaining unused allowance. Buffer release does not automatically grant
    /// another allocation; the shard makes that admission decision explicitly.
    pub fn remaining(&self) -> Quota {
        self.reserved.remaining()
    }

    /// Release an unused allowance after its advertised intake credit is fenced.
    /// This cannot release capacity that already became a physical buffer.
    pub fn release(&self, quota: Quota) -> io::Result<()> {
        self.reserved.take(quota)?;
        self.reserved.usage.changed.notify_changed();
        Ok(())
    }

    pub(crate) fn allocator(&self) -> Allocator {
        Allocator {
            local: self.local.clone(),
            thread: self.thread,
            maximum: usize::MAX,
            reserved: Some(Arc::downgrade(&self.reserved)),
        }
    }
}

impl Reserved {
    fn remaining(&self) -> Quota {
        Quota {
            bytes: self.remaining_bytes.load(Ordering::Acquire),
            buffers: self.remaining_buffers.load(Ordering::Acquire),
        }
    }

    pub(super) fn fits(&self, bytes: usize) -> bool {
        let remaining = self.remaining();
        remaining.bytes >= bytes && remaining.buffers > 0
    }

    fn add(&self, quota: Quota) {
        self.usage.claim(quota);
        self.remaining_bytes
            .fetch_add(quota.bytes, Ordering::AcqRel);
        self.remaining_buffers
            .fetch_add(quota.buffers, Ordering::AcqRel);
        self.usage
            .reserved_bytes
            .fetch_add(quota.bytes, Ordering::AcqRel);
        self.usage
            .reserved_buffers
            .fetch_add(quota.buffers, Ordering::AcqRel);
    }

    pub(super) fn take_remaining(&self, quota: Quota) -> io::Result<()> {
        self.remaining_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes| {
                bytes.checked_sub(quota.bytes)
            })
            .map_err(|_| io::ErrorKind::WouldBlock)?;
        if self
            .remaining_buffers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |buffers| {
                buffers.checked_sub(quota.buffers)
            })
            .is_err()
        {
            self.remaining_bytes
                .fetch_add(quota.bytes, Ordering::AcqRel);
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(())
    }

    fn take(&self, quota: Quota) -> io::Result<()> {
        self.take_remaining(quota)?;
        self.usage
            .reserved_bytes
            .fetch_sub(quota.bytes, Ordering::AcqRel);
        self.usage
            .reserved_buffers
            .fetch_sub(quota.buffers, Ordering::AcqRel);
        self.usage.release(quota);
        Ok(())
    }

    pub(super) fn spend(self: &Arc<Self>, bytes: usize) -> io::Result<Spent> {
        let quota = Quota { bytes, buffers: 1 };
        self.take(quota)?;
        Ok(Spent {
            reserved: self.clone(),
            quota: Some(quota),
        })
    }
}

impl Drop for Reserved {
    fn drop(&mut self) {
        let remaining = Quota {
            bytes: *self.remaining_bytes.get_mut(),
            buffers: *self.remaining_buffers.get_mut(),
        };
        self.usage
            .reserved_bytes
            .fetch_sub(remaining.bytes, Ordering::AcqRel);
        self.usage
            .reserved_buffers
            .fetch_sub(remaining.buffers, Ordering::AcqRel);
        self.usage.release(remaining);
        if remaining != Quota::default() {
            self.usage.changed.notify_changed();
        }
    }
}

pub(super) struct Spent {
    reserved: Arc<Reserved>,
    quota: Option<Quota>,
}

impl Spent {
    pub(super) fn commit(mut self) {
        self.quota = None;
    }
}

impl Drop for Spent {
    fn drop(&mut self) {
        if let Some(quota) = self.quota {
            self.reserved.add(quota);
        }
    }
}
