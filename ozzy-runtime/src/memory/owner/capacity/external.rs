//! Charge foreign backing without allocating it or granting file-I/O authority.

use super::{Capacity, Owner, Quota, Reserved, Usage};
use std::{
    io,
    sync::{Arc, atomic::Ordering},
};

/// Sendable consumption of capacity reserved on the allocation owner. It cannot
/// reserve more memory, allocate a Ozzy buffer, or change local cache placement.
#[derive(Debug)]
pub(crate) struct Allowance(Arc<Reserved>);

/// Actual backing retained after admission. Its final owner releases the charge.
#[derive(Debug)]
pub(crate) struct Charge {
    usage: Arc<Usage>,
    quota: Quota,
}

impl Owner {
    pub(crate) fn external(&self, quota: Quota) -> io::Result<Allowance> {
        let capacity = self.capacity();
        self.reserve(&capacity, quota)?;
        Ok(Allowance(capacity.reserved.clone()))
    }

    pub(crate) fn reserve_external(&self, allowance: &Allowance, quota: Quota) -> io::Result<()> {
        // Reconstruct only a local reserve capability. The public sendable
        // allowance has no owner-local pointer or ability to mint more credit.
        let capacity = Capacity {
            reserved: allowance.0.clone(),
            ..self.capacity()
        };
        let local = self.0.borrow();
        if !Arc::ptr_eq(&local.shared.usage, &capacity.reserved.usage) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        drop(local);
        self.reserve(&capacity, quota)
    }
}

impl Allowance {
    pub(crate) fn admit(&self, quota: Quota) -> io::Result<Charge> {
        self.0.take_remaining(quota)?;
        // The coherent claim is unchanged through this conversion. Separate
        // diagnostic counters cannot make an owner see a transient free budget.
        let usage = &self.0.usage;
        usage.bytes.fetch_add(quota.bytes, Ordering::AcqRel);
        usage.buffers.fetch_add(quota.buffers, Ordering::AcqRel);
        usage
            .reserved_bytes
            .fetch_sub(quota.bytes, Ordering::AcqRel);
        usage
            .reserved_buffers
            .fetch_sub(quota.buffers, Ordering::AcqRel);
        Ok(Charge {
            usage: usage.clone(),
            quota,
        })
    }

    pub(crate) fn release(&self, quota: Quota) {
        self.0.take(quota).expect("unused foreign reservation");
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.usage
            .bytes
            .fetch_sub(self.quota.bytes, Ordering::AcqRel);
        self.usage
            .buffers
            .fetch_sub(self.quota.buffers, Ordering::AcqRel);
        self.usage.release(self.quota);
        self.usage.changed.notify_changed();
    }
}
