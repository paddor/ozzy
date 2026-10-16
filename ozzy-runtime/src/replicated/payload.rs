//! One capacity-bounded client payload allocation, including canceled sends.

use std::{cell::RefCell, sync::Arc};

use bytes::Bytes;
use futures::task::AtomicWaker;

use crate::returning::{Lent, ReturnSlot};

/// The connection never allocates a replacement while transport holds its body.
#[derive(Debug)]
pub(crate) struct Payload(RefCell<Lent<Vec<u8>, AtomicWaker>>);

impl Payload {
    pub(crate) fn waking(capacity: usize, wake: Arc<AtomicWaker>) -> Self {
        Self(RefCell::new(Lent::new(Vec::with_capacity(capacity), wake)))
    }

    /// Lend the body. The lease reserves its return slot now, so dropping it
    /// cannot fail.
    pub(crate) fn try_take(&self) -> Option<Lease> {
        let (mut body, slot) = self.0.borrow_mut().try_lend()?;
        body.clear();
        Some(Lease {
            body,
            slot: Some(slot),
            transmitted: false,
        })
    }
}

/// Returns on encode failure, canceled send, or the last immutable frame drop.
/// Only a transmitted body wakes the owner: the others return during its own
/// visit.
#[derive(Debug)]
pub(crate) struct Lease {
    pub(crate) body: Vec<u8>,
    slot: Option<ReturnSlot<Vec<u8>, AtomicWaker>>,
    transmitted: bool,
}

impl Lease {
    /// Keep the same single outstanding transport lease while sharing an
    /// independently bounded immutable source allocation.
    pub(crate) fn freeze_shared(mut self, bytes: Bytes) -> Bytes {
        self.transmitted = true;
        Bytes::from_owner(Shared {
            bytes,
            _lease: self,
        })
    }

    pub(crate) fn freeze(mut self) -> Bytes {
        // The owner is private and capacity-bounded, unlike an arbitrary public
        // Bytes slice. No payload copy and no mutation while transport uses it.
        self.transmitted = true;
        Bytes::from_owner(self)
    }
}

struct Shared {
    bytes: Bytes,
    _lease: Lease,
}

impl AsRef<[u8]> for Shared {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl AsRef<[u8]> for Lease {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            slot.send(std::mem::take(&mut self.body), self.transmitted);
        }
    }
}
