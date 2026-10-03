//! One capacity-bounded client payload allocation, including canceled sends.

use std::{cell::RefCell, sync::Arc};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::signal::DataSignal;

/// The connection never allocates a replacement while transport holds its body.
#[derive(Debug)]
pub(crate) struct Payload {
    body: RefCell<Option<Vec<u8>>>,
    returned: RefCell<mpsc::Receiver<Vec<u8>>>,
    return_to: mpsc::Sender<Vec<u8>>,
    work: Arc<DataSignal>,
}

impl Payload {
    pub(crate) fn notifying(capacity: usize, work: Arc<DataSignal>) -> Self {
        let (return_to, returned) = mpsc::channel(1);
        Self {
            body: RefCell::new(Some(Vec::with_capacity(capacity))),
            returned: RefCell::new(returned),
            return_to,
            work,
        }
    }

    pub(crate) fn try_take(&self) -> Option<Lease> {
        self.body
            .borrow_mut()
            .take()
            .or_else(|| self.returned.borrow_mut().try_recv().ok())
            .map(|body| Lease {
                body,
                return_to: self.return_to.clone(),
                work: self.work.clone(),
                transmitted: false,
            })
    }
}

/// Returns on encode failure, canceled send, or the last immutable frame drop.
#[derive(Debug)]
pub(crate) struct Lease {
    pub(crate) body: Vec<u8>,
    return_to: mpsc::Sender<Vec<u8>>,
    work: Arc<DataSignal>,
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
        self.body.clear();
        let body = std::mem::take(&mut self.body);
        match self.return_to.try_send(body) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                panic!("single payload return slot overflowed")
            }
        }
        if self.transmitted {
            self.work.mark();
        }
    }
}
