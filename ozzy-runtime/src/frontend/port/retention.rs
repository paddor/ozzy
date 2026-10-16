//! Final aliases and unobserved results retain owner-local capacity.

use super::PortError;
use crate::{
    dispatch::{self, Budget},
    returning::{ReturnLane, ReturnSlot, Returning},
    signal::{CloseSignal, StateSignal},
};
use bytes::Bytes;
use omq_tokio::{Message, message::Payload};
use std::sync::Arc;

/// One earlier charge, given back to the port when its holder drops it.
#[derive(Clone, Copy, Debug)]
enum Release {
    Queue(usize),
    Retained(usize, usize),
}

/// A charge with its return place reserved at admission, so releasing it
/// later cannot fail.
type Credit = Returning<Release, Option<ReturnSlot<Release, StateSignal>>>;

#[derive(Debug)]
pub(super) struct Capacity {
    pub(super) limits: [Budget; 3],
    used: [Budget; 3],
    returns: ReturnLane<Release, StateSignal>,
    /// Closed by either end: the shard's port or the dispatcher's mailbox.
    pub(super) closed: CloseSignal,
}

impl Capacity {
    pub(super) fn new(limits: [Budget; 3], return_slots: usize) -> Self {
        Self {
            limits,
            used: [Budget::default(); 3],
            returns: ReturnLane::new(return_slots, Arc::new(StateSignal::default())),
            closed: CloseSignal::default(),
        }
    }

    /// Changes whenever capacity comes back.
    pub(super) fn changed(&self) -> &Arc<StateSignal> {
        self.returns.notice()
    }

    pub(super) fn collect(&mut self) {
        let used = &mut self.used;
        // Fixed-size returns: at most 64 events (2 KiB) per owner turn.
        self.returns.drain(64, |released| match released {
            // Each release returns exactly one earlier charge.
            Release::Queue(bucket) => {
                let used = &mut used[bucket];
                used.queue_slots = used.queue_slots.saturating_sub(1);
            }
            Release::Retained(bucket, bytes) => {
                let used = &mut used[bucket];
                used.retained_messages = used.retained_messages.saturating_sub(1);
                used.bytes = used.bytes.saturating_sub(bytes);
            }
        });
    }

    pub(super) fn reserve(
        &mut self,
        bucket: usize,
        bytes: usize,
    ) -> Result<(QueueSlot, Retention), PortError> {
        self.collect();
        if self.closed.is_closed() {
            return Err(PortError::Admission(dispatch::SendFailure::Closed));
        }
        let limit = self.limits[bucket];
        let used = &mut self.used[bucket];
        let full = PortError::Admission(dispatch::SendFailure::Admission(dispatch::Error::Full));
        if used.queue_slots >= limit.queue_slots
            || used.retained_messages >= limit.retained_messages
            || bytes > limit.bytes.saturating_sub(used.bytes)
        {
            return Err(full);
        }
        let (Some(queue_return), Some(retained_return)) =
            (self.returns.reserve(), self.returns.reserve())
        else {
            return Err(full);
        };
        // The limit checks above keep each charge within its budget.
        used.queue_slots = used.queue_slots.saturating_add(1);
        used.retained_messages = used.retained_messages.saturating_add(1);
        used.bytes = used.bytes.saturating_add(bytes);
        let queue = Returning::new(Release::Queue(bucket), Some(queue_return));
        let retained = Returning::new(Release::Retained(bucket, bytes), Some(retained_return));
        Ok((QueueSlot { _credit: queue }, Retention(Arc::new(retained))))
    }
}

/// Rides in a queued command's header; dropping the header returns the slot.
#[derive(Debug)]
pub(super) struct QueueSlot {
    _credit: Credit,
}

#[derive(Clone, Debug)]
pub(super) struct Retention(Arc<Credit>);

impl Retention {
    fn bytes(&self) -> usize {
        match **self.0 {
            Release::Retained(_, bytes) => bytes,
            Release::Queue(_) => 0,
        }
    }
}

struct TrackedBytes {
    bytes: Bytes,
    _retention: Retention,
}
impl AsRef<[u8]> for TrackedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Retention {
    pub(super) fn message(&self, mut message: Message) -> Message {
        let parts = std::iter::from_fn(|| message.pop_front_payload()).map(|payload| {
            let retained = payload.retained_size().unwrap_or(self.bytes());
            let bytes = Bytes::from_owner(TrackedBytes {
                bytes: payload.as_bytes(),
                _retention: self.clone(),
            });
            Payload::from_bytes_with_retained_size(
                bytes,
                retained.max(payload.len()).saturating_add(256),
            )
        });
        Message::multipart_payloads(parts)
    }
}
