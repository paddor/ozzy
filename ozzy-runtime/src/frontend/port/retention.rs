//! Final aliases and unobserved results retain owner-local capacity.

use super::PortError;
use crate::{
    dispatch::{self, Budget},
    signal::{CloseSignal, StateSignal},
};
use bytes::Bytes;
use omq_tokio::{Message, message::Payload};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug)]
enum Release {
    Queue(usize),
    Retained(usize, usize),
}

#[derive(Debug)]
pub(super) struct Return {
    sender: mpsc::Sender<Release>,
    pub(super) changed: Arc<StateSignal>,
    pub(super) closed: CloseSignal,
}

impl Return {
    fn release(&self, released: Release) {
        match self.sender.try_send(released) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                panic!("bounded port return mailbox overflowed")
            }
        }
        self.changed.notify_changed();
    }
}

#[derive(Debug)]
pub(super) struct Capacity {
    pub(super) limits: [Budget; 3],
    used: [Budget; 3],
    returned: mpsc::Receiver<Release>,
    pub(super) returns: Arc<Return>,
}

impl Capacity {
    pub(super) fn new(limits: [Budget; 3], return_slots: usize) -> Self {
        let (sender, returned) = mpsc::channel(return_slots);
        Self {
            limits,
            used: [Budget::default(); 3],
            returned,
            returns: Arc::new(Return {
                sender,
                changed: Arc::new(StateSignal::default()),
                closed: CloseSignal::default(),
            }),
        }
    }

    pub(super) fn collect(&mut self) {
        // Fixed-size returns: at most 64 events (2 KiB) per owner turn.
        for _ in 0..64 {
            let Ok(released) = self.returned.try_recv() else {
                break;
            };
            match released {
                Release::Queue(bucket) => self.used[bucket].queue_slots -= 1,
                Release::Retained(bucket, bytes) => {
                    self.used[bucket].retained_messages -= 1;
                    self.used[bucket].bytes -= bytes;
                }
            }
        }
        if !self.returned.is_empty() {
            self.returns.changed.notify_changed();
        }
    }

    pub(super) fn reserve(
        &mut self,
        bucket: usize,
        bytes: usize,
    ) -> Result<(QueueSlot, Retention), PortError> {
        self.collect();
        if self.returns.closed.is_closed() {
            return Err(PortError::Admission(dispatch::SendFailure::Closed));
        }
        let limit = self.limits[bucket];
        let used = &mut self.used[bucket];
        if used.queue_slots >= limit.queue_slots
            || used.retained_messages >= limit.retained_messages
            || bytes > limit.bytes.saturating_sub(used.bytes)
        {
            return Err(PortError::Admission(dispatch::SendFailure::Admission(
                dispatch::Error::Full,
            )));
        }
        used.queue_slots += 1;
        used.retained_messages += 1;
        used.bytes += bytes;
        Ok((
            QueueSlot {
                bucket,
                returns: self.returns.clone(),
            },
            Retention(Arc::new(Retained {
                bucket,
                bytes,
                returns: self.returns.clone(),
            })),
        ))
    }
}

#[derive(Debug)]
pub(super) struct QueueSlot {
    bucket: usize,
    returns: Arc<Return>,
}
impl Drop for QueueSlot {
    fn drop(&mut self) {
        self.returns.release(Release::Queue(self.bucket));
    }
}

#[derive(Clone, Debug)]
pub(super) struct Retention(Arc<Retained>);
#[derive(Debug)]
struct Retained {
    bucket: usize,
    bytes: usize,
    returns: Arc<Return>,
}
impl Drop for Retained {
    fn drop(&mut self) {
        self.returns
            .release(Release::Retained(self.bucket, self.bytes));
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
            let retained = payload.retained_size().unwrap_or(self.0.bytes);
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
