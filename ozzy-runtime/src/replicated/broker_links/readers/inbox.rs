//! Separate bounded inproc lanes for reliable replay and disposable live data.

use super::{Admission, frame_slots};
use crate::signal::StateSignal;
use crate::transport::Frames;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use omq_tokio::{Context, Message, Socket, SocketType};
use ozzy_proto::{EnvelopeLimits, LinkSessionId, NodeId, RequestId, SubscriptionId, reader};
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
struct Lease {
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct MessageLease {
    permit: Option<OwnedSemaphorePermit>,
    _subscription: Arc<Lease>,
    changed: Arc<StateSignal>,
}

impl Drop for MessageLease {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.changed.notify_changed();
    }
}

/// A sent SUBSCRIBE: its broker, link session, and request ID. Records and
/// replies of the selection must come back on the same.
#[derive(Clone, Copy, Debug)]
pub(in crate::replicated::broker_links) struct Sent {
    pub(in crate::replicated::broker_links) broker: NodeId,
    pub(in crate::replicated::broker_links) session: LinkSessionId,
    pub(in crate::replicated::broker_links) request: RequestId,
}

#[derive(Debug)]
struct Selection {
    sent: Sent,
    subscribed: reader::Subscribed,
}

#[derive(Debug)]
struct Lane {
    send: Socket,
    receive: Socket,
    slots: Arc<Semaphore>,
    capacity: usize,
}

impl Lane {
    fn new(context: &Context, queue: usize, maximum: usize) -> Self {
        let options = crate::transport::socket_options()
            .send_hwm(1)
            .recv_hwm(queue as u32)
            .max_message_size(maximum)
            .linger(std::time::Duration::ZERO);
        Self {
            send: context.socket(SocketType::Pair, options.clone()),
            receive: context.socket(SocketType::Pair, options),
            slots: Arc::new(Semaphore::new(queue)),
            capacity: queue,
        }
    }

    async fn initialize(&self, endpoint: omq_tokio::Endpoint) -> Result<(), omq_tokio::Error> {
        self.receive.bind(endpoint.clone()).await?;
        self.send.connect(endpoint).await?;
        // Connect acknowledges registration before the inproc pipe is ready.
        // No record producer/consumer uses the lane until this probe is drained.
        self.send.send(Message::from_slice(b"ready")).await?;
        self.receive.recv().await?;
        Ok(())
    }

    fn pop(&self) -> Option<Message> {
        let message = self.receive.try_recv().ok()?;
        // Queue capacity and retained payload backing have different lifetimes.
        self.slots.add_permits(1);
        Some(message)
    }
}

/// A broker refusal of a subscription, tagged with the generation it ended.
#[derive(Debug)]
struct Rejection {
    subscription: reader::Subscription,
    message: Message,
}

/// One partition subscription's input. The SDK driver alone selects, pushes
/// and rejects; the reader cursor alone pops and clears. A selection changes
/// only when the driver sends a SUBSCRIBE or UNSUBSCRIBE, so the cursor drops
/// input of any generation other than the one it selected or is opening.
#[derive(Debug)]
pub(in crate::replicated) struct Inbox {
    pub(in crate::replicated) id: SubscriptionId,
    namespace: SubscriptionId,
    selected: ArcSwapOption<Selection>,
    messages: Lane,
    publications: Lane,
    rejection: ArcSwapOption<Rejection>,
    ready: OnceLock<()>,
    lease: Arc<Lease>,
    frames: Arc<Semaphore>,
    limits: EnvelopeLimits,
    changed: Arc<StateSignal>,
    interests: Arc<StateSignal>,
}

impl Inbox {
    pub(super) fn new(
        id: SubscriptionId,
        context: &Context,
        permit: OwnedSemaphorePermit,
        admission: &Admission,
    ) -> Self {
        Self {
            id,
            namespace: SubscriptionId::new(),
            selected: ArcSwapOption::empty(),
            messages: Lane::new(context, admission.queue, admission.maximum),
            publications: Lane::new(context, admission.queue, admission.maximum),
            rejection: ArcSwapOption::empty(),
            ready: OnceLock::new(),
            lease: Arc::new(Lease { _permit: permit }),
            frames: Arc::new(Semaphore::new(frame_slots(admission.queue))),
            limits: admission.limits,
            changed: admission.changed.clone(),
            interests: admission.interests.clone(),
        }
    }

    pub(super) async fn initialize(&self) -> Result<(), omq_tokio::Error> {
        let namespace = u128::from_be_bytes(*self.namespace.as_bytes());
        self.messages
            .initialize(format!("inproc://ozzy-reader-{namespace:032x}-replay").parse()?)
            .await?;
        self.publications
            .initialize(format!("inproc://ozzy-reader-{namespace:032x}-live").parse()?)
            .await?;
        self.ready.get_or_init(|| ());
        self.changed.notify_changed();
        Ok(())
    }

    /// Driver: a SUBSCRIBE was sent. Queued input of earlier generations
    /// stays for the cursor to drop.
    pub(super) fn select(&self, sent: Sent, subscribed: reader::Subscribed) {
        self.selected
            .store(Some(Arc::new(Selection { sent, subscribed })));
    }

    /// Driver: an UNSUBSCRIBE of `subscription` was sent. `true` when that
    /// generation was selected, so its later frames no longer enter.
    pub(super) fn unselect(&self, subscription: reader::Subscription) -> bool {
        let selected = self
            .selected
            .load()
            .as_ref()
            .is_some_and(|selected| selected.subscribed.subscription == subscription);
        if selected {
            self.selected.store(None);
        }
        selected
    }

    pub(super) fn matches_records(
        &self,
        broker: NodeId,
        session: Option<LinkSessionId>,
        subscription: reader::Subscription,
        source: reader::Source,
    ) -> bool {
        self.selected.load().as_ref().is_some_and(|selected| {
            selected.sent.broker == broker
                && Some(selected.sent.session) == session
                && selected.subscribed.subscription == subscription
                && selected.subscribed.source == source
        })
    }

    pub(super) fn matches_request(
        &self,
        broker: NodeId,
        session: Option<LinkSessionId>,
        request: Option<RequestId>,
    ) -> bool {
        self.selected.load().as_ref().is_some_and(|selected| {
            selected.sent.broker == broker
                && Some(selected.sent.session) == session
                && Some(selected.sent.request) == request
        })
    }

    /// Driver: the broker refused the selected subscription.
    pub(super) fn reject(&self, message: &Message) {
        let Some(selected) = self.selected.load_full() else {
            return;
        };
        self.rejection.store(Some(Arc::new(Rejection {
            subscription: selected.subscribed.subscription,
            message: Message::multipart(message.slices().map(Bytes::copy_from_slice)),
        })));
        self.changed.notify_changed();
    }

    pub(super) fn push(&self, message: &Message, live: bool) -> bool {
        if self.ready.get().is_none() {
            return false;
        }
        let lane = if live {
            &self.publications
        } else {
            &self.messages
        };
        let Ok(slot) = lane.slots.clone().try_acquire_owned() else {
            return false;
        };
        let Some(message) = self.compact(message) else {
            return false;
        };
        if lane.send.try_send(message).is_err() {
            return false;
        }
        // The receiving socket returns this slot on dequeue. Frame aliases
        // independently retain the bounded backing-memory admission.
        slot.forget();
        true
    }

    pub(in crate::replicated) fn can_subscribe(&self) -> bool {
        // The cursor drops queued replay of old generations while it opens
        // a replacement. Late old frames must not keep it from opening.
        self.ready.get().is_some()
            && (self.frames.available_permits() > 0
                || self.messages.slots.available_permits() < self.messages.capacity)
    }

    fn compact(&self, message: &Message) -> Option<Message> {
        let lease = Arc::new(MessageLease {
            permit: Some(self.frames.clone().try_acquire_owned().ok()?),
            _subscription: self.lease.clone(),
            changed: self.changed.clone(),
        });
        Some(Message::multipart_payloads(message.slices().map(|frame| {
            omq_tokio::message::Payload::from_bytes(Bytes::from_owner(Frame {
                bytes: Bytes::copy_from_slice(frame),
                _lease: lease.clone(),
            }))
        })))
    }

    pub(in crate::replicated) fn publication(&self) -> Option<Message> {
        self.ready.get()?;
        self.publications.pop()
    }

    pub(in crate::replicated) fn selected(
        &self,
    ) -> Option<(NodeId, LinkSessionId, reader::Subscribed)> {
        self.selected.load().as_ref().map(|selected| {
            (
                selected.sent.broker,
                selected.sent.session,
                selected.subscribed,
            )
        })
    }

    /// Cursor: the next replay frame or rejection of `current`, the
    /// generation selected or being opened. Input of every other generation
    /// is stale and dropped, which frees its queue slot and backing.
    pub(in crate::replicated) fn pop(
        &self,
        current: Option<reader::Subscription>,
    ) -> Option<Message> {
        let mut popped = false;
        let mut found = None;
        if self.ready.get().is_some() {
            for _ in 0..self.messages.capacity {
                let Some(message) = self.messages.pop() else {
                    break;
                };
                popped = true;
                if current.is_some() && self.subscription(&message) == current {
                    found = Some(message);
                    break;
                }
            }
        }
        if found.is_none()
            && let Some(rejection) = self.rejection.swap(None)
        {
            popped = true;
            if Some(rejection.subscription) == current {
                found = Some(rejection.message.clone());
            }
        }
        if popped {
            self.changed.notify_changed();
        }
        found
    }

    /// The subscription generation a replay frame belongs to.
    fn subscription(&self, message: &Message) -> Option<reader::Subscription> {
        let (_, frames) = message.routed_packet()?;
        let packet = ozzy_proto::decode_packet(&frames, self.limits).ok()?;
        reader::route_subscription(packet, self.limits)
            .ok()
            .map(|(subscription, _)| subscription)
    }

    /// Cursor: drop all queued replay input and any rejection.
    pub(in crate::replicated) fn clear(&self) {
        let _ = self.pop(None);
        self.changed.notify_changed();
    }
}

impl Drop for Inbox {
    fn drop(&mut self) {
        self.interests.notify_changed();
    }
}

#[derive(Debug)]
struct Frame {
    bytes: Bytes,
    _lease: Arc<MessageLease>,
}

impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
