//! Separate bounded inproc lanes for reliable replay and disposable live data.

use super::{Registry, frame_slots};
use crate::signal::StateSignal;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use omq_tokio::{Context, Message, Socket, SocketType};
use ozzy_proto::{LinkSessionId, NodeId, RequestId, SubscriptionId, reader};
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

#[derive(Debug)]
struct Selection {
    broker: NodeId,
    session: LinkSessionId,
    request: RequestId,
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

    fn clear(&self) {
        for _ in 0..self.capacity {
            if self.pop().is_none() {
                break;
            }
        }
    }
}

#[derive(Debug)]
pub(in crate::replicated) struct Inbox {
    pub(in crate::replicated) id: SubscriptionId,
    namespace: SubscriptionId,
    selected: ArcSwapOption<Selection>,
    messages: Lane,
    publications: Lane,
    rejection: ArcSwapOption<Message>,
    ready: OnceLock<()>,
    lease: Arc<Lease>,
    frames: Arc<Semaphore>,
    changed: Arc<StateSignal>,
    interests: Arc<StateSignal>,
}

impl Inbox {
    pub(super) fn new(
        id: SubscriptionId,
        context: &Context,
        permit: OwnedSemaphorePermit,
        registry: &Registry,
    ) -> Self {
        Self {
            id,
            namespace: SubscriptionId::new(),
            selected: ArcSwapOption::empty(),
            messages: Lane::new(context, registry.queue, registry.maximum),
            publications: Lane::new(context, registry.queue, registry.maximum),
            rejection: ArcSwapOption::empty(),
            ready: OnceLock::new(),
            lease: Arc::new(Lease { _permit: permit }),
            frames: Arc::new(Semaphore::new(frame_slots(registry.queue))),
            changed: registry.changed.clone(),
            interests: registry.interests.clone(),
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
        self.ready.set(()).expect("one inbox initializer");
        self.changed.notify_changed();
        Ok(())
    }

    pub(super) fn select(
        &self,
        broker: NodeId,
        session: LinkSessionId,
        request: RequestId,
        subscribed: reader::Subscribed,
    ) {
        self.reset_delivery();
        self.selected.store(Some(Arc::new(Selection {
            broker,
            session,
            request,
            subscribed,
        })));
    }

    pub(super) fn matches_records(
        &self,
        broker: NodeId,
        session: Option<LinkSessionId>,
        subscription: reader::Subscription,
        source: reader::Source,
    ) -> bool {
        self.selected.load().as_ref().is_some_and(|selected| {
            selected.broker == broker
                && Some(selected.session) == session
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
            selected.broker == broker
                && Some(selected.session) == session
                && Some(selected.request) == request
        })
    }

    pub(super) fn reject(&self, message: &Message) {
        self.rejection
            .store(Some(Arc::new(Message::multipart((0..4).map(|i| {
                Bytes::copy_from_slice(message.part_slice(i).expect("four frames"))
            })))));
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
        // A replacement subscription clears queued replay. Late old frames
        // after reset must not prevent that replacement from opening.
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
        Some(Message::multipart_payloads((0..4).map(|i| {
            omq_tokio::message::Payload::from_bytes(Bytes::from_owner(Frame {
                bytes: Bytes::copy_from_slice(message.part_slice(i).expect("four frames")),
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
        self.selected
            .load()
            .as_ref()
            .map(|selected| (selected.broker, selected.session, selected.subscribed))
    }

    pub(in crate::replicated) fn pop(&self) -> Option<Message> {
        let message = self
            .ready
            .get()
            .and_then(|()| self.messages.pop())
            .or_else(|| self.rejection.swap(None).map(Arc::unwrap_or_clone));
        if message.is_some() {
            self.changed.notify_changed();
        }
        message
    }

    pub(in crate::replicated) fn clear(&self) {
        self.selected.store(None);
        self.reset_delivery();
    }

    pub(in crate::replicated) fn reset_delivery(&self) {
        if self.ready.get().is_some() {
            self.messages.clear();
        }
        self.rejection.store(None);
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
