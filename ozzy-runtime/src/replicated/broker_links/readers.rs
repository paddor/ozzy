//! Shared PEER reader inboxes. Slow subscriptions cannot stop another link user.

use super::{
    Arc, BTreeMap, BrokerLinkError, BrokerLinks, BrokerLinksConfig, Bytes, LinkSessionId, Mutex,
    NodeId, RequestId, Semaphore, StateSignal, driver, handshake,
};
use omq_tokio::Message;
use ozzy_proto::{EnvelopeLimits, Opcode, Packet, SubscriptionId, reader};
use std::{collections::VecDeque, sync::Weak};
use tokio::sync::OwnedSemaphorePermit;

/// Aggregate SDK reader admission, including unread transport frames.
#[derive(Clone, Copy, Debug)]
pub struct ReaderLinkLimits {
    /// Maximum partition subscriptions across all topic readers.
    pub subscriptions: usize,
    /// Reserved inbox and decode storage across these subscriptions.
    pub bytes: usize,
    /// Frames per partition before replay resumes from delivered progress.
    pub queue_messages: usize,
}

impl ReaderLinkLimits {
    /// Storage for every declared subscription, decode buffer, retained frame
    /// alias, and each broker publication queue. Independent of `self.bytes`.
    /// Invalid counts or arithmetic overflow return `None`.
    pub fn reservation_bytes(
        &self,
        receive: ozzy_proto::data::DataLimits,
        brokers: usize,
    ) -> Option<usize> {
        let (charge, transport) = self.costs(receive, brokers)?;
        charge
            .checked_mul(self.subscriptions)?
            .checked_add(transport)
    }

    fn costs(
        &self,
        receive: ozzy_proto::data::DataLimits,
        brokers: usize,
    ) -> Option<(usize, usize)> {
        if !(1..=65536).contains(&self.subscriptions)
            || !(1..=64).contains(&self.queue_messages)
            || !matches!(brokers, 1 | 3)
        {
            return None;
        }
        let message = crate::transport::message_size_limit(receive.envelope)?;
        let decode = receive.envelope.max_payload_bytes.checked_mul(2)?;
        let charge = receive
            .max_record_bytes
            .checked_mul(receive.max_records)?
            .checked_add(message)?
            .checked_add(4096)?
            .checked_add(receive.max_parts.checked_mul(256)?)?
            .checked_add(decode)?
            .checked_mul(frame_slots(self.queue_messages))?
            .checked_add(decode)?
            .checked_add(receive.envelope.max_metadata_bytes.checked_add(256)?)?;
        let transport = message
            .checked_add(4096)?
            .checked_mul(self.queue_messages)?
            .checked_mul(brokers)?;
        Some((charge, transport))
    }
}

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

#[derive(Debug, Default)]
struct State {
    selected: Option<Selection>,
    messages: VecDeque<Message>,
    publications: VecDeque<Message>,
    rejection: Option<Message>,
}

#[derive(Debug)]
pub(in crate::replicated) struct Inbox {
    pub(in crate::replicated) id: SubscriptionId,
    state: Mutex<State>,
    queue: usize,
    lease: Arc<Lease>,
    frames: Arc<Semaphore>,
    changed: Arc<StateSignal>,
    interests: Arc<StateSignal>,
}

#[derive(Debug)]
struct Topology {
    endpoints: BTreeMap<NodeId, Option<omq_tokio::Endpoint>>,
    prefixes: BTreeMap<Bytes, BTreeMap<SubscriptionId, Weak<Inbox>>>,
}

#[derive(Debug)]
pub(super) struct Registry {
    entries: Mutex<BTreeMap<SubscriptionId, Weak<Inbox>>>,
    slots: Arc<Semaphore>,
    queue: usize,
    pub(super) changed: Arc<StateSignal>,
    topology: Mutex<Topology>,
    pub(super) interests: Arc<StateSignal>,
}

impl Registry {
    pub(super) fn capacity(config: &BrokerLinksConfig) -> Result<(usize, usize), BrokerLinkError> {
        match config.reader {
            None => Ok((0, 0)),
            Some(limits) => {
                let (charge, transport) = limits
                    .costs(config.parameters.receive, config.brokers.len())
                    .ok_or(BrokerLinkError::Configuration)?;
                let slots = limits
                    .subscriptions
                    .min(limits.bytes.saturating_sub(transport) / charge);
                if slots == 0
                    || config.parameters.roles & handshake::CONSUMER == 0
                    || config.parameters.capabilities & handshake::OWNER_READ == 0
                {
                    return Err(BrokerLinkError::Configuration);
                }
                Ok((slots, limits.queue_messages))
            }
        }
    }

    pub(super) fn new(config: &BrokerLinksConfig) -> Result<Self, BrokerLinkError> {
        let (slots, queue) = Self::capacity(config)?;
        Ok(Self {
            entries: Mutex::new(BTreeMap::new()),
            slots: Arc::new(Semaphore::new(slots)),
            queue,
            changed: Arc::new(StateSignal::default()),
            topology: Mutex::new(Topology {
                endpoints: config.brokers.iter().map(|b| (b.node, None)).collect(),
                prefixes: BTreeMap::new(),
            }),
            interests: Arc::new(StateSignal::default()),
        })
    }

    pub(in crate::replicated) fn register(
        &self,
        id: SubscriptionId,
        prefix: Bytes,
    ) -> Result<Arc<Inbox>, BrokerLinkError> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| BrokerLinkError::Configuration)?;
        let inbox = Arc::new(Inbox {
            id,
            state: Mutex::new(State::default()),
            queue: self.queue,
            lease: Arc::new(Lease { _permit: permit }),
            frames: Arc::new(Semaphore::new(frame_slots(self.queue))),
            changed: self.changed.clone(),
            interests: self.interests.clone(),
        });
        let mut entries = self.entries.lock().expect("reader registry poisoned");
        entries.retain(|_, entry| entry.strong_count() != 0);
        if entries.contains_key(&id) {
            return Err(BrokerLinkError::Configuration);
        }
        entries.insert(id, Arc::downgrade(&inbox));
        let mut topology = self.topology.lock().expect("reader topology poisoned");
        topology.prefixes.retain(|_, entries| {
            entries.retain(|_, entry| entry.strong_count() != 0);
            !entries.is_empty()
        });
        topology
            .prefixes
            .entry(prefix)
            .or_default()
            .insert(id, Arc::downgrade(&inbox));
        self.interests.notify_changed();
        Ok(inbox)
    }

    pub(super) fn topic(
        &self,
        topic: &crate::topic_metadata::TopicMetadata,
    ) -> Result<(), BrokerLinkError> {
        let endpoints = topic
            .brokers()
            .iter()
            .map(|b| {
                b.reader_pub
                    .parse::<omq_tokio::Endpoint>()
                    .map(|endpoint| (b.node, endpoint))
                    .map_err(BrokerLinkError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut topology = self.topology.lock().expect("reader topology poisoned");
        if endpoints.iter().any(|(node, endpoint)| {
            topology
                .endpoints
                .get(node)
                .is_none_or(|old| old.as_ref().is_some_and(|old| old != endpoint))
        }) {
            return Err(BrokerLinkError::Configuration);
        }
        for (node, endpoint) in endpoints {
            topology.endpoints.insert(node, Some(endpoint));
        }
        drop(topology);
        self.interests.notify_changed();
        Ok(())
    }

    pub(super) fn endpoint(&self, node: NodeId) -> Option<omq_tokio::Endpoint> {
        self.topology
            .lock()
            .expect("reader topology poisoned")
            .endpoints
            .get(&node)
            .cloned()
            .flatten()
    }

    pub(super) fn prefixes(&self) -> std::collections::BTreeSet<Bytes> {
        self.topology
            .lock()
            .expect("reader topology poisoned")
            .prefixes
            .iter()
            .filter(|(_, entries)| entries.values().any(|entry| entry.strong_count() != 0))
            .map(|(prefix, _)| prefix.clone())
            .collect()
    }

    pub(super) fn publication(&self, broker: NodeId, message: &Message, limits: EnvelopeLimits) {
        if message.len() != 4 {
            return;
        }
        let frames =
            std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).expect("four frames"));
        let Ok(packet) = ozzy_proto::decode_packet(&frames, limits) else {
            return;
        };
        if packet.envelope.sender != broker
            || packet.envelope.session.is_some()
            || packet.envelope.request_id.is_some()
        {
            return;
        }
        let Ok(source) = reader::route_publication(packet, limits) else {
            return;
        };
        let Ok(prefix) = reader::publication_topic(source) else {
            return;
        };
        if message.part_slice(0) != Some(prefix.as_slice()) {
            return;
        }
        let targets = self
            .topology
            .lock()
            .expect("reader topology poisoned")
            .prefixes
            .get(prefix.as_slice())
            .map(|entries| {
                entries
                    .values()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut changed = false;
        for inbox in targets {
            let mut state = inbox.state.lock().expect("reader inbox poisoned");
            if state.publications.len() < inbox.queue
                && let Some(message) = inbox.compact(message)
            {
                state.publications.push_back(message);
                changed = true;
            }
        }
        if changed {
            self.changed.notify_changed();
        }
    }

    pub(super) fn select(
        &self,
        broker: NodeId,
        session: LinkSessionId,
        request: RequestId,
        subscribe: &reader::Subscribe,
    ) {
        let Some(source) = subscribe.target.group_source() else {
            return;
        };
        let Some(inbox) = self
            .entries
            .lock()
            .expect("reader registry poisoned")
            .get(&subscribe.subscription.id)
            .and_then(Weak::upgrade)
        else {
            return;
        };
        let mut state = inbox.state.lock().expect("reader inbox poisoned");
        state.messages.clear();
        state.rejection = None;
        state.selected = Some(Selection {
            broker,
            session,
            request,
            subscribed: reader::Subscribed {
                subscription: subscribe.subscription,
                source,
            },
        });
    }

    pub(super) fn receive(
        &self,
        broker: NodeId,
        message: &Message,
        packet: Packet<'_>,
        limits: EnvelopeLimits,
    ) -> Result<bool, Arc<Inbox>> {
        let entries = self.entries.lock().expect("reader registry poisoned");
        let target = if packet.envelope.opcode == Opcode::Records {
            reader::route_subscription(packet, limits)
                .ok()
                .and_then(|(subscription, source)| {
                    let inbox = entries.get(&subscription.id)?.upgrade()?;
                    let current = inbox
                        .state
                        .lock()
                        .expect("reader inbox poisoned")
                        .selected
                        .as_ref()
                        .is_some_and(|selected| {
                            selected.broker == broker
                                && Some(selected.session) == packet.envelope.session
                                && selected.subscribed
                                    == reader::Subscribed {
                                        subscription,
                                        source,
                                    }
                        });
                    current.then_some(inbox)
                })
        } else {
            entries.values().filter_map(Weak::upgrade).find(|inbox| {
                inbox
                    .state
                    .lock()
                    .expect("reader inbox poisoned")
                    .selected
                    .as_ref()
                    .is_some_and(|selected| {
                        selected.broker == broker
                            && Some(selected.session) == packet.envelope.session
                            && Some(selected.request) == packet.envelope.request_id
                    })
            })
        };
        drop(entries);
        if let Some(inbox) = target {
            let mut state = inbox.state.lock().expect("reader inbox poisoned");
            if packet.envelope.opcode == Opcode::Nack {
                // One terminal reply has reserved metadata backing independent
                // of record aliases. Slow readers cannot stall control replies.
                state.rejection =
                    Some(Message::multipart((0..4).map(|i| {
                        Bytes::copy_from_slice(message.part_slice(i).expect("four frames"))
                    })));
            } else {
                let compact = (state.messages.len() < inbox.queue)
                    .then(|| inbox.compact(message))
                    .flatten();
                let Some(message) = compact else {
                    drop(state);
                    return Err(inbox);
                };
                state.messages.push_back(message);
            }
            drop(state);
            self.changed.notify_changed();
        }
        Ok(true)
    }
}

impl Inbox {
    pub(in crate::replicated) fn has_capacity(&self) -> bool {
        self.frames.available_permits() > 0
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
        self.state
            .lock()
            .expect("reader inbox poisoned")
            .publications
            .pop_front()
    }
    pub(in crate::replicated) fn selected(&self) -> Option<(NodeId, reader::Subscribed)> {
        self.state
            .lock()
            .expect("reader inbox poisoned")
            .selected
            .as_ref()
            .map(|selected| (selected.broker, selected.subscribed))
    }
    pub(in crate::replicated) fn pop(&self) -> Option<Message> {
        let mut state = self.state.lock().expect("reader inbox poisoned");
        let message = state
            .messages
            .pop_front()
            .or_else(|| state.rejection.take());
        drop(state);
        if message.is_some() {
            self.changed.notify_changed();
        }
        message
    }
    pub(in crate::replicated) fn clear(&self) {
        let mut state = self.state.lock().expect("reader inbox poisoned");
        state.messages.clear();
        state.selected = None;
        state.rejection = None;
        drop(state);
        self.changed.notify_changed();
    }
    pub(in crate::replicated) fn reset_delivery(&self) {
        let mut state = self.state.lock().expect("reader inbox poisoned");
        state.messages.clear();
        state.rejection = None;
        drop(state);
        self.changed.notify_changed();
        // Keep uncertain broker state available to close. A replacement
        // Subscribe overwrites this selection and fences its queued records.
    }
}

impl Drop for Inbox {
    fn drop(&mut self) {
        self.interests.notify_changed();
    }
}

struct Frame {
    bytes: Bytes,
    _lease: Arc<MessageLease>,
}

fn frame_slots(queue: usize) -> usize {
    2 * queue + 5
}

#[cfg(test)]
mod tests;
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl BrokerLinks {
    #[cfg(test)]
    pub(crate) fn inject_reader_publication(&self, broker: NodeId, message: &Message) {
        self.0.shared.readers.publication(
            broker,
            message,
            self.0.config.parameters.receive.envelope,
        );
    }
    pub(in crate::replicated) fn reader_publications(
        &self,
        topic: &crate::topic_metadata::TopicMetadata,
    ) -> Result<(), BrokerLinkError> {
        self.0.shared.readers.topic(topic)
    }
    pub(in crate::replicated) fn reader_inbox(
        &self,
        prefix: Bytes,
    ) -> Result<Arc<Inbox>, BrokerLinkError> {
        self.0.shared.readers.register(
            SubscriptionId::from_bytes(*self.next_request()?.as_bytes()),
            prefix,
        )
    }
    pub(in crate::replicated) fn reader_parameters(&self) -> handshake::Parameters {
        self.0.config.parameters
    }
    pub(in crate::replicated) fn reader_generation(&self) -> u64 {
        self.0.shared.readers.changed.generation()
    }
    pub(in crate::replicated) async fn reader_changed_after(&self, seen: u64) {
        self.0.shared.readers.changed.changed_after(seen).await;
    }
    pub(in crate::replicated) async fn subscribe(
        &self,
        broker: NodeId,
        subscribe: reader::Subscribe,
    ) -> Result<reader::Subscribed, BrokerLinkError> {
        let message = self
            .request(broker, driver::Body::Subscribe(subscribe.clone()))
            .await?;
        let packet = driver::packet(&message, broker, self.0.config.parameters.receive.envelope)?;
        let selected =
            reader::decode_subscribed(packet, self.0.config.parameters.receive.envelope)?;
        if selected.subscription != subscribe.subscription
            || Some(selected.source) != subscribe.target.group_source()
        {
            return Err(BrokerLinkError::Response);
        }
        Ok(selected)
    }
    pub(in crate::replicated) async fn reader_ack(
        &self,
        broker: NodeId,
        ack: reader::Ack,
    ) -> Result<(), BrokerLinkError> {
        let message = self
            .request_reader_control(broker, driver::Body::Ack(ack))
            .await?;
        let packet = driver::packet(&message, broker, self.0.config.parameters.receive.envelope)?;
        if reader::decode_ack(packet, self.0.config.parameters.receive.envelope)? != ack {
            return Err(BrokerLinkError::Response);
        }
        Ok(())
    }

    async fn request_reader_control(
        &self,
        broker: NodeId,
        body: driver::Body,
    ) -> Result<Message, BrokerLinkError> {
        let deadline = self
            .clock()
            .now()
            .saturating_add(self.0.config.request_timeout);
        loop {
            match self.request_until(broker, body.clone(), deadline).await {
                Err(BrokerLinkError::Rejected {
                    retry: ozzy_proto::nack::RetryClass::AfterBackoff,
                    ..
                }) if self.clock().now() < deadline => {
                    self.clock()
                        .until(
                            self.clock()
                                .now()
                                .saturating_add(self.0.config.retry_interval)
                                .min(deadline),
                        )
                        .await;
                }
                result => return result,
            }
        }
    }
    pub(in crate::replicated) async fn unsubscribe(
        &self,
        broker: NodeId,
        selected: reader::Subscribed,
    ) -> Result<(), BrokerLinkError> {
        let deadline = self
            .0
            .config
            .clock
            .now()
            .saturating_add(self.0.config.request_timeout);
        let message = loop {
            match self
                .request_until(broker, driver::Body::Unsubscribe(selected), deadline)
                .await
            {
                Ok(message) => break message,
                Err(BrokerLinkError::Rejected {
                    retry: ozzy_proto::nack::RetryClass::AfterBackoff,
                    ..
                }) if self.0.config.clock.now() < deadline => {
                    self.0
                        .config
                        .clock
                        .until(
                            self.0
                                .config
                                .clock
                                .now()
                                .saturating_add(self.0.config.retry_interval)
                                .min(deadline),
                        )
                        .await;
                }
                // Old link/scope subscriptions are fenced by the broker owner.
                Err(BrokerLinkError::Session | BrokerLinkError::Rejected { code: 5 | 12, .. }) => {
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        };
        let packet = driver::packet(&message, broker, self.0.config.parameters.receive.envelope)?;
        if reader::decode_unsubscribed(packet, self.0.config.parameters.receive.envelope)?
            != selected
        {
            return Err(BrokerLinkError::Response);
        }
        Ok(())
    }
}
