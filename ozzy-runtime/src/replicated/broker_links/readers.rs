//! Shared PEER reader inboxes. Slow subscriptions cannot stop another link user.

use super::{
    Arc, BTreeMap, BrokerLinkError, BrokerLinks, BrokerLinksConfig, Bytes, LinkSessionId, NodeId,
    Semaphore, Shared, StateSignal, driver, handshake,
};
use crate::transport::Frames;
use omq_tokio::Message;
use ozzy_proto::{EnvelopeLimits, Opcode, Packet, SubscriptionId, reader};
use std::{
    cell::{Cell, RefCell},
    sync::Weak,
};

mod inbox;
pub(in crate::replicated) use inbox::Inbox;
pub(super) use inbox::Sent;

/// Aggregate SDK reader admission, including unread transport frames.
#[derive(Clone, Copy, Debug)]
pub struct ReaderLinkLimits {
    /// Maximum partition subscriptions across all topic readers.
    pub subscriptions: usize,
    /// Reserved inbox, decode, and control-completion storage across subscriptions.
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
            .checked_add(receive.envelope.max_metadata_bytes.checked_add(256)?)?
            .checked_add(std::mem::size_of::<Result<driver::Reply, BrokerLinkError>>())?
            .checked_add(32768 + 2 * self.queue_messages * size_of::<Message>())?;
        let transport = message
            .checked_add(4096)?
            .checked_mul(self.queue_messages)?
            .checked_mul(brokers)?;
        Some((charge, transport))
    }
}

/// Reader slots and delivery signals, shared with application threads.
#[derive(Debug)]
pub(super) struct Admission {
    slots: Arc<Semaphore>,
    queue: usize,
    maximum: usize,
    limits: EnvelopeLimits,
    /// An inbox gained input or released capacity.
    pub(super) changed: Arc<StateSignal>,
    /// Reader prefixes or endpoints changed, or an inbox was dropped.
    interests: Arc<StateSignal>,
}

impl Admission {
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
            slots: Arc::new(Semaphore::new(slots)),
            queue,
            maximum: crate::transport::message_size_limit(config.parameters.receive.envelope)
                .ok_or(BrokerLinkError::Configuration)?,
            limits: config.parameters.receive.envelope,
            changed: Arc::new(StateSignal::default()),
            interests: Arc::new(StateSignal::default()),
        })
    }

    /// A new inbox holding one reader slot, for the driver to install.
    fn inbox(
        &self,
        context: &omq_tokio::Context,
        id: SubscriptionId,
    ) -> Result<Arc<Inbox>, BrokerLinkError> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| BrokerLinkError::Configuration)?;
        Ok(Arc::new(Inbox::new(id, context, permit, self)))
    }
}

/// A new inbox for the driver to install. Until then it holds the inbox,
/// and with it a reader slot, so a lane with a place per slot never fills.
#[derive(Debug)]
pub(super) struct Registration {
    inbox: Arc<Inbox>,
    prefix: Bytes,
}

/// Reader state of the SDK thread: the inboxes by subscription, the
/// publication prefixes they wait for, and each broker's PUB endpoint. The
/// driver shares it with its SUB drivers on that thread.
#[derive(Debug)]
pub(super) struct Registry {
    /// Installed when a SUBSCRIBE is sent, so its records always find it.
    entries: RefCell<BTreeMap<SubscriptionId, Weak<Inbox>>>,
    prefixes: RefCell<BTreeMap<Bytes, BTreeMap<SubscriptionId, Weak<Inbox>>>>,
    endpoints: RefCell<BTreeMap<NodeId, Option<omq_tokio::Endpoint>>>,
    /// Subscriptions a broker accepted, whose live topic its reader PUB now
    /// admits from this SDK. A replay cancellation keeps them.
    accepted: RefCell<BTreeMap<SubscriptionId, Accepted>>,
    /// Renewing acceptances so far; each renews its SUB's SUBSCRIBE.
    acceptances: Cell<u64>,
    changed: Arc<StateSignal>,
    pub(super) interests: Arc<StateSignal>,
}

/// A broker accepted one subscription: its SUB may now subscribe to `topic`.
#[derive(Debug)]
struct Accepted {
    broker: NodeId,
    topic: Bytes,
    token: u64,
}

impl Registry {
    pub(super) fn new(config: &BrokerLinksConfig, admission: &Admission) -> Self {
        Self {
            entries: RefCell::new(BTreeMap::new()),
            prefixes: RefCell::new(BTreeMap::new()),
            endpoints: RefCell::new(config.brokers.iter().map(|b| (b.node, None)).collect()),
            accepted: RefCell::new(BTreeMap::new()),
            acceptances: Cell::new(0),
            changed: admission.changed.clone(),
            interests: admission.interests.clone(),
        }
    }

    /// Fan publications under the registration's prefix out to its inbox.
    /// Returns the inbox for its lanes to be initialized.
    pub(super) fn register(&self, Registration { inbox, prefix }: Registration) -> Arc<Inbox> {
        let mut prefixes = self.prefixes.borrow_mut();
        prefixes.retain(|_, entries| {
            entries.retain(|_, entry| entry.strong_count() != 0);
            !entries.is_empty()
        });
        prefixes
            .entry(prefix)
            .or_default()
            .insert(inbox.id, Arc::downgrade(&inbox));
        drop(prefixes);
        self.interests.notify_changed();
        inbox
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
        let mut installed = self.endpoints.borrow_mut();
        if endpoints.iter().any(|(node, endpoint)| {
            installed
                .get(node)
                .is_none_or(|old| old.as_ref().is_some_and(|old| old != endpoint))
        }) {
            return Err(BrokerLinkError::Configuration);
        }
        for (node, endpoint) in endpoints {
            installed.insert(node, Some(endpoint));
        }
        drop(installed);
        self.interests.notify_changed();
        Ok(())
    }

    pub(super) fn endpoint(&self, node: NodeId) -> Option<omq_tokio::Endpoint> {
        self.endpoints.borrow().get(&node).cloned().flatten()
    }

    #[cfg(test)]
    pub(super) fn prefixes(&self) -> std::collections::BTreeSet<Bytes> {
        self.prefixes
            .borrow()
            .iter()
            .filter(|(_, entries)| entries.values().any(|entry| entry.strong_count() != 0))
            .map(|(prefix, _)| prefix.clone())
            .collect()
    }

    /// The live topics `broker` accepted a subscription to, each with its
    /// newest acceptance. That broker's SUB subscribes to exactly these.
    pub(super) fn admitted(&self, broker: NodeId) -> BTreeMap<Bytes, u64> {
        let entries = self.entries.borrow();
        let mut admitted = BTreeMap::new();
        for (id, accepted) in self.accepted.borrow().iter() {
            if accepted.broker == broker
                && entries
                    .get(id)
                    .is_some_and(|inbox| inbox.strong_count() != 0)
            {
                let token = admitted.entry(accepted.topic.clone()).or_insert(0);
                *token = accepted.token.max(*token);
            }
        }
        admitted
    }

    pub(super) fn publication(&self, broker: NodeId, message: &Message, limits: EnvelopeLimits) {
        let Some((_, frames)) = message.routed_packet() else {
            return;
        };
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
        let targets = self.prefixes.borrow();
        if let Some(entries) = targets.get(prefix.as_slice()) {
            let mut accepted = false;
            for inbox in entries.values().filter_map(Weak::upgrade) {
                accepted |= inbox.push(message, true);
            }
            if accepted {
                self.changed.notify_changed();
            }
        }
    }

    /// Install the selection of a SUBSCRIBE just sent, unless its inbox is
    /// gone or belongs to another subscription.
    pub(super) fn select(&self, inbox: &Weak<Inbox>, sent: Sent, subscribe: &reader::Subscribe) {
        let Some(source) = subscribe.target.group_source() else {
            return;
        };
        let Some(inbox) = inbox
            .upgrade()
            .filter(|inbox| inbox.id == subscribe.subscription.id)
        else {
            return;
        };
        let mut entries = self.entries.borrow_mut();
        entries.retain(|_, entry| entry.strong_count() != 0);
        entries.insert(inbox.id, Arc::downgrade(&inbox));
        drop(entries);
        inbox.select(
            sent,
            reader::Subscribed {
                resolved_offset: 0,
                subscription: subscribe.subscription,
                source,
            },
        );
    }

    /// An UNSUBSCRIBE was just sent: later frames of that generation are
    /// dropped here, and a source paused on its full inbox can resume. Only
    /// a cancellation of the whole subscription leaves its live topic.
    pub(super) fn unselect(&self, unsubscribe: &reader::Unsubscribe) {
        let subscription = unsubscribe.selected.subscription;
        let entries = self.entries.borrow();
        if let Some(inbox) = entries.get(&subscription.id).and_then(Weak::upgrade)
            && inbox.unselect(subscription)
        {
            self.changed.notify_changed();
        }
        if unsubscribe.cancel == reader::Cancel::Subscription
            && self
                .accepted
                .borrow_mut()
                .remove(&subscription.id)
                .is_some()
        {
            self.interests.notify_changed();
        }
    }

    pub(super) fn receive(
        &self,
        broker: NodeId,
        message: &Message,
        packet: Packet<'_>,
        limits: EnvelopeLimits,
    ) -> Result<bool, Arc<Inbox>> {
        let entries = self.entries.borrow();
        let target = if packet.envelope.opcode == Opcode::Records {
            reader::route_subscription(packet, limits)
                .ok()
                .and_then(|(subscription, source)| {
                    let inbox = entries.get(&subscription.id)?.upgrade()?;
                    inbox
                        .matches_records(broker, packet.envelope.session, subscription, source)
                        .then_some(inbox)
                })
        } else {
            entries.values().filter_map(Weak::upgrade).find(|inbox| {
                inbox.matches_request(broker, packet.envelope.session, packet.envelope.request_id)
            })
        };
        drop(entries);
        if let Some(inbox) = target {
            if packet.envelope.opcode == Opcode::Nack {
                inbox.reject(message);
            } else if !inbox.push(message, false) {
                return Err(inbox);
            } else {
                self.changed.notify_changed();
            }
        }
        Ok(true)
    }

    /// `broker` accepted `selected`. Its reader PUB published the grant before
    /// replying, so a SUBSCRIBE to the live topic sent now is admitted. One
    /// already sent stays admitted while the subscription kept its grant, so
    /// only a `renew` acceptance, or one for another broker or topic,
    /// renews it.
    pub(super) fn accept(&self, broker: NodeId, selected: &reader::Subscribed, renew: bool) {
        let Ok(topic) = reader::publication_topic(selected.source) else {
            return;
        };
        let entries = self.entries.borrow();
        let mut accepted = self.accepted.borrow_mut();
        accepted.retain(|id, _| {
            entries
                .get(id)
                .is_some_and(|inbox| inbox.strong_count() != 0)
        });
        let id = selected.subscription.id;
        let kept = (accepted.get(&id))
            .filter(|old| !renew && old.broker == broker && *old.topic == topic)
            .map(|old| old.token);
        let token = kept.unwrap_or_else(|| {
            let token = self.acceptances.get() + 1;
            self.acceptances.set(token);
            token
        });
        accepted.insert(
            id,
            Accepted {
                broker,
                topic: Bytes::copy_from_slice(&topic),
                token,
            },
        );
        drop((accepted, entries));
        self.interests.notify_changed();
    }
}

/// Bind and probe a new inbox's lanes. A failure stops the SDK owner.
pub(super) async fn initialize(inbox: Arc<Inbox>, shared: Arc<Shared>) {
    tokio::select! {
        () = shared.stop.closed() => {},
        result = inbox.initialize() => {
            if let Err(error) = result {
                let _ = shared.failure.set(error.to_string());
                shared.stop.close();
            }
        }
    }
}

fn frame_slots(queue: usize) -> usize {
    2 * queue + 5
}

#[cfg(test)]
mod tests;
impl BrokerLinks {
    #[cfg(test)]
    pub(crate) async fn inject_reader_publication(&self, broker: NodeId, message: &Message) {
        let (reply, received) = tokio::sync::oneshot::channel();
        let publication = driver::Registration::Publication {
            broker,
            message: message.clone(),
            reply,
        };
        if self.0.registrations.send(publication).await.is_ok() {
            let _ = received.await;
        }
    }
    pub(in crate::replicated) fn reader_inbox(
        &self,
        prefix: Bytes,
    ) -> Result<Arc<Inbox>, BrokerLinkError> {
        // Request IDs never repeat, so neither do subscription IDs.
        let id = SubscriptionId::from_bytes(*self.next_request()?.as_bytes());
        let inbox = self.0.shared.readers.inbox(self.0.runtime.context(), id)?;
        // The lane has a place for every slot: only a stopped driver refuses.
        self.0
            .inboxes
            .try_send(Registration {
                inbox: inbox.clone(),
                prefix,
            })
            .map_err(|_| self.closed_error())?;
        Ok(inbox)
    }
    pub(in crate::replicated) fn reader_parameters(&self) -> handshake::Parameters {
        self.0.config.parameters
    }
    #[inline]
    pub(in crate::replicated) fn reader_generation(&self) -> u64 {
        self.0.shared.readers.changed.generation()
    }
    pub(in crate::replicated) async fn reader_changed_after(&self, seen: u64) {
        self.0.shared.readers.changed.changed_after(seen).await;
    }
    /// Subscribe `inbox`, which takes the records once the request is sent.
    /// A `renew` acceptance renews the SUB's SUBSCRIBE to the live topic.
    pub(in crate::replicated) async fn subscribe(
        &self,
        broker: NodeId,
        inbox: Weak<Inbox>,
        subscribe: reader::Subscribe,
        renew: bool,
    ) -> Result<reader::Subscribed, BrokerLinkError> {
        let body = driver::Body::Subscribe {
            subscribe,
            inbox,
            renew,
        };
        match self.request(broker, body).await? {
            driver::Reply::Subscribed(selected) => Ok(selected),
            _ => Err(BrokerLinkError::Response),
        }
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
                result => return result.and_then(driver::Reply::message),
            }
        }
    }
    pub(in crate::replicated) async fn unsubscribe(
        &self,
        broker: NodeId,
        session: LinkSessionId,
        unsubscribe: reader::Unsubscribe,
    ) -> Result<(), BrokerLinkError> {
        let deadline = self
            .0
            .config
            .clock
            .now()
            .saturating_add(self.0.config.request_timeout);
        let reply = loop {
            match self
                .request_until(
                    broker,
                    driver::Body::Unsubscribe {
                        session,
                        unsubscribe,
                    },
                    deadline,
                )
                .await
            {
                Ok(reply) => break reply,
                Err(BrokerLinkError::Rejected {
                    retry: ozzy_proto::nack::RetryClass::AfterBackoff,
                    ..
                }) if self.0.config.clock.now() < deadline => {
                    self.0
                        .shared
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
        match reply {
            driver::Reply::Unsubscribed => Ok(()),
            _ => Err(BrokerLinkError::Response),
        }
    }
}
