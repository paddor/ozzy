//! Shared PEER reader inboxes. Slow subscriptions cannot stop another link user.

use super::{
    Arc, BTreeMap, BrokerLinkError, BrokerLinks, BrokerLinksConfig, Bytes, LinkSessionId, Mutex,
    NodeId, RequestId, Semaphore, StateSignal, driver, handshake,
};
use arc_swap::ArcSwap;
use omq_tokio::Message;
use ozzy_proto::{EnvelopeLimits, Opcode, Packet, SubscriptionId, reader};
use std::sync::Weak;

mod inbox;
pub(in crate::replicated) use inbox::Inbox;

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

#[derive(Debug)]
struct Topology {
    endpoints: BTreeMap<NodeId, Option<omq_tokio::Endpoint>>,
    prefixes: BTreeMap<Bytes, BTreeMap<SubscriptionId, Weak<Inbox>>>,
}

#[derive(Debug)]
pub(super) struct Registry {
    entries: Mutex<BTreeMap<SubscriptionId, Weak<Inbox>>>,
    published: ArcSwap<BTreeMap<SubscriptionId, Weak<Inbox>>>,
    publications: ArcSwap<BTreeMap<Bytes, BTreeMap<SubscriptionId, Weak<Inbox>>>>,
    slots: Arc<Semaphore>,
    queue: usize,
    maximum: usize,
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
            published: ArcSwap::from_pointee(BTreeMap::new()),
            publications: ArcSwap::from_pointee(BTreeMap::new()),
            slots: Arc::new(Semaphore::new(slots)),
            queue,
            maximum: crate::transport::message_size_limit(config.parameters.receive.envelope)
                .ok_or(BrokerLinkError::Configuration)?,
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
        context: &omq_tokio::Context,
        id: SubscriptionId,
        prefix: Bytes,
    ) -> Result<Arc<Inbox>, BrokerLinkError> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| BrokerLinkError::Configuration)?;
        let inbox = Arc::new(Inbox::new(id, context, permit, self));
        let mut entries = self.entries.lock().expect("reader registry poisoned");
        entries.retain(|_, entry| entry.strong_count() != 0);
        if entries.contains_key(&id) {
            return Err(BrokerLinkError::Configuration);
        }
        entries.insert(id, Arc::downgrade(&inbox));
        self.published.store(Arc::new(entries.clone()));
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
        self.publications.store(Arc::new(topology.prefixes.clone()));
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
        let targets = self.publications.load();
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
        let entries = self.published.load();
        let Some(inbox) = entries
            .get(&subscribe.subscription.id)
            .and_then(Weak::upgrade)
        else {
            return;
        };
        inbox.select(
            broker,
            session,
            request,
            reader::Subscribed {
                resolved_offset: 0,
                subscription: subscribe.subscription,
                source,
            },
        );
    }

    pub(super) fn receive(
        &self,
        broker: NodeId,
        message: &Message,
        packet: Packet<'_>,
        limits: EnvelopeLimits,
    ) -> Result<bool, Arc<Inbox>> {
        let entries = self.published.load();
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
}

fn frame_slots(queue: usize) -> usize {
    2 * queue + 5
}

#[cfg(test)]
mod tests;
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
        let inbox = self.0.shared.readers.register(
            self.0.runtime.context(),
            SubscriptionId::from_bytes(*self.next_request()?.as_bytes()),
            prefix,
        )?;
        let owned = inbox.clone();
        let shared = Arc::downgrade(&self.0.shared);
        self.0.runtime.driver().spawn(async move {
            let Some(shared) = shared.upgrade() else {
                return;
            };
            tokio::select! {
                () = shared.stop.closed() => {},
                result = owned.initialize() => {
                    if let Err(error) = result {
                        let _ = shared.failure.set(error.to_string());
                        shared.stop.close();
                    }
                }
            }
        });
        Ok(inbox)
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
        match self
            .request(broker, driver::Body::Subscribe(subscribe))
            .await?
        {
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
        selected: reader::Subscribed,
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
                    driver::Body::Unsubscribe { session, selected },
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
        match reply {
            driver::Reply::Unsubscribed => Ok(()),
            _ => Err(BrokerLinkError::Response),
        }
    }
}
