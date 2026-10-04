//! Two SDK PEER sockets across all brokers: control/replies and data/replay.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use bytes::Bytes;
use omq_tokio::{Endpoint, IdentitySocket, SocketType};
use ozzy_proto::{LinkSessionId, NodeId, RequestId, append::Policy, handshake, producer};
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::WriterRuntime;
use crate::{
    frontend::{LinkIds, LinkSessions},
    signal::{CloseSignal, StateSignal},
    topic_metadata::TopicMetadata,
};

mod clock;
pub use clock::{ClockError, SdkClock};
pub(super) mod append;
pub use append::AppendLinkLimits;
mod driver;
mod metadata;
mod publications;
pub(in crate::replicated) mod readers;
pub use readers::ReaderLinkLimits;
mod routing;
pub use routing::{RouteGeneration, TopicRoutes};

/// One configured broker address. Shard placement never enters the SDK table.
#[derive(Clone, Debug)]
pub struct BrokerAddress {
    /// Persistent broker identity, independently trusted or authenticated.
    pub node: NodeId,
    /// Shared native PEER endpoint.
    pub endpoint: Endpoint,
    /// Independent APPEND and replay data endpoint.
    pub data_endpoint: Endpoint,
}

/// Fixed shared-link and control-resource bounds for one SDK owner.
#[derive(Clone, Debug)]
pub struct BrokerLinksConfig {
    /// SDK process identity used across its broker connections.
    pub local: NodeId,
    /// One or three brokers, each exactly once.
    pub brokers: Vec<BrokerAddress>,
    /// Shared negotiation profile for writer and reader commands.
    pub parameters: handshake::Parameters,
    /// Aggregate control-frame slots, including unread raw reply aliases.
    /// Decoded subscription completions use the reader reservation instead.
    pub requests: usize,
    /// Aggregate control storage, including unused slots and encoding scratch.
    pub control_bytes: usize,
    /// Aggregate cached topic identity, watch registrations, and hints. Zero
    /// disables routing watches. Idle cached interests stay bounded here.
    pub routing_bytes: usize,
    /// Aggregate APPEND transport capacity across logical writers and brokers.
    /// Absent for control-only owners. Writer intake has separate bounds.
    pub append: Option<AppendLinkLimits>,
    /// Bounded shared reader inboxes. Absent for writer-only owners.
    pub reader: Option<ReaderLinkLimits>,
    /// Per-broker partition bound for topic discovery. One lookup attempts
    /// each of the one or three configured brokers within shared admission.
    pub maximum_partitions: usize,
    /// Includes link negotiation and waiting for local control admission.
    pub request_timeout: Duration,
    /// HELLO retry interval while a broker is unavailable.
    pub retry_interval: Duration,
    /// Production monotonic clock or an injected deterministic clock.
    pub clock: SdkClock,
}

impl BrokerLinksConfig {
    /// Storage for every declared control slot and each broker's receive and
    /// encoding scratch. This includes unused capacity. Arithmetic overflow
    /// returns `None`; it does not validate identities or negotiation settings.
    pub fn control_reservation_bytes(&self) -> Option<usize> {
        let (scratch, slot) = control_sizes(self)?;
        self.requests.checked_mul(slot)?.checked_add(scratch)
    }

    /// Validate fixed admission and negotiation without starting an SDK owner
    /// or creating sockets. Smaller byte budgets may admit fewer request slots.
    pub fn validate(&self) -> Result<(), BrokerLinkError> {
        validate(self).map(|_| ())
    }
}

#[derive(Debug)]
struct Peer {
    data: Arc<IdentitySocket>,
    sender: mpsc::Sender<driver::Command>,
    slots: Arc<Semaphore>,
    closed: CloseSignal,
}

#[derive(Debug)]
struct Shared {
    local: NodeId,
    sessions: LinkSessions,
    stop: CloseSignal,
    changed: StateSignal,
    failure: OnceLock<String>,
    request_ids: LinkIds,
    routing: Mutex<routing::Registry>,
    appends: Arc<append::Registry>,
    readers: readers::Registry,
}

#[derive(Debug)]
struct Inner {
    config: BrokerLinksConfig,
    peers: BTreeMap<NodeId, Arc<Peer>>,
    publications: Vec<Arc<publications::Link>>,
    shared: Arc<Shared>,
    runtime: WriterRuntime,
}

struct ReplyObserver<'a> {
    received: Option<oneshot::Receiver<Result<driver::Reply, BrokerLinkError>>>,
    changed: &'a StateSignal,
}

impl Drop for ReplyObserver<'_> {
    fn drop(&mut self) {
        // Close the receiver before scheduling reclamation. No command or
        // physical frame backing is released until its owner observes this.
        drop(self.received.take());
        self.changed.notify_changed();
    }
}

/// Shared broker connections. Opening another logical writer or looking up
/// another topic reuses these exact sockets and negotiated link sessions.
#[derive(Clone, Debug)]
pub struct BrokerLinks(Arc<Inner>);

impl BrokerLinks {
    /// Start on the owned SDK runtime. Socket creation does not await every
    /// broker's HELLO; one reachable broker can answer metadata immediately.
    pub async fn connect(
        runtime: &WriterRuntime,
        config: BrokerLinksConfig,
    ) -> Result<Self, BrokerLinkError> {
        Self::connect_with_ids(runtime, config, LinkIds::random(), LinkIds::random()).await
    }

    /// Inject distinct startup namespaces for link fences and request IDs.
    #[expect(
        clippy::too_many_lines,
        reason = "initialize both sockets and unwind partial owners together"
    )]
    pub async fn connect_with_ids(
        runtime: &WriterRuntime,
        mut config: BrokerLinksConfig,
        link_ids: LinkIds,
        request_ids: LinkIds,
    ) -> Result<Self, BrokerLinkError> {
        let slots = validate(&config)?;
        config.clock.bind_owner(runtime.driver().clone());
        let sdk = runtime.clone();
        runtime
            .driver()
            .spawn(async move {
                let shared = Arc::new(Shared {
                    local: config.local,
                    sessions: LinkSessions::with_ids(
                        config.local,
                        config.parameters,
                        handshake::OWNER,
                        config.brokers.len(),
                        link_ids,
                    )?,
                    stop: CloseSignal::default(),
                    changed: StateSignal::default(),
                    failure: OnceLock::new(),
                    request_ids,
                    routing: Mutex::new(routing::Registry::new(
                        config.maximum_partitions,
                        config.routing_bytes,
                    )),
                    appends: Arc::new(append::Registry::new(config.append)),
                    readers: readers::Registry::new(&config)?,
                });
                let mut peers = BTreeMap::new();
                let mut drivers = Vec::new();
                let mut publications = Vec::new();
                let mut publication_drivers = Vec::new();
                let message_bytes =
                    crate::transport::message_size_limit(config.parameters.receive.envelope)
                        .ok_or(BrokerLinkError::Configuration)?;
                let hwm = slots + config.append.map_or(0, |limits| limits.requests) + 4;
                let options = crate::transport::socket_options()
                    .identity(crate::transport::peer_identity(config.local))
                    .router_mandatory(true)
                    .send_hwm(hwm as u32)
                    .recv_hwm(hwm as u32)
                    .max_message_size(message_bytes)
                    .linger(Duration::from_millis(5));
                let socket = Arc::new(
                    sdk.context()
                        .socket(SocketType::Peer, options.clone())
                        .identity_routing()?,
                );
                let data = Arc::new(
                    sdk.context()
                        .socket(SocketType::Peer, options)
                        .identity_routing()?,
                );
                let monitor = socket.monitor();
                for (index, address) in config.brokers.iter().enumerate() {
                    let count = slots / config.brokers.len()
                        + usize::from(index < slots % config.brokers.len());
                    socket.connect(address.endpoint.clone()).await?;
                    data.connect(address.data_endpoint.clone()).await?;
                    let (sender, input) = mpsc::channel(count);
                    let peer = Arc::new(Peer {
                        data: data.clone(),
                        sender,
                        slots: Arc::new(Semaphore::new(count)),
                        closed: CloseSignal::default(),
                    });
                    drivers.push(driver::LinkState::new(
                        socket.clone(),
                        input,
                        peer.clone(),
                        shared.clone(),
                        address.node,
                        &config,
                    ));
                    peers.insert(address.node, peer);
                    if config.reader.is_some() {
                        let (link, driver) = publications::Driver::new(
                            sdk.context(),
                            shared.clone(),
                            address.node,
                            &config,
                        )?;
                        publications.push(link);
                        publication_drivers.push(driver);
                    }
                }
                let driver = driver::Driver::new(socket, data, monitor, drivers, shared.clone());
                let links = Self(Arc::new(Inner {
                    config,
                    peers,
                    publications,
                    shared,
                    runtime: sdk,
                }));
                tokio::spawn(driver.run());
                for driver in publication_drivers {
                    tokio::spawn(driver.run());
                }
                Ok::<_, BrokerLinkError>(links)
            })
            .await
            .map_err(|_| BrokerLinkError::Closed)?
    }

    /// Fixed physical socket count, independent of topics or partition count.
    pub fn socket_count(&self) -> usize {
        2 + self.0.publications.len()
    }

    /// Delay before a refused request is sent again.
    pub(in crate::replicated) fn retry_interval(&self) -> Duration {
        self.0.config.retry_interval
    }

    /// Current broker link fence. It grants no partition leadership authority.
    pub fn session(&self, broker: NodeId) -> Option<LinkSessionId> {
        self.0.shared.sessions.session(broker)
    }

    #[cfg(test)]
    pub(crate) fn control_capacity(&self, broker: NodeId) -> usize {
        self.0.peers[&broker].slots.available_permits()
    }

    /// Cache checked topic identity for lazy SDK routing interests. Other
    /// writers opening the same topic reuse its exact state and broker links.
    pub fn routes(&self, topic: TopicMetadata) -> Result<TopicRoutes, BrokerLinkError> {
        if self.0.shared.stop.is_closed() {
            return Err(BrokerLinkError::Closed);
        }
        if self.0.config.parameters.capabilities & handshake::OWNER_ROUTING == 0
            || topic.brokers().iter().any(|broker| {
                !self.0.config.brokers.iter().any(|address| {
                    address.node == broker.node && address.endpoint.to_string() == broker.peer
                })
            })
        {
            return Err(BrokerLinkError::Configuration);
        }
        let state = self
            .0
            .shared
            .routing
            .lock()
            .expect("SDK routing poisoned")
            .topic(topic)?;
        Ok(TopicRoutes::new(self.clone(), state))
    }

    pub(super) fn next_request(&self) -> Result<RequestId, BrokerLinkError> {
        self.0.shared.next_request()
    }

    pub(super) fn runtime(&self) -> &WriterRuntime {
        &self.0.runtime
    }
    pub(super) fn local(&self) -> NodeId {
        self.0.config.local
    }
    pub(super) fn clock(&self) -> SdkClock {
        self.0.config.clock.clone()
    }

    pub(super) async fn closed(&self) {
        self.0.shared.stop.closed().await;
    }

    pub(super) fn closed_error(&self) -> BrokerLinkError {
        self.0
            .shared
            .failure
            .get()
            .cloned()
            .map_or(BrokerLinkError::Closed, BrokerLinkError::Failed)
    }

    /// Open or fence one logical writer on the selected partition leader. On
    /// uncertainty, retry the exact supplied operation ID and expected epoch.
    pub async fn open_producer(
        &self,
        broker: NodeId,
        open: producer::Open,
        policy: Policy,
    ) -> Result<producer::Opened, BrokerLinkError> {
        let expected = match open.mode {
            producer::Mode::Resume => open.expected_epoch,
            producer::Mode::Create => Some(1),
            producer::Mode::Fence => Some(
                open.expected_epoch
                    .and_then(|epoch| epoch.checked_add(1))
                    .ok_or(BrokerLinkError::Configuration)?,
            ),
        };
        let message = self
            .request(broker, driver::Body::Open(open))
            .await?
            .message()?;
        let packet = driver::packet(&message, broker, self.0.config.parameters.receive.envelope)?;
        let opened = producer::decode_opened(packet, self.0.config.parameters.receive.envelope)?;
        if opened.authority.group_id != open.authority.group_id
            || opened.authority.config_epoch != open.authority.config_epoch
            || opened.authority.view < open.authority.view
            || opened.partition != open.partition
            || opened.producer != open.producer
            || opened.policy != policy
            || expected.is_some_and(|epoch| opened.epoch != epoch)
        {
            return Err(BrokerLinkError::Response);
        }
        Ok(opened)
    }

    async fn request(
        &self,
        broker: NodeId,
        body: driver::Body,
    ) -> Result<driver::Reply, BrokerLinkError> {
        let deadline = self
            .0
            .config
            .clock
            .now()
            .saturating_add(self.0.config.request_timeout);
        self.request_until(broker, body, deadline).await
    }

    async fn request_until(
        &self,
        broker: NodeId,
        body: driver::Body,
        deadline: Duration,
    ) -> Result<driver::Reply, BrokerLinkError> {
        let peer = self
            .0
            .peers
            .get(&broker)
            .ok_or(BrokerLinkError::Configuration)?;
        let scope = match &body {
            driver::Body::Unsubscribe { session, .. } => Some(*session),
            _ => None,
        };
        let work = async {
            let permit = peer
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| self.closed_error())?;
            let id = self.next_request()?;
            let (reply, received) = oneshot::channel();
            let mut observer = ReplyObserver {
                received: Some(received),
                changed: &self.0.shared.changed,
            };
            peer.sender
                .try_send(driver::Command {
                    body,
                    id,
                    deadline,
                    lease: Arc::new(driver::Lease { _permit: permit }),
                    reply,
                })
                .map_err(|_| self.closed_error())?;
            self.0.shared.changed.notify_changed();
            observer
                .received
                .as_mut()
                .expect("control reply observer")
                .await
                .map_err(|_| self.closed_error())?
        };
        tokio::select! {
            result = work => result,
            () = async {
                let Some(session) = scope else {
                    return std::future::pending::<()>().await;
                };
                loop {
                    let seen = self.0.shared.changed.generation();
                    if self.session(broker) != Some(session) {
                        return;
                    }
                    self.0.shared.changed.changed_after(seen).await;
                }
            } => Err(BrokerLinkError::Session),
            () = self.0.shared.stop.closed() => Err(self.closed_error()),
            () = peer.closed.closed() => Err(self.closed_error()),
            () = self.0.config.clock.until(deadline) => Err(BrokerLinkError::Timeout),
        }
    }

    /// Close all links and observe socket teardown on the owned SDK runtime.
    /// Cancellation only stops observing shutdown; its request stays active.
    pub async fn shutdown(&self) -> Result<(), BrokerLinkError> {
        self.0.shared.stop.close();
        for peer in self.0.peers.values() {
            peer.slots.close();
        }
        for peer in self.0.peers.values() {
            peer.closed.closed().await;
        }
        for link in &self.0.publications {
            link.closed.closed().await;
        }
        match self.0.shared.failure.get().cloned() {
            Some(reason) => Err(BrokerLinkError::Failed(reason)),
            None => Ok(()),
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shared.stop.close();
        for peer in self.peers.values() {
            peer.slots.close();
        }
    }
}

impl Shared {
    fn next_request(&self) -> Result<RequestId, BrokerLinkError> {
        Ok(RequestId::from_bytes(
            self.request_ids.next()?.to_be_bytes(),
        ))
    }
}

fn validate(config: &BrokerLinksConfig) -> Result<usize, BrokerLinkError> {
    config.parameters.validate()?;
    if let Some(append) = config.append {
        append.validate()?;
        if config.parameters.capabilities & handshake::OWNER_STREAM == 0
            || config.parameters.roles & handshake::PRODUCER == 0
        {
            return Err(BrokerLinkError::Configuration);
        }
    }
    let metadata = config.parameters.receive.envelope.max_metadata_bytes;
    let (scratch, slot_bytes) = control_sizes(config).ok_or(BrokerLinkError::Configuration)?;
    let slots = config
        .requests
        .min(config.control_bytes.saturating_sub(scratch) / slot_bytes);
    let mut unique = BTreeSet::new();
    if config.local.as_bytes() == &[0; 16]
        || !matches!(config.brokers.len(), 1 | 3)
        || config.requests > 4096
        || slots < config.brokers.len()
        || !(90..=65536).contains(&metadata)
        || !(1..=65536).contains(&config.maximum_partitions)
        || config.request_timeout.is_zero()
        || config.request_timeout > Duration::from_hours(24)
        || config.retry_interval.is_zero()
        || config.retry_interval > config.request_timeout
        || config.parameters.roles == 0
        || config.parameters.roles & !(handshake::PRODUCER | handshake::CONSUMER) != 0
        || config.brokers.iter().any(|broker| {
            broker.node == config.local
                || broker.node.as_bytes() == &[0; 16]
                || !unique.insert(broker.node)
                || !matches!(
                    broker.endpoint,
                    Endpoint::Tcp { .. } | Endpoint::Ipc(_) | Endpoint::Inproc { .. }
                )
                || broker.data_endpoint == broker.endpoint
                || !matches!(
                    broker.data_endpoint,
                    Endpoint::Tcp { .. } | Endpoint::Ipc(_) | Endpoint::Inproc { .. }
                )
        })
    {
        return Err(BrokerLinkError::Configuration);
    }
    readers::Registry::capacity(config)?;
    Ok(slots)
}

fn control_sizes(config: &BrokerLinksConfig) -> Option<(usize, usize)> {
    let envelope = config.parameters.receive.envelope;
    let scratch = envelope
        .max_metadata_bytes
        .checked_add(envelope.max_payload_bytes)?
        .checked_mul(4)?
        .checked_add(8192)?
        .checked_mul(config.brokers.len())?;
    let slot = envelope
        .max_metadata_bytes
        .checked_mul(2)?
        .checked_add(envelope.max_payload_bytes)?
        .checked_add(8192)?;
    Some((scratch, slot))
}

/// Link, bounded control, or typed broker response failed. An uncertain writer
/// open does not authorize a fresh operation ID or weaker confirmation policy.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BrokerLinkError {
    /// Missing or ambiguous record ID in the broker's retained history.
    #[error(transparent)]
    Seek(#[from] ozzy_core::reader::seek::SeekError),
    /// Requested offset predates the currently retained history.
    #[error("records expired; earliest retained offset is {earliest:?}")]
    RetentionGap {
        /// First currently retained record offset.
        earliest: ozzy_proto::Offset,
    },
    /// Invalid identity, profile, address set, or aggregate bounds.
    #[error("invalid SDK broker link configuration")]
    Configuration,
    /// SDK owner or selected broker link ended.
    #[error("SDK broker link closed")]
    Closed,
    /// Local admission, negotiation, or response exceeded the observed deadline.
    #[error("SDK broker request timed out")]
    Timeout,
    /// Broker replaced the link fence while a control request was outstanding.
    #[error("SDK broker link session changed")]
    Session,
    /// Response contradicts the request or trusted metadata.
    #[error("SDK broker response does not match request")]
    Response,
    /// Owned link failed during progress or teardown.
    #[error("SDK broker link failed: {0}")]
    Failed(String),
    /// Transport failed independently of canonical confirmation.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
    /// Link negotiation or injected identity generation failed.
    #[error(transparent)]
    Native(#[from] crate::Error),
    /// Invalid negotiated parameters.
    #[error(transparent)]
    Handshake(#[from] handshake::HandshakeError),
    /// Invalid native envelope.
    #[error(transparent)]
    Envelope(#[from] ozzy_proto::EnvelopeError),
    /// Control metadata codec failed.
    #[error(transparent)]
    Codec(#[from] ozzy_proto::data::CodecError),
    /// Negative reply metadata failed validation.
    #[error(transparent)]
    Nack(#[from] ozzy_proto::nack::NackError),
    /// Broker explicitly refused this attempt.
    #[error("SDK broker rejected request: {code}, {retry:?}")]
    Rejected {
        /// Protocol code.
        code: u16,
        /// Required retry behavior.
        retry: ozzy_proto::nack::RetryClass,
        /// Scoped routing hint. Validate it against trusted topic metadata.
        hint: Option<ozzy_proto::nack::AuthorityHint>,
    },
    /// Topic pages changed identity or left a gap.
    #[error(transparent)]
    Metadata(#[from] crate::topic_metadata::TopicMetadataError),
    /// Invalid identity or conflicting current routing hints.
    #[error(transparent)]
    Routing(#[from] crate::topic_metadata::RouteCacheError),
}
