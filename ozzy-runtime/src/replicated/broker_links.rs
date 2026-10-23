//! Two SDK PEER sockets across all brokers: control/replies and data/replay.

use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
    time::Duration,
};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use omq_tokio::IdentitySocket;
use ozzy_proto::{LinkSessionId, NodeId, RequestId, append::Policy, group, handshake, producer};
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::WriterRuntime;
use crate::{
    frontend::{LinkIds, LinkSessions},
    peer_sessions::Established,
    signal::{CloseSignal, StateSignal},
    topic_metadata::TopicMetadata,
};

mod clock;
mod config;
use config::validate;
pub use config::{BrokerAddress, BrokerLinksConfig};
mod groups;
mod startup;
pub(in crate::replicated) use clock::OwnerClock;
pub use clock::{ClockError, SdkClock};
use startup::Startup;
pub(super) mod append;
pub use append::AppendLinkLimits;
mod driver;
mod metadata;
mod publications;
pub(in crate::replicated) mod readers;
pub use readers::ReaderLinkLimits;
mod routing;
#[cfg(test)]
mod tests;
pub(in crate::replicated) use routing::LeaderRoute;
pub use routing::{RouteGeneration, TopicRoutes};

#[derive(Debug)]
struct Peer {
    data: Arc<IdentitySocket>,
    sender: mpsc::Sender<driver::Command>,
    slots: Arc<Semaphore>,
    /// Rung when a command is queued or its caller stops observing the
    /// reply. Only the driver waits on it.
    doorbell: StateSignal,
    closed: CloseSignal,
    /// Live link, installed by the driver after it fences the one replaced.
    link: ArcSwapOption<Established>,
}

impl Peer {
    /// The live link as the driver last installed it.
    fn link(&self) -> Option<Established> {
        self.link.load().as_deref().copied()
    }

    /// Publish `link` to callers. Only the driver installs links.
    fn install(&self, link: Option<Established>) {
        if self.link() != link {
            self.link.store(link.map(Arc::new));
        }
    }
}

#[derive(Debug)]
struct Shared {
    local: NodeId,
    stop: CloseSignal,
    /// A live link was installed or fenced, or the driver stopped.
    changed: StateSignal,
    /// A partition's routing interest was raised or refreshed.
    interests: StateSignal,
    failure: OnceLock<String>,
    request_ids: LinkIds,
    appends: Arc<append::Registry>,
    readers: readers::Admission,
    clock: OwnerClock,
    /// Successful control replies handed to their observers.
    #[cfg(test)]
    delivered: std::sync::atomic::AtomicUsize,
}

#[derive(Debug)]
struct Inner {
    config: BrokerLinksConfig,
    peers: BTreeMap<NodeId, Arc<Peer>>,
    /// Lane to the driver, which owns the topic registry.
    registrations: mpsc::Sender<driver::Registration>,
    /// Lane to the driver for new reader inboxes.
    inboxes: mpsc::Sender<readers::Registration>,
    publications: Vec<Arc<publications::Link>>,
    shared: Arc<Shared>,
    runtime: WriterRuntime,
}

struct ReplyObserver<'a> {
    received: Option<oneshot::Receiver<Result<driver::Reply, BrokerLinkError>>>,
    doorbell: &'a StateSignal,
}

impl Drop for ReplyObserver<'_> {
    fn drop(&mut self) {
        // Close the receiver before scheduling reclamation. No command or
        // physical frame backing is released until its owner observes this.
        drop(self.received.take());
        self.doorbell.notify_changed();
    }
}

/// Shared broker connections. Opening another logical writer or looking up
/// another topic reuses these exact sockets and negotiated link sessions.
#[derive(Clone, Debug)]
pub struct BrokerLinks(Arc<Inner>);

impl Shared {
    fn new(
        config: &BrokerLinksConfig,
        owner: tokio::runtime::Handle,
        request_ids: LinkIds,
    ) -> Result<Self, BrokerLinkError> {
        Ok(Self {
            local: config.local,
            stop: CloseSignal::default(),
            changed: StateSignal::default(),
            interests: StateSignal::default(),
            failure: OnceLock::new(),
            request_ids,
            appends: Arc::new(append::Registry::new(config.append)),
            readers: readers::Admission::new(config)?,
            clock: config.clock.bind(owner),
            #[cfg(test)]
            delivered: std::sync::atomic::AtomicUsize::new(0),
        })
    }
}

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
    pub async fn connect_with_ids(
        runtime: &WriterRuntime,
        config: BrokerLinksConfig,
        link_ids: LinkIds,
        request_ids: LinkIds,
    ) -> Result<Self, BrokerLinkError> {
        let slots = validate(&config)?;
        let sdk = runtime.clone();
        runtime
            .on_owner(move || async move {
                let (startup, sessions) = Startup::open(sdk, config, slots, link_ids, request_ids)?;
                let brokers = startup.connect(slots).await?;
                startup.spawn(sessions, brokers)
            })
            .await
            .ok_or(BrokerLinkError::Closed)?
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
        self.link(broker).map(|link| link.session)
    }

    /// Live link to `broker`: its session fence, negotiated parameters, and
    /// send limits, published only after the replaced link was fenced.
    pub(in crate::replicated) fn link(&self, broker: NodeId) -> Option<Established> {
        self.0.peers.get(&broker)?.link()
    }

    #[cfg(test)]
    pub(crate) fn control_capacity(&self, broker: NodeId) -> usize {
        self.0.peers[&broker].slots.available_permits()
    }

    /// Successful control replies delivered to their observers so far.
    #[cfg(test)]
    pub(crate) fn delivered_replies(&self) -> usize {
        self.0
            .shared
            .delivered
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Cache checked topic identity for lazy SDK routing interests. Other
    /// writers opening the same topic reuse its exact state and broker links.
    /// The SDK owner registers the topic; this waits for it.
    pub async fn routes(&self, topic: TopicMetadata) -> Result<TopicRoutes, BrokerLinkError> {
        self.register_topic(topic, false).await
    }

    /// [`Self::routes`], after installing the topic's reader endpoints.
    pub(in crate::replicated) async fn reader_routes(
        &self,
        topic: TopicMetadata,
    ) -> Result<TopicRoutes, BrokerLinkError> {
        self.register_topic(topic, true).await
    }

    async fn register_topic(
        &self,
        topic: TopicMetadata,
        readers: bool,
    ) -> Result<TopicRoutes, BrokerLinkError> {
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
        let (reply, received) = oneshot::channel();
        self.0
            .registrations
            .send(driver::Registration::Topic {
                topic,
                readers,
                reply,
            })
            .await
            .map_err(|_| self.closed_error())?;
        let state = received.await.map_err(|_| self.closed_error())??;
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
    pub(super) fn clock(&self) -> OwnerClock {
        self.0.shared.clock.clone()
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
                doorbell: &peer.doorbell,
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
            peer.doorbell.notify_changed();
            let Some(received) = observer.received.as_mut() else {
                return Err(self.closed_error());
            };
            received.await.map_err(|_| self.closed_error())?
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
            () = self.0.shared.clock.until(deadline) => Err(BrokerLinkError::Timeout),
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

/// Link, bounded control, or typed broker response failed. An uncertain writer
/// open does not authorize a fresh operation ID or weaker confirmation policy.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BrokerLinkError {
    /// Durable processing assignment was fenced or its resume history expired.
    #[error(transparent)]
    Progress(#[from] super::ProgressError),
    /// Missing or ambiguous record ID in the broker's retained history.
    #[error(transparent)]
    Seek(#[from] ozzy_core::reader::seek::SeekError),
    /// Requested offset predates the currently retained history.
    #[error("records expired; earliest retained offset is {earliest:?}")]
    RetentionGap {
        /// First currently retained record offset.
        earliest: ozzy_proto::Offset,
    },
    /// A group creation offset lies outside the source's confirmed retained history.
    #[error("group start {0:?} is outside confirmed retained history")]
    InitialPosition(group::InitialPosition),
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
