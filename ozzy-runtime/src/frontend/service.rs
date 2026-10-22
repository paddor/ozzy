//! Shared link negotiation, routing, and bounded transport progress.

use std::{collections::BTreeMap, sync::Arc};

use arc_swap::ArcSwap;
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{NodeId, Opcode, data::DataLimits, handshake, nack};

use super::{
    Binding, CatalogError, Dispatcher, Kind, LinkIds, LinkSessions, ReplyError, SetupError,
    TopicCatalog, WatchRegistry,
};
use crate::dispatch::Class;
use crate::signal::StateSignal;

mod admission;
mod directory;
mod links;
use links::LinkState;
pub use links::{Access, Link, Links};
mod progress;
mod receive;

const BROKER_ROLE: u32 = 1 << 3;

#[derive(Debug)]
struct Peer {
    access: Access,
    trusted: bool,
    initiating: bool,
    handshake: Option<Message>,
    awaiting_welcome: bool,
    link: Option<Link>,
}

impl Peer {
    const fn new(access: Access, trusted: bool) -> Self {
        Self {
            access,
            trusted,
            initiating: false,
            handshake: None,
            awaiting_welcome: false,
            link: None,
        }
    }

    /// Returns whether the established link changed.
    fn replace_link(&mut self, link: Option<Link>) -> bool {
        let changed = self.link != link;
        self.link = link;
        changed
    }
}

/// Dispatcher-thread service shared by broker transports and deterministic
/// harnesses. Owns no partition state or payload validation. Each authorized
/// peer has one replaceable handshake reply and independent bounded outboxes.
#[derive(Debug)]
pub struct Service {
    pub(super) dispatcher: Dispatcher,
    sessions: LinkSessions,
    parameters: handshake::Parameters,
    peers: BTreeMap<NodeId, Peer>,
    links: Links,
    pub(super) watches: Option<WatchRegistry>,
    catalog: Option<TopicCatalog>,
    handshake_turn: Option<NodeId>,
    handshake_first: bool,
    trusted_maximum: usize,
    pub(super) ports: BTreeMap<u32, super::port::Mailbox>,
    pub(super) port_turn: Option<u32>,
    pub(super) publications: [Option<(u32, Message)>; 2],
    pub(super) publication_next: usize,
    /// One send-progress wait per refused destination class, kept across
    /// progress calls. Building OMQ waits is too costly to repeat each turn.
    pub(super) reply_waits: progress::ReplyWaits,
    /// Per shard and destination class, the lowest reply sequence refused for
    /// capacity and not yet re-admitted. Later sequences wait behind it.
    pub(super) refused_replies: BTreeMap<(u32, NodeId, Class), u64>,
}

impl Service {
    /// Construct on the dispatcher thread, before binding any peer. The fixed
    /// authorization table and dispatcher limits bound all link metadata.
    pub fn new(
        dispatcher: Dispatcher,
        parameters: handshake::Parameters,
        access: &[Access],
        ids: LinkIds,
    ) -> Result<Self, ServiceError> {
        Self::new_with_broker_limits(dispatcher, parameters, parameters.receive, access, ids)
    }

    /// Advertise canonical transfer bounds to independently configured brokers.
    /// Clients retain the default native record profile. Record bounds carry
    /// neither broker authorization nor partition authority.
    pub fn new_with_broker_limits(
        dispatcher: Dispatcher,
        parameters: handshake::Parameters,
        broker_receive: DataLimits,
        access: &[Access],
        ids: LinkIds,
    ) -> Result<Self, ServiceError> {
        if !dispatcher.peers.is_empty() || access.len() > dispatcher.limits.peers {
            return Err(ServiceError::Access);
        }
        let mut peers = BTreeMap::new();
        for &access in access {
            if access.peer == dispatcher.local
                || access.peer.as_bytes() == &[0; 16]
                || peers
                    .insert(access.peer, Peer::new(access, false))
                    .is_some()
            {
                return Err(ServiceError::Access);
            }
        }
        let profiles = access
            .iter()
            .filter(|access| access.kind == Kind::Broker && broker_receive != parameters.receive)
            .map(|access| (access.peer, broker_receive))
            .collect::<Vec<_>>();
        let sessions = LinkSessions::with_ids(
            dispatcher.local,
            parameters,
            0,
            dispatcher.limits.peers,
            ids,
        )?
        .with_receive_profiles(&profiles)?;
        let links = Links(Arc::new(LinkState {
            current: ArcSwap::from_pointee(peers.keys().map(|&peer| (peer, None)).collect()),
            changed: StateSignal::default(),
        }));
        Ok(Self {
            dispatcher,
            sessions,
            parameters,
            peers,
            links,
            watches: None,
            catalog: None,
            handshake_turn: None,
            handshake_first: true,
            trusted_maximum: 0,
            ports: BTreeMap::new(),
            port_turn: None,
            reply_waits: progress::ReplyWaits::default(),
            refused_replies: BTreeMap::new(),
            publications: [None, None],
            publication_next: 0,
        })
    }

    /// Read-only observations for every application shard and service.
    pub fn links(&self) -> Links {
        self.links.clone()
    }

    /// Only the dispatcher writes link state. Shards read immutable snapshots.
    fn set_link(&mut self, peer: NodeId, link: Option<Link>) {
        if self
            .peers
            .get_mut(&peer)
            .is_some_and(|state| state.replace_link(link))
        {
            self.publish_links();
        }
    }

    fn publish_links(&self) {
        let current = self.peers.iter().map(|(&id, state)| (id, state.link));
        self.links.0.current.store(Arc::new(current.collect()));
        self.links.0.changed.notify_changed();
    }

    /// Local identity of the single bound broker socket.
    pub fn local(&self) -> NodeId {
        self.dispatcher.local
    }

    /// Independently configured access, not a role claimed by a remote HELLO.
    pub fn access(&self, peer: NodeId) -> Option<Access> {
        self.peers.get(&peer).map(|state| state.access)
    }

    /// Install configured routing interests before accepting connections.
    /// The maximum snapshot must fit one reserved control reply; larger topic
    /// sets use multiple bounded registrations. No partition actor moves here.
    pub fn install_watches(&mut self, watches: WatchRegistry) -> Result<(), ServiceError> {
        let limits = watches.limits();
        let maximum = ozzy_proto::directory::Limits::default()
            .maximum_snapshot_bytes(limits.interests_per_registration)
            .and_then(|size| size.checked_add(80));
        if self.watches.is_some()
            || self.peers.values().any(|peer| peer.link.is_some())
            || limits.peers
                < self
                    .peers
                    .values()
                    .filter(|peer| peer.access.kind == Kind::Client)
                    .count()
            || maximum
                .is_none_or(|size| size > self.dispatcher.limits.replies.control.message_bytes)
        {
            return Err(ServiceError::Watch);
        }
        self.watches = Some(watches);
        self.parameters.capabilities |= handshake::OWNER_ROUTING;
        Ok(())
    }

    /// Install checked topic identity before client links are established.
    /// One partition plus explicit broker endpoints must fit a control reply.
    pub fn install_catalog(&mut self, catalog: TopicCatalog) -> Result<(), ServiceError> {
        if self.catalog.is_some()
            || self.peers.values().any(|peer| peer.link.is_some())
            || catalog
                .largest_single_bytes()
                .checked_add(80)
                .is_none_or(|bytes| bytes > self.dispatcher.limits.replies.control.message_bytes)
        {
            return Err(ServiceError::Catalog);
        }
        self.catalog = Some(catalog);
        Ok(())
    }

    /// Begin a fresh attempt, or retry an unfinished one. Starting after a live
    /// session fences its replies and shard observation immediately.
    /// No connection to another broker is required to finish this negotiation.
    pub fn start(&mut self, peer: NodeId) -> Result<(), ServiceError> {
        let state = self.peers.get_mut(&peer).ok_or(ServiceError::Access)?;
        if let (Some(watches), Some(link)) = (&mut self.watches, state.link) {
            watches.disconnect(peer, link.binding.session);
        }
        let frames = self.sessions.start(peer)?;
        self.dispatcher.disconnect(peer);
        state.initiating = true;
        state.awaiting_welcome = false;
        state.handshake = Some(routed(peer, frames));
        if state.replace_link(None) {
            self.publish_links();
        }
        Ok(())
    }

    /// Timer-driven retry of an initiated, unfinished attempt. This preserves
    /// its HELLO identity and never renegotiates a healthy link.
    pub fn retry(&mut self, peer: NodeId) -> Result<(), ServiceError> {
        self.peers.get(&peer).ok_or(ServiceError::Access)?;
        if self.retrying(peer) {
            self.start(peer)?;
        }
        Ok(())
    }

    /// Whether `peer` has an initiated, unfinished attempt for [`Self::retry`].
    pub fn retrying(&self, peer: NodeId) -> bool {
        self.peers
            .get(&peer)
            .is_some_and(|state| state.initiating && self.sessions.session(peer).is_none())
    }

    /// Fence an independently observed disconnect. A late disconnect cannot
    /// invalidate a replacement session. Reconnect requires an explicit start
    /// or a fresh remote HELLO; old queued frames remain charged until released.
    pub fn disconnect(&mut self, binding: Binding) -> bool {
        let Some(state) = self
            .peers
            .get_mut(&binding.peer)
            .filter(|state| state.link.map(|link| link.binding) == Some(binding))
        else {
            return false;
        };
        self.dispatcher.disconnect(binding.peer);
        if let Some(watches) = &mut self.watches {
            watches.disconnect(binding.peer, binding.session);
        }
        self.sessions.disconnect(binding.peer);
        state.handshake = None;
        state.initiating = false;
        state.awaiting_welcome = false;
        state.link = None;
        self.publish_links();
        true
    }

    /// Broker-local maximum native framing, independent of remote negotiation.
    pub fn envelope_limits(&self) -> ozzy_proto::EnvelopeLimits {
        self.dispatcher.routes.limits
    }

    /// Observe producer queue space after a failed enqueue without holding
    /// the service across the wait.
    pub fn data_space_changed_after(
        &self,
        shard: u32,
        kind: Kind,
        class: Class,
        generation: u64,
    ) -> Option<impl std::future::Future<Output = ()> + use<>> {
        self.dispatcher
            .data_space_changed_after(shard, kind, class, generation)
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        for peer in std::mem::take(&mut self.peers).into_keys() {
            self.dispatcher.disconnect(peer);
            self.set_link(peer, None);
        }
    }
}

/// Destination identity in a routed message's first frame.
pub(super) fn route_peer(message: &Message) -> Option<NodeId> {
    Some(NodeId::from_bytes(message.part_slice(0)?.try_into().ok()?))
}

fn routed(peer: NodeId, frames: Vec<Bytes>) -> Message {
    Message::with_prefix(
        Bytes::copy_from_slice(peer.as_bytes()),
        Message::multipart(frames),
    )
}

/// Invalid setup or failed local negotiation operation.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Duplicate, foreign, excessive, or invalid authorized identities.
    #[error("invalid frontend authorized peers")]
    Access,
    /// Topic metadata cannot fit one reserved control reply or was installed late.
    #[error("invalid frontend topic catalog setup")]
    Catalog,
    /// Watch table cannot fit the authorized clients or one bounded reply.
    #[error("invalid frontend routing watch setup")]
    Watch,
    /// Profile or session ID source failure.
    #[error(transparent)]
    Session(#[from] crate::Error),
}

/// Unaccepted input. A malformed peer or full queue need not stop healthy
/// destinations. Setup failures and dispatch accounting invariants are fatal.
#[derive(Debug, thiserror::Error)]
pub enum ReceiveError {
    /// Unknown transport identity or invalid routed frame count.
    #[error("unknown or invalid frontend transport peer")]
    Peer,
    /// Claimed roles conflict with the independent authorization table.
    #[error("handshake role differs from authorized frontend profile")]
    Role,
    /// Native framing is malformed or exceeds local receive limits.
    #[error(transparent)]
    Envelope(#[from] ozzy_proto::EnvelopeError),
    /// Invalid HELLO/WELCOME properties.
    #[error(transparent)]
    Handshake(#[from] handshake::HandshakeError),
    /// Invalid routing-interest command metadata.
    #[error(transparent)]
    Directory(#[from] ozzy_proto::data::CodecError),
    /// Negative directory reply could not fit negotiated bounds.
    #[error(transparent)]
    Nack(#[from] nack::NackError),
    /// Requested topic or page is unavailable under the checked deployment.
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    /// Routing-interest registration, identity, or session refused.
    #[error(transparent)]
    Watch(#[from] super::WatchError),
    /// Local reply configuration cannot hold the generated snapshot.
    #[error(transparent)]
    DirectoryReply(ReplyError),
    /// Session negotiation failed before dispatch admission.
    #[error(transparent)]
    Session(#[from] crate::Error),
    /// Fatal failure installing newly negotiated routing state.
    #[error(transparent)]
    Setup(#[from] SetupError),
    /// Routing or bounded shard admission refused the packet.
    #[error(transparent)]
    Dispatch(super::Rejection),
}

#[cfg(test)]
pub(super) mod tests;
