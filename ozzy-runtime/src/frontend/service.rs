//! Shared link negotiation, routing, and bounded transport progress.

use crate::transport::Frames;
use std::{collections::BTreeMap, sync::Arc};

use arc_swap::ArcSwap;
use bytes::Bytes;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{NodeId, Opcode, data::DataLimits, decode_packet, handshake, nack};

use super::{
    Binding, CatalogError, Dispatcher, Kind, LinkIds, LinkSessions, ReplyError, Routed, SetupError,
    TopicCatalog, WatchRegistry,
};
use crate::dispatch::Class;
use crate::replica_transport::SendAttempt;
use crate::signal::StateSignal;

mod admission;
mod directory;
mod links;
use links::LinkState;
pub use links::{Access, Link, Links};
mod progress;

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
        let state = self.peers.get(&peer).ok_or(ServiceError::Access)?;
        if state.initiating && self.sessions.session(peer).is_none() {
            self.start(peer)?;
        }
        Ok(())
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

    /// Lossy group publication from an independently configured SUB connection.
    /// Full shard capacity discards this attempt; accepted-tail probes repair it.
    pub fn receive_publication(
        &mut self,
        publisher: NodeId,
        message: &Message,
        retained_bytes: usize,
    ) -> Result<Option<Routed>, ReceiveError> {
        let Some((route, frames)) = message.routed_packet() else {
            return Err(ReceiveError::Peer);
        };
        if self
            .links
            .get(publisher)
            .is_none_or(|link| link.binding.kind != Kind::Broker)
        {
            return Err(ReceiveError::Peer);
        }
        let packet = decode_packet(&frames, self.dispatcher.routes.limits)?;
        let scope = ozzy_replication::wire::route(packet, self.dispatcher.routes.limits).map_err(
            |error| {
                ReceiveError::Dispatch(super::Rejection::Routing(super::RoutingError::Broker(
                    error,
                )))
            },
        )?;
        if packet.envelope.opcode != Opcode::PreparePub
            || route != scope.group_id.as_bytes()
            || packet.envelope.sender != publisher
        {
            return Err(ReceiveError::Peer);
        }
        let (_, packet_frames) = message.routed_packet_bytes().ok_or(ReceiveError::Peer)?;
        let normalized = Message::multipart(
            std::iter::once(Bytes::copy_from_slice(publisher.as_bytes())).chain(packet_frames),
        );
        self.dispatcher
            .dispatch_data(publisher, normalized, retained_bytes)
            .map(Some)
            .map_err(|rejected| {
                if matches!(
                    rejected.reason,
                    super::Rejection::Data(super::DataPressure::Full { .. })
                ) {
                    crate::profiling::event(crate::profiling::Event::ReplicaPublicationQueueDrop);
                }
                ReceiveError::Dispatch(rejected.reason)
            })
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

    /// Receive one PEER frame. Full admission returns pressure without a refusal.
    /// The transport owner restores its original frame to the same receive source.
    /// Retained bytes cover backing storage, not just visible payload length.
    pub fn receive(
        &mut self,
        message: Message,
        retained_bytes: usize,
    ) -> Result<Option<Routed>, ReceiveError> {
        if message.len() == 2 {
            return self.receive_compact(&message, retained_bytes);
        }
        let Some((route, frames)) = message.routed_packet() else {
            return Err(ReceiveError::Peer);
        };
        let peer = NodeId::from_bytes(route.try_into().map_err(|_| ReceiveError::Peer)?);
        let access = self.peers.get(&peer).map(|state| state.access);
        if access.is_none() && self.trusted_maximum == 0 {
            return Err(ReceiveError::Peer);
        }
        let envelope = self.sessions.receive_limits(peer).envelope;
        let packet = decode_packet(&frames, envelope)?;
        if packet.envelope.opcode == Opcode::PreparePub {
            return Err(ReceiveError::Peer);
        }
        let access = match access {
            Some(access) => access,
            None => self.admit_trusted_client(peer, packet)?,
        };
        if matches!(packet.envelope.opcode, Opcode::Hello | Opcode::Welcome) {
            self.receive_handshake(peer, access.kind, packet, envelope)?;
            Ok(None)
        } else if packet.envelope.opcode == Opcode::StateSnapshotRequest {
            match packet.metadata.first() {
                Some(0) => self.receive_topic(peer, packet)?,
                Some(1) => self.receive_watch(peer, packet)?,
                _ => {
                    return Err(ReceiveError::Directory(
                        ozzy_proto::data::CodecError::Profile,
                    ));
                }
            }
            Ok(None)
        } else {
            if packet.envelope.opcode == Opcode::ReplicaState {
                self.bind_incoming_channel(peer, packet, envelope)?;
            }
            self.dispatcher
                .dispatch_data(peer, message, retained_bytes)
                .map(Some)
                .map_err(|rejected| ReceiveError::Dispatch(rejected.reason))
        }
    }

    /// Nonblocking per-peer reply admission under negotiated directional limits.
    pub fn try_reply(
        &mut self,
        class: Class,
        message: Message,
    ) -> Result<(), (ReplyError, Message)> {
        if let Err(error) = self.check_reply(&message) {
            return Err((error, message));
        }
        self.dispatcher.try_reply(class, message)
    }

    fn check_reply(&self, message: &Message) -> Result<(), ReplyError> {
        if message.len() == 2 {
            return Ok(());
        }
        let (route, [_, metadata, payload]) = message.routed_packet().ok_or(ReplyError::Frames)?;
        let peer = NodeId::from_bytes(route.try_into().map_err(|_| ReplyError::Peer)?);
        let limits = self.links.get(peer).ok_or(ReplyError::Peer)?.send.envelope;
        if metadata.len() > limits.max_metadata_bytes || payload.len() > limits.max_payload_bytes {
            return Err(ReplyError::Size);
        }
        Ok(())
    }

    /// Whether any handshake or established-link reply still needs transport.
    pub fn has_pending(&self) -> bool {
        self.dispatcher.has_replies() || self.peers.values().any(|peer| peer.handshake.is_some())
    }

    /// At most one handshake and one peer's two reply classes per turn. Full
    /// destinations retain their owners and never block another destination.
    pub fn flush(
        &mut self,
        mut send: impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<bool, omq_tokio::Error> {
        let mut advanced = false;
        for handshake in [self.handshake_first, !self.handshake_first] {
            let progress = if handshake {
                self.flush_handshake(&mut send)?
            } else {
                let peers = &self.peers;
                let progress = self.dispatcher.flush_replies(|message| {
                    let awaiting = route_peer(&message)
                        .and_then(|peer| peers.get(&peer))
                        .is_some_and(|state| state.awaiting_welcome);
                    if awaiting {
                        Err(TrySendError::Full(message))
                    } else {
                        send(message)
                    }
                })?;
                [progress.control, progress.data]
                    .into_iter()
                    .any(|attempt| {
                        matches!(attempt, SendAttempt::Submitted | SendAttempt::Unroutable)
                    })
            };
            if progress {
                self.handshake_first = !handshake;
                advanced = true;
            }
        }
        Ok(advanced)
    }

    fn flush_handshake(
        &mut self,
        send: &mut impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<bool, omq_tokio::Error> {
        let mut advanced = false;
        if let Some((next, state)) = crate::turn::after_mut(&mut self.peers, self.handshake_turn) {
            self.handshake_turn = Some(next);
            if let Some(message) = state.handshake.take() {
                match send(message) {
                    Ok(()) => {
                        state.awaiting_welcome = false;
                        // Replies refused while the handshake was unsent were
                        // not refused by OMQ.
                        self.dispatcher.unblock(next, 0);
                        self.dispatcher.unblock(next, 1);
                        advanced = true;
                    }
                    Err(TrySendError::Error(omq_tokio::Error::Unroutable)) => advanced = true,
                    Err(TrySendError::Full(message)) => state.handshake = Some(message),
                    Err(TrySendError::Closed) => return Err(omq_tokio::Error::Closed),
                    Err(TrySendError::Error(error)) => return Err(error),
                }
            }
        }
        Ok(advanced)
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
