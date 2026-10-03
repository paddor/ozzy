//! Shared link negotiation, routing, and bounded transport progress.

use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

use arc_swap::ArcSwap;
use bytes::Bytes;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{
    Envelope, NodeId, Opcode, data::DataLimits, decode_packet, directory, handshake, nack,
};

use super::{
    Binding, CatalogError, Dispatcher, GrantSpec, Kind, LinkIds, LinkSessions, ReplyError,
    RouteState, Routed, SetupError, TopicCatalog, WatchRegistry,
};
use crate::dispatch::{Class, Grant};
use crate::replica_transport::SendAttempt;
use crate::signal::StateSignal;

mod admission;
mod progress;

const BROKER_ROLE: u32 = 1 << 3;

/// Transport identity authorized by an adapter or explicit trust policy.
/// HELLO role claims never grant broker membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Access {
    /// Identity from the authenticated adapter or explicit trust configuration.
    pub peer: NodeId,
    /// Authorized protocol role, independent of the peer's claimed role.
    pub kind: Kind,
}

/// Established link observation, with directional limits negotiated separately
/// from shard-owned admission credit. This grants no partition authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Link {
    /// Current peer, role, and session fence.
    pub binding: Binding,
    /// Common outbound codec bounds.
    pub send: DataLimits,
    /// Selected capabilities and remote receive windows.
    pub remote: handshake::Parameters,
}

/// Read-only coalesced link state for application shards. Replacement first
/// fences dispatcher grants/replies, then publishes this observation. A shard
/// checks the current session before delivering already queued work to actors.
#[derive(Clone, Debug)]
pub struct Links(Arc<LinkState>);

#[derive(Debug)]
struct LinkState {
    current: ArcSwap<BTreeMap<NodeId, Option<Link>>>,
    changed: StateSignal,
}

impl Links {
    /// Latest established session, absent before negotiation or after closure.
    pub fn get(&self, peer: NodeId) -> Option<Link> {
        self.0.current.load().get(&peer).copied().flatten()
    }

    /// Capture before inspecting links, then wait for a subsequent change.
    pub fn generation(&self) -> u64 {
        self.0.changed.generation()
    }

    /// No event backlog: reconnecting shards inspect the newest session. Taking
    /// a generation before inspection prevents a lost snapshot/update race.
    pub async fn changed_after(&self, generation: u64) {
        self.0.changed.changed_after(generation).await;
    }
}

#[derive(Debug)]
struct Peer {
    access: Access,
    initiating: bool,
    handshake: Option<Message>,
    awaiting_welcome: bool,
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
    link_current: BTreeMap<NodeId, Option<Link>>,
    links: Links,
    pub(super) watches: Option<WatchRegistry>,
    catalog: Option<TopicCatalog>,
    handshake_turn: Option<NodeId>,
    handshake_first: bool,
    trusted_maximum: usize,
    pub(super) requests: BTreeMap<u32, super::demand::Requests>,
    pub(super) ports: BTreeMap<u32, super::port::Mailbox>,
    pub(super) port_turn: Option<u32>,
    pub(super) publication: Option<(u32, Message)>,
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
    /// Clients retain the default native record profile. Neither profile grants
    /// admission credit, broker authorization, or partition authority.
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
                    .insert(
                        access.peer,
                        Peer {
                            access,
                            initiating: false,
                            handshake: None,
                            awaiting_welcome: false,
                        },
                    )
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
        let sessions = LinkSessions::with_receive_profiles(
            dispatcher.local,
            parameters,
            0,
            dispatcher.limits.peers,
            ids,
            &profiles,
        )?;
        let link_current: BTreeMap<_, _> = peers.keys().map(|&peer| (peer, None)).collect();
        let links = Links(Arc::new(LinkState {
            current: ArcSwap::from_pointee(link_current.clone()),
            changed: StateSignal::default(),
        }));
        Ok(Self {
            dispatcher,
            sessions,
            parameters,
            peers,
            link_current,
            links,
            watches: None,
            catalog: None,
            handshake_turn: None,
            handshake_first: true,
            trusted_maximum: 0,
            requests: BTreeMap::new(),
            ports: BTreeMap::new(),
            port_turn: None,
            publication: None,
        })
    }

    /// Read-only observations for every application shard and service.
    pub fn links(&self) -> Links {
        self.links.clone()
    }

    /// Only the dispatcher writes link state. Shards read immutable snapshots.
    fn set_link(&mut self, peer: NodeId, link: Option<Link>) {
        let current = self.link_current.get_mut(&peer).expect("authorized peer");
        if *current != link {
            *current = link;
            self.links
                .0
                .current
                .store(Arc::new(self.link_current.clone()));
            self.links.0.changed.notify_changed();
        }
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
        let maximum = directory::Limits::default()
            .maximum_snapshot_bytes(limits.interests_per_registration)
            .and_then(|size| size.checked_add(80));
        if self.watches.is_some()
            || self.link_current.values().any(Option::is_some)
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
            || self.link_current.values().any(Option::is_some)
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
    /// session fences its grants, replies, and shard observation immediately.
    /// No connection to another broker is required to finish this negotiation.
    pub fn start(&mut self, peer: NodeId) -> Result<(), ServiceError> {
        if !self.peers.contains_key(&peer) {
            return Err(ServiceError::Access);
        }
        if let (Some(watches), Some(link)) = (&mut self.watches, self.links.get(peer)) {
            watches.disconnect(peer, link.binding.session);
        }
        let frames = self.sessions.start(peer)?;
        self.fence_requests(peer);
        self.dispatcher.disconnect(peer);
        self.set_link(peer, None);
        let state = self.peers.get_mut(&peer).expect("authorized peer");
        state.initiating = true;
        state.awaiting_welcome = false;
        state.handshake = Some(routed(peer, frames));
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
        if self.links.get(binding.peer).map(|link| link.binding) != Some(binding) {
            return false;
        }
        self.fence_requests(binding.peer);
        self.dispatcher.disconnect(binding.peer);
        if let Some(watches) = &mut self.watches {
            watches.disconnect(binding.peer, binding.session);
        }
        self.sessions.disconnect(binding.peer);
        let state = self.peers.get_mut(&binding.peer).expect("authorized peer");
        state.handshake = None;
        state.initiating = false;
        state.awaiting_welcome = false;
        self.set_link(binding.peer, None);
        true
    }

    /// One ordinary PEER receive. `retained_bytes` comes from the transport's
    /// conservative allocation bound, not visible payload length. Handshakes
    /// retain only freshly encoded bounded metadata. Refused input is dropped.
    pub fn receive(
        &mut self,
        message: Message,
        retained_bytes: usize,
    ) -> Result<Option<Routed>, ReceiveError> {
        if message.len() == 2 {
            return self.receive_compact(&message, retained_bytes);
        }
        if message.len() != 4 {
            return Err(ReceiveError::Peer);
        }
        let peer = NodeId::from_bytes(
            message
                .part_slice(0)
                .and_then(|p| p.try_into().ok())
                .ok_or(ReceiveError::Peer)?,
        );
        let access = self.peers.get(&peer).map(|state| state.access);
        if access.is_none() && self.trusted_maximum == 0 {
            return Err(ReceiveError::Peer);
        }
        let frames =
            std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).expect("four frames"));
        let envelope = self.sessions.receive_limits(peer).envelope;
        let packet = decode_packet(&frames, envelope)?;
        let access = match access {
            Some(access) => access,
            None => self.admit_trusted_client(peer, packet)?,
        };
        if matches!(packet.envelope.opcode, Opcode::Hello | Opcode::Welcome) {
            let hello = handshake::decode(packet, envelope)?;
            // These bits constrain the authorized profile. They do not authorize
            // a routing identity or establish replication membership.
            admission::validate_roles(access.kind, hello.parameters.roles)?;
            let handled = self.sessions.receive(peer, packet)?;
            if handled.replaced {
                self.fence_requests(peer);
                let binding = Binding {
                    peer,
                    kind: access.kind,
                    session: self.sessions.session(peer).expect("established handshake"),
                };
                if let Err(error) = self.dispatcher.bind(binding) {
                    self.dispatcher.disconnect(peer);
                    self.sessions.disconnect(peer);
                    self.set_link(peer, None);
                    self.peers
                        .get_mut(&peer)
                        .expect("authorized peer")
                        .handshake = None;
                    return Err(error.into());
                }
                if access.kind == Kind::Client
                    && let Some(watches) = &mut self.watches
                    && let Err(error) = watches.bind(peer, binding.session)
                {
                    self.dispatcher.disconnect(peer);
                    self.sessions.disconnect(peer);
                    self.set_link(peer, None);
                    return Err(ReceiveError::Watch(error));
                }
                self.set_link(
                    peer,
                    Some(Link {
                        binding,
                        send: self.sessions.send_limits(peer)?,
                        remote: self
                            .sessions
                            .remote_parameters(peer)
                            .expect("established handshake"),
                    }),
                );
                let state = self.peers.get_mut(&peer).expect("authorized peer");
                state.handshake = None;
                state.awaiting_welcome = handled.reply.is_some();
            }
            if let Some(reply) = handled.reply {
                self.peers
                    .get_mut(&peer)
                    .expect("authorized peer")
                    .handshake = Some(routed(peer, reply));
            }
            Ok(None)
        } else if packet.envelope.opcode == Opcode::StateSnapshotRequest {
            match packet.metadata.first() {
                Some(0) => self.receive_topic(peer, packet)?,
                Some(1) if self.watches.is_some() => self.receive_watch(peer, packet)?,
                Some(1) => {
                    self.reject_directory(peer, packet.envelope, 2, nack::RetryClass::Permanent)?;
                }
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
            let request = packet.envelope;
            self.receive_partition(peer, request, message, retained_bytes)
        }
    }

    fn receive_topic(
        &mut self,
        peer: NodeId,
        packet: ozzy_proto::Packet<'_>,
    ) -> Result<(), ReceiveError> {
        let link = self.links.get(peer).ok_or(CatalogError::Unknown)?;
        if link.binding.kind != Kind::Client
            || packet.envelope.sender != peer
            || packet.envelope.session != Some(link.binding.session)
        {
            return Err(ReceiveError::Role);
        }
        let Ok(request) = directory::decode_topic_request(
            packet,
            self.parameters.receive.envelope,
            directory::Limits::default(),
        ) else {
            return self.reject_directory(peer, packet.envelope, 1, nack::RetryClass::Permanent);
        };
        let max_metadata = link.send.envelope.max_metadata_bytes.min(
            self.dispatcher
                .limits
                .replies
                .control
                .message_bytes
                .saturating_sub(80),
        );
        let Some(catalog) = self.catalog.as_ref() else {
            return self.reject_directory(peer, packet.envelope, 2, nack::RetryClass::Permanent);
        };
        let page = match catalog.page(&request, max_metadata) {
            Ok(page) => page,
            Err(error) => {
                return self.reject_directory(
                    peer,
                    packet.envelope,
                    if error == CatalogError::Unknown {
                        18
                    } else {
                        1
                    },
                    nack::RetryClass::Permanent,
                );
            }
        };
        let mut metadata = Vec::with_capacity(page.metadata_bytes()?);
        let header = directory::encode_topic_page(
            Envelope {
                opcode: Opcode::StateSnapshot,
                response: true,
                request_id: packet.envelope.request_id,
                sender: self.local(),
                session: Some(link.binding.session),
            },
            &page,
            &mut metadata,
            link.send.envelope,
            directory::Limits::default(),
        )?;
        let reply = Message::multipart([
            Bytes::copy_from_slice(peer.as_bytes()),
            Bytes::copy_from_slice(&header),
            Bytes::from(metadata.into_boxed_slice()),
            Bytes::new(),
        ]);
        match self.try_reply(Class::Control, reply) {
            Ok(()) | Err((ReplyError::Full, _)) => Ok(()),
            Err((error, _)) => Err(ReceiveError::DirectoryReply(error)),
        }
    }

    fn reject_directory(
        &mut self,
        peer: NodeId,
        request: Envelope,
        code: u16,
        retry: nack::RetryClass,
    ) -> Result<(), ReceiveError> {
        let link = self.links.get(peer).ok_or(ReceiveError::Role)?;
        if link.binding.kind != Kind::Client
            || request.sender != peer
            || request.session != Some(link.binding.session)
            || request.request_id.is_none()
        {
            return Err(ReceiveError::Role);
        }
        let mut metadata = Vec::with_capacity(11);
        let header = nack::encode(
            Envelope {
                opcode: Opcode::Nack,
                response: true,
                sender: self.local(),
                ..request
            },
            nack::Nack {
                code,
                retry,
                detail: &[],
                diagnostic: "",
            },
            &mut metadata,
            link.send.envelope,
        )?;
        let reply = crate::native_frames::message(peer.as_bytes(), header, &metadata, Bytes::new());
        match self.try_reply(Class::Control, reply) {
            Ok(()) | Err((ReplyError::Full, _)) => Ok(()),
            Err((error, _)) => Err(ReceiveError::DirectoryReply(error)),
        }
    }

    fn receive_watch(
        &mut self,
        peer: NodeId,
        packet: ozzy_proto::Packet<'_>,
    ) -> Result<(), ReceiveError> {
        let link = self.links.get(peer).ok_or(super::WatchError::Session)?;
        if link.binding.kind != Kind::Client
            || packet.envelope.sender != peer
            || packet.envelope.session != Some(link.binding.session)
        {
            return Err(ReceiveError::Role);
        }
        let Ok(request) = directory::decode_request(
            packet,
            self.parameters.receive.envelope,
            directory::Limits::default(),
        ) else {
            return self.reject_directory(peer, packet.envelope, 1, nack::RetryClass::Permanent);
        };
        let routes = match self
            .watches
            .as_mut()
            .expect("installed watch registry")
            .register_bounded(
                peer,
                link.binding.session,
                request.watch,
                &request.groups,
                link.send.envelope.max_metadata_bytes,
            ) {
            Ok(routes) => routes,
            Err(super::WatchError::Full) => {
                return self.reject_directory(
                    peer,
                    packet.envelope,
                    10,
                    nack::RetryClass::AfterCredit,
                );
            }
            Err(super::WatchError::Unknown) => {
                return self.reject_directory(
                    peer,
                    packet.envelope,
                    18,
                    nack::RetryClass::Permanent,
                );
            }
            Err(super::WatchError::Session) => return Err(ReceiveError::Role),
            Err(
                super::WatchError::Invalid | super::WatchError::Conflict | super::WatchError::Stale,
            ) => {
                return self.reject_directory(
                    peer,
                    packet.envelope,
                    1,
                    nack::RetryClass::Permanent,
                );
            }
        };
        let snapshot = directory::Snapshot {
            watch: request.watch,
            routes,
        };
        let mut metadata = Vec::with_capacity(snapshot.metadata_bytes()?);
        let header = directory::encode_snapshot(
            Envelope {
                opcode: Opcode::StateSnapshot,
                response: true,
                request_id: packet.envelope.request_id,
                sender: self.local(),
                session: Some(link.binding.session),
            },
            &snapshot,
            &mut metadata,
            link.send.envelope,
            directory::Limits::default(),
        )?;
        let reply = Message::multipart([
            Bytes::copy_from_slice(peer.as_bytes()),
            Bytes::copy_from_slice(&header),
            Bytes::from(metadata.into_boxed_slice()),
            Bytes::new(),
        ]);
        match self.try_reply(Class::Control, reply) {
            Ok(()) | Err((ReplyError::Full, _)) => Ok(()),
            Err((error, _)) => Err(ReceiveError::DirectoryReply(error)),
        }
    }

    /// Install one trusted partition actor's newer observation. This changes
    /// only routing hints, never election or confirmation authority. A bounded
    /// shard port command supplies the cross-thread path in broker assembly.
    pub fn publish_route(&mut self, update: &RouteState) -> Result<bool, ServiceError> {
        self.watches
            .as_mut()
            .ok_or(ServiceError::Watch)?
            .publish(update)
            .map_err(|_| ServiceError::Watch)
    }

    /// Queue at most one pending watch hint. Full peer queues preserve it for a
    /// later attempt. A queued hint may still be lost on disconnect, so clients
    /// also refresh after timeout or a wrong-leader response.
    pub fn poll_watch(&mut self) -> Result<bool, ServiceError> {
        let Some(notice) = self.watches.as_mut().and_then(WatchRegistry::next_notice) else {
            return Ok(false);
        };
        let link = self.links.get(notice.peer).ok_or(ServiceError::Watch)?;
        if link.binding.session != notice.session {
            return Err(ServiceError::Watch);
        }
        let (header, metadata) = if let Some(route) = &notice.update {
            let update = directory::Update {
                watch: notice.watch,
                route: route.clone(),
            };
            let mut metadata =
                Vec::with_capacity(update.metadata_bytes().map_err(|_| ServiceError::Watch)?);
            let header = directory::encode_update(
                Envelope {
                    opcode: Opcode::StateUpdate,
                    response: false,
                    request_id: None,
                    sender: self.local(),
                    session: Some(notice.session),
                },
                &update,
                &mut metadata,
                link.send.envelope,
                directory::Limits::default(),
            )
            .map_err(|_| ServiceError::Watch)?;
            (header, metadata)
        } else {
            let mut metadata = Vec::with_capacity(17);
            let header = directory::encode_resync(
                Envelope {
                    opcode: Opcode::StateResync,
                    response: false,
                    request_id: None,
                    sender: self.local(),
                    session: Some(notice.session),
                },
                directory::Resync {
                    watch: notice.watch,
                },
                &mut metadata,
                link.send.envelope,
            )
            .map_err(|_| ServiceError::Watch)?;
            (header, metadata)
        };
        let message = Message::multipart([
            Bytes::copy_from_slice(notice.peer.as_bytes()),
            Bytes::copy_from_slice(&header),
            Bytes::from(metadata.into_boxed_slice()),
            Bytes::new(),
        ]);
        match self.try_reply(Class::Control, message) {
            Ok(()) => {
                self.watches
                    .as_mut()
                    .expect("watch registry")
                    .queued(&notice)
                    .map_err(|_| ServiceError::Watch)?;
                Ok(true)
            }
            Err((ReplyError::Full, _)) => Ok(false),
            Err(_) => Err(ServiceError::Watch),
        }
    }

    /// Install only after the destination reserves lane and retained capacity.
    /// Revoked tokens can be replaced; a live token must be replenished
    /// through its shard-held key. Failure returns the owning reservation.
    pub fn install(
        &mut self,
        peer: NodeId,
        spec: impl Into<GrantSpec>,
        grant: Grant,
    ) -> Result<(), (SetupError, Grant)> {
        let spec = spec.into();
        let target = spec.target();
        let class = grant.class();
        self.dispatcher.install(peer, spec, grant)?;
        if let Some(shard) = target.shard(&self.dispatcher.routes)
            && let Some(requests) = self.requests.get_mut(&shard)
        {
            requests.installed(peer, target, class);
        }
        Ok(())
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
        if message.len() != 4 {
            return Err(ReplyError::Frames);
        }
        let peer = NodeId::from_bytes(
            message
                .part_slice(0)
                .and_then(|p| p.try_into().ok())
                .ok_or(ReplyError::Peer)?,
        );
        let limits = self.links.get(peer).ok_or(ReplyError::Peer)?.send.envelope;
        if message.part_slice(2).expect("four frames").len() > limits.max_metadata_bytes
            || message.part_slice(3).expect("four frames").len() > limits.max_payload_bytes
        {
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
                    let peer = NodeId::from_bytes(
                        message
                            .part_slice(0)
                            .expect("validated reply")
                            .try_into()
                            .expect("peer identity"),
                    );
                    if peers[&peer].awaiting_welcome {
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
        let next = self
            .handshake_turn
            .and_then(|last| self.peers.range((Excluded(last), Unbounded)).next())
            .or_else(|| self.peers.first_key_value())
            .map(|(&id, _)| id);
        let mut advanced = false;
        if let Some(next) = next {
            self.handshake_turn = Some(next);
            let state = self.peers.get_mut(&next).expect("selected peer");
            if let Some(message) = state.handshake.take() {
                match send(message) {
                    Ok(()) => {
                        state.awaiting_welcome = false;
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

/// Unaccepted input. A malformed or over-credit peer need not stop healthy
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
    /// Routing or shard-credit admission refused the packet.
    #[error(transparent)]
    Dispatch(super::Rejection),
}

#[cfg(test)]
pub(super) mod tests;
