//! Nonblocking dispatch state. Network handshake and physical socket ownership
//! sit outside this state machine, so deterministic schedules can drive it too.

use std::collections::BTreeMap;

use omq_tokio::Message;
use ozzy_proto::{GroupId, NodeId, ProducerId};
use ozzy_replication::wire::ReceiveFence;

use super::ReplyLimits;
use super::{Binding, Kind, Routed, RoutingError, RoutingTable};
use crate::dispatch::{Class, Grant, SendFailure, Sender};
use crate::replica_transport::Queue;

/// Fixed connection and grant-index bounds, independent of frame byte limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatcherLimits {
    /// Live independently established peer bindings.
    pub peers: usize,
    /// Grant entries per peer per class. Data cannot consume control entries.
    pub grants_per_class: usize,
    /// Independent outgoing queues per peer. Retained backing remains charged
    /// to its payload owner, including after socket submission.
    pub replies: ReplyLimits,
}

/// Partition/client scope of a shard-issued reservation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Subject {
    /// Configured partition group.
    pub group: GroupId,
    /// Independent APPEND writer, or `None` for broker/reader control traffic.
    pub writer: Option<ProducerId>,
}

/// Scope of one shard-issued token. Data always names a partition/writer.
/// Common control capacity can cover every configured partition on one shard.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum GrantTarget {
    /// One partition and optional writer. Also permits narrower control quotas.
    Partition(Subject),
    /// Control only, shared across this destination shard's partition actors.
    Control(u32),
}

impl From<Subject> for GrantTarget {
    fn from(subject: Subject) -> Self {
        Self::Partition(subject)
    }
}

impl GrantTarget {
    pub(super) fn shard(self, routes: &RoutingTable) -> Option<u32> {
        match self {
            Self::Partition(subject) => routes.partitions.get(&subject.group).map(|p| p.shard),
            Self::Control(shard) => routes.shards.contains(&shard).then_some(shard),
        }
    }
}

/// Shard-issued token scope, including the actor's exact replica receive fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrantSpec {
    /// SDK partition/writer scope or separately bounded control scope.
    Target(GrantTarget),
    /// Replica data for an exact actor-selected receive purpose.
    Replica(ReceiveFence),
}

impl From<GrantTarget> for GrantSpec {
    fn from(target: GrantTarget) -> Self {
        Self::Target(target)
    }
}

impl From<Subject> for GrantSpec {
    fn from(subject: Subject) -> Self {
        GrantTarget::Partition(subject).into()
    }
}

impl GrantSpec {
    /// Replica data can spend this token only for the actor-selected purpose.
    pub fn replica(fence: ReceiveFence) -> Self {
        Self::Replica(fence)
    }

    pub(super) fn target(self) -> GrantTarget {
        match self {
            Self::Target(target) => target,
            Self::Replica(fence) => GrantTarget::Partition(Subject {
                group: fence.scope().group_id,
                writer: None,
            }),
        }
    }

    pub(super) fn fence(self) -> Option<ReceiveFence> {
        match self {
            Self::Target(_) => None,
            Self::Replica(fence) => Some(fence),
        }
    }
}

#[derive(Debug)]
struct InstalledGrant {
    grant: Grant,
    fence: Option<ReceiveFence>,
}

#[derive(Debug)]
pub(super) struct Peer {
    pub(super) binding: Binding,
    pub(super) incoming_channels: super::flow::Channels,
    pub(super) outgoing_channels: super::flow::Channels,
    grants: BTreeMap<(GrantTarget, Class), InstalledGrant>,
    pub(super) replies: [Queue; 2],
    pub(super) next_reply_class: usize,
}

impl Peer {
    fn target(&self, route: Routed) -> GrantTarget {
        let partition = GrantTarget::Partition(Subject {
            group: route.placement.group,
            writer: route.writer,
        });
        if route.class == Class::Control && !self.grants.contains_key(&(partition, route.class)) {
            GrantTarget::Control(route.placement.shard)
        } else {
            partition
        }
    }
}

/// One dispatcher owns link routing and grant consumption. It holds no pending
/// ingress messages and never waits for a destination. Application shards alone
/// issue or replenish reservations. This is not a session negotiation service.
#[derive(Debug)]
pub struct Dispatcher {
    pub(super) local: NodeId,
    pub(super) routes: RoutingTable,
    lanes: BTreeMap<u32, Sender<Message>>,
    pub(super) peers: BTreeMap<NodeId, Peer>,
    pub(super) limits: DispatcherLimits,
    pub(super) replied: Option<NodeId>,
}

impl Dispatcher {
    /// Every configured application shard supplies exactly one sender. No shard
    /// has transport duties. More partitions do not create producer lanes.
    pub fn new(
        local: NodeId,
        routes: RoutingTable,
        lanes: Vec<(u32, Sender<Message>)>,
        limits: DispatcherLimits,
    ) -> Result<Self, SetupError> {
        if local.as_bytes() == &[0; 16] || limits.peers == 0 || limits.grants_per_class == 0 {
            return Err(SetupError::Limits);
        }
        limits.replies.validate()?;
        let count = lanes.len();
        let lanes: BTreeMap<_, _> = lanes.into_iter().collect();
        if lanes.len() != count
            || lanes
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                != routes.shards
        {
            return Err(SetupError::Destination);
        }
        Ok(Self {
            local,
            routes,
            lanes,
            peers: BTreeMap::new(),
            limits,
            replied: None,
        })
    }

    /// Install independently established link state. Session replacement drops
    /// every old unused grant before new grants can be installed. Already queued
    /// work retains its charge and old session stamp for destination validation.
    /// Returns whether the binding changed; duplicates preserve existing grants.
    pub fn bind(&mut self, binding: Binding) -> Result<bool, SetupError> {
        if binding.peer == self.local
            || binding.peer.as_bytes() == &[0; 16]
            || binding.session.as_bytes() == &[0; 16]
        {
            return Err(SetupError::Binding);
        }
        if let Some(peer) = self.peers.get(&binding.peer) {
            if peer.binding == binding {
                return Ok(false);
            }
            if peer.binding.session == binding.session {
                return Err(SetupError::Binding);
            }
        } else if self.peers.len() >= self.limits.peers {
            return Err(SetupError::Full);
        }
        self.peers.insert(
            binding.peer,
            Peer {
                binding,
                incoming_channels: super::flow::Channels::default(),
                outgoing_channels: super::flow::Channels::default(),
                grants: BTreeMap::new(),
                next_reply_class: 0,
                replies: [
                    Queue::new(self.limits.replies.control).map_err(|_| SetupError::Allocation)?,
                    Queue::new(self.limits.replies.data).map_err(|_| SetupError::Allocation)?,
                ],
            },
        );
        Ok(true)
    }

    /// Forget a closed link and reclaim its unused reservations. Destination
    /// actors must also fence the old session before accepting further work.
    pub fn disconnect(&mut self, peer: NodeId) {
        self.peers.remove(&peer);
    }

    /// Install a shard-issued token for one exact session and scope. Replacing a
    /// live token is forbidden: replenish it through its `GrantKey` instead.
    /// On failure the caller retains the token and all unused credit.
    pub fn install(
        &mut self,
        peer: NodeId,
        spec: impl Into<GrantSpec>,
        grant: Grant,
    ) -> Result<(), (SetupError, Grant)> {
        let spec = spec.into();
        let target = spec.target();
        self.prune_revoked(peer);
        if let Err(error) = self.check_install(peer, spec, &grant) {
            return Err((error, grant));
        }
        let class = grant.class();
        self.peers
            .get_mut(&peer)
            .expect("checked peer")
            .grants
            .insert(
                (target, class),
                InstalledGrant {
                    grant,
                    fence: spec.fence(),
                },
            );
        Ok(())
    }

    fn check_install(
        &self,
        peer: NodeId,
        spec: GrantSpec,
        grant: &Grant,
    ) -> Result<(), SetupError> {
        let peer = self.peers.get(&peer).ok_or(SetupError::Binding)?;
        let target = spec.target();
        if !grant.is_live() || grant.session() != peer.binding.session {
            return Err(SetupError::Binding);
        }
        let shard = target.shard(&self.routes).ok_or(SetupError::Destination)?;
        if !self.lanes[&shard].owns_grant(grant) {
            return Err(SetupError::Destination);
        }
        let class = grant.class();
        let replica_data = peer.binding.kind == Kind::Broker && class == Class::Data;
        if replica_data != spec.fence().is_some() {
            return Err(SetupError::Binding);
        }
        match target {
            GrantTarget::Control(_) if class != Class::Control => return Err(SetupError::Binding),
            GrantTarget::Partition(subject) => {
                let writer = peer.binding.kind == Kind::Client && class == Class::Data;
                if writer != subject.writer.is_some()
                    || subject.writer.is_some_and(|id| id.as_bytes() == &[0; 16])
                {
                    return Err(SetupError::Binding);
                }
            }
            GrantTarget::Control(_) => {}
        }
        let previous = peer.grants.get(&(target, class));
        if previous.is_some_and(|installed| installed.grant.is_live()) {
            return Err(SetupError::Duplicate);
        }
        if previous.is_none()
            && peer
                .grants
                .keys()
                .filter(|(_, kind)| *kind == class)
                .count()
                >= self.limits.grants_per_class
        {
            return Err(SetupError::Full);
        }
        Ok(())
    }

    /// Route and consume an existing reservation, then attempt exactly one
    /// nonblocking enqueue. `peer` comes from the independently established
    /// transport binding, not the packet's sender. Rejections return all frames.
    ///
    /// The transport adapter supplies a conservative full backing-storage charge
    /// in `retained_bytes`, including hidden allocation capacity and descriptors.
    /// This method rejects a charge smaller than the visible message footprint;
    /// it cannot discover allocation capacity behind arbitrary `Bytes` slices.
    pub fn dispatch(
        &mut self,
        peer: NodeId,
        message: Message,
        retained_bytes: usize,
    ) -> Result<Routed, Rejected> {
        self.prune_revoked(peer);
        match self.admit(peer, &message, retained_bytes) {
            Ok(route) => {
                let peer = self.peers.get_mut(&peer).expect("checked peer");
                let broker_data = peer.binding.kind == Kind::Broker && route.class == Class::Data;
                let target = peer.target(route);
                let grant = peer
                    .grants
                    .get_mut(&(target, route.class))
                    .expect("checked grant");
                let grant = &mut grant.grant;
                let session = grant.session();
                match self
                    .lanes
                    .get_mut(&route.placement.shard)
                    .expect("configured lane")
                    .try_send(grant, session, retained_bytes, message)
                {
                    Ok(()) => Ok(route),
                    Err(error) => {
                        if broker_data
                            && crate::profiling::enabled()
                            && matches!(
                                error.reason,
                                SendFailure::Admission(crate::dispatch::Error::Full)
                            )
                        {
                            let remaining = grant.remaining();
                            let cause = if remaining.messages == 0 {
                                crate::profiling::Event::DispatcherBrokerMessageFull
                            } else if retained_bytes > remaining.bytes {
                                crate::profiling::Event::DispatcherBrokerByteFull
                            } else {
                                crate::profiling::Event::DispatcherBrokerOtherFull
                            };
                            crate::profiling::event(cause);
                        }
                        Err(Rejected {
                            reason: Rejection::Admission(error.reason),
                            message: error.value,
                        })
                    }
                }
            }
            Err(reason) => Err(Rejected { reason, message }),
        }
    }

    fn prune_revoked(&mut self, peer: NodeId) {
        if let Some(peer) = self.peers.get_mut(&peer) {
            // Revocation already fenced admission and returned unused credit.
            // Dropping its token frees metadata, never retained payload charges.
            peer.grants.retain(|_, installed| installed.grant.is_live());
        }
    }

    fn admit(&self, peer: NodeId, message: &Message, bytes: usize) -> Result<Routed, Rejection> {
        let binding = self.peers.get(&peer).ok_or(Rejection::Peer)?.binding;
        let route = self.routes.route(message, binding)?;
        if bytes
            < message
                .max_message_size_len()
                .saturating_add(std::mem::size_of::<Message>())
        {
            return Err(Rejection::Charge);
        }
        let peer = &self.peers[&peer];
        let installed = peer
            .grants
            .get(&(peer.target(route), route.class))
            .ok_or(Rejection::NoGrant)?;
        if let Some(fence) = installed.fence {
            let frames = std::array::from_fn::<_, 3, _>(|index| {
                message.part_slice(index + 1).expect("routed frames")
            });
            let packet = ozzy_proto::decode_packet(&frames, self.routes.limits)
                .map_err(RoutingError::from)?;
            fence
                .validate_routing(packet, self.routes.limits)
                .map_err(Rejection::Fence)?;
        }
        Ok(route)
    }
}

/// Setup failed before installing or replacing dispatch state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SetupError {
    /// Reserving bounded outbound slots failed before replacing link state.
    #[error("dispatcher queue allocation failed")]
    Allocation,
    /// Invalid metadata limits.
    #[error("invalid dispatcher limits")]
    Limits,
    /// Missing, duplicate, foreign, or inconsistent destination lane.
    #[error("invalid dispatcher destination")]
    Destination,
    /// Unknown, obsolete, or inconsistent peer/session/role binding.
    #[error("invalid dispatcher binding")]
    Binding,
    /// A live token already covers this session and scope.
    #[error("duplicate dispatcher grant")]
    Duplicate,
    /// Bounded peer or grant-index table is full.
    #[error("dispatcher metadata capacity exhausted")]
    Full,
}

/// Unqueued traffic remains owned by the caller for bounded rejection or close.
#[derive(Debug)]
pub struct Rejected {
    /// Routing or admission failure. This never proves an earlier retry failed.
    pub reason: Rejection,
    /// Original frames. The dispatcher retained no overflow copy.
    pub message: Message,
}

/// A frame never reached a destination actor during this attempt.
#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    /// Replica data does not match the actor-backed receive purpose.
    #[error("dispatcher receive fence mismatch: {0}")]
    Fence(ozzy_replication::wire::WireError),
    /// No current independently established binding for this connection.
    #[error("unknown dispatcher peer")]
    Peer,
    /// No shard-issued token for this exact partition/writer/class.
    #[error("no dispatcher grant")]
    NoGrant,
    /// Supplied charge does not cover even the visible message footprint.
    #[error("insufficient dispatcher backing charge")]
    Charge,
    /// Invalid routing fields or stale session.
    #[error(transparent)]
    Routing(#[from] RoutingError),
    /// Reservation exhausted/revoked, receiver closed, or accounting failed.
    #[error("dispatcher admission failed: {0:?}")]
    Admission(SendFailure),
}

#[cfg(test)]
pub(super) mod tests;
