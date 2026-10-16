//! Established link observations the dispatcher publishes to shards.

use crate::frontend::{Binding, Kind};
use crate::signal::StateSignal;
use arc_swap::ArcSwap;
use ozzy_proto::{Envelope, NodeId, data::DataLimits, handshake};
use std::{collections::BTreeMap, sync::Arc};

/// Transport identity authorized by an adapter or explicit trust policy.
/// HELLO role claims never grant broker membership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Access {
    /// Identity from the authenticated adapter or explicit trust configuration.
    pub peer: NodeId,
    /// Authorized protocol role, independent of the peer's claimed role.
    pub kind: Kind,
}

/// Established link observation, with negotiated directional codec limits.
/// This grants no partition authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Link {
    /// Current peer, role, and session fence.
    pub binding: Binding,
    /// Common outbound codec bounds.
    pub send: DataLimits,
    /// Selected capabilities and remote codec limits.
    pub remote: handshake::Parameters,
}

/// Read-only coalesced link state for application shards. Replacement first
/// fences dispatcher replies, then publishes this observation. A shard
/// checks the current session before delivering already queued work to actors.
#[derive(Clone, Debug)]
pub struct Links(pub(super) Arc<LinkState>);

#[derive(Debug)]
pub(super) struct LinkState {
    pub(super) current: ArcSwap<BTreeMap<NodeId, Option<Link>>>,
    pub(super) changed: StateSignal,
}

impl Link {
    /// A client request on this link's established session.
    pub(super) fn client_session(&self, peer: NodeId, envelope: &Envelope) -> bool {
        self.binding.kind == Kind::Client
            && envelope.sender == peer
            && envelope.session == Some(self.binding.session)
    }
}

impl Links {
    /// Latest established session, absent before negotiation or after closure.
    pub fn get(&self, peer: NodeId) -> Option<Link> {
        self.0.current.load().get(&peer).copied().flatten()
    }

    /// Established sessions of peers authorized as `kind`.
    pub fn established(&self, kind: Kind) -> usize {
        self.0
            .current
            .load()
            .values()
            .flatten()
            .filter(|link| link.binding.kind == kind)
            .count()
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
