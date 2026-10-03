//! Bounded, session-fenced routing interests on one broker dispatcher.
//! Updates are hints. Partition actors alone establish leader authority.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};

pub use ozzy_proto::directory::RouteState;
use ozzy_proto::{GroupId, LinkSessionId, NodeId, RequestId};

/// Fixed metadata bounds. No limit grows with the number of clients at run time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WatchLimits {
    /// Configured partition identities held by this broker.
    pub partitions: usize,
    /// Independently bound client sessions.
    pub peers: usize,
    /// Live interest registrations across all sessions.
    pub registrations: usize,
    /// Partition interests in one registration and its initial snapshot.
    pub interests_per_registration: usize,
    /// Coalesced updates held for one registration before resync is required.
    pub pending_per_registration: usize,
}

#[derive(Debug)]
struct Registration {
    interests: BTreeSet<GroupId>,
    pending: BTreeMap<GroupId, RouteState>,
    resync: bool,
}

/// One dispatcher-owned table. Registering an interest and capturing its
/// snapshot are one synchronous mutation, so a later update cannot slip past
/// both. Sending a snapshot remains a separate bounded transport operation.
#[derive(Debug)]
pub struct WatchRegistry {
    limits: WatchLimits,
    routes: BTreeMap<GroupId, RouteState>,
    sessions: BTreeMap<NodeId, LinkSessionId>,
    registrations: BTreeMap<(NodeId, RequestId), Registration>,
    ready: BTreeSet<(NodeId, RequestId)>,
    ready_turn: Option<(NodeId, RequestId)>,
}

/// Bounded delivery from one interest registration.
#[derive(Debug, Eq, PartialEq)]
pub enum WatchUpdates {
    /// Current coalesced hints, at most the requested count.
    Updates(Vec<RouteState>),
    /// The client must ask this broker for another snapshot. No stale queued
    /// update is delivered after this marker.
    Resync,
}

/// One current, bounded notice. It stays pending until outgoing queue admission
/// succeeds. `None` asks the client to resnapshot after watcher overflow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchNotice {
    /// Independently established client identity.
    pub peer: NodeId,
    /// Exact current connection session.
    pub session: LinkSessionId,
    /// Registration receiving this hint.
    pub watch: RequestId,
    /// Latest route, or a resync marker.
    pub update: Option<RouteState>,
}

/// Invalid configuration, session, identity, or bounded watch operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WatchError {
    /// Zero identity, duplicate initial group, or invalid fixed limits.
    #[error("invalid routing watch configuration")]
    Invalid,
    /// Missing group or registration.
    #[error("unknown routing watch target")]
    Unknown,
    /// The supplied connection session is no longer current.
    #[error("stale routing watch session")]
    Session,
    /// Group incarnation/configuration changed or equal views conflict.
    #[error("routing identity or view conflict")]
    Conflict,
    /// Older view cannot replace a newer observation.
    #[error("older routing view")]
    Stale,
    /// Peer, registration, or interest bound reached.
    #[error("routing watch capacity exhausted")]
    Full,
}

impl WatchRegistry {
    pub(super) fn route(&self, group: GroupId) -> Option<&RouteState> {
        self.routes.get(&group)
    }

    /// Fixed table and per-registration bounds for frontend setup checks.
    pub fn limits(&self) -> WatchLimits {
        self.limits
    }

    /// Install configured group identities and finite watch bounds.
    pub fn new(
        limits: WatchLimits,
        routes: impl IntoIterator<Item = RouteState>,
    ) -> Result<Self, WatchError> {
        if limits.partitions == 0
            || limits.peers == 0
            || limits.registrations == 0
            || limits.interests_per_registration == 0
            || limits.pending_per_registration == 0
        {
            return Err(WatchError::Invalid);
        }
        let mut known = BTreeMap::new();
        let mut incarnations = BTreeSet::new();
        for state in routes {
            if !state.valid()
                || known.len() == limits.partitions
                || !incarnations.insert(state.partition)
                || known.insert(state.group, state.clone()).is_some()
            {
                return Err(WatchError::Invalid);
            }
        }
        Ok(Self {
            limits,
            routes: known,
            sessions: BTreeMap::new(),
            registrations: BTreeMap::new(),
            ready: BTreeSet::new(),
            ready_turn: None,
        })
    }

    /// Bind an independently established connection. Replacement forgets every
    /// old registration and pending notice for this peer before a new snapshot.
    pub fn bind(&mut self, peer: NodeId, session: LinkSessionId) -> Result<bool, WatchError> {
        if peer.as_bytes() == &[0; 16] || session.as_bytes() == &[0; 16] {
            return Err(WatchError::Invalid);
        }
        if self.sessions.get(&peer) == Some(&session) {
            return Ok(false);
        }
        if !self.sessions.contains_key(&peer) && self.sessions.len() == self.limits.peers {
            return Err(WatchError::Full);
        }
        self.registrations.retain(|(id, _), _| *id != peer);
        self.ready.retain(|(id, _)| *id != peer);
        self.sessions.insert(peer, session);
        Ok(true)
    }

    /// End one connection. Delayed old-session commands can no longer register
    /// or drain observations; another peer's watches are unaffected.
    pub fn disconnect(&mut self, peer: NodeId, session: LinkSessionId) -> bool {
        if self.sessions.get(&peer) != Some(&session) {
            return false;
        }
        self.sessions.remove(&peer);
        self.registrations.retain(|(id, _), _| *id != peer);
        self.ready.retain(|(id, _)| *id != peer);
        true
    }

    /// Capture current state and register interest in one dispatcher turn.
    /// Retrying the same ID with the same interests returns a fresh snapshot and
    /// clears older pending hints. A different interest set needs a new ID.
    pub fn register(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        id: RequestId,
        groups: &[GroupId],
    ) -> Result<Vec<RouteState>, WatchError> {
        self.register_bounded(peer, session, id, groups, usize::MAX)
    }

    /// Reserve a registration only if its complete initial snapshot fits the
    /// client's negotiated metadata limit. Rejection changes no watch state.
    pub fn register_bounded(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        id: RequestId,
        groups: &[GroupId],
        metadata_bytes: usize,
    ) -> Result<Vec<RouteState>, WatchError> {
        self.check_session(peer, session)?;
        if id.as_bytes() == &[0; 16]
            || groups.is_empty()
            || groups.len() > self.limits.interests_per_registration
        {
            return Err(WatchError::Invalid);
        }
        let interests: BTreeSet<_> = groups.iter().copied().collect();
        if interests.len() != groups.len() {
            return Err(WatchError::Invalid);
        }
        if interests
            .iter()
            .any(|group| !self.routes.contains_key(group))
        {
            return Err(WatchError::Unknown);
        }
        let key = (peer, id);
        if let Some(existing) = self.registrations.get(&key) {
            if existing.interests != interests {
                return Err(WatchError::Conflict);
            }
        } else if self.registrations.len() == self.limits.registrations {
            return Err(WatchError::Full);
        }
        let routes = interests
            .iter()
            .map(|group| self.routes[group].clone())
            .collect();
        let snapshot = ozzy_proto::directory::Snapshot { watch: id, routes };
        if snapshot.metadata_bytes().map_err(|_| WatchError::Invalid)? > metadata_bytes {
            return Err(WatchError::Invalid);
        }
        self.registrations.insert(
            key,
            Registration {
                interests,
                pending: BTreeMap::new(),
                resync: false,
            },
        );
        self.ready.remove(&key);
        Ok(snapshot.routes)
    }

    /// Drop one interest without affecting other registrations on this session.
    pub fn unregister(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        id: RequestId,
    ) -> Result<bool, WatchError> {
        self.check_session(peer, session)?;
        self.ready.remove(&(peer, id));
        Ok(self.registrations.remove(&(peer, id)).is_some())
    }

    /// Record a leader observation. A known leader changes only in a higher
    /// view; an unknown leader may become known within the same view.
    /// Identity and member order must match trusted deployment state.
    /// This never grants partition authority or writer confirmation.
    pub fn publish(&mut self, update: &RouteState) -> Result<bool, WatchError> {
        if !update.valid() {
            return Err(WatchError::Invalid);
        }
        let previous = self.routes.get(&update.group).ok_or(WatchError::Unknown)?;
        if !previous.same_identity(update) {
            return Err(WatchError::Conflict);
        }
        if update.view < previous.view {
            return Err(WatchError::Stale);
        }
        if update.view == previous.view {
            if update.leader == previous.leader || update.leader.is_none() {
                return Ok(false);
            }
            if previous.leader.is_some() {
                return Err(WatchError::Conflict);
            }
        }
        self.routes.insert(update.group, update.clone());
        for (&key, registration) in &mut self.registrations {
            if !registration.interests.contains(&update.group) || registration.resync {
                continue;
            }
            if !registration.pending.contains_key(&update.group)
                && registration.pending.len() == self.limits.pending_per_registration
            {
                registration.pending.clear();
                registration.resync = true;
            } else {
                registration.pending.insert(update.group, update.clone());
            }
            self.ready.insert(key);
        }
        Ok(true)
    }

    /// Inspect one ready registration in fair key order. This does not consume
    /// the hint, so a full outgoing queue cannot lose it. At most one notice is
    /// created per call even when a client watches many partitions.
    pub fn next_notice(&mut self) -> Option<WatchNotice> {
        let key = self
            .ready_turn
            .and_then(|last| {
                self.ready
                    .range((Excluded(last), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| self.ready.first().copied())?;
        self.ready_turn = Some(key);
        let registration = &self.registrations[&key];
        Some(WatchNotice {
            peer: key.0,
            session: self.sessions[&key.0],
            watch: key.1,
            update: if registration.resync {
                None
            } else {
                Some(registration.pending.first_key_value()?.1.clone())
            },
        })
    }

    /// Complete a notice only after it enters the bounded peer reply queue.
    /// A resync marker ends the registration; a retry starts a new snapshot.
    pub fn queued(&mut self, notice: &WatchNotice) -> Result<(), WatchError> {
        self.check_session(notice.peer, notice.session)?;
        let key = (notice.peer, notice.watch);
        let registration = self
            .registrations
            .get_mut(&key)
            .ok_or(WatchError::Unknown)?;
        match &notice.update {
            Some(update) if !registration.resync => {
                if registration.pending.get(&update.group) != Some(update) {
                    return Err(WatchError::Stale);
                }
                registration.pending.remove(&update.group);
                if registration.pending.is_empty() {
                    self.ready.remove(&key);
                }
            }
            None if registration.resync => {
                self.registrations.remove(&key);
                self.ready.remove(&key);
            }
            _ => return Err(WatchError::Stale),
        }
        Ok(())
    }

    /// Drain at most `maximum` current hints. Overflow keeps returning Resync
    /// until registration is renewed, so notification loss cannot appear caught up.
    pub fn take(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        id: RequestId,
        maximum: usize,
    ) -> Result<WatchUpdates, WatchError> {
        self.check_session(peer, session)?;
        if maximum == 0 {
            return Err(WatchError::Invalid);
        }
        let registration = self
            .registrations
            .get_mut(&(peer, id))
            .ok_or(WatchError::Unknown)?;
        if registration.resync {
            return Ok(WatchUpdates::Resync);
        }
        let mut updates = Vec::with_capacity(maximum.min(registration.pending.len()));
        for _ in 0..maximum {
            let Some((&group, _)) = registration.pending.first_key_value() else {
                break;
            };
            updates.push(registration.pending.remove(&group).expect("present hint"));
        }
        if registration.pending.is_empty() {
            self.ready.remove(&(peer, id));
        }
        Ok(WatchUpdates::Updates(updates))
    }

    fn check_session(&self, peer: NodeId, session: LinkSessionId) -> Result<(), WatchError> {
        if self.sessions.get(&peer) != Some(&session) {
            return Err(WatchError::Session);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
