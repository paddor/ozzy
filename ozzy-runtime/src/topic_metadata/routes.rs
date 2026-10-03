//! Client-side routing hints, fenced by link session and watch identity.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use ozzy_proto::{
    GroupId, LinkSessionId, NodeId, RequestId,
    directory::{Resync, RouteState, Snapshot, Update},
};

use super::TopicMetadata;

/// A stale session, foreign group, malformed snapshot, or conflicting view.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RouteCacheError {
    /// Unrecognized peer, zero identity, excess interest, or malformed route.
    #[error("invalid routing hint")]
    Invalid,
    /// Link or registration no longer matches the incoming notification.
    #[error("stale routing watch")]
    Stale,
    /// A full snapshot omits or repeats an interest, or equal views disagree.
    #[error("conflicting routing observation")]
    Conflict,
}

#[derive(Debug)]
struct Registration {
    session: LinkSessionId,
    interests: BTreeSet<GroupId>,
    routes: BTreeMap<GroupId, RouteState>,
    snapshot: bool,
    resync: bool,
}

/// One topic's bounded, best-known broker observations. Hints never prove a
/// leader or confirm an APPEND; the destination actor checks both separately.
#[derive(Debug)]
pub struct RouteCache {
    topic: Arc<TopicMetadata>,
    sessions: BTreeMap<NodeId, LinkSessionId>,
    registrations: BTreeMap<(NodeId, RequestId), Registration>,
    // Membership only. Session, snapshot, and resync checks still belong to
    // the registration. Total entries are bounded by admitted watch interests.
    by_group: BTreeMap<GroupId, BTreeSet<(NodeId, RequestId)>>,
    max_registrations: usize,
    max_interests: usize,
}

impl RouteCache {
    /// Bound live registrations and groups per watch, independent of traffic.
    pub fn new(
        topic: TopicMetadata,
        max_registrations: usize,
        max_interests: usize,
    ) -> Result<Self, RouteCacheError> {
        Self::shared(Arc::new(topic), max_registrations, max_interests)
    }

    pub(crate) fn shared(
        topic: Arc<TopicMetadata>,
        max_registrations: usize,
        max_interests: usize,
    ) -> Result<Self, RouteCacheError> {
        if max_registrations == 0 || max_interests == 0 || max_interests > 256 {
            return Err(RouteCacheError::Invalid);
        }
        Ok(Self {
            topic,
            sessions: BTreeMap::new(),
            registrations: BTreeMap::new(),
            by_group: BTreeMap::new(),
            max_registrations,
            max_interests,
        })
    }

    /// Checked topic identity used to validate every incoming route.
    pub fn topic(&self) -> &TopicMetadata {
        &self.topic
    }

    /// Replace one broker's session, dropping all of its old watches and hints.
    pub fn bind(&mut self, peer: NodeId, session: LinkSessionId) -> Result<bool, RouteCacheError> {
        if self.topic.broker(peer).is_none() || session.as_bytes() == &[0; 16] {
            return Err(RouteCacheError::Invalid);
        }
        if self.sessions.get(&peer) == Some(&session) {
            return Ok(false);
        }
        self.remove_peer(peer);
        self.sessions.insert(peer, session);
        Ok(true)
    }

    /// Ignore a late disconnect after a replacement session is already bound.
    pub fn disconnect(&mut self, peer: NodeId, session: LinkSessionId) -> bool {
        if self.sessions.get(&peer) != Some(&session) {
            return false;
        }
        self.sessions.remove(&peer);
        self.remove_peer(peer);
        true
    }

    /// Register exact interests before sending the matching request. Updates may
    /// arrive before its snapshot; the later snapshot cannot replace newer views.
    pub fn register(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        watch: RequestId,
        groups: &[GroupId],
    ) -> Result<(), RouteCacheError> {
        self.check_session(peer, session)?;
        if watch.as_bytes() == &[0; 16] || groups.is_empty() || groups.len() > self.max_interests {
            return Err(RouteCacheError::Invalid);
        }
        let interests: BTreeSet<_> = groups.iter().copied().collect();
        if interests.len() != groups.len()
            || interests
                .iter()
                .any(|group| self.topic.partition_by_group(*group).is_none())
        {
            return Err(RouteCacheError::Invalid);
        }
        let key = (peer, watch);
        if let Some(existing) = self.registrations.get(&key) {
            return if existing.session == session && existing.interests == interests {
                Ok(())
            } else {
                Err(RouteCacheError::Conflict)
            };
        }
        if self.registrations.len() == self.max_registrations {
            return Err(RouteCacheError::Invalid);
        }
        for group in &interests {
            self.by_group.entry(*group).or_default().insert(key);
        }
        self.registrations.insert(
            key,
            Registration {
                session,
                interests,
                routes: BTreeMap::new(),
                snapshot: false,
                resync: false,
            },
        );
        Ok(())
    }

    /// Release an interest after its replacement watch has a fresh snapshot.
    pub fn unregister(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        watch: RequestId,
    ) -> Result<bool, RouteCacheError> {
        self.check_session(peer, session)?;
        let key = (peer, watch);
        let Some(registration) = self.registrations.remove(&key) else {
            return Ok(false);
        };
        for group in registration.interests {
            let watches = self.by_group.get_mut(&group).expect("registered group");
            watches.remove(&key);
            if watches.is_empty() {
                self.by_group.remove(&group);
            }
        }
        Ok(true)
    }

    /// Atomically install the exact requested set, merging earlier newer hints.
    pub fn snapshot(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        snapshot: Snapshot,
    ) -> Result<(), RouteCacheError> {
        self.check_session(peer, session)?;
        let key = (peer, snapshot.watch);
        let registration = self.registrations.get(&key).ok_or(RouteCacheError::Stale)?;
        if registration.session != session || snapshot.routes.len() != registration.interests.len()
        {
            return Err(RouteCacheError::Conflict);
        }
        let mut merged = BTreeMap::new();
        for route in snapshot.routes {
            self.validate_route(&route)?;
            if !registration.interests.contains(&route.group)
                || merged.insert(route.group, route).is_some()
            {
                return Err(RouteCacheError::Conflict);
            }
        }
        for (group, newer) in &registration.routes {
            let current = merged.get_mut(group).ok_or(RouteCacheError::Conflict)?;
            merge(current, newer)?;
        }
        let registration = self
            .registrations
            .get_mut(&key)
            .expect("checked registration");
        registration.routes = merged;
        registration.snapshot = true;
        registration.resync = false;
        Ok(())
    }

    /// Coalesce a validated route without allowing an older or equal conflict.
    pub fn update(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        update: Update,
    ) -> Result<bool, RouteCacheError> {
        self.check_session(peer, session)?;
        self.validate_route(&update.route)?;
        let registration = self
            .registrations
            .get_mut(&(peer, update.watch))
            .ok_or(RouteCacheError::Stale)?;
        if registration.session != session || !registration.interests.contains(&update.route.group)
        {
            return Err(RouteCacheError::Stale);
        }
        if let Some(current) = registration.routes.get_mut(&update.route.group) {
            merge(current, &update.route)
        } else {
            registration.routes.insert(update.route.group, update.route);
            Ok(true)
        }
    }

    /// Mark one overflowed registration for a new snapshot.
    pub fn resync(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        notice: Resync,
    ) -> Result<(), RouteCacheError> {
        self.check_session(peer, session)?;
        let registration = self
            .registrations
            .get_mut(&(peer, notice.watch))
            .ok_or(RouteCacheError::Stale)?;
        registration.resync = true;
        Ok(())
    }

    /// Whether this broker registration needs a fresh snapshot after loss.
    pub fn needs_snapshot(&self, peer: NodeId, watch: RequestId) -> bool {
        self.registrations
            .get(&(peer, watch))
            .is_some_and(|registration| !registration.snapshot || registration.resync)
    }

    /// Highest observed view among current snapshots. No older leader wins over
    /// a newer view with no leader. Conflicting equal-view hints are rejected.
    pub fn leader(&self, group: GroupId) -> Result<Option<NodeId>, RouteCacheError> {
        Ok(self.route(group)?.and_then(|route| route.leader))
    }

    /// Highest current observation, including a newer view with no leader.
    pub fn route(&self, group: GroupId) -> Result<Option<RouteState>, RouteCacheError> {
        self.route_matching(group, |_, _| true)
    }

    /// Select only observations whose physical session is still current.
    pub(crate) fn route_matching(
        &self,
        group: GroupId,
        mut current: impl FnMut(NodeId, LinkSessionId) -> bool,
    ) -> Result<Option<RouteState>, RouteCacheError> {
        if self.topic.partition_by_group(group).is_none() {
            return Err(RouteCacheError::Invalid);
        }
        let mut best: Option<&RouteState> = None;
        let mut conflict = false;
        for key in self.by_group.get(&group).into_iter().flatten() {
            let registration = self.registrations.get(key).expect("indexed registration");
            if !registration.snapshot
                || registration.resync
                || !current(key.0, registration.session)
            {
                continue;
            }
            let Some(route) = registration.routes.get(&group) else {
                continue;
            };
            match best {
                None => best = Some(route),
                Some(current) if route.view > current.view => {
                    best = Some(route);
                    conflict = false;
                }
                Some(current) if route.view == current.view && route.leader != current.leader => {
                    if current.leader.is_none() {
                        best = Some(route);
                    } else if route.leader.is_some() {
                        conflict = true;
                    }
                }
                _ => {}
            }
        }
        if conflict {
            Err(RouteCacheError::Conflict)
        } else {
            Ok(best.cloned())
        }
    }

    fn remove_peer(&mut self, peer: NodeId) {
        let by_group = &mut self.by_group;
        self.registrations.retain(|key, registration| {
            if key.0 != peer {
                return true;
            }
            for group in &registration.interests {
                by_group
                    .get_mut(group)
                    .expect("registered group")
                    .remove(key);
            }
            false
        });
        by_group.retain(|_, watches| !watches.is_empty());
    }

    fn check_session(&self, peer: NodeId, session: LinkSessionId) -> Result<(), RouteCacheError> {
        if self.sessions.get(&peer) != Some(&session) {
            return Err(RouteCacheError::Stale);
        }
        Ok(())
    }

    fn validate_route(&self, route: &RouteState) -> Result<(), RouteCacheError> {
        let partition = self
            .topic
            .partition_by_group(route.group)
            .ok_or(RouteCacheError::Invalid)?;
        if !route.valid()
            || route.config_epoch != partition.config_epoch
            || route.partition != partition.incarnation
            || route.members.as_ref() != partition.members.as_slice()
        {
            return Err(RouteCacheError::Invalid);
        }
        Ok(())
    }
}

fn merge(current: &mut RouteState, candidate: &RouteState) -> Result<bool, RouteCacheError> {
    if !current.same_identity(candidate) {
        return Err(RouteCacheError::Conflict);
    }
    if candidate.view > current.view {
        *current = candidate.clone();
        Ok(true)
    } else if candidate.view == current.view && candidate.leader != current.leader {
        if current.leader.is_none() {
            *current = candidate.clone();
            Ok(true)
        } else if candidate.leader.is_some() {
            Err(RouteCacheError::Conflict)
        } else {
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests;
