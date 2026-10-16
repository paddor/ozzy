use super::{BrokerLinkError, BrokerLinks};
use crate::{
    signal::StateSignal,
    topic_metadata::{RouteObservations, TopicMetadata},
};
use ozzy_proto::{
    GroupId, LinkSessionId, NodeId, TopicId,
    directory::{BrokerEndpoint, RouteState},
    nack::RetryClass,
};
use std::{collections::BTreeMap, sync::Arc};

mod interest;
use interest::Interest;
mod waiting;
pub(in crate::replicated) use waiting::LeaderRoute;
mod watch;
pub(super) use watch::Watcher;

#[derive(Debug)]
pub(super) struct State {
    topic: Arc<TopicMetadata>,
    /// Per partition number, the view callers route by.
    views: Vec<arc_swap::ArcSwap<RouteView>>,
    /// Per partition number, the interest callers raise.
    interests: Box<[Interest]>,
    changed: StateSignal,
}

/// One partition's routing view, as the watchers last published it: route
/// observations and permanent snapshot rejections by broker, each bound to
/// the broker session that produced it.
#[derive(Debug, Default)]
struct RouteView {
    routes: RouteObservations,
    rejections: BTreeMap<NodeId, Rejection>,
}

#[derive(Clone, Copy, Debug)]
struct Rejection {
    session: LinkSessionId,
    code: u16,
    retry: RetryClass,
}

impl RouteView {
    /// The first broker's rejection once every broker rejected the snapshot
    /// on its `current` session.
    fn rejected(
        &self,
        brokers: &[BrokerEndpoint],
        current: impl Fn(NodeId, LinkSessionId) -> bool,
    ) -> Option<Rejection> {
        let first = *self.rejections.get(&brokers.first()?.node)?;
        brokers
            .iter()
            .all(|broker| {
                self.rejections
                    .get(&broker.node)
                    .is_some_and(|rejection| current(broker.node, rejection.session))
            })
            .then_some(first)
    }
}

impl State {
    /// The group's interest revision, 0 without interest.
    fn revision(&self, group: GroupId) -> u64 {
        self.topic
            .partition_by_group(group)
            .and_then(|partition| self.interests.get(partition.number as usize))
            .map_or(0, Interest::revision)
    }

    /// Replace `broker`'s route observations of the group with `routes`.
    fn publish_routes(&self, group: GroupId, broker: NodeId, routes: RouteObservations) {
        self.publish(group, |view| {
            Some(RouteView {
                routes: view.routes.replacing(broker, routes),
                rejections: view.rejections.clone(),
            })
        });
    }

    /// Record `broker`'s permanent rejection of the group's snapshot.
    fn reject(&self, group: GroupId, broker: NodeId, rejection: Rejection) {
        self.publish(group, |view| {
            let mut rejections = view.rejections.clone();
            rejections.insert(broker, rejection);
            Some(RouteView {
                routes: view.routes.clone(),
                rejections,
            })
        });
    }

    /// Forget `broker`'s rejection once it answered a snapshot.
    fn clear_rejection(&self, group: GroupId, broker: NodeId) {
        self.publish(group, |view| {
            let mut rejections = view.rejections.clone();
            rejections.remove(&broker)?;
            Some(RouteView {
                routes: view.routes.clone(),
                rejections,
            })
        });
    }

    /// Replace the group's view with `next`, unless it returns `None`. Only
    /// watchers on the SDK driver thread publish, so a changed copy loses no
    /// concurrent update.
    fn publish(&self, group: GroupId, next: impl FnOnce(&RouteView) -> Option<RouteView>) {
        if let Some(slot) = self
            .topic
            .partition_by_group(group)
            .and_then(|partition| self.views.get(partition.number as usize))
            && let Some(view) = next(&slot.load())
        {
            slot.store(Arc::new(view));
        }
    }
}

#[derive(Debug)]
pub(super) struct Registry {
    topics: BTreeMap<TopicId, Arc<State>>,
    groups: BTreeMap<GroupId, Arc<State>>,
    maximum: usize,
    bytes: usize,
    used: usize,
    partitions: usize,
}

impl Registry {
    pub(super) fn new(maximum: usize, bytes: usize) -> Self {
        Self {
            topics: BTreeMap::new(),
            groups: BTreeMap::new(),
            maximum,
            bytes,
            used: 0,
            partitions: 0,
        }
    }

    pub(super) fn topic(&mut self, topic: TopicMetadata) -> Result<Arc<State>, BrokerLinkError> {
        if let Some(state) = self.topics.get(&topic.id()) {
            return if state.topic.as_ref() == &topic {
                Ok(state.clone())
            } else {
                Err(BrokerLinkError::Response)
            };
        }
        // Includes topic strings/endpoints, immutable partition tables, three
        // per-partition registrations and coalesced routes, and tree overhead.
        let charge = TopicRoutes::reservation_bytes(topic.partition_count())
            .ok_or(BrokerLinkError::Configuration)?;
        let partitions = self
            .partitions
            .checked_add(topic.partition_count())
            .filter(|n| *n <= self.maximum)
            .ok_or(BrokerLinkError::Configuration)?;
        let used = self
            .used
            .checked_add(charge)
            .filter(|n| *n <= self.bytes)
            .ok_or(BrokerLinkError::Configuration)?;
        // Each group belongs to one topic, so one watch serves it.
        if topic.groups().any(|group| self.groups.contains_key(&group)) {
            return Err(BrokerLinkError::Response);
        }
        let topic = Arc::new(topic);
        let state = Arc::new(State {
            views: (0..topic.partition_count())
                .map(|_| arc_swap::ArcSwap::from_pointee(RouteView::default()))
                .collect(),
            interests: (0..topic.partition_count())
                .map(|_| Interest::default())
                .collect(),
            topic,
            changed: StateSignal::default(),
        });
        self.topics.insert(state.topic.id(), state.clone());
        for group in state.topic.groups() {
            self.groups.insert(group, state.clone());
        }
        self.partitions = partitions;
        self.used = used;
        Ok(state)
    }
}

/// One topic's cached routing hints on the SDK's existing broker links.
/// Interests are registered lazily by the topic writer when a partition first
/// receives a record. Idle cached interests remain bounded until SDK shutdown.
#[derive(Clone, Debug)]
pub struct TopicRoutes {
    links: BrokerLinks,
    state: Arc<State>,
}

/// Opaque coalesced wake generations. They describe no leadership order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteGeneration {
    routes: u64,
    links: u64,
}

impl TopicRoutes {
    /// Cached storage charged for one topic, including immutable metadata,
    /// routing registrations and coalesced hints. No partition authority is added.
    pub fn reservation_bytes(partitions: usize) -> Option<usize> {
        if partitions == 0 {
            return None;
        }
        partitions.checked_mul(2048)?.checked_add(16384)
    }

    pub(in crate::replicated) fn links(&self) -> &BrokerLinks {
        &self.links
    }

    pub(super) fn new(links: BrokerLinks, state: Arc<State>) -> Self {
        Self { links, state }
    }

    /// Checked immutable topic metadata in numeric partition order.
    pub fn metadata(&self) -> &TopicMetadata {
        &self.state.topic
    }

    /// Register one first-used partition on every configured broker link.
    /// This never waits for a broker or grants leadership authority.
    pub fn interest(&self, number: u32) -> Result<(), BrokerLinkError> {
        self.partition_interest(number)?
            .register(&self.links.0.shared.interests);
        Ok(())
    }

    /// Request fresh snapshots after timeout or an authority rejection. Record
    /// partition, message IDs, producer epoch, and sequences stay unchanged.
    pub fn refresh(&self, number: u32) -> Result<(), BrokerLinkError> {
        self.partition_interest(number)?
            .refresh(&self.links.0.shared.interests);
        Ok(())
    }

    /// Best current view, possibly without a leader. Hints from an obsolete
    /// physical session are ignored before comparing election views.
    pub fn route(&self, number: u32) -> Result<Option<RouteState>, BrokerLinkError> {
        if self.links.0.shared.stop.is_closed() {
            return Err(BrokerLinkError::Closed);
        }
        let view = self
            .state
            .views
            .get(number as usize)
            .ok_or(BrokerLinkError::Configuration)?
            .load();
        let current = |broker, session| self.links.session(broker) == Some(session);
        if let Some(route) = view.routes.matching(current)? {
            return Ok(Some(route));
        }
        match view.rejected(self.state.topic.brokers(), current) {
            Some(rejection) => Err(BrokerLinkError::Rejected {
                code: rejection.code,
                retry: rejection.retry,
                hint: None,
            }),
            None => Ok(None),
        }
    }

    /// Capture before reading the current route to close the read/wait race.
    #[inline]
    pub fn generation(&self) -> RouteGeneration {
        RouteGeneration {
            routes: self.state.changed.generation(),
            links: self.links.0.shared.changed.generation(),
        }
    }

    /// Coalesced change observation. Canceling a wait cannot consume a hint.
    pub async fn changed_after(&self, generation: RouteGeneration) -> Result<(), BrokerLinkError> {
        tokio::select! {
            () = self.state.changed.changed_after(generation.routes) => Ok(()),
            () = self.links.0.shared.stop.closed() => Err(BrokerLinkError::Closed),
            () = self.links.0.shared.changed.changed_after(generation.links) => Ok(()),
        }
    }

    fn partition_interest(&self, number: u32) -> Result<&Interest, BrokerLinkError> {
        if self.links.0.shared.stop.is_closed() {
            return Err(BrokerLinkError::Closed);
        }
        self.state
            .interests
            .get(number as usize)
            .ok_or(BrokerLinkError::Configuration)
    }
}

#[cfg(test)]
mod tests;
