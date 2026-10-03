use super::{BrokerLinkError, BrokerLinks};
use crate::{
    signal::StateSignal,
    topic_metadata::{RouteCache, TopicMetadata},
};
use ozzy_proto::{
    GroupId, LinkSessionId, NodeId, TopicId, directory::RouteState, nack::RetryClass,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

mod watch;
pub(super) use watch::Watcher;

#[derive(Debug)]
pub(super) struct State {
    topic: Arc<TopicMetadata>,
    cache: Mutex<RouteCache>,
    interests: Mutex<BTreeMap<GroupId, u64>>,
    changed: StateSignal,
    rejections: Mutex<BTreeMap<(GroupId, NodeId), Rejection>>,
}

#[derive(Debug)]
struct Rejection {
    session: LinkSessionId,
    code: u16,
    retry: RetryClass,
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
        let registrations = topic
            .partition_count()
            .checked_mul(topic.brokers().len())
            .ok_or(BrokerLinkError::Configuration)?;
        let topic = Arc::new(topic);
        let state = Arc::new(State {
            cache: Mutex::new(RouteCache::shared(topic.clone(), registrations, 1)?),
            topic,
            interests: Mutex::new(BTreeMap::new()),
            changed: StateSignal::default(),
            rejections: Mutex::new(BTreeMap::new()),
        });
        self.topics.insert(state.topic.id(), state.clone());
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
    /// routing registrations and coalesced hints. No partition credit is added.
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
        if self.links.0.shared.stop.is_closed() {
            return Err(BrokerLinkError::Closed);
        }
        let group = self.group(number)?;
        let mut registry = self
            .links
            .0
            .shared
            .routing
            .lock()
            .expect("SDK routing poisoned");
        if registry
            .groups
            .get(&group)
            .is_some_and(|state| !Arc::ptr_eq(state, &self.state))
        {
            return Err(BrokerLinkError::Response);
        }
        let mut interests = self.state.interests.lock().expect("SDK interests poisoned");
        if interests.contains_key(&group) {
            return Ok(());
        }
        interests.insert(group, 1);
        registry.groups.insert(group, self.state.clone());
        drop((interests, registry));
        self.links.0.shared.changed.notify_changed();
        Ok(())
    }

    /// Request fresh snapshots after timeout or an authority rejection. Record
    /// partition, message IDs, producer epoch, and sequences stay unchanged.
    pub fn refresh(&self, number: u32) -> Result<(), BrokerLinkError> {
        self.interest(number)?;
        let group = self.group(number)?;
        let mut interests = self.state.interests.lock().expect("SDK interests poisoned");
        let revision = interests.get_mut(&group).expect("registered interest");
        *revision = revision
            .checked_add(1)
            .ok_or(BrokerLinkError::Configuration)?;
        drop(interests);
        self.links.0.shared.changed.notify_changed();
        Ok(())
    }

    /// Best current view, possibly without a leader. Hints from an obsolete
    /// physical session are removed before comparing election views.
    pub fn route(&self, number: u32) -> Result<Option<RouteState>, BrokerLinkError> {
        if self.links.0.shared.stop.is_closed() {
            return Err(BrokerLinkError::Closed);
        }
        let group = self.group(number)?;
        let mut cache = self.state.cache.lock().expect("SDK route cache poisoned");
        let mut changed = false;
        for broker in self.state.topic.brokers() {
            let current = self.links.session(broker.node);
            if let Some(old) = cache.session(broker.node)
                && Some(old) != current
            {
                changed |= cache.disconnect(broker.node, old);
            }
        }
        let result = cache.route(group)?;
        drop(cache);
        if changed {
            self.state.changed.notify_changed();
        }
        if result.is_none() {
            let rejected = self
                .state
                .rejections
                .lock()
                .expect("SDK route rejections poisoned");
            if self.state.topic.brokers().iter().all(|broker| {
                rejected.get(&(group, broker.node)).is_some_and(|rejected| {
                    self.links.session(broker.node) == Some(rejected.session)
                })
            }) {
                let rejection = rejected
                    .get(&(group, self.state.topic.brokers()[0].node))
                    .expect("nonempty brokers");
                return Err(BrokerLinkError::Rejected {
                    code: rejection.code,
                    retry: rejection.retry,
                    hint: None,
                });
            }
        }
        Ok(result)
    }

    /// Capture before reading the current route to close the read/wait race.
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

    fn group(&self, number: u32) -> Result<GroupId, BrokerLinkError> {
        self.state
            .topic
            .partition(number)
            .map(|partition| partition.group)
            .ok_or(BrokerLinkError::Configuration)
    }
}

#[cfg(test)]
mod tests;
