//! Routes of replicated logs created while the broker runs. Shards publish
//! immutable snapshots; the dispatcher and every shard read them.

use super::Placement;
use arc_swap::ArcSwap;
use ozzy_proto::{GroupId, NodeId};
use std::{collections::BTreeMap, sync::Arc};

/// Where one runtime log's copies live.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeRoute {
    /// This broker's copy.
    pub placement: Placement,
    /// The shard of each other member's copy, for addressed repair.
    pub remote: [(NodeId, u32); 2],
}

/// Broker-wide routes of runtime logs. Only broker traffic uses them:
/// clients reach such a log through its embedder, never over a link.
#[derive(Clone, Debug, Default)]
pub struct RuntimeRoutes {
    routes: Arc<ArcSwap<BTreeMap<GroupId, RuntimeRoute>>>,
}

impl RuntimeRoutes {
    /// Publish the route of a new log. False leaves an existing route of
    /// the group, or one of its partition incarnation, in place.
    pub fn insert(&self, route: RuntimeRoute) -> bool {
        let mut inserted = false;
        self.routes.rcu(|routes| {
            inserted = !routes.contains_key(&route.placement.group)
                && routes
                    .values()
                    .all(|other| other.placement.partition != route.placement.partition);
            let mut routes = BTreeMap::clone(routes);
            if inserted {
                routes.insert(route.placement.group, route);
            }
            routes
        });
        inserted
    }

    /// Withdraw a log's route. Traffic that names it afterwards is unknown.
    pub fn remove(&self, group: GroupId) -> bool {
        let mut removed = false;
        self.routes.rcu(|routes| {
            let mut routes = BTreeMap::clone(routes);
            removed = routes.remove(&group).is_some();
            routes
        });
        removed
    }

    /// The current route of `group`.
    pub fn get(&self, group: GroupId) -> Option<RuntimeRoute> {
        self.routes.load().get(&group).copied()
    }

    /// The shard of `peer`'s copy of `group`.
    pub fn remote_shard(&self, peer: NodeId, group: GroupId) -> Option<u32> {
        self.get(group)?
            .remote
            .iter()
            .find(|(node, _)| *node == peer)
            .map(|&(_, shard)| shard)
    }

    /// Number of published routes.
    pub fn len(&self) -> usize {
        self.routes.load().len()
    }

    /// Whether no route is published.
    pub fn is_empty(&self) -> bool {
        self.routes.load().is_empty()
    }
}
