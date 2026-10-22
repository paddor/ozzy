//! Peer connections the monitor reports, bounded and fenced by connection ID.

use super::{FollowerRoutes, StartupError, failure};
use omq_tokio::{MonitorEvent, MonitorTryRecvError};
use ozzy_proto::NodeId;
use ozzy_runtime::frontend::{Kind, Service};
use std::collections::BTreeMap;

pub(super) struct Connections {
    pub(super) map: BTreeMap<NodeId, u64>,
    control: bool,
}

impl Default for Connections {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Connections {
    pub(super) fn new(control: bool) -> Self {
        Self {
            map: BTreeMap::new(),
            control,
        }
    }

    /// Observe up to 64 queued events: `true` once none is left, `None`
    /// when the monitor lost some.
    pub(super) fn drain(
        &mut self,
        monitor: &mut omq_tokio::MonitorStream,
        service: &mut Service,
        followers: &FollowerRoutes,
    ) -> Result<Option<bool>, StartupError> {
        for _ in 0..64 {
            match monitor.try_recv() {
                Ok(event) => self.observe(event, service, followers)?,
                Err(MonitorTryRecvError::Empty) => return Ok(Some(true)),
                Err(MonitorTryRecvError::Lagged(_)) => return Ok(None),
                Err(error) => return Err(failure(error)),
            }
        }
        Ok(Some(false))
    }

    pub(super) fn observe(
        &mut self,
        event: MonitorEvent,
        service: &mut Service,
        followers: &FollowerRoutes,
    ) -> Result<(), StartupError> {
        let (info, connected) = match event {
            MonitorEvent::HandshakeSucceeded { peer, .. } => (peer, true),
            MonitorEvent::Disconnected { peer, .. } => (peer, false),
            _ => return Ok(()),
        };
        let Some(peer) = info
            .peer_identity
            .as_ref()
            .and_then(|identity| ozzy_runtime::transport::decode_peer_identity(identity))
        else {
            return Ok(());
        };
        self.apply((peer, info.connection_id), connected, service, followers)
    }

    /// Track or retire one peer connection.
    pub(super) fn apply(
        &mut self,
        (peer, connection): (NodeId, u64),
        connected: bool,
        service: &mut Service,
        followers: &FollowerRoutes,
    ) -> Result<(), StartupError> {
        let repair = followers.incoming.contains_key(&peer);
        let changed = if !connected {
            self.changed(peer, connection, false)
        } else if repair || service.accepts_transport_peer(peer) {
            self.track(
                peer,
                connection,
                service.transport_peer_capacity() + followers.incoming.len(),
                |node| service.access(node).is_some() || followers.incoming.contains_key(&node),
            )
        } else {
            false
        };
        // A repair connection cannot replace the control session.
        if repair || !changed || !self.control {
            return Ok(());
        }
        if let Some(link) = service.links().get(peer) {
            service.disconnect(link.binding);
        }
        if !connected {
            service.retire_transport_client(peer);
        }
        if service
            .access(peer)
            .is_some_and(|access| access.kind == Kind::Broker)
        {
            service.start(peer).map_err(failure)?;
        }
        Ok(())
    }

    pub(super) fn contains(&self, message: &omq_tokio::Message) -> bool {
        message
            .part_slice(0)
            .and_then(|identity| <[u8; 16]>::try_from(identity).ok())
            .is_some_and(|identity| self.map.contains_key(&NodeId::from_bytes(identity)))
    }

    fn track(
        &mut self,
        peer: NodeId,
        connection: u64,
        capacity: usize,
        known: impl Fn(NodeId) -> bool,
    ) -> bool {
        if !self.map.contains_key(&peer) && self.map.len() >= capacity {
            if !known(peer) {
                return false;
            }
            // A configured broker or admitted client can displace an identity
            // that has not passed native HELLO. Its queued input stays refused.
            let Some(unadmitted) = self.map.keys().copied().find(|node| !known(*node)) else {
                return false;
            };
            self.map.remove(&unadmitted);
        }
        self.changed(peer, connection, true)
    }

    fn changed(&mut self, peer: NodeId, connection: u64, connected: bool) -> bool {
        if connected {
            self.map.insert(peer, connection) != Some(connection)
        } else if self.map.get(&peer) == Some(&connection) {
            self.map.remove(&peer);
            true
        } else {
            false // Late teardown cannot fence a replacement connection.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_connection_storm_is_bounded_and_cannot_starve_known_peers() {
        let mut connections = Connections::new(true);
        let broker = NodeId::from_bytes([9; 16]);
        let known = |peer| peer == broker;
        for id in 1..9 {
            connections.track(NodeId::from_bytes([id; 16]), u64::from(id), 2, known);
            assert!(connections.map.len() <= 2);
        }
        assert_eq!(connections.map.len(), 2);
        assert!(connections.track(broker, 90, 2, known));
        assert_eq!(connections.map[&broker], 90);
        assert_eq!(connections.map.len(), 2);
        assert!(!connections.changed(NodeId::from_bytes([1; 16]), 1, false));
        assert!(connections.track(broker, 91, 2, known));
        assert!(!connections.changed(broker, 90, false));
        assert_eq!(connections.map[&broker], 91);
    }

    #[test]
    fn duplicate_and_late_teardown_do_not_fence_the_new_connection() {
        let peer = NodeId::from_bytes([1; 16]);
        let mut connections = Connections::new(true);
        assert!(connections.changed(peer, 1, true));
        assert!(connections.changed(peer, 2, true));
        assert!(!connections.changed(peer, 1, false));
        assert!(!connections.changed(peer, 2, true));
        assert_eq!(connections.map[&peer], 2);
        assert!(connections.changed(peer, 2, false));
        assert!(!connections.changed(peer, 2, false));
        assert!(connections.map.is_empty());
    }
}
