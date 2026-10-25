//! Live topics each reader PUB connection holds, to revoke those whose
//! subscription ended. Admission checks a SUBSCRIBE only when it arrives.

use super::{StartupError, error, failure};
use bytes::Bytes;
use omq_tokio::{
    MonitorEvent, MonitorRecvError, MonitorStream, PeerInfo, PeerSubscriptions, Socket,
};
use ozzy_proto::NodeId;
use ozzy_runtime::{
    frontend::ReaderGrants,
    profiling::{self, Event},
};
use std::{collections::BTreeMap, time::Duration};

/// Interval between checks of held topics against the grants. A connection
/// that holds a revoked topic at two checks in a row is closed, so a client
/// has at least this long to cancel it.
const CHECK: Duration = Duration::from_secs(1);
/// Connections one check closes at most, so a mass revocation spreads
/// reconnects.
const REVOCATIONS: usize = 64;

/// Monitors of the publication sockets, taken before bind so they see every
/// connection.
#[derive(Debug)]
pub(in crate::frontend) struct Monitors {
    reader: MonitorStream,
    follower: Option<MonitorStream>,
}

impl Monitors {
    pub(in crate::frontend) fn new(reader: &Socket, follower: Option<&Socket>) -> Self {
        Self {
            reader: reader.monitor(),
            follower: follower.map(Socket::monitor),
        }
    }
}

/// Monitor capacity of a reader PUB with up to `peers` connections: room for
/// each one's handshake and a few subscriptions at once, so a reconnect burst
/// does not lag the watch.
pub(in crate::frontend) fn monitor_capacity(peers: usize) -> usize {
    peers.saturating_mul(16).clamp(1024, 1 << 16)
}

pub(super) enum Watched {
    Reader(Result<MonitorEvent, MonitorRecvError>),
    Follower(Result<MonitorEvent, MonitorRecvError>),
    Check,
}

/// Watches the reader PUB's subscriptions and closes connections that keep a
/// revoked topic. The SUB reconnects, and admission checks its replay.
pub(super) struct Subscribers<'a> {
    socket: &'a Socket,
    grants: &'a ReaderGrants,
    monitors: Monitors,
    held: Held,
    check: tokio::time::Interval,
}

impl<'a> Subscribers<'a> {
    pub(super) fn new(socket: &'a Socket, grants: &'a ReaderGrants, monitors: Monitors) -> Self {
        let mut check = tokio::time::interval(CHECK);
        check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self {
            socket,
            grants,
            monitors,
            held: Held::default(),
            check,
        }
    }

    /// The next monitor event, or a due check while any connection exists.
    pub(super) async fn next(&mut self) -> Watched {
        let watching = !self.held.connections.is_empty();
        let Self {
            monitors, check, ..
        } = self;
        let follower = async {
            match &mut monitors.follower {
                Some(monitor) => monitor.recv().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            event = monitors.reader.recv() => Watched::Reader(event),
            event = follower => Watched::Follower(event),
            _ = check.tick(), if watching => Watched::Check,
        }
    }

    pub(super) async fn turn(&mut self, watched: Watched) -> Result<(), StartupError> {
        match watched {
            Watched::Reader(Ok(event)) => {
                if matches!(event, MonitorEvent::SubscribeRejected { .. }) {
                    profiling::event(Event::ReaderSubscriptionRejected);
                }
                self.held.observe(&event);
            }
            Watched::Reader(Err(MonitorRecvError::Lagged(_))) => self.resync().await?,
            Watched::Follower(Ok(MonitorEvent::SubscribeRejected { .. })) => {
                profiling::event(Event::FollowerSubscriptionRejected);
            }
            Watched::Follower(Ok(_) | Err(MonitorRecvError::Lagged(_))) => {}
            Watched::Reader(Err(_)) | Watched::Follower(Err(_)) => {
                return Err(error("publication monitor closed"));
            }
            Watched::Check => self.revoke().await?,
        }
        Ok(())
    }

    /// The monitor lost events: reload every connection's subscriptions and
    /// watch on from that snapshot.
    async fn resync(&mut self) -> Result<(), StartupError> {
        profiling::event(Event::ReaderSubscriptionLag);
        let (monitor, peers) = (self.socket.monitor_with_subscriptions().await).map_err(failure)?;
        self.monitors.reader = monitor;
        let peers = (peers.into_iter()).map(|PeerSubscriptions { peer, prefixes }| {
            (peer.connection_id, node(&peer), prefixes)
        });
        self.held.rebuild(peers);
        Ok(())
    }

    async fn revoke(&mut self) -> Result<(), StartupError> {
        for connection in self.held.check(self.grants) {
            match self.socket.disconnect_peer(connection).await {
                Ok(()) => profiling::event(Event::ReaderSubscriptionRevoked),
                Err(omq_tokio::Error::Unroutable) => {}
                Err(error) => return Err(failure(error)),
            }
        }
        Ok(())
    }
}

/// The node a peer's routing identity names, if any.
fn node(peer: &PeerInfo) -> Option<NodeId> {
    (peer.peer_identity.as_deref()).and_then(ozzy_runtime::transport::decode_peer_identity)
}

/// Topics each reader PUB connection holds, as its monitor reported them.
#[derive(Debug, Default)]
struct Held {
    connections: BTreeMap<u64, Connection>,
}

#[derive(Debug, Default)]
struct Connection {
    /// Its routing identity's node. Without one, any held topic is revoked.
    node: Option<NodeId>,
    /// Admitted SUBSCRIBEs not yet cancelled. PUB counts repeats.
    topics: BTreeMap<Bytes, u32>,
    /// Held a revoked topic at the last check.
    suspect: bool,
}

impl Held {
    fn observe(&mut self, event: &MonitorEvent) {
        match event {
            MonitorEvent::HandshakeSucceeded { peer, .. } => {
                self.connections.entry(peer.connection_id).or_default().node = node(peer);
            }
            MonitorEvent::SubscribeReceived {
                connection_id,
                prefix,
            } => {
                let connection = self.connections.entry(*connection_id).or_default();
                *connection.topics.entry(prefix.clone()).or_default() += 1;
            }
            MonitorEvent::UnsubscribeReceived {
                connection_id,
                prefix,
            } => {
                let connection = self.connections.get_mut(connection_id);
                if let Some(topics) = connection.map(|connection| &mut connection.topics)
                    && let Some(count) = topics.get_mut(prefix)
                {
                    *count -= 1;
                    if *count == 0 {
                        topics.remove(prefix);
                    }
                }
            }
            MonitorEvent::Disconnected { peer, .. } => {
                self.connections.remove(&peer.connection_id);
            }
            _ => {}
        }
    }

    /// Replace the table with the socket's snapshot of each connection's
    /// node and admitted prefixes, after lost events. The monitor resumes
    /// right after the snapshot. A connection keeps its grace, so a lag
    /// never extends it.
    fn rebuild(&mut self, peers: impl IntoIterator<Item = (u64, Option<NodeId>, Vec<Bytes>)>) {
        let mut connections = BTreeMap::new();
        for (id, node, prefixes) in peers {
            let mut connection = Connection {
                node,
                suspect: self.connections.get(&id).is_some_and(|known| known.suspect),
                ..Connection::default()
            };
            for prefix in prefixes {
                *connection.topics.entry(prefix).or_default() += 1;
            }
            connections.insert(id, connection);
        }
        self.connections = connections;
    }

    /// Connections to close: each held a revoked topic at this check and
    /// the last one. They are forgotten here.
    fn check(&mut self, grants: &ReaderGrants) -> Vec<u64> {
        let mut revoked = Vec::new();
        for (&id, connection) in &mut self.connections {
            let node = connection.node;
            let violates = (connection.topics.keys())
                .any(|topic| node.is_none_or(|node| grants.revokes(topic, node)));
            if violates && connection.suspect && revoked.len() < REVOCATIONS {
                revoked.push(id);
            }
            connection.suspect = violates;
        }
        for id in &revoked {
            self.connections.remove(id);
        }
        revoked
    }
}

#[cfg(test)]
mod tests;
