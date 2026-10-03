//! Ordinary receive and transport lifecycle. Partition work stays on shards.

use omq_tokio::{Endpoint, MonitorEvent, TrySendError};
use ozzy_proto::NodeId;
use ozzy_runtime::{
    dispatch::{Error as AdmissionError, SendFailure},
    frontend::{BufferError, Kind, ReceiveBuffers, ReceiveError, Rejection, Service},
};
use std::{collections::BTreeMap, time::Duration};

use super::{FrontendContext, StartupError, error};

impl FrontendContext {
    /// Serve an already assembled bounded frontend on its owned dispatcher.
    /// Configured brokers share one physical connection per pair. Readiness
    /// does not wait for every broker. The service owns no partition authority.
    /// Input rejection drops this attempt; native record retry identity remains
    /// with the sender. Accounting failures and monitor loss stop the frontend.
    pub async fn serve(
        mut self,
        mut service: Service,
        brokers: BTreeMap<NodeId, Endpoint>,
        buffers: ReceiveBuffers,
        retry: Duration,
    ) -> Result<(), StartupError> {
        if service.local() != self.local || retry.is_zero() {
            return Err(error(
                "invalid serving identity or handshake retry interval",
            ));
        }
        buffers
            .check_transport(&self.endpoint, self.limits.message_bytes)
            .map_err(failure)?;
        for (&peer, endpoint) in &brokers {
            buffers
                .check_transport(endpoint, self.limits.message_bytes)
                .map_err(failure)?;
            if service
                .access(peer)
                .is_none_or(|access| access.kind != Kind::Broker)
            {
                return Err(error("outbound broker lacks independent authorization"));
            }
        }
        for (&peer, endpoint) in &brokers {
            if self.local < peer {
                tokio::select! {
                    () = self.shutdown.requested() => return Ok(()),
                    result = self.peer.connect(endpoint.clone()) => result.map_err(failure)?,
                }
            }
            service.start(peer).map_err(failure)?;
        }
        self.ready()?;
        let mut connections = Connections::default();
        let mut tick = tokio::time::interval(retry);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if let Some(message) = service.take_publication() {
                match self.reader_pub.try_send(message) {
                    Ok(()) | Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Closed) => return Err(error("reader PUB socket closed")),
                    Err(TrySendError::Error(error)) => return Err(failure(error)),
                }
            }
            tokio::select! {
                () = self.shutdown.requested() => return Ok(()),
                event = self.monitor.recv() => {
                    connections.observe(event.map_err(failure)?, &mut service)?;
                }
                message = self.peer.recv_from() => {
                    let (identity, body) = message.map_err(failure)?;
                    let message = omq_tokio::Message::with_prefix(identity, body);
                    // OMQ publishes connection events before delivering that
                    // connection's data. Apply those fences before queued input.
                    // A storm gets bounded turns; this packet remains retryable.
                    if !connections.drain(&mut self.monitor, &mut service)?
                        || !connections.contains(&message)
                    {
                        continue;
                    }
                    let (message, retained) = match buffers.prepare(message) {
                        Ok(input) => input,
                        Err(BufferError::Frames) => continue,
                        Err(error) => return Err(failure(error)),
                    };
                    if let Err(error) = service.receive(message, retained)
                        && fatal(&error)
                    {
                        return Err(failure(error));
                    }
                }
                result = service.progress(&self.peer) => {
                    if !self.shutdown.is_requested() {
                        result.map_err(failure)?;
                    }
                },
                _ = tick.tick() => {
                    for &peer in brokers.keys() {
                        service.retry(peer).map_err(failure)?;
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct Connections(BTreeMap<NodeId, u64>);

impl Connections {
    fn drain(
        &mut self,
        monitor: &mut omq_tokio::MonitorStream,
        service: &mut Service,
    ) -> Result<bool, StartupError> {
        for _ in 0..64 {
            match monitor.try_recv() {
                Ok(event) => self.observe(event, service)?,
                Err(omq_tokio::MonitorTryRecvError::Empty) => return Ok(true),
                Err(error) => return Err(failure(error)),
            }
        }
        Ok(false)
    }

    fn observe(&mut self, event: MonitorEvent, service: &mut Service) -> Result<(), StartupError> {
        let (info, connected) = match event {
            MonitorEvent::HandshakeSucceeded { peer, .. } => (peer, true),
            MonitorEvent::Disconnected { peer, .. } => (peer, false),
            _ => return Ok(()),
        };
        let Some(peer) = info
            .peer_identity
            .as_ref()
            .and_then(|identity| <[u8; 16]>::try_from(identity.as_ref()).ok())
            .map(NodeId::from_bytes)
        else {
            return Ok(());
        };
        let changed = if connected {
            service.accepts_transport_peer(peer)
                && self.track(
                    peer,
                    info.connection_id,
                    service.transport_peer_capacity(),
                    |node| service.access(node).is_some(),
                )
        } else {
            self.changed(peer, info.connection_id, false)
        };
        if !changed {
            return Ok(());
        }
        if let Some(link) = service.links().get(peer) {
            service.disconnect(link.binding);
        }
        if service
            .access(peer)
            .is_some_and(|access| access.kind == Kind::Broker)
        {
            service.start(peer).map_err(failure)?;
        }
        Ok(())
    }

    fn contains(&self, message: &omq_tokio::Message) -> bool {
        message
            .part_slice(0)
            .and_then(|identity| <[u8; 16]>::try_from(identity).ok())
            .is_some_and(|identity| self.0.contains_key(&NodeId::from_bytes(identity)))
    }

    fn track(
        &mut self,
        peer: NodeId,
        connection: u64,
        capacity: usize,
        known: impl Fn(NodeId) -> bool,
    ) -> bool {
        if !self.0.contains_key(&peer) && self.0.len() >= capacity {
            if !known(peer) {
                return false;
            }
            // A configured broker or admitted client can displace an identity
            // that has not passed native HELLO. Its queued input stays refused.
            let Some(unadmitted) = self.0.keys().copied().find(|node| !known(*node)) else {
                return false;
            };
            self.0.remove(&unadmitted);
        }
        self.changed(peer, connection, true)
    }

    fn changed(&mut self, peer: NodeId, connection: u64, connected: bool) -> bool {
        if connected {
            self.0.insert(peer, connection) != Some(connection)
        } else if self.0.get(&peer) == Some(&connection) {
            self.0.remove(&peer);
            true
        } else {
            false // Late teardown cannot fence a replacement connection.
        }
    }
}

fn fatal(error: &ReceiveError) -> bool {
    matches!(
        error,
        ReceiveError::Setup(_)
            | ReceiveError::DirectoryReply(_)
            | ReceiveError::Dispatch(
                Rejection::Charge
                    | Rejection::Admission(
                        SendFailure::CapacityInvariant
                            | SendFailure::Admission(
                                AdmissionError::Invalid | AdmissionError::Destination
                            )
                    )
            )
    )
}

fn failure(reason: impl std::fmt::Display) -> StartupError {
    error(reason.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_connection_storm_is_bounded_and_cannot_starve_known_peers() {
        let mut connections = Connections::default();
        let broker = NodeId::from_bytes([9; 16]);
        let known = |peer| peer == broker;
        for id in 1..9 {
            connections.track(NodeId::from_bytes([id; 16]), u64::from(id), 2, known);
            assert!(connections.0.len() <= 2);
        }
        assert_eq!(connections.0.len(), 2);
        assert!(connections.track(broker, 90, 2, known));
        assert_eq!(connections.0[&broker], 90);
        assert_eq!(connections.0.len(), 2);
        assert!(!connections.changed(NodeId::from_bytes([1; 16]), 1, false));
        assert!(connections.track(broker, 91, 2, known));
        assert!(!connections.changed(broker, 90, false));
        assert_eq!(connections.0[&broker], 91);
    }

    #[test]
    fn duplicate_and_late_teardown_do_not_fence_the_new_connection() {
        let peer = NodeId::from_bytes([1; 16]);
        let mut connections = Connections::default();
        assert!(connections.changed(peer, 1, true));
        assert!(connections.changed(peer, 2, true));
        assert!(!connections.changed(peer, 1, false));
        assert!(!connections.changed(peer, 2, true));
        assert_eq!(connections.0[&peer], 2);
        assert!(connections.changed(peer, 2, false));
        assert!(!connections.changed(peer, 2, false));
        assert!(connections.0.is_empty());
    }
}
