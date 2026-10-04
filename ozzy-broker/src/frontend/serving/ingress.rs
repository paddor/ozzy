//! One PEER receive owner: lifecycle fences, admission, and bounded retry turns.

use super::{FollowerRoutes, StartupError, error, failure, fatal};
use futures::{StreamExt, stream::FuturesUnordered};
use omq_tokio::{
    IdentitySocket, Message, MonitorEvent, MonitorStream, ReceiveReceipt, ReceiveSource,
};
use ozzy_proto::{NodeId, Opcode};
use ozzy_runtime::{
    dispatch::Class,
    frontend::{BufferError, DataPressure, Kind, ReceiveBuffers, ReceiveError, Rejection, Service},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
};

type Lane = (u32, Kind, Class);
type LaneWake = Pin<Box<dyn Future<Output = Lane>>>;
const TURN_MESSAGES: usize = 16;
const TURN_BYTES: usize = 2 * 1024 * 1024;

pub(super) struct Ingress<'a> {
    pub(super) socket: &'a IdentitySocket,
    shutdown: &'a crate::Shutdown,
    pub(super) monitor: MonitorStream,
    connections: Connections,
    pub(super) paused: PausedSources,
    bulk: bool,
}

impl<'a> Ingress<'a> {
    pub(super) fn new(
        socket: &'a IdentitySocket,
        monitor: MonitorStream,
        bulk: bool,
        shutdown: &'a crate::Shutdown,
    ) -> Self {
        Self {
            socket,
            shutdown,
            monitor,
            connections: Connections::new(!bulk),
            paused: PausedSources::default(),
            bulk,
        }
    }

    pub(super) fn observe(
        &mut self,
        event: MonitorEvent,
        service: &mut Service,
        followers: &FollowerRoutes,
    ) -> Result<(), StartupError> {
        self.connections.observe(event, service, followers)
    }

    pub(super) fn receive(
        &mut self,
        service: &mut Service,
        buffers: &ReceiveBuffers,
        followers: &FollowerRoutes,
        receipt: ReceiveReceipt,
        body: Message,
    ) -> Result<(), StartupError> {
        if let Some(pressure) = self.admit(service, buffers, followers, receipt, body)? {
            self.paused.hold(service, pressure)?;
        }
        Ok(())
    }

    fn admit(
        &mut self,
        service: &mut Service,
        buffers: &ReceiveBuffers,
        followers: &FollowerRoutes,
        receipt: ReceiveReceipt,
        body: Message,
    ) -> Result<Option<Pressure>, StartupError> {
        // Retain the original receive body for exact-source restoration on Full.
        let identity = receipt
            .identity_bytes()
            .and_then(ozzy_runtime::transport::native_peer_identity)
            .ok_or_else(|| error("PEER receive lacks an identity"))?;
        let message = Message::with_prefix(identity, body.clone());
        if !self
            .connections
            .drain(&mut self.monitor, service, followers)?
            || !self.connections.contains(&message)
        {
            return Ok(None);
        }
        let (message, retained) = match buffers.prepare_borrowed(&message) {
            Ok(input) => input,
            Err(BufferError::Frames) => return Ok(None),
            Err(error) => return Err(failure(error)),
        };
        let Some(message) = self.route(service, followers, message)? else {
            return Ok(None);
        };
        match service.receive(message, retained) {
            Err(ReceiveError::Dispatch(Rejection::Data(DataPressure::Full {
                shard,
                kind,
                class,
                generation,
            }))) => {
                let source = receipt
                    .source()
                    .cloned()
                    .ok_or_else(|| error("PEER receive lacks a source"))?;
                match self.socket.unshift(receipt, body) {
                    Ok(()) => Ok(Some(Pressure {
                        lane: (shard, kind, class),
                        generation,
                        source,
                    })),
                    Err(error) if matches!(error.error, omq_tokio::Error::Closed) => Ok(None),
                    Err(error) => Err(failure(error)),
                }
            }
            Err(ReceiveError::Dispatch(Rejection::Data(DataPressure::Closed)))
                if self.shutdown.is_requested() =>
            {
                Ok(None)
            }
            Err(error) if fatal(&error) => Err(failure(error)),
            _ => Ok(None),
        }
    }

    fn route(
        &self,
        service: &Service,
        followers: &FollowerRoutes,
        mut message: Message,
    ) -> Result<Option<Message>, StartupError> {
        if message.len() != 4 {
            // Compact broker progress belongs to control; Service validates it.
            return Ok((!self.bulk).then_some(message));
        }
        let frames =
            std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).expect("four frames"));
        let Ok(packet) = ozzy_proto::decode_packet(&frames, service.envelope_limits()) else {
            return Ok(None);
        };
        let repair = matches!(
            packet.envelope.opcode,
            Opcode::PrepareFlow | Opcode::Ops | Opcode::SnapshotChunk
        );
        if self.bulk != (repair || packet.envelope.opcode == Opcode::Append) {
            return Ok(None);
        }
        let physical = NodeId::from_bytes(
            message
                .part_slice(0)
                .and_then(|part| part.try_into().ok())
                .ok_or_else(|| error("invalid physical identity"))?,
        );
        if let Some(&(broker, shard)) = followers.incoming.get(&physical) {
            if !repair || packet.envelope.sender != broker {
                return Ok(None);
            }
            let Ok(scope) = ozzy_replication::wire::route(packet, service.envelope_limits()) else {
                return Ok(None);
            };
            if followers.local.get(&scope.group_id) != Some(&shard) {
                return Ok(None);
            }
            message.pop_front_payload();
            message =
                Message::with_prefix(bytes::Bytes::copy_from_slice(broker.as_bytes()), message);
        } else if repair
            && !followers.incoming.is_empty()
            && service
                .access(physical)
                .is_some_and(|access| access.kind == Kind::Broker)
        {
            return Ok(None);
        }
        Ok(Some(message))
    }

    pub(super) fn retry(
        &mut self,
        lane: Lane,
        service: &mut Service,
        buffers: &ReceiveBuffers,
        followers: &FollowerRoutes,
    ) -> Result<(), StartupError> {
        self.paused.waiting.remove(&lane);
        let mut sources = self.paused.sources.remove(&lane).unwrap_or_default();
        let mut bytes = 0usize;
        for _ in 0..TURN_MESSAGES {
            if bytes >= TURN_BYTES {
                break;
            }
            let Some(source) = sources.pop_front() else {
                return Ok(());
            };
            let (receipt, body) = match self.socket.try_recv_from_source(Some(&source)) {
                Ok(received) => received,
                Err(omq_tokio::Error::Closed | omq_tokio::Error::WouldBlock) => continue,
                Err(error) => return Err(failure(error)),
            };
            bytes = bytes.saturating_add(body.max_message_size_len());
            if let Some(pressure) = self.admit(service, buffers, followers, receipt, body)? {
                if pressure.lane == lane {
                    sources.push_back(source);
                    self.paused.sources.insert(lane, sources);
                    self.paused
                        .wait_for_space(service, lane, pressure.generation)?;
                    return Ok(());
                }
                self.paused.hold(service, pressure)?;
            }
        }
        if !sources.is_empty() {
            self.paused.sources.insert(lane, sources);
            self.paused.reschedule(lane);
        }
        Ok(())
    }
}

struct Pressure {
    lane: Lane,
    generation: u64,
    source: ReceiveSource,
}

#[derive(Default)]
pub(super) struct PausedSources {
    sources: BTreeMap<Lane, VecDeque<ReceiveSource>>,
    waiting: BTreeSet<Lane>,
    wakes: FuturesUnordered<LaneWake>,
}

impl PausedSources {
    pub(super) async fn writable(&mut self) -> Option<Lane> {
        self.wakes.next().await
    }

    fn hold(&mut self, service: &Service, pressure: Pressure) -> Result<(), StartupError> {
        self.sources
            .entry(pressure.lane)
            .or_default()
            .push_back(pressure.source);
        self.wait_for_space(service, pressure.lane, pressure.generation)
    }
    fn wait_for_space(
        &mut self,
        service: &Service,
        lane: Lane,
        generation: u64,
    ) -> Result<(), StartupError> {
        if self.waiting.insert(lane) {
            let changed = service
                .data_space_changed_after(lane.0, lane.1, lane.2, generation)
                .ok_or_else(|| error("missing data shard queue"))?;
            self.wakes.push(Box::pin(async move {
                changed.await;
                lane
            }));
        }
        Ok(())
    }
    fn reschedule(&mut self, lane: Lane) {
        if self.waiting.insert(lane) {
            self.wakes.push(Box::pin(std::future::ready(lane)));
        }
    }
}

struct Connections {
    map: BTreeMap<NodeId, u64>,
    control: bool,
}
impl Default for Connections {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Connections {
    fn new(control: bool) -> Self {
        Self {
            map: BTreeMap::new(),
            control,
        }
    }
    fn drain(
        &mut self,
        monitor: &mut omq_tokio::MonitorStream,
        service: &mut Service,
        followers: &FollowerRoutes,
    ) -> Result<bool, StartupError> {
        for _ in 0..64 {
            match monitor.try_recv() {
                Ok(event) => self.observe(event, service, followers)?,
                Err(omq_tokio::MonitorTryRecvError::Empty) => return Ok(true),
                Err(error) => return Err(failure(error)),
            }
        }
        Ok(false)
    }

    fn observe(
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
        if followers.incoming.contains_key(&peer) {
            if connected {
                self.track(
                    peer,
                    info.connection_id,
                    service.transport_peer_capacity() + followers.incoming.len(),
                    |node| service.access(node).is_some() || followers.incoming.contains_key(&node),
                );
            } else {
                self.changed(peer, info.connection_id, false);
            }
            return Ok(()); // A repair connection cannot replace the control session.
        }
        let changed = if connected {
            service.accepts_transport_peer(peer)
                && self.track(
                    peer,
                    info.connection_id,
                    service.transport_peer_capacity() + followers.incoming.len(),
                    |node| service.access(node).is_some() || followers.incoming.contains_key(&node),
                )
        } else {
            self.changed(peer, info.connection_id, false)
        };
        if !changed {
            return Ok(());
        }
        if !self.control {
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
