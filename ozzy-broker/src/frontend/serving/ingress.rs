//! One PEER receive owner: lifecycle fences, admission, and bounded retry turns.

mod connections;

use super::{FollowerRoutes, Serving, StartupError, error, failure, fatal};
use connections::Connections;
use futures::{StreamExt, stream::FuturesUnordered};
use omq_tokio::{IdentitySocket, Message, ReceiveLane, ReceiveReceipt};
use ozzy_proto::{NodeId, Opcode};
use ozzy_runtime::transport::Frames;
use ozzy_runtime::{
    dispatch::Class,
    frontend::{BufferError, DataPressure, Kind, ReceiveError, Rejection, Service},
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
    shutdown: &'a crate::signal::CloseSignal,
    serving: Serving<'a>,
    /// Connection generation the tracked set reflects; `None` before the
    /// first snapshot.
    pub(super) seen: Option<u64>,
    connections: Connections,
    pub(super) paused: PausedSources,
    bulk: bool,
}

impl<'a> Ingress<'a> {
    pub(super) fn new(
        socket: &'a IdentitySocket,
        bulk: bool,
        shutdown: &'a crate::signal::CloseSignal,
        serving: Serving<'a>,
    ) -> Self {
        Self {
            socket,
            shutdown,
            serving,
            seen: None,
            connections: Connections::new(!bulk),
            paused: PausedSources::default(),
            bulk,
        }
    }

    fn follower_routes(&self) -> &'a FollowerRoutes {
        &self.serving.followers.routes
    }

    /// Bring the tracked connections up to the socket's ready set when it
    /// changed. A connection's input becomes readable only after the set
    /// changes for it; input from one that became ready after this snapshot
    /// is refused until the next turn tracks it, and its sender retries.
    pub(super) async fn prepare(&mut self, service: &mut Service) -> Result<(), StartupError> {
        let generation = self.socket.connections_generation();
        if self.seen == Some(generation) {
            return Ok(());
        }
        // The generation precedes the snapshot it describes, so a change
        // between the two wakes the next turn.
        self.seen = Some(generation);
        let live =
            ozzy_runtime::transport::live_peers(self.socket.connections().await.map_err(failure)?);
        let routes = self.follower_routes();
        let gone: Vec<_> = (self.connections.map.iter())
            .filter(|&(peer, connection)| live.get(peer) != Some(connection))
            .map(|(&peer, &connection)| (peer, connection))
            .collect();
        for link in gone {
            self.connections.apply(link, false, service, routes)?;
        }
        for link in live {
            if self.connections.map.get(&link.0) != Some(&link.1) {
                self.connections.apply(link, true, service, routes)?;
            }
        }
        Ok(())
    }

    pub(super) async fn receive(
        &mut self,
        service: &mut Service,
        receipt: ReceiveReceipt,
        body: Message,
    ) -> Result<(), StartupError> {
        // Input of a connection that became ready after the last snapshot:
        // track it first. Admission still refuses any older connection.
        if source(&receipt)
            .is_some_and(|(peer, connection)| !self.connections.tracks(peer, connection))
            && self.seen != Some(self.socket.connections_generation())
        {
            self.prepare(service).await?;
        }
        if let Some(pressure) = self.admit(service, receipt, body)? {
            self.paused.hold(service, pressure)?;
        }
        Ok(())
    }

    fn admit(
        &mut self,
        service: &mut Service,
        receipt: ReceiveReceipt,
        body: Message,
    ) -> Result<Option<Pressure>, StartupError> {
        // Retain the original receive body for exact-source restoration on Full.
        let identity = receipt
            .identity_bytes()
            .and_then(ozzy_runtime::transport::native_peer_identity)
            .ok_or_else(|| error("PEER receive lacks an identity"))?;
        let message = Message::with_prefix(identity, body.clone());
        // Only the tracked connection of a peer is admitted, so an old body
        // cannot cross client retirement into a reused slot. Admission itself
        // contains no await or lifecycle observation. OMQ fences queued
        // generations and rejects restoration to a retired source.
        if receipt.lane().is_none()
            || !source(&receipt)
                .is_some_and(|(peer, connection)| self.connections.tracks(peer, connection))
        {
            return Ok(None);
        }
        let (message, retained) = match self.serving.buffers.prepare_borrowed(&message) {
            Ok(input) => input,
            Err(BufferError::Frames) => return Ok(None),
            Err(error) => return Err(failure(error)),
        };
        let Some(message) = self.route(service, message)? else {
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
                    .lane()
                    .cloned()
                    .ok_or_else(|| error("PEER receive lacks a source"))?;
                ozzy_runtime::profiling::event(
                    ozzy_runtime::profiling::Event::DispatcherInputPaused,
                );
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
                if self.shutdown.is_closed() =>
            {
                Ok(None)
            }
            Err(error) if fatal(&error) => Err(failure(error)),
            Err(_) => {
                ozzy_runtime::profiling::event(
                    ozzy_runtime::profiling::Event::DispatcherInputDropped,
                );
                Ok(None)
            }
            Ok(_) => Ok(None),
        }
    }

    fn route(
        &self,
        service: &Service,
        mut message: Message,
    ) -> Result<Option<Message>, StartupError> {
        let followers = self.follower_routes();
        let Some((_, frames)) = message.routed_packet() else {
            // Compact broker progress belongs to control; Service validates it.
            return Ok((!self.bulk).then_some(message));
        };
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

    pub(super) fn retry(&mut self, lane: Lane, service: &mut Service) -> Result<(), StartupError> {
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
            ozzy_runtime::profiling::event(ozzy_runtime::profiling::Event::DispatcherInputRetry);
            let (receipt, body) = match self.socket.try_recv_with_receipt(Some(&source)) {
                Ok(received) => received,
                Err(omq_tokio::Error::Closed | omq_tokio::Error::WouldBlock) => continue,
                Err(error) => return Err(failure(error)),
            };
            bytes = bytes.saturating_add(body.max_message_size_len());
            if let Some(pressure) = self.admit(service, receipt, body)? {
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

/// The peer and connection a PEER body arrived on.
fn source(receipt: &ReceiveReceipt) -> Option<(NodeId, u64)> {
    let identity = receipt.peer_identity()?;
    let peer = ozzy_runtime::transport::decode_peer_identity(receipt.identity()?)?;
    Some((peer, identity.peer_info().connection_id))
}

struct Pressure {
    lane: Lane,
    generation: u64,
    source: ReceiveLane,
}

#[derive(Default)]
pub(super) struct PausedSources {
    sources: BTreeMap<Lane, VecDeque<ReceiveLane>>,
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
