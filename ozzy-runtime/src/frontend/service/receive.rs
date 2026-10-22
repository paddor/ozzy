//! Inbound frames: peer checks, decoding, and routing to shards.

use super::{ReceiveError, Service};
use crate::frontend::{DataPressure, Kind, Rejection, Routed, RoutingError};
use crate::transport::Frames;
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{NodeId, Opcode, decode_packet};

impl Service {
    /// Receive one PEER frame. Full admission returns pressure without a refusal.
    /// The transport owner restores its original frame to the same receive source.
    /// Retained bytes cover backing storage, not just visible payload length.
    pub fn receive(
        &mut self,
        message: Message,
        retained_bytes: usize,
    ) -> Result<Option<Routed>, ReceiveError> {
        if message.len() == 2 {
            return self.receive_compact(&message, retained_bytes);
        }
        let Some((route, frames)) = message.routed_packet() else {
            return Err(ReceiveError::Peer);
        };
        let peer = NodeId::from_bytes(route.try_into().map_err(|_| ReceiveError::Peer)?);
        let access = self.peers.get(&peer).map(|state| state.access);
        if access.is_none() && self.trusted_maximum == 0 {
            return Err(ReceiveError::Peer);
        }
        let envelope = self.sessions.receive_limits(peer).envelope;
        let packet = decode_packet(&frames, envelope)?;
        if packet.envelope.opcode == Opcode::PreparePub {
            return Err(ReceiveError::Peer);
        }
        let access = match access {
            Some(access) => access,
            None => self.admit_trusted_client(peer, packet)?,
        };
        match packet.envelope.opcode {
            Opcode::Hello | Opcode::Welcome => {
                self.receive_handshake(peer, access.kind, packet, envelope)?;
                return Ok(None);
            }
            Opcode::StateSnapshotRequest => {
                match packet.metadata.first() {
                    Some(0) => self.receive_topic(peer, packet)?,
                    Some(1) => self.receive_watch(peer, packet)?,
                    _ => {
                        return Err(ReceiveError::Directory(
                            ozzy_proto::data::CodecError::Profile,
                        ));
                    }
                }
                return Ok(None);
            }
            Opcode::ReplicaState => self.bind_incoming_channel(peer, packet, envelope)?,
            _ => {}
        }
        self.dispatcher
            .dispatch_data(peer, message, retained_bytes)
            .map(Some)
            .map_err(|rejected| ReceiveError::Dispatch(rejected.reason))
    }

    /// Lossy group publication from an independently configured SUB connection.
    /// Full shard capacity discards this attempt; accepted-tail probes repair it.
    pub fn receive_publication(
        &mut self,
        publisher: NodeId,
        message: &Message,
        retained_bytes: usize,
    ) -> Result<Option<Routed>, ReceiveError> {
        let Some((route, frames)) = message.routed_packet() else {
            return Err(ReceiveError::Peer);
        };
        if self
            .links
            .get(publisher)
            .is_none_or(|link| link.binding.kind != Kind::Broker)
        {
            return Err(ReceiveError::Peer);
        }
        let packet = decode_packet(&frames, self.dispatcher.routes.limits)?;
        let scope = ozzy_replication::wire::route(packet, self.dispatcher.routes.limits).map_err(
            |error| ReceiveError::Dispatch(Rejection::Routing(RoutingError::Broker(error))),
        )?;
        if packet.envelope.opcode != Opcode::PreparePub
            || route != scope.group_id.as_bytes()
            || packet.envelope.sender != publisher
        {
            return Err(ReceiveError::Peer);
        }
        let (_, packet_frames) = message.routed_packet_bytes().ok_or(ReceiveError::Peer)?;
        let normalized = Message::multipart(
            std::iter::once(Bytes::copy_from_slice(publisher.as_bytes())).chain(packet_frames),
        );
        self.dispatcher
            .dispatch_data(publisher, normalized, retained_bytes)
            .map(Some)
            .map_err(|rejected| {
                if matches!(rejected.reason, Rejection::Data(DataPressure::Full { .. })) {
                    crate::profiling::event(crate::profiling::Event::ReplicaPublicationQueueDrop);
                }
                ReceiveError::Dispatch(rejected.reason)
            })
    }
}
