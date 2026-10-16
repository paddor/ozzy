//! Binding cached PREPARE packets to a backup's receipt channel and queueing
//! them as live sends, catch-up or repair.

use super::{PacketSend, remote_peer};
use crate::replica_actor::{
    ActorError, Bytes, Message, Prefix, ReplicaActor, SendClass, require_normal, wire,
};
use crate::replica_transport::EnqueueError;
use crate::transport::Frames;
use ozzy_proto::Opcode;
use ozzy_replication::flow::{ReceiveEpoch, Repair, TransmitError};

/// Where a cached packet goes once its voter's channel can take it.
#[derive(Debug, Clone, Copy)]
struct PacketRoute {
    session: ozzy_proto::LinkSessionId,
    epoch: ReceiveEpoch,
    repair: Option<Repair>,
    /// Data messages and bytes already queued for the voter.
    queued: (usize, usize),
}

impl ReplicaActor {
    pub(in crate::replica_actor::normal) fn send_live(&mut self) -> Result<(), ActorError> {
        if !self.application_ready() {
            return Ok(());
        }
        if self.work.flow.publication_enabled && self.work.flow.publication.is_none() {
            let snapshot = require_normal(&self.driver)?.snapshot();
            if let Some(live) = self
                .work
                .live
                .iter_mut()
                .find(|live| !live.published && live.scope == snapshot.scope)
                && let Some(frames) = live
                    .packets
                    .iter()
                    .flatten()
                    .find_map(Frames::frame_bytes::<3>)
            {
                let encoded = wire::encode_publication(
                    self.local,
                    &frames.each_ref().map(AsRef::as_ref),
                    snapshot.committed,
                    &mut self.metadata,
                    self.wire_limits,
                )?;
                self.work.flow.publication = Some(Message::multipart([
                    Bytes::copy_from_slice(snapshot.scope.group_id.as_bytes()),
                    Bytes::copy_from_slice(&encoded.header),
                    Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
                    frames[2].clone(),
                ]));
                live.published = true;
                self.more_work = true;
            }
        }
        for index in 0..self.work.live.len() {
            for voter in 0..3 {
                let live = &mut self.work.live[index];
                if live.scope != self.driver.scope() {
                    continue;
                }
                let Some(packet) = live.packets[voter].take() else {
                    continue;
                };
                let (predecessor, end) = (live.predecessor, live.end);
                let (_, packet) = self.send_cached_packet(voter, packet, predecessor, end)?;
                self.work.live[index].packets[voter] = Some(packet);
            }
        }
        Ok(())
    }

    pub(in crate::replica_actor::normal) fn send_cached_packet(
        &mut self,
        voter: usize,
        packet: Message,
        predecessor: Prefix,
        end: Prefix,
    ) -> Result<(PacketSend, Message), ActorError> {
        // Return the owned template on every nonterminal path. Polling a live
        // window without local room/new work must not clone its multipart vectors.
        let route = match self.route_packet(voter, predecessor, end)? {
            Ok(route) => route,
            Err(outcome) => return Ok((outcome, packet)),
        };
        let bytes = bound_packet_bytes(&packet);
        let queued = route.queued;
        if queued.0 == self.config.data.messages || bytes > self.config.data.bytes - queued.1 {
            return Ok((PacketSend::Wait, packet));
        }
        let committed = require_normal(&self.driver)?.snapshot().committed;
        let commit = if end.op <= committed.op {
            end
        } else {
            committed
        };
        let packet = self.bind_packet(packet, route.epoch, commit, route.session)?;
        self.transmit(voter, route.repair, end, packet.clone())?;
        crate::profiling::event(if route.repair.is_some() {
            crate::profiling::Event::ReplicaRepair
        } else {
            crate::profiling::Event::ReplicaSend
        });
        Ok((PacketSend::Sent, packet))
    }

    /// Check that the voter's channel can take the packet from `predecessor`
    /// through `end` now, and stage its operations in the flow scratch.
    fn route_packet(
        &mut self,
        voter: usize,
        predecessor: Prefix,
        end: Prefix,
    ) -> Result<Result<PacketRoute, PacketSend>, ActorError> {
        let to = self.configuration.voters()[voter];
        let Some(session) = self.session(to) else {
            return Ok(Err(PacketSend::Wait));
        };
        let Some(peer) = &self.work.flow.peers[voter] else {
            return Ok(Err(PacketSend::Wait));
        };
        let Some(sender) = peer.sender() else {
            return Ok(Err(PacketSend::Wait));
        };
        if self.work.flow.handles[voter].is_none_or(|(channel, _)| channel != sender.channel()) {
            return Ok(Err(PacketSend::Wait));
        }
        let queued = self
            .outbox
            .queued(to, SendClass::Data)
            .ok_or(ActorError::Enqueue(EnqueueError::Peer))?;
        let repair = peer.repair(queued.0 > 0);
        // An existing retry blocked on local transport must not become a fresh send.
        if repair.is_none() && peer.repair(false).is_some() {
            return Ok(Err(PacketSend::Wait));
        }
        // With publication, a fresh send only catches a lagging backup up.
        let catching_up = self.work.flow.publication_enabled && repair.is_none();
        if catching_up && !peer.needs_catch_up() {
            return Ok(Err(PacketSend::Wait));
        }
        let cursor = repair.map_or(sender.sent(), |repair| repair.after);
        if cursor.op < predecessor.op
            || cursor.op >= end.op
            || repair.is_some_and(|repair| end.op > repair.through.op)
            || (catching_up
                && peer
                    .catch_up_through()
                    .is_none_or(|through| end.op > through))
        {
            return Ok(Err(PacketSend::Miss));
        }
        let route = PacketRoute {
            session,
            epoch: sender.channel().epoch,
            repair,
            queued,
        };
        let available = repair.is_none().then(|| sender.available());
        Ok(self
            .work
            .flow
            .stage_operations(cursor, end, available)
            .map(|()| route))
    }

    /// Charge one bound packet to the voter's receipt window and queue it.
    pub(in crate::replica_actor::normal) fn transmit(
        &mut self,
        voter: usize,
        repair: Option<Repair>,
        end: Prefix,
        packet: Message,
    ) -> Result<(), ActorError> {
        let peer = remote_peer(&mut self.work.flow.peers, voter)?;
        if let Some(repair) = repair {
            peer.record_repair(repair, end)
        } else {
            peer.record_send(&self.work.flow.scratch)
        }
        .map_err(TransmitError::from)?;
        crate::profiling::count(
            crate::profiling::Event::ReplicaPayloadBytes,
            packet.part_slice(2).map_or(0, <[u8]>::len) as u64,
        );
        self.outbox
            .try_enqueue(self.configuration.voters()[voter], SendClass::Data, packet)
            .map_err(|(error, _)| ActorError::Enqueue(error))?;
        Ok(())
    }

    pub(in crate::replica_actor::normal) fn bind_packet(
        &mut self,
        message: Message,
        epoch: ReceiveEpoch,
        commit: Prefix,
        session: ozzy_proto::LinkSessionId,
    ) -> Result<Message, ActorError> {
        let frames = message.frame_bytes::<3>().ok_or(wire::WireError::Length)?;
        let envelope = if frames[0].is_empty() {
            ozzy_proto::Envelope {
                opcode: Opcode::Prepare,
                response: false,
                request_id: None,
                sender: self.local,
                session: None,
            }
        } else {
            ozzy_proto::decode_packet(
                &frames.each_ref().map(AsRef::as_ref),
                self.wire_limits.envelope,
            )
            .map_err(wire::WireError::from)?
            .envelope
        };
        let old_offset = if envelope.opcode == Opcode::PrepareFlow {
            96
        } else {
            80
        };
        let metadata = &frames[1];
        let mut prefix = [0u8; 40];
        prefix[..8].copy_from_slice(&commit.op.0.to_be_bytes());
        prefix[8..].copy_from_slice(commit.digest.as_bytes());
        if old_offset == 96
            && metadata[80..96] == epoch.get().to_be_bytes()
            && metadata[136..176] == prefix
            && envelope.session == Some(session)
        {
            return Ok(message);
        }
        let length = metadata.len() + 96 - old_offset;
        self.metadata[..80].copy_from_slice(&metadata[..80]);
        self.metadata[80..96].copy_from_slice(&epoch.get().to_be_bytes());
        self.metadata[96..length].copy_from_slice(&metadata[old_offset..]);
        self.metadata[136..176].copy_from_slice(&prefix);
        let header = ozzy_proto::Envelope {
            opcode: Opcode::PrepareFlow,
            session: Some(session),
            ..envelope
        }
        .encode_header(length, frames[2].len(), self.wire_limits.envelope)
        .map_err(wire::WireError::from)?;
        Ok(Message::multipart([
            Bytes::copy_from_slice(&header),
            Bytes::copy_from_slice(&self.metadata[..length]),
            frames[2].clone(),
        ]))
    }
}

fn bound_packet_bytes(packet: &Message) -> usize {
    let header = packet.part_slice(0).unwrap_or_default();
    let added = if header.is_empty() {
        80 // Envelope plus newly bound receive epoch.
    } else if header.get(5) == Some(&(Opcode::Prepare as u8)) {
        16 // Receive epoch; the template already has an envelope.
    } else {
        0
    };
    16 + packet.byte_len() + added // OMQ destination identity precedes Ozzy frames.
}
