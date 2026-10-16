use crate::transport::Frames;

use omq_tokio::{Message, TrySendError};
use ozzy_proto::{EnvelopeLimits, NodeId, Opcode, decode_packet};

use super::{Dispatcher, SetupError, dispatch::Peer};
use crate::dispatch::Class;
use crate::replica_transport::{QueueLimits, SendAttempt};

/// Per-peer queued frame bounds. Full backing allocations remain the payload
/// owner's responsibility; queued slices can retain larger allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplyLimits {
    /// Reserved confirmations, elections, and session/control traffic.
    pub control: QueueLimits,
    /// Independent canonical transfer and reader delivery traffic.
    pub data: QueueLimits,
}

impl ReplyLimits {
    pub(super) fn validate(self) -> Result<(), SetupError> {
        self.control.validate().map_err(|_| SetupError::Limits)?;
        self.data.validate().map_err(|_| SetupError::Limits)?;
        if self.control.message_bytes < 80 || self.data.message_bytes < 80 {
            return Err(SetupError::Limits);
        }
        Ok(())
    }
}

/// At most one control and one data attempt for one peer. Round-robin selection
/// continues across full peers; submission is never a confirmation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplyProgress {
    /// Selected peer, or absent when no peers are registered.
    pub peer: Option<NodeId>,
    /// Control attempt shares bounded send turns with data for this peer.
    pub control: SendAttempt,
    /// Independent data attempt.
    pub data: SendAttempt,
}

impl Dispatcher {
    /// Queue an already routed four-frame PEER reply. Rejections preserve the
    /// original message. The session must match this exact destination's current
    /// binding, and the opcode must fit the chosen control/data class.
    /// Exact queued control duplicates share one transmission. Distinct controls
    /// retain FIFO order, including different prefixes, scopes, and request IDs.
    pub fn try_reply(
        &mut self,
        class: Class,
        message: Message,
    ) -> Result<(), (ReplyError, Message)> {
        let (peer, bytes) = match self.reply_target(class, &message) {
            Ok(target) => target,
            Err(error) => return Err((error, message)),
        };
        let Some(peer) = self.peers.get_mut(&peer) else {
            return Err((ReplyError::Peer, message));
        };
        if let Some((_, frames)) = message.routed_packet() {
            let limits = self.routes.limits;
            if let Ok(packet) = decode_packet(&frames, limits)
                && packet.envelope.opcode == Opcode::ReplicaState
            {
                let Ok(state) = ozzy_replication::wire::flow_state_route(packet, limits) else {
                    return Err((ReplyError::Frames, message));
                };
                if !self
                    .routes
                    .partitions
                    .contains_key(&state.report.channel.scope.group_id)
                    || !peer.outgoing_channels.bind(state)
                {
                    return Err((ReplyError::Session, message));
                }
            }
        }
        let queue = &mut peer.replies[index(class)];
        if class == Class::Control && queue.contains(&message, 0) {
            return Ok(());
        }
        queue.messages.push_back((message, bytes));
        queue.bytes += bytes;
        Ok(())
    }

    fn reply_target(&self, class: Class, message: &Message) -> Result<(NodeId, usize), ReplyError> {
        if message.len() != 4 && message.len() != 2 {
            return Err(ReplyError::Frames);
        }
        let to = super::service::route_peer(message).ok_or(ReplyError::Peer)?;
        let peer = self.peers.get(&to).ok_or(ReplyError::Peer)?;
        let queue = &peer.replies[index(class)];
        let bytes = message.byte_len();
        if bytes > queue.limits.message_bytes {
            return Err(ReplyError::Size);
        }
        if message.len() == 2 {
            check_compact(peer, class, message)?;
            if queue.lacks_room(bytes) {
                crate::profiling::event(crate::profiling::Event::DispatcherBrokerFull);
                return Err(ReplyError::Full);
            }
            return Ok((to, bytes));
        }
        self.check_packet(peer, class, message)?;
        if queue.lacks_room(bytes) && !(class == Class::Control && queue.contains(message, 0)) {
            crate::profiling::event(if peer.binding.kind == super::Kind::Broker {
                crate::profiling::Event::DispatcherBrokerFull
            } else {
                crate::profiling::Event::DispatcherReplyFull
            });
            return Err(ReplyError::Full);
        }
        Ok((to, bytes))
    }

    /// A routed reply comes from this broker in the peer's current session,
    /// and its opcode fits the chosen class.
    fn check_packet(&self, peer: &Peer, class: Class, message: &Message) -> Result<(), ReplyError> {
        let message_bytes = peer.replies[index(class)].limits.message_bytes;
        let limits = EnvelopeLimits {
            max_metadata_bytes: message_bytes,
            max_payload_bytes: message_bytes,
        };
        let (_, frames) = message.routed_packet().ok_or(ReplyError::Frames)?;
        let packet = decode_packet(&frames, limits).map_err(|_| ReplyError::Frames)?;
        if packet.envelope.sender != self.local
            || packet.envelope.session != Some(peer.binding.session)
        {
            return Err(ReplyError::Session);
        }
        if !class_allows(class, &packet) {
            return Err(ReplyError::Class);
        }
        Ok(())
    }

    /// Attempt one peer's control and data independently, then advance to the
    /// next peer even if both queues were full. The callback must return the
    /// unchanged owning message on `Full`. Fatal socket errors stop the frontend.
    /// Each peer rotates classes after actual progress so recurring controls
    /// cannot monopolize single-slot socket capacity. Full polls retain the turn.
    pub fn flush_replies(
        &mut self,
        mut try_send: impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<ReplyProgress, omq_tokio::Error> {
        let next = crate::turn::after_mut(&mut self.peers, self.replied);
        let mut progress = ReplyProgress {
            peer: next.as_ref().map(|&(id, _)| id),
            control: SendAttempt::Idle,
            data: SendAttempt::Idle,
        };
        if let Some((next, peer)) = next {
            self.replied = Some(next);
            for class in [peer.next_reply_class, 1 - peer.next_reply_class] {
                let attempt = if peer.blocked[class] {
                    SendAttempt::Idle
                } else {
                    flush_class(peer, class, &mut try_send)?
                };
                if class == 0 {
                    progress.control = attempt;
                } else {
                    progress.data = attempt;
                }
                if matches!(attempt, SendAttempt::Submitted | SendAttempt::Unroutable) {
                    peer.next_reply_class = 1 - class;
                }
            }
        }
        Ok(progress)
    }

    /// Front frames of classes OMQ refused, for registering send-progress
    /// waits. Retrying them before that progress only spends the dispatcher's
    /// core.
    pub(super) fn blocked(&self) -> impl Iterator<Item = (NodeId, usize, &Message)> {
        self.peers.iter().flat_map(|(&node, peer)| {
            (0..2).filter_map(move |class| {
                peer.blocked[class]
                    .then(|| peer.replies[class].messages.front())
                    .flatten()
                    .map(|(message, _)| (node, class, message))
            })
        })
    }

    pub(super) fn is_blocked(&self, peer: NodeId, class: usize) -> bool {
        self.peers
            .get(&peer)
            .is_some_and(|peer| peer.blocked[class])
    }

    /// Make a refused class eligible for flushing again.
    pub(super) fn unblock(&mut self, peer: NodeId, class: usize) {
        if let Some(peer) = self.peers.get_mut(&peer) {
            peer.blocked[class] = false;
        }
    }

    /// Attempt one class of one peer, outside the round-robin rotation.
    pub(super) fn flush_peer_class(
        &mut self,
        peer: NodeId,
        class: usize,
        mut try_send: impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<SendAttempt, omq_tokio::Error> {
        match self.peers.get_mut(&peer) {
            Some(peer) => flush_class(peer, class, &mut try_send),
            None => Ok(SendAttempt::Idle),
        }
    }

    /// Whether bounded outgoing PEER queues contain unsent messages.
    pub fn has_replies(&self) -> bool {
        self.peers
            .values()
            .any(|peer| peer.replies.iter().any(|queue| !queue.messages.is_empty()))
    }

    /// Queued message count and logical frame bytes for one destination/class.
    pub fn queued_replies(&self, peer: NodeId, class: Class) -> Option<(usize, usize)> {
        let queue = &self.peers.get(&peer)?.replies[index(class)];
        Some((queue.messages.len(), queue.bytes))
    }
}

/// Compact flow state goes only to a broker over control, on a bound channel.
fn check_compact(peer: &Peer, class: Class, message: &Message) -> Result<(), ReplyError> {
    let [_, state] = message.frames().ok_or(ReplyError::Frames)?;
    let compact =
        ozzy_replication::wire::CompactState::decode(state).map_err(|_| ReplyError::Frames)?;
    if peer.binding.kind != super::Kind::Broker
        || class != Class::Control
        || peer.outgoing_channels.channel(compact.handle).is_none()
    {
        return Err(ReplyError::Session);
    }
    Ok(())
}

/// Data carries transfer and delivery opcodes; control carries the rest
/// without payload. Client requests never travel as replies.
fn class_allows(class: Class, packet: &ozzy_proto::Packet<'_>) -> bool {
    let data = matches!(
        packet.envelope.opcode,
        Opcode::PrepareFlow | Opcode::Ops | Opcode::SnapshotChunk | Opcode::Records
    );
    data == (class == Class::Data)
        && (data || packet.payload.is_empty())
        && !matches!(
            packet.envelope.opcode,
            Opcode::Prepare
                | Opcode::Append
                | Opcode::Subscribe
                | Opcode::Unsubscribe
                | Opcode::ProgressCommit
                | Opcode::GroupJoin
                | Opcode::GroupAssign
                | Opcode::GroupInspect
                | Opcode::GroupDelete
        )
}

fn flush_class(
    peer: &mut super::dispatch::Peer,
    class: usize,
    try_send: &mut impl FnMut(Message) -> Result<(), TrySendError>,
) -> Result<SendAttempt, omq_tokio::Error> {
    let attempt = peer.replies[class].flush(try_send)?;
    match attempt {
        SendAttempt::Submitted => {
            crate::profiling::event(crate::profiling::Event::DispatcherReplySent);
        }
        SendAttempt::Blocked => {
            peer.blocked[class] = true;
            crate::profiling::event(if peer.binding.kind == super::Kind::Broker {
                crate::profiling::Event::DispatcherBrokerBlocked
            } else {
                crate::profiling::Event::DispatcherReplyBlocked
            });
        }
        SendAttempt::Idle | SendAttempt::Unroutable => {}
    }
    Ok(attempt)
}

const fn index(class: Class) -> usize {
    match class {
        Class::Control => 0,
        Class::Data => 1,
    }
}

/// Reply admission failed before socket submission; the sender retains its bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReplyError {
    /// Destination is not a currently bound peer.
    #[error("unknown reply peer")]
    Peer,
    /// Incorrect frame count or native framing.
    #[error("invalid reply frames")]
    Frames,
    /// Obsolete destination session or incorrect local sender.
    #[error("obsolete reply session")]
    Session,
    /// Opcode/payload cannot consume the selected traffic class.
    #[error("invalid reply traffic class")]
    Class,
    /// A single reply exceeds its configured frame-byte limit.
    #[error("reply exceeds message byte limit")]
    Size,
    /// Per-peer count or aggregate frame-byte allowance is occupied.
    #[error("reply queue full")]
    Full,
}

#[cfg(test)]
mod tests;
