use std::ops::Bound::{Excluded, Unbounded};

use omq_tokio::{Message, TrySendError};
use ozzy_proto::{EnvelopeLimits, NodeId, Opcode, decode_packet};

use super::{Dispatcher, SetupError};
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
        match self.reply_target(class, &message) {
            Ok((peer, bytes)) => {
                if message.len() == 4 {
                    let frames =
                        std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
                    let limits = self.routes.limits;
                    if let Ok(packet) = decode_packet(&frames, limits)
                        && packet.envelope.opcode == Opcode::ReplicaState
                    {
                        let Ok(state) = ozzy_replication::wire::flow_state_route(packet, limits)
                        else {
                            return Err((ReplyError::Frames, message));
                        };
                        if !self
                            .routes
                            .partitions
                            .contains_key(&state.report.channel.scope.group_id)
                            || !self
                                .peers
                                .get_mut(&peer)
                                .expect("bound peer")
                                .outgoing_channels
                                .bind(state)
                        {
                            return Err((ReplyError::Session, message));
                        }
                    }
                }
                let queue =
                    &mut self.peers.get_mut(&peer).expect("validated peer").replies[index(class)];
                if class == Class::Control && queue.contains(&message, 0) {
                    return Ok(());
                }
                queue.messages.push_back((message, bytes));
                queue.bytes += bytes;
                Ok(())
            }
            Err(error) => Err((error, message)),
        }
    }

    fn reply_target(&self, class: Class, message: &Message) -> Result<(NodeId, usize), ReplyError> {
        if message.len() != 4 && message.len() != 2 {
            return Err(ReplyError::Frames);
        }
        let to = NodeId::from_bytes(
            message
                .part_slice(0)
                .and_then(|part| part.try_into().ok())
                .ok_or(ReplyError::Peer)?,
        );
        let peer = self.peers.get(&to).ok_or(ReplyError::Peer)?;
        let queue = &peer.replies[index(class)];
        let bytes = message.byte_len();
        if bytes > queue.limits.message_bytes {
            return Err(ReplyError::Size);
        }
        let limits = EnvelopeLimits {
            max_metadata_bytes: queue.limits.message_bytes,
            max_payload_bytes: queue.limits.message_bytes,
        };
        if message.len() == 2 {
            let compact = ozzy_replication::wire::CompactState::decode(
                message.part_slice(1).unwrap_or_default(),
            )
            .map_err(|_| ReplyError::Frames)?;
            if peer.binding.kind != super::Kind::Broker
                || class != Class::Control
                || peer.outgoing_channels.channel(compact.handle).is_none()
            {
                return Err(ReplyError::Session);
            }
            if queue.messages.len() == queue.limits.messages
                || bytes > queue.limits.bytes - queue.bytes
            {
                return Err(ReplyError::Full);
            }
            return Ok((to, bytes));
        }
        let frames =
            std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).expect("four frames"));
        let packet = decode_packet(&frames, limits).map_err(|_| ReplyError::Frames)?;
        if packet.envelope.sender != self.local
            || packet.envelope.session != Some(peer.binding.session)
        {
            return Err(ReplyError::Session);
        }
        let data = matches!(
            packet.envelope.opcode,
            Opcode::PrepareFlow | Opcode::Ops | Opcode::Records
        );
        if data != (class == Class::Data)
            || (!data && !packet.payload.is_empty())
            || matches!(
                packet.envelope.opcode,
                Opcode::Prepare | Opcode::Append | Opcode::Subscribe | Opcode::Unsubscribe
            )
        {
            return Err(ReplyError::Class);
        }
        if (queue.messages.len() == queue.limits.messages
            || bytes > queue.limits.bytes - queue.bytes)
            && !(class == Class::Control && queue.contains(message, 0))
        {
            return Err(ReplyError::Full);
        }
        Ok((to, bytes))
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
        let next = self
            .replied
            .and_then(|last| self.peers.range((Excluded(last), Unbounded)).next())
            .or_else(|| self.peers.first_key_value())
            .map(|(node, _)| *node);
        let mut progress = ReplyProgress {
            peer: next,
            control: SendAttempt::Idle,
            data: SendAttempt::Idle,
        };
        if let Some(next) = next {
            self.replied = Some(next);
            let peer = self.peers.get_mut(&next).expect("selected peer");
            for class in [peer.next_reply_class, 1 - peer.next_reply_class] {
                let attempt = peer.replies[class].flush(&mut try_send)?;
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
