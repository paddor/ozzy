//! Bounded outbound scheduling for one fixed-voter replica actor.
//!
//! One actor owns these queues; no extra thread, channel, or sender task is needed.
//! Poll sends alongside receive, protocol timers, and disk completions. Successful
//! OMQ admission is never delivery, durability, or commit evidence. The replica
//! core/payload owner must retain retry history independently of this outbox.
//!
//! This is not a complete replica runtime or an authentication layer. Supply a
//! PEER socket with bounded HWM and `router_mandatory(true)`, and independently
//! bind configured voters to authenticated sessions before interpreting traffic.

use std::collections::VecDeque;
use std::future::{Future, pending, poll_fn};
use std::task::Poll;

use bytes::Bytes;
use omq_tokio::{Error, IdentitySocket, Message, TrySendError};
use ozzy_proto::NodeId;
use ozzy_replication::Configuration;

#[cfg(test)]
mod tests;

/// Independently reserved outbound work classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendClass {
    /// ACK, commit, election, and session controls. Never share data capacity.
    Control,
    /// Bounded prepare/history chunks. Bulk transfers must be chunked upstream.
    Data,
    /// Latest unsolicited receipt/credit report. One coalesced slot per peer.
    Receipt,
    /// Latest correlated flow probe or response. One independent coalesced slot.
    Exchange,
}

impl SendClass {
    const fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Data => 1,
            Self::Receipt => 2,
            Self::Exchange => 3,
        }
    }
}

/// Hard per-peer, per-class bounds, independent of OMQ's own outbound buffers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueLimits {
    /// Maximum queued messages. Slots are reserved at construction.
    pub messages: usize,
    /// Maximum summed frame bytes, including the 16-byte routing identity.
    pub bytes: usize,
    /// Maximum bytes of one routed message, including all frames.
    pub message_bytes: usize,
}

impl QueueLimits {
    pub(crate) fn validate(self) -> Result<(), OutboxError> {
        if self.messages == 0 || self.message_bytes < 16 || self.message_bytes > self.bytes {
            return Err(OutboxError::Limits);
        }
        Ok(())
    }
}

/// Result of one nonblocking transmission attempt, not peer receipt evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendAttempt {
    /// No queued message in this class.
    Idle,
    /// Exact message retained because that peer's OMQ buffers are full.
    Blocked,
    /// Ownership passed to OMQ. Replica ACK/retransmission is still required.
    Submitted,
    /// No route existed. This transmission was discarded, not its retry history.
    /// The adapter must retry from retained protocol state on its retry schedule.
    Unroutable,
}

/// One peer's independent control/data results for a bounded flush round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerFlush {
    /// Configured destination voter.
    pub peer: NodeId,
    /// Control attempt, sharing bounded send turns with the other classes.
    pub control: SendAttempt,
    /// Latest correlated flow exchange, attempted independently of receipt updates.
    pub exchange: SendAttempt,
    /// Latest volatile receipt/credit report, never sharing durable-control capacity.
    pub receipt: SendAttempt,
    /// Data attempt. A blocked peer never prevents trying the other voter.
    pub data: SendAttempt,
}

/// Exactly two peers, at most one message per class per peer per flush round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushProgress(pub [PeerFlush; 2]);

impl FlushProgress {
    /// Whether a transmission left the outbox, successfully or as unroutable.
    pub fn advanced(self) -> bool {
        self.0.iter().any(|peer| {
            [peer.control, peer.exchange, peer.receipt, peer.data]
                .iter()
                .any(|attempt| matches!(attempt, SendAttempt::Submitted | SendAttempt::Unroutable))
        })
    }
}

#[derive(Debug)]
pub(crate) struct Queue {
    pub(crate) messages: VecDeque<(Message, usize)>,
    pub(crate) bytes: usize,
    pub(crate) limits: QueueLimits,
}

impl Queue {
    /// Compare exact frames, optionally excluding a stored routing prefix.
    pub(crate) fn contains(&self, message: &Message, skip: usize) -> bool {
        self.messages.iter().any(|(held, _)| {
            held.len() == message.len() + skip
                && (0..message.len())
                    .all(|index| held.part_slice(index + skip) == message.part_slice(index))
        })
    }

    pub(crate) fn new(limits: QueueLimits) -> Result<Self, OutboxError> {
        limits.validate()?;
        let mut messages = VecDeque::new();
        messages.try_reserve_exact(limits.messages)?;
        Ok(Self {
            messages,
            bytes: 0,
            limits,
        })
    }

    pub(crate) fn flush(
        &mut self,
        try_send: &mut impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<SendAttempt, Error> {
        let Some((message, bytes)) = self.messages.pop_front() else {
            return Ok(SendAttempt::Idle);
        };
        self.bytes -= bytes;
        match try_send(message) {
            Ok(()) => Ok(SendAttempt::Submitted),
            Err(TrySendError::Full(message)) => {
                self.messages.push_front((message, bytes));
                self.bytes += bytes;
                Ok(SendAttempt::Blocked)
            }
            Err(TrySendError::Error(Error::Unroutable)) => Ok(SendAttempt::Unroutable),
            Err(TrySendError::Closed) => Err(Error::Closed),
            Err(TrySendError::Error(error)) => Err(error),
        }
    }
}

#[derive(Debug)]
struct Peer {
    node: NodeId,
    route: Bytes,
    queues: [Queue; 4],
    next_class: usize,
}

/// Single-owner outbox with independent preallocated control/data lanes per voter.
///
/// Bounds count logical bytes; borrowed `Bytes` slices can pin larger backing
/// allocations. The payload owner must bound that retained memory separately.
/// Once OMQ takes ownership, its per-stage limits (including fixed transport and
/// actor buffers), HWM, and maximum message sizes bound those additional buffers.
/// Transport submission never releases replica credit.
#[derive(Debug)]
pub struct ReplicaOutbox {
    peers: [Peer; 2],
}

impl ReplicaOutbox {
    /// Reserve two peers' queues plus two coalesced flow slots per peer.
    /// Each flow slot uses the configured control message-byte limit independently.
    pub fn new(
        configuration: Configuration,
        local: NodeId,
        control: QueueLimits,
        data: QueueLimits,
    ) -> Result<Self, OutboxError> {
        Self::from_voters(configuration.voters(), local, control, data)
    }

    fn from_voters(
        configured: &[NodeId; 3],
        local: NodeId,
        control: QueueLimits,
        data: QueueLimits,
    ) -> Result<Self, OutboxError> {
        if !configured.contains(&local) {
            return Err(OutboxError::LocalVoter);
        }
        // At most 2 * (3 * control.message_bytes + data.message_bytes) per round.
        control
            .message_bytes
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(data.message_bytes))
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(OutboxError::Limits)?;
        let mut voters = configured.iter().copied().filter(|&node| node != local);
        let mut peer = || -> Result<Peer, OutboxError> {
            let node = voters.next().expect("two other configured voters");
            Ok(Peer {
                node,
                route: Bytes::copy_from_slice(node.as_bytes()),
                next_class: 0,
                queues: [
                    Queue::new(control)?,
                    Queue::new(data)?,
                    Queue::new(QueueLimits {
                        messages: 1,
                        bytes: control.message_bytes,
                        message_bytes: control.message_bytes,
                    })?,
                    Queue::new(QueueLimits {
                        messages: 1,
                        bytes: control.message_bytes,
                        message_bytes: control.message_bytes,
                    })?,
                ],
            })
        };
        Ok(Self {
            peers: [peer()?, peer()?],
        })
    }

    /// Enqueue exactly three encoded Ozzy frames, without a routing frame.
    ///
    /// Returns the untouched message on rejection. No wait, implicit retry, or
    /// allocation of additional queue slots occurs. Caller supplies wire-validated
    /// bytes and chooses the appropriate reserved class; no protocol parsing here.
    /// Receipt/Exchange replace their previous unsent value after validating the
    /// replacement. Control keeps one queued copy of an exact retry; distinct
    /// controls and Data retain bounded FIFO admission. Submitted controls can
    /// be queued again because socket admission does not prove delivery.
    pub fn try_enqueue(
        &mut self,
        to: NodeId,
        class: SendClass,
        message: Message,
    ) -> Result<(), (EnqueueError, Message)> {
        let Some(peer) = self.peers.iter_mut().find(|peer| peer.node == to) else {
            return Err((EnqueueError::Peer, message));
        };
        if message.len() != 3
            && !(class == SendClass::Receipt
                && message.len() == 1
                && ozzy_replication::wire::CompactState::decode(message.part_slice(0).unwrap())
                    .is_ok())
        {
            return Err((EnqueueError::Frames, message));
        }
        let queue = &mut peer.queues[class.index()];
        let bytes = message
            .iter()
            .try_fold(16usize, |total, frame| total.checked_add(frame.len()));
        let Some(bytes) = bytes.filter(|&bytes| bytes <= queue.limits.message_bytes) else {
            return Err((EnqueueError::Size, message));
        };
        if matches!(class, SendClass::Control) && queue.contains(&message, 1) {
            return Ok(());
        }
        if matches!(class, SendClass::Receipt | SendClass::Exchange) {
            queue.messages.clear();
            queue.bytes = 0;
        }
        if queue.messages.len() == queue.limits.messages || bytes > queue.limits.bytes - queue.bytes
        {
            return Err((EnqueueError::Full, message));
        }
        queue
            .messages
            .push_back((Message::with_prefix(peer.route.clone(), message), bytes));
        queue.bytes += bytes;
        Ok(())
    }

    /// Queued (message count, routed bytes) for one configured destination/class.
    pub fn queued(&self, peer: NodeId, class: SendClass) -> Option<(usize, usize)> {
        self.peers
            .iter()
            .find(|entry| entry.node == peer)
            .map(|entry| {
                let queue = &entry.queues[class.index()];
                (queue.messages.len(), queue.bytes)
            })
    }

    /// Retire unsent packets for one exact peer/class after their protocol epoch is fenced.
    /// Does not remove retained canonical retry history or claim transport delivery.
    pub fn discard(&mut self, peer: NodeId, class: SendClass) -> Option<(usize, usize)> {
        let peer = self.peers.iter_mut().find(|entry| entry.node == peer)?;
        let queue = &mut peer.queues[class.index()];
        let discarded = (queue.messages.len(), queue.bytes);
        queue.messages.clear();
        queue.bytes = 0;
        Some(discarded)
    }

    /// Whether any class has an unsent transmission, not unacknowledged history.
    pub fn has_pending(&self) -> bool {
        self.peers
            .iter()
            .any(|peer| peer.queues.iter().any(|queue| !queue.messages.is_empty()))
    }

    /// Attempt each class once per peer, beginning with control on a new outbox.
    /// Rotate after each submission so repeated controls cannot starve history
    /// when only one socket slot opens. Blocked polls preserve the next turn.
    /// Hard count and byte bounds apply even when every send succeeds immediately.
    /// Call under the actor's scheduling budget; never drain in an unbounded loop.
    /// Socket/protocol errors are terminal adapter failures, not quorum evidence.
    pub fn flush(&mut self, socket: &IdentitySocket) -> Result<FlushProgress, Error> {
        self.flush_with(|message| crate::transport::try_send_peer(socket, message))
    }

    pub(crate) fn flush_with(
        &mut self,
        mut try_send: impl FnMut(Message) -> Result<(), TrySendError>,
    ) -> Result<FlushProgress, Error> {
        let mut progress = self.peers.each_ref().map(|peer| PeerFlush {
            peer: peer.node,
            control: SendAttempt::Idle,
            exchange: SendAttempt::Idle,
            receipt: SendAttempt::Idle,
            data: SendAttempt::Idle,
        });
        let classes = [
            SendClass::Control,
            SendClass::Exchange,
            SendClass::Receipt,
            SendClass::Data,
        ];
        let starts = self.peers.each_ref().map(|peer| peer.next_class);
        for offset in 0..classes.len() {
            for ((peer, result), start) in self.peers.iter_mut().zip(&mut progress).zip(starts) {
                let turn = (start + offset) % classes.len();
                let class = classes[turn];
                let attempt = peer.queues[class.index()].flush(&mut try_send)?;
                match class {
                    SendClass::Control => result.control = attempt,
                    SendClass::Exchange => result.exchange = attempt,
                    SendClass::Receipt => result.receipt = attempt,
                    SendClass::Data => result.data = attempt,
                }
                if matches!(attempt, SendAttempt::Submitted | SendAttempt::Unroutable) {
                    peer.next_class = (turn + 1) % classes.len();
                }
            }
        }
        Ok(FlushProgress(progress))
    }

    /// Flush once, or await peer space while the actor selects other event sources.
    ///
    /// Cancellation retains all messages not already handed to OMQ. Space waiters
    /// are registered before a second bounded flush, closing the full-to-wait race.
    /// A readiness wake is advisory: another producer can consume the capacity.
    /// Initial polling performs at most two flush rounds (16 send attempts).
    /// Call only while `has_pending()`, otherwise this returns immediately.
    pub async fn flush_ready(&mut self, socket: &IdentitySocket) -> Result<FlushProgress, Error> {
        let progress = self.flush(socket)?;
        if progress.advanced() || !self.has_pending() {
            return Ok(progress);
        }
        // Only the HWM slow path clones probes. No queue slot is relinquished.
        let probes = self.peers.each_ref().map(|peer| {
            peer.queues
                .each_ref()
                .map(|queue| queue.messages.front().map(|(message, _)| message.clone()))
        });
        let ready = async {
            tokio::select! {
                () = wait_for(socket, probes[0][0].as_ref()) => {}
                () = wait_for(socket, probes[0][1].as_ref()) => {}
                () = wait_for(socket, probes[0][2].as_ref()) => {}
                () = wait_for(socket, probes[0][3].as_ref()) => {}
                () = wait_for(socket, probes[1][0].as_ref()) => {}
                () = wait_for(socket, probes[1][1].as_ref()) => {}
                () = wait_for(socket, probes[1][2].as_ref()) => {}
                () = wait_for(socket, probes[1][3].as_ref()) => {}
            }
        };
        tokio::pin!(ready);
        let mut registered = false;
        poll_fn(|cx| {
            let signaled = ready.as_mut().poll(cx).is_ready();
            if !registered || signaled {
                registered = true;
                match self.flush(socket) {
                    Ok(progress) if signaled || progress.advanced() => {
                        return Poll::Ready(Ok(progress));
                    }
                    Err(error) => return Poll::Ready(Err(error)),
                    Ok(_) => {}
                }
            }
            Poll::Pending
        })
        .await
    }
}

async fn wait_for(socket: &IdentitySocket, message: Option<&Message>) {
    match message {
        Some(message) => crate::transport::wait_send_peer(socket, message).await,
        None => pending().await,
    }
}

/// Rejected admission; the accompanying message remains owned by the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueError {
    /// Destination is not one of the two other configured voters.
    Peer,
    /// Expected exactly three Ozzy frames without a routing prefix.
    Frames,
    /// Routed message exceeds its class's hard byte bound.
    Size,
    /// That peer/class has exhausted its count or total-byte budget.
    Full,
}

/// Outbox construction failure before any traffic or storage side effect.
#[derive(Debug, thiserror::Error)]
pub enum OutboxError {
    /// Local node must belong to the immutable configuration.
    #[error("local node is not a configured voter")]
    LocalVoter,
    /// Empty, contradictory, or overflowing queue/scheduling bounds.
    #[error("invalid replica outbox limits")]
    Limits,
    /// Startup could not reserve the configured bounded queue slots.
    #[error(transparent)]
    Allocation(#[from] std::collections::TryReserveError),
}
