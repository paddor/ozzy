//! Bounded observation and retry of shard-to-dispatcher replies.

use super::failure;
use crate::StartupError;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{EnvelopeLimits, NodeId, Opcode};
use ozzy_runtime::{
    dispatch::Class,
    frontend::{Pending, Port, PublicationError, PublicationResult, ReplyError, ReplyResult},
};
use std::{
    collections::{BTreeMap, VecDeque, btree_map},
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

enum State {
    Ready(Message),
    Pending(Pending<ReplyResult>),
}

enum Publication {
    Ready(Message),
    Pending(Pending<PublicationResult>),
}
struct Entry {
    peer: NodeId,
    class: Class,
    bytes: usize,
    state: State,
    retry: Duration,
    queued: Option<Instant>,
    /// Send order across this shard's replies. One destination class receives
    /// its replies in this order; the dispatcher refuses a later sequence
    /// while an earlier refused one awaits re-admission.
    sequence: u64,
}

pub(super) struct Outbound {
    slots: Vec<Option<Entry>>,
    /// Free data and control slots. Data slots come first in `slots`.
    free: [Vec<usize>; 2],
    usage: BTreeMap<(NodeId, Class), (usize, usize)>,
    limits: [usize; 2],
    sequence: u64,
    /// Occupied slots in send order, oldest first. A turn visits only these.
    order: VecDeque<(u64, usize)>,
    /// Destination classes whose oldest unsent reply stayed unsent this turn.
    held: Vec<(NodeId, Class)>,
    publications: [Option<Publication>; 2],
    publication_bytes: usize,
    envelope: EnvelopeLimits,
    /// A send was refused as full. The next freed slot wakes the shard so
    /// the refused partition retries; other releases wake nobody.
    refused: bool,
    /// The port refused a reply or publication in the last poll. Only then
    /// does returning port capacity need to wake the shard.
    port_refused: bool,
}

impl Outbound {
    /// The earliest retry of a refused reply. Nothing else announces it.
    pub(super) fn next_retry(&self, now: Duration) -> Option<Duration> {
        self.order
            .iter()
            .filter_map(|&(_, index)| self.slots[index].as_ref())
            .filter(|entry| matches!(entry.state, State::Ready(_)))
            .map(|entry| entry.retry)
            .filter(|retry| *retry > now)
            .min()
    }

    pub(super) fn new(peers: usize, message_bytes: usize, envelope: EnvelopeLimits) -> Self {
        let slots = peers.saturating_mul(12);
        let data = slots / 3;
        Self {
            slots: (0..slots).map(|_| None).collect(),
            // Popped from the back, so the lowest index is reused first.
            free: [(0..data).rev().collect(), (data..slots).rev().collect()],
            usage: BTreeMap::new(),
            limits: [message_bytes.saturating_mul(4), 1024 * 1024],
            sequence: 0,
            order: VecDeque::with_capacity(slots),
            held: Vec::with_capacity(8),
            publications: [None, None],
            publication_bytes: message_bytes.saturating_add(2048),
            envelope,
            refused: false,
            port_refused: false,
        }
    }

    /// Whether returning port capacity should wake the shard.
    pub(super) fn port_refused(&self) -> bool {
        self.port_refused
    }

    pub(super) fn send(&mut self, message: Message) -> Result<(), TrySendError> {
        let frames: [&[u8]; 3] =
            std::array::from_fn(|index| message.part_slice(index + 1).unwrap_or_default());
        let opcode = ozzy_proto::decode_packet(&frames, self.envelope)
            .ok()
            .map(|packet| packet.envelope.opcode);
        if opcode == Some(Opcode::PreparePub)
            || message
                .part_slice(0)
                .is_some_and(|prefix| prefix.len() == 32)
        {
            // Retain each partition's prepared frame until this shared slot
            // is available. Publication never consumes reply/control slots.
            let slot = usize::from(opcode != Some(Opcode::PreparePub));
            if self.publications[slot].is_some() {
                self.refused = true;
                return Err(TrySendError::Full(message));
            }
            self.publications[slot] = Some(Publication::Ready(message));
            return Ok(());
        }
        let Some(peer) = message
            .part_slice(0)
            .and_then(|bytes| bytes.try_into().ok())
            .map(NodeId::from_bytes)
        else {
            return Err(TrySendError::Error(omq_tokio::Error::Config(
                "invalid shard reply routing".into(),
            )));
        };
        let class = match opcode {
            Some(Opcode::PrepareFlow | Opcode::Ops | Opcode::SnapshotChunk | Opcode::Records) => {
                Class::Data
            }
            _ => Class::Control,
        };
        self.queue(peer, class, message)
    }

    /// Charge one reply to its destination class and take a free slot, or
    /// refuse it while that class is at its count or byte limit.
    fn queue(&mut self, peer: NodeId, class: Class, message: Message) -> Result<(), TrySendError> {
        let index = usize::from(class != Class::Data);
        let bytes = message.max_message_size_len().saturating_add(2048);
        let (count, charged) = self.usage.get(&(peer, class)).copied().unwrap_or_default();
        let count_limit = if class == Class::Data { 4 } else { 8 };
        let charged = charged
            .checked_add(bytes)
            .filter(|&total| count < count_limit && total <= self.limits[index]);
        let (Some(charged), Some(sequence)) = (charged, self.sequence.checked_add(1)) else {
            self.refused = true;
            return Err(TrySendError::Full(message));
        };
        let Some(slot) = self.free[index].pop() else {
            self.refused = true;
            return Err(TrySendError::Full(message));
        };
        // The count limit above keeps this far from overflow.
        self.usage
            .insert((peer, class), (count.saturating_add(1), charged));
        self.sequence = sequence;
        self.order.push_back((self.sequence, slot));
        self.slots[slot] = Some(Entry {
            peer,
            class,
            bytes,
            state: State::Ready(message),
            retry: Duration::ZERO,
            queued: ozzy_runtime::profiling::start(),
            sequence: self.sequence,
        });
        Ok(())
    }

    pub(super) fn poll(
        &mut self,
        cx: &mut Context<'_>,
        port: &mut Port,
        now: Duration,
    ) -> Result<(), StartupError> {
        port.poll_progress(cx).map_err(failure)?;
        self.port_refused = false;
        for slot in 0..2 {
            self.poll_publication(cx, port, slot)?;
        }
        // Visit the oldest entries first, at most 64 per turn. One destination
        // class is handed over in send order: once its oldest unsent reply
        // stays unsent, its later replies wait for a following turn.
        self.held.clear();
        let mut released = false;
        for position in 0..self.order.len().min(64) {
            let index = self.order[position].1;
            let Some(entry) = self.slots[index].take() else {
                continue;
            };
            // A reply handed over now is polled at once, which registers
            // this turn's waker for its result.
            let mut entry = self.offer(entry, port, now)?;
            if let State::Pending(pending) = &mut entry.state {
                match Pin::new(pending).poll(cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(Ok(()) | Err((ReplyError::Peer | ReplyError::Session, _)))) => {
                        self.release(index, &entry);
                        released = true;
                        continue;
                    }
                    Poll::Ready(Ok(Err((ReplyError::Full, message)))) => {
                        ozzy_runtime::profiling::event(
                            ozzy_runtime::profiling::Event::ShardReplyRefusal,
                        );
                        entry.state = State::Ready(message);
                        entry.retry = now.saturating_add(Duration::from_millis(10));
                    }
                    Poll::Ready(Ok(Err((error, _)))) => return Err(failure(error)),
                    Poll::Ready(Err(error)) => return Err(failure(error)),
                }
            }
            let destination = (entry.peer, entry.class);
            if matches!(entry.state, State::Ready(_)) && !self.held.contains(&destination) {
                self.held.push(destination);
            }
            self.slots[index] = Some(entry);
        }
        if released {
            let slots = &self.slots;
            self.order.retain(|&(_, index)| slots[index].is_some());
            self.wake_refused(cx);
        }
        Ok(())
    }

    /// Wake the shard for a partition whose send was refused as full.
    fn wake_refused(&mut self, cx: &Context<'_>) {
        if std::mem::take(&mut self.refused) {
            cx.waker().wake_by_ref();
        }
    }

    /// Free a delivered reply's slot and its destination class charge.
    fn release(&mut self, index: usize, entry: &Entry) {
        ozzy_runtime::profiling::finish(
            ozzy_runtime::profiling::Stage::ShardReplyQueue,
            entry.queued,
        );
        if let btree_map::Entry::Occupied(mut usage) = self.usage.entry((entry.peer, entry.class)) {
            let (count, bytes) = usage.get_mut();
            *count = count.saturating_sub(1);
            *bytes = bytes.saturating_sub(entry.bytes);
            if *count == 0 {
                usage.remove();
            }
        }
        self.free[usize::from(entry.class != Class::Data)].push(index);
    }

    /// Offer a due reply to the dispatcher port unless an older reply of its
    /// destination class is still unsent. A reply left unsent holds back the
    /// rest of its class this turn.
    fn offer(
        &mut self,
        mut entry: Entry,
        port: &mut Port,
        now: Duration,
    ) -> Result<Entry, StartupError> {
        let destination = (entry.peer, entry.class);
        if now >= entry.retry
            && !self.held.contains(&destination)
            && let State::Ready(message) = entry.state
        {
            entry.state = match port.try_reply(entry.class, message, entry.bytes, entry.sequence) {
                Ok(pending) => State::Pending(pending),
                Err((ozzy_runtime::frontend::PortError::Admission(_), message)) => {
                    ozzy_runtime::profiling::event(
                        ozzy_runtime::profiling::Event::ShardPortRefusal,
                    );
                    entry.retry = now.saturating_add(Duration::from_millis(10));
                    self.port_refused = true;
                    State::Ready(message)
                }
                Err((error, _)) => return Err(failure(error)),
            };
        }
        Ok(entry)
    }

    fn poll_publication(
        &mut self,
        cx: &mut Context<'_>,
        port: &mut Port,
        slot: usize,
    ) -> Result<(), StartupError> {
        let mut pending = match self.publications[slot].take() {
            None => return Ok(()),
            Some(Publication::Pending(pending)) => pending,
            // A publication handed over now is polled at once, which
            // registers this turn's waker for its result.
            Some(Publication::Ready(message)) => {
                match port.try_publish(message, self.publication_bytes) {
                    Ok(pending) => pending,
                    Err((ozzy_runtime::frontend::PortError::Admission(_), message)) => {
                        self.publications[slot] = Some(Publication::Ready(message));
                        self.port_refused = true;
                        return Ok(());
                    }
                    Err((error, _)) => return Err(failure(error)),
                }
            }
        };
        match Pin::new(&mut pending).poll(cx) {
            Poll::Pending => self.publications[slot] = Some(Publication::Pending(pending)),
            Poll::Ready(Ok(Err((PublicationError::Full, message)))) => {
                self.publications[slot] = Some(Publication::Ready(message));
                cx.waker().wake_by_ref();
            }
            Poll::Ready(Ok(_)) => self.wake_refused(cx),
            Poll::Ready(Err(error)) => return Err(failure(error)),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
