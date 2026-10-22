//! Shard-side native requests using independently established shared links.

use std::{pin::Pin, task::Context};

use omq_tokio::{Message, TrySendError};
use ozzy_proto::{
    Envelope, GroupId, NodeId, Opcode, Packet, append,
    data::DataLimits,
    handshake,
    nack::{AuthorityHint, RetryClass},
    producer,
};

use super::{PendingProposal, ProposalOutcome, ProposalSubmitError, ProposalSubmitter};
use crate::{frontend::Link, replica_journal::ProposalBuffer};

mod access;
mod completion;
mod config;
mod control;
mod error;
mod prepare;
mod progress;
mod work;
pub use access::NativeAccess;
use access::Writers;
use completion::Visit;
pub use config::NativeIntakeConfig;
use config::validate;
pub use error::NativeIntakeError;
use prepare::{PrepareFailure, prepare_failure};

/// Joined progress updates tracked inline per batch.
const PROGRESS_BATCH: usize = 16;

/// Request disposition. None of these establish confirmation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeReceive {
    /// Proposal or bounded rejection reply occupies an existing slot.
    Accepted,
    /// Obsolete, malformed, or unauthorized link input was discarded.
    Ignored,
    /// No request slot was available. Retry the unchanged identity later.
    Busy,
}

#[derive(Clone, Copy, Debug)]
struct Request {
    envelope: Envelope,
    send: DataLimits,
    command: Command,
}

#[derive(Clone, Copy, Debug)]
enum Command {
    Open,
    Append(append::stream::Confirmed),
    Progress,
    Assign,
}

#[derive(Debug)]
struct Pending {
    request: Request,
    proposal: Pin<Box<PendingProposal>>,
    started: Option<std::time::Instant>,
}

#[derive(Debug)]
struct Reply {
    request: Request,
    outcome: ProposalOutcome,
    next: usize,
    frame: Option<Message>,
}

#[derive(Debug)]
struct Slot {
    buffer: Option<ProposalBuffer>,
    pending: Option<Pending>,
    reply: Option<Reply>,
    /// A prepared progress update held for, or riding in, this partition's
    /// progress proposal.
    batch: Option<Batched>,
}

impl Slot {
    /// Whether the slot owns a proposal or a reply.
    const fn active(&self) -> bool {
        self.pending.is_some() || self.reply.is_some()
    }
}

/// One progress update outside its own proposal.
#[derive(Debug)]
struct Batched {
    request: Request,
    started: Option<std::time::Instant>,
    state: BatchState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchState {
    /// Waits for the proposal in flight; `solo` updates go in their own.
    Held { solo: bool },
    /// Joined the carrier slot's proposal as its `index`th extra update.
    Riding { carrier: usize, index: usize },
}

/// One partition's external client adapter. It owns no socket, journal state,
/// timer, or replication authority. Poll beside the partition actor; no task or
/// thread is spawned. Each writer has independent data slots and one control slot.
#[derive(Debug)]
pub struct NativeIntake {
    config: NativeIntakeConfig,
    submitter: ProposalSubmitter,
    slots: Vec<Slot>,
    metadata: Vec<u8>,
    next: usize,
    remaining: usize,
    /// Slots that own a proposal, a reply, or a batched progress update.
    busy: usize,
    /// One bit per slot that owns a proposal or a reply: the slots a poll
    /// visits. Idle slots are skipped a word at a time.
    active: Vec<u64>,
    /// Slot whose progress proposal is in flight. Updates received meanwhile
    /// wait and then share the next one.
    progress_flight: Option<usize>,
    /// Updates held for the next progress proposal.
    progress_held: usize,
    /// Updates riding in the progress proposal in flight.
    progress_riders: usize,
    /// First slot of the progress updates, after every writer's slots.
    progress_start: usize,
    /// Progress proposals submitted, for tests.
    #[cfg(test)]
    progress_proposals: usize,
    writers: Writers,
}

impl NativeIntake {
    /// Leases come from this partition actor before shared scheduling starts.
    /// Requires exactly `config.required_buffers()`
    /// nonempty-capacity, empty arenas. Dynamic clients have independent
    /// rejection slots. The caller backs ingress with shard/lane reservations.
    pub fn new(
        config: NativeIntakeConfig,
        submitter: ProposalSubmitter,
        buffers: Vec<ProposalBuffer>,
    ) -> Result<Self, NativeIntakeError> {
        validate(&config, &buffers)?;
        let active = vec![0; buffers.len().div_ceil(64)];
        let progress_start = config
            .access
            .required_buffers(config.requests_per_writer)
            .ok_or(NativeIntakeError::Configuration)?;
        Ok(Self {
            writers: Writers::new(config.access.clone()),
            config,
            submitter,
            slots: buffers
                .into_iter()
                .map(|buffer| Slot {
                    buffer: Some(buffer),
                    pending: None,
                    reply: None,
                    batch: None,
                })
                .collect(),
            metadata: Vec::with_capacity(256),
            next: 0,
            remaining: 0,
            busy: 0,
            active,
            progress_flight: None,
            progress_held: 0,
            progress_riders: 0,
            progress_start,
            #[cfg(test)]
            progress_proposals: 0,
        })
    }

    /// Validate the latest independently established link before copying a
    /// request into an arena. The caller rechecks queued dispatch session stamps.
    /// Pending replies are checked against that link again during progress.
    pub fn receive(
        &mut self,
        message: &Message,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let Some((peer, packet)) = self.packet(message, link) else {
            return Ok(NativeReceive::Ignored);
        };
        if packet.envelope.opcode == Opcode::ProgressCommit {
            return self.receive_progress(packet, link, hint);
        }
        if packet.envelope.opcode == Opcode::GroupAssign {
            return self.receive_assign(packet, link, hint);
        }
        let request = Request {
            envelope: packet.envelope,
            send: link.send,
            command: Command::Open,
        };
        if link.send.envelope.max_metadata_bytes < 107 {
            return Ok(NativeReceive::Ignored);
        }
        let selected = self.select_writer(packet, link, hint);
        let writer = match selected {
            Ok(Some(writer)) => writer,
            Ok(None) if packet.envelope.opcode == Opcode::Append => {
                return Ok(NativeReceive::Busy);
            }
            // A selected writer was taken above; only refusals reach here.
            result => {
                let refusal = result.err().unwrap_or((10, RetryClass::AfterBackoff));
                return self.refuse_client(peer, request, refusal, hint);
            }
        };
        let Some(slot) = self.free_slot(writer, packet.envelope.opcode) else {
            crate::profiling::event(crate::profiling::Event::NativeSlotRefusal);
            return Ok(NativeReceive::Busy);
        };
        // New input may target an already visited slot. Complete a fresh bounded
        // scan so its completion receiver is polled before the scheduler sleeps.
        self.remaining = self.slots.len();
        let prepared = self.prepare(slot, packet, link, hint);
        match prepared {
            Ok(command) => {
                let request = Request { command, ..request };
                return self.submit_prepared(slot, request, hint);
            }
            Err(PrepareFailure::Busy) => {
                crate::profiling::event(crate::profiling::Event::NativeArenaRefusal);
                if let Some(buffer) = self.slots[slot].buffer.as_mut() {
                    buffer.clear();
                }
                return Ok(NativeReceive::Busy);
            }
            Err(PrepareFailure::Reject(code, retry)) => {
                self.reject(slot, request, (code, retry), hint)?;
            }
        }
        Ok(NativeReceive::Accepted)
    }

    /// Return bounded retry metadata when shard initialization has not finished.
    /// This consumes no proposal slot and establishes no producer state.
    pub fn defer(
        &mut self,
        message: &Message,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let Some((peer, packet)) = self.packet(message, link) else {
            return Ok(NativeReceive::Ignored);
        };
        let request = Request {
            envelope: packet.envelope,
            send: link.send,
            command: Command::Open,
        };
        self.refuse_client(peer, request, (10, RetryClass::AfterBackoff), hint)
    }

    /// Answer a refused request in its client's rejection slot, or `Busy`
    /// while that slot is taken.
    fn refuse_client(
        &mut self,
        peer: usize,
        request: Request,
        refusal: (u16, RetryClass),
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let slot = self.writers.rejection_slot(peer, self.stride());
        if !self.slot_free(slot) {
            return Ok(NativeReceive::Busy);
        }
        self.reject(slot, request, refusal, hint)?;
        self.remaining = self.slots.len();
        Ok(NativeReceive::Accepted)
    }

    /// Slots per writer: its control slot, then its request slots.
    const fn stride(&self) -> usize {
        self.config.requests_per_writer + 1
    }

    fn packet<'a>(&mut self, message: &'a Message, link: Link) -> Option<(usize, Packet<'a>)> {
        let packet = super::response::client_packet(
            message,
            link,
            self.config.local,
            self.config.limits.envelope,
            &[
                Opcode::OpenProducer,
                Opcode::Append,
                Opcode::ProgressCommit,
                Opcode::GroupAssign,
            ],
        )?;
        let peer = self.writers.peer(link.binding.peer)?;
        Some((peer, packet))
    }

    fn slot_free(&self, index: usize) -> bool {
        let slot = &self.slots[index];
        slot.buffer.is_some() && !slot.active() && slot.batch.is_none()
    }

    fn select_writer(
        &mut self,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<Option<usize>, (u16, RetryClass)> {
        if link.remote.roles & handshake::PRODUCER == 0 {
            return Err((3, RetryClass::Permanent));
        }
        let (authority, partition, writer) = if packet.envelope.opcode == Opcode::OpenProducer {
            let open = producer::decode_open(packet, self.config.limits.envelope)
                .map_err(|_| (1, RetryClass::Permanent))?;
            (open.authority, open.partition, open.producer)
        } else {
            let route = append::route(packet, self.config.limits.envelope)
                .map_err(|_| (1, RetryClass::Permanent))?;
            (route.authority, route.partition, route.key.producer_id)
        };
        if partition != self.config.partition || writer.as_bytes() == &[0; 16] {
            return Err((3, RetryClass::Permanent));
        }
        if !super::response::leads(self.config.group, self.config.local, authority, hint) {
            return Err((5, RetryClass::AfterAuthorityRefresh));
        }
        let stride = self.stride();
        if packet.envelope.opcode == Opcode::OpenProducer {
            self.writers.attach(link, writer, &self.slots, stride);
        } else if self.writers.revoked(link.binding.peer, writer) {
            return Err((3, RetryClass::Permanent));
        }
        Ok(self.writers.select(link, writer, &self.slots, stride))
    }

    fn free_slot(&self, peer: usize, opcode: Opcode) -> Option<usize> {
        let first = peer * self.stride();
        let range = if opcode == Opcode::OpenProducer {
            first..first + 1
        } else {
            first + 1..first + 1 + self.config.requests_per_writer
        };
        range.into_iter().find(|&index| self.slot_free(index))
    }

    /// Observe at most `turn_slots` slots. `current` returns the latest link;
    /// session replacement discards old replies without canceling admitted work.
    /// Full sends retain their exact frame and continue with other destinations.
    /// The caller must arrange a wakeup when its bounded reply path gains space.
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        hint: AuthorityHint,
        mut current: impl FnMut(NodeId) -> Option<Link>,
        mut try_send: impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, NativeIntakeError> {
        let mut changed = self.dispatch_progress(hint, &mut current)?;
        if self.remaining == 0 {
            self.remaining = self.slots.len();
        }
        if self.busy == 0 {
            self.remaining = 0;
        }
        // Only slots with a proposal or reply count against the turn bound.
        // A scan that finds no work ends in this call and asks for no turn.
        let len = self.slots.len();
        let mut turn = self.config.turn_slots;
        while turn > 0 && self.remaining > 0 {
            let index = self.next;
            let offset = index % 64;
            let idle = ((self.active[index / 64] >> offset).trailing_zeros() as usize)
                .min(64 - offset)
                .min(len - index)
                .min(self.remaining);
            if idle != 0 {
                self.next = (index + idle) % len;
                self.remaining -= idle;
                continue;
            }
            self.next = (index + 1) % len;
            self.remaining -= 1;
            turn -= 1;
            changed |= match self.poll_slot(index, cx, &mut current)? {
                Visit::Done(changed) => changed,
                Visit::Reply(changed) => self.send_reply(index, cx, hint, &mut try_send)? | changed,
            };
            self.track(index);
        }
        if changed || self.remaining > 0 {
            cx.waker().wake_by_ref();
        }
        self.writers
            .reclaim(&self.slots, self.stride(), &mut current);
        Ok(changed)
    }

    /// Record whether `index` owns a proposal or a reply after it changed.
    fn track(&mut self, index: usize) {
        let bit = 1 << (index % 64);
        if self.slots[index].active() {
            self.active[index / 64] |= bit;
        } else {
            self.active[index / 64] &= !bit;
        }
    }

    /// Progress updates riding in another update's proposal.
    #[cfg(test)]
    pub(crate) const fn progress_riders(&self) -> usize {
        self.progress_riders
    }

    /// Cursor-update proposals this intake submitted.
    #[cfg(test)]
    pub(crate) const fn progress_proposals(&self) -> usize {
        self.progress_proposals
    }

    pub(super) fn identity(&self) -> (GroupId, NodeId, append::Policy) {
        (self.config.group, self.config.local, self.config.policy)
    }
}
