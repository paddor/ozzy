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
use crate::{
    frontend::Link,
    replica_journal::{JournalError, ProposalBuffer},
};

mod access;
mod completion;
mod config;
mod control;
mod error;
mod progress;
pub use access::NativeAccess;
use access::Writers;
use completion::Visit;
pub use config::NativeIntakeConfig;
use config::validate;
pub use error::NativeIntakeError;

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

#[derive(Clone, Copy)]
enum PrepareFailure {
    Busy,
    Reject(u16, RetryClass),
}

impl From<(u16, RetryClass)> for PrepareFailure {
    fn from((code, retry): (u16, RetryClass)) -> Self {
        Self::Reject(code, retry)
    }
}

fn prepare_failure(error: &JournalError) -> PrepareFailure {
    match error {
        JournalError::Io(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
            PrepareFailure::Busy
        }
        _ => PrepareFailure::from(super::response::rejection(error)),
    }
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
            result => {
                let slot = self
                    .writers
                    .rejection_slot(peer, self.config.requests_per_writer + 1);
                if !self.slot_free(slot) {
                    return Ok(NativeReceive::Busy);
                }
                // A selected writer was taken above; only refusals reach here.
                let (code, retry) = result.err().unwrap_or((10, RetryClass::AfterBackoff));
                self.reject(slot, request, (code, retry), hint)?;
                self.remaining = self.slots.len();
                return Ok(NativeReceive::Accepted);
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
        let slot = self
            .writers
            .rejection_slot(peer, self.config.requests_per_writer + 1);
        if !self.slot_free(slot) {
            return Ok(NativeReceive::Busy);
        }
        let request = Request {
            envelope: packet.envelope,
            send: link.send,
            command: Command::Open,
        };
        self.reject(slot, request, (10, RetryClass::AfterBackoff), hint)?;
        self.remaining = self.slots.len();
        Ok(NativeReceive::Accepted)
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
        self.slots[index].buffer.is_some()
            && self.slots[index].pending.is_none()
            && self.slots[index].reply.is_none()
            && self.slots[index].batch.is_none()
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
        if packet.envelope.opcode == Opcode::OpenProducer {
            self.writers.attach(
                link,
                writer,
                &self.slots,
                self.config.requests_per_writer + 1,
            );
        } else if self.writers.revoked(link.binding.peer, writer) {
            return Err((3, RetryClass::Permanent));
        }
        Ok(self.writers.select(
            link,
            writer,
            &self.slots,
            self.config.requests_per_writer + 1,
        ))
    }

    fn free_slot(&self, peer: usize, opcode: Opcode) -> Option<usize> {
        let first = peer * (self.config.requests_per_writer + 1);
        let range = if opcode == Opcode::OpenProducer {
            first..first + 1
        } else {
            first + 1..first + 1 + self.config.requests_per_writer
        };
        range.into_iter().find(|&index| self.slot_free(index))
    }

    /// Copy a request into the arena of `slot`, one of its writer's slots.
    fn prepare(
        &mut self,
        slot: usize,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<Command, PrepareFailure> {
        let buffer = self.slots[slot]
            .buffer
            .as_mut()
            .ok_or(PrepareFailure::Busy)?;
        buffer.clear();
        let writer = slot / (self.config.requests_per_writer + 1);
        let authorized = self.writers.producer(writer).ok_or(PrepareFailure::Busy)?;
        if hint.authority.group_id != self.config.group || hint.primary != self.config.local {
            return Err((5, RetryClass::AfterAuthorityRefresh).into());
        }
        if packet.envelope.opcode == Opcode::OpenProducer {
            let open = producer::decode_open(packet, self.config.limits.envelope)
                .map_err(|_| (1, RetryClass::Permanent))?;
            if open.producer != authorized || open.partition != self.config.partition {
                return Err((3, RetryClass::Permanent).into());
            }
            if open.authority != hint.authority {
                return Err((5, RetryClass::AfterAuthorityRefresh).into());
            }
            buffer
                .prepare_producer_open(open, self.config.policy)
                .map_err(|error| prepare_failure(&error))?;
            Ok(Command::Open)
        } else {
            let append = append::validate_append(packet, self.config.limits)
                .map_err(|_| (1, RetryClass::Permanent))?;
            if !append.records.nonzero_message_ids() {
                return Err((1, RetryClass::Permanent).into());
            }
            if link.remote.capabilities & handshake::OWNER_STREAM == 0
                || append.policy != self.config.policy
            {
                return Err((2, RetryClass::Permanent).into());
            }
            if append.key.producer_id != authorized || append.partition != self.config.partition {
                return Err((3, RetryClass::Permanent).into());
            }
            if append.authority != hint.authority {
                return Err((5, RetryClass::AfterAuthorityRefresh).into());
            }
            let end_sequence = append
                .key
                .first_sequence
                .checked_add(append.records.len() as u64)
                .ok_or((1, RetryClass::Permanent))?;
            let confirmed = append::stream::Confirmed {
                authority: append.authority,
                partition: append.partition,
                owner_epoch: append.owner_epoch,
                key: append.key,
                end_sequence,
                first_offset: 0,
                disposition: append::AppendDisposition::Stored,
                policy: append.policy,
            };
            buffer
                .prepare_stream_wire_append(append)
                .map_err(|error| prepare_failure(&error))?;
            Ok(Command::Append(confirmed))
        }
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
        self.writers.reclaim(
            &self.slots,
            self.config.requests_per_writer + 1,
            &mut current,
        );
        Ok(changed)
    }

    /// Record whether `index` owns a proposal or a reply after it changed.
    fn track(&mut self, index: usize) {
        let slot = &self.slots[index];
        let bit = 1 << (index % 64);
        if slot.pending.is_some() || slot.reply.is_some() {
            self.active[index / 64] |= bit;
        } else {
            self.active[index / 64] &= !bit;
        }
    }

    /// Requests of this client's writer that still own a proposal or a reply,
    /// including a refusal in the client's rejection slot. Other writers on
    /// the partition do not count.
    pub fn writer_work(&self, node: NodeId, producer: ozzy_proto::ProducerId) -> usize {
        let stride = self.config.requests_per_writer + 1;
        let busy = |slot: &Slot| slot.pending.is_some() || slot.reply.is_some();
        let requests = self.writers.assigned(node, producer).map_or(0, |writer| {
            self.slots[writer * stride..(writer + 1) * stride]
                .iter()
                .filter(|slot| busy(slot))
                .count()
        });
        let rejection = self
            .writers
            .assigned_rejection(node, stride)
            .is_some_and(|slot| self.slots.get(slot).is_some_and(busy));
        requests + usize::from(rejection)
    }

    /// Writer opens and refusals of this client that still own a slot.
    /// APPENDs count against their writer instead.
    pub fn client_work(&self, node: NodeId) -> usize {
        let stride = self.config.requests_per_writer + 1;
        let busy = |slot: &Slot| slot.pending.is_some() || slot.reply.is_some();
        let opens = self
            .writers
            .assigned_to(node)
            .filter(|&writer| busy(&self.slots[writer * stride]))
            .count();
        let rejection = self
            .writers
            .assigned_rejection(node, stride)
            .is_some_and(|slot| self.slots.get(slot).is_some_and(busy));
        opens + usize::from(rejection) + self.progress_work(node)
    }

    /// Pending proposals and replies remain bounded by startup slots.
    pub fn has_work(&self) -> bool {
        debug_assert_eq!(
            self.busy,
            self.slots
                .iter()
                .filter(|slot| slot.pending.is_some()
                    || slot.reply.is_some()
                    || slot.batch.is_some())
                .count()
        );
        debug_assert!(self.slots.iter().enumerate().all(|(index, slot)| {
            (slot.pending.is_some() || slot.reply.is_some())
                == (self.active[index / 64] & 1 << (index % 64) != 0)
        }));
        debug_assert_eq!(
            self.progress_held,
            self.slots
                .iter()
                .filter(|slot| {
                    slot.batch
                        .as_ref()
                        .is_some_and(|batched| matches!(batched.state, BatchState::Held { .. }))
                })
                .count()
        );
        self.busy != 0
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
