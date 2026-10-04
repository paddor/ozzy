//! Shard-side native requests using independently established shared links.

use std::{pin::Pin, task::Context};

use omq_tokio::{Message, TrySendError};
use ozzy_proto::{
    Envelope, GroupId, NodeId, Opcode, Packet, PartitionIncarnation, append,
    data::DataLimits,
    decode_packet, handshake,
    nack::{self, AuthorityHint, RetryClass},
    producer,
};

use super::{PendingProposal, ProposalOutcome, ProposalSubmitError, ProposalSubmitter};
use crate::{
    frontend::{Kind, Link},
    replica_journal::{JournalError, ProposalBuffer},
};

mod access;
mod completion;
pub use access::NativeAccess;
use access::Writers;

/// Fixed per-partition resources. The broker link and its HELLO are shared;
/// journal proposals and logical writer authorization stay on this shard.
#[derive(Clone, Debug)]
pub struct NativeIntakeConfig {
    /// Broker process identity.
    pub local: NodeId,
    /// Immutable partition group.
    pub group: GroupId,
    /// Immutable partition incarnation.
    pub partition: PartitionIncarnation,
    /// Exact configured confirmation boundary.
    pub policy: append::Policy,
    /// Trusted physical clients or explicitly provisioned logical writers.
    pub access: NativeAccess,
    /// Full validation limits, independently bounded by journal arenas.
    pub limits: DataLimits,
    /// Outstanding APPENDs per writer. Each also reserves one separate open slot.
    pub requests_per_writer: usize,
    /// Slots polled per turn, including blocked destinations.
    pub turn_slots: usize,
}

impl NativeIntakeConfig {
    /// Exact arena profile for a startup slot. APPEND slots cover the complete
    /// canonical request. Writer-open slots need at most 65 bytes; independent
    /// rejection slots never prepare a body. Narrow allocation limits prevent
    /// a small control request from retaining a large cached APPEND allocation.
    pub fn buffer_limits(&self, index: usize) -> Option<ozzy_replication::PipelineLimits> {
        let count = self.access.required_buffers(self.requests_per_writer)?;
        let stride = self.requests_per_writer.checked_add(1)?;
        if self.requests_per_writer == 0 || index >= count {
            return None;
        }
        let writer_slots = self.access.writer_count().checked_mul(stride)?;
        let max_body_bytes = if index >= writer_slots {
            1
        } else if index.is_multiple_of(stride) {
            65
        } else {
            crate::replicated::ClientConfig {
                peers: Vec::new(),
                limits: self.limits,
            }
            .prepared_canonical_body_bytes()?
        };
        Some(ozzy_replication::PipelineLimits {
            max_operations: 1,
            max_body_bytes,
        })
    }
}

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
}

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
    /// Slots that own a proposal or a reply.
    busy: usize,
    writers: Writers,
}

impl NativeIntake {
    /// Leases come from this partition actor before shared scheduling starts.
    /// Requires exactly `config.access.required_buffers(requests_per_writer)`
    /// nonempty-capacity, empty arenas. Dynamic clients have independent
    /// rejection slots. The caller backs ingress with shard/lane reservations.
    pub fn new(
        config: NativeIntakeConfig,
        submitter: ProposalSubmitter,
        buffers: Vec<ProposalBuffer>,
    ) -> Result<Self, NativeIntakeError> {
        validate(&config, &buffers)?;
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
                })
                .collect(),
            metadata: Vec::with_capacity(256),
            next: 0,
            remaining: 0,
            busy: 0,
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
        let request = Request {
            envelope: packet.envelope,
            send: link.send,
            command: Command::Open,
        };
        if link.send.envelope.max_metadata_bytes < 106 {
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
                let (code, retry) = match result {
                    Err(rejection) => rejection,
                    Ok(None) => (10, RetryClass::AfterBackoff),
                    Ok(Some(_)) => unreachable!(),
                };
                self.reject(slot, request, code, retry, hint)?;
                self.remaining = self.slots.len();
                return Ok(NativeReceive::Accepted);
            }
        };
        let Some(slot) = self.free_slot(writer, packet.envelope.opcode) else {
            return Ok(NativeReceive::Busy);
        };
        // New input may target an already visited slot. Complete a fresh bounded
        // scan so its completion receiver is polled before the scheduler sleeps.
        self.remaining = self.slots.len();
        let prepared = self.prepare(slot, writer, packet, link, hint);
        match prepared {
            Ok(command) => {
                let request = Request { command, ..request };
                let buffer = self.slots[slot].buffer.take().expect("free arena");
                match self.submitter.try_submit(buffer) {
                    Ok(proposal) => {
                        self.busy += 1;
                        self.slots[slot].pending = Some(Pending {
                            request,
                            proposal: Box::pin(proposal),
                        });
                    }
                    Err(rejected) => {
                        crate::profiling::event(crate::profiling::Event::NativeProposalRefusal);
                        let full = rejected.reason == ProposalSubmitError::Full;
                        self.slots[slot].buffer = Some(rejected.buffer);
                        if full {
                            // Keep the unchanged request on the shard. A later
                            // actor turn releases proposal capacity and retries it.
                            return Ok(NativeReceive::Busy);
                        }
                        self.reject(slot, request, 10, RetryClass::AfterBackoff, hint)?;
                    }
                }
            }
            Err(PrepareFailure::Busy) => {
                return Ok(NativeReceive::Busy);
            }
            Err(PrepareFailure::Reject(code, retry)) => {
                self.reject(slot, request, code, retry, hint)?;
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
        self.reject(slot, request, 10, RetryClass::AfterBackoff, hint)?;
        self.remaining = self.slots.len();
        Ok(NativeReceive::Accepted)
    }

    fn packet<'a>(&mut self, message: &'a Message, link: Link) -> Option<(usize, Packet<'a>)> {
        if link.binding.kind != Kind::Client
            || link.binding.peer == self.config.local
            || link.binding.peer.as_bytes() == &[0; 16]
            || message.len() != 4
            || message.part_slice(0) != Some(link.binding.peer.as_bytes().as_slice())
        {
            return None;
        }
        let frames = std::array::from_fn::<_, 3, _>(|index| {
            message.part_slice(index + 1).expect("checked frames")
        });
        let packet = decode_packet(&frames, self.config.limits.envelope).ok()?;
        if packet.envelope.sender != link.binding.peer
            || packet.envelope.session != Some(link.binding.session)
            || packet.envelope.response
            || packet.envelope.request_id.is_none()
            || !matches!(
                packet.envelope.opcode,
                Opcode::OpenProducer | Opcode::Append
            )
        {
            return None;
        }
        let peer = self.writers.peer(link.binding.peer)?;
        Some((peer, packet))
    }

    fn slot_free(&self, index: usize) -> bool {
        self.slots[index].buffer.is_some()
            && self.slots[index].pending.is_none()
            && self.slots[index].reply.is_none()
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
        if authority.group_id != self.config.group
            || authority != hint.authority
            || hint.primary != self.config.local
        {
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

    fn prepare(
        &mut self,
        slot: usize,
        peer: usize,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<Command, PrepareFailure> {
        let buffer = self.slots[slot].buffer.as_mut().expect("free arena");
        buffer.clear();
        let authorized = self.writers.producer(peer).expect("assigned writer");
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
        let mut changed = false;
        if self.remaining == 0 {
            self.remaining = self.slots.len();
        }
        if self.busy == 0 {
            self.remaining = 0;
        }
        // Only slots with a proposal or reply count against the turn bound.
        // A scan that finds no work ends in this call and asks for no turn.
        let mut turn = self.config.turn_slots;
        while turn > 0 && self.remaining > 0 {
            let index = self.next;
            self.next = (index + 1) % self.slots.len();
            self.remaining -= 1;
            if self.slots[index].pending.is_none() && self.slots[index].reply.is_none() {
                continue;
            }
            turn -= 1;
            changed |= self.poll_slot(index, cx, hint, &mut current, &mut try_send)?;
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
        opens + usize::from(rejection)
    }

    /// Pending proposals and replies remain bounded by startup slots.
    pub fn has_work(&self) -> bool {
        debug_assert_eq!(
            self.busy,
            self.slots
                .iter()
                .filter(|slot| slot.pending.is_some() || slot.reply.is_some())
                .count()
        );
        self.busy != 0
    }

    pub(super) fn identity(&self) -> (GroupId, NodeId, append::Policy) {
        (self.config.group, self.config.local, self.config.policy)
    }
}

fn validate(
    config: &NativeIntakeConfig,
    buffers: &[ProposalBuffer],
) -> Result<(), NativeIntakeError> {
    config.access.validate(config.local)?;
    let count = config.access.required_buffers(config.requests_per_writer);
    if config.local.as_bytes() == &[0; 16]
        || config.group.as_bytes() == &[0; 16]
        || config.partition.as_bytes() == &[0; 16]
        || config.requests_per_writer == 0
        || count != Some(buffers.len())
        || buffers.len() > 1024
        || !(1..=1024).contains(&config.turn_slots)
        || !matches!(
            config.policy,
            append::Policy::LocalDurable
                | append::Policy::QuorumDurable
                | append::Policy::QuorumReplicatedPersisting
        )
        || config.limits.max_records == 0
        || config.limits.max_parts == 0
        || config.limits.envelope.max_metadata_bytes < 106
        || buffers.iter().enumerate().any(|(index, buffer)| {
            !buffer.is_empty()
                || buffer.limits().max_operations == 0
                || config
                    .buffer_limits(index)
                    .is_none_or(|limits| limits.max_body_bytes > buffer.limits().max_body_bytes)
        })
    {
        return Err(NativeIntakeError::Configuration);
    }
    Ok(())
}

/// Terminal adapter error. No error creates confirmation evidence.
#[derive(Debug, thiserror::Error)]
pub enum NativeIntakeError {
    /// Invalid startup identities, admission limits, or arena count/capacity.
    #[error("invalid native partition intake configuration")]
    Configuration,
    /// Actor stopped with an unresolved proposal.
    #[error(transparent)]
    Stopped(#[from] super::ProposalStopped),
    /// Completed arena does not describe the admitted request.
    #[error("native proposal result does not match its request")]
    History,
    /// Bounded wire result cannot be encoded.
    #[error(transparent)]
    Codec(#[from] ozzy_proto::data::CodecError),
    /// Bounded rejection cannot be encoded.
    #[error(transparent)]
    Nack(#[from] nack::NackError),
    /// Shared outgoing transport failed.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
}
