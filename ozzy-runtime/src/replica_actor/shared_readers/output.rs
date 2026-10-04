use super::{
    ActorError, Context, DataLimits, Envelope, Failure, Message, NodeId, Opcode, PartitionActor,
    Poll, SharedReaders, Slot, TrySendError, reader_frame, wait,
};
use crate::replica_journal::ReplicaJournal;
use ozzy_proto::reader::{RecordHeader, RecordsEncoder};
use ozzy_replication::driver::ValidationTicket;

impl SharedReaders {
    pub(in crate::replica_actor) fn poll(
        &mut self,
        actor: &mut PartitionActor,
        cx: &mut Context<'_>,
        send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, ActorError> {
        self.discard_stale();
        if self.readiness.as_mut().poll(cx).is_ready() {
            self.work.drain(|| ());
            self.readiness = wait(self.work.clone());
            cx.waker().wake_by_ref();
        }
        let hint = actor.authority_hint();
        let (ticket, journal) = actor.read_access().ok_or(ActorError::StartupMismatch)?;
        let observed = ticket.map(|ticket| (ticket.scope(), ticket.generation(), ticket.applied()));
        if self.observed != observed {
            let fenced = observed.is_none()
                || self.observed.is_some_and(|(scope, generation, _)| {
                    observed.map(|(new_scope, new_generation, _)| (new_scope, new_generation))
                        != Some((scope, generation))
                });
            if fenced {
                self.slots.iter_mut().for_each(|slot| *slot = None);
            }
            self.observed = observed;
            self.publication.changed(fenced, ticket.is_some());
            for slot in self.slots.iter_mut().flatten() {
                slot.delivery.schedule.source_changed();
            }
        }
        let mut progress = flush(&mut self.rejection, cx, send)?;
        for _ in 0..16 {
            let index = self.next;
            self.next = (index + 1) % self.slots.len();
            if self.slots[index].is_none() {
                continue;
            }
            let slot = self.slots[index].as_mut().expect("selected slot");
            progress |= flush(&mut slot.pending, cx, send)?;
            // Keep subscriptions until there is room for their possible
            // failure reply. A blocked reply must not erase another cursor.
            if slot.pending.is_some() || self.rejection.is_some() {
                continue;
            }
            let Some(link) = self.links.get(slot.peer) else {
                continue;
            };
            match output(
                slot,
                self.local,
                self.config.limits.intersection(link.send),
                &mut self.metadata,
                ticket,
                journal,
                cx,
            ) {
                Ok(changed) => progress |= changed,
                Err(error) => {
                    let slot = self.slots[index].take().expect("failed subscription");
                    self.reject(slot.peer, slot.delivery.request, link, error, hint)?;
                    progress = true;
                }
            }
        }
        progress |= self
            .publication
            .poll(self.local, self.config, ticket, journal, cx, send)?;
        if progress {
            cx.waker().wake_by_ref();
        }
        Ok(progress)
    }
}

pub(super) fn flush(
    pending: &mut Option<Message>,
    cx: &mut Context<'_>,
    send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
) -> Result<bool, ActorError> {
    let Some(message) = pending.take() else {
        return Ok(false);
    };
    match send(cx, message) {
        Ok(()) => Ok(true),
        Err(TrySendError::Full(message)) => {
            *pending = Some(message);
            Ok(false)
        }
        Err(TrySendError::Closed) => Err(omq_tokio::Error::Closed.into()),
        Err(TrySendError::Error(error)) => Err(error.into()),
    }
}

fn output(
    slot: &mut Slot,
    local: NodeId,
    limits: DataLimits,
    metadata: &mut Vec<u8>,
    ticket: Option<ValidationTicket>,
    journal: &mut ReplicaJournal,
    cx: &mut Context<'_>,
) -> Result<bool, Failure> {
    if slot.reply_due {
        return open_reply(slot, local, limits, metadata, ticket, journal, cx);
    }
    let delivery = &mut slot.delivery;
    if !delivery.schedule.is_runnable() {
        return Ok(false);
    }
    let Some(ticket) = ticket else {
        return Err(Failure::new(12));
    };
    let Some(mut payload) = slot.payload.try_take() else {
        return Ok(false);
    };
    let envelope = Envelope {
        opcode: Opcode::Records,
        response: false,
        request_id: None,
        sender: local,
        session: delivery.request.session,
    };
    let mut output = RecordsEncoder::new(
        envelope,
        RecordHeader {
            subscription: delivery.subscribe.subscription,
            source: delivery.source,
            first_offset: delivery.next,
        },
        metadata,
        &mut payload.body,
        limits,
    )
    .map_err(|_| Failure::new(1))?;
    output.allow_shared_payload();
    assert!(delivery.schedule.begin_poll());
    let more = match delivery
        .cursor
        .poll(delivery.source, ticket, journal, &mut output, limits, cx)
    {
        Poll::Ready(Ok(state)) => delivery
            .schedule
            .complete(state)
            .map_err(|_| Failure::new(11))?,
        Poll::Ready(Err(error)) => return Err(error),
        Poll::Pending => false,
    };
    if output.is_empty() {
        return Ok(more);
    }
    let records = output.len() as u64;
    let (header, shared) = output.finish_with_payload().map_err(|_| Failure::new(1))?;
    delivery.next = delivery
        .next
        .checked_add(records)
        .ok_or_else(|| Failure::new(11))?;
    slot.pending = Some(crate::native_frames::message(
        slot.peer.as_bytes(),
        header,
        metadata,
        reader_frame(payload, shared),
    ));
    Ok(true)
}

fn open_reply(
    slot: &mut Slot,
    local: NodeId,
    limits: DataLimits,
    metadata: &mut Vec<u8>,
    ticket: Option<ValidationTicket>,
    journal: &mut ReplicaJournal,
    cx: &mut Context<'_>,
) -> Result<bool, Failure> {
    let ticket = ticket.ok_or_else(|| Failure::new(12))?;
    let delivery = &mut slot.delivery;
    let resolved = match slot.resolved_offset {
        Some(offset) => offset,
        None => match delivery.cursor.poll_open(
            journal_partition(delivery.source)?,
            ticket,
            journal,
            cx,
        ) {
            Poll::Pending => return Ok(false),
            Poll::Ready(result) => {
                let offset = result?.next_offset().get();
                slot.resolved_offset = Some(offset);
                delivery.next = offset;
                offset
            }
        },
    };
    let header = ozzy_proto::reader::encode_subscribed(
        Envelope {
            opcode: Opcode::Subscribed,
            response: true,
            sender: local,
            ..delivery.request
        },
        ozzy_proto::reader::Subscribed {
            subscription: delivery.subscribe.subscription,
            source: delivery.source,
            resolved_offset: resolved,
        },
        metadata,
        limits.envelope,
    )
    .map_err(|_| Failure::new(1))?;
    slot.pending = Some(crate::native_frames::message(
        slot.peer.as_bytes(),
        header,
        metadata,
        bytes::Bytes::new(),
    ));
    slot.reply_due = false;
    Ok(true)
}

fn journal_partition(
    source: ozzy_proto::reader::Source,
) -> Result<ozzy_proto::PartitionIncarnation, Failure> {
    match source {
        ozzy_proto::reader::Source::Group { partition, .. } => Ok(partition),
        ozzy_proto::reader::Source::Local { .. } => Err(Failure::new(2)),
    }
}
