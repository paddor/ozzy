use super::{
    ActorError, Client, Context, Cursor, DataLimits, Delivery, Envelope, Failure, Message, NodeId,
    Opcode, PartitionActor, Poll, SharedReaders, Slot, TrySendError, Visit, reader_frame,
};
use ozzy_proto::reader::{RecordHeader, RecordsEncoder};
use ozzy_replication::driver::ValidationTicket;

impl SharedReaders {
    pub(in crate::replica_actor) fn poll(
        &mut self,
        actor: &mut PartitionActor,
        cx: &mut Context<'_>,
        send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, ActorError> {
        self.wake.register(cx.waker());
        self.discard_stale();
        let hint = actor.authority_hint();
        let (ticket, journal) = actor.read_access().ok_or(ActorError::StartupMismatch)?;
        self.observe(ticket);
        let mut visit = Visit {
            local: self.local,
            ticket,
            journal,
        };
        let mut progress = flush(&mut self.rejection, cx, send)?;
        // At most 16 subscriptions make progress per poll. Idle ones cost no
        // budget, so a poll that ends without progress has checked every
        // subscription and needs no further wake.
        let mut budget = 16;
        let mut visits = self.slots.len();
        while budget > 0 && visits > 0 && !self.slots.is_empty() {
            visits -= 1;
            let index = self.next % self.slots.len();
            self.next = index + 1;
            let slot = &mut self.slots[index];
            let mut served = flush(&mut slot.pending, cx, send)?;
            // Keep subscriptions until there is room for their possible
            // failure reply. A blocked reply must not erase another cursor.
            if slot.pending.is_none()
                && self.rejection.is_none()
                && let Some(link) = self.links.get(slot.peer)
            {
                let limits = self.config.limits.intersection(link.send);
                match output(slot, &mut visit, limits, &mut self.metadata, cx) {
                    Ok(changed) => served |= changed,
                    Err(error) => {
                        let slot = self.slots.remove(index);
                        self.next = index;
                        let client = Client {
                            peer: slot.peer,
                            link,
                        };
                        self.reject(client, slot.delivery.request, error, hint)?;
                        served = true;
                    }
                }
            }
            if served {
                progress = true;
                budget -= 1;
            }
        }
        progress |= self.publication.poll(&mut visit, self.config, cx, send)?;
        if progress {
            cx.waker().wake_by_ref();
        }
        Ok(progress)
    }

    /// Follow the journal view. Losing it, or a new scope or generation,
    /// drops every subscription; otherwise each one reschedules its read.
    fn observe(&mut self, ticket: Option<ValidationTicket>) {
        let observed = ticket.map(|ticket| (ticket.scope(), ticket.generation(), ticket.applied()));
        if self.observed == observed {
            return;
        }
        let fenced = observed.is_none()
            || self.observed.is_some_and(|(scope, generation, _)| {
                observed.map(|(new_scope, new_generation, _)| (new_scope, new_generation))
                    != Some((scope, generation))
            });
        if fenced {
            self.slots.clear();
        }
        self.observed = observed;
        self.publication.changed(fenced, ticket.is_some());
        for slot in &mut self.slots {
            slot.delivery.schedule.source_changed();
        }
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
    visit: &mut Visit<'_>,
    limits: DataLimits,
    metadata: &mut Vec<u8>,
    cx: &mut Context<'_>,
) -> Result<bool, Failure> {
    if slot.reply_due {
        return open_reply(slot, visit, limits, metadata, cx);
    }
    let delivery = &mut slot.delivery;
    if !delivery.schedule.is_runnable() {
        return Ok(false);
    }
    let Some(ticket) = visit.ticket else {
        return Err(Failure::new(12));
    };
    let Some(mut payload) = slot.payload.try_take() else {
        return Ok(false);
    };
    let (envelope, header) = records_header(delivery, visit.local);
    let mut output = RecordsEncoder::new(envelope, header, metadata, &mut payload.body, limits)
        .map_err(|_| Failure::new(1))?;
    output.allow_shared_payload();
    assert!(delivery.schedule.begin_poll());
    let more = match delivery.cursor.poll(ticket, visit.journal, &mut output, cx) {
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

/// The RECORDS envelope and header for one subscription's next offset.
fn records_header(delivery: &Delivery<Cursor>, local: NodeId) -> (Envelope, RecordHeader) {
    let envelope = Envelope {
        opcode: Opcode::Records,
        response: false,
        request_id: None,
        sender: local,
        session: delivery.request.session,
    };
    let header = RecordHeader {
        subscription: delivery.subscribe.subscription,
        source: delivery.source,
        first_offset: delivery.next,
    };
    (envelope, header)
}

fn open_reply(
    slot: &mut Slot,
    visit: &mut Visit<'_>,
    limits: DataLimits,
    metadata: &mut Vec<u8>,
    cx: &mut Context<'_>,
) -> Result<bool, Failure> {
    let ticket = visit.ticket.ok_or_else(|| Failure::new(12))?;
    let delivery = &mut slot.delivery;
    let resolved = match slot.resolved_offset {
        Some(offset) => offset,
        None => match delivery.cursor.poll_open(
            journal_partition(delivery.source)?,
            ticket,
            visit.journal,
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
            sender: visit.local,
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
