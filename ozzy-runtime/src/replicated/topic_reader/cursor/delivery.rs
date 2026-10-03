//! Private delivery buffers and individual checkpoint advancement.

use super::{
    BrokerLinkError, BrokerLinks, Bytes, Context, Cursor, Decoder, Duration, FrameRecords, Message,
    Offset, Publication, Replayed, TopicReaderError, TopicRecord, TopicRoutes, reader,
};
use ozzy_proto::Opcode;

impl Cursor {
    pub(super) fn deliver(
        &mut self,
        links: &BrokerLinks,
        routes: &TopicRoutes,
        decoder: &mut Decoder,
    ) -> Result<Option<TopicRecord>, TopicReaderError> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(None);
        };
        let started = crate::profiling::start();
        let Some(record) = pending.next(decoder, links.reader_parameters().receive)? else {
            return Ok(None);
        };
        let partition = routes
            .metadata()
            .partition(self.number)
            .expect("checked partition");
        let offset = self.next;
        self.next = self.next.checked_add(1).ok_or(BrokerLinkError::Response)?;
        if self.pending_live {
            self.live_records += 1;
        } else {
            self.replayed_records += 1;
        }
        let record = TopicRecord {
            topic: routes.metadata().id(),
            partition: self.number,
            incarnation: partition.incarnation,
            offset: Offset::new(offset),
            message_id: record.message_id,
            payload: record.payload,
        };
        crate::profiling::finish(crate::profiling::Stage::ReaderMaterialize, started);
        Ok(Some(record))
    }

    pub(super) fn replay(
        &mut self,
        links: &BrokerLinks,
        message: &Message,
        now: Duration,
        cx: &mut Context<'_>,
    ) -> Result<(), TopicReaderError> {
        let (broker, session, selected) = self.selected.ok_or(BrokerLinkError::Session)?;
        let frames = frames(message);
        let limits = links.reader_parameters().receive;
        let packet =
            ozzy_proto::decode_packet(&frames.each_ref().map(AsRef::as_ref), limits.envelope)
                .map_err(BrokerLinkError::from)?;
        if packet.envelope.session != Some(session) || packet.envelope.sender != broker {
            return Ok(());
        }
        if packet.envelope.opcode == Opcode::Nack {
            let nack =
                ozzy_proto::nack::decode(packet, limits.envelope).map_err(BrokerLinkError::from)?;
            if nack.code == 14 && nack.detail.len() == 8 {
                return Err(TopicReaderError::RetentionGap {
                    partition: self.number,
                    earliest: Offset::new(u64::from_be_bytes(
                        nack.detail.try_into().expect("eight bytes"),
                    )),
                });
            }
            if matches!(
                nack.retry,
                ozzy_proto::nack::RetryClass::AfterAuthorityRefresh
                    | ozzy_proto::nack::RetryClass::AfterCredit
                    | ozzy_proto::nack::RetryClass::UnknownOutcome
            ) {
                self.reset();
                return Ok(());
            }
            return Err(BrokerLinkError::Rejected {
                code: nack.code,
                retry: nack.retry,
                hint: None,
            }
            .into());
        }
        let delivery =
            reader::decode_owned_records(packet, &frames[1], &frames[2], limits, &mut self.decode)
                .map_err(BrokerLinkError::from)?;
        if delivery.header.subscription != selected.subscription
            || delivery.header.source != selected.source
        {
            return Ok(());
        }
        let decision = self
            .live
            .replayed(
                delivery.header.first_offset,
                delivery.records.len() as u64,
                now,
            )
            .map_err(|_| BrokerLinkError::Response)?;
        if decision == Replayed::Ignore {
            return Ok(());
        }
        self.released = Some((
            delivery.records.len() as u64,
            delivery.records.as_records().payload_bytes() as u64,
        ));
        self.pending = Some(FrameRecords::new(delivery.records, message).into_batch(0));
        self.pending_live = false;
        if let Replayed::DeliverThenHeld { skip } = decision {
            let held = self.held.take().expect("held publication");
            self.ready = skip.map(|skip| held.into_batch(skip));
        }
        self.due = now + self.refresh;
        cx.waker().wake_by_ref();
        Ok(())
    }

    pub(super) fn publication(
        &mut self,
        links: &BrokerLinks,
        message: &Message,
        now: Duration,
    ) -> Result<(), TopicReaderError> {
        let (broker, session, accepted) = self.accepted.ok_or(BrokerLinkError::Session)?;
        if links.session(broker) != Some(session) {
            return Ok(());
        }
        let frames = frames(message);
        let limits = links.reader_parameters().receive;
        let Ok(packet) =
            ozzy_proto::decode_packet(&frames.each_ref().map(AsRef::as_ref), limits.envelope)
        else {
            return Ok(());
        };
        if packet.envelope.sender != broker
            || reader::route_publication(packet, limits.envelope).ok() != Some(accepted.source)
        {
            return Ok(());
        }
        let Ok(delivery) = reader::decode_owned_publication(
            packet,
            &frames[1],
            &frames[2],
            limits,
            &mut self.decode,
        ) else {
            return Ok(());
        };
        match self
            .live
            .publication(
                delivery.header.first_offset,
                delivery.records.len() as u64,
                now,
            )
            .map_err(|_| BrokerLinkError::Response)?
        {
            Publication::Drop => {}
            Publication::Hold => {
                self.held = Some(FrameRecords::new(delivery.records, message));
                // A known gap needs repair now, independent of quiet refresh.
                self.due = now;
            }
            Publication::Deliver { skip } => {
                self.pending = Some(FrameRecords::new(delivery.records, message).into_batch(skip));
                self.pending_live = true;
                self.released = None;
            }
        }
        Ok(())
    }
}

fn frames(message: &Message) -> [Bytes; 3] {
    std::array::from_fn(|i| {
        message
            .part_bytes(i + 1)
            .expect("registry validates four frames")
    })
}
