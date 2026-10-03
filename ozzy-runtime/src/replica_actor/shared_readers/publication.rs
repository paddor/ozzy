//! Leader-only, applied-record publication without a partition-owned socket.

use super::{ActorError, Cursor, NodeId, Payload, SharedReaderConfig, output::flush};
use crate::{
    reader_service::publication_frame,
    replica_journal::{PartitionReadBuffer, PartitionReadLimits, ReplicaJournal},
    signal::DataSignal,
};
use omq_tokio::{Message, TrySendError};
use ozzy_core::reader::{ReadOutcome, ReadScheduler};
use ozzy_proto::{
    Envelope, Opcode,
    reader::{self, PublicationHeader, RecordsEncoder, Source},
};
use ozzy_replication::{PipelineLimits, driver::ValidationTicket};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug)]
pub(super) struct Publication {
    pool: PartitionReadBuffer,
    cursor: Cursor,
    schedule: ReadScheduler,
    payload: Payload,
    metadata: Vec<u8>,
    pending: Option<Message>,
}

impl Publication {
    pub(super) fn new(
        journal: &mut ReplicaJournal,
        config: SharedReaderConfig,
        work: Arc<DataSignal>,
    ) -> Result<Self, ActorError> {
        let buffer = journal.lease_append_buffer_with_limits(PipelineLimits {
            max_operations: 1,
            max_body_bytes: config.limits.envelope.max_payload_bytes,
        })?;
        let pool = PartitionReadBuffer::new(
            buffer,
            PartitionReadLimits {
                max_records: config.limits.max_records,
                max_parts: config.limits.max_parts,
                max_payload_bytes: config.limits.envelope.max_payload_bytes,
            },
            work.clone(),
        )?;
        Ok(Self {
            cursor: Cursor::new(pool.clone(), None),
            pool,
            schedule: ReadScheduler::new(),
            payload: Payload::notifying(config.limits.envelope.max_payload_bytes, work),
            metadata: Vec::with_capacity(config.limits.envelope.max_metadata_bytes),
            pending: None,
        })
    }

    pub(super) fn changed(&mut self, fenced: bool, active: bool) {
        if fenced {
            self.reset(active);
        } else {
            self.schedule.source_changed();
        }
    }

    fn reset(&mut self, active: bool) {
        self.cursor = Cursor::new(self.pool.clone(), None);
        self.pending = None;
        self.schedule = ReadScheduler::new();
        if !active {
            assert!(self.schedule.begin_poll());
            self.schedule
                .complete(ReadOutcome::CaughtUp)
                .expect("pending publication");
        }
    }

    pub(super) fn poll(
        &mut self,
        local: NodeId,
        config: SharedReaderConfig,
        ticket: Option<ValidationTicket>,
        journal: &mut ReplicaJournal,
        cx: &mut Context<'_>,
        send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, ActorError> {
        let had_pending = self.pending.is_some();
        let advanced = flush(&mut self.pending, cx, send)?;
        if had_pending && self.pending.is_none() {
            crate::profiling::event(crate::profiling::Event::ReaderPublication);
        }
        if self.pending.is_some() || !self.schedule.is_runnable() {
            return Ok(advanced);
        }
        let Some(ticket) = ticket else {
            return Ok(advanced);
        };
        let position = match self.cursor.poll_open(config.partition, ticket, journal) {
            Poll::Pending => return Ok(advanced),
            Poll::Ready(Err(_)) => {
                self.reset(false);
                return Ok(advanced);
            }
            Poll::Ready(Ok(position)) => position,
        };
        let Some(mut payload) = self.payload.try_take() else {
            return Ok(advanced);
        };
        let scope = ticket.scope();
        let source = Source::Group {
            authority: ozzy_proto::data::Authority {
                group_id: scope.group_id,
                config_epoch: scope.configuration_epoch,
                view: scope.view,
            },
            partition: config.partition,
            owner_epoch: position.owner_epoch().get(),
        };
        let mut output = RecordsEncoder::publication(
            Envelope {
                opcode: Opcode::RecordsPub,
                response: false,
                request_id: None,
                sender: local,
                session: None,
            },
            PublicationHeader {
                source,
                first_offset: position.next_offset().get(),
            },
            &mut self.metadata,
            &mut payload.body,
            config.limits,
        )
        .map_err(|_| ActorError::Limits)?;
        output.allow_shared_payload();
        assert!(self.schedule.begin_poll());
        let more = match self
            .cursor
            .poll(source, ticket, journal, &mut output, config.limits, cx)
        {
            Poll::Pending => false,
            Poll::Ready(Err(_)) => {
                self.reset(false);
                return Ok(advanced);
            }
            Poll::Ready(Ok(outcome)) => self
                .schedule
                .complete(outcome)
                .expect("pending publication"),
        };
        if output.is_empty() {
            return Ok(advanced || more);
        }
        let (header, shared) = output
            .finish_with_payload()
            .map_err(|_| ActorError::Limits)?;
        let prefix = reader::publication_topic(source).map_err(|_| ActorError::Limits)?;
        self.pending = Some(crate::native_frames::message(
            &prefix,
            header,
            &self.metadata,
            publication_frame(payload, shared),
        ));
        Ok(true)
    }
}
