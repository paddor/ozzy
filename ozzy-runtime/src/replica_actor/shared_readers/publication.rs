//! Leader-only, applied-record publication without a partition-owned socket.

use super::{ActorError, Cursor, Payload, SharedReaderConfig, Visit, output::flush};
use crate::{
    reader_service::publication_frame,
    replica_actor::response,
    replica_journal::{PartitionReadBuffer, PartitionReadLimits, ReplicaJournal},
};
use futures::task::AtomicWaker;
use omq_tokio::{Message, TrySendError};
use ozzy_core::reader::ReadScheduler;
use ozzy_proto::{
    Envelope, Opcode, PartitionIncarnation,
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
        wake: Arc<AtomicWaker>,
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
            wake.clone(),
        )?;
        Ok(Self {
            cursor: Cursor::new(pool.clone(), None),
            pool,
            schedule: ReadScheduler::new(),
            payload: Payload::waking(config.limits.envelope.max_payload_bytes, wake),
            metadata: Vec::with_capacity(config.limits.envelope.max_metadata_bytes),
            pending: None,
        })
    }

    /// A framed publication still waits for the shard's dispatcher slot.
    /// A sent frame that a slow SUB retains does not count.
    pub(super) fn waiting(&self) -> bool {
        self.pending.is_some()
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
        self.schedule = if active {
            ReadScheduler::new()
        } else {
            ReadScheduler::waiting()
        };
    }

    pub(super) fn poll(
        &mut self,
        visit: &mut Visit<'_>,
        config: SharedReaderConfig,
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
        let published = self.publish(visit, config, cx)?;
        Ok(advanced || published)
    }

    /// Where the next frame starts: the cursor's open position under the
    /// current authority. A failed open restarts the cursor.
    fn start(
        &mut self,
        visit: &mut Visit<'_>,
        partition: PartitionIncarnation,
        cx: &mut Context<'_>,
    ) -> Option<(ValidationTicket, PublicationHeader)> {
        let ticket = visit.ticket?;
        let position = match self.cursor.poll_open(partition, ticket, visit.journal, cx) {
            Poll::Pending => return None,
            Poll::Ready(Err(_)) => {
                self.reset(false);
                return None;
            }
            Poll::Ready(Ok(position)) => position,
        };
        let source = Source::Group {
            authority: response::authority(ticket.scope()),
            partition,
            owner_epoch: position.owner_epoch().get(),
        };
        let first_offset = position.next_offset().get();
        Some((
            ticket,
            PublicationHeader {
                source,
                first_offset,
            },
        ))
    }

    /// Frame the next applied records for publication. A failed read
    /// restarts the cursor and reports no progress.
    fn publish(
        &mut self,
        visit: &mut Visit<'_>,
        config: SharedReaderConfig,
        cx: &mut Context<'_>,
    ) -> Result<bool, ActorError> {
        let Some((ticket, header)) = self.start(visit, config.partition, cx) else {
            return Ok(false);
        };
        let Some(mut payload) = self.payload.try_take() else {
            return Ok(false);
        };
        let source = header.source;
        let mut output = RecordsEncoder::publication(
            Envelope {
                opcode: Opcode::RecordsPub,
                response: false,
                request_id: None,
                sender: visit.local,
                session: None,
            },
            header,
            &mut self.metadata,
            &mut payload.body,
            config.limits,
        )
        .map_err(|_| ActorError::Limits)?;
        output.allow_shared_payload();
        assert!(self.schedule.begin_poll());
        let more = match self.cursor.poll(ticket, visit.journal, &mut output, cx) {
            Poll::Pending => false,
            Poll::Ready(Err(_)) => {
                self.reset(false);
                return Ok(false);
            }
            // A read completing without one pending asks for no further turn.
            Poll::Ready(Ok(outcome)) => self.schedule.complete(outcome).unwrap_or(false),
        };
        if output.is_empty() {
            return Ok(more);
        }
        let (header, shared) = output
            .finish_with_payload()
            .map_err(|_| ActorError::Limits)?;
        let prefix = reader::publication_topic(source).map_err(|_| ActorError::Limits)?;
        self.pending = Some(crate::native_frames::message(
            &prefix,
            header,
            &self.metadata,
            publication_frame(payload, shared).ok_or(ActorError::Limits)?,
        ));
        Ok(true)
    }
}
