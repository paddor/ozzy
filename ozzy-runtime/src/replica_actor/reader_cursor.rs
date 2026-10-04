//! Partition-local cursor over confirmed journal records.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ozzy_proto::Offset;
use ozzy_proto::reader::{RecordsEncoder, Source};
use ozzy_replication::Scope;
use ozzy_replication::driver::ValidationTicket;

use crate::reader_service::{Failure, ReadState};
use crate::replica_journal::{
    JournalCompletion, JournalError, PartitionReadBuffer, PartitionReadCursor, PartitionReadError,
    PartitionReadLease, PartitionReadLimits, ReadDelivery, ReplicaJournal, SubmitError,
};

#[derive(Debug)]
pub(super) struct Cursor {
    pool: PartitionReadBuffer,
    /// None starts at the applied end: only later records are read.
    from: Option<Offset>,
    selector: Option<ozzy_proto::reader::Start>,
    seeking: Option<JournalCompletion<PartitionReadCursor>>,
    position: Option<PartitionReadCursor>,
    reading: Option<JournalCompletion<ReadDelivery>>,
    read_started: Option<std::time::Instant>,
    buffer: Option<PartitionReadLease>,
}

impl Cursor {
    pub(super) const fn new(pool: PartitionReadBuffer, from: Option<Offset>) -> Self {
        Self {
            pool,
            from,
            selector: None,
            seeking: None,
            position: None,
            reading: None,
            read_started: None,
            buffer: None,
        }
    }

    pub(super) fn select(pool: PartitionReadBuffer, selector: ozzy_proto::reader::Start) -> Self {
        let mut cursor = Self::new(pool, None);
        cursor.selector = Some(selector);
        cursor
    }

    /// Resolve the start once. The position names the next offset and owner epoch.
    pub(super) fn poll_open(
        &mut self,
        partition: ozzy_proto::PartitionIncarnation,
        ticket: ValidationTicket,
        journal: &mut ReplicaJournal,
        cx: &mut Context<'_>,
    ) -> Poll<Result<PartitionReadCursor, Failure>> {
        if let Some(position) = self.position {
            return Poll::Ready(Ok(position));
        }
        if let Some(selector) = self.selector {
            if self.seeking.is_none() {
                match journal.seek_reader(ticket, partition, selector) {
                    Ok(completion) => self.seeking = Some(completion),
                    Err(SubmitError::Full) => return Poll::Pending,
                    Err(_) => return Poll::Ready(Err(Failure::new(11))),
                }
            }
            let result =
                std::task::ready!(Pin::new(self.seeking.as_mut().expect("pending seek")).poll(cx));
            self.seeking = None;
            let position = result.map_err(|error| failure(&error))?;
            self.position = Some(position);
            return Poll::Ready(Ok(position));
        }
        let Some(position) = journal
            .open_reader(ticket, partition, self.from)
            .map_err(|error| failure(&error))?
        else {
            return Poll::Pending;
        };
        self.position = Some(position);
        Poll::Ready(Ok(position))
    }

    pub(super) fn poll(
        &mut self,
        source: Source,
        ticket: ValidationTicket,
        journal: &mut ReplicaJournal,
        output: &mut RecordsEncoder<'_>,
        maximum: ozzy_proto::data::DataLimits,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ReadState, Failure>> {
        validate_source(source, ticket.scope())?;
        let Source::Group {
            partition,
            owner_epoch,
            ..
        } = source
        else {
            return Poll::Ready(Err(Failure::new(2)));
        };
        let cursor = std::task::ready!(self.poll_open(partition, ticket, journal, cx))?;
        if cursor.scope() != ticket.scope()
            || cursor.generation() != ticket.generation()
            || cursor.owner_epoch().get() != owner_epoch
        {
            return Poll::Ready(Err(Failure::new(5)));
        }
        if self.reading.is_none() {
            let Some(buffer) = self.buffer.take().or_else(|| self.pool.try_lease()) else {
                return Poll::Pending;
            };
            let limits = output.remaining();
            match journal.read_delivery(
                cursor,
                PartitionReadLimits {
                    max_records: limits.max_records,
                    max_parts: limits.max_parts,
                    max_payload_bytes: limits.envelope.max_payload_bytes,
                },
                buffer,
            ) {
                Ok(completion) => {
                    self.read_started = crate::profiling::start();
                    self.reading = Some(completion);
                }
                Err(rejected) => {
                    self.buffer = Some(rejected.value);
                    return if rejected.reason == SubmitError::Full {
                        Poll::Pending
                    } else {
                        Poll::Ready(Err(Failure::new(11)))
                    };
                }
            }
        }
        let result =
            std::task::ready!(Pin::new(self.reading.as_mut().expect("pending read")).poll(cx));
        self.reading = None;
        crate::profiling::finish(
            crate::profiling::Stage::ReaderJournal,
            self.read_started.take(),
        );
        match result.and_then(|records| records.encode(output, maximum)) {
            Ok(cursor) => {
                self.position = Some(cursor);
                Poll::Ready(Ok(if cursor.next_offset() == cursor.committed_end() {
                    ReadState::CaughtUp
                } else {
                    ReadState::More
                }))
            }
            Err(error) => Poll::Ready(Err(failure(&error))),
        }
    }
}

fn validate_source(source: Source, scope: Scope) -> Result<(), Failure> {
    let Source::Group { authority, .. } = source else {
        return Err(Failure::new(2));
    };
    if authority.group_id != scope.group_id
        || authority.config_epoch != scope.configuration_epoch
        || authority.view != scope.view
    {
        return Err(Failure::new(5));
    }
    Ok(())
}

fn failure(error: &JournalError) -> Failure {
    match error {
        JournalError::Seek(ozzy_core::reader::seek::SeekError::NotFound { earliest }) => {
            Failure::position(19, earliest.get())
        }
        JournalError::Seek(ozzy_core::reader::seek::SeekError::Ambiguous { first, last }) => {
            Failure::ambiguous(first.get(), last.get())
        }
        JournalError::Io(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            Failure::new(10)
        }
        JournalError::Read(PartitionReadError::RetentionGap { earliest }) => {
            Failure::position(14, earliest.get())
        }
        JournalError::Read(PartitionReadError::Ahead { committed_end }) => {
            Failure::position(16, committed_end.get())
        }
        JournalError::Read(PartitionReadError::RecordTooLarge { bytes, parts }) => {
            Failure::record_too_large(*bytes, *parts)
        }
        JournalError::Read(PartitionReadError::Fenced) => Failure::new(5),
        JournalError::Read(PartitionReadError::Unavailable) => Failure::new(12),
        JournalError::Read(PartitionReadError::Partition) => Failure::new(6),
        JournalError::Read(PartitionReadError::Limits) => Failure::new(1),
        _ => Failure::new(11),
    }
}
