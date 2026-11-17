//! Reads of committed records, one at a time per log, through the journal
//! reader. Records are copied once into one shared buffer.

use super::LogService;
use crate::log::{LogRecord, ReadBatch, ReadError, ReadRequest, StoredRecord};
use crate::replica_actor::PartitionActor;
use crate::replica_journal::{
    JournalCompletion, JournalError, PartitionReadError, PartitionReadLimits, ReadPartition,
    SubmitError,
};
use bytes::{Bytes, BytesMut};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The read in progress.
#[derive(Debug)]
pub(super) struct Reading {
    request: ReadRequest,
    completion: Option<JournalCompletion<ReadPartition>>,
}

impl LogService {
    /// Start `read`, or hand it back while another read runs.
    pub(super) fn start_read(&mut self, read: ReadRequest) -> Option<ReadRequest> {
        if self.read.is_some() {
            return Some(read);
        }
        if self.lane.is_none() {
            let _ = read.reply.send(Err(ReadError::NotLeader));
            return None;
        }
        self.read = Some(Reading {
            request: read,
            completion: None,
        });
        None
    }

    /// Advance the read in progress.
    pub(super) fn poll_read(&mut self, actor: &mut PartitionActor, cx: &mut Context<'_>) -> bool {
        let submitted = match &self.read {
            None => return false,
            Some(reading) => reading.completion.is_some(),
        };
        let mut progress = false;
        if !submitted {
            match self.submit_read(actor) {
                Poll::Ready(Ok(completion)) => {
                    if let Some(reading) = &mut self.read {
                        reading.completion = Some(completion);
                    }
                    progress = true;
                }
                Poll::Ready(Err(answer)) => return self.answer_read(answer),
                Poll::Pending => return false,
            }
        }
        let Some(completion) = self
            .read
            .as_mut()
            .and_then(|reading| reading.completion.as_mut())
        else {
            return progress;
        };
        match Pin::new(completion).poll(cx) {
            Poll::Ready(result) => self.answer_read(
                result
                    .map_err(|error| read_error(&error))
                    .map(|read| batch(&read)),
            ),
            Poll::Pending => progress,
        }
    }

    fn answer_read(&mut self, answer: Result<ReadBatch, ReadError>) -> bool {
        if let Some(reading) = self.read.take() {
            let _ = reading.request.reply.send(answer);
        }
        true
    }

    /// Open a cursor at the read's start and submit the journal read. An
    /// empty range or a start at the committed end answers at once.
    fn submit_read(
        &mut self,
        actor: &mut PartitionActor,
    ) -> Poll<Result<JournalCompletion<ReadPartition>, Result<ReadBatch, ReadError>>> {
        let (Some(reading), Some(lane)) = (&self.read, &self.lane) else {
            return Poll::Ready(Err(Err(ReadError::Stopped)));
        };
        let request = &reading.request;
        let Some((Some(ticket), journal)) = actor.read_access() else {
            return Poll::Ready(Err(Err(ReadError::NotLeader)));
        };
        let cursor = match journal.open_reader(ticket, self.port.partition, Some(request.from)) {
            Ok(Some(cursor)) => cursor,
            Ok(None) => return Poll::Pending,
            Err(error) => return Poll::Ready(Err(Err(read_error(&error)))),
        };
        let wanted = request.end.get().saturating_sub(request.from.get());
        if wanted == 0 || cursor.next_offset() == cursor.committed_end() {
            return Poll::Ready(Err(Ok(ReadBatch {
                first: request.from,
                records: Vec::new(),
            })));
        }
        let Some(lease) = lane.reads.try_lease() else {
            return Poll::Pending;
        };
        let limits = PartitionReadLimits {
            max_records: request
                .max_records
                .min(usize::try_from(wanted).unwrap_or(usize::MAX))
                .min(lane.read_limits.max_records),
            max_parts: lane.read_limits.max_parts,
            max_payload_bytes: request.max_bytes.min(lane.read_limits.max_payload_bytes),
        };
        match journal.read_partition(cursor, limits, lease) {
            Ok(completion) => Poll::Ready(Ok(completion)),
            Err(rejected) if rejected.reason == SubmitError::Full => Poll::Pending,
            Err(_) => Poll::Ready(Err(Err(ReadError::Stopped))),
        }
    }
}

/// Copy the read's records into one buffer and slice their parts from it.
fn batch(read: &ReadPartition) -> ReadBatch {
    let bytes = read
        .records()
        .map(|(_, _, parts)| parts.map(<[u8]>::len).sum::<usize>())
        .sum();
    let mut copy = BytesMut::with_capacity(bytes);
    for (_, _, parts) in read.records() {
        for part in parts {
            copy.extend_from_slice(part);
        }
    }
    let copy: Bytes = copy.freeze();
    let mut at = 0;
    let records = read
        .records()
        .zip(read.times())
        .map(|((id, _, parts), time_millis)| StoredRecord {
            time_millis,
            record: LogRecord {
                id,
                parts: parts
                    .map(|part| {
                        let slice = copy.slice(at..at + part.len());
                        at += part.len();
                        slice
                    })
                    .collect(),
            },
        })
        .collect();
    ReadBatch {
        first: read.first_offset(),
        records,
    }
}

fn read_error(error: &JournalError) -> ReadError {
    match error {
        JournalError::Read(PartitionReadError::RetentionGap { earliest }) => ReadError::Gap {
            earliest: *earliest,
        },
        JournalError::Read(PartitionReadError::Ahead { committed_end }) => ReadError::Ahead {
            end: *committed_end,
        },
        JournalError::Read(
            PartitionReadError::RecordTooLarge { .. } | PartitionReadError::Limits,
        ) => ReadError::TooLarge,
        JournalError::Read(
            PartitionReadError::Fenced
            | PartitionReadError::Unavailable
            | PartitionReadError::Partition,
        ) => ReadError::NotLeader,
        _ => ReadError::Stopped,
    }
}
