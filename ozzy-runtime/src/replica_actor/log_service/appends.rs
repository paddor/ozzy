//! Owner appends: fence by the observed term, encode into a free arena,
//! submit on the log's lane, and answer from the proposal outcome.

use super::{LogService, Term};
use crate::log::{AppendError, AppendRequest, Appended, LogRole, LogStatus};
use crate::replica_actor::{PendingProposal, ProposalOutcome, ProposalStopped};
use crate::replica_journal::{
    AppendAdmissionError, JournalError, OwnerAppendError, ProposalBuffer,
};
use ozzy_journal::operation::AppendRecord;
use ozzy_proto::Offset;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

type Reply = oneshot::Sender<Result<Appended, AppendError>>;

/// One submitted append.
#[derive(Debug)]
pub(super) struct Flight {
    pending: Pin<Box<PendingProposal>>,
    reply: Reply,
    first: Offset,
    end: Offset,
}

impl Flight {
    /// The actor went away with this append admitted or not.
    pub(super) fn unknown(self) {
        let _ = self.reply.send(Err(AppendError::Unknown));
    }
}

impl LogService {
    /// Submit `append`, or hand it back while every arena is in flight.
    pub(super) fn start_append(&mut self, append: AppendRequest) -> Option<AppendRequest> {
        let term = match self.fence(append.view) {
            Ok(term) => term,
            Err(error) => {
                let _ = append.reply.send(Err(error));
                return None;
            }
        };
        if self.stepping.is_some() || self.port.published().role == LogRole::Doubt {
            let _ = append.reply.send(Err(AppendError::NotLeader));
            return None;
        }
        let Some(lane) = &mut self.lane else {
            let _ = append.reply.send(Err(AppendError::NotLeader));
            return None;
        };
        if append.records.len() > lane.max_records {
            let _ = append.reply.send(Err(AppendError::TooLarge));
            return None;
        }
        let Some(mut buffer) = lane.arenas.pop() else {
            return Some(append);
        };
        let count = append.records.len() as u64;
        let prepared = prepare(&mut buffer, &append, term, self.port.partition);
        if let Err(error) = prepared {
            buffer.clear();
            lane.arenas.push(buffer);
            let _ = append.reply.send(Err(error));
            return None;
        }
        match lane.submitter.try_submit(buffer) {
            Ok(pending) => {
                self.appends.push(Flight {
                    pending: Box::pin(pending),
                    reply: append.reply,
                    first: append.expected,
                    end: Offset::new(append.expected.get().saturating_add(count)),
                });
                None
            }
            Err(unsubmitted) => {
                let mut buffer = unsubmitted.buffer;
                buffer.clear();
                lane.arenas.push(buffer);
                let _ = append.reply.send(Err(AppendError::NotLeader));
                None
            }
        }
    }

    /// The term an append for `view` runs in.
    fn fence(&self, view: u64) -> Result<Term, AppendError> {
        match self.term {
            Some(term) if term.scope.view == view => Ok(term),
            Some(term) => Err(AppendError::View {
                named: view,
                current: term.scope.view,
            }),
            None => Err(AppendError::NotLeader),
        }
    }

    /// Answer every append whose outcome is known and reuse its arena.
    pub(super) fn finish_appends(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progress = false;
        let mut index = 0;
        while index < self.appends.len() {
            let Poll::Ready(result) = self.appends[index].pending.as_mut().poll(cx) else {
                index += 1;
                continue;
            };
            let flight = self.appends.swap_remove(index);
            progress = true;
            let (answer, buffer) = outcome(&flight, result);
            if let Ok(appended) = answer {
                self.committed(appended.end);
            }
            let _ = flight.reply.send(answer);
            if let (Some(lane), Some(mut buffer)) = (&mut self.lane, buffer) {
                buffer.clear();
                lane.arenas.push(buffer);
            }
        }
        progress
    }

    /// Publish a committed end the term's own append reached.
    fn committed(&self, end: Offset) {
        let published = self.port.published();
        if published.lead.is_some() && end > published.committed_end {
            self.port.publish(LogStatus {
                committed_end: end,
                ..published
            });
        }
    }
}

/// Encode `append` into `buffer` under `term`.
fn prepare(
    buffer: &mut ProposalBuffer,
    append: &AppendRequest,
    term: Term,
    partition: ozzy_proto::PartitionIncarnation,
) -> Result<(), AppendError> {
    let records = append
        .records
        .iter()
        .map(|record| AppendRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: record.id,
            parts: record.parts.iter().map(AsRef::as_ref).collect(),
        })
        .collect();
    buffer
        .prepare_owner_append(term.authority(), partition, append.expected, records)
        .map_err(|error| match error {
            JournalError::AppendCapacity | JournalError::Operation(_) => AppendError::TooLarge,
            _ => AppendError::Refused,
        })
}

/// The append error a validation refusal stands for.
fn refusal(error: &JournalError) -> AppendError {
    match error {
        JournalError::OwnerAppend(OwnerAppendError::Offset { next }) => {
            AppendError::Offset { next: *next }
        }
        JournalError::OwnerAppend(_) => AppendError::Refused,
        JournalError::ProducerAppend(AppendAdmissionError::Authority)
        | JournalError::AppendMismatch => AppendError::NotLeader,
        JournalError::AppendCapacity | JournalError::RollCapacity => AppendError::Full,
        JournalError::Operation(_) => AppendError::TooLarge,
        _ => AppendError::Stopped,
    }
}

/// The answer to a finished append and the arena it returns.
fn outcome(
    flight: &Flight,
    result: Result<crate::replica_actor::ProposalReply, ProposalStopped>,
) -> (Result<Appended, AppendError>, Option<ProposalBuffer>) {
    let Ok(reply) = result else {
        return (Err(AppendError::Unknown), None);
    };
    let answer = match reply.outcome {
        ProposalOutcome::Committed { .. } => Ok(Appended {
            first: flight.first,
            end: flight.end,
            time_millis: reply.buffer.owner_timestamp().unwrap_or(0),
        }),
        ProposalOutcome::NotAdmitted => Err(AppendError::NotLeader),
        ProposalOutcome::Unknown => Err(AppendError::Unknown),
        ProposalOutcome::Invalid(error) => Err(refusal(&error)),
    };
    (answer, Some(reply.buffer))
}
