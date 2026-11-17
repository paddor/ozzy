//! An embedder's own log on one partition. Its one writer appends batches
//! at the offset it expects, in the view it leads; it reads committed
//! records back by offset. Handles work from any thread. The partition's
//! shard runs the service behind them.

mod trim;

pub use trim::TrimError;
pub(crate) use trim::TrimRequest;

use bytes::Bytes;
use ozzy_proto::{MessageId, NodeId, Offset, PartitionIncarnation};
use smallvec::SmallVec;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot, watch};

/// One record: a nonzero ID and at least one part. Ozzy does not look at
/// either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    /// Record ID, nonzero.
    pub id: MessageId,
    /// Opaque parts in order. Empty parts are kept.
    pub parts: SmallVec<[Bytes; 2]>,
}

/// Batch header bytes of one encoded append.
const BATCH_BYTES: usize = 80;

impl LogRecord {
    /// Bytes this record takes in an encoded batch at most: its ID, a part
    /// count, a length per part, and the parts.
    fn encoded_bytes(&self) -> usize {
        20 + self.parts.iter().map(|part| 4 + part.len()).sum::<usize>()
    }
}

/// A committed append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// Offset of the first record.
    pub first: Offset,
    /// Offset after the last record.
    pub end: Offset,
    /// The leader's wall time stamped on the batch, in Unix milliseconds.
    pub time_millis: u64,
}

/// Why an append did not commit. Every one but `NotLeader` in doubt or
/// during a step-down that then fails ends
/// the writer's term: its later appends fail too, so it rebuilds from the
/// log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AppendError {
    /// The batch does not start at the log's next offset.
    #[error("append does not start at the next offset {next:?}")]
    Offset {
        /// The log's next offset after every accepted append.
        next: Offset,
    },
    /// The batch names another view than the one this broker leads.
    #[error("append names view {named}, the log is in view {current}")]
    View {
        /// View the append named.
        named: u64,
        /// View this broker last observed.
        current: u64,
    },
    /// This broker does not lead the log now, or it leads in doubt.
    #[error("this broker does not lead the log")]
    NotLeader,
    /// The view changed after admission. The batch may or may not be in
    /// the log; read it to find out.
    #[error("append outcome unknown after a view change")]
    Unknown,
    /// The batch exceeds one append's byte or record bound.
    #[error("append exceeds the batch bound")]
    TooLarge,
    /// The journal is out of segment space.
    #[error("log is out of space")]
    Full,
    /// The partition takes producer appends only, or does not exist.
    #[error("partition refuses owner appends")]
    Refused,
    /// The service or its partition stopped.
    #[error("log stopped")]
    Stopped,
}

/// One committed record as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    /// The leader's wall time stamped on the record's batch, in Unix
    /// milliseconds: the `time_millis` its append returned.
    pub time_millis: u64,
    /// The record as appended.
    pub record: LogRecord,
}

/// Committed records read back from one offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBatch {
    /// Offset of the first record, the requested start.
    pub first: Offset,
    /// Records at consecutive offsets. Empty when `first` is the committed
    /// end.
    pub records: Vec<StoredRecord>,
}

/// Why a read failed. Reads never skip records silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// The start lies below the retained floor.
    #[error("records before {earliest:?} are trimmed")]
    Gap {
        /// Earliest retained offset.
        earliest: Offset,
    },
    /// The start lies past the committed end.
    #[error("start lies past the committed end {end:?}")]
    Ahead {
        /// Committed end.
        end: Offset,
    },
    /// The next record does not fit the read bounds.
    #[error("next record exceeds the read bounds")]
    TooLarge,
    /// This broker does not lead the log now.
    #[error("this broker does not lead the log")]
    NotLeader,
    /// The service or its partition stopped.
    #[error("log stopped")]
    Stopped,
}

/// Why a step-down did not start a view change. The term goes on unless
/// the status says otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StepDownError {
    /// The request names another view than the one this broker leads.
    #[error("step-down names view {named}, the log is in view {current}")]
    View {
        /// View the request named.
        named: u64,
        /// View this broker leads.
        current: u64,
    },
    /// This broker does not lead the log now.
    #[error("this broker does not lead the log")]
    NotLeader,
    /// The target is this broker or holds no copy of the log.
    #[error("target cannot lead the log")]
    Target,
    /// Accepted records did not commit within the wait.
    #[error("uncommitted records remained")]
    Busy,
    /// Another step-down runs or started less than a second ago.
    #[error("step-down is rate limited")]
    Limited,
    /// The service or its partition stopped.
    #[error("log stopped")]
    Stopped,
}

/// A request the service did not take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SubmitError {
    /// The queue is full. Nothing was queued; submit again after an earlier
    /// request completes.
    #[error("log queue is full")]
    Busy,
    /// The request exceeds the service's bounds.
    #[error("request exceeds the log bounds")]
    TooLarge,
    /// A record has a zero ID or no part.
    #[error("record without ID or parts")]
    Invalid,
    /// The service stopped.
    #[error("log stopped")]
    Stopped,
}

/// This broker's role in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LogRole {
    /// It leads the view and admits appends.
    Leader,
    /// It leads the view but heard no follower for the primary timeout, so
    /// nothing it admits could commit. Appends fail with `NotLeader`. The
    /// term goes on: when a follower of the view answers, the role returns
    /// to `Leader` and the writer continues at its first refused offset.
    Doubt,
    /// Another broker leads the view.
    Follower,
    /// A view change is in progress.
    Electing,
    /// The partition stopped serving here.
    Stopped,
}

/// One term of this broker as leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lead {
    /// The log's end when the term began. Everything below it is
    /// committed; records the old leader appended may lack replies.
    pub start_end: Offset,
    /// The log's end at the commit point the view was installed with, at
    /// most `start_end`. Records from here to `start_end` committed during
    /// the view change, so the old leader never confirmed them. Earlier
    /// records may lack replies too: the commit point is a lower bound.
    pub installed_commit_end: Offset,
}

/// The log as this broker last saw it. Each view change publishes a new
/// view, so a stale status cannot revive a term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogStatus {
    /// Latest view.
    pub view: u64,
    /// Role in that view.
    pub role: LogRole,
    /// Committed end, as of the last leader observation.
    pub committed_end: Offset,
    /// Retained floor, as of the last leader observation.
    pub retained_from: Offset,
    /// Present while `role` is `Leader` or `Doubt`.
    pub lead: Option<Lead>,
}

impl LogStatus {
    const INITIAL: Self = Self {
        view: 0,
        role: LogRole::Electing,
        committed_end: Offset::ZERO,
        retained_from: Offset::ZERO,
        lead: None,
    };
}

/// Bounds of one log service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogLimits {
    /// Requests queued for the service, at least 1.
    pub queued: usize,
    /// Encoded bytes queued for the service, at least `append_bytes`.
    pub queued_bytes: usize,
    /// Appends in flight on the shard, at least 1.
    pub appends: usize,
    /// Encoded bytes of one append, its batch header and record framing
    /// included, more than 80.
    pub append_bytes: usize,
    /// Records one read returns at most, at least 1.
    pub read_records: usize,
    /// Record bytes one read returns at most, at least 1.
    pub read_bytes: usize,
}

impl LogLimits {
    fn valid(self) -> bool {
        self.queued > 0
            && self.appends > 0
            && self.append_bytes > BATCH_BYTES
            && self.queued_bytes >= self.append_bytes
            && self.read_records > 0
            && self.read_bytes > 0
    }
}

#[derive(Debug)]
pub(crate) struct AppendRequest {
    pub(crate) view: u64,
    pub(crate) expected: Offset,
    pub(crate) records: Vec<LogRecord>,
    pub(crate) bytes: usize,
    pub(crate) reply: oneshot::Sender<Result<Appended, AppendError>>,
}

#[derive(Debug)]
pub(crate) struct ReadRequest {
    pub(crate) from: Offset,
    pub(crate) end: Offset,
    pub(crate) max_records: usize,
    pub(crate) max_bytes: usize,
    pub(crate) reply: oneshot::Sender<Result<ReadBatch, ReadError>>,
}

#[derive(Debug)]
pub(crate) struct StepDownRequest {
    pub(crate) view: u64,
    pub(crate) target: NodeId,
    pub(crate) reply: oneshot::Sender<Result<u64, StepDownError>>,
}

#[derive(Debug)]
pub(crate) enum Command {
    Append(AppendRequest),
    Read(ReadRequest),
    StepDown(StepDownRequest),
    Trim(TrimRequest),
}

/// Handle to one partition's log. Cheap to clone; usable from any thread.
#[derive(Debug, Clone)]
pub struct Log {
    commands: mpsc::Sender<Command>,
    queued: Arc<AtomicUsize>,
    limits: LogLimits,
    status: watch::Receiver<LogStatus>,
}

/// The shard's end of one log.
#[derive(Debug)]
pub struct LogPort {
    pub(crate) partition: PartitionIncarnation,
    pub(crate) commands: mpsc::Receiver<Command>,
    pub(crate) queued: Arc<AtomicUsize>,
    pub(crate) limits: LogLimits,
    pub(crate) status: watch::Sender<LogStatus>,
}

/// A handle and the shard's end of one partition's log. `None` for invalid
/// limits.
pub fn log_channel(partition: PartitionIncarnation, limits: LogLimits) -> Option<(Log, LogPort)> {
    if !limits.valid() {
        return None;
    }
    let (commands, receiver) = mpsc::channel(limits.queued);
    let (status, observed) = watch::channel(LogStatus::INITIAL);
    let queued = Arc::new(AtomicUsize::new(0));
    Some((
        Log {
            commands,
            queued: queued.clone(),
            limits,
            status: observed,
        },
        LogPort {
            partition,
            commands: receiver,
            queued,
            limits,
            status,
        },
    ))
}

impl Log {
    /// The log's status. It changes on every role or view change and when
    /// the committed end moves; wait on it to follow the tail.
    pub fn status(&self) -> watch::Receiver<LogStatus> {
        self.status.clone()
    }

    /// The bounds this log was created with.
    pub const fn limits(&self) -> LogLimits {
        self.limits
    }

    /// Queue `records` at `expected` in `view`. Requests run in the order
    /// they are queued, so the writer may queue its next batch at the end of
    /// this one before this one commits.
    ///
    /// # Errors
    ///
    /// `Busy` when the queue is full, `Invalid` for a record with a zero ID
    /// or no part, `TooLarge` for an empty or oversized batch, `Stopped`
    /// after the service ends.
    pub fn append(
        &self,
        view: u64,
        expected: Offset,
        records: Vec<LogRecord>,
    ) -> Result<Pending<Appended, AppendError>, SubmitError> {
        if records
            .iter()
            .any(|record| record.parts.is_empty() || record.id.as_bytes() == &[0; 16])
        {
            return Err(SubmitError::Invalid);
        }
        let bytes = records.iter().map(LogRecord::encoded_bytes).sum::<usize>();
        if records.is_empty() || bytes + BATCH_BYTES > self.limits.append_bytes {
            return Err(SubmitError::TooLarge);
        }
        let (reply, pending) = oneshot::channel();
        self.submit(
            Command::Append(AppendRequest {
                view,
                expected,
                records,
                bytes,
                reply,
            }),
            bytes,
        )?;
        Ok(Pending(pending))
    }

    /// Read committed records in `[from, end)`, at most `max_records` and
    /// `max_bytes` of them, cut at the service's read bounds. At least one
    /// record returns unless `from` is the committed end. Leader only.
    ///
    /// # Errors
    ///
    /// `Busy` when the queue is full, `Stopped` after the service ends.
    pub fn read(
        &self,
        from: Offset,
        end: Offset,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Pending<ReadBatch, ReadError>, SubmitError> {
        let (reply, pending) = oneshot::channel();
        self.submit(
            Command::Read(ReadRequest {
                from,
                end,
                max_records: max_records.clamp(1, self.limits.read_records),
                max_bytes: max_bytes.clamp(1, self.limits.read_bytes),
                reply,
            }),
            0,
        )?;
        Ok(Pending(pending))
    }

    /// Hand the log to `target`, another broker holding a copy. The leader
    /// of `view` stops taking appends, waits up to a second for accepted
    /// records to commit, then starts a view change to the next view
    /// `target` leads, which the reply names. `target` leads once it
    /// installs that view; the status of its log shows it. At most one
    /// step-down starts per second.
    ///
    /// # Errors
    ///
    /// `Busy` when the queue is full, `Stopped` after the service ends.
    pub fn step_down(
        &self,
        view: u64,
        target: NodeId,
    ) -> Result<Pending<u64, StepDownError>, SubmitError> {
        let (reply, pending) = oneshot::channel();
        self.submit(
            Command::StepDown(StepDownRequest {
                view,
                target,
                reply,
            }),
            0,
        )?;
        Ok(Pending(pending))
    }

    fn submit(&self, command: Command, bytes: usize) -> Result<(), SubmitError> {
        // A failed reservation is undone below, so the service never sees
        // more than the limit. Relaxed: the counter guards no other memory.
        let before = self.queued.fetch_add(bytes, Ordering::Relaxed);
        let undo = || self.queued.fetch_sub(bytes, Ordering::Relaxed);
        if before + bytes > self.limits.queued_bytes {
            undo();
            return Err(SubmitError::Busy);
        }
        self.commands.try_send(command).map_err(|error| {
            undo();
            match error {
                mpsc::error::TrySendError::Full(_) => SubmitError::Busy,
                mpsc::error::TrySendError::Closed(_) => SubmitError::Stopped,
            }
        })
    }
}

impl LogPort {
    /// The partition this log belongs to.
    pub const fn partition(&self) -> PartitionIncarnation {
        self.partition
    }

    /// The bounds this log was created with.
    pub const fn limits(&self) -> LogLimits {
        self.limits
    }

    /// Take the next queued request and release its queued bytes.
    pub(crate) fn poll_command(&mut self, cx: &mut Context<'_>) -> Poll<Option<Command>> {
        let command = std::task::ready!(self.commands.poll_recv(cx));
        if let Some(Command::Append(append)) = &command {
            self.queued.fetch_sub(append.bytes, Ordering::Relaxed);
        }
        Poll::Ready(command)
    }

    /// Publish `status` if it changed.
    pub(crate) fn publish(&self, status: LogStatus) {
        self.status.send_if_modified(|current| {
            let changed = *current != status;
            *current = status;
            changed
        });
    }

    /// The status last published.
    pub(crate) fn published(&self) -> LogStatus {
        *self.status.borrow()
    }
}

/// The outcome of one queued request. Dropping it never cancels the
/// request.
#[derive(Debug)]
pub struct Pending<T, E>(oneshot::Receiver<Result<T, E>>);

impl<T, E> Pending<T, E> {
    fn poll_reply(&mut self, cx: &mut Context<'_>, stopped: E) -> Poll<Result<T, E>> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|result| result.unwrap_or(Err(stopped)))
    }
}

impl Future for Pending<Appended, AppendError> {
    type Output = Result<Appended, AppendError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_reply(cx, AppendError::Stopped)
    }
}

impl Future for Pending<ReadBatch, ReadError> {
    type Output = Result<ReadBatch, ReadError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_reply(cx, ReadError::Stopped)
    }
}

impl Future for Pending<u64, StepDownError> {
    type Output = Result<u64, StepDownError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_reply(cx, StepDownError::Stopped)
    }
}
