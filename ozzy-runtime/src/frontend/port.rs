//! Bounded shard-to-dispatcher commands over separate OMQ inproc lanes.
//! Payload leases follow final aliases; completions are correlated on the shard.

mod retention;
mod service;
mod wire;
use super::inproc::Inbox;
use super::{PublicationResult, ReplyError, RouteState, ServiceError};
use crate::{
    dispatch::{self, Budget, Class},
    signal::Closed,
};
use omq_tokio::Message;
use retention::{Capacity, Retention};
pub(super) use service::Mailbox;
pub use service::PortSetupError;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use wire::Status;

// Covers command, correlation entry, one-shot reply, adapters and descriptors.
const COMMAND_BYTES: usize = 1024;

/// Dispatcher reply-queue admission outcome. Full preserves the owning message.
pub type ReplyResult = Result<(), (ReplyError, Message)>;
/// One trusted shard's route publication outcome. Queue admission is not an
/// election result or proof that any SDK has observed the hint.
pub type RouteResult = Result<bool, RouteError>;

/// Route publication was outside the shard or rejected by watch state.
#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    /// Port belongs to another configured partition owner.
    #[error("route publication belongs to another shard")]
    Destination,
    /// Configured route identity or view was refused.
    #[error(transparent)]
    Watch(#[from] ServiceError),
}

#[derive(Debug)]
struct Completion<T> {
    result: T,
    _retention: Retention,
}

/// Observer of one bounded dispatcher command. Poll `Port::poll_progress`
/// beside this future on the shard owner. Canceling observation does not undo
/// accepted work. Record identity and retries belong to the application protocol.
#[derive(Debug)]
pub struct Pending<T> {
    receiver: oneshot::Receiver<Completion<T>>,
    closed: Pin<Box<Closed>>,
}

impl<T> Future for Pending<T> {
    type Output = Result<T, PortError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(result) = Pin::new(&mut self.receiver).poll(cx) {
            return Poll::Ready(
                result
                    .map(|completion| completion.result)
                    .map_err(|_| PortError::Closed),
            );
        }
        self.closed
            .as_mut()
            .poll(cx)
            .map(|()| Err(PortError::Closed))
    }
}

/// Queued command awaiting its completion. Messages stay here so a refusal
/// can return them to the caller.
#[derive(Debug)]
enum Command {
    Reply {
        message: Message,
        reply: oneshot::Sender<Completion<ReplyResult>>,
    },
    Publication {
        message: Message,
        reply: oneshot::Sender<Completion<PublicationResult>>,
    },
    Route(oneshot::Sender<Completion<RouteResult>>),
}

impl Request {
    /// Deliver a completion of the matching kind. Any other kind leaves the
    /// request pending and returns it.
    fn complete(self, status: Status) -> Result<(), Self> {
        let retention = self.retention;
        match (self.command, status) {
            (Command::Reply { message, reply }, Status::Reply(result)) => {
                finish(reply, result.map_err(|error| (error, message)), retention);
            }
            (Command::Publication { message, reply }, Status::Publication(result)) => {
                finish(reply, result.map_err(|error| (error, message)), retention);
            }
            (Command::Route(reply), Status::Route(result)) => {
                finish(reply, wire::route_result(result), retention);
            }
            (command, _) => return Err(Self { command, retention }),
        }
        Ok(())
    }
}

fn finish<T>(reply: oneshot::Sender<Completion<T>>, result: T, retention: Retention) {
    let _ = reply.send(Completion {
        result,
        _retention: retention,
    });
}

/// Command queued to the dispatcher; its retention charge now travels with it.
struct Sent {
    id: u64,
    retention: Retention,
}

#[derive(Debug)]
struct Request {
    command: Command,
    retention: Retention,
}

/// Shard-owned command sockets and bounded completion correlations. Data cannot
/// consume control capacity. Unobserved completions and socket-retained aliases
/// keep their count/byte charges until physical release.
#[derive(Debug)]
pub struct Port {
    lanes: [Inbox; 2],
    capacity: Capacity,
    requests: BTreeMap<u64, Request>,
    incarnation: ozzy_proto::RequestId,
    next_id: u64,
    _context: omq_tokio::Context,
}

impl Drop for Port {
    fn drop(&mut self) {
        self.capacity.closed.close();
    }
}

impl Port {
    /// Capture before admission and use `changed_after` if capacity is full.
    pub fn generation(&self) -> u64 {
        self.capacity.changed().generation()
    }

    /// Independent readiness observation, cancel-safe across later send calls.
    pub fn changed_after(&self, generation: u64) -> impl Future<Output = ()> + use<> {
        let changed = self.capacity.changed().clone();
        let closed = self.capacity.closed.clone();
        async move {
            tokio::select! {
                () = changed.changed_after(generation) => {},
                () = closed.closed() => {},
            }
        }
    }

    /// Submit a reply under its full backing-storage charge. Failure leaves the
    /// message with the caller. Success means only queued for the dispatcher;
    /// await the returned observation for per-peer outbox admission. A full peer
    /// returns the original message, allowing unrelated peers to keep progressing.
    /// `sequence` orders this shard's replies to one destination: the
    /// dispatcher refuses a later sequence while an earlier refused one
    /// awaits re-admission, so a retried reply cannot be overtaken.
    pub fn try_reply(
        &mut self,
        class: Class,
        message: Message,
        retained_bytes: usize,
        sequence: u64,
    ) -> Result<Pending<ReplyResult>, (PortError, Message)> {
        if message.len() != 4 && message.len() != 2 {
            return Err((PortError::Frames, message));
        }
        let Some(bytes) = message_charge(&message, retained_bytes) else {
            return Err((PortError::Charge, message));
        };
        let body = wire::Body::Reply {
            message: &message,
            sequence,
        };
        match self.send(index(class), class, bytes, body) {
            Ok(sent) => Ok(self.track(sent, |reply| Command::Reply { message, reply })),
            Err(error) => Err((error, message)),
        }
    }

    /// Queue one leader publication through the data lane. The dispatcher
    /// validates its owning shard and current source before PUB admission.
    /// Full capacity preserves the message; no control capacity is consumed.
    pub fn try_publish(
        &mut self,
        message: Message,
        retained_bytes: usize,
    ) -> Result<Pending<PublicationResult>, (PortError, Message)> {
        if message.len() != 4
            || message
                .part_slice(0)
                .is_none_or(|prefix| !matches!(prefix.len(), 16 | 32))
        {
            return Err((PortError::Frames, message));
        }
        let Some(bytes) = message_charge(&message, retained_bytes) else {
            return Err((PortError::Charge, message));
        };
        if self.capacity.limits[2] == Budget::default() {
            return Err((PortError::Publication, message));
        }
        match self.send(2, Class::Data, bytes, wire::Body::Publication(&message)) {
            Ok(sent) => Ok(self.track(sent, |reply| Command::Publication { message, reply })),
            Err(error) => Err((error, message)),
        }
    }

    /// Publish one actor's current route observation through this shard's
    /// bounded control lane. The dispatcher verifies shard placement before
    /// updating session-scoped watches. Completion is local admission only.
    pub fn try_route(
        &mut self,
        route: RouteState,
    ) -> Result<Pending<RouteResult>, (PortError, RouteState)> {
        let encoded = route.valid().then(|| wire::route_message(&route)).flatten();
        let Some(encoded) = encoded else {
            return Err((PortError::Route, route));
        };
        let body = wire::Body::Route(encoded);
        match self.send(index(Class::Control), Class::Control, COMMAND_BYTES, body) {
            Ok(sent) => Ok(self.track(sent, Command::Route)),
            Err(error) => Err((error, route)),
        }
    }

    /// Correlate a queued command with its completion observer.
    fn track<T>(
        &mut self,
        sent: Sent,
        command: impl FnOnce(oneshot::Sender<Completion<T>>) -> Command,
    ) -> Pending<T> {
        let (reply, receiver) = oneshot::channel();
        let request = Request {
            command: command(reply),
            retention: sent.retention,
        };
        self.requests.insert(sent.id, request);
        Pending {
            receiver,
            closed: Box::pin(self.capacity.closed.closed()),
        }
    }

    /// Drain a bounded completion batch and register OMQ receive wakeups. Call
    /// on the shard owner before polling command observers. No destination wait
    /// blocks another lane. Returns whether any command outcome was observed.
    pub fn poll_progress(&mut self, cx: &mut Context<'_>) -> Result<bool, PortError> {
        let progressed = self.collect_results()?;
        for lane in &mut self.lanes {
            if let Poll::Ready(result) = lane.poll_ready(cx) {
                result.map_err(|_| PortError::Closed)?;
                cx.waker().wake_by_ref();
            }
        }
        Ok(progressed)
    }

    fn collect_results(&mut self) -> Result<bool, PortError> {
        if self.capacity.closed.is_closed() {
            self.requests.clear();
            self.capacity.collect();
            return Ok(false);
        }
        let mut progressed = false;
        for index in [1, 0] {
            for _ in 0..16 {
                match self.lanes[index].try_recv() {
                    Ok(message) => {
                        self.observe(&message)?;
                        progressed = true;
                    }
                    Err(omq_tokio::Error::WouldBlock) => break,
                    Err(_) => return Err(PortError::Closed),
                }
            }
        }
        self.capacity.collect();
        Ok(progressed)
    }

    fn observe(&mut self, message: &Message) -> Result<(), PortError> {
        let (key, status) = wire::decode_completion(message)?;
        if key.generation == self.incarnation
            && let Some(request) = self.requests.remove(&key.id)
            && let Err(request) = request.complete(status)
        {
            self.requests.insert(key.id, request);
        }
        Ok(())
    }

    /// Queue one command under `bytes` of `bucket` capacity. Failure leaves
    /// the borrowed content with the caller.
    fn send(
        &mut self,
        bucket: usize,
        class: Class,
        bytes: usize,
        body: wire::Body<'_>,
    ) -> Result<Sent, PortError> {
        let next_id = self.next_id.checked_add(1).ok_or(PortError::Exhausted)?;
        self.collect_results()?;
        let (queue, retention) = self.capacity.reserve(bucket, bytes)?;
        let key = wire::Key {
            generation: self.incarnation,
            id: self.next_id,
        };
        self.next_id = next_id;
        let message = wire::command(key, class, body, queue, &retention);
        if let Err(error) = self.lanes[index(class)].socket.try_send(message) {
            drop(retention);
            self.capacity.collect();
            return Err(PortError::Admission(send_failure(&error)));
        }
        Ok(Sent {
            id: key.id,
            retention,
        })
    }
}

fn send_failure(error: &omq_tokio::TrySendError) -> dispatch::SendFailure {
    match error {
        omq_tokio::TrySendError::Full(_) => dispatch::SendFailure::Admission(dispatch::Error::Full),
        omq_tokio::TrySendError::Closed => dispatch::SendFailure::Closed,
        omq_tokio::TrySendError::Error(_) => dispatch::SendFailure::CapacityInvariant,
    }
}

fn message_charge(message: &Message, retained_bytes: usize) -> Option<usize> {
    retained_bytes.checked_add(COMMAND_BYTES).filter(|_| {
        retained_bytes
            >= message
                .max_message_size_len()
                .saturating_add(std::mem::size_of::<Message>())
    })
}

const fn index(class: Class) -> usize {
    match class {
        Class::Data => 0,
        Class::Control => 1,
    }
}

fn valid_budget(budget: Budget) -> bool {
    budget.queue_slots > 0
        && budget.retained_messages > 0
        && budget.bytes > 0
        && isize::try_from(budget.bytes).is_ok()
}

/// Outgoing command refused before queue ownership transferred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PortError {
    /// No separate publication allowance was reserved for this port.
    #[error("frontend port publications disabled")]
    Publication,
    /// Route identity or bounded membership is malformed.
    #[error("invalid frontend route publication")]
    Route,
    /// Replies require one routing frame and exactly three native frames.
    #[error("invalid frontend port reply frames")]
    Frames,
    /// Retained-byte charge undercounts visible frames or overflows.
    #[error("invalid frontend port backing charge")]
    Charge,
    /// Dispatcher ended before reporting this command's outcome.
    #[error("frontend port closed")]
    Closed,
    /// This port incarnation used every request identity.
    #[error("frontend port request identities exhausted")]
    Exhausted,
    /// Independent class capacity is full, revoked, or closed.
    #[error("frontend port admission failed: {0:?}")]
    Admission(dispatch::SendFailure),
}

/// Fatal service progress failure; pending protocol records remain uncertain.
#[derive(Debug, thiserror::Error)]
pub enum ProgressError {
    /// Internal command accounting invariant failed.
    #[error(transparent)]
    Admission(#[from] dispatch::Error),
    /// Routing notice could not fit the configured control reply boundary.
    #[error(transparent)]
    Watch(#[from] super::ServiceError),
    /// OMQ socket failed, independently of application confirmation.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
}

#[cfg(test)]
mod tests;
