//! Bounded shard-to-dispatcher commands. The shard owns command capacity;
//! final aliases return it through a bounded mailbox.

use std::{
    future::Future,
    ops::Bound::{Excluded, Unbounded},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::NodeId;
use tokio::sync::{mpsc, oneshot};

use super::{
    GrantSpec, PublicationResult, ReplyError, RouteState, Service, ServiceError, SetupError,
};
use crate::{
    command_channel::{
        NotifiedReceiver, NotifiedSender, TryRecvError, TrySendError, notified_channel,
    },
    dispatch::{self, Budget, Budgets, Class, Grant},
    signal::{CloseSignal, StateSignal},
};

// Covers command, one-shot reply, retention adapters, and frame descriptors.
const COMMAND_BYTES: usize = 1024;

/// Dispatcher installation outcome. Failure returns unused owning credit.
pub type InstallResult = Result<(), (SetupError, Grant)>;
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

#[derive(Clone, Copy, Debug)]
enum Release {
    Queue(usize),
    Retained(usize, usize),
}

#[derive(Debug)]
struct Return {
    sender: mpsc::Sender<Release>,
    changed: Arc<StateSignal>,
    closed: CloseSignal,
}

impl Return {
    fn release(&self, released: Release) {
        match self.sender.try_send(released) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                panic!("bounded port return mailbox overflowed")
            }
        }
        self.changed.notify_changed();
    }
}

#[derive(Debug)]
struct QueueSlot {
    bucket: usize,
    returns: Arc<Return>,
}

impl Drop for QueueSlot {
    fn drop(&mut self) {
        self.returns.release(Release::Queue(self.bucket));
    }
}

#[derive(Clone, Debug)]
struct Retention(Arc<Retained>);

#[derive(Debug)]
struct Retained {
    bucket: usize,
    bytes: usize,
    returns: Arc<Return>,
}

impl Drop for Retained {
    fn drop(&mut self) {
        self.returns
            .release(Release::Retained(self.bucket, self.bytes));
    }
}

#[derive(Debug)]
struct TrackedBytes {
    bytes: Bytes,
    _retention: Retention,
}

impl AsRef<[u8]> for TrackedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Retention {
    fn attach(&self, bytes: Bytes) -> Bytes {
        debug_assert!(bytes.len() <= self.0.bytes);
        Bytes::from_owner(TrackedBytes {
            bytes,
            _retention: self.clone(),
        })
    }
}

#[derive(Debug)]
struct Queued {
    value: Command,
    class: Class,
    queue: QueueSlot,
    retention: Retention,
}

#[derive(Debug)]
struct Received<T> {
    value: T,
    class: Class,
    retention: Retention,
}

impl Received<Message> {
    fn into_retained_message(self) -> Message {
        Message::multipart_payloads((0..self.value.len()).map(|index| {
            omq_tokio::message::Payload::from_bytes(
                self.retention
                    .attach(self.value.part_bytes(index).expect("frame index")),
            )
        }))
    }
}

/// Observer of one bounded dispatcher command. Canceling observation does not
/// undo an installation or transmission. Revoke a grant by its shard-held key
/// when abandoning it. Record retry state belongs to the application protocol.
#[derive(Debug)]
pub struct Pending<T>(oneshot::Receiver<Completion<T>>);

impl<T> Future for Pending<T> {
    type Output = Result<T, PortError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|result| {
            result
                .map(|completion| completion.result)
                .map_err(|_| PortError::Closed)
        })
    }
}

#[derive(Debug)]
enum Command {
    Install {
        peer: NodeId,
        spec: Box<GrantSpec>,
        grant: Grant,
        reply: oneshot::Sender<Completion<InstallResult>>,
    },
    Reply {
        message: Message,
        reply: oneshot::Sender<Completion<ReplyResult>>,
    },
    Publication {
        message: Message,
        reply: oneshot::Sender<Completion<PublicationResult>>,
    },
    Route {
        route: RouteState,
        reply: oneshot::Sender<Completion<RouteResult>>,
    },
}

/// One application shard's outgoing fanring producer. Data cannot consume its
/// control slots. Unobserved completions and socket-retained aliases keep their
/// count/byte charges, so neither creates an unbounded side queue.
#[derive(Debug)]
pub struct Port {
    sender: NotifiedSender<Queued>,
    limits: [Budget; 3],
    used: [Budget; 3],
    returned: mpsc::Receiver<Release>,
    returns: Arc<Return>,
}

impl Port {
    /// Capture before admission and use `changed_after` if capacity is full.
    pub fn generation(&self) -> u64 {
        self.returns.changed.generation()
    }

    /// Independent readiness observation, cancel-safe across later send calls.
    pub fn changed_after(&self, generation: u64) -> impl Future<Output = ()> + use<> {
        let changed = self.returns.changed.clone();
        let closed = self.returns.closed.clone();
        async move {
            tokio::select! {
                () = changed.changed_after(generation) => {},
                () = closed.closed() => {},
            }
        }
    }

    /// Submit a shard-issued ingress reservation. Await successful installation
    /// before advertising its protocol credit. This queue has separate control
    /// capacity from payload replies. Rejection retains the original token.
    pub fn try_install(
        &mut self,
        peer: NodeId,
        spec: impl Into<GrantSpec>,
        grant: Grant,
    ) -> Result<Pending<InstallResult>, (PortError, Grant)> {
        let (reply, receiver) = oneshot::channel();
        let command = Command::Install {
            peer,
            spec: Box::new(spec.into()),
            grant,
            reply,
        };
        match self.submit(Class::Control, COMMAND_BYTES, command) {
            Ok(()) => Ok(Pending(receiver)),
            Err((error, Command::Install { grant, .. })) => Err((error, grant)),
            Err(_) => unreachable!("unchanged command"),
        }
    }

    /// Submit a reply under its full backing-storage charge. Failure leaves the
    /// message with the caller. Success means only queued for the dispatcher;
    /// await the returned observation for per-peer outbox admission. A full peer
    /// returns the original message, allowing unrelated peers to keep progressing.
    pub fn try_reply(
        &mut self,
        class: Class,
        message: Message,
        retained_bytes: usize,
    ) -> Result<Pending<ReplyResult>, (PortError, Message)> {
        if message.len() != 4 && message.len() != 2 {
            return Err((PortError::Frames, message));
        }
        let Some(bytes) = retained_bytes.checked_add(COMMAND_BYTES).filter(|_| {
            retained_bytes
                >= message
                    .max_message_size_len()
                    .saturating_add(std::mem::size_of::<Message>())
        }) else {
            return Err((PortError::Charge, message));
        };
        let (reply, receiver) = oneshot::channel();
        match self.submit(class, bytes, Command::Reply { message, reply }) {
            Ok(()) => Ok(Pending(receiver)),
            Err((error, Command::Reply { message, .. })) => Err((error, message)),
            Err(_) => unreachable!("unchanged command"),
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
                .is_none_or(|prefix| prefix.len() != 32)
        {
            return Err((PortError::Frames, message));
        }
        let Some(bytes) = retained_bytes.checked_add(COMMAND_BYTES).filter(|_| {
            retained_bytes
                >= message
                    .max_message_size_len()
                    .saturating_add(std::mem::size_of::<Message>())
        }) else {
            return Err((PortError::Charge, message));
        };
        let (reply, receiver) = oneshot::channel();
        let command = Command::Publication { message, reply };
        let result = if self.limits[2] == Budget::default() {
            Err((PortError::Publication, command))
        } else {
            self.submit_bucket(2, Class::Data, bytes, command)
        };
        match result {
            Ok(()) => Ok(Pending(receiver)),
            Err((error, Command::Publication { message, .. })) => Err((error, message)),
            Err(_) => unreachable!("unchanged publication"),
        }
    }

    /// Publish one actor's current route observation through this shard's
    /// bounded control lane. The dispatcher verifies shard placement before
    /// updating session-scoped watches. Completion is local admission only.
    pub fn try_route(
        &mut self,
        route: RouteState,
    ) -> Result<Pending<RouteResult>, (PortError, RouteState)> {
        if !route.valid() {
            return Err((PortError::Route, route));
        }
        let (reply, receiver) = oneshot::channel();
        match self.submit(
            Class::Control,
            COMMAND_BYTES,
            Command::Route { route, reply },
        ) {
            Ok(()) => Ok(Pending(receiver)),
            Err((error, Command::Route { route, .. })) => Err((error, route)),
            Err(_) => unreachable!("unchanged route command"),
        }
    }

    fn submit(
        &mut self,
        class: Class,
        bytes: usize,
        command: Command,
    ) -> Result<(), (PortError, Command)> {
        self.submit_bucket(index(class), class, bytes, command)
    }

    fn submit_bucket(
        &mut self,
        bucket: usize,
        class: Class,
        bytes: usize,
        command: Command,
    ) -> Result<(), (PortError, Command)> {
        self.collect_returns();
        if self.returns.closed.is_closed() || self.sender.is_disconnected() {
            return Err((PortError::Admission(dispatch::SendFailure::Closed), command));
        }
        let limit = self.limits[bucket];
        let used = &mut self.used[bucket];
        if used.queue_slots >= limit.queue_slots
            || used.retained_messages >= limit.retained_messages
            || bytes > limit.bytes.saturating_sub(used.bytes)
        {
            return Err((
                PortError::Admission(dispatch::SendFailure::Admission(dispatch::Error::Full)),
                command,
            ));
        }
        used.queue_slots += 1;
        used.retained_messages += 1;
        used.bytes += bytes;
        let queued = Queued {
            value: command,
            class,
            queue: QueueSlot {
                bucket,
                returns: self.returns.clone(),
            },
            retention: Retention(Arc::new(Retained {
                bucket,
                bytes,
                returns: self.returns.clone(),
            })),
        };
        match self.sender.try_send(queued) {
            Ok(()) => Ok(()),
            Err(error) => {
                let (queued, reason) = match error {
                    TrySendError::Full(queued) => {
                        (queued, dispatch::SendFailure::CapacityInvariant)
                    }
                    TrySendError::Disconnected(queued) => (queued, dispatch::SendFailure::Closed),
                };
                let Queued {
                    value,
                    queue,
                    retention,
                    ..
                } = queued;
                drop(queue);
                drop(retention);
                self.collect_returns();
                Err((PortError::Admission(reason), value))
            }
        }
    }

    fn collect_returns(&mut self) {
        while let Ok(released) = self.returned.try_recv() {
            match released {
                Release::Queue(bucket) => self.used[bucket].queue_slots -= 1,
                Release::Retained(bucket, bytes) => {
                    self.used[bucket].retained_messages -= 1;
                    self.used[bucket].bytes -= bytes;
                }
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct Mailbox {
    receiver: NotifiedReceiver<Queued>,
    returns: Arc<Return>,
    closed: bool,
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        self.returns.closed.close();
    }
}

impl Service {
    /// Construct on the dispatcher, then send the port to this configured shard.
    /// At most one port per shard. Outbound budgets are independent of ingress
    /// and per-peer socket queues, and never multiply after reconnects.
    pub fn port(&mut self, shard: u32, capacity: Budgets) -> Result<Port, SetupError> {
        self.make_port(shard, capacity, None)
    }

    /// Reserve publication capacity inside the configured outgoing data budget.
    /// PUB aliases cannot consume the remaining reply allowance. Both share the
    /// same bounded fanring and retain independent grant/accounting lifetimes.
    pub fn port_with_publications(
        &mut self,
        shard: u32,
        capacity: Budgets,
        publications: Budget,
    ) -> Result<Port, SetupError> {
        if publications.queue_slots == 0
            || publications.retained_messages == 0
            || publications.bytes < COMMAND_BYTES
            || publications.queue_slots >= capacity.data.queue_slots
            || publications.retained_messages >= capacity.data.retained_messages
            || publications.bytes >= capacity.data.bytes
        {
            return Err(SetupError::Limits);
        }
        self.make_port(shard, capacity, Some(publications))
    }

    fn make_port(
        &mut self,
        shard: u32,
        capacity: Budgets,
        publications: Option<Budget>,
    ) -> Result<Port, SetupError> {
        if !self.dispatcher.routes.shards.contains(&shard) || self.ports.contains_key(&shard) {
            return Err(SetupError::Destination);
        }
        if capacity.control.bytes < COMMAND_BYTES {
            return Err(SetupError::Limits);
        }
        let mut replies = capacity;
        if let Some(publications) = publications {
            replies.data.queue_slots -= publications.queue_slots;
            replies.data.retained_messages -= publications.retained_messages;
            replies.data.bytes -= publications.bytes;
        }
        if ![replies.data, replies.control]
            .into_iter()
            .all(valid_budget)
        {
            return Err(SetupError::Limits);
        }
        let limits = [
            replies.data,
            replies.control,
            publications.unwrap_or_default(),
        ];
        let slots = limits
            .iter()
            .try_fold(0usize, |sum, budget| sum.checked_add(budget.queue_slots))
            .filter(|slots| *slots > 0 && *slots <= fanring::mpsc::MAX_CAPACITY_PER_SENDER)
            .ok_or(SetupError::Limits)?;
        let returns_capacity = limits
            .iter()
            .try_fold(0usize, |sum, budget| {
                sum.checked_add(budget.queue_slots)?
                    .checked_add(budget.retained_messages)
            })
            .ok_or(SetupError::Limits)?;
        let (sender, receiver) = notified_channel(slots);
        let (return_to, returned) = mpsc::channel(returns_capacity);
        let returns = Arc::new(Return {
            sender: return_to,
            changed: Arc::new(StateSignal::default()),
            closed: CloseSignal::default(),
        });
        self.ports.insert(
            shard,
            Mailbox {
                receiver,
                returns: returns.clone(),
                closed: false,
            },
        );
        Ok(Port {
            sender,
            limits,
            used: [Budget::default(); 3],
            returned,
            returns,
        })
    }

    /// Process at most one shard command, rotating even after empty/full peers.
    /// Installation and reply rejection are returned to their bounded observer.
    pub fn poll_command(&mut self) -> Result<bool, dispatch::Error> {
        let next = self
            .port_turn
            .and_then(|last| self.ports.range((Excluded(last), Unbounded)).next())
            .or_else(|| self.ports.first_key_value())
            .map(|(&id, _)| id);
        let Some(shard) = next else { return Ok(false) };
        self.port_turn = Some(shard);
        let mailbox = self.ports.get_mut(&shard).expect("selected shard");
        if mailbox.closed {
            return Ok(false);
        }
        let received = match mailbox.receiver.try_recv() {
            Ok(received) => received,
            Err(TryRecvError::Empty) => return Ok(false),
            Err(TryRecvError::Disconnected) => {
                mailbox.closed = true;
                return Ok(true);
            }
        };
        let Queued {
            value,
            class,
            queue,
            retention,
        } = received;
        drop(queue);
        self.apply_command(
            shard,
            Received {
                value,
                class,
                retention,
            },
        );
        Ok(true)
    }

    fn apply_command(
        &mut self,
        shard: u32,
        Received {
            value,
            class,
            retention,
        }: Received<Command>,
    ) {
        match value {
            Command::Install {
                peer,
                spec,
                grant,
                reply,
            } => {
                let result = if spec.target().shard(&self.dispatcher.routes) == Some(shard) {
                    self.install(peer, *spec, grant)
                } else {
                    Err((SetupError::Destination, grant))
                };
                let _ = reply.send(Completion {
                    result,
                    _retention: retention,
                });
            }
            Command::Reply { message, reply } => {
                // Keep an unwrapped alias only through admission. A full peer
                // returns it without retaining this queue's byte reservation,
                // so retry cannot deadlock while asking for its own old credit.
                let retry = message.clone();
                let retained = Received {
                    value: message,
                    class,
                    retention: retention.clone(),
                }
                .into_retained_message();
                let result = self
                    .try_reply(class, retained)
                    .map_err(|(error, _)| (error, retry));
                let _ = reply.send(Completion {
                    result,
                    _retention: retention,
                });
            }
            Command::Route { route, reply } => {
                let result = if self
                    .dispatcher
                    .routes
                    .partitions
                    .get(&route.group)
                    .is_some_and(|placement| {
                        placement.shard == shard && placement.partition == route.partition
                    }) {
                    self.publish_route(&route).map_err(RouteError::from)
                } else {
                    Err(RouteError::Destination)
                };
                let _ = reply.send(Completion {
                    result,
                    _retention: retention,
                });
            }
            Command::Publication { message, reply } => {
                let retry = message.clone();
                let retained = Received {
                    value: message,
                    class,
                    retention: retention.clone(),
                }
                .into_retained_message();
                let result = self
                    .publish(shard, retained)
                    .map_err(|(error, _)| (error, retry));
                let _ = reply.send(Completion {
                    result,
                    _retention: retention,
                });
            }
        }
    }

    /// Wait for commands, released backing capacity, or socket progress. Both
    /// command and socket paths use bounded turns. Canceling the wait leaves
    /// accepted commands and queued replies owned by this service.
    pub async fn progress(
        &mut self,
        socket: &omq_tokio::IdentitySocket,
    ) -> Result<(), ProgressError> {
        use futures::{StreamExt, stream::FuturesUnordered};
        let mut waits: FuturesUnordered<Pin<Box<dyn Future<Output = ()>>>> =
            FuturesUnordered::new();
        for mailbox in self.ports.values_mut().filter(|mailbox| !mailbox.closed) {
            waits.push(Box::pin(mailbox.receiver.owned_ready()));
        }
        for index in 0..self.ports.len() {
            if self.poll_command()? {
                return Ok(());
            }
            if index % 64 == 63 {
                tokio::task::yield_now().await;
            }
        }
        if self.poll_watch()? {
            return Ok(());
        }
        tokio::select! {
            result = self.flush_ready(socket), if self.has_pending() => result?,
            _ = waits.next(), if !waits.is_empty() => {},
            else => std::future::pending().await,
        }
        Ok(())
    }
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
