//! Dispatcher side of bounded shard command lanes.

use super::{
    Port, ProgressError, ServiceError,
    retention::{Capacity, Return},
    wire,
};
use crate::{
    dispatch::{self, Budget, Budgets, Class},
    frontend::{
        Service, SetupError,
        inproc::{self, Inbox},
    },
};
use omq_tokio::{Message, Options, TrySendError};
use std::{
    future::Future,
    ops::Bound::{Excluded, Unbounded},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

type Wait = Pin<Box<dyn Future<Output = ()> + Send>>;

struct Parked {
    message: Message,
    wait: Wait,
}
impl std::fmt::Debug for Parked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parked")
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(in crate::frontend) struct Mailbox {
    lanes: [Inbox; 2],
    parked: [Option<Parked>; 2],
    returns: Arc<Return>,
    incarnation: ozzy_proto::RequestId,
    next_lane: usize,
    _context: omq_tokio::Context,
}
impl Drop for Mailbox {
    fn drop(&mut self) {
        self.returns.closed.close();
    }
}

impl Mailbox {
    fn send(&mut self, index: usize, message: Message) -> Result<(), dispatch::Error> {
        let socket = &self.lanes[index].socket;
        match socket.try_send(message) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(message)) => {
                let socket = socket.clone_shared();
                let probe = message.clone();
                self.parked[index] = Some(Parked {
                    message,
                    wait: Box::pin(async move { socket.wait_send_progress_for(&probe).await }),
                });
                Ok(())
            }
            Err(_) => Err(dispatch::Error::Closed),
        }
    }

    fn receive(&mut self) -> Result<Option<(usize, wire::Incoming)>, dispatch::Error> {
        if self.returns.closed.is_closed() {
            return Ok(None);
        }
        for _ in 0..2 {
            let index = self.next_lane;
            self.next_lane ^= 1;
            if let Some(parked) = self.parked[index].take() {
                self.send(index, parked.message)?;
            }
            if self.parked[index].is_some() {
                continue;
            }
            match self.lanes[index].try_recv() {
                Ok(message) => {
                    let class = if index == 0 {
                        Class::Data
                    } else {
                        Class::Control
                    };
                    return wire::decode(message, self.incarnation, class)
                        .map(|input| Some((index, input)))
                        .map_err(|_| dispatch::Error::Invalid);
                }
                Err(omq_tokio::Error::WouldBlock) => {}
                Err(_) => return Err(dispatch::Error::Closed),
            }
        }
        Ok(None)
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), omq_tokio::Error>> {
        if self.returns.closed.is_closed() {
            return Poll::Pending;
        }
        for index in [1, 0] {
            if let Some(parked) = &mut self.parked[index] {
                if parked.wait.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Ok(()));
                }
            } else if let Poll::Ready(result) = self.lanes[index].poll_ready(cx) {
                return Poll::Ready(result);
            }
        }
        Poll::Pending
    }
}

/// Shard port setup failed before publishing the port.
#[derive(Debug, thiserror::Error)]
pub enum PortSetupError {
    /// Configured shard or owner-local budgets are invalid.
    #[error(transparent)]
    Configuration(#[from] SetupError),
    /// Broker-local OMQ socket creation failed.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
}

impl Service {
    /// Create on the dispatcher's OMQ context, then move to this configured
    /// shard. One port per shard; reconnects cannot multiply local capacity.
    pub fn port(
        &mut self,
        context: &omq_tokio::Context,
        shard: u32,
        capacity: Budgets,
    ) -> Result<Port, PortSetupError> {
        self.make_port(context, shard, capacity, None)
    }

    /// Reserve publication capacity inside the outgoing data allowance. Data
    /// and control use separate inproc sockets; PUB aliases retain their own
    /// allowance until final release, without consuming reply capacity.
    pub fn port_with_publications(
        &mut self,
        context: &omq_tokio::Context,
        shard: u32,
        capacity: Budgets,
        publications: Budget,
    ) -> Result<Port, PortSetupError> {
        self.make_port(context, shard, capacity, Some(publications))
    }

    fn make_port(
        &mut self,
        context: &omq_tokio::Context,
        shard: u32,
        capacity: Budgets,
        publications: Option<Budget>,
    ) -> Result<Port, PortSetupError> {
        if !self.dispatcher.routes.shards.contains(&shard) || self.ports.contains_key(&shard) {
            return Err(SetupError::Destination.into());
        }
        if context.io_threads() == 0 {
            return Err(SetupError::Limits.into());
        }
        let limits = outgoing_limits(capacity, publications)?;
        let return_slots = limits
            .iter()
            .try_fold(0usize, |sum, limit| {
                sum.checked_add(limit.queue_slots)?
                    .checked_add(limit.retained_messages)
            })
            .ok_or(SetupError::Limits)?;
        let pairs = [
            capacity_lane(context, limits, Class::Data)?,
            capacity_lane(context, limits, Class::Control)?,
        ];
        let capacity = Capacity::new(limits, return_slots);
        let [(data, data_in), (control, control_in)] = pairs;
        let incarnation = ozzy_proto::RequestId::new();
        self.ports.insert(
            shard,
            Mailbox {
                lanes: [Inbox::new(data_in), Inbox::new(control_in)],
                parked: [None, None],
                returns: capacity.returns.clone(),
                incarnation,
                next_lane: 1,
                _context: context.clone(),
            },
        );
        Ok(Port {
            lanes: [Inbox::new(data), Inbox::new(control)],
            capacity,
            requests: std::collections::BTreeMap::new(),
            incarnation,
            next_id: 1,
            _context: context.clone(),
        })
    }

    /// Process one command. Rotate shards and classes after every bounded turn;
    /// a parked data completion leaves control and other shards runnable.
    pub fn poll_command(&mut self) -> Result<bool, dispatch::Error> {
        let next = self
            .port_turn
            .and_then(|last| self.ports.range((Excluded(last), Unbounded)).next())
            .or_else(|| self.ports.first_key_value())
            .map(|(&id, _)| id);
        let Some(shard) = next else { return Ok(false) };
        self.port_turn = Some(shard);
        let Some((index, incoming)) = self
            .ports
            .get_mut(&shard)
            .expect("selected shard")
            .receive()?
        else {
            return Ok(false);
        };
        let status = self.apply_command(shard, incoming.action);
        let completion =
            Message::with_prefix(incoming.identity, wire::completion(incoming.key, status));
        self.ports
            .get_mut(&shard)
            .expect("selected shard")
            .send(index, completion)?;
        Ok(true)
    }

    fn apply_command(&mut self, shard: u32, action: wire::Action) -> wire::Status {
        match action {
            wire::Action::Reply(class, message) => {
                wire::Status::Reply(self.try_reply(class, message).map_err(|(error, _)| error))
            }
            wire::Action::Publication(message) => {
                wire::Status::Publication(self.publish(shard, message).map_err(|(error, _)| error))
            }
            wire::Action::Route(route) => {
                let result = if self
                    .dispatcher
                    .routes
                    .partitions
                    .get(&route.group)
                    .is_some_and(|placement| {
                        placement.shard == shard && placement.partition == route.partition
                    }) {
                    match self.publish_route(&route) {
                        Ok(false) => wire::RouteStatus::Unchanged,
                        Ok(true) => wire::RouteStatus::Changed,
                        Err(ServiceError::Watch) => wire::RouteStatus::Watch,
                        Err(_) => unreachable!("route publication only accesses watch state"),
                    }
                } else {
                    wire::RouteStatus::Destination
                };
                wire::Status::Route(result)
            }
        }
    }

    /// Wait for shard commands or network socket progress. Canceling preserves
    /// accepted commands, registered inproc receives, and parked completions.
    pub async fn progress(
        &mut self,
        socket: &omq_tokio::IdentitySocket,
    ) -> Result<(), ProgressError> {
        self.progress_with(
            |message| crate::transport::try_send_peer(socket, message),
            |probe| {
                let socket = socket.clone();
                Box::pin(async move { crate::transport::wait_send_peer(&socket, &probe).await })
            },
        )
        .await
    }

    /// Progress through adapter-owned network routing and destination waits.
    /// Both paths retain the exact unsent message and independent class bounds.
    pub async fn progress_with(
        &mut self,
        send: impl FnMut(Message) -> Result<(), TrySendError>,
        wait: impl FnMut(Message) -> Pin<Box<dyn Future<Output = ()>>>,
    ) -> Result<(), ProgressError> {
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
        let mut send = send;
        if self.flush_turn(&mut send).await? {
            return Ok(());
        }
        let mut replies = self.reply_progress(wait).await;
        std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = replies.poll_ready(self, cx, &mut send) {
                return Poll::Ready(result);
            }
            for mailbox in self.ports.values_mut() {
                if let Poll::Ready(result) = mailbox.poll_ready(cx) {
                    return Poll::Ready(result);
                }
            }
            Poll::Pending
        })
        .await?;
        Ok(())
    }
}

fn capacity_lane(
    context: &omq_tokio::Context,
    limits: [Budget; 3],
    class: Class,
) -> Result<(omq_tokio::Socket, omq_tokio::Socket), PortSetupError> {
    let buckets: &[usize] = match class {
        Class::Data => &[0, 2],
        Class::Control => &[1],
    };
    let slots = buckets
        .iter()
        .try_fold(0usize, |sum, &index| {
            sum.checked_add(
                limits[index]
                    .queue_slots
                    .max(limits[index].retained_messages),
            )
        })
        .filter(|&slots| slots > 0 && slots <= 65536)
        .ok_or(SetupError::Limits)?;
    let bytes = buckets
        .iter()
        .map(|&index| limits[index].bytes)
        .max()
        .unwrap_or(0);
    let options = Options::default()
        .send_hwm(slots as u32)
        .recv_hwm(slots as u32)
        .max_message_size(bytes)
        .linger(Duration::ZERO);
    Ok(inproc::pair(context, options)?)
}

fn outgoing_limits(
    capacity: Budgets,
    publications: Option<Budget>,
) -> Result<[Budget; 3], SetupError> {
    if capacity.control.bytes < super::COMMAND_BYTES {
        return Err(SetupError::Limits);
    }
    let mut replies = capacity;
    if let Some(publications) = publications {
        if publications.queue_slots == 0
            || publications.retained_messages == 0
            || publications.bytes < super::COMMAND_BYTES
            || publications.queue_slots >= capacity.data.queue_slots
            || publications.retained_messages >= capacity.data.retained_messages
            || publications.bytes >= capacity.data.bytes
        {
            return Err(SetupError::Limits);
        }
        replies.data.queue_slots -= publications.queue_slots;
        replies.data.retained_messages -= publications.retained_messages;
        replies.data.bytes -= publications.bytes;
    }
    if ![replies.data, replies.control]
        .into_iter()
        .all(super::valid_budget)
    {
        return Err(SetupError::Limits);
    }
    Ok([
        replies.data,
        replies.control,
        publications.unwrap_or_default(),
    ])
}
