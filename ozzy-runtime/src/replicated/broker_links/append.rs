//! Logical APPEND reply lanes on the existing physical broker sockets.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use omq_tokio::{Message, TrySendError, message::Payload};
use ozzy_proto::{LinkSessionId, NodeId, Opcode, Packet, RequestId, handshake::Parameters};

use super::{BrokerLinkError, BrokerLinks, StateSignal};
use tokio::sync::mpsc;

mod budget;
use budget::{Budget, Cost, Lease};

/// Fixed APPEND transport bounds for one SDK owner, shared across its brokers.
/// Requests and record windows remain reserved through actual buffer release.
/// Bytes include owned frame backing and reserved worst-case confirmation queues.
/// Writer intake has local count/byte bounds; its declared storage and idle
/// packing capacity are also reserved here before a logical writer opens.
#[derive(Clone, Copy, Debug)]
pub struct AppendLinkLimits {
    /// Logical writers, independent of partition count or physical links.
    pub writers: usize,
    /// Aggregate requests with correlations or retained transport aliases.
    pub requests: usize,
    /// Aggregate transmitted records, including retained old-session frames.
    pub records: usize,
    /// Aggregate reply-lane storage and request/reply backing reservations.
    pub bytes: usize,
}

impl AppendLinkLimits {
    pub(super) fn validate(self) -> Result<(), BrokerLinkError> {
        if !(1..=65536).contains(&self.writers)
            || !(1..=65536).contains(&self.requests)
            || !(1..=1_048_576).contains(&self.records)
            || self.bytes < 16384
            || self.bytes > isize::MAX as usize
        {
            return Err(BrokerLinkError::Configuration);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Stream {
    id: RequestId,
    sender: mpsc::Sender<Message>,
    ready: StateSignal,
    failed: AtomicBool,
    memory: Arc<Lease>,
}

#[derive(Debug)]
struct Request {
    stream: Weak<Stream>,
    writer: RequestId,
    broker: NodeId,
    session: LinkSessionId,
    end: u64,
    remaining_replies: usize,
    lease: Arc<Lease>,
}

#[derive(Debug)]
pub(super) struct Registry {
    budget: Arc<Budget>,
    requests: Mutex<BTreeMap<RequestId, Request>>,
}

/// Keeps declared SDK allocation capacity reserved through owner and alias
/// destruction. It never keeps the socket manager or SDK runtime alive.
#[derive(Clone, Debug)]
pub(in crate::replicated) struct Reservation {
    _lease: Arc<Lease>,
}

impl Registry {
    pub(super) fn new(limits: Option<AppendLinkLimits>) -> Self {
        Self {
            budget: Arc::new(Budget::new(limits)),
            requests: Mutex::new(BTreeMap::new()),
        }
    }

    pub(super) fn fence(&self, broker: NodeId) {
        self.requests
            .lock()
            .expect("SDK APPEND correlations poisoned")
            .retain(|_, request| {
                if request.broker != broker {
                    return true;
                }
                if let Some(stream) = request.stream.upgrade() {
                    stream.failed.store(true, Ordering::Release);
                }
                false
            });
    }

    fn forget(&self, writer: RequestId, confirmed: Option<u64>) {
        self.requests
            .lock()
            .expect("SDK APPEND correlations poisoned")
            .retain(|_, request| {
                request.writer != writer || confirmed.is_some_and(|end| request.end > end)
            });
    }

    /// Single physical receiver never waits for one logical writer's queue.
    /// Canonical authority, writer identity, and offsets stay driver checks.
    pub(super) fn receive(&self, broker: NodeId, message: &Message, packet: Packet<'_>) -> bool {
        let Some(id) = packet.envelope.request_id else {
            return false;
        };
        let mut requests = self
            .requests
            .lock()
            .expect("SDK APPEND correlations poisoned");
        let Some(request) = requests.get_mut(&id) else {
            return false;
        };
        if request.broker != broker
            || packet.envelope.session != Some(request.session)
            || !packet.envelope.response
            || !matches!(packet.envelope.opcode, Opcode::Appended | Opcode::Nack)
            || !packet.payload.is_empty()
        {
            return true;
        }
        let Some(stream) = request.stream.upgrade() else {
            return true;
        };
        if request.remaining_replies == 0 {
            stream.failed.store(true, Ordering::Release);
            self.budget.changed.notify_changed();
            return true;
        }
        request.remaining_replies -= 1;
        let lease = request.lease.clone();
        drop(requests);
        let response = track(message, &lease, Some(&stream.memory), true);
        if stream.sender.try_send(response).is_err() {
            stream.failed.store(true, Ordering::Release);
            self.budget.changed.notify_changed();
        } else {
            stream.ready.notify_changed();
        }
        true
    }
}

#[derive(Clone, Copy, Debug)]
enum Blocked {
    Budget(Cost),
    Socket,
}

/// One logical writer. It neither creates a socket nor receives from a socket.
#[derive(Debug)]
pub(in crate::replicated) struct Connection {
    links: BrokerLinks,
    stream: Arc<Stream>,
    incoming: Mutex<mpsc::Receiver<Message>>,
    broker: Option<NodeId>,
    session: Option<LinkSessionId>,
    parameters: Option<Parameters>,
    blocked: Mutex<Option<Blocked>>,
}

impl Connection {
    pub(in crate::replicated) fn new(
        links: BrokerLinks,
        reply_slots: usize,
        writer_bytes: usize,
        progress_bytes: usize,
    ) -> Result<Self, BrokerLinkError> {
        if links.0.shared.stop.is_closed() {
            return Err(BrokerLinkError::Closed);
        }
        let limits = links
            .0
            .config
            .append
            .ok_or(BrokerLinkError::Configuration)?;
        if reply_slots == 0 || reply_slots > limits.records.saturating_add(limits.requests) {
            return Err(BrokerLinkError::Configuration);
        }
        // Include rounded ring slots, signaling/registration storage, and an
        // idle correlation entry. The sole sender is not cloned per broker.
        let bytes =
            Self::writer_bytes(reply_slots, writer_bytes).ok_or(BrokerLinkError::Configuration)?;
        let memory = links
            .0
            .shared
            .appends
            .budget
            .acquire(Cost {
                writers: 1,
                bytes,
                progress_bytes,
                ..Cost::default()
            })
            .ok_or(BrokerLinkError::Configuration)?;
        let id = links.next_request()?;
        let (sender, incoming) = mpsc::channel(reply_slots);
        Ok(Self {
            links,
            stream: Arc::new(Stream {
                id,
                sender,
                ready: StateSignal::default(),
                failed: AtomicBool::new(false),
                memory,
            }),
            incoming: Mutex::new(incoming),
            broker: None,
            session: None,
            parameters: None,
            blocked: Mutex::new(None),
        })
    }

    pub(in crate::replicated) const fn session(&self) -> Option<LinkSessionId> {
        self.session
    }
    pub(in crate::replicated) const fn parameters(&self) -> Option<Parameters> {
        self.parameters
    }
    pub(in crate::replicated) fn clock(&self) -> super::SdkClock {
        self.links.clock()
    }

    pub(in crate::replicated) fn reservation(&self) -> Reservation {
        Reservation {
            _lease: self.stream.memory.clone(),
        }
    }
    pub(in crate::replicated) fn writer_reservation(
        links: &BrokerLinks,
        config: &crate::replicated::WriterConfig,
    ) -> Option<crate::replicated::SharedWriterReservation> {
        crate::replicated::writer::reservation::Bounds::from(config).reservation(
            links
                .0
                .config
                .parameters
                .receive
                .envelope
                .max_metadata_bytes,
        )
    }
    pub(in crate::replicated) fn next_request(&self) -> Result<RequestId, BrokerLinkError> {
        self.links.next_request()
    }

    pub(in crate::replicated) fn request_bytes(
        links: &BrokerLinks,
        records: usize,
        retained_bytes: usize,
    ) -> Option<usize> {
        Self::request_bytes_with_metadata(
            links
                .0
                .config
                .parameters
                .receive
                .envelope
                .max_metadata_bytes,
            records,
            retained_bytes,
        )
    }

    pub(in crate::replicated) fn writer_bytes(reply_slots: usize, storage: usize) -> Option<usize> {
        reply_slots
            .checked_next_power_of_two()?
            .checked_mul(512)?
            .checked_add(8192)?
            .checked_add(storage)
    }

    pub(in crate::replicated) fn request_bytes_with_metadata(
        metadata: usize,
        records: usize,
        retained_bytes: usize,
    ) -> Option<usize> {
        records
            .checked_add(1)?
            .checked_mul(metadata.checked_add(512)?)?
            .checked_add(retained_bytes)?
            .checked_add(8192)
    }

    pub(in crate::replicated) fn invalidate_session(&mut self) {
        self.links.0.shared.appends.forget(self.stream.id, None);
        while self
            .incoming
            .lock()
            .expect("SDK APPEND receiver poisoned")
            .try_recv()
            .is_ok()
        {}
        self.stream.failed.store(false, Ordering::Release);
        self.session = None;
        self.parameters = None;
        *self.blocked.lock().expect("SDK APPEND send poisoned") = None;
    }

    pub(in crate::replicated) async fn refresh_session(
        &mut self,
        broker: NodeId,
    ) -> Result<(), BrokerLinkError> {
        self.invalidate_session();
        let peer = self
            .links
            .0
            .peers
            .get(&broker)
            .ok_or(BrokerLinkError::Configuration)?;
        self.broker = Some(broker);
        loop {
            let seen = self.links.0.shared.changed.generation();
            if self.links.0.shared.stop.is_closed() || peer.closed.is_closed() {
                return Err(BrokerLinkError::Closed);
            }
            if let Some(session) = self.links.session(broker)
                && let Some(parameters) = self.links.0.shared.sessions.remote_parameters(broker)
                && self.links.session(broker) == Some(session)
            {
                if parameters.capabilities & ozzy_proto::handshake::OWNER_STREAM == 0 {
                    return Err(BrokerLinkError::Configuration);
                }
                self.session = Some(session);
                self.parameters = Some(parameters);
                return Ok(());
            }
            self.links.0.shared.changed.changed_after(seen).await;
        }
    }

    fn live(&self) -> bool {
        !self.links.0.shared.stop.is_closed()
            && !self.stream.failed.load(Ordering::Acquire)
            && self.session.is_some()
            && self
                .broker
                .is_some_and(|broker| self.links.session(broker) == self.session)
    }

    /// Register before actual socket admission on the same SDK thread. A full
    /// socket rolls back the registration and returns the original frame intact.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::replicated) fn try_send(
        &self,
        message: Message,
        id: RequestId,
        end: u64,
        records: usize,
        retained_bytes: usize,
    ) -> Result<(), TrySendError> {
        if !self.live() {
            return Err(TrySendError::Closed);
        }
        let broker = self.broker.expect("selected broker");
        let replies = records.checked_add(1);
        let bytes = Self::request_bytes(&self.links, records, retained_bytes);
        let Some(bytes) = bytes.filter(|_| records != 0) else {
            return Err(TrySendError::Error(omq_tokio::Error::Config(
                "invalid shared APPEND reservation".into(),
            )));
        };
        let cost = Cost {
            requests: 1,
            records,
            bytes,
            ..Cost::default()
        };
        let registry = &self.links.0.shared.appends;
        if !registry.budget.possible(cost) {
            return Err(TrySendError::Error(omq_tokio::Error::Config(
                "shared APPEND exceeds aggregate capacity".into(),
            )));
        }
        let Some(lease) = registry.budget.acquire(cost) else {
            *self.blocked.lock().expect("SDK APPEND send poisoned") = Some(Blocked::Budget(cost));
            return Err(TrySendError::Full(message));
        };
        let tracked = track(&message, &lease, Some(&self.stream.memory), false);
        let mut requests = registry
            .requests
            .lock()
            .expect("SDK APPEND correlations poisoned");
        if requests.contains_key(&id) {
            return Err(TrySendError::Error(omq_tokio::Error::Config(
                "duplicate shared APPEND request ID".into(),
            )));
        }
        requests.insert(
            id,
            Request {
                stream: Arc::downgrade(&self.stream),
                writer: self.stream.id,
                broker,
                session: self.session.unwrap(),
                end,
                remaining_replies: replies.unwrap(),
                lease,
            },
        );
        drop(requests);
        match crate::transport::try_send_peer(&self.links.0.peers[&broker].data, tracked) {
            Ok(()) => {
                *self.blocked.lock().expect("SDK APPEND send poisoned") = None;
                Ok(())
            }
            Err(error) => {
                registry
                    .requests
                    .lock()
                    .expect("SDK APPEND correlations poisoned")
                    .remove(&id);
                match error {
                    TrySendError::Full(returned) => {
                        drop(returned);
                        *self.blocked.lock().expect("SDK APPEND send poisoned") =
                            Some(Blocked::Socket);
                        Err(TrySendError::Full(message))
                    }
                    error => Err(error),
                }
            }
        }
    }

    /// Only the writer's validated confirmed prefix retires correlations.
    pub(in crate::replicated) fn confirm(&self, end: u64) {
        self.links
            .0
            .shared
            .appends
            .forget(self.stream.id, Some(end));
    }

    /// Retry outstanding records without replacing the negotiated link session.
    pub(in crate::replicated) fn forget_requests(&self) {
        self.links.0.shared.appends.forget(self.stream.id, None);
        *self.blocked.lock().expect("SDK APPEND send poisoned") = None;
    }

    pub(in crate::replicated) fn try_recv_many_into(
        &self,
        maximum: usize,
        output: &mut Vec<Message>,
    ) -> Result<usize, omq_tokio::Error> {
        if !self.live() {
            return Err(omq_tokio::Error::Closed);
        }
        let mut incoming = self.incoming.lock().expect("SDK APPEND receiver poisoned");
        let mut count = 0;
        while count < maximum {
            match incoming.try_recv() {
                Ok(message) => {
                    output.push(message);
                    count += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(omq_tokio::Error::Closed);
                }
            }
        }
        Ok(count)
    }

    pub(in crate::replicated) async fn recv(&self) -> Result<Message, omq_tokio::Error> {
        loop {
            let links = self.links.0.shared.changed.generation();
            let budget = self.links.0.shared.appends.budget.changed.generation();
            if !self.live() {
                return Err(omq_tokio::Error::Closed);
            }
            let generation = self.stream.ready.generation();
            match self
                .incoming
                .lock()
                .expect("SDK APPEND receiver poisoned")
                .try_recv()
            {
                Ok(message) => return Ok(message),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(omq_tokio::Error::Closed);
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            tokio::select! {
                () = self.stream.ready.changed_after(generation) => {},
                () = self.links.0.shared.changed.changed_after(links) => {},
                () = self.links.0.shared.appends.budget.changed.changed_after(budget) => {},
                () = self.links.0.shared.stop.closed() => return Err(omq_tokio::Error::Closed),
            }
        }
    }

    pub(in crate::replicated) async fn wait_send_progress_for(&self, message: &Message) {
        let registry = &self.links.0.shared.appends;
        loop {
            let generation = registry.budget.changed.generation();
            let links = self.links.0.shared.changed.generation();
            if !self.live() {
                return;
            }
            let blocked = *self.blocked.lock().expect("SDK APPEND send poisoned");
            if let Some(Blocked::Budget(cost)) = blocked {
                if registry.budget.available(cost) {
                    return;
                }
                tokio::select! {
                    () = registry.budget.changed.changed_after(generation) => {},
                    () = self.links.0.shared.changed.changed_after(links) => {},
                    () = self.links.0.shared.stop.closed() => return,
                }
            } else {
                let broker = self.broker.expect("selected broker");
                tokio::select! {
                    () = crate::transport::wait_send_peer(&self.links.0.peers[&broker].data, message) => {},
                    () = self.links.0.shared.changed.changed_after(links) => {},
                    () = self.links.0.shared.stop.closed() => {},
                }
                return;
            }
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.links.0.shared.appends.forget(self.stream.id, None);
    }
}

struct Tracked {
    payload: Payload,
    _lease: Arc<Lease>,
    _memory: Option<Arc<Lease>>,
}

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

fn track(
    message: &Message,
    lease: &Arc<Lease>,
    memory: Option<&Arc<Lease>>,
    compact: bool,
) -> Message {
    Message::multipart_payloads((0..4).map(|index| {
        let payload = if compact {
            Payload::from_slice(message.part_slice(index).unwrap())
        } else {
            Payload::from_bytes(message.part_bytes(index).unwrap())
        };
        Payload::from_bytes(Bytes::from_owner(Tracked {
            payload,
            _lease: lease.clone(),
            _memory: memory.cloned(),
        }))
    }))
}

#[cfg(test)]
mod tests;
