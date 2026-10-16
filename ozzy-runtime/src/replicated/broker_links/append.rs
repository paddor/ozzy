//! Logical APPEND reply lanes on the existing physical broker sockets.

use crate::transport::Frames;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use omq_tokio::{Message, TrySendError, message::Payload};
use ozzy_proto::{LinkSessionId, NodeId, RequestId, handshake::Parameters};

use super::{BrokerLinkError, BrokerLinks, StateSignal};
use tokio::sync::mpsc;

mod budget;
mod registry;
use budget::{Cost, Lease};
pub(super) use registry::{Owner, Registry};
use registry::{Request, Stream};

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

/// Keeps declared SDK allocation capacity reserved through owner and alias
/// destruction. It never keeps the socket manager or SDK runtime alive.
#[derive(Clone, Debug)]
pub(in crate::replicated) struct Reservation {
    _lease: Arc<Lease>,
}

/// One APPEND request leaving a writer: its correlation, the end of its
/// sequence range, and the records and backing bytes it reserves.
#[derive(Clone, Copy, Debug)]
pub(in crate::replicated) struct Outgoing {
    pub(in crate::replicated) id: RequestId,
    pub(in crate::replicated) end: u64,
    pub(in crate::replicated) records: usize,
    pub(in crate::replicated) retained_bytes: usize,
}

/// A send that can never succeed with this configuration.
fn configuration(reason: &'static str) -> TrySendError {
    TrySendError::Error(omq_tokio::Error::Config(reason.into()))
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
    registry: Owner,
    stream: Arc<Stream>,
    incoming: mpsc::Receiver<Message>,
    broker: Option<NodeId>,
    session: Option<LinkSessionId>,
    parameters: Option<Parameters>,
    blocked: Option<Blocked>,
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
            registry: links.0.shared.appends.owner(),
            links,
            stream: Arc::new(Stream {
                id,
                sender,
                ready: StateSignal::default(),
                failed: AtomicBool::new(false),
                memory,
            }),
            incoming,
            broker: None,
            session: None,
            parameters: None,
            blocked: None,
        })
    }

    pub(in crate::replicated) const fn session(&self) -> Option<LinkSessionId> {
        self.session
    }
    pub(in crate::replicated) const fn parameters(&self) -> Option<Parameters> {
        self.parameters
    }
    pub(in crate::replicated) fn clock(&self) -> super::OwnerClock {
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
        self.registry.forget(self.stream.id, None);
        while self.incoming.try_recv().is_ok() {}
        self.stream.failed.store(false, Ordering::Release);
        self.session = None;
        self.parameters = None;
        self.blocked = None;
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
            if let Some(link) = peer.link() {
                if link.remote.capabilities & ozzy_proto::handshake::OWNER_STREAM == 0 {
                    return Err(BrokerLinkError::Configuration);
                }
                self.session = Some(link.session);
                self.parameters = Some(link.remote);
                return Ok(());
            }
            // Wait for closure itself too, not only the driver's teardown change.
            tokio::select! {
                () = self.links.0.shared.changed.changed_after(seen) => {}
                () = self.links.0.shared.stop.closed() => {}
                () = peer.closed.closed() => {}
            }
        }
    }

    fn live(&self) -> bool {
        self.bound().is_some()
    }

    /// Selected broker and session while both are current and the stream is
    /// healthy.
    fn bound(&self) -> Option<(NodeId, LinkSessionId)> {
        let (broker, session) = (self.broker?, self.session?);
        (!self.links.0.shared.stop.is_closed()
            && !self.stream.failed.load(Ordering::Acquire)
            && self.links.session(broker) == Some(session))
        .then_some((broker, session))
    }

    /// Register before actual socket admission on the same SDK thread. A full
    /// socket rolls back the registration and returns the original frame intact.
    pub(in crate::replicated) fn try_send(
        &mut self,
        message: Message,
        outgoing: Outgoing,
    ) -> Result<(), TrySendError> {
        let Some((broker, session)) = self.bound() else {
            return Err(TrySendError::Closed);
        };
        let (cost, replies) = self.cost(outgoing)?;
        let registry = &self.registry;
        let Some(lease) = registry.budget.acquire(cost) else {
            self.blocked = Some(Blocked::Budget(cost));
            return Err(TrySendError::Full(message));
        };
        let tracked = track(&message, &lease, Some(&self.stream.memory), false);
        let id = outgoing.id;
        let mut requests = registry.requests.borrow_mut();
        if requests.contains_key(&id) {
            return Err(configuration("duplicate shared APPEND request ID"));
        }
        requests.insert(
            id,
            Request {
                stream: Arc::downgrade(&self.stream),
                writer: self.stream.id,
                broker,
                session,
                end: outgoing.end,
                remaining_replies: replies,
                lease,
            },
        );
        drop(requests);
        match crate::transport::try_send_peer(&self.links.0.peers[&broker].data, tracked) {
            Ok(()) => {
                self.blocked = None;
                Ok(())
            }
            Err(error) => {
                registry.requests.borrow_mut().remove(&id);
                match error {
                    TrySendError::Full(returned) => {
                        drop(returned);
                        self.blocked = Some(Blocked::Socket);
                        Err(TrySendError::Full(message))
                    }
                    error => Err(error),
                }
            }
        }
    }

    /// Budget cost and expected reply count of `outgoing`. A reservation
    /// that is invalid or exceeds aggregate capacity can never be admitted.
    fn cost(&self, outgoing: Outgoing) -> Result<(Cost, usize), TrySendError> {
        let records = outgoing.records;
        let reservation = Self::request_bytes(&self.links, records, outgoing.retained_bytes)
            .filter(|_| records != 0)
            .zip(records.checked_add(1));
        let Some((bytes, replies)) = reservation else {
            return Err(configuration("invalid shared APPEND reservation"));
        };
        let cost = Cost {
            requests: 1,
            records,
            bytes,
            ..Cost::default()
        };
        if !self.registry.budget.possible(cost) {
            return Err(configuration("shared APPEND exceeds aggregate capacity"));
        }
        Ok((cost, replies))
    }

    /// Only the writer's validated confirmed prefix retires correlations.
    pub(in crate::replicated) fn confirm(&self, end: u64) {
        self.registry.forget(self.stream.id, Some(end));
    }

    /// Retry outstanding records without replacing the negotiated link session.
    pub(in crate::replicated) fn forget_requests(&mut self) {
        self.registry.forget(self.stream.id, None);
        self.blocked = None;
    }

    pub(in crate::replicated) fn try_recv_many_into(
        &mut self,
        maximum: usize,
        output: &mut Vec<Message>,
    ) -> Result<usize, omq_tokio::Error> {
        if !self.live() {
            return Err(omq_tokio::Error::Closed);
        }
        let mut count = 0;
        while count < maximum {
            match self.incoming.try_recv() {
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

    /// Wait without consuming. Only the writer owner drains the reply lane.
    pub(in crate::replicated) async fn recv_ready(&self) -> Result<(), omq_tokio::Error> {
        loop {
            let links = self.links.0.shared.changed.generation();
            let budget = self.links.0.shared.appends.budget.changed.generation();
            if !self.live() {
                return Err(omq_tokio::Error::Closed);
            }
            let generation = self.stream.ready.generation();
            if !self.incoming.is_empty() {
                return Ok(());
            }
            if self.incoming.is_closed() {
                return Err(omq_tokio::Error::Closed);
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
            let Some((broker, _)) = self.bound() else {
                return;
            };
            let blocked = self.blocked;
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
        self.registry.forget(self.stream.id, None);
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
    let tracked = |payload| {
        Payload::from_bytes(Bytes::from_owner(Tracked {
            payload,
            _lease: lease.clone(),
            _memory: memory.cloned(),
        }))
    };
    if compact {
        Message::multipart_payloads(message.slices().map(Payload::from_slice).map(tracked))
    } else {
        Message::multipart_payloads(message.iter().map(Payload::from_bytes).map(tracked))
    }
}

#[cfg(test)]
mod tests;
