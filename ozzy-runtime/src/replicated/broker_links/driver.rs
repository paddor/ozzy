use crate::transport::Frames;
use std::{collections::BTreeMap, rc::Rc, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use omq_tokio::{
    Identity, IdentitySocket, Message, MonitorEvent, MonitorStream, MonitorTryRecvError,
};
use ozzy_proto::{EnvelopeLimits, NodeId, Opcode, Packet, RequestId, decode_packet};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};

use super::{BrokerLinkError, LinkSessions, Shared, readers, routing};

mod body;
mod link;

pub(super) use body::{Body, Reply};
pub(super) use link::LinkState;

pub(super) const TURN: usize = 32;
const TURN_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug)]
pub(super) struct Lease {
    pub(super) _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(super) struct Command {
    pub(super) body: Body,
    pub(super) id: RequestId,
    pub(super) deadline: Duration,
    pub(super) lease: Arc<Lease>,
    pub(super) reply: oneshot::Sender<Result<Reply, BrokerLinkError>>,
}

/// Owner state a caller on another thread asks the driver to change.
#[derive(Debug)]
pub(super) enum Registration {
    /// Cache a topic's routing state, and with `readers` first install its
    /// reader endpoints.
    Topic {
        topic: crate::topic_metadata::TopicMetadata,
        readers: bool,
        reply: oneshot::Sender<Result<Arc<routing::State>, BrokerLinkError>>,
    },
    /// Fan a publication out as if a SUB driver had received it.
    #[cfg(test)]
    Publication {
        broker: NodeId,
        message: Message,
        reply: oneshot::Sender<()>,
    },
}

enum Event {
    Registration(Registration),
    Inbox(readers::Registration),
    Control((Identity, Message)),
    Data((omq_tokio::ReceiveReceipt, Message)),
    Monitor(MonitorEvent),
    Wake,
}

/// The SDK owner's inputs: control and data sockets, each connected to
/// every configured broker, the control socket's lifecycle monitor, and the
/// topics and reader inboxes callers register.
pub(super) struct Inputs {
    pub(super) control: Arc<IdentitySocket>,
    pub(super) data: Arc<IdentitySocket>,
    pub(super) monitor: MonitorStream,
    pub(super) registrations: mpsc::Receiver<Registration>,
    pub(super) inboxes: mpsc::Receiver<readers::Registration>,
}

/// State only the SDK thread touches. The driver lends it to each broker's
/// link and shares the reader registry with its SUB drivers.
pub(super) struct Local {
    pub(super) sessions: LinkSessions,
    pub(super) routing: routing::Registry,
    pub(super) readers: Rc<readers::Registry>,
}

/// Exactly one control and one data socket per SDK owner, each connected to
/// every configured broker. Per-broker state never receives or closes a socket.
pub(super) struct Driver {
    control: Arc<IdentitySocket>,
    data: Arc<IdentitySocket>,
    monitor: MonitorStream,
    registrations: mpsc::Receiver<Registration>,
    inboxes: mpsc::Receiver<readers::Registration>,
    links: Vec<LinkState>,
    local: Local,
    shared: Arc<Shared>,
    cursor: usize,
    paused: BTreeMap<NodeId, omq_tokio::ReceiveLane>,
}

impl Driver {
    pub(super) fn new(
        Inputs {
            control,
            data,
            monitor,
            registrations,
            inboxes,
        }: Inputs,
        links: Vec<LinkState>,
        local: Local,
        shared: Arc<Shared>,
    ) -> Self {
        Self {
            control,
            data,
            monitor,
            registrations,
            inboxes,
            links,
            local,
            shared,
            cursor: 0,
            paused: BTreeMap::new(),
        }
    }

    pub(super) async fn run(mut self) {
        let result = self.serve().await;
        for link in &mut self.links {
            link.close(&mut self.local.sessions);
        }
        let control = self.control.as_ref().clone().into_inner().close().await;
        let data = self.data.as_ref().clone().into_inner().close().await;
        if let Err(error) = result
            .and(control.map_err(BrokerLinkError::from))
            .and(data.map_err(BrokerLinkError::from))
        {
            let _ = self.shared.failure.set(error.to_string());
        }
        for link in &self.links {
            link.peer.closed.close();
        }
        self.shared.changed.notify_changed();
    }

    fn observe(&mut self, event: MonitorEvent) -> Result<(), BrokerLinkError> {
        let (MonitorEvent::HandshakeSucceeded { peer, .. }
        | MonitorEvent::Disconnected { peer, .. }) = &event
        else {
            return Ok(());
        };
        let node = peer
            .peer_identity
            .as_deref()
            .and_then(crate::transport::decode_peer_identity);
        if let Some(link) = self
            .links
            .iter_mut()
            .find(|link| Some(link.remote) == node || link.connection == Some(peer.connection_id))
        {
            link.observe(&mut self.local.sessions, event)?;
        }
        Ok(())
    }

    fn register(&mut self, registration: Registration) {
        match registration {
            Registration::Topic {
                topic,
                readers,
                reply,
            } => {
                let installed = if readers {
                    self.local.readers.topic(&topic)
                } else {
                    Ok(())
                };
                let _ = reply.send(installed.and_then(|()| self.local.routing.topic(topic)));
            }
            #[cfg(test)]
            Registration::Publication {
                broker,
                message,
                reply,
            } => {
                self.local
                    .readers
                    .publication(broker, &message, self.links[0].limits);
                let _ = reply.send(());
            }
        }
    }

    /// Fan publications out to a new inbox and initialize its lanes.
    fn install(&mut self, registration: readers::Registration) {
        let inbox = self.local.readers.register(registration);
        tokio::task::spawn_local(readers::initialize(inbox, self.shared.clone()));
    }

    fn receive_control(
        &mut self,
        identity: &Identity,
        body: Message,
    ) -> Result<(), BrokerLinkError> {
        let Some(identity) = crate::transport::native_peer_identity(identity.to_bytes()) else {
            return Ok(());
        };
        let Some(remote) = <[u8; 16]>::try_from(identity.as_ref())
            .ok()
            .map(NodeId::from_bytes)
        else {
            return Ok(());
        };
        if let Some(link) = self.links.iter_mut().find(|link| link.remote == remote) {
            link.receive(&mut self.local, &Message::with_prefix(identity, body))?;
        }
        Ok(())
    }

    fn receive_data(
        &mut self,
        receipt: omq_tokio::ReceiveReceipt,
        body: Message,
    ) -> Result<(), BrokerLinkError> {
        let Some(identity) = receipt.identity_bytes() else {
            return Ok(());
        };
        let Some(identity) = crate::transport::native_peer_identity(identity) else {
            return Ok(());
        };
        let Some(remote) = <[u8; 16]>::try_from(identity.as_ref())
            .ok()
            .map(NodeId::from_bytes)
        else {
            return Ok(());
        };
        let message = Message::with_prefix(identity, body.clone());
        let Ok(packet) = packet(&message, remote, self.links[0].limits) else {
            return Ok(());
        };
        let session = self
            .links
            .iter()
            .find(|link| link.remote == remote)
            .and_then(LinkState::session);
        if session.is_none()
            || session != packet.envelope.session
            || packet.envelope.opcode != Opcode::Records
        {
            return Ok(());
        }
        if self
            .local
            .readers
            .receive(remote, &message, packet, self.links[0].limits)
            .is_err()
            && let Some(source) = receipt.lane().cloned()
        {
            match self.data.unshift(receipt, body) {
                Ok(()) => {
                    self.paused.insert(remote, source);
                }
                Err(error) if matches!(error.error, omq_tokio::Error::Closed) => {}
                Err(error) => return Err(error.error.into()),
            }
        }
        Ok(())
    }

    async fn serve(&mut self) -> Result<(), BrokerLinkError> {
        loop {
            if self.shared.stop.is_closed() {
                return Ok(());
            }
            let interests = self.shared.interests.generation();
            let readers = self.shared.readers.changed.generation();
            if self.turn()? {
                tokio::task::yield_now().await;
                continue;
            }
            let Some(event) = self.wait(interests, readers).await? else {
                return Ok(());
            };
            match event {
                Event::Registration(registration) => self.register(registration),
                Event::Inbox(registration) => self.install(registration),
                Event::Control((identity, body)) => self.receive_control(&identity, body)?,
                Event::Data((receipt, body)) => self.receive_data(receipt, body)?,
                Event::Monitor(event) => self.observe(event)?,
                Event::Wake => {}
            }
        }
    }

    /// One bounded turn over registrations, inboxes, monitor, control, and
    /// data input, then every link's progress. `true` when a bound was
    /// reached and more may be ready.
    fn turn(&mut self) -> Result<bool, BrokerLinkError> {
        let mut full = drained(|| match self.registrations.try_recv() {
            Ok(registration) => {
                self.register(registration);
                Ok(Some(0))
            }
            Err(_) => Ok(None),
        })?;
        full |= drained(|| match self.inboxes.try_recv() {
            Ok(registration) => {
                self.install(registration);
                Ok(Some(0))
            }
            Err(_) => Ok(None),
        })?;
        // Lifecycle events fence root sessions before their queued input.
        full |= drained(|| match self.monitor.try_recv() {
            Ok(event) => self.observe(event).map(|()| Some(0)),
            Err(MonitorTryRecvError::Empty) => Ok(None),
            Err(_) => Err(BrokerLinkError::Failed(
                "SDK transport monitor lost events".into(),
            )),
        })?;
        full |= drained(|| match self.control.try_recv_from() {
            Ok((identity, body)) => {
                let bytes = body.byte_len();
                self.receive_control(&identity, body).map(|()| Some(bytes))
            }
            Err(omq_tokio::Error::WouldBlock) => Ok(None),
            Err(error) => Err(error.into()),
        })?;
        self.retry_paused()?;
        full |= drained(|| match self.data.try_recv_with_receipt(None) {
            Ok((receipt, body)) => {
                let bytes = body.byte_len();
                self.receive_data(receipt, body).map(|()| Some(bytes))
            }
            Err(omq_tokio::Error::WouldBlock) => Ok(None),
            Err(error) => Err(error.into()),
        })?;
        let now = self.links[0].clock.now();
        for _ in 0..self.links.len() {
            let index = self.cursor;
            self.cursor = (index + 1) % self.links.len();
            full |= self.links[index].progress(&mut self.local, now)?;
        }
        Ok(full)
    }

    /// Retry exact paused sources once per bounded turn. OMQ keeps other
    /// broker connections runnable while retained reader backing is full.
    fn retry_paused(&mut self) -> Result<(), BrokerLinkError> {
        for (remote, source) in std::mem::take(&mut self.paused) {
            match self.data.try_recv_with_receipt(Some(&source)) {
                Ok((receipt, body)) => self.receive_data(receipt, body)?,
                Err(omq_tokio::Error::WouldBlock) => {
                    self.paused.insert(remote, source);
                }
                Err(omq_tokio::Error::Closed) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// Wait for a registration, a new inbox, input, send space, reader
    /// capacity, a routing interest, a link's commands or watch capacity, or
    /// the next link deadline. `None` once the owner stops.
    async fn wait(
        &mut self,
        interests: u64,
        readers: u64,
    ) -> Result<Option<Event>, BrokerLinkError> {
        let deadline = self.links.iter().filter_map(LinkState::deadline).min();
        let waits = futures::stream::FuturesUnordered::new();
        for link in &self.links {
            let socket = self.control.clone();
            let frame = link.frame().filter(|_| !link.missing_route).cloned();
            waits.push(async move {
                if let Some(frame) = frame {
                    crate::transport::wait_send_peer(&socket, &frame).await;
                } else {
                    std::future::pending::<()>().await;
                }
            });
        }
        let mut waits = std::pin::pin!(waits);
        let ready: futures::stream::FuturesUnordered<_> =
            self.links.iter().map(LinkState::ready).collect();
        let mut ready = std::pin::pin!(ready);
        let clock = self.links[0].clock.clone();
        let due = clock.until_due(deadline);
        Ok(Some(tokio::select! {
            () = self.shared.stop.closed() => return Ok(None),
            registration = self.registrations.recv() => match registration {
                Some(registration) => Event::Registration(registration),
                None => return Ok(None),
            },
            registration = self.inboxes.recv() => match registration {
                Some(registration) => Event::Inbox(registration),
                None => return Ok(None),
            },
            () = self.shared.interests.changed_after(interests) => Event::Wake,
            () = self.shared.readers.changed.changed_after(readers) => Event::Wake,
            _ = waits.next() => Event::Wake,
            _ = ready.next() => Event::Wake,
            received = self.control.recv_from() => Event::Control(received?),
            received = self.data.recv_with_receipt(None) => Event::Data(received?),
            event = self.monitor.recv() => Event::Monitor(event.map_err(|_| {
                BrokerLinkError::Failed("SDK transport monitor lost events".into())
            })?),
            () = due => Event::Wake,
        }))
    }
}

/// Receive up to `TURN` messages or `TURN_BYTES`. `receive` returns the
/// bytes of one message, or `None` once nothing is ready. `true` when a
/// bound was reached.
fn drained(
    mut receive: impl FnMut() -> Result<Option<usize>, BrokerLinkError>,
) -> Result<bool, BrokerLinkError> {
    let mut bytes = 0_usize;
    for index in 0..TURN {
        let Some(received) = receive()? else {
            return Ok(false);
        };
        bytes += received;
        if index + 1 == TURN || bytes >= TURN_BYTES {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn packet(
    message: &Message,
    remote: NodeId,
    limits: EnvelopeLimits,
) -> Result<Packet<'_>, BrokerLinkError> {
    let Some((_, frames)) = message
        .routed_packet()
        .filter(|(route, _)| *route == remote.as_bytes())
    else {
        return Err(BrokerLinkError::Response);
    };
    let packet = decode_packet(&frames, limits)?;
    if packet.envelope.sender != remote {
        return Err(BrokerLinkError::Response);
    }
    Ok(packet)
}

struct Tracked {
    bytes: Bytes,
    _lease: Arc<Lease>,
}

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(super) fn track(message: &Message, lease: &Arc<Lease>, compact: bool) -> Message {
    let tracked = |bytes| {
        Bytes::from_owner(Tracked {
            bytes,
            _lease: lease.clone(),
        })
    };
    // The routing, header and metadata frames; the payload frame is dropped.
    let payload = std::iter::once(Bytes::new());
    if compact {
        let frames = message.slices().take(3).map(Bytes::copy_from_slice);
        Message::multipart(frames.map(tracked).chain(payload))
    } else {
        Message::multipart(message.iter().take(3).map(tracked).chain(payload))
    }
}

#[cfg(test)]
use body::{progress_rejection, reader_rejection};
#[cfg(test)]
mod tests;
