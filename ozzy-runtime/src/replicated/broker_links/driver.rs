use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use omq_tokio::{
    IdentitySocket, Message, MonitorEvent, MonitorStream, MonitorTryRecvError, TrySendError,
};
use ozzy_proto::{
    Envelope, EnvelopeLimits, LinkSessionId, NodeId, Opcode, Packet, RequestId, decode_packet,
    directory, handshake, nack, producer, reader,
};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};

use super::{BrokerLinkError, BrokerLinksConfig, Peer, SdkClock, Shared};

const TURN: usize = 32;
const TURN_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(super) enum Body {
    Topic(directory::TopicRequest),
    Open(producer::Open),
    Subscribe(reader::Subscribe),
    Ack(reader::Ack),
    Unsubscribe(reader::Subscribed),
}

impl Body {
    fn opcodes(&self) -> (Opcode, Opcode) {
        match self {
            Self::Topic(_) => (Opcode::StateSnapshotRequest, Opcode::StateSnapshot),
            Self::Open(_) => (Opcode::OpenProducer, Opcode::ProducerOpened),
            Self::Subscribe(_) => (Opcode::Subscribe, Opcode::Subscribed),
            Self::Ack(_) => (Opcode::Ack, Opcode::Ack),
            Self::Unsubscribe(_) => (Opcode::Unsubscribe, Opcode::Unsubscribed),
        }
    }

    fn encode(
        &self,
        envelope: Envelope,
        metadata: &mut Vec<u8>,
        limits: EnvelopeLimits,
    ) -> Result<[u8; 64], BrokerLinkError> {
        Ok(match self {
            Self::Topic(request) => directory::encode_topic_request(
                envelope,
                request,
                metadata,
                limits,
                directory::Limits::default(),
            )?,
            Self::Open(open) => producer::encode_open(envelope, *open, metadata, limits)?,
            Self::Subscribe(subscribe) => {
                reader::encode_subscribe(envelope, subscribe, metadata, limits)?
            }
            Self::Ack(ack) => reader::encode_ack(envelope, *ack, metadata, limits)?,
            Self::Unsubscribe(subscribed) => {
                reader::encode_unsubscribe(envelope, *subscribed, metadata, limits)?
            }
        })
    }
}

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
    pub(super) reply: oneshot::Sender<Result<Message, BrokerLinkError>>,
}

#[derive(Debug)]
struct Waiting {
    command: Command,
    frame: Option<Message>,
}

#[derive(Debug)]
struct Active {
    command: Command,
    session: LinkSessionId,
}

pub(super) struct LinkState {
    socket: Arc<IdentitySocket>,
    input: mpsc::Receiver<Command>,
    peer: Arc<Peer>,
    shared: Arc<Shared>,
    remote: NodeId,
    limits: EnvelopeLimits,
    clock: SdkClock,
    retry: Duration,
    next_hello: Duration,
    handshake: Option<Message>,
    connection: Option<u64>,
    missing_route: bool,
    sending: Option<Waiting>,
    active: BTreeMap<RequestId, Active>,
    metadata: Vec<u8>,
    watches: super::routing::Watcher,
}

impl LinkState {
    pub(super) fn new(
        socket: Arc<IdentitySocket>,
        input: mpsc::Receiver<Command>,
        peer: Arc<Peer>,
        shared: Arc<Shared>,
        remote: NodeId,
        config: &BrokerLinksConfig,
    ) -> Self {
        Self {
            watches: super::routing::Watcher::new(shared.clone(), remote, config),
            socket,
            input,
            peer,
            shared,
            remote,
            limits: config.parameters.receive.envelope,
            clock: config.clock.clone(),
            retry: config.retry_interval,
            next_hello: Duration::ZERO,
            handshake: None,
            connection: None,
            missing_route: false,
            sending: None,
            active: BTreeMap::new(),
            metadata: Vec::with_capacity(config.parameters.receive.envelope.max_metadata_bytes),
        }
    }

    fn progress(&mut self, now: Duration) -> Result<bool, BrokerLinkError> {
        self.expire(now);
        let mut full = self.watches.progress(now)?;
        if self.shared.sessions.session(self.remote).is_none() && now >= self.next_hello {
            self.handshake = Some(Message::with_prefix(
                Bytes::copy_from_slice(self.remote.as_bytes()),
                Message::multipart(self.shared.sessions.start(self.remote)?),
            ));
            self.next_hello = now.saturating_add(self.retry);
        }
        self.flush_handshake()?;
        self.flush_watch()?;
        for index in 0..TURN {
            if self.sending.is_none() {
                match self.input.try_recv() {
                    Ok(command) => {
                        self.sending = Some(Waiting {
                            command,
                            frame: None,
                        });
                    }
                    Err(_) => break,
                }
            }
            if !self.send(now)? {
                break;
            }
            full |= index + 1 == TURN;
        }
        Ok(full)
    }

    fn deadline(&self) -> Option<Duration> {
        self.active
            .values()
            .map(|active| active.command.deadline)
            .chain(self.sending.iter().map(|waiting| waiting.command.deadline))
            .chain(
                self.shared
                    .sessions
                    .session(self.remote)
                    .is_none()
                    .then_some(self.next_hello),
            )
            .chain(self.watches.deadline())
            .min()
    }

    fn frame(&self) -> Option<&Message> {
        self.handshake
            .as_ref()
            .or_else(|| {
                self.sending
                    .as_ref()
                    .and_then(|waiting| waiting.frame.as_ref())
            })
            .or_else(|| self.watches.frame())
    }

    fn observe(&mut self, event: MonitorEvent) -> Result<(), BrokerLinkError> {
        match event {
            MonitorEvent::HandshakeSucceeded { peer, .. } => {
                if peer
                    .peer_identity
                    .as_deref()
                    .is_some_and(|peer| peer != self.remote.as_bytes())
                {
                    return Err(BrokerLinkError::Response);
                }
                if self.connection.is_some_and(|id| id != peer.connection_id) {
                    self.fence();
                }
                self.connection = Some(peer.connection_id);
                self.missing_route = false;
            }
            MonitorEvent::Disconnected { peer, .. }
                if self.connection == Some(peer.connection_id) =>
            {
                self.connection = None;
                self.fence();
            }
            _ => {}
        }
        Ok(())
    }

    fn fence(&mut self) {
        self.shared.sessions.disconnect(self.remote);
        self.shared.appends.fence(self.remote);
        self.watches.fence();
        self.handshake = None;
        self.next_hello = Duration::ZERO;
        self.missing_route = true;
        for (_, active) in std::mem::take(&mut self.active) {
            let _ = active.command.reply.send(Err(BrokerLinkError::Session));
        }
        if self
            .sending
            .as_ref()
            .is_some_and(|waiting| waiting.frame.is_some())
        {
            let waiting = self.sending.take().unwrap();
            let _ = waiting.command.reply.send(Err(BrokerLinkError::Session));
        }
        self.shared.changed.notify_changed();
    }

    fn expire(&mut self, now: Duration) {
        let expired: Vec<_> = self
            .active
            .iter()
            .filter(|(_, active)| {
                active.command.deadline <= now || active.command.reply.is_closed()
            })
            .map(|(&id, _)| id)
            .collect();
        for id in expired {
            let active = self.active.remove(&id).unwrap();
            let _ = active.command.reply.send(Err(BrokerLinkError::Timeout));
        }
        if self
            .sending
            .as_ref()
            .is_some_and(|waiting| waiting.command.deadline <= now)
        {
            let waiting = self.sending.take().unwrap();
            let _ = waiting.command.reply.send(Err(BrokerLinkError::Timeout));
        }
    }

    fn flush_handshake(&mut self) -> Result<(), BrokerLinkError> {
        let Some(frame) = &self.handshake else {
            return Ok(());
        };
        match crate::transport::try_send_peer(&self.socket, frame.clone()) {
            Ok(()) => {
                self.handshake = None;
                self.missing_route = false;
            }
            Err(TrySendError::Full(_)) => self.missing_route = false,
            Err(TrySendError::Error(omq_tokio::Error::Unroutable)) => self.missing_route = true,
            Err(TrySendError::Error(error)) => return Err(error.into()),
            Err(TrySendError::Closed) => return Err(BrokerLinkError::Closed),
        }
        Ok(())
    }

    fn flush_watch(&mut self) -> Result<(), BrokerLinkError> {
        let Some(frame) = self.watches.frame() else {
            return Ok(());
        };
        match crate::transport::try_send_peer(&self.socket, frame.clone()) {
            Ok(()) => {
                self.watches.sent();
                self.missing_route = false;
            }
            Err(TrySendError::Full(_)) => self.missing_route = false,
            Err(TrySendError::Error(omq_tokio::Error::Unroutable)) => self.missing_route = true,
            Err(TrySendError::Closed) => return Err(BrokerLinkError::Closed),
            Err(TrySendError::Error(error)) => return Err(error.into()),
        }
        Ok(())
    }

    fn send(&mut self, now: Duration) -> Result<bool, BrokerLinkError> {
        let waiting = self.sending.as_mut().expect("selected command");
        if waiting.command.reply.is_closed() || waiting.command.deadline <= now {
            self.sending = None;
            return Ok(true);
        }
        let Some(session) = self.shared.sessions.session(self.remote) else {
            return Ok(false);
        };
        if waiting.frame.is_none() {
            let limits = self.shared.sessions.send_limits(self.remote)?.envelope;
            let header = match waiting.command.body.encode(
                Envelope {
                    opcode: waiting.command.body.opcodes().0,
                    response: false,
                    request_id: Some(waiting.command.id),
                    sender: self.shared.local,
                    session: Some(session),
                },
                &mut self.metadata,
                limits,
            ) {
                Ok(header) => header,
                Err(error) => {
                    let waiting = self.sending.take().unwrap();
                    let _ = waiting.command.reply.send(Err(error));
                    return Ok(true);
                }
            };
            waiting.frame = Some(track(
                &crate::native_frames::message(
                    self.remote.as_bytes(),
                    header,
                    &self.metadata,
                    Bytes::new(),
                ),
                &waiting.command.lease,
                false,
            ));
        }
        match crate::transport::try_send_peer(&self.socket, waiting.frame.as_ref().unwrap().clone())
        {
            Ok(()) => {
                let waiting = self.sending.take().unwrap();
                self.missing_route = false;
                if let Body::Subscribe(subscribe) = &waiting.command.body {
                    self.shared
                        .readers
                        .select(self.remote, session, waiting.command.id, subscribe);
                }
                self.active.insert(
                    waiting.command.id,
                    Active {
                        command: waiting.command,
                        session,
                    },
                );
                Ok(true)
            }
            Err(TrySendError::Full(_)) => {
                self.missing_route = false;
                Ok(false)
            }
            Err(TrySendError::Error(omq_tokio::Error::Unroutable)) => {
                self.missing_route = true;
                Ok(false)
            }
            Err(TrySendError::Error(error)) => Err(error.into()),
            Err(TrySendError::Closed) => Err(BrokerLinkError::Closed),
        }
    }

    fn receive(&mut self, message: &Message) -> Result<(), BrokerLinkError> {
        let Ok(packet) = packet(message, self.remote, self.limits) else {
            return Ok(());
        };
        if matches!(packet.envelope.opcode, Opcode::Hello | Opcode::Welcome) {
            return self.negotiate(packet);
        }
        if self.watches.receive(packet, self.clock.now())? {
            return Ok(());
        }
        if self.shared.sessions.session(self.remote) == packet.envelope.session
            && self.shared.appends.receive(self.remote, message, packet)
        {
            return Ok(());
        }
        if self.shared.sessions.session(self.remote) == packet.envelope.session
            && packet.envelope.opcode == Opcode::Nack
            && packet
                .envelope
                .request_id
                .is_none_or(|id| !self.active.contains_key(&id))
            && self
                .shared
                .readers
                .receive(self.remote, message, packet, self.limits)
                .is_ok()
        {
            return Ok(());
        }
        let Some(id) = packet.envelope.request_id else {
            return Ok(());
        };
        let Some(active) = self.active.get(&id) else {
            return Ok(());
        };
        if !packet.envelope.response
            || packet.envelope.session != Some(active.session)
            || self.shared.sessions.session(self.remote) != Some(active.session)
            || !packet.payload.is_empty()
            || !matches!(packet.envelope.opcode, Opcode::Nack)
                && packet.envelope.opcode != active.command.body.opcodes().1
        {
            return Ok(());
        }
        let rejected = if packet.envelope.opcode == Opcode::Nack {
            let reply = nack::decode(packet, self.limits)?;
            let hint = if matches!(reply.code, 5 | 12 | 13)
                && self
                    .shared
                    .sessions
                    .remote_parameters(self.remote)
                    .is_some_and(|remote| remote.capabilities & handshake::OWNER_ROUTING != 0)
            {
                Some(
                    nack::AuthorityHint::decode(reply.detail)
                        .map_err(|_| BrokerLinkError::Response)?,
                )
            } else {
                None
            };
            Some(BrokerLinkError::Rejected {
                code: reply.code,
                retry: reply.retry,
                hint,
            })
        } else {
            None
        };
        let active = self.active.remove(&id).unwrap();
        let result = match rejected {
            Some(error) => Err(error),
            None => Ok(track(message, &active.command.lease, true)),
        };
        let _ = active.command.reply.send(result);
        Ok(())
    }

    fn negotiate(&mut self, packet: Packet<'_>) -> Result<(), BrokerLinkError> {
        let old = self.shared.sessions.session(self.remote);
        let handled = self.shared.sessions.receive(self.remote, packet)?;
        let current = self.shared.sessions.session(self.remote);
        if old.is_some() && old != current {
            self.shared.appends.fence(self.remote);
            self.watches.fence();
            for (_, active) in std::mem::take(&mut self.active) {
                let _ = active.command.reply.send(Err(BrokerLinkError::Session));
            }
            if self
                .sending
                .as_ref()
                .is_some_and(|waiting| waiting.frame.is_some())
            {
                let waiting = self.sending.take().unwrap();
                let _ = waiting.command.reply.send(Err(BrokerLinkError::Session));
            }
        }
        self.handshake = handled.reply.map(|reply| {
            Message::with_prefix(
                Bytes::copy_from_slice(self.remote.as_bytes()),
                Message::multipart(reply),
            )
        });
        if handled.replaced {
            self.shared.changed.notify_changed();
        }
        Ok(())
    }
}

enum Event {
    Control((Bytes, Message)),
    Data((omq_tokio::ReceiveReceipt, Message)),
    Monitor(MonitorEvent),
    Wake,
}

/// Exactly one control and one data socket per SDK owner, each connected to
/// every configured broker. Per-broker state never receives or closes a socket.
pub(super) struct Driver {
    control: Arc<IdentitySocket>,
    data: Arc<IdentitySocket>,
    monitor: MonitorStream,
    links: Vec<LinkState>,
    shared: Arc<Shared>,
    cursor: usize,
    paused: BTreeMap<NodeId, omq_tokio::ReceiveSource>,
}

impl Driver {
    pub(super) fn new(
        control: Arc<IdentitySocket>,
        data: Arc<IdentitySocket>,
        monitor: MonitorStream,
        links: Vec<LinkState>,
        shared: Arc<Shared>,
    ) -> Self {
        Self {
            control,
            data,
            monitor,
            links,
            shared,
            cursor: 0,
            paused: BTreeMap::new(),
        }
    }

    pub(super) async fn run(mut self) {
        let result = self.serve().await;
        for link in &mut self.links {
            link.fence();
            link.sending = None;
            link.peer.slots.close();
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
            .and_then(|id| <[u8; 16]>::try_from(id).ok())
            .map(NodeId::from_bytes);
        if let Some(link) = self
            .links
            .iter_mut()
            .find(|link| Some(link.remote) == node || link.connection == Some(peer.connection_id))
        {
            link.observe(event)?;
        }
        Ok(())
    }

    fn receive_control(&mut self, identity: Bytes, body: Message) -> Result<(), BrokerLinkError> {
        let Some(remote) = <[u8; 16]>::try_from(identity.as_ref())
            .ok()
            .map(NodeId::from_bytes)
        else {
            return Ok(());
        };
        if let Some(link) = self.links.iter_mut().find(|link| link.remote == remote) {
            link.receive(&Message::with_prefix(identity, body))?;
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
        if self.shared.sessions.session(remote).is_none()
            || self.shared.sessions.session(remote) != packet.envelope.session
            || packet.envelope.opcode != Opcode::Records
        {
            return Ok(());
        }
        if self
            .shared
            .readers
            .receive(remote, &message, packet, self.links[0].limits)
            .is_err()
            && let Some(source) = receipt.source().cloned()
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

    #[expect(
        clippy::too_many_lines,
        reason = "one bounded control and data socket owner turn"
    )]
    async fn serve(&mut self) -> Result<(), BrokerLinkError> {
        loop {
            if self.shared.stop.is_closed() {
                return Ok(());
            }
            let generation = self.shared.changed.generation();
            let readers = self.shared.readers.changed.generation();
            let mut full = false;
            // Lifecycle events fence root sessions before their queued input.
            for index in 0..TURN {
                match self.monitor.try_recv() {
                    Ok(event) => self.observe(event)?,
                    Err(MonitorTryRecvError::Empty) => break,
                    Err(_) => {
                        return Err(BrokerLinkError::Failed(
                            "SDK transport monitor lost events".into(),
                        ));
                    }
                }
                full |= index + 1 == TURN;
            }
            let mut control_bytes = 0usize;
            for index in 0..TURN {
                match self.control.try_recv_from() {
                    Ok((identity, body)) => {
                        control_bytes += body.byte_len();
                        self.receive_control(identity, body)?;
                    }
                    Err(omq_tokio::Error::WouldBlock) => break,
                    Err(error) => return Err(error.into()),
                }
                full |= index + 1 == TURN || control_bytes >= TURN_BYTES;
                if control_bytes >= TURN_BYTES {
                    break;
                }
            }
            // Retry exact paused sources once per bounded turn. OMQ keeps other
            // broker connections runnable while retained reader backing is full.
            for (remote, source) in std::mem::take(&mut self.paused) {
                match self.data.try_recv_from_source(Some(&source)) {
                    Ok((receipt, body)) => self.receive_data(receipt, body)?,
                    Err(omq_tokio::Error::WouldBlock) => {
                        self.paused.insert(remote, source);
                    }
                    Err(omq_tokio::Error::Closed) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            let mut data_bytes = 0usize;
            for index in 0..TURN {
                match self.data.try_recv_from_source(None) {
                    Ok((receipt, body)) => {
                        data_bytes += body.byte_len();
                        self.receive_data(receipt, body)?;
                    }
                    Err(omq_tokio::Error::WouldBlock) => break,
                    Err(error) => return Err(error.into()),
                }
                full |= index + 1 == TURN || data_bytes >= TURN_BYTES;
                if data_bytes >= TURN_BYTES {
                    break;
                }
            }
            let now = self.links[0].clock.now();
            for _ in 0..self.links.len() {
                let index = self.cursor;
                self.cursor = (index + 1) % self.links.len();
                full |= self.links[index].progress(now)?;
            }
            if full {
                tokio::task::yield_now().await;
                continue;
            }
            let deadline = self.links.iter().filter_map(LinkState::deadline).min();
            let event = {
                let waits = futures::stream::FuturesUnordered::new();
                for link in &self.links {
                    let socket = self.control.clone();
                    let frame = link.frame().filter(|_| !link.missing_route).cloned();
                    waits.push(async move {
                        if let Some(frame) = frame {
                            socket.wait_send_progress_for(&frame).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    });
                }
                let mut waits = std::pin::pin!(waits);
                let capacity = futures::stream::FuturesUnordered::new();
                for link in &self.links {
                    capacity.push(link.watches.capacity_ready());
                }
                let mut capacity = std::pin::pin!(capacity);
                tokio::select! {
                    () = self.shared.stop.closed() => return Ok(()),
                    () = self.shared.changed.changed_after(generation) => Event::Wake,
                    () = self.shared.readers.changed.changed_after(readers) => Event::Wake,
                    _ = waits.next() => Event::Wake,
                    _ = capacity.next() => Event::Wake,
                    received = self.control.recv_from() => Event::Control(received?),
                    received = self.data.recv_from_source(None) => Event::Data(received?),
                    event = self.monitor.recv() => Event::Monitor(event.map_err(|_| BrokerLinkError::Failed("SDK transport monitor lost events".into()))?),
                    () = async { if let Some(deadline) = deadline { self.links[0].clock.until(deadline).await } else { std::future::pending().await } } => Event::Wake,
                }
            };
            match event {
                Event::Control((identity, body)) => self.receive_control(identity, body)?,
                Event::Data((receipt, body)) => self.receive_data(receipt, body)?,
                Event::Monitor(event) => self.observe(event)?,
                Event::Wake => {}
            }
        }
    }
}

pub(super) fn packet(
    message: &Message,
    remote: NodeId,
    limits: EnvelopeLimits,
) -> Result<Packet<'_>, BrokerLinkError> {
    if message.len() != 4 || message.part_slice(0) != Some(remote.as_bytes().as_slice()) {
        return Err(BrokerLinkError::Response);
    }
    let frames = std::array::from_fn::<_, 3, _>(|index| message.part_slice(index + 1).unwrap());
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
    Message::multipart((0..4).map(|index| {
        if index == 3 {
            return Bytes::new();
        }
        let bytes = if compact {
            Bytes::copy_from_slice(message.part_slice(index).unwrap())
        } else {
            message.part_bytes(index).unwrap()
        };
        Bytes::from_owner(Tracked {
            bytes,
            _lease: lease.clone(),
        })
    }))
}

#[cfg(test)]
mod tests;
