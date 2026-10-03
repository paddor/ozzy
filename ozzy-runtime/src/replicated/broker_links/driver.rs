use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
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

#[derive(Debug, Clone)]
pub(super) enum Body {
    Topic(directory::TopicRequest),
    Open(producer::Open),
    Subscribe(reader::Subscribe),
    Credit(reader::Credit),
    Ack(reader::Ack),
    Unsubscribe(reader::Subscribed),
}

impl Body {
    fn opcodes(&self) -> (Opcode, Opcode) {
        match self {
            Self::Topic(_) => (Opcode::StateSnapshotRequest, Opcode::StateSnapshot),
            Self::Open(_) => (Opcode::OpenProducer, Opcode::ProducerOpened),
            Self::Subscribe(_) => (Opcode::Subscribe, Opcode::Subscribed),
            Self::Credit(_) => (Opcode::Credit, Opcode::Credit),
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
            Self::Credit(credit) => reader::encode_credit(envelope, *credit, metadata, limits)?,
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

enum Event {
    Input(Option<Command>),
    Message(Result<(Bytes, Message), omq_tokio::Error>),
    Monitor(Result<MonitorEvent, omq_tokio::MonitorRecvError>),
    Wake,
}

pub(super) struct Driver {
    socket: Arc<IdentitySocket>,
    monitor: MonitorStream,
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

impl Driver {
    pub(super) fn new(
        socket: Arc<IdentitySocket>,
        monitor: MonitorStream,
        input: mpsc::Receiver<Command>,
        peer: Arc<Peer>,
        shared: Arc<Shared>,
        remote: NodeId,
        config: &BrokerLinksConfig,
    ) -> Self {
        Self {
            watches: super::routing::Watcher::new(shared.clone(), remote, config),
            socket,
            monitor,
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

    pub(super) async fn run(mut self) {
        let result = self.serve().await;
        self.fence();
        self.sending = None;
        self.peer.slots.close();
        // Drop the coordinated receiver before close waits, reclaiming queued
        // observers even while application handles retain live senders.
        drop(self.input);
        let result = result.and(
            self.socket
                .as_ref()
                .clone()
                .into_inner()
                .close()
                .await
                .map_err(BrokerLinkError::from),
        );
        if let Err(error) = result {
            let _ = self.shared.failure.set(error.to_string());
        }
        self.peer.closed.close();
        self.shared.changed.notify_changed();
    }

    async fn serve(&mut self) -> Result<(), BrokerLinkError> {
        loop {
            tokio::task::consume_budget().await;
            if self.shared.stop.is_closed() {
                return Ok(());
            }
            let now = self.clock.now();
            let generation = self.shared.changed.generation();
            let mut full_turn = self.receive_ready()?;
            self.expire(now);
            full_turn |= self.watches.progress(now)?;
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
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()),
                    }
                }
                if !self.send(now)? {
                    break;
                }
                full_turn |= index + 1 == TURN;
            }
            if full_turn {
                tokio::task::yield_now().await;
                continue;
            }
            let deadline = self
                .active
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
                .min();
            // An absent route is not socket-capacity pressure. Its capacity
            // future may already be ready, starving every other SDK link.
            // Await connection events or the next HELLO retry instead.
            let frame = self
                .handshake
                .as_ref()
                .or_else(|| {
                    self.sending
                        .as_ref()
                        .and_then(|waiting| waiting.frame.as_ref())
                })
                .or_else(|| self.watches.frame());
            let event = tokio::select! {
                () = self.shared.stop.closed() => return Ok(()),
                command = self.input.recv(), if self.sending.is_none() => Event::Input(command),
                () = self.shared.changed.changed_after(generation) => Event::Wake,
                () = self.watches.capacity_ready() => Event::Wake,
                message = self.socket.recv_from() => Event::Message(message),
                event = self.monitor.recv() => Event::Monitor(event),
                () = async { match frame {
                    Some(frame) if !self.missing_route => self.socket.wait_send_progress_for(frame).await,
                    _ => std::future::pending().await,
                }} => Event::Wake,
                () = async { match deadline {
                    Some(deadline) => self.clock.until(deadline).await,
                    None => std::future::pending().await,
                }} => Event::Wake,
            };
            match event {
                Event::Input(Some(command)) => {
                    self.sending = Some(Waiting {
                        command,
                        frame: None,
                    });
                }
                Event::Input(None) => return Ok(()),
                Event::Wake => {}
                Event::Message(message) => {
                    let (identity, body) = message?;
                    self.receive(&Message::with_prefix(identity, body))?;
                }
                Event::Monitor(event) => self.observe(event.map_err(|_| {
                    BrokerLinkError::Failed("SDK transport monitor lost events".to_owned())
                })?)?,
            }
        }
    }

    fn receive_ready(&mut self) -> Result<bool, BrokerLinkError> {
        let mut full_turn = false;
        for index in 0..TURN {
            match self.monitor.try_recv() {
                Ok(event) => self.observe(event)?,
                Err(MonitorTryRecvError::Empty) => break,
                Err(_) => {
                    return Err(BrokerLinkError::Failed(
                        "SDK transport monitor lost events".to_owned(),
                    ));
                }
            }
            full_turn |= index + 1 == TURN;
        }
        for index in 0..TURN {
            match self.socket.try_recv_from() {
                Ok((identity, body)) => self.receive(&Message::with_prefix(identity, body))?,
                Err(omq_tokio::Error::WouldBlock) => break,
                Err(error) => return Err(error.into()),
            }
            full_turn |= index + 1 == TURN;
        }
        Ok(full_turn)
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
            && (packet.envelope.opcode == Opcode::Records
                || packet.envelope.opcode == Opcode::Nack
                    && packet
                        .envelope
                        .request_id
                        .is_none_or(|id| !self.active.contains_key(&id)))
            && self
                .shared
                .readers
                .receive(self.remote, message, packet, self.limits)
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
