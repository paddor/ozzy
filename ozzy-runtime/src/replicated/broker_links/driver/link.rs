//! One broker's link state: handshake, session, and in-flight requests.

use std::{
    collections::{BTreeMap, btree_map},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use omq_tokio::{IdentitySocket, Message, MonitorEvent, TrySendError};
use ozzy_proto::{
    Envelope, EnvelopeLimits, LinkSessionId, NodeId, Opcode, Packet, RequestId, handshake, nack,
};
use tokio::sync::mpsc;

use super::super::{
    BrokerLinkError, Established, LinkSessions, OwnerClock, Peer, Shared, Startup, readers,
};
use super::body::{group_rejection, progress_rejection, reader_rejection};
use super::{Body, Command, Local, TURN, packet, track};

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

pub(in crate::replicated::broker_links) struct LinkState {
    pub(super) socket: Arc<IdentitySocket>,
    pub(super) input: mpsc::Receiver<Command>,
    pub(super) peer: Arc<Peer>,
    pub(super) shared: Arc<Shared>,
    pub(super) remote: NodeId,
    pub(super) limits: EnvelopeLimits,
    pub(super) clock: OwnerClock,
    pub(super) retry: Duration,
    pub(super) next_hello: Duration,
    pub(super) handshake: Option<Message>,
    pub(super) connection: Option<u64>,
    pub(super) missing_route: bool,
    sending: Option<Waiting>,
    active: BTreeMap<RequestId, Active>,
    /// Doorbell generation captured before this link last read its commands.
    doorbell: u64,
    pub(super) metadata: Vec<u8>,
    pub(super) watches: super::super::routing::Watcher,
    pub(super) appends: super::super::append::Owner,
}

impl LinkState {
    /// The link to `remote`. Callers queue commands through `peer`; this
    /// link reads them from `input`.
    pub(in crate::replicated::broker_links) fn new(
        startup: &Startup,
        remote: NodeId,
        peer: Arc<Peer>,
        input: mpsc::Receiver<Command>,
    ) -> Self {
        let (config, shared) = (&startup.config, &startup.shared);
        Self {
            clock: shared.clock.clone(),
            appends: shared.appends.owner(),
            watches: super::super::routing::Watcher::new(shared.clone(), remote, config),
            socket: startup.control.clone(),
            input,
            peer,
            shared: shared.clone(),
            remote,
            limits: config.parameters.receive.envelope,
            retry: config.retry_interval,
            next_hello: Duration::ZERO,
            handshake: None,
            connection: None,
            missing_route: false,
            sending: None,
            active: BTreeMap::new(),
            doorbell: 0,
            metadata: Vec::with_capacity(config.parameters.receive.envelope.max_metadata_bytes),
        }
    }

    pub(super) fn progress(
        &mut self,
        local: &mut Local,
        now: Duration,
    ) -> Result<bool, BrokerLinkError> {
        self.doorbell = self.peer.doorbell.generation();
        self.expire(now);
        let mut full = self.watches.progress(now, self.link(), &local.routing)?;
        if self.link().is_none() && now >= self.next_hello {
            self.handshake = Some(Message::with_prefix(
                Bytes::copy_from_slice(self.remote.as_bytes()),
                Message::multipart(local.sessions.start(self.remote)?),
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
            if !self.send(now, &local.readers)? {
                break;
            }
            full |= index + 1 == TURN;
        }
        Ok(full)
    }

    /// Wait for a queued or abandoned command, or freed watch capacity.
    pub(super) async fn ready(&self) {
        tokio::select! {
            () = self.peer.doorbell.changed_after(self.doorbell) => {}
            () = self.watches.capacity_ready() => {}
        }
    }

    pub(super) fn deadline(&self) -> Option<Duration> {
        self.active
            .values()
            .map(|active| active.command.deadline)
            .chain(self.sending.iter().map(|waiting| waiting.command.deadline))
            .chain(self.link().is_none().then_some(self.next_hello))
            .chain(self.watches.deadline())
            .min()
    }

    pub(super) fn frame(&self) -> Option<&Message> {
        self.handshake
            .as_ref()
            .or_else(|| {
                self.sending
                    .as_ref()
                    .and_then(|waiting| waiting.frame.as_ref())
            })
            .or_else(|| self.watches.frame())
    }

    pub(super) fn observe(
        &mut self,
        sessions: &mut LinkSessions,
        event: MonitorEvent,
    ) -> Result<(), BrokerLinkError> {
        match event {
            MonitorEvent::HandshakeSucceeded { peer, .. } => {
                if peer.peer_identity.as_deref().is_some_and(|peer| {
                    crate::transport::decode_peer_identity(peer) != Some(self.remote)
                }) {
                    return Err(BrokerLinkError::Response);
                }
                if self.connection.is_some_and(|id| id != peer.connection_id) {
                    self.fence(sessions);
                }
                self.connection = Some(peer.connection_id);
                self.missing_route = false;
            }
            MonitorEvent::Disconnected { peer, .. }
                if self.connection == Some(peer.connection_id) =>
            {
                self.connection = None;
                self.fence(sessions);
            }
            _ => {}
        }
        Ok(())
    }

    /// Fail every request and admit no more once the owner stops.
    pub(super) fn close(&mut self, sessions: &mut LinkSessions) {
        self.fence(sessions);
        self.sending = None;
        self.peer.slots.close();
    }

    fn fence(&mut self, sessions: &mut LinkSessions) {
        sessions.disconnect(self.remote);
        self.replace(None);
        self.handshake = None;
        self.next_hello = Duration::ZERO;
        self.missing_route = true;
        self.shared.changed.notify_changed();
    }

    /// The live link as this driver last installed it.
    fn link(&self) -> Option<Established> {
        self.peer.link()
    }

    pub(super) fn session(&self) -> Option<LinkSessionId> {
        self.link().map(|link| link.session)
    }

    /// Fence appends and watches of the old session, publish `link`, then fail
    /// every request bound to the old session, including a framed one. A
    /// caller woken by that failure already sees `link`.
    fn replace(&mut self, link: Option<Established>) {
        self.appends.fence(self.remote);
        self.watches.fence();
        self.peer.install(link);
        for (_, active) in std::mem::take(&mut self.active) {
            let _ = active.command.reply.send(Err(BrokerLinkError::Session));
        }
        if let Some(waiting) = self.sending.take_if(|waiting| waiting.frame.is_some()) {
            let _ = waiting.command.reply.send(Err(BrokerLinkError::Session));
        }
    }

    pub(super) fn expire(&mut self, now: Duration) {
        let expired = self.active.extract_if(.., |_, active| {
            active.command.deadline <= now || active.command.reply.is_closed()
        });
        for (_, active) in expired {
            let _ = active.command.reply.send(Err(BrokerLinkError::Timeout));
        }
        if let Some(waiting) = self
            .sending
            .take_if(|waiting| waiting.command.deadline <= now)
        {
            let _ = waiting.command.reply.send(Err(BrokerLinkError::Timeout));
        }
    }

    pub(super) fn flush_handshake(&mut self) -> Result<(), BrokerLinkError> {
        if let Some(frame) = self.handshake.clone()
            && self.try_send(frame)?
        {
            self.handshake = None;
        }
        Ok(())
    }

    pub(super) fn flush_watch(&mut self) -> Result<(), BrokerLinkError> {
        if let Some(frame) = self.watches.frame().cloned()
            && self.try_send(frame)?
        {
            self.watches.sent();
        }
        Ok(())
    }

    /// Whether the frame was sent. Full and unroutable destinations wait.
    fn try_send(&mut self, frame: Message) -> Result<bool, BrokerLinkError> {
        match crate::transport::try_send_peer(&self.socket, frame) {
            Ok(()) => {
                self.missing_route = false;
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

    pub(super) fn send(
        &mut self,
        now: Duration,
        readers: &readers::Registry,
    ) -> Result<bool, BrokerLinkError> {
        let Some(mut waiting) = self.sending.take() else {
            return Ok(false);
        };
        if waiting.command.reply.is_closed() || waiting.command.deadline <= now {
            return Ok(true);
        }
        let link = self.link();
        if let Body::Unsubscribe {
            session: expected, ..
        } = &waiting.command.body
            && link.map(|link| link.session) != Some(*expected)
        {
            // The broker already fenced this subscription with its old link.
            // A queued cancellation must never wait for or use another session.
            let _ = waiting.command.reply.send(Err(BrokerLinkError::Session));
            return Ok(true);
        }
        let Some(link) = link else {
            self.sending = Some(waiting);
            return Ok(false);
        };
        let frame = if let Some(frame) = waiting.frame.take() {
            frame
        } else {
            match self.encode(&waiting.command, link.session, link.send.envelope) {
                Ok(frame) => frame,
                Err(error) => {
                    let _ = waiting.command.reply.send(Err(error));
                    return Ok(true);
                }
            }
        };
        let sent = self.try_send(frame.clone());
        if let Ok(true) = sent {
            self.activate(waiting.command, link.session, readers);
        } else {
            waiting.frame = Some(frame);
            self.sending = Some(waiting);
        }
        sent
    }

    /// One request frame for this session, charged to the command's lease.
    fn encode(
        &mut self,
        command: &Command,
        session: LinkSessionId,
        limits: EnvelopeLimits,
    ) -> Result<Message, BrokerLinkError> {
        let header = command.body.encode(
            Envelope {
                opcode: command.body.opcodes().0,
                response: false,
                request_id: Some(command.id),
                sender: self.shared.local,
                session: Some(session),
            },
            &mut self.metadata,
            limits,
        )?;
        Ok(track(
            &crate::native_frames::message(
                self.remote.as_bytes(),
                header,
                &self.metadata,
                Bytes::new(),
            ),
            &command.lease,
            false,
        ))
    }

    /// Track a sent request until its response, timeout, or session loss.
    fn activate(&mut self, command: Command, session: LinkSessionId, readers: &readers::Registry) {
        match &command.body {
            Body::Subscribe { subscribe, inbox } => {
                let sent = readers::Sent {
                    broker: self.remote,
                    session,
                    request: command.id,
                };
                readers.select(inbox, sent, subscribe);
            }
            Body::Unsubscribe { selected, .. } => readers.unselect(selected),
            _ => {}
        }
        self.active.insert(command.id, Active { command, session });
    }

    pub(super) fn receive(
        &mut self,
        local: &mut Local,
        message: &Message,
    ) -> Result<(), BrokerLinkError> {
        let Ok(packet) = packet(message, self.remote, self.limits) else {
            return Ok(());
        };
        if matches!(packet.envelope.opcode, Opcode::Hello | Opcode::Welcome) {
            return self.negotiate(&mut local.sessions, packet);
        }
        let link = self.link();
        if self.route(local, message, packet, link.map(|link| link.session))? {
            return Ok(());
        }
        self.complete(message, packet, link)
    }

    /// Hand watch, APPEND, and uncorrelated reader NACK traffic to its owner.
    /// Whether one of them took `packet`.
    fn route(
        &mut self,
        local: &Local,
        message: &Message,
        packet: Packet<'_>,
        session: Option<LinkSessionId>,
    ) -> Result<bool, BrokerLinkError> {
        if self.watches.receive(packet, session, self.clock.now())? {
            return Ok(true);
        }
        if session == packet.envelope.session && self.appends.receive(self.remote, message, packet)
        {
            return Ok(true);
        }
        Ok(session == packet.envelope.session
            && packet.envelope.opcode == Opcode::Nack
            && packet
                .envelope
                .request_id
                .is_none_or(|id| !self.active.contains_key(&id))
            && local
                .readers
                .receive(self.remote, message, packet, self.limits)
                .is_ok())
    }

    /// Complete the active request `packet` answers on the live `link`.
    fn complete(
        &mut self,
        message: &Message,
        packet: Packet<'_>,
        link: Option<Established>,
    ) -> Result<(), BrokerLinkError> {
        let Some(id) = packet.envelope.request_id else {
            return Ok(());
        };
        let btree_map::Entry::Occupied(entry) = self.active.entry(id) else {
            return Ok(());
        };
        let active = entry.get();
        if !answers(packet, active, link.map(|link| link.session)) {
            return Ok(());
        }
        let rejected = if packet.envelope.opcode == Opcode::Nack {
            Some(rejection(&active.command.body, packet, link, self.limits)?)
        } else {
            None
        };
        let active = entry.remove();
        let result = match rejected {
            Some(error) => Err(error),
            None => active
                .command
                .body
                .reply(packet, message, &active.command.lease, self.limits),
        };
        #[cfg(test)]
        if result.is_ok() {
            self.shared
                .delivered
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let _ = active.command.reply.send(result);
        Ok(())
    }

    fn negotiate(
        &mut self,
        sessions: &mut LinkSessions,
        packet: Packet<'_>,
    ) -> Result<(), BrokerLinkError> {
        let handled = sessions.receive(self.remote, packet)?;
        self.handshake = handled.reply.map(|reply| {
            Message::with_prefix(
                Bytes::copy_from_slice(self.remote.as_bytes()),
                Message::multipart(reply),
            )
        });
        if let Some(link) = handled.replaced {
            if self.link().is_some() {
                self.replace(Some(link));
            } else {
                self.peer.install(Some(link));
            }
            self.shared.changed.notify_changed();
        }
        Ok(())
    }
}

/// Whether `packet` is the reply `active` awaits on the live `session`: an
/// empty NACK or the response opcode of its request.
fn answers(packet: Packet<'_>, active: &Active, session: Option<LinkSessionId>) -> bool {
    let opcode = packet.envelope.opcode;
    packet.envelope.response
        && packet.envelope.session == Some(active.session)
        && session == Some(active.session)
        && packet.payload.is_empty()
        && (opcode == Opcode::Nack || opcode == active.command.body.opcodes().1)
}

/// The typed rejection a NACK carries for `body`. Only a link that
/// negotiated owner routing gets an authority hint with it.
fn rejection(
    body: &Body,
    packet: Packet<'_>,
    link: Option<Established>,
    limits: EnvelopeLimits,
) -> Result<BrokerLinkError, BrokerLinkError> {
    let reply = nack::decode(packet, limits)?;
    let hint = if matches!(reply.code, 5 | 12 | 13)
        && link.is_some_and(|link| link.remote.capabilities & handshake::OWNER_ROUTING != 0)
    {
        Some(nack::AuthorityHint::decode(reply.detail).map_err(|_| BrokerLinkError::Response)?)
    } else {
        None
    };
    Ok(match *body {
        Body::Progress(request) => progress_rejection(request, reply, hint),
        Body::Assign(_) | Body::Join(_) | Body::Inspect(_) | Body::Delete(_) => {
            group_rejection(reply, hint)
        }
        _ => reader_rejection(reply, matches!(body, Body::Subscribe { .. }), hint),
    })
}
