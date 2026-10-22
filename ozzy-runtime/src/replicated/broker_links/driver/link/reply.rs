//! Inbound frames on one broker link: handshakes, owner traffic, and the
//! replies that complete requests.

use super::{
    Active, Body, BrokerLinkError, Bytes, EnvelopeLimits, Established, LinkSessionId, LinkSessions,
    LinkState, Local, Message, Opcode, Packet, btree_map, group_rejection, handshake, nack, packet,
    progress_rejection, reader_rejection,
};

impl LinkState {
    pub(in crate::replicated::broker_links) fn receive(
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
