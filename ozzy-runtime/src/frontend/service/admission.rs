//! Explicit admission for a trusted client transport domain.

use super::{
    Access, BROKER_ROLE, Binding, Kind, Link, NodeId, Opcode, Peer, ReceiveError, Service,
    ServiceError, handshake, routed,
};
use crate::peer_sessions::Established;

pub(super) fn validate_roles(kind: Kind, roles: u32) -> Result<(), ReceiveError> {
    let valid = match kind {
        Kind::Client => roles != 0 && roles & !(handshake::PRODUCER | handshake::CONSUMER) == 0,
        Kind::Broker => roles & BROKER_ROLE != 0 && roles & !(handshake::OWNER | BROKER_ROLE) == 0,
    };
    valid.then_some(()).ok_or(ReceiveError::Role)
}

impl Service {
    /// Install the negotiated link only after dispatcher and watch fencing.
    pub(super) fn receive_handshake(
        &mut self,
        peer: NodeId,
        kind: Kind,
        packet: ozzy_proto::Packet<'_>,
        envelope: ozzy_proto::EnvelopeLimits,
    ) -> Result<(), ReceiveError> {
        let hello = handshake::decode(packet, envelope)?;
        // These bits constrain the authorized profile. They do not authorize
        // a routing identity or establish replication membership.
        validate_roles(kind, hello.parameters.roles)?;
        let handled = self.sessions.receive(peer, packet)?;
        let link = match handled.replaced {
            Some(established) => Some(self.establish(peer, kind, established)?),
            None => None,
        };
        let state = self.peers.get_mut(&peer).ok_or(ReceiveError::Peer)?;
        let reply = handled.reply.map(|reply| routed(peer, reply));
        let changed = if let Some(link) = link {
            state.awaiting_welcome = reply.is_some();
            state.handshake = reply;
            state.replace_link(Some(link))
        } else {
            if reply.is_some() {
                state.handshake = reply;
            }
            false
        };
        if changed {
            self.publish_links();
        }
        Ok(())
    }

    /// Bind a replacement session in the dispatcher and watch registry. A
    /// refusal fences the session again.
    fn establish(
        &mut self,
        peer: NodeId,
        kind: Kind,
        established: Established,
    ) -> Result<Link, ReceiveError> {
        let binding = Binding {
            peer,
            kind,
            session: established.session,
        };
        if let Err(error) = self.dispatcher.bind(binding) {
            self.abandon(peer);
            if let Some(state) = self.peers.get_mut(&peer) {
                state.handshake = None;
            }
            return Err(error.into());
        }
        if kind == Kind::Client
            && let Some(watches) = &mut self.watches
            && let Err(error) = watches.bind(peer, binding.session)
        {
            self.abandon(peer);
            return Err(ReceiveError::Watch(error));
        }
        Ok(Link {
            binding,
            send: established.send,
            remote: established.remote,
        })
    }

    fn abandon(&mut self, peer: NodeId) {
        self.dispatcher.disconnect(peer);
        self.sessions.disconnect(peer);
        self.set_link(peer, None);
    }

    /// Bound physical connection metadata, including trusted clients before HELLO.
    pub fn transport_peer_capacity(&self) -> usize {
        self.trusted_maximum.max(self.peers.len())
    }

    /// A trusted transport can introduce SDK identities, never broker authority.
    pub fn accepts_transport_peer(&self, peer: NodeId) -> bool {
        self.peers.contains_key(&peer)
            || (self.peers.len() < self.trusted_maximum
                && peer != self.dispatcher.local
                && peer.as_bytes() != &[0; 16])
    }

    /// Allow additional client identities from an explicitly trusted transport
    /// domain. This policy grants only client access. Broker access still comes
    /// from the configured authorization table. Node IDs are routing labels.
    ///
    /// Configure before starting negotiation. The transport owner completes
    /// already dequeued input before observing client retirement and relies on
    /// OMQ generation fencing for queued and restored input.
    /// The limit bounds concurrently retained client metadata, not lifetime churn.
    pub fn with_trusted_clients(mut self, maximum: usize) -> Result<Self, ServiceError> {
        if maximum == 0
            || self.trusted_maximum != 0
            || !self.dispatcher.peers.is_empty()
            || self
                .peers
                .values()
                .any(|peer| peer.handshake.is_some() || peer.initiating)
            || maximum
                > self
                    .dispatcher
                    .limits
                    .peers
                    .saturating_sub(self.peers.len())
        {
            return Err(ServiceError::Access);
        }
        self.trusted_maximum = self.peers.len() + maximum;
        Ok(self)
    }

    pub(super) fn admit_trusted_client(
        &mut self,
        peer: NodeId,
        packet: ozzy_proto::Packet<'_>,
    ) -> Result<Access, ReceiveError> {
        if packet.envelope.opcode != Opcode::Hello
            || packet.envelope.sender != peer
            || peer == self.dispatcher.local
            || peer.as_bytes() == &[0; 16]
        {
            return Err(ReceiveError::Peer);
        }
        if self.peers.len() >= self.trusted_maximum {
            // No reply route exists before admission; the client times out.
            crate::profiling::event(crate::profiling::Event::ClientLimitRefusal);
            crate::profiling::log(
                crate::profiling::Level::Warn,
                "client limit reached; dropped HELLO",
            );
            return Err(ReceiveError::Peer);
        }
        let hello = handshake::decode(packet, self.parameters.receive.envelope)?;
        validate_roles(Kind::Client, hello.parameters.roles)?;
        // Complete profile validation before reserving a metadata slot.
        self.parameters.select(hello.parameters)?;
        let access = Access {
            peer,
            kind: Kind::Client,
        };
        self.peers.insert(peer, Peer::new(access, true));
        Ok(access)
    }

    /// Reclaim a dynamically admitted client after control retirement. The owner
    /// must complete already dequeued input before observing retirement, then
    /// rely on OMQ generation fencing for queued and restored input, including
    /// HELLO. Configured clients and broker membership are never removed.
    pub fn retire_transport_client(&mut self, peer: NodeId) -> bool {
        if !self.peers.get(&peer).is_some_and(|state| state.trusted) {
            return false;
        }
        if let Some(link) = self.links.get(peer) {
            self.disconnect(link.binding);
        }
        self.dispatcher.disconnect(peer);
        self.sessions.retire(peer);
        self.peers.remove(&peer);
        self.publish_links();
        true
    }
}
