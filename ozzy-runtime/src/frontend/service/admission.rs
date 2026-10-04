//! Explicit admission for a trusted client transport domain.

use super::{
    Access, BROKER_ROLE, Binding, Kind, Link, NodeId, Opcode, Peer, ReceiveError, Service,
    ServiceError, handshake, routed,
};

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
        if handled.replaced {
            let binding = Binding {
                peer,
                kind,
                session: self.sessions.session(peer).expect("established handshake"),
            };
            if let Err(error) = self.dispatcher.bind(binding) {
                self.dispatcher.disconnect(peer);
                self.sessions.disconnect(peer);
                self.set_link(peer, None);
                self.peers
                    .get_mut(&peer)
                    .expect("authorized peer")
                    .handshake = None;
                return Err(error.into());
            }
            if kind == Kind::Client
                && let Some(watches) = &mut self.watches
                && let Err(error) = watches.bind(peer, binding.session)
            {
                self.dispatcher.disconnect(peer);
                self.sessions.disconnect(peer);
                self.set_link(peer, None);
                return Err(ReceiveError::Watch(error));
            }
            self.set_link(
                peer,
                Some(Link {
                    binding,
                    send: self.sessions.send_limits(peer)?,
                    remote: self
                        .sessions
                        .remote_parameters(peer)
                        .expect("established handshake"),
                }),
            );
            let state = self.peers.get_mut(&peer).expect("authorized peer");
            state.handshake = None;
            state.awaiting_welcome = handled.reply.is_some();
        }
        if let Some(reply) = handled.reply {
            self.peers
                .get_mut(&peer)
                .expect("authorized peer")
                .handshake = Some(routed(peer, reply));
        }
        Ok(())
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
    /// Configure before starting negotiation. The transport owner may reclaim
    /// a disconnected client only after fencing its exact physical source.
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
            || self.peers.len() >= self.trusted_maximum
        {
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
        self.peers.insert(
            peer,
            Peer {
                access,
                trusted: true,
                initiating: false,
                handshake: None,
                awaiting_welcome: false,
            },
        );
        self.link_current.insert(peer, None);
        Ok(access)
    }

    /// Reclaim a dynamically admitted client after its physical control source
    /// has been retired. The transport owner must reject every receipt from that
    /// retired source before calling `receive`, including HELLO. Configured
    /// clients and broker membership are never removed by this operation.
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
        self.link_current.remove(&peer);
        self.links
            .0
            .current
            .store(std::sync::Arc::new(self.link_current.clone()));
        self.links.0.changed.notify_changed();
        true
    }
}
