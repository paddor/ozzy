//! Explicit admission for a trusted client transport domain.

use super::{
    Access, BROKER_ROLE, Envelope, Kind, NodeId, Opcode, Peer, ReceiveError, Service, ServiceError,
    handshake, nack,
};
use crate::{
    dispatch::{Error, SendFailure},
    frontend::{GrantRequest, Rejected, Rejection, Routed},
};
use omq_tokio::Message;

pub(super) fn validate_roles(kind: Kind, roles: u32) -> Result<(), ReceiveError> {
    let valid = match kind {
        Kind::Client => roles != 0 && roles & !(handshake::PRODUCER | handshake::CONSUMER) == 0,
        Kind::Broker => roles & BROKER_ROLE != 0 && roles & !(handshake::OWNER | BROKER_ROLE) == 0,
    };
    valid.then_some(()).ok_or(ReceiveError::Role)
}

impl Service {
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

    pub(super) fn receive_partition(
        &mut self,
        peer: NodeId,
        envelope: Envelope,
        message: Message,
        retained_bytes: usize,
    ) -> Result<Option<Routed>, ReceiveError> {
        let route = self
            .links
            .get(peer)
            .and_then(|link| self.dispatcher.routes.route(&message, link.binding).ok());
        match self.dispatcher.dispatch(peer, message, retained_bytes) {
            Ok(route) => Ok(Some(route)),
            Err(Rejected { reason, message }) => {
                if matches!(reason, Rejection::Fence(_)) {
                    crate::profiling::event(
                        crate::profiling::Event::DispatcherReplicaFenceRejected,
                    );
                }
                if matches!(
                    reason,
                    Rejection::NoGrant
                        | Rejection::Admission(SendFailure::Admission(
                            Error::Full | Error::Revoked
                        ))
                ) && let Some(route) = route
                    && let Some(requests) = self.requests.get_mut(&route.placement.shard)
                    && let Some(link) = self.links.get(peer)
                {
                    requests.request(
                        GrantRequest {
                            binding: link.binding,
                            route,
                        },
                        retained_bytes,
                    );
                    // Retain only response metadata. The refused payload never
                    // becomes pending dispatcher or shard application work.
                    drop(message);
                    if link.binding.kind == Kind::Broker
                        && route.class == crate::dispatch::Class::Data
                    {
                        crate::profiling::event(crate::profiling::Event::DispatcherBrokerRefusal);
                        let cause = match reason {
                            Rejection::NoGrant => crate::profiling::Event::DispatcherBrokerNoGrant,
                            Rejection::Admission(SendFailure::Admission(Error::Full)) => {
                                crate::profiling::Event::DispatcherBrokerGrantFull
                            }
                            Rejection::Admission(SendFailure::Admission(Error::Revoked)) => {
                                crate::profiling::Event::DispatcherBrokerGrantRevoked
                            }
                            _ => unreachable!("broker refusal must be a grant failure"),
                        };
                        crate::profiling::event(cause);
                    }
                    if link.binding.kind == Kind::Client && envelope.request_id.is_some() {
                        crate::profiling::event(crate::profiling::Event::DispatcherCreditRefusal);
                        match reason {
                            Rejection::NoGrant => crate::profiling::event(
                                crate::profiling::Event::DispatcherMissingGrant,
                            ),
                            Rejection::Admission(SendFailure::Admission(Error::Full)) => {
                                crate::profiling::event(
                                    crate::profiling::Event::DispatcherGrantExhausted,
                                );
                            }
                            _ => {}
                        }
                        self.reject_directory(peer, envelope, 10, nack::RetryClass::AfterCredit)?;
                    }
                }
                Err(ReceiveError::Dispatch(reason))
            }
        }
    }

    /// Allow additional client identities from an explicitly trusted transport
    /// domain. This policy grants only client access. Broker access still comes
    /// from the configured authorization table. Node IDs are routing labels.
    ///
    /// Configure before starting negotiation. Metadata slots remain reserved
    /// across disconnects to retain old HELLO fences, so the limit covers all
    /// distinct client identities observed during this broker lifetime.
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
                initiating: false,
                handshake: None,
                awaiting_welcome: false,
            },
        );
        self.link_current.insert(peer, None);
        Ok(access)
    }
}
