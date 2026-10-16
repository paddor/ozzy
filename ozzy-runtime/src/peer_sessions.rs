//! Native broker/client link negotiation, including simultaneous HELLO.

use std::collections::HashMap;

use bytes::Bytes;
use ozzy_proto::data::DataLimits;
use ozzy_proto::handshake::{self, Handshake, Parameters};
use ozzy_proto::{Envelope, LinkSessionId, NodeId, Opcode, Packet, RequestId};

use crate::{Error, Result};

mod ids;
pub use ids::LinkIds;

#[derive(Debug, Clone, Copy)]
struct Live {
    id: LinkSessionId,
    remote: Parameters,
}

#[derive(Debug, Default)]
struct Peer {
    pending: Option<(RequestId, Handshake)>,
    incoming: Option<Handshake>,
    live: Option<Live>,
    observed: Option<(u128, u128)>,
    welcome_superseded: Option<u128>,
}

/// Bounded HELLO/WELCOME state of one broker or SDK transport owner. Owns link
/// fences, not authorization, partition roles, queue permits, or confirmations.
/// The owner publishes its own fenced view of the live links.
#[derive(Debug)]
pub struct Sessions {
    local: NodeId,
    instance: u128,
    next_nonce: u64,
    parameters: Parameters,
    receive_profiles: HashMap<NodeId, DataLimits>,
    required_remote_roles: u32,
    maximum: usize,
    ids: LinkIds,
    peers: HashMap<NodeId, Peer>,
}

/// Result of a handshake observation. Install any changed fence before routing
/// application traffic. A reply still needs bounded transport admission.
#[derive(Debug, Default)]
pub struct Handled {
    /// Three Ozzy frames, excluding the transport destination identity.
    pub reply: Option<Vec<Bytes>>,
    /// A new live session replaced the old one. Old grants and replies must
    /// be invalidated.
    pub replaced: Option<Established>,
}

/// Live session installed by a handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Established {
    /// New link fence.
    pub session: LinkSessionId,
    /// Remote receive limits and the capabilities both sides support.
    pub remote: Parameters,
    /// Common outbound codec bounds within both endpoint profiles.
    pub send: DataLimits,
}

impl Sessions {
    #[cfg(test)]
    pub(super) fn new(local: NodeId, limits: DataLimits, maximum: usize) -> Result<Self> {
        Self::with_ids(
            local,
            Parameters::reader(limits, handshake::OWNER | handshake::CONSUMER)?,
            handshake::OWNER | handshake::CONSUMER,
            maximum,
            LinkIds::random(),
        )
    }

    /// Supply a directional protocol profile and identity source. Required role
    /// bits are checked on both HELLO and WELCOME. Zero permits mixed broker and
    /// client roles; the caller must then validate its independently authorized
    /// peer profile before calling `receive`. Role claims never authorize peers.
    ///
    /// Peer metadata is bounded across reconnects and is not evicted. A caller
    /// must reject unknown/unauthorized transport identities before negotiation.
    pub fn with_ids(
        local: NodeId,
        parameters: Parameters,
        required_remote_roles: u32,
        maximum: usize,
        ids: LinkIds,
    ) -> Result<Self> {
        parameters.validate()?;
        if local.as_bytes() == &[0; 16] || maximum == 0 || required_remote_roles & !31 != 0 {
            return Err(Error::NotConnected);
        }
        Ok(Self {
            local,
            instance: ids.next()?,
            next_nonce: 1,
            parameters,
            receive_profiles: HashMap::new(),
            required_remote_roles,
            maximum,
            ids,
            peers: HashMap::new(),
        })
    }

    /// Fixed, independently authorized peer profiles. Overrides change receive
    /// bounds only, preserving capability/role negotiation and session fencing.
    pub(crate) fn with_receive_profiles(
        mut self,
        profiles: &[(NodeId, DataLimits)],
    ) -> Result<Self> {
        if profiles.len() > self.maximum {
            return Err(Error::NotConnected);
        }
        for &(remote, receive) in profiles {
            Parameters {
                receive,
                ..self.parameters
            }
            .validate()?;
            if remote == self.local
                || remote.as_bytes() == &[0; 16]
                || self.receive_profiles.insert(remote, receive).is_some()
            {
                return Err(Error::NotConnected);
            }
        }
        Ok(self)
    }

    /// Begin or retry one HELLO. Repeated calls preserve its exact request and
    /// nonce until negotiation finishes. Beginning a new attempt fences the old
    /// live session immediately; the caller must also fence its grants/replies.
    pub fn start(&mut self, remote: NodeId) -> Result<Vec<Bytes>> {
        if remote == self.local || remote.as_bytes() == &[0; 16] {
            return Err(Error::NotConnected);
        }
        let parameters = self.profile(remote);
        self.capacity(remote)?;
        let peer = self.peers.entry(remote).or_default();
        let (id, hello) = if let Some(attempt) = peer.pending {
            attempt
        } else {
            let nonce = self.next_nonce;
            self.next_nonce = nonce.checked_add(1).ok_or(Error::NotConnected)?;
            *peer.pending.insert((
                RequestId::from_bytes(self.ids.next()?.to_be_bytes()),
                Handshake {
                    superseded_hello: None,
                    instance_id: self.instance,
                    hello_nonce: u128::from(nonce),
                    parameters,
                },
            ))
        };
        // A repeated simultaneous initiator shares the active HELLO attempt.
        peer.live = None;
        peer.incoming = None;
        encode(
            Envelope {
                opcode: Opcode::Hello,
                response: false,
                request_id: Some(id),
                sender: self.local,
                session: None,
            },
            hello,
            parameters.receive,
        )
    }

    /// Current independently negotiated link fence, if established.
    pub fn session(&self, remote: NodeId) -> Option<LinkSessionId> {
        self.peers.get(&remote)?.live.map(|live| live.id)
    }

    /// Test a command's session before admitting any application work.
    pub fn is_current(&self, remote: NodeId, session: LinkSessionId) -> bool {
        self.session(remote) == Some(session)
    }

    /// The live session, its negotiated parameters, and send limits. The
    /// remote limits do not constitute destination-backed admission credit.
    pub fn established(&self, remote: NodeId) -> Option<Established> {
        Some(self.establish(remote, self.peers.get(&remote)?.live?))
    }

    fn establish(&self, remote: NodeId, live: Live) -> Established {
        Established {
            session: live.id,
            remote: Parameters {
                capabilities: live.remote.capabilities & self.parameters.capabilities,
                ..live.remote
            },
            send: self
                .receive_limits(remote)
                .intersection(live.remote.receive),
        }
    }

    /// Forget live/pending state while retaining the old receive fence and its
    /// metadata slot. A delayed duplicate HELLO cannot resurrect the old link.
    pub fn disconnect(&mut self, remote: NodeId) {
        if let Some(peer) = self.peers.get_mut(&remote) {
            peer.live = None;
            peer.pending = None;
            peer.incoming = None;
            peer.welcome_superseded = None;
        }
    }

    /// Reclaim metadata after the transport owner completes dequeued input and
    /// observes control retirement. OMQ fences remaining queued/restored input.
    pub(crate) fn retire(&mut self, remote: NodeId) {
        self.peers.remove(&remote);
    }

    /// Process one bounded handshake. `remote` comes from an independently
    /// validated transport identity. No socket, timer, or payload work occurs.
    pub fn receive(&mut self, remote: NodeId, packet: Packet<'_>) -> Result<Handled> {
        let (h, selected) = self.check_handshake(remote, packet)?;
        if packet.envelope.opcode == Opcode::Welcome && !self.peers.contains_key(&remote) {
            return Ok(Handled::default());
        }
        self.capacity(remote)?;
        let peer = self.peers.entry(remote).or_default();
        let old = peer.live.map(|l| l.id);
        let reply = match packet.envelope.opcode {
            Opcode::Hello => {
                if peer.ignores_hello(h, self.local < remote) {
                    return Ok(Handled::default());
                }
                let duplicate = peer.incoming == Some(h);
                let superseded = if duplicate {
                    peer.welcome_superseded
                } else {
                    peer.pending.map(|(_, h)| h.hello_nonce)
                };
                let session = if duplicate {
                    old.ok_or(Error::NotConnected)?
                } else {
                    LinkSessionId::from_bytes(self.ids.next()?.to_be_bytes())
                };
                let response = encode(
                    Envelope {
                        opcode: Opcode::Welcome,
                        response: true,
                        sender: self.local,
                        session: Some(session),
                        ..packet.envelope
                    },
                    Handshake {
                        superseded_hello: superseded,
                        instance_id: self.instance,
                        hello_nonce: h.hello_nonce,
                        parameters: selected,
                    },
                    h.parameters.receive,
                )?;
                peer.accept_hello(h, session, superseded);
                Some(response)
            }
            Opcode::Welcome => {
                peer.welcome(packet.envelope, h, selected)?;
                None
            }
            _ => return Err(Error::NotConnected),
        };
        let replaced = peer
            .live
            .filter(|live| old != Some(live.id))
            .map(|live| self.establish(remote, live));
        Ok(Handled { reply, replaced })
    }

    /// Decode a HELLO or WELCOME from `remote` and select common parameters.
    fn check_handshake(
        &self,
        remote: NodeId,
        packet: Packet<'_>,
    ) -> Result<(Handshake, Parameters)> {
        let parameters = self.profile(remote);
        let h = handshake::decode(packet, parameters.receive.envelope)?;
        if packet.envelope.sender != remote
            || remote == self.local
            || remote.as_bytes() == &[0; 16]
            || h.parameters.roles & self.required_remote_roles != self.required_remote_roles
        {
            return Err(Error::NotConnected);
        }
        Ok((h, parameters.select(h.parameters)?))
    }

    pub(crate) fn receive_limits(&self, remote: NodeId) -> DataLimits {
        self.receive_profiles
            .get(&remote)
            .copied()
            .unwrap_or(self.parameters.receive)
    }

    fn profile(&self, remote: NodeId) -> Parameters {
        Parameters {
            receive: self.receive_limits(remote),
            ..self.parameters
        }
    }

    fn capacity(&self, remote: NodeId) -> Result<()> {
        if !self.peers.contains_key(&remote) && self.peers.len() >= self.maximum {
            return Err(Error::TooManyPendingRequests);
        }
        Ok(())
    }
}

impl Peer {
    /// Drop a stale or replayed HELLO, or one that loses simultaneous
    /// initiation to this side's pending HELLO.
    fn ignores_hello(&mut self, h: Handshake, local_wins: bool) -> bool {
        if self.observed.is_some_and(|(instance, nonce)| {
            instance == h.instance_id
                && (h.hello_nonce < nonce || (h.hello_nonce == nonce && self.incoming != Some(h)))
        }) {
            return true;
        }
        // During simultaneous initiation, lower ID's HELLO wins.
        if self.pending.is_some() && local_wins {
            self.observed = Some((h.instance_id, h.hello_nonce));
            return true;
        }
        false
    }

    /// Install the session this side's WELCOME grants.
    fn accept_hello(&mut self, h: Handshake, session: LinkSessionId, superseded: Option<u128>) {
        self.observed = Some((h.instance_id, h.hello_nonce));
        self.pending = None;
        self.welcome_superseded = superseded;
        self.incoming = Some(h);
        self.live = Some(Live {
            id: session,
            remote: h.parameters,
        });
    }

    fn welcome(&mut self, envelope: Envelope, h: Handshake, selected: Parameters) -> Result<()> {
        let Some((request, hello)) = self.pending else {
            return Ok(());
        };
        if envelope.request_id != Some(request) || h.hello_nonce != hello.hello_nonce {
            return Ok(());
        }
        if selected.capabilities != h.parameters.capabilities {
            return Err(Error::NotConnected);
        }
        let id = envelope.session.ok_or(Error::NotConnected)?;
        self.pending = None;
        self.incoming = None;
        if let Some(nonce) = h.superseded_hello {
            let previous = self
                .observed
                .filter(|(instance, _)| *instance == h.instance_id)
                .map_or(0, |(_, n)| n);
            self.observed = Some((h.instance_id, previous.max(nonce)));
        }
        self.live = Some(Live {
            id,
            remote: h.parameters,
        });

        Ok(())
    }
}

fn encode(envelope: Envelope, h: Handshake, limits: DataLimits) -> Result<Vec<Bytes>> {
    // The fixed handshake properties fit in 512 bytes, irrespective of the
    // much larger application metadata allowance on this multiplexed link.
    let mut metadata = Vec::with_capacity(512);
    let header = handshake::encode(envelope, h, &mut metadata, limits.envelope)?;
    Ok(vec![
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ])
}

#[cfg(test)]
mod tests;
