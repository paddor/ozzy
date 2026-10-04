//! Native broker/client link negotiation, including simultaneous HELLO.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use bytes::Bytes;
use ozzy_proto::data::DataLimits;
use ozzy_proto::handshake::{self, Handshake, Parameters};
use ozzy_proto::{Envelope, LinkSessionId, NodeId, Opcode, Packet, RequestId};

use crate::signal::StateSignal;
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

/// Bounded HELLO/WELCOME state shared by broker and SDK transports. Owns link
/// fences, not authorization, partition roles, queue permits, or confirmations.
#[derive(Debug)]
pub struct Sessions {
    local: NodeId,
    instance: u128,
    next_nonce: AtomicU64,
    parameters: Parameters,
    receive_profiles: HashMap<NodeId, DataLimits>,
    required_remote_roles: u32,
    maximum: usize,
    ids: Mutex<LinkIds>,
    peers: Mutex<HashMap<NodeId, Peer>>,
    current: ArcSwap<HashMap<NodeId, LinkSessionId>>,
    changed: StateSignal,
}

/// Result of a handshake observation. Install any changed fence before routing
/// application traffic. A reply still needs bounded transport admission.
#[derive(Debug)]
pub struct Handled {
    /// Three Ozzy frames, excluding the transport destination identity.
    pub reply: Option<Vec<Bytes>>,
    /// The live session changed. Old grants and replies must be invalidated.
    pub replaced: bool,
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
        Self::with_receive_profiles(local, parameters, required_remote_roles, maximum, ids, &[])
    }

    /// Fixed, independently authorized peer profiles. Overrides change receive
    /// bounds only, preserving capability/role negotiation and session fencing.
    pub(crate) fn with_receive_profiles(
        local: NodeId,
        parameters: Parameters,
        required_remote_roles: u32,
        maximum: usize,
        ids: LinkIds,
        profiles: &[(NodeId, DataLimits)],
    ) -> Result<Self> {
        parameters.validate()?;
        if local.as_bytes() == &[0; 16]
            || maximum == 0
            || required_remote_roles & !31 != 0
            || profiles.len() > maximum
        {
            return Err(Error::NotConnected);
        }
        let mut receive_profiles = HashMap::new();
        for &(remote, receive) in profiles {
            Parameters {
                receive,
                ..parameters
            }
            .validate()?;
            if remote == local
                || remote.as_bytes() == &[0; 16]
                || receive_profiles.insert(remote, receive).is_some()
            {
                return Err(Error::NotConnected);
            }
        }
        Ok(Self {
            local,
            instance: ids.next()?,
            next_nonce: AtomicU64::new(1),
            parameters,
            receive_profiles,
            required_remote_roles,
            maximum,
            ids: Mutex::new(ids),
            peers: Mutex::new(HashMap::new()),
            current: ArcSwap::from_pointee(HashMap::new()),
            changed: StateSignal::default(),
        })
    }

    /// Begin or retry one HELLO. Repeated calls preserve its exact request and
    /// nonce until negotiation finishes. Beginning a new attempt fences the old
    /// live session immediately; the caller must also fence its grants/replies.
    pub fn start(&self, remote: NodeId) -> Result<Vec<Bytes>> {
        if remote == self.local || remote.as_bytes() == &[0; 16] {
            return Err(Error::NotConnected);
        }
        let parameters = self.profile(remote);
        let mut peers = self.peers.lock().expect("session mutex poisoned");
        self.capacity(&peers, remote)?;
        let peer = peers.entry(remote).or_default();
        if peer.pending.is_none() {
            let nonce = self
                .next_nonce
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .map_err(|_| Error::NotConnected)?;
            peer.pending = Some((
                RequestId::from_bytes(self.next_id()?.to_be_bytes()),
                Handshake {
                    superseded_hello: None,
                    instance_id: self.instance,
                    hello_nonce: u128::from(nonce),
                    parameters,
                },
            ));
        }
        let (id, hello) = peer.pending.expect("installed attempt");
        // A repeated simultaneous initiator shares the active HELLO attempt.
        peer.live = None;
        peer.incoming = None;
        self.publish(&peers);
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

    /// Cancel-safe wait for this one broker link, without waiting for other
    /// brokers, partitions, or application authority.
    pub async fn ready(&self, remote: NodeId) {
        self.changed
            .wait_for(|| self.session(remote).map(|_| ()))
            .await;
    }

    /// Current independently negotiated link fence, if established.
    pub fn session(&self, remote: NodeId) -> Option<LinkSessionId> {
        self.current.load().get(&remote).copied()
    }

    /// Test a command's session before admitting any application work.
    pub fn is_current(&self, remote: NodeId, session: LinkSessionId) -> bool {
        self.session(remote) == Some(session)
    }

    /// Negotiated capabilities and the remote's directional receive limits.
    /// These limits do not constitute destination-backed admission credit.
    pub fn remote_parameters(&self, remote: NodeId) -> Option<Parameters> {
        let peers = self.peers.lock().expect("session mutex poisoned");
        let mut parameters = peers.get(&remote)?.live?.remote;
        parameters.capabilities &= self.parameters.capabilities;
        Some(parameters)
    }

    /// Forget live/pending state while retaining the old receive fence and its
    /// metadata slot. A delayed duplicate HELLO cannot resurrect the old link.
    pub fn disconnect(&self, remote: NodeId) {
        let mut peers = self.peers.lock().expect("session mutex poisoned");
        if let Some(peer) = peers.get_mut(&remote) {
            peer.live = None;
            peer.pending = None;
            peer.incoming = None;
            peer.welcome_superseded = None;
        }
        self.publish(&peers);
        drop(peers);
        self.changed.notify_changed();
    }

    /// Reclaim metadata after the transport owner completes dequeued input and
    /// observes control retirement. OMQ fences remaining queued/restored input.
    pub(crate) fn retire(&self, remote: NodeId) {
        let mut peers = self.peers.lock().expect("session mutex poisoned");
        peers.remove(&remote);
        self.publish(&peers);
        drop(peers);
        self.changed.notify_changed();
    }

    /// Common codec bounds within both configured endpoint profiles.
    pub fn send_limits(&self, remote: NodeId) -> Result<DataLimits> {
        let local = self.receive_limits(remote);
        let peers = self.peers.lock().expect("session mutex poisoned");
        let remote = peers
            .get(&remote)
            .and_then(|p| p.live)
            .ok_or(Error::NotConnected)?
            .remote
            .receive;
        Ok(local.intersection(remote))
    }

    /// Process one bounded handshake. `remote` comes from an independently
    /// validated transport identity. No socket, timer, or payload work occurs.
    pub fn receive(&self, remote: NodeId, packet: Packet<'_>) -> Result<Handled> {
        let parameters = self.profile(remote);
        let h = handshake::decode(packet, parameters.receive.envelope)?;
        if packet.envelope.sender != remote
            || remote == self.local
            || remote.as_bytes() == &[0; 16]
            || h.parameters.roles & self.required_remote_roles != self.required_remote_roles
        {
            return Err(Error::NotConnected);
        }
        let selected = parameters.select(h.parameters)?;
        let mut peers = self.peers.lock().expect("session mutex poisoned");
        if packet.envelope.opcode == Opcode::Welcome && !peers.contains_key(&remote) {
            return Ok(Handled {
                reply: None,
                replaced: false,
            });
        }
        self.capacity(&peers, remote)?;
        let peer = peers.entry(remote).or_default();
        let old = peer.live.map(|l| l.id);
        let reply = match packet.envelope.opcode {
            Opcode::Hello => {
                if peer.observed.is_some_and(|(instance, nonce)| {
                    instance == h.instance_id
                        && (h.hello_nonce < nonce
                            || (h.hello_nonce == nonce && peer.incoming != Some(h)))
                }) {
                    return Ok(Handled {
                        reply: None,
                        replaced: false,
                    });
                }
                // During simultaneous initiation, lower ID's HELLO wins.
                if peer.pending.is_some() && self.local < remote {
                    peer.observed = Some((h.instance_id, h.hello_nonce));
                    return Ok(Handled {
                        reply: None,
                        replaced: false,
                    });
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
                    LinkSessionId::from_bytes(self.next_id()?.to_be_bytes())
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
                peer.observed = Some((h.instance_id, h.hello_nonce));
                peer.pending = None;
                peer.welcome_superseded = superseded;
                peer.incoming = Some(h);
                peer.live = Some(Live {
                    id: session,
                    remote: h.parameters,
                });
                Some(response)
            }
            Opcode::Welcome => {
                peer.welcome(packet.envelope, h, selected)?;
                None
            }
            _ => return Err(Error::NotConnected),
        };
        let replaced = old != peer.live.map(|l| l.id);
        self.publish(&peers);
        drop(peers);
        self.changed.notify_changed();
        Ok(Handled { reply, replaced })
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

    fn capacity(&self, peers: &HashMap<NodeId, Peer>, remote: NodeId) -> Result<()> {
        if !peers.contains_key(&remote) && peers.len() >= self.maximum {
            return Err(Error::TooManyPendingRequests);
        }
        Ok(())
    }

    fn publish(&self, peers: &HashMap<NodeId, Peer>) {
        self.current.store(std::sync::Arc::new(
            peers
                .iter()
                .filter_map(|(&node, peer)| peer.live.map(|live| (node, live.id)))
                .collect(),
        ));
    }

    fn next_id(&self) -> Result<u128> {
        self.ids.lock().expect("session ID mutex poisoned").next()
    }
}

impl Peer {
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
