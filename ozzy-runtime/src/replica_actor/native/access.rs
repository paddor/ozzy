//! Trusted client authorization and bounded logical writer assignments.

use super::{NativeIntakeError, Slot};
use crate::{frontend::Link, replicated::ClientAccess};
use ozzy_proto::{LinkSessionId, NodeId, ProducerId};

/// Physical client trust is established independently of producer epochs.
/// Neither a node ID nor a generated writer ID authenticates an application.
#[derive(Clone, Debug)]
pub enum NativeAccess {
    /// Explicit writer identities, retaining the existing provisioning boundary.
    Writers(Vec<ClientAccess>),
    /// Trusted or authenticated client processes may choose fresh writer IDs.
    /// Each partition reserves a finite table and arenas before serving them.
    Clients {
        /// Allowed physical client identities, at most 32 distinct nodes.
        peers: Vec<NodeId>,
        /// Maximum logical writer assignments across these clients, 1..=32.
        writers: usize,
    },
    /// Accept independently established client links from the frontend's
    /// explicit trusted-domain policy, without prelisting SDK node identities.
    TrustedClients {
        /// Maximum client identities assigned concurrently on this partition.
        clients: usize,
        /// Maximum logical writer assignments, 1..=32.
        writers: usize,
    },
}

impl NativeAccess {
    /// Exact actor arena count, including separate rejection capacity for
    /// dynamic clients. No allocation is derived from incoming writer IDs.
    pub fn required_buffers(&self, requests_per_writer: usize) -> Option<usize> {
        self.writer_count()
            .checked_mul(requests_per_writer.checked_add(1)?)?
            .checked_add(match self {
                Self::Writers(_) => 0,
                Self::Clients { peers, .. } => peers.len(),
                Self::TrustedClients { clients, .. } => *clients,
            })
    }

    pub(super) fn writer_count(&self) -> usize {
        match self {
            Self::Writers(peers) => peers.len(),
            Self::Clients { writers, .. } | Self::TrustedClients { writers, .. } => *writers,
        }
    }

    pub(super) fn validate(&self, local: NodeId) -> Result<(), NativeIntakeError> {
        let valid_node = |node: NodeId| node != local && node.as_bytes() != &[0; 16];
        let valid = match self {
            Self::Writers(peers) => {
                !peers.is_empty()
                    && peers.len() <= 32
                    && peers.iter().enumerate().all(|(index, peer)| {
                        valid_node(peer.node)
                            && peer.producer.as_bytes() != &[0; 16]
                            && !peers[..index]
                                .iter()
                                .any(|old| old.node == peer.node && old.producer == peer.producer)
                    })
            }
            Self::Clients { peers, writers } => {
                !peers.is_empty()
                    && peers.len() <= 32
                    && (1..=32).contains(writers)
                    && peers
                        .iter()
                        .enumerate()
                        .all(|(index, &node)| valid_node(node) && !peers[..index].contains(&node))
            }
            Self::TrustedClients { clients, writers } => {
                (1..=32).contains(clients) && (1..=32).contains(writers)
            }
        };
        if valid {
            Ok(())
        } else {
            Err(NativeIntakeError::Configuration)
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Writer {
    client: ClientAccess,
    session: Option<LinkSessionId>,
}

/// Entries own no payload or authority. All arenas and slots are preallocated.
#[derive(Debug)]
pub(super) struct Writers {
    access: NativeAccess,
    entries: Vec<Option<Writer>>,
    clients: Vec<Option<NodeId>>,
}

impl Writers {
    pub(super) fn new(access: NativeAccess) -> Self {
        let entries = match &access {
            NativeAccess::Writers(peers) => peers
                .iter()
                .map(|&client| {
                    Some(Writer {
                        client,
                        session: None,
                    })
                })
                .collect(),
            NativeAccess::Clients { writers, .. }
            | NativeAccess::TrustedClients { writers, .. } => vec![None; *writers],
        };
        let clients = match &access {
            NativeAccess::TrustedClients { clients, .. } => vec![None; *clients],
            _ => Vec::new(),
        };
        Self {
            access,
            entries,
            clients,
        }
    }

    pub(super) fn peer(&mut self, node: NodeId) -> Option<usize> {
        match &self.access {
            NativeAccess::Writers(peers) => peers.iter().position(|peer| peer.node == node),
            NativeAccess::Clients { peers, .. } => peers.iter().position(|&peer| peer == node),
            NativeAccess::TrustedClients { .. } => {
                if let Some(index) = self.clients.iter().position(|&peer| peer == Some(node)) {
                    return Some(index);
                }
                let index = self.clients.iter().position(Option::is_none)?;
                self.clients[index] = Some(node);
                Some(index)
            }
        }
    }

    pub(super) fn rejection_slot(&self, peer: usize, stride: usize) -> usize {
        match self.access {
            NativeAccess::Writers(_) => peer * stride,
            NativeAccess::Clients { .. } | NativeAccess::TrustedClients { .. } => {
                self.entries.len() * stride + peer
            }
        }
    }

    pub(super) fn select(
        &mut self,
        link: Link,
        producer: ProducerId,
        slots: &[Slot],
        stride: usize,
    ) -> Option<usize> {
        if let Some(index) = self.entries.iter().position(|entry| {
            entry.is_some_and(|entry| {
                entry.client.node == link.binding.peer && entry.client.producer == producer
            })
        }) {
            if !matches!(self.access, NativeAccess::Writers(_)) {
                self.entries[index].as_mut().unwrap().session = Some(link.binding.session);
            }
            return Some(index);
        }
        match self.access {
            NativeAccess::Writers(_) => self.peer(link.binding.peer),
            NativeAccess::Clients { .. } | NativeAccess::TrustedClients { .. } => {
                let index = self.entries.iter().position(Option::is_none).or_else(|| {
                    (0..self.entries.len()).find(|&index| idle(slots, index, stride))
                })?;
                // Assignment is only an arena cache. Canonical epochs, sequences,
                // and retry results remain owned by the partition journal.
                self.entries[index] = Some(Writer {
                    client: ClientAccess {
                        node: link.binding.peer,
                        producer,
                    },
                    session: Some(link.binding.session),
                });
                Some(index)
            }
        }
    }

    /// Existing assignment of this client's writer. Assigns nothing.
    pub(super) fn assigned(&self, node: NodeId, producer: ProducerId) -> Option<usize> {
        self.entries.iter().position(|entry| {
            entry
                .is_some_and(|entry| entry.client.node == node && entry.client.producer == producer)
        })
    }

    /// Existing rejection slot of this client. Assigns nothing.
    pub(super) fn assigned_rejection(&self, node: NodeId, stride: usize) -> Option<usize> {
        let peer = match &self.access {
            NativeAccess::Writers(peers) => peers.iter().position(|peer| peer.node == node),
            NativeAccess::Clients { peers, .. } => peers.iter().position(|&peer| peer == node),
            NativeAccess::TrustedClients { .. } => {
                self.clients.iter().position(|&peer| peer == Some(node))
            }
        }?;
        Some(self.rejection_slot(peer, stride))
    }

    pub(super) fn producer(&self, writer: usize) -> Option<ProducerId> {
        self.entries.get(writer)?.map(|entry| entry.client.producer)
    }

    pub(super) fn reclaim(
        &mut self,
        slots: &[Slot],
        stride: usize,
        current: &mut impl FnMut(NodeId) -> Option<Link>,
    ) {
        if matches!(self.access, NativeAccess::Writers(_)) {
            return;
        }
        for (index, entry) in self.entries.iter_mut().enumerate() {
            let Some(writer) = entry else {
                continue;
            };
            let live = current(writer.client.node)
                .is_some_and(|link| Some(link.binding.session) == writer.session);
            if !live && idle(slots, index, stride) {
                *entry = None;
            }
        }
        for (index, client) in self.clients.iter_mut().enumerate() {
            let Some(node) = *client else {
                continue;
            };
            let rejection = &slots[self.entries.len() * stride + index];
            if current(node).is_none()
                && rejection.pending.is_none()
                && rejection.reply.is_none()
                && !self
                    .entries
                    .iter()
                    .any(|entry| entry.is_some_and(|writer| writer.client.node == node))
            {
                *client = None;
            }
        }
    }
}

fn idle(slots: &[Slot], index: usize, stride: usize) -> bool {
    slots[index * stride..(index + 1) * stride]
        .iter()
        .all(|slot| slot.buffer.is_some() && slot.pending.is_none() && slot.reply.is_none())
}
