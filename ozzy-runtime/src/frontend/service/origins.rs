//! The stores behind each broker peer.
//!
//! A broker names the origin of its stores in every HELLO and WELCOME. The
//! origin stays while the broker serves those stores and changes when they
//! were made anew. A peer that comes back with another origin lost what it
//! had stored and acknowledged, so it gets no link: without one it neither
//! votes nor counts toward a quorum. The first origin a peer names is kept
//! before its link exists, so a later run of this broker refuses a changed
//! one too.
//!
//! A broker that replaces a lost one says so beside its new origin. It
//! holds every replicated log as a nonvoting copy until the copy has the
//! log of the other two, so it gets its link, and its new origin is kept
//! in place of the lost one.

use super::{Kind, NodeId, ReceiveError, Service};
use std::collections::BTreeMap;

/// Keeps a peer's origin for later runs. `false` when it could not.
type Keep = Box<dyn FnMut(NodeId, u128) -> bool + Send>;

pub(super) struct Origins {
    known: BTreeMap<NodeId, u128>,
    keep: Keep,
    /// What each refused peer last named, so one refusal is logged once.
    refused: BTreeMap<NodeId, Option<u128>>,
}

impl std::fmt::Debug for Origins {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Origins")
            .field("known", &self.known)
            .field("refused", &self.refused)
            .finish_non_exhaustive()
    }
}

impl Service {
    /// Name `local` as the origin of this broker's stores, and link only
    /// broker peers that name the origin `known` holds for them. `keep`
    /// stores the origin of a peer without one; it must be durable before
    /// it returns `true`, and it may block: it runs once per peer in the
    /// life of this broker's stores. Install before negotiation; a service
    /// without it names no origin and checks none.
    pub fn install_origins(
        &mut self,
        local: u128,
        known: impl IntoIterator<Item = (NodeId, u128)>,
        keep: impl FnMut(NodeId, u128) -> bool + Send + 'static,
    ) {
        self.sessions.set_origin(local);
        self.origins = Some(Origins {
            known: known.into_iter().collect(),
            keep: Box::new(keep),
            refused: BTreeMap::new(),
        });
    }

    /// Say in every HELLO and WELCOME that the local origin replaces one
    /// that was lost. Only a broker that holds every replicated log as a
    /// nonvoting copy until it has the log of the other two may say so.
    pub const fn announce_replacement(&mut self) {
        self.sessions.set_replacement();
    }

    /// Refuse the handshake of a broker whose stores are not the ones it
    /// had, or cannot be told to be.
    pub(super) fn check_origin(
        &mut self,
        peer: NodeId,
        kind: Kind,
        (named, replacement): (Option<u128>, bool),
    ) -> Result<(), ReceiveError> {
        let Some(origins) = self.origins.as_mut().filter(|_| kind == Kind::Broker) else {
            return Ok(());
        };
        let known = origins.known.get(&peer).copied();
        let admitted = match (known, named) {
            (Some(known), Some(named)) if known == named => true,
            (Some(_), Some(_)) if !replacement => false,
            (_, Some(named)) => {
                let kept = (origins.keep)(peer, named);
                if kept {
                    origins.known.insert(peer, named);
                }
                kept
            }
            (_, None) => false,
        };
        if admitted {
            origins.refused.remove(&peer);
            return Ok(());
        }
        if origins.refused.insert(peer, named) != Some(named) {
            let reason = match (known, named) {
                (_, None) => "names no store origin",
                (Some(_), _) if !replacement => {
                    "serves other stores than before; start it as a replacement"
                }
                _ => "has a store origin that could not be kept",
            };
            crate::profiling::log(
                crate::profiling::Level::Warn,
                &format!("refused broker {peer:?}: it {reason}"),
            );
        }
        Err(ReceiveError::Origin)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{remote, setup};
    use super::*;
    use bytes::Bytes;
    use omq_tokio::Message;
    use ozzy_proto::handshake;
    use std::sync::{Arc, Mutex};

    const BROKER: [u8; 16] = [8; 16];

    /// One HELLO of a fresh run of `id`, naming `origin`.
    fn hello(service: &Service, id: u8, roles: u32, origin: Option<u128>) -> Message {
        let mut sessions = remote(id, roles);
        if let Some(origin) = origin {
            sessions.set_origin(origin);
        }
        Message::with_prefix(
            Bytes::copy_from_slice(&[id; 16]),
            Message::multipart(sessions.start(service.dispatcher.local).unwrap()),
        )
    }

    fn broker(service: &Service, origin: Option<u128>) -> Message {
        hello(service, 8, handshake::OWNER | 8, origin)
    }

    #[test]
    fn a_broker_links_only_under_the_origin_first_kept_for_it() {
        let (mut service, _lanes, _) = setup();
        let kept = Arc::new(Mutex::new(Vec::new()));
        let sink = kept.clone();
        service.install_origins(5, [], move |peer, origin| {
            sink.lock().unwrap().push((peer, origin));
            true
        });
        let peer = NodeId::from_bytes(BROKER);
        let message = broker(&service, Some(70));
        service.receive(message, 4096).unwrap();
        assert_eq!(*kept.lock().unwrap(), [(peer, 70)]);

        // The same stores again.
        let message = broker(&service, Some(70));
        service.receive(message, 4096).unwrap();
        let second = service.links.get(peer).unwrap().binding;
        assert_eq!(kept.lock().unwrap().len(), 1, "kept once");

        // A run on other stores, and one that names none.
        for origin in [Some(71), None] {
            let message = broker(&service, origin);
            assert!(matches!(
                service.receive(message, 4096),
                Err(ReceiveError::Origin)
            ));
            assert_eq!(service.links.get(peer).unwrap().binding, second);
        }
        assert_eq!(kept.lock().unwrap().len(), 1);

        // A client names no origin and needs none.
        let message = hello(&service, 1, handshake::PRODUCER, None);
        service.receive(message, 4096).unwrap();
        assert!(service.links.get(NodeId::from_bytes([1; 16])).is_some());
    }

    #[test]
    fn a_known_origin_holds_and_one_that_was_not_kept_grants_nothing() {
        let (mut service, _lanes, _) = setup();
        let peer = NodeId::from_bytes(BROKER);
        let durable = Arc::new(Mutex::new(false));
        let disk = durable.clone();
        service.install_origins(5, [], move |_, _| *disk.lock().unwrap());
        let message = broker(&service, Some(70));
        assert!(matches!(
            service.receive(message, 4096),
            Err(ReceiveError::Origin)
        ));
        assert!(service.links.get(peer).is_none());
        *durable.lock().unwrap() = true;
        let message = broker(&service, Some(71));
        service.receive(message, 4096).unwrap();
        assert!(service.links.get(peer).is_some(), "71 came first to disk");

        let (mut service, _lanes, _) = setup();
        service.install_origins(5, [(peer, 70)], |_, _| unreachable!());
        let message = broker(&service, Some(71));
        assert!(matches!(
            service.receive(message, 4096),
            Err(ReceiveError::Origin)
        ));
        let message = broker(&service, Some(70));
        service.receive(message, 4096).unwrap();
        assert!(service.links.get(peer).is_some());
    }

    #[test]
    fn a_replacement_links_under_its_new_origin_which_is_kept() {
        let (mut service, _lanes, _) = setup();
        let peer = NodeId::from_bytes(BROKER);
        let kept = Arc::new(Mutex::new(Vec::new()));
        let sink = kept.clone();
        service.install_origins(5, [(peer, 70)], move |peer, origin| {
            sink.lock().unwrap().push((peer, origin));
            true
        });
        let mut sessions = remote(8, handshake::OWNER | 8);
        sessions.set_origin(71);
        sessions.set_replacement();
        let message = Message::with_prefix(
            Bytes::copy_from_slice(&BROKER),
            Message::multipart(sessions.start(service.dispatcher.local).unwrap()),
        );
        service.receive(message, 4096).unwrap();
        assert!(service.links.get(peer).is_some());
        assert_eq!(*kept.lock().unwrap(), [(peer, 71)]);

        // Later runs on the new stores need not say it again, and the lost
        // stores do not come back.
        let message = broker(&service, Some(71));
        service.receive(message, 4096).unwrap();
        let message = broker(&service, Some(70));
        assert!(matches!(
            service.receive(message, 4096),
            Err(ReceiveError::Origin)
        ));
        assert_eq!(kept.lock().unwrap().len(), 1);
    }
}
