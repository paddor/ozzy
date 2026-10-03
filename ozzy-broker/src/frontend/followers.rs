//! Configured lossy publication and isolated PEER repair transport.

use super::{FrontendContext, StartupError, error};
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use omq_tokio::{Endpoint, IdentitySocket, Message, Socket, SocketType, TrySendError};
use ozzy_proto::{GroupId, NodeId, Opcode};
use std::{collections::BTreeMap, future::Future, pin::Pin};

type PublicationWait =
    Pin<Box<dyn Future<Output = (NodeId, Socket, Result<Message, omq_tokio::Error>)>>>;

/// Keep socket readiness across cancelled dispatcher selects. Rearming only the
/// consumed source preserves `FuturesUnordered`'s ready-source rotation.
pub(super) struct Publications {
    waits: FuturesUnordered<PublicationWait>,
}

impl Publications {
    fn wait(broker: NodeId, socket: Socket) -> PublicationWait {
        Box::pin(async move {
            let message = socket.recv().await;
            (broker, socket, message)
        })
    }

    pub(super) async fn receive(&mut self) -> (NodeId, Result<Message, omq_tokio::Error>) {
        match self.waits.next().await {
            Some((broker, socket, message)) => {
                self.waits.push(Self::wait(broker, socket));
                (broker, message)
            }
            None => std::future::pending().await,
        }
    }
}

/// Immutable broker-local routes. Repair aliases share the independently
/// established broker session, and never establish or replace that session.
#[derive(Clone, Debug, Default)]
pub struct FollowerRoutes {
    /// Configured publication endpoint for each remote broker.
    pub publications: BTreeMap<NodeId, Endpoint>,
    /// Explicit bulk endpoint for repair on each remote broker.
    pub repairs: BTreeMap<NodeId, Endpoint>,
    /// Partition placement on each remote broker, for isolated repair sends.
    pub destinations: BTreeMap<(NodeId, GroupId), u32>,
    /// Physical repair identity mapped to its configured sender and local shard.
    pub incoming: BTreeMap<NodeId, (NodeId, u32)>,
    /// Local partition placement checked before alias dispatch.
    pub local: BTreeMap<GroupId, u32>,
}

impl FollowerRoutes {
    /// Stable transport label only. This does not authenticate a broker or vote.
    pub(crate) fn identity(from: NodeId, to: NodeId, shard: u32) -> NodeId {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(b"ozzy/follower-repair/shard");
        bytes.extend_from_slice(from.as_bytes());
        bytes.extend_from_slice(to.as_bytes());
        bytes.extend_from_slice(&shard.to_be_bytes());
        NodeId::from_bytes(xxhash_rust::xxh3::xxh3_128(&bytes).to_be_bytes())
    }
}

pub(super) struct Followers {
    repair: BTreeMap<(NodeId, u32), IdentitySocket>,
    subscribers: BTreeMap<NodeId, Socket>,
    pub(super) routes: FollowerRoutes,
}

impl Followers {
    pub(super) async fn open(
        frontend: &FrontendContext,
        _brokers: &BTreeMap<NodeId, Endpoint>,
        routes: FollowerRoutes,
    ) -> Result<Self, StartupError> {
        let mut transport = Self {
            repair: BTreeMap::new(),
            subscribers: BTreeMap::new(),
            routes,
        };
        let options = ozzy_runtime::transport::socket_options()
            .router_mandatory(true)
            .send_hwm(frontend.limits.send_messages)
            .recv_hwm(frontend.limits.receive_messages)
            .max_message_size(frontend.limits.message_bytes)
            .linger(frontend.limits.close_linger);
        let result = async {
            for (&(broker, _), &shard) in &transport.routes.destinations {
                if transport.repair.contains_key(&(broker, shard)) {
                    continue;
                }
                let socket = frontend
                    .context
                    .socket(
                        SocketType::Peer,
                        options.clone().identity(Bytes::copy_from_slice(
                            FollowerRoutes::identity(frontend.local, broker, shard).as_bytes(),
                        )),
                    )
                    .identity_routing()?;
                transport.repair.insert((broker, shard), socket.clone());
                socket
                    .connect(
                        transport
                            .routes
                            .repairs
                            .get(&broker)
                            .ok_or_else(|| {
                                omq_tokio::Error::Config("unknown repair broker".into())
                            })?
                            .clone(),
                    )
                    .await?;
            }
            for (&broker, endpoint) in &transport.routes.publications {
                let socket = frontend.context.socket(SocketType::Sub, options.clone());
                transport.subscribers.insert(broker, socket.clone());
                socket.subscribe(Bytes::new()).await?;
                socket.connect(endpoint.clone()).await?;
            }
            Ok::<(), omq_tokio::Error>(())
        }
        .await;
        if let Err(reason) = result {
            let _ = transport.close().await;
            return Err(error(reason.to_string()));
        }
        Ok(transport)
    }

    fn destination(&self, message: &Message) -> Option<(NodeId, u32)> {
        if message.len() != 4 {
            return None;
        }
        let frames =
            std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).expect("four frames"));
        let packet = ozzy_proto::decode_packet(
            &frames,
            ozzy_proto::EnvelopeLimits {
                max_metadata_bytes: usize::MAX,
                max_payload_bytes: usize::MAX,
            },
        )
        .ok()?;
        if !matches!(packet.envelope.opcode, Opcode::PrepareFlow | Opcode::Ops) {
            return None;
        }
        let peer = NodeId::from_bytes(message.part_slice(0)?.try_into().ok()?);
        let scope = ozzy_replication::wire::route(
            packet,
            ozzy_proto::EnvelopeLimits {
                max_metadata_bytes: usize::MAX,
                max_payload_bytes: usize::MAX,
            },
        )
        .ok()?;
        Some((
            peer,
            *self.routes.destinations.get(&(peer, scope.group_id))?,
        ))
    }

    pub(super) fn send(
        &self,
        control: &IdentitySocket,
        message: Message,
    ) -> Result<(), TrySendError> {
        let socket = self
            .destination(&message)
            .and_then(|key| self.repair.get(&key))
            .unwrap_or(control);
        ozzy_runtime::transport::try_send_peer(socket, message)
    }

    pub(super) fn wait(
        &self,
        control: &IdentitySocket,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = ()>>> {
        let socket = self
            .destination(&message)
            .and_then(|key| self.repair.get(&key))
            .unwrap_or(control)
            .clone();
        Box::pin(async move { socket.wait_send_progress_for(&message).await })
    }

    pub(super) fn publications(&self) -> Publications {
        Publications {
            waits: self
                .subscribers
                .iter()
                .map(|(&broker, socket)| Publications::wait(broker, socket.clone()))
                .collect(),
        }
    }

    pub(super) async fn close(self) -> Result<(), omq_tokio::Error> {
        let mut result = Ok(());
        for socket in self.repair.into_values() {
            let closed = socket.into_inner().close().await;
            result = result.and(closed);
        }
        for socket in self.subscribers.into_values() {
            let closed = socket.close().await;
            result = result.and(closed);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn ready_publishers_share_receive_turns() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let context = omq_tokio::Context::new();
            let mut subscribers = BTreeMap::new();
            let mut publishers = Vec::new();
            for id in [1, 2] {
                let endpoint: Endpoint =
                    format!("inproc://follower-fair-{}", ozzy_proto::RequestId::new())
                        .parse()
                        .unwrap();
                let publisher =
                    context.socket(SocketType::Pub, ozzy_runtime::transport::socket_options());
                let subscriber =
                    context.socket(SocketType::Sub, ozzy_runtime::transport::socket_options());
                publisher.bind(endpoint.clone()).await.unwrap();
                subscriber.subscribe(Bytes::new()).await.unwrap();
                subscriber.connect(endpoint).await.unwrap();
                publisher
                    .wait_subscribed(1, Duration::from_secs(2))
                    .await
                    .unwrap();
                subscribers.insert(NodeId::from_bytes([id; 16]), subscriber);
                publishers.push(publisher);
            }
            let followers = Followers {
                repair: BTreeMap::new(),
                subscribers,
                routes: FollowerRoutes::default(),
            };
            let mut inputs = followers.publications();
            for _ in 0..8 {
                assert!(futures::FutureExt::now_or_never(inputs.receive()).is_none());
            }
            for publisher in &publishers {
                for _ in 0..32 {
                    publisher.send(Message::single("ready")).await.unwrap();
                }
            }
            // Let both inproc receivers fill before testing continuously ready
            // sources. A newly constructed selector always favors the first.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut received = Vec::new();
            for _ in 0..16 {
                let (broker, message) = inputs.receive().await;
                assert_eq!(message.unwrap().part_slice(0), Some(b"ready".as_slice()));
                received.push(broker);
            }
            for pair in received.as_chunks::<2>().0 {
                assert_ne!(pair[0], pair[1], "one ready publisher starved the other");
            }
            drop(inputs);
            followers.close().await.unwrap();
            for publisher in publishers {
                publisher.close().await.unwrap();
            }
        })
        .await
        .unwrap();
    }
}
