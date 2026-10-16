//! A PEER client of a broker's admin endpoint.

use bytes::Bytes;
use omq_tokio::{Context, IdentitySocket, Message, MonitorEvent, Options, SocketType};
use ozzy_proto::{
    NodeId,
    admin::{self, Reply, Request},
};
use std::time::Duration;

pub(crate) struct AdminClient {
    socket: IdentitySocket,
    broker: Bytes,
    next: u64,
}

impl AdminClient {
    /// Connect and learn the broker's peer identity from the handshake.
    pub(crate) async fn connect(context: &Context, endpoint: &str) -> Self {
        let socket = context.socket(SocketType::Peer, Options::default());
        let mut monitor = socket.monitor();
        socket.connect(endpoint).await.unwrap();
        let broker = loop {
            if let MonitorEvent::HandshakeSucceeded { peer, .. } = monitor.recv().await.unwrap() {
                break peer.peer_identity.unwrap();
            }
        };
        Self {
            socket: socket.identity_routing().unwrap(),
            broker,
            next: 0,
        }
    }

    /// The broker the endpoint announced.
    pub(crate) fn broker(&self) -> Option<NodeId> {
        ozzy_runtime::transport::decode_peer_identity(&self.broker)
    }

    pub(crate) async fn ask(&mut self, request: Request) -> Reply {
        self.next += 1;
        let bytes = admin::encode_request(self.next, request);
        self.ask_raw(bytes).await
    }

    /// Send `request` as is; its ID must be the next one.
    pub(crate) async fn ask_raw(&mut self, request: Vec<u8>) -> Reply {
        let id = admin::request_id(&request).unwrap();
        self.next = id;
        self.socket
            .send_to(&self.broker, Message::from(request))
            .await
            .unwrap();
        let (from, reply) = tokio::time::timeout(Duration::from_secs(5), self.socket.recv_from())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(from.as_ref(), self.broker.as_ref());
        let (answered, reply) = admin::decode_reply(&reply.iter().next().unwrap()).unwrap();
        assert_eq!(answered, id);
        reply
    }

    pub(crate) async fn close(self) {
        self.socket.into_inner().close().await.unwrap();
    }
}
