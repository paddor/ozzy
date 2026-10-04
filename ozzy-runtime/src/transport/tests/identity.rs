use super::*;
use omq_tokio::{Context, Message};
use std::time::Duration;

#[test]
fn canonical_peer_labels_preserve_every_node_prefix_and_refuse_aliases() {
    for first in 0..=255 {
        let mut id = [42; 16];
        id[0] = first;
        let node = NodeId::from_bytes(id);
        let physical = peer_identity(node);
        assert!(
            socket_options()
                .identity(physical.clone())
                .validate()
                .is_ok()
        );
        assert_eq!(decode_peer_identity(&physical), Some(node));
        assert_eq!(native_peer_identity(physical).unwrap().as_ref(), &id);
        let mut alternate = vec![1];
        alternate.extend_from_slice(&id);
        if first != 0 {
            assert!(decode_peer_identity(&alternate).is_none());
        } else {
            assert!(decode_peer_identity(&id).is_none());
            alternate[0] = 2;
            assert!(decode_peer_identity(&alternate).is_none());
        }
    }
}

#[tokio::test]
async fn zero_leading_peer_nodes_exchange_native_control_and_data_over_inproc_and_tcp() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for tcp in [false, true] {
            let context = Context::new();
            let from = NodeId::from_bytes([0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]);
            let to = NodeId::from_bytes([0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]);
            let socket = |node| {
                context
                    .socket(
                        SocketType::Peer,
                        socket_options()
                            .identity(peer_identity(node))
                            .router_mandatory(true)
                            .linger(Duration::from_millis(5)),
                    )
                    .identity_routing()
                    .unwrap()
            };
            let sender = socket(from);
            let receiver = socket(to);
            let endpoint = if tcp {
                "tcp://127.0.0.1:0".to_owned()
            } else {
                format!("inproc://{}", ozzy_proto::RequestId::new())
            };
            receiver.bind(endpoint.parse().unwrap()).await.unwrap();
            sender
                .connect(receiver.last_bound_endpoint().unwrap())
                .await
                .unwrap();
            sender
                .wait_connected(1, Duration::from_secs(2))
                .await
                .unwrap();
            receiver
                .wait_connected(1, Duration::from_secs(2))
                .await
                .unwrap();
            for body in [
                Message::single("compact"),
                Message::multipart([
                    Bytes::from_static(b"header"),
                    Bytes::from_static(b"metadata"),
                    Bytes::from_static(b"opaque data"),
                ]),
            ] {
                let packet =
                    Message::with_prefix(Bytes::copy_from_slice(to.as_bytes()), body.clone());
                try_send_peer(&sender, packet).unwrap();
                let (identity, body_at_peer) = receiver.recv_from().await.unwrap();
                assert_eq!(decode_peer_identity(&identity), Some(from));
                assert_eq!(body_at_peer, body);
                let reply = Message::with_prefix(
                    Bytes::copy_from_slice(from.as_bytes()),
                    body_at_peer.clone(),
                );
                try_send_peer(&receiver, reply).unwrap();
                let (identity, returned) = sender.recv_from().await.unwrap();
                assert_eq!(decode_peer_identity(&identity), Some(to));
                assert_eq!(returned, body_at_peer);
            }
            sender.into_inner().close().await.unwrap();
            receiver.into_inner().close().await.unwrap();
        }
    })
    .await
    .unwrap();
}
