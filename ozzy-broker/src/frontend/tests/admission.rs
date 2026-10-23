//! Publication admission: which SUBs each PUB serves.

use super::*;
use omq_tokio::Message;
use std::collections::BTreeSet;

/// A SUB of `prefix` claiming `node`, or no identity, connected to `endpoint`.
async fn subscriber(
    context: &Context,
    endpoint: &str,
    node: Option<NodeId>,
    prefix: &[u8],
) -> Socket {
    let mut options = omq_tokio::Options::default().linger(Duration::ZERO);
    if let Some(node) = node {
        options = options.identity(ozzy_runtime::transport::peer_identity(node));
    }
    let socket = context.socket(SocketType::Sub, options);
    socket
        .subscribe(Bytes::copy_from_slice(prefix))
        .await
        .unwrap();
    socket.connect(endpoint).await.unwrap();
    socket
        .wait_connected(1, Duration::from_secs(5))
        .await
        .unwrap();
    socket
}

/// Publish under `topic` until `admitted` receives, then once more; no other
/// SUB may receive anything.
async fn serves_only(publisher: &Socket, topic: &[u8], admitted: &Socket, refused: &[Socket]) {
    let frame = || Message::multipart([Bytes::copy_from_slice(topic), Bytes::from_static(b"x")]);
    loop {
        publisher.send(frame()).await.unwrap();
        let wait = tokio::time::timeout(Duration::from_millis(20), admitted.recv());
        if wait.await.is_ok() {
            break;
        }
    }
    publisher.send(frame()).await.unwrap();
    for socket in refused {
        let received = tokio::time::timeout(Duration::from_millis(50), socket.recv()).await;
        assert!(received.is_err(), "a PUB served a SUB it does not admit");
    }
}

#[tokio::test]
async fn publication_sockets_serve_only_granted_readers_and_configured_followers() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let omq = Context::new();
        let endpoints = endpoints(true);
        let (reader, follower, client, other) = (
            NodeId::from_bytes([1; 16]),
            NodeId::from_bytes([2; 16]),
            NodeId::from_bytes([3; 16]),
            NodeId::from_bytes([4; 16]),
        );
        let topic = [7; 32];
        let admission = PublicationAdmission {
            readers: ozzy_runtime::frontend::ReaderGrants::new([0]),
            followers: BTreeSet::from([follower]),
        };
        admission
            .readers
            .shard(0)
            .unwrap()
            .set(topic, BTreeSet::from([reader]));
        let plan = plan();
        let (sockets, published) = oneshot::channel();
        let frontend = Frontend::start_with_context(
            FrontendPlan {
                broker: &plan,
                local: local(),
                endpoints: &endpoints,
                limits: limits(),
                admission: &admission,
            },
            omq.clone(),
            move |mut context| async move {
                context.ready()?;
                let follower_pub = context.follower_pub.clone().unwrap();
                sockets
                    .send((context.reader_pub.clone(), follower_pub))
                    .unwrap();
                context.shutdown.closed().await;
                Ok(())
            },
        )
        .await
        .unwrap();
        let (reader_pub, follower_pub) = published.await.unwrap();

        let live = &endpoints.reader_pub;
        let admitted = subscriber(&omq, live, Some(reader), &topic).await;
        let refused = [
            subscriber(&omq, live, None, &topic).await,
            subscriber(&omq, live, Some(other), &topic).await,
            subscriber(&omq, live, Some(reader), &topic[..16]).await,
            subscriber(&omq, live, Some(reader), &[]).await,
        ];
        serves_only(&reader_pub, &topic, &admitted, &refused).await;

        let replicas = endpoints.follower_pub.as_deref().unwrap();
        let admitted = subscriber(&omq, replicas, Some(follower), &[]).await;
        let refused = [
            subscriber(&omq, replicas, None, &[]).await,
            subscriber(&omq, replicas, Some(client), &[]).await,
        ];
        serves_only(&follower_pub, &topic[..16], &admitted, &refused).await;
        frontend.shutdown().await.unwrap();
    })
    .await
    .expect("publication admission stalled");
}
