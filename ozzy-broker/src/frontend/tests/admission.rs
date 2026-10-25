//! Publication admission: which SUBs each PUB serves, and for how long.

use super::*;
use omq_tokio::Message;
use ozzy_runtime::{
    frontend::{ReceiveBuffers, ReceiveStorage},
    profiling::Event,
};
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

fn frame(topic: &[u8]) -> Message {
    Message::multipart([Bytes::copy_from_slice(topic), Bytes::from_static(b"x")])
}

/// Publish under `topic` until `admitted` receives, then once more; no other
/// SUB may receive anything.
async fn serves_only(publisher: &Socket, topic: &[u8], admitted: &Socket, refused: &[Socket]) {
    loop {
        publisher.send(frame(topic)).await.unwrap();
        let wait = tokio::time::timeout(Duration::from_millis(20), admitted.recv());
        if wait.await.is_ok() {
            break;
        }
    }
    publisher.send(frame(topic)).await.unwrap();
    for socket in refused {
        assert!(quiet(socket).await, "a PUB served a SUB it does not admit");
    }
}

/// Whether `socket` receives nothing for a while.
async fn quiet(socket: &Socket) -> bool {
    let received = tokio::time::timeout(Duration::from_millis(50), socket.recv()).await;
    received.is_err()
}

/// Wait until the frontend has counted `event` at least `count` times.
async fn counted(event: Event, count: u64) {
    while ozzy_runtime::profiling::totals().get(event) < count {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn publication_sockets_serve_only_granted_readers_and_configured_followers() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let totals = ozzy_runtime::profiling::totals();
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
        let grants = admission.readers.shard(0).unwrap();
        grants.set(topic, BTreeSet::from([reader]));
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
            move |context| async move {
                let follower_pub = context.follower_pub.clone().unwrap();
                sockets
                    .send((context.reader_pub.clone(), follower_pub))
                    .unwrap();
                let (service, _receivers) = super::sessions::serving_service(true);
                let buffers = ReceiveBuffers::new(
                    ozzy_proto::data::DataLimits::default().envelope,
                    ReceiveStorage::Inproc {
                        payload_backing_bytes: 4096,
                    },
                )
                .unwrap();
                context
                    .serve(
                        service,
                        crate::RemoteBrokers::default(),
                        buffers,
                        Duration::from_millis(10),
                    )
                    .await
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
        let rejected = totals.get(Event::ReaderSubscriptionRejected) + 4;
        counted(Event::ReaderSubscriptionRejected, rejected).await;

        let replicas = endpoints.follower_pub.as_deref().unwrap();
        let following = subscriber(&omq, replicas, Some(follower), &[]).await;
        let strangers = [
            subscriber(&omq, replicas, None, &[]).await,
            subscriber(&omq, replicas, Some(client), &[]).await,
        ];
        serves_only(&follower_pub, &topic[..16], &following, &strangers).await;
        let strange = totals.get(Event::FollowerSubscriptionRejected) + 2;
        counted(Event::FollowerSubscriptionRejected, strange).await;

        // The reader's subscription ends while the topic stays live. Its SUB
        // keeps the topic, so the PUB closes it; admission refuses the replay.
        grants.set(topic, BTreeSet::new());
        let revoked = totals.get(Event::ReaderSubscriptionRevoked) + 1;
        counted(Event::ReaderSubscriptionRevoked, revoked).await;
        counted(Event::ReaderSubscriptionRejected, rejected + 1).await;
        while !quiet(&admitted).await {}
        reader_pub.send(frame(&topic)).await.unwrap();
        assert!(quiet(&admitted).await, "a revoked SUB still receives");
        frontend.shutdown().await.unwrap();
    })
    .await
    .expect("publication admission stalled");
}
