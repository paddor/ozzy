use super::*;
use omq_tokio::{Context, Options, SocketType};
use std::collections::BTreeSet;

const TOPIC: [u8; 32] = [7; 32];
const NONE: [u64; 0] = [];

fn node(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 16])
}

/// Grants of one shard admitting `readers` to the live `TOPIC`.
fn grants(readers: &[NodeId]) -> ReaderGrants {
    let grants = ReaderGrants::new([0]);
    grant(&grants, readers);
    grants
}

fn grant(grants: &ReaderGrants, readers: &[NodeId]) {
    let readers = readers.iter().copied().collect::<BTreeSet<_>>();
    grants.shard(0).unwrap().set(TOPIC, readers);
}

fn topic() -> Bytes {
    Bytes::copy_from_slice(&TOPIC)
}

fn subscribe(held: &mut Held, connection_id: u64) {
    held.observe(&MonitorEvent::SubscribeReceived {
        connection_id,
        prefix: topic(),
    });
}

fn cancel(held: &mut Held, connection_id: u64) {
    held.observe(&MonitorEvent::UnsubscribeReceived {
        connection_id,
        prefix: topic(),
    });
}

/// Connection 1 of `reader` holds `TOPIC`; connection 2 of another node
/// holds nothing.
fn holding(reader: NodeId) -> Held {
    let mut held = Held::default();
    held.rebuild([
        (1, Some(reader), Vec::new()),
        (2, Some(node(2)), Vec::new()),
    ]);
    subscribe(&mut held, 1);
    held
}

#[test]
fn a_topic_held_after_its_subscription_ended_costs_the_connection_at_the_second_check() {
    let reader = node(1);
    let grants = grants(&[reader]);
    let mut held = holding(reader);
    assert_eq!(held.check(&grants), NONE);
    grant(&grants, &[]);
    assert_eq!(held.check(&grants), NONE, "no grace to cancel");
    assert_eq!(held.check(&grants), [1]);
    assert_eq!(held.check(&grants), NONE);
    assert_eq!(
        held.connections.keys().copied().collect::<Vec<_>>(),
        [2],
        "a closed connection is forgotten; one holding nothing stays"
    );
}

#[test]
fn a_cancel_or_a_new_subscription_within_the_grace_keeps_the_connection() {
    let reader = node(1);
    let grants = grants(&[]);
    let mut held = holding(reader);
    subscribe(&mut held, 1);
    assert_eq!(held.check(&grants), NONE);
    cancel(&mut held, 1);
    assert_eq!(held.check(&grants), [1], "PUB counts repeated SUBSCRIBEs");

    let mut held = holding(reader);
    assert_eq!(held.check(&grants), NONE);
    cancel(&mut held, 1);
    assert_eq!(held.check(&grants), NONE);

    let mut held = holding(reader);
    assert_eq!(held.check(&grants), NONE);
    grant(&grants, &[reader]);
    assert_eq!(held.check(&grants), NONE);
    grant(&grants, &[]);
    assert_eq!(held.check(&grants), NONE, "the grace starts over");
}

#[test]
fn a_topic_whose_incarnation_ended_is_harmless_to_hold() {
    let grants = grants(&[]);
    grants.shard(0).unwrap().end(TOPIC);
    let mut held = holding(node(1));
    assert_eq!(held.check(&grants), NONE);
    assert_eq!(held.check(&grants), NONE);
}

#[test]
fn a_rebuild_counts_repeats_and_keeps_the_grace() {
    let reader = node(1);
    let grants = grants(&[]);
    let mut held = holding(reader);
    assert_eq!(held.check(&grants), NONE);
    held.rebuild([
        (1, Some(reader), vec![topic(), topic()]),
        (3, Some(reader), vec![topic()]),
        (4, None, vec![topic()]),
    ]);
    assert_eq!(
        held.connections.keys().copied().collect::<Vec<_>>(),
        [1, 3, 4]
    );
    cancel(&mut held, 1);
    assert_eq!(held.check(&grants), [1], "a lag does not restart the grace");
    assert_eq!(held.check(&grants), [3, 4]);
}

/// Each connection's topics with their counts.
fn table(held: &Held) -> Vec<Vec<(Bytes, u32)>> {
    (held.connections.values())
        .map(|connection| {
            (connection.topics.iter())
                .map(|(topic, count)| (topic.clone(), *count))
                .collect()
        })
        .collect()
}

/// Watch until the table matches `topics`, each held once by one connection.
async fn until(subscribers: &mut Subscribers<'_>, topics: &[[u8; 32]]) {
    let expected = vec![
        (topics.iter())
            .map(|topic| (Bytes::copy_from_slice(topic), 1))
            .collect::<Vec<_>>(),
    ];
    while table(&subscribers.held) != expected {
        let watched = subscribers.next().await;
        subscribers.turn(watched).await.unwrap();
    }
}

#[tokio::test]
async fn a_lagged_monitor_rebuilds_the_table_and_resumes_after_the_snapshot() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let reader = node(1);
        let grants = grants(&[reader]);
        let (other, third) = ([8; 32], [9; 32]);
        let omq = Context::new();
        let options = Options::default().linger(Duration::ZERO);
        let publisher = omq.socket(SocketType::Pub, options.clone().monitor_capacity(2));
        let monitors = Monitors::new(&publisher, None);
        publisher.bind("inproc://subscribers-lag").await.unwrap();
        let identity = ozzy_runtime::transport::peer_identity(reader);
        let subscriber = omq.socket(SocketType::Sub, options.identity(identity));
        for prefix in [TOPIC, other, third] {
            let prefix = Bytes::copy_from_slice(&prefix);
            subscriber.subscribe(prefix).await.unwrap();
        }
        subscriber
            .connect("inproc://subscribers-lag")
            .await
            .unwrap();
        publisher
            .wait_subscribed(3, Duration::from_secs(5))
            .await
            .unwrap();

        let lags = profiling::totals().get(Event::ReaderSubscriptionLag);
        let mut subscribers = Subscribers::new(&publisher, &grants, monitors);
        until(&mut subscribers, &[TOPIC, other, third]).await;
        assert!(profiling::totals().get(Event::ReaderSubscriptionLag) > lags);
        let nodes: Vec<_> = (subscribers.held.connections.values())
            .map(|connection| connection.node)
            .collect();
        assert_eq!(nodes, [Some(reader)]);

        let cancel = Bytes::copy_from_slice(&other);
        subscriber.unsubscribe(cancel).await.unwrap();
        until(&mut subscribers, &[TOPIC, third]).await;
    })
    .await
    .expect("rebuilding a lagged subscription table stalled");
}
