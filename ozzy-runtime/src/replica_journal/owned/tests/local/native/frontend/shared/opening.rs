use super::{
    appends::{retry, writer_config},
    *,
};
use crate::replicated::{RecordInput, Writer};
use ozzy_proto::MessageId;

#[tokio::test(flavor = "current_thread")]
async fn automatic_opening_stays_lazy_and_waits_for_durable_state() {
    tokio::time::timeout(Duration::from_secs(10), scenario(false))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn automatic_opening_retries_lost_reply_with_same_operation() {
    tokio::time::timeout(Duration::from_secs(10), scenario(true))
        .await
        .unwrap();
}

#[allow(clippy::too_many_lines)]
async fn scenario(lose_reply: bool) {
    let (mut harness, links, clock, authority) = setup_shared_with_writers(&[]).await;
    let topic = harness.drive(links.topic("orders"), false).await.unwrap();
    let routes = links.routes(topic).unwrap();
    let mut a = Writer::open_shared(&routes, 0, writer_config(40, 0), retry())
        .await
        .unwrap();
    let mut b = Writer::open_shared(&routes, 0, writer_config(30, 0), retry())
        .await
        .unwrap();
    for _ in 0..100 {
        harness.pump(false);
        tokio::task::yield_now().await;
    }
    assert!(harness.openings.is_empty());
    assert!(harness.watch.is_none());
    if lose_reply {
        harness.drop_opened = Some(ProducerId::from_bytes([40; 16]));
    }
    let first = a
        .send(RecordInput::copy_from_slice(
            MessageId::from_bytes([50; 16]),
            b"a",
        ))
        .await
        .unwrap();
    let second = b
        .send(RecordInput::copy_from_slice(
            MessageId::from_bytes([60; 16]),
            b"b",
        ))
        .await
        .unwrap();
    for _ in 0..10000 {
        harness.pump(false);
        if harness.openings.len() == 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(harness.openings.len(), 2);
    assert!(!harness.controller.jobs().is_empty());
    assert!(
        harness.appends.is_empty(),
        "APPEND preceded durable opening"
    );
    assert!(first.try_confirmed().is_none());
    assert!(second.try_confirmed().is_none());
    let healthy = harness.drive(second.confirmed(), true).await.unwrap();
    if lose_reply {
        assert!(harness.drop_opened.is_none());
        assert!(first.try_confirmed().is_none());
        assert!(
            harness
                .appends
                .iter()
                .all(|append| append.0 != ProducerId::from_bytes([40; 16]))
        );
        clock.advance(Duration::from_secs(5)).unwrap();
    }
    let recovered = harness
        .drive_advancing(&clock, first.confirmed(), true)
        .await
        .unwrap();
    assert_eq!(recovered.message_id, MessageId::from_bytes([50; 16]));
    assert_eq!(healthy.message_id, MessageId::from_bytes([60; 16]));
    for receipt in [&healthy, &recovered] {
        assert_eq!(receipt.key.first_sequence, 0);
        assert_eq!(receipt.key.producer_epoch, 1);
        assert_eq!(receipt.policy, Policy::LocalDurable);
    }
    let mut offsets = [healthy.offset, recovered.offset];
    offsets.sort_unstable();
    assert_eq!(offsets, [0, 1]);
    let attempts = harness
        .openings
        .iter()
        .filter(|opening| opening.0 == ProducerId::from_bytes([40; 16]))
        .collect::<Vec<_>>();
    assert_eq!(attempts.len(), if lose_reply { 2 } else { 1 });
    assert!(attempts.iter().all(|opening| opening.1 == attempts[0].1));
    if lose_reply {
        assert_ne!(attempts[0].2, attempts[1].2);
    }
    assert_eq!(
        links.session(authority.primary),
        Some(harness.session.unwrap())
    );
    assert_eq!(links.socket_count(), 2);
    harness.drive(a.close(), true).await.unwrap();
    harness.drive(b.close(), true).await.unwrap();
    harness.drive(links.shutdown(), true).await.unwrap();
    harness.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn refused_opening_waits_for_its_backoff() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut harness, links, clock, _) = setup_shared_with_writers(&[]).await;
        let topic = harness.drive(links.topic("orders"), false).await.unwrap();
        let routes = links.routes(topic).unwrap();
        let mut writer = Writer::open_shared(&routes, 0, writer_config(40, 0), retry())
            .await
            .unwrap();
        harness.defer_openings = true;
        let record = writer
            .send(RecordInput::copy_from_slice(
                MessageId::from_bytes([50; 16]),
                b"a",
            ))
            .await
            .unwrap();
        // The clock stands still, so no backoff can expire. The leader and
        // its view stay the same.
        for _ in 0..2000 {
            harness.pump(true);
            tokio::task::yield_now().await;
        }
        assert_eq!(
            harness.openings.len(),
            1,
            "a refused opening was repeated before its backoff expired"
        );
        // The broker alone knows when it is ready. The writer asks again at
        // the initial 10 ms interval. Doubling would need 250 ms for five
        // more openings.
        let refused_at = clock.now();
        while harness.openings.len() < 6 {
            assert!(clock.now().saturating_sub(refused_at) < Duration::from_millis(120));
            clock
                .advance(clock.now().saturating_add(Duration::from_millis(1)))
                .unwrap();
            for _ in 0..50 {
                harness.pump(true);
                tokio::task::yield_now().await;
            }
        }
        let refused = harness.openings.len();
        harness.defer_openings = false;
        let mut confirmed = pin!(record.confirmed());
        let mut receipt = None;
        for _ in 0..10000 {
            harness.pump(true);
            if let Poll::Ready(value) = futures::poll!(confirmed.as_mut()) {
                receipt = Some(value);
                break;
            }
            clock
                .advance(clock.now().saturating_add(Duration::from_millis(1)))
                .unwrap();
            tokio::task::yield_now().await;
        }
        let receipt = receipt.expect("opening retry did not settle").unwrap();
        assert_eq!(receipt.message_id, MessageId::from_bytes([50; 16]));
        assert_eq!(harness.openings.len(), refused + 1);
        harness.drive(writer.close(), true).await.unwrap();
        harness.drive(links.shutdown(), true).await.unwrap();
        harness.shutdown().await;
    })
    .await
    .unwrap();
}
