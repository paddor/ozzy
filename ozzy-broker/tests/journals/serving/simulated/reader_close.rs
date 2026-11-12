//! Full subscription release after an earlier replay cancellation timed out.

use super::*;
use ozzy_broker::ServingContext;
use ozzy_proto::{data::Authority, reader};
use ozzy_runtime::profiling::{Event, totals};

#[tokio::test(flavor = "current_thread")]
async fn reader_close_releases_the_live_grant_after_replay_cancel_timeout() {
    tokio::time::timeout(Duration::from_secs(10), run(true))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn reader_close_still_fails_when_full_subscription_release_cannot_complete() {
    tokio::time::timeout(Duration::from_secs(10), run(false))
        .await
        .unwrap();
}

#[expect(
    clippy::too_many_lines,
    reason = "one ordered cancellation and release fault schedule"
)]
async fn run(heal: bool) {
    let runtime = WriterRuntime::new().unwrap();
    let context = ServingContext::from(runtime.context().clone());
    let root = std::path::PathBuf::from(format!("/ozzy-reader-close-{}", Uuid::now_v7()));
    let deployment = deployment(&root, DeploymentMode::Three, 1);
    let checked = deployment[0].0.clone();
    let (brokers, _, images) = start_images(context.clone(), deployment, true).await;
    let producer = role_links(&runtime, &checked, handshake::PRODUCER).await;
    let clock = SdkClock::manual();
    let consumer_node = NodeId::from_bytes(*Uuid::now_v7().as_bytes());
    let consumer = role_links_with_config(
        &runtime,
        &checked,
        handshake::CONSUMER,
        limits(),
        |config| {
            config.local = consumer_node;
            config.clock = clock.clone();
        },
    )
    .await;
    let mut writer = live_many(
        &brokers,
        "open close-regression writer",
        SharedTopicWriter::open(
            &producer,
            "orders",
            SharedTopicWriterConfig::new(limits()),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    let first = super::super::live::publish_record(&brokers, &mut writer, 0).await;
    let mut reader = live_many(
        &brokers,
        "open close-regression reader",
        TopicReader::open(
            consumer.clone(),
            "orders",
            TopicReaderConfig {
                refresh: Duration::from_secs(60),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    let record = live_many(&brokers, "accept replay subscription", reader.next())
        .await
        .unwrap();
    assert_eq!(record.offset.get(), 0);
    assert_eq!(reader.stats().replayed_records, 1);
    let routes = consumer.routes(writer.metadata().clone()).await.unwrap();
    let route = routes.route(0).unwrap().unwrap();
    let leader = route.leader.unwrap();
    let source = reader::Source::Group {
        authority: Authority {
            group_id: route.group,
            config_epoch: route.config_epoch,
            view: route.view,
        },
        partition: route.partition,
        owner_epoch: first.record.owner_epoch,
    };
    let witness = super::super::live::subscriber(
        &runtime,
        &writer.metadata().broker(leader).unwrap().reader_pub,
        source,
        4,
        Some(consumer_node),
    )
    .await;
    // Existing live delivery keeps working while this consumer's PEER
    // cancellation is lost. Do not poll the reader after its deadline.
    context.cut_peer(consumer_node, true);
    super::super::live::publish_record(&brokers, &mut writer, 1).await;
    let record = live_many(
        &brokers,
        "live record with replay cancel cut",
        reader.next(),
    )
    .await
    .unwrap();
    assert_eq!(record.offset.get(), 1);
    assert_eq!(reader.stats().live_records, 1);
    // Observe the witness's actual topic admission before checking revocation.
    // Extra publications remain unread by the SDK reader being closed.
    super::super::live::settle_publications(&brokers, &mut writer, &witness, leader, source, 1, 2)
        .await;
    let checkpoint = reader.checkpoint();
    clock.advance(Duration::from_secs(6)).unwrap();
    if heal {
        context.cut_peer(consumer_node, false);
    }
    let revoked = totals().get(Event::ReaderSubscriptionRevoked);
    let closing = reader.close();
    let result = if heal {
        live_many(&brokers, "release after replay cancel timeout", closing).await
    } else {
        live_many(&brokers, "full release still needs a reply", async {
            let mut closing = std::pin::pin!(closing);
            loop {
                if let std::task::Poll::Ready(result) = futures::poll!(closing.as_mut()) {
                    break result;
                }
                if let Some(deadline) = clock.next_deadline() {
                    clock.advance(deadline).unwrap();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
    };
    assert_eq!(result.is_ok(), heal, "full release outcome: {result:?}");
    assert_eq!(reader.checkpoint(), checkpoint);
    if heal {
        live_many(&brokers, "last live grant was removed", async {
            while totals().get(Event::ReaderSubscriptionRevoked) == revoked {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
    context.cut_peer(consumer_node, false);
    witness.close().await.unwrap();
    live_many(&brokers, "close regression writer", writer.close())
        .await
        .unwrap();
    consumer.shutdown().await.unwrap();
    producer.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    for image in images {
        image.await.unwrap();
    }
}
