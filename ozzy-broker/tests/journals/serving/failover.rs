//! Actual shared SDKs survive broker loss and need the intact restarted broker.

use super::*;
use ozzy_runtime::replicated::TopicCheckpoint;

pub(super) async fn check(
    runtime: &WriterRuntime,
    brokers: &mut Vec<Broker>,
    restart: (CheckedConfig, BrokerIdentity),
    sdk: &BrokerLinks,
    writer: &mut SharedTopicWriter,
    expected: &mut [live::History],
) {
    let sockets = sdk.socket_count();
    let mut positions = expected.iter().map(Vec::len).collect::<Vec<_>>();
    let checkpoint = TopicCheckpoint {
        topic: writer.metadata().id(),
        positions: positions
            .iter()
            .enumerate()
            .map(|(n, &next)| (n as u32, ozzy_proto::Offset::new(next as u64)))
            .collect(),
    };
    let mut reader = live_many(
        brokers,
        "open reader before broker loss",
        TopicReader::open(
            sdk.clone(),
            "orders",
            TopicReaderConfig {
                checkpoint: Some(checkpoint),
                ..TopicReaderConfig::default()
            },
        ),
    )
    .await
    .unwrap();
    // Start subscription setup before loss; canceling its observer retains the
    // cursor. Leaders rotate across partitions, so broker 0 leads only some.
    let mut opening = Box::pin(reader.next());
    assert!(futures::poll!(opening.as_mut()).is_pending());
    drop(opening);
    let first = brokers.remove(0);
    first.shutdown().await.unwrap();
    drop(first);
    wave(
        brokers,
        sdk,
        writer,
        &mut reader,
        expected,
        &mut positions,
        5,
    )
    .await;

    let restarted =
        Broker::start_trusted_with_context(restart.0, restart.1, runtime.context().clone())
            .await
            .unwrap();
    brokers.push(restarted);
    // Only broker 2 and the restarted broker 0 remain. Confirmation now needs
    // broker 0 to recover the missed history and join the current authority.
    let second = brokers.remove(0);
    second.shutdown().await.unwrap();
    drop(second);
    wave(
        brokers,
        sdk,
        writer,
        &mut reader,
        expected,
        &mut positions,
        6,
    )
    .await;
    assert_eq!(positions, expected.iter().map(Vec::len).collect::<Vec<_>>());
    assert_eq!(sdk.socket_count(), sockets);
    live_many(brokers, "close reader after broker restart", reader.close())
        .await
        .unwrap();
    read_topic(brokers, sdk, expected).await;
}

async fn wave(
    brokers: &[Broker],
    sdk: &BrokerLinks,
    writer: &mut SharedTopicWriter,
    reader: &mut TopicReader,
    expected: &mut [live::History],
    positions: &mut [usize],
    wave: usize,
) {
    let records = live::inputs(writer.metadata(), expected, wave, 4);
    let writing = live::write(brokers, writer, records);
    let reading = live::read(brokers, reader, expected, positions);
    futures::join!(writing, reading);
    assert_eq!(sdk.socket_count(), 5, "two PEER and three SUB sockets");
}
