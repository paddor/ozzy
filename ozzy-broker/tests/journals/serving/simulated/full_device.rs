//! A device without room for another segment: the broker keeps serving,
//! records stop before the active segment's tail, and writes resume once
//! space returns.

use super::retention::selected;
use super::*;
use ozzy_runtime::replicated::ReaderStart;
use ozzy_sim::broker::Control;
use std::path::PathBuf;

/// The fixture topic's `segment_bytes`.
const SEGMENT: usize = 1024 * 1024;

/// Selected segment count and the active segment's length.
fn active(image: &Image, directory: &Path) -> (usize, usize) {
    let manifest = selected(image, directory);
    let active = manifest.segments.last().unwrap();
    assert_eq!(active.file_generation, 0, "a rolled segment");
    let path = directory.join(format!("segments/{}.log", active.segment_id));
    (
        manifest.segments.len(),
        image.bytes(&path, false).unwrap().len(),
    )
}

/// Poll the device image until `done` holds.
async fn until(
    brokers: &[Broker],
    control: &Control,
    stage: &str,
    mut done: impl FnMut(&Image) -> bool,
) {
    live_many(brokers, stage, async {
        while !done(&control.image().await) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

fn record(number: u128) -> RecordInput {
    RecordInput::single(
        MessageId::from_bytes(number.to_be_bytes()),
        bytes::Bytes::from(vec![number as u8; 512]),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn single_durable_full_device_holds_records_until_space_returns() {
    let runtime = WriterRuntime::new().unwrap();
    let root = PathBuf::from(format!("/ozzy-full-device-{}", Uuid::now_v7()));
    let deployment = deployment(&root, DeploymentMode::Single, 1);
    let directory = deployment[0].0.plan.partitions[0].directory.clone();
    let spare = directory.join("segments/spare.log");
    let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
    let consumer = role_links(&runtime, &deployment[0].0, handshake::CONSUMER).await;
    let (brokers, controls) = Box::pin(start_controlled(&runtime, deployment, false)).await;
    let mut writer = live_many(
        &brokers,
        "open writer",
        SharedTopicWriter::open(
            &producer,
            "orders",
            SharedTopicWriterConfig::new(limits()),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    until(&brokers, &controls[0], "allocate the spare", |image| {
        image.exists(&spare, false)
    })
    .await;
    // Room for metadata, none for another segment.
    controls[0].limit_free_space(Some(64 * 1024));

    // The roll takes the allocated spare. Allocating the next one fails, so
    // records stop before the second segment's last sixteenth: the SDK sends
    // a refused record again after a backoff.
    let mut sent = 0;
    let pending = loop {
        sent += 1;
        let pending = writer.send(record(sent), None).await.unwrap();
        let retried = async {
            while writer.partition_stats(0).unwrap().records <= sent as u64 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::select! {
            result = pending.confirmed() => {
                result.unwrap();
            }
            () = live_many(&brokers, "fill the device", retried) => break pending,
        }
    };
    let image = controls[0].image().await;
    let (segments, length) = active(&image, &directory);
    assert_eq!(segments, 2, "no segment rolls without a spare");
    assert!(!image.exists(&spare, false));
    assert!(
        (SEGMENT - SEGMENT / 16 - 8192..SEGMENT - SEGMENT / 32).contains(&length),
        "records stop at the tail: {length}"
    );

    // Freed space: the spare is allocated again and the held record lands.
    controls[0].limit_free_space(None);
    live_many(&brokers, "confirm the held record", pending.confirmed())
        .await
        .unwrap();
    while active(&controls[0].image().await, &directory).0 < 3 {
        sent += 1;
        let pending = writer.send(record(sent), None).await.unwrap();
        live_many(&brokers, "roll after space returns", pending.confirmed())
            .await
            .unwrap();
    }

    let mut reader = live_many(
        &brokers,
        "open reader",
        TopicReader::open(
            consumer.clone(),
            "orders",
            TopicReaderConfig {
                start: ReaderStart::Earliest,
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    for number in 1..=sent {
        let record = live_many(&brokers, "read every record", reader.next())
            .await
            .unwrap();
        assert_eq!(record.offset.get(), number as u64 - 1);
        assert_eq!(
            record.message_id,
            MessageId::from_bytes(number.to_be_bytes())
        );
    }
    live_many(&brokers, "close reader", reader.close())
        .await
        .unwrap();
    writer.close().await.unwrap();
    producer.shutdown().await.unwrap();
    consumer.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}
