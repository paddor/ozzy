use super::*;
use ozzy_proto::{MessageId, append::AppendDisposition};
use ozzy_runtime::replicated::{
    RecordInput, RetryPolicy, SharedTopicWriter, SharedTopicWriterConfig, TopicReader,
    TopicReaderConfig,
};

/// Broker time at manual clock zero, in Unix milliseconds.
const EPOCH_MILLIS: u64 = 1_000_000;

fn broker_now(clock: &SdkClock) -> u64 {
    EPOCH_MILLIS + u64::try_from(clock.now().as_millis()).unwrap()
}

fn record(id: u8, due: Option<u64>) -> RecordInput {
    let record = RecordInput::copy_from_slice(MessageId::from_bytes([id; 16]), &[id; 8]);
    match due {
        Some(due) => record.with_due_millis(due),
        None => record,
    }
}

async fn delay_cluster(mode: DeploymentMode, clock: &SdkClock) -> broker::Cluster {
    progress(
        clock,
        broker::Cluster::configured(mode, Some(clock.clone()), |config| {
            config.topics.get_mut("orders").unwrap().delay = Some(ozzy_config::TopicDelay {
                max_secs: 60,
                max_entries: 8,
            });
        }),
    )
    .await
}

async fn writer(clock: &SdkClock, client: &Client) -> SharedTopicWriter {
    progress(
        clock,
        SharedTopicWriter::open(
            &client.links,
            "orders",
            SharedTopicWriterConfig::new(ozzy_sim::client::DATA_LIMITS),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap()
}

/// Send records to partition 0 and return their receipts.
async fn confirm(
    clock: &SdkClock,
    writer: &mut SharedTopicWriter,
    records: impl IntoIterator<Item = RecordInput>,
) -> Vec<(AppendDisposition, u64)> {
    let mut pending = Vec::new();
    for record in records {
        pending.push(writer.send_to_partition(record, 0).await.unwrap());
    }
    let mut receipts = Vec::new();
    for pending in pending {
        let receipt = progress(clock, pending.confirmed()).await.unwrap();
        receipts.push((receipt.record.disposition, receipt.record.offset));
    }
    receipts
}

/// Read `count` records of partition 0 from its start: ID byte, offset, and
/// broker time when each arrived.
async fn released(clock: &SdkClock, client: &Client, count: usize) -> Vec<(u8, u64, u64)> {
    let mut reader = progress(
        clock,
        TopicReader::open(
            client.links.clone(),
            "orders",
            TopicReaderConfig {
                partitions: Some(vec![0]),
                ..TopicReaderConfig::default()
            },
        ),
    )
    .await
    .unwrap();
    let mut records = Vec::new();
    while records.len() < count {
        let record = progress(clock, reader.next()).await.unwrap();
        records.push((
            record.message_id.as_bytes()[0],
            record.offset.get(),
            broker_now(clock),
        ));
    }
    reader.close().await.unwrap();
    records
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_scheduled_records_release_in_due_order_in_every_mode() {
    use AppendDisposition::{Scheduled, Stored};
    tokio::time::timeout(Duration::from_secs(60), async {
        for mode in [DeploymentMode::Single, DeploymentMode::Three] {
            let clock = SdkClock::manual();
            let cluster = delay_cluster(mode, &clock).await;
            let client = progress(
                &clock,
                Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
            )
            .await;
            let mut writer = writer(&clock, &client).await;
            let now = broker_now(&clock);
            let receipts = confirm(
                &clock,
                &mut writer,
                [
                    record(1, Some(now + 4_000)),
                    record(2, Some(now + 2_000)),
                    record(3, Some(now - 1)),
                    record(4, None),
                ],
            )
            .await;
            assert_eq!(
                receipts[..3],
                [(Scheduled, 0), (Scheduled, 1), (Scheduled, 2)]
            );
            assert_eq!(receipts[3].0, Stored, "{mode:?}");
            let records = released(&clock, &client, 4).await;
            // A past due time releases at once; the others wait, in due order.
            let order: Vec<_> = records.iter().map(|&(id, _, _)| id).collect();
            assert!(
                order[..2].contains(&3) && order[..2].contains(&4),
                "{order:?}"
            );
            assert_eq!(order[2..], [2, 1], "{mode:?}");
            assert!(records.iter().map(|&(_, offset, _)| offset).eq(0..4));
            assert!(records[2].2 >= now + 2_000 && records[3].2 >= now + 4_000);

            // A due time past the maximum delay fails permanently.
            let late = writer
                .send_to_partition(record(5, Some(broker_now(&clock) + 61_000)), 0)
                .await
                .unwrap();
            assert!(
                progress(&clock, late.confirmed()).await.is_err(),
                "{mode:?}"
            );
            drop(writer);
            progress(&clock, client.close()).await;
            progress(&clock, cluster.shutdown()).await;
        }
    })
    .await
    .expect("scheduled records made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_held_records_survive_a_restart_or_leader_crash_and_release_once() {
    tokio::time::timeout(Duration::from_secs(90), async {
        for mode in [DeploymentMode::Single, DeploymentMode::Three] {
            let clock = SdkClock::manual();
            let mut cluster = delay_cluster(mode, &clock).await;
            let client = progress(
                &clock,
                Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
            )
            .await;
            let mut writer = writer(&clock, &client).await;
            let due = broker_now(&clock) + 2_000;
            confirm(
                &clock,
                &mut writer,
                [record(1, Some(due)), record(2, Some(due + 500))],
            )
            .await;
            if mode == DeploymentMode::Single {
                progress(&clock, cluster.restart(0)).await;
            } else {
                // Crash the primary as its first release comes due.
                let primary = progress(&clock, client.leader(0)).await;
                let index = cluster
                    .configs
                    .iter()
                    .position(|(config, _)| {
                        ozzy_proto::NodeId::from_bytes(
                            *config.identity.brokers[&config.plan.name].as_bytes(),
                        ) == primary
                    })
                    .unwrap();
                progress(&clock, async {
                    while broker_now(&clock) < due {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
                progress(&clock, cluster.crash(index, false)).await;
                progress(&clock, cluster.wait_recovered(index)).await;
            }
            let records = released(&clock, &client, 2).await;
            assert_eq!(
                records
                    .iter()
                    .map(|&(id, offset, _)| (id, offset))
                    .collect::<Vec<_>>(),
                [(1, 0), (2, 1)],
                "{mode:?}"
            );
            // A repeated release would take the next offsets first.
            let receipts = confirm(&clock, &mut writer, [record(3, None)]).await;
            assert_eq!(receipts, [(AppendDisposition::Stored, 2)], "{mode:?}");
            drop(writer);
            progress(&clock, client.close()).await;
            progress(&clock, cluster.shutdown()).await;
        }
    })
    .await
    .expect("held records made no bounded progress across a restart or crash");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_full_schedule_capacity_backs_off_until_a_release() {
    use futures::FutureExt;
    tokio::time::timeout(Duration::from_secs(60), async {
        for mode in [DeploymentMode::Single, DeploymentMode::Three] {
            let clock = SdkClock::manual();
            let cluster = delay_cluster(mode, &clock).await;
            let client = progress(
                &clock,
                Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
            )
            .await;
            let mut writer = writer(&clock, &client).await;
            let due = broker_now(&clock) + 1_000;
            confirm(&clock, &mut writer, (1..=8).map(|id| record(id, Some(due)))).await;
            let waiting = writer
                .send_to_partition(record(9, Some(due + 1_000)), 0)
                .await
                .unwrap();
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
            assert!(waiting.confirmed().now_or_never().is_none(), "{mode:?}");
            // Releasing the first eight makes room; the writer retries.
            let receipt = progress(&clock, waiting.confirmed()).await.unwrap();
            assert!(broker_now(&clock) >= due, "{mode:?}");
            assert_eq!(receipt.record.offset, 8, "{mode:?}");
            assert_eq!(released(&clock, &client, 9).await.len(), 9);
            drop(writer);
            progress(&clock, client.close()).await;
            progress(&clock, cluster.shutdown()).await;
        }
    })
    .await
    .expect("full schedule capacity made no bounded progress");
}
