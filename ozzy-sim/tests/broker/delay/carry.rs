//! Held records outlive retention: copies carry them forward, so old
//! segments retire while the records wait.

use super::*;

/// The first retained offset of partition 0, read by a fresh reader.
async fn earliest(clock: &SdkClock, client: &Client) -> u64 {
    let mut reader = partition_reader(clock, client).await;
    let earliest = match progress(clock, reader.next()).await {
        Ok(record) => record.offset,
        // Retention passed the first record before the reader read it.
        Err(ozzy_runtime::replicated::TopicReaderError::RetentionGap { earliest, .. }) => earliest,
        Err(error) => panic!("{error}"),
    };
    reader.close().await.unwrap();
    earliest.get()
}

/// A cluster of delay partitions, a client, a writer, and a live reader.
struct Retained {
    cluster: broker::Cluster,
    client: Client,
    writer: SharedTopicWriter,
    arrivals: tokio::task::JoinHandle<Vec<(u8, u64, u64)>>,
}

/// Start delay partitions with `retention`, read live until `count`
/// records arrive.
async fn retained(
    mode: DeploymentMode,
    clock: &SdkClock,
    held: Option<(usize, usize)>,
    retention: ozzy_config::TopicRetention,
    count: usize,
) -> Retained {
    let cluster = held_cluster(mode, clock, 64, held, Some(retention)).await;
    let client = progress(
        clock,
        Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
    )
    .await;
    let writer = writer(clock, &client).await;
    let arrivals = arrivals(clock, &client, count).await;
    Retained {
        cluster,
        client,
        writer,
        arrivals,
    }
}

/// Wait for the last due time, then check every record left once and in
/// release order, and that none left twice.
async fn finish(mode: DeploymentMode, clock: &SdkClock, started: Retained, sent: &[Receipt]) {
    let Retained {
        cluster,
        client,
        mut writer,
        arrivals,
    } = started;
    let last_due = sent.iter().filter_map(|sent| sent.due).max().unwrap();
    wait_until(clock, last_due).await;
    let records = progress(clock, arrivals).await.unwrap();
    // A repeated release would take the next offset first.
    let receipts = confirm(clock, &mut writer, [record(u8::MAX, None)]).await;
    assert_eq!(
        receipts,
        [(AppendDisposition::Stored, sent.len() as u64)],
        "{mode:?}"
    );
    check_releases(mode, sent, &records);
    drop(writer);
    progress(clock, client.close()).await;
    progress(clock, cluster.shutdown()).await;
}

/// Retention ages segments out after 2 s while records wait up to 20 s.
/// Each copy rolls its segment at eight held rows, so held records reach
/// sealed segments without stored records.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_held_records_outlive_age_retention_and_release_once_in_order() {
    tokio::time::timeout(Duration::from_secs(90), async {
        for mode in [DeploymentMode::Single, DeploymentMode::Three] {
            let clock = SdkClock::manual();
            let retention = ozzy_config::TopicRetention {
                max_age_secs: Some(2),
                max_bytes: None,
            };
            let mut started = retained(mode, &clock, Some((4, 8)), retention, 32).await;
            let mut sent = Vec::new();
            for round in 0..4_u8 {
                let now = broker_now(&clock);
                // Due 8 to 20 s ahead; every fourth record is stored at once.
                let records = (0..8_u8).map(|n| {
                    let id = round * 8 + n + 1;
                    let due = (n % 4 != 3)
                        .then(|| now + 8_000 + u64::from((n * 3 + round * 5) % 7) * 2_000);
                    (id, due, record(id, due))
                });
                sent.extend(send(&clock, &mut started.writer, records).await);
                wait_until(&clock, broker_now(&clock) + 2_500).await;
            }
            // The first round's stored records aged out while some of its
            // held records still wait: their segment was carried and retired.
            let first = &sent[..8];
            assert!(
                first
                    .iter()
                    .filter_map(|sent| sent.due)
                    .any(|due| due > broker_now(&clock)),
                "{mode:?}"
            );
            assert!(earliest(&clock, &started.client).await > 0, "{mode:?}");
            finish(mode, &clock, started, &sent).await;
        }
    })
    .await
    .expect("held records past age retention made no bounded progress");
}

/// A one-segment byte target retires the sealed segment of scheduled
/// records long before they are due. Each copy rolls its segment at eight
/// held rows.
#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_byte_retention_retires_segments_past_a_long_hold() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for mode in [DeploymentMode::Single, DeploymentMode::Three] {
            let clock = SdkClock::manual();
            let retention = ozzy_config::TopicRetention {
                max_age_secs: None,
                max_bytes: Some(1024 * 1024),
            };
            let mut started = retained(mode, &clock, Some((4, 8)), retention, 13).await;
            let now = broker_now(&clock);
            let stored =
                |ids: std::ops::RangeInclusive<u8>| ids.map(|id| (id, None, record(id, None)));
            let mut sent = send(&clock, &mut started.writer, stored(1..=4)).await;
            let held = (5..=12_u8).map(|id| {
                let due = Some(now + 20_000 + u64::from(id) * 500);
                (id, due, record(id, due))
            });
            sent.extend(send(&clock, &mut started.writer, held).await);
            // Eight held rows roll the segment before this write.
            sent.extend(send(&clock, &mut started.writer, stored(13..=13)).await);
            let mut retained_from = 0;
            while retained_from == 0 && broker_now(&clock) < now + 20_000 {
                wait_until(&clock, broker_now(&clock) + 1_000).await;
                retained_from = earliest(&clock, &started.client).await;
            }
            assert!(retained_from > 0, "{mode:?}");
            finish(mode, &clock, started, &sent).await;
        }
    })
    .await
    .expect("byte retention past held records made no bounded progress");
}
