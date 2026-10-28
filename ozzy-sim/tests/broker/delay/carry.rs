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

/// Retention ages segments out after 1 s while the first round's records
/// wait 18 to 30 s. Each copy rolls its segment at eight held rows, so held
/// records reach sealed segments without stored records.
async fn outlive_age_retention(mode: DeploymentMode) {
    let clock = SdkClock::manual();
    let retention = ozzy_config::TopicRetention {
        max_age_secs: Some(1),
        max_bytes: None,
    };
    let mut started = retained(mode, &clock, Some((4, 8)), retention, 20).await;
    let mut sent = Vec::new();
    // The first round's nine held rows roll the segment before the second
    // round.
    for (round, count) in [(0, 12), (1, 8)] {
        let now = broker_now(&clock);
        // Due 18 to 30 s ahead in the first round, 4 to 10 s in the second;
        // every fourth record is stored at once.
        let (base, step) = if round == 0 {
            (18_000, 2_000)
        } else {
            (4_000, 1_000)
        };
        let records = (0..count).map(|n| {
            let id = round * 12 + n + 1;
            let due = (n % 4 != 3).then(|| now + base + u64::from((n * 3 + round * 5) % 7) * step);
            (id, due, record(id, due))
        });
        sent.extend(send(&clock, &mut started.writer, records).await);
        wait_until(&clock, broker_now(&clock) + 1_500).await;
    }
    // The first round's stored records age out while some of its held
    // records still wait: their segment was carried and retired.
    let held_until = sent[..12].iter().filter_map(|sent| sent.due).max().unwrap();
    let mut retained_from = 0;
    while retained_from == 0 && broker_now(&clock) < held_until {
        retained_from = earliest(&clock, &started.client).await;
        wait_until(&clock, broker_now(&clock) + 500).await;
    }
    assert!(retained_from > 0, "{mode:?}");
    finish(mode, &clock, started, &sent).await;
}

/// A one-segment byte target retires the sealed segment of scheduled
/// records long before they are due. Each copy rolls its segment at eight
/// held rows.
async fn outlive_byte_retention(mode: DeploymentMode) {
    let clock = SdkClock::manual();
    let retention = ozzy_config::TopicRetention {
        max_age_secs: None,
        max_bytes: Some(1024 * 1024),
    };
    let mut started = retained(mode, &clock, Some((4, 8)), retention, 13).await;
    let now = broker_now(&clock);
    let stored = |ids: std::ops::RangeInclusive<u8>| ids.map(|id| (id, None, record(id, None)));
    let mut sent = send(&clock, &mut started.writer, stored(1..=4)).await;
    let held = (5..=12_u8).map(|id| {
        let due = Some(now + 28_000 + u64::from(id) * 250);
        (id, due, record(id, due))
    });
    sent.extend(send(&clock, &mut started.writer, held).await);
    // Eight held rows roll the segment before this write.
    sent.extend(send(&clock, &mut started.writer, stored(13..=13)).await);
    let mut retained_from = 0;
    while retained_from == 0 && broker_now(&clock) < now + 28_000 {
        wait_until(&clock, broker_now(&clock) + 1_000).await;
        retained_from = earliest(&clock, &started.client).await;
    }
    assert!(retained_from > 0, "{mode:?}");
    finish(mode, &clock, started, &sent).await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_held_records_outlive_age_retention_and_release_once_in_order() {
    tokio::time::timeout(
        Duration::from_secs(60),
        outlive_age_retention(DeploymentMode::Single),
    )
    .await
    .expect("held records past age retention made no bounded progress");
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_byte_retention_retires_segments_past_a_long_hold() {
    tokio::time::timeout(
        Duration::from_secs(60),
        outlive_byte_retention(DeploymentMode::Single),
    )
    .await
    .expect("byte retention past held records made no bounded progress");
}

/// Three copies need the retirement before the held records are due, which
/// slips on a loaded host as broker time runs ahead.
mod stress {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn inproc_memory_three_copies_hold_records_past_age_retention() {
        tokio::time::timeout(
            Duration::from_secs(90),
            outlive_age_retention(DeploymentMode::Three),
        )
        .await
        .expect("held records past age retention made no bounded progress");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inproc_memory_three_copies_hold_records_past_byte_retention() {
        tokio::time::timeout(
            Duration::from_secs(90),
            outlive_byte_retention(DeploymentMode::Three),
        )
        .await
        .expect("byte retention past held records made no bounded progress");
    }
}
