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

async fn delay_cluster(
    mode: DeploymentMode,
    clock: &SdkClock,
    max_entries: u64,
) -> broker::Cluster {
    progress(
        clock,
        broker::Cluster::configured(mode, Some(clock.clone()), |config| {
            config.topics.get_mut("orders").unwrap().delay = Some(ozzy_config::TopicDelay {
                max_secs: 60,
                max_entries,
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

/// Index of the broker that leads partition 0.
async fn primary(clock: &SdkClock, cluster: &broker::Cluster, client: &Client) -> usize {
    let primary = progress(clock, client.leader(0)).await;
    cluster
        .configs
        .iter()
        .position(|(config, _)| {
            ozzy_proto::NodeId::from_bytes(*config.identity.brokers[&config.plan.name].as_bytes())
                == primary
        })
        .unwrap()
}

/// Advance broker time to `millis`.
async fn wait_until(clock: &SdkClock, millis: u64) {
    progress(clock, async {
        while broker_now(clock) < millis {
            tokio::task::yield_now().await;
        }
    })
    .await;
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

/// A reader of partition 0 from its start.
async fn partition_reader(clock: &SdkClock, client: &Client) -> TopicReader {
    progress(
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
    .unwrap()
}

/// Read `count` records of partition 0 from its start: ID byte, offset, and
/// broker time when each arrived.
async fn released(clock: &SdkClock, client: &Client, count: usize) -> Vec<(u8, u64, u64)> {
    let mut reader = partition_reader(clock, client).await;
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
            let cluster = delay_cluster(mode, &clock, 8).await;
            let client = progress(
                &clock,
                Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
            )
            .await;
            let mut writer = writer(&clock, &client).await;
            let receipts = confirm(
                &clock,
                &mut writer,
                [record(3, Some(broker_now(&clock) - 1)), record(4, None)],
            )
            .await;
            assert_eq!(receipts[0], (Scheduled, 0), "{mode:?}");
            assert_eq!(receipts[1].0, Stored, "{mode:?}");
            // Confirming advances the clock, so these due times start after
            // the first two records are in.
            let now = broker_now(&clock);
            let receipts = confirm(
                &clock,
                &mut writer,
                [record(1, Some(now + 4_000)), record(2, Some(now + 2_000))],
            )
            .await;
            assert_eq!(receipts, [(Scheduled, 1), (Scheduled, 2)], "{mode:?}");
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

            // A due time past the maximum delay fails permanently. Twice the
            // maximum leaves room for the clock to advance before validation.
            let late = writer
                .send_to_partition(record(5, Some(broker_now(&clock) + 120_000)), 0)
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
            let mut cluster = delay_cluster(mode, &clock, 8).await;
            let client = progress(
                &clock,
                Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
            )
            .await;
            let mut writer = writer(&clock, &client).await;
            // Confirming advances the clock; leave room for a slow host.
            let due = broker_now(&clock) + 10_000;
            confirm(
                &clock,
                &mut writer,
                [record(1, Some(due)), record(2, Some(due + 500))],
            )
            .await;
            assert!(
                broker_now(&clock) < due,
                "{mode:?}: released before the fault"
            );
            if mode == DeploymentMode::Single {
                progress(&clock, cluster.restart(0)).await;
            } else {
                // Crash the primary as its first release comes due.
                let index = primary(&clock, &cluster, &client).await;
                wait_until(&clock, due).await;
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
            let cluster = delay_cluster(mode, &clock, 8).await;
            let client = progress(
                &clock,
                Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
            )
            .await;
            let mut writer = writer(&clock, &client).await;
            // Confirming advances the clock; leave room for a slow host.
            let due = broker_now(&clock) + 10_000;
            confirm(&clock, &mut writer, (1..=8).map(|id| record(id, Some(due)))).await;
            let sent = writer.partition_stats(0).unwrap().requests;
            let waiting = writer
                .send_to_partition(record(9, Some(due + 1_000)), 0)
                .await
                .unwrap();
            // NACK 10 backs the writer off; its retry is a second request.
            progress(&clock, async {
                while writer.partition_stats(0).unwrap().requests < sent + 2
                    && broker_now(&clock) < due
                {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            assert!(
                broker_now(&clock) < due,
                "{mode:?}: no retry before the release"
            );
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

mod stress {
    use super::*;

    const ROUNDS: u8 = 12;
    const RECORDS: u8 = 10;

    /// A confirmed record. Its leader accepted it between `sent_at` and
    /// `confirmed_at`, broker time.
    struct Receipt {
        id: u8,
        due: Option<u64>,
        sent_at: u64,
        confirmed_at: u64,
        disposition: AppendDisposition,
        /// Offset when stored, schedule sequence when scheduled.
        position: u64,
    }

    #[tokio::test(flavor = "current_thread")]
    async fn inproc_memory_held_records_release_once_in_order_across_repeated_faults() {
        tokio::time::timeout(Duration::from_secs(120), async {
            for mode in [DeploymentMode::Single, DeploymentMode::Three] {
                faulted_releases(mode).await;
            }
        })
        .await
        .expect("held records made no bounded progress across repeated faults");
    }

    /// Send rounds of records with spread due times and fault one copy in
    /// each round while earlier rounds release: the primary with its writes
    /// held, or after its records confirm, with or without power loss, or a
    /// follower. Then every record must have left exactly once, in release
    /// order, and none before its due time.
    async fn faulted_releases(mode: DeploymentMode) {
        let clock = SdkClock::manual();
        let mut cluster = delay_cluster(mode, &clock, 64).await;
        let client = progress(
            &clock,
            Client::open_with_clock(&cluster.configs[0].0, &cluster.runtime, clock.clone()),
        )
        .await;
        let mut writer = writer(&clock, &client).await;
        // Read live, so each record's arrival time follows its release
        // closely. The test drives broker time.
        let mut reader = partition_reader(&clock, &client).await;
        let arrivals = tokio::spawn({
            let clock = clock.clone();
            async move {
                let mut records = Vec::new();
                while records.len() < usize::from(ROUNDS * RECORDS) {
                    let record = reader.next().await.unwrap();
                    records.push((
                        record.message_id.as_bytes()[0],
                        record.offset.get(),
                        broker_now(&clock),
                    ));
                }
                reader.close().await.unwrap();
                records
            }
        });
        let mut sent = Vec::new();
        for round in 0..ROUNDS {
            // (hold writes, crash the primary, power loss, broker millis to
            // release before the fault)
            let (hold, on_primary, power_loss, advance) = match round % 4 {
                0 => (false, true, false, 2_500),
                1 => (true, true, true, 0),
                2 => (false, false, true, 2_500),
                _ => (false, true, true, 1_500),
            };
            let primary = primary(&clock, &cluster, &client).await;
            let target = if on_primary || mode == DeploymentMode::Single {
                primary
            } else {
                (primary + 1) % cluster.configs.len()
            };
            let control = cluster.controls[target].clone();
            control.hold_completions(hold);
            let sent_at = broker_now(&clock);
            let mut pending = Vec::new();
            for n in 0..RECORDS {
                let id = round * RECORDS + n + 1;
                // From 1 s past due to 8 s ahead, out of send order; every
                // fourth record is stored at once.
                let due = (n % 4 != 3)
                    .then(|| sent_at - 1_000 + u64::from((n * 7 + round * 3) % 19) * 500);
                let confirmed = writer.send_to_partition(record(id, due), 0).await.unwrap();
                pending.push((id, due, confirmed));
            }
            if hold {
                progress(&clock, async {
                    while control.pending_completions() == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await;
                progress(&clock, cluster.crash(target, power_loss)).await;
            }
            for (id, due, confirmed) in pending {
                let receipt = progress(&clock, confirmed.confirmed()).await.unwrap();
                sent.push(Receipt {
                    id,
                    due,
                    sent_at,
                    confirmed_at: broker_now(&clock),
                    disposition: receipt.record.disposition,
                    position: receipt.record.offset,
                });
            }
            if !hold {
                wait_until(&clock, broker_now(&clock) + advance).await;
                progress(&clock, cluster.crash(target, power_loss)).await;
            }
            progress(&clock, cluster.wait_recovered(target)).await;
        }
        let last_due = sent.iter().filter_map(|sent| sent.due).max().unwrap();
        wait_until(&clock, last_due).await;

        let records = progress(&clock, arrivals).await.unwrap();
        // A repeated release would take the next offset first.
        let receipts = confirm(&clock, &mut writer, [record(u8::MAX, None)]).await;
        assert_eq!(
            receipts,
            [(AppendDisposition::Stored, sent.len() as u64)],
            "{mode:?}"
        );
        assert!(
            records
                .iter()
                .map(|&(_, offset, _)| offset)
                .eq(0..sent.len() as u64),
            "{mode:?}"
        );
        let offsets: BTreeMap<u8, (u64, u64)> = records
            .iter()
            .map(|&(id, offset, arrived)| (id, (offset, arrived)))
            .collect();
        assert_eq!(offsets.len(), sent.len(), "{mode:?}: a record left twice");
        let mut held = Vec::new();
        for receipt in &sent {
            let (offset, arrived) = *offsets
                .get(&receipt.id)
                .unwrap_or_else(|| panic!("{mode:?}: record {} was lost", receipt.id));
            match (receipt.disposition, receipt.due) {
                (AppendDisposition::Stored, None) => {
                    assert_eq!(receipt.position, offset, "{mode:?}: record {}", receipt.id);
                }
                (AppendDisposition::Scheduled, Some(due)) => held.push((
                    offset,
                    receipt.position,
                    due.max(receipt.sent_at),
                    due.max(receipt.confirmed_at),
                    due,
                    arrived,
                )),
                other => panic!("{mode:?}: record {}: {other:?}", receipt.id),
            }
        }
        // A record arrives after its release.
        for &(offset, _, _, _, due, arrived) in &held {
            assert!(
                arrived >= due,
                "{mode:?}: offset {offset} due at {due} arrived at {arrived}"
            );
        }
        // Each record releases at key (max(due, acceptance), schedule
        // sequence). Acceptance lies within the send-to-receipt window, so
        // the key lies within [early, late]; one released before another
        // must not have a key that is surely larger.
        let sequences: std::collections::BTreeSet<_> =
            held.iter().map(|&(_, sequence, ..)| sequence).collect();
        assert_eq!(sequences.len(), held.len(), "{mode:?}: schedule sequences");
        for first in &held {
            for later in held.iter().filter(|later| later.0 > first.0) {
                let (_, sequence, early, ..) = *first;
                let (_, later_sequence, _, late, ..) = *later;
                assert!(
                    late > early || (late == early && later_sequence > sequence),
                    "{mode:?}: released out of order: {first:?} before {later:?}"
                );
            }
        }
        drop(writer);
        progress(&clock, client.close()).await;
        progress(&clock, cluster.shutdown()).await;
    }
}
