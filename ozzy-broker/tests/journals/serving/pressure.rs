//! Real OMQ inproc load, shared partition offsets, and paused reader repair.

use super::*;
use ozzy_runtime::replicated::{SharedTopicPendingRecord, TopicRecord};
use std::collections::{BTreeMap, VecDeque};

const WRITERS: usize = 4;
const RECORDS: usize = 256;
const PARTITIONS: usize = 3;

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_load_preserves_records_with_slow_readers() {
    Box::pin(run(false, WRITERS)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_load_preserves_records_with_reordered_disk_completions() {
    Box::pin(run(true, WRITERS)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_load_preserves_records_with_many_producer_sources() {
    // More sources than one paused-input turn, sharing the same shard lanes.
    Box::pin(run_policy(false, Confirmation::LocalDurable, 20)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_load_preserves_many_producer_sources_with_disk_quorum() {
    Box::pin(run_policy(false, Confirmation::DiskQuorum, 20)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_load_preserves_many_producer_sources_with_replicated_persisting() {
    Box::pin(run_policy(false, Confirmation::ReplicatedPersisting, 20)).await;
}

async fn run(reverse: bool, writers: usize) {
    for policy in [
        Confirmation::LocalDurable,
        Confirmation::DiskQuorum,
        Confirmation::ReplicatedPersisting,
    ] {
        Box::pin(run_policy(reverse, policy, writers)).await;
    }
}

async fn run_policy(reverse: bool, policy: Confirmation, writers: usize) {
    eprintln!("inproc memory pressure: reverse={reverse}/{policy:?}");
    tokio::time::timeout(
        Duration::from_secs(90),
        Box::pin(scenario(reverse, policy, writers)),
    )
    .await
    .unwrap_or_else(|_| panic!("reverse={reverse}/{policy:?}: inproc load timed out"));
}

fn wire() -> DataLimits {
    DataLimits {
        envelope: EnvelopeLimits {
            max_metadata_bytes: 16 * 1024,
            max_payload_bytes: 64 * 1024,
        },
        max_records: 32,
        max_parts: 32,
        max_record_bytes: 32 * 1024,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one end-to-end bounded load and repair scenario"
)]
async fn scenario(reverse: bool, policy: Confirmation, writers: usize) {
    let root = std::path::PathBuf::from(format!("/ozzy-simulation-{}", Uuid::now_v7()));
    let runtime = WriterRuntime::new().unwrap();
    let mode = if policy == Confirmation::LocalDurable {
        DeploymentMode::Single
    } else {
        DeploymentMode::Three
    };
    let deployment = deployment_with(&root, mode, policy, PARTITIONS as u32, |config| {
        config.topics.get_mut("orders").unwrap().max_append_bytes = 128 * 1024;
        for broker in config.brokers.values_mut() {
            for shard in &mut broker.topology.shards {
                shard.budget.resident_bytes = 4 * 1024 * 1024;
            }
            let workers = &mut broker.devices.get_mut("ssd").unwrap().workers;
            workers.aio_depth = 2;
            workers.max_inflight = 2;
            workers.queued_jobs = 8;
        }
    });
    let mut producers = Vec::new();
    for _ in 0..writers {
        producers.push(
            role_links_with_config(
                &runtime,
                &deployment[0].0,
                handshake::PRODUCER,
                wire(),
                |config| {
                    config.control_bytes = 4 * 1024 * 1024;
                    let append = config.append.as_mut().unwrap();
                    append.requests = 128;
                    append.records = 512;
                },
            )
            .await,
        );
    }
    let consumer = role_links_with_config(
        &runtime,
        &deployment[0].0,
        handshake::CONSUMER,
        wire(),
        |config| {
            config.control_bytes = 4 * 1024 * 1024;
            let reader = config.reader.as_mut().unwrap();
            reader.subscriptions = PARTITIONS;
            reader.queue_messages = 2;
            reader.bytes = reader
                .reservation_bytes(wire(), config.brokers.len())
                .unwrap();
        },
    )
    .await;
    let brokers = Box::pin(simulated::start_brokers(&runtime, deployment, reverse)).await;
    let mut reader = load_progress(
        &brokers,
        "open pressured reader",
        TopicReader::open(consumer.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    let mut opening = Box::pin(reader.next());
    assert!(futures::poll!(opening.as_mut()).is_pending());
    drop(opening);
    let (tasks, mut progress) = start_writers(&brokers, &producers).await;
    // Returned payload aliases stay charged while later traffic fills the SDK.
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(
            load_progress(&brokers, "retain reader payload", reader.next())
                .await
                .unwrap(),
        );
    }
    for completed in 0..writers * RECORDS {
        assert!(
            load_progress(
                &brokers,
                &format!("paused reader, confirmed {completed} records"),
                progress.recv()
            )
            .await
            .is_some()
        );
    }
    let mut expected = BTreeMap::new();
    for task in tasks {
        for row in load_progress(&brokers, "confirm with reader paused", task)
            .await
            .unwrap()
        {
            assert!(expected.insert((row.partition, row.offset), row).is_none());
        }
    }
    assert_eq!(expected.len(), writers * RECORDS);
    let mut offsets = [0; PARTITIONS];
    for record in held {
        verify(&record, &mut expected, &mut offsets);
    }
    while !expected.is_empty() {
        let stage = format!(
            "repair paused reader: remaining={}, offsets={offsets:?}, checkpoint={:?}",
            expected.len(),
            reader.checkpoint()
        );
        let record = load_progress(&brokers, &stage, reader.next())
            .await
            .unwrap();
        verify(&record, &mut expected, &mut offsets);
    }
    let stats = reader.stats();
    assert_eq!(
        stats.live_records + stats.replayed_records,
        (writers * RECORDS) as u64
    );
    assert!(
        stats.replayed_records > 0,
        "paused reader did not exercise PEER replay"
    );
    eprintln!(
        "inproc memory pressure verified reverse={reverse}/{policy:?}: {} records, {stats:?}",
        writers * RECORDS
    );
    load_progress(&brokers, "close pressured reader", reader.close())
        .await
        .unwrap();
    for producer in producers {
        producer.shutdown().await.unwrap();
    }
    consumer.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

async fn start_writers(
    brokers: &[Broker],
    sdks: &[BrokerLinks],
) -> (
    Vec<tokio::task::JoinHandle<Vec<Expected>>>,
    tokio::sync::mpsc::Receiver<()>,
) {
    let (progress, received) = tokio::sync::mpsc::channel(sdks.len() * RECORDS);
    let mut tasks = Vec::new();
    for (writer_number, sdk) in sdks.iter().enumerate() {
        let mut config = SharedTopicWriterConfig::new(wire());
        config.inflight_appends = 3;
        config.compress_payloads = false;
        let writer = load_progress(
            brokers,
            "open load writer",
            SharedTopicWriter::open(sdk, "orders", config, RetryPolicy::default()),
        )
        .await
        .unwrap();
        tasks.push(tokio::spawn(write(writer, writer_number, progress.clone())));
    }
    (tasks, received)
}

#[derive(Debug)]
struct Expected {
    partition: u32,
    offset: u64,
    id: MessageId,
    bytes: usize,
    fill: u8,
}

async fn write(
    mut writer: SharedTopicWriter,
    number: usize,
    progress: tokio::sync::mpsc::Sender<()>,
) -> Vec<Expected> {
    let keys: Vec<_> = (0..PARTITIONS as u32)
        .map(|partition| {
            (0_u64..1000)
                .map(u64::to_le_bytes)
                .find(|key| writer.metadata().keyed_partition(key).number == partition)
                .unwrap()
        })
        .collect();
    let mut pending = VecDeque::new();
    let mut confirmed = Vec::new();
    for sequence in 0..RECORDS {
        let id = MessageId::from_bytes(
            (((number + 1) as u128) << 64 | (sequence + 1) as u128).to_be_bytes(),
        );
        let bytes = [32768, 128, 8192, 512][sequence / 64];
        let fill = ((number * 17 + sequence) % 251) as u8;
        let mut body = vec![fill; bytes];
        body[..16].copy_from_slice(id.as_bytes());
        let admitted = writer
            .send(
                RecordInput::single(id, body.into()),
                Some(&keys[sequence % PARTITIONS]),
            )
            .await
            .unwrap();
        pending.push_back((admitted, id, bytes, fill));
        if pending.len() == 64 {
            confirmed.push(confirm(pending.pop_front().unwrap()).await);
            progress.try_send(()).unwrap();
        }
    }
    for entry in pending {
        confirmed.push(confirm(entry).await);
        progress.try_send(()).unwrap();
    }
    writer.close().await.unwrap();
    confirmed
}

async fn confirm(entry: (SharedTopicPendingRecord, MessageId, usize, u8)) -> Expected {
    let (pending, id, bytes, fill) = entry;
    let receipt = pending.confirmed().await.unwrap();
    assert_eq!(receipt.record.message_id, id);
    assert_eq!(receipt.record.key.first_sequence, pending.sequence());
    Expected {
        partition: receipt.partition,
        offset: receipt.record.offset,
        id,
        bytes,
        fill,
    }
}

fn verify(
    record: &TopicRecord,
    expected: &mut BTreeMap<(u32, u64), Expected>,
    offsets: &mut [u64; PARTITIONS],
) {
    let row = expected
        .remove(&(record.partition, record.offset.get()))
        .expect("unexpected or duplicate record");
    assert_eq!(record.offset.get(), offsets[record.partition as usize]);
    offsets[record.partition as usize] += 1;
    assert_eq!(record.message_id, row.id);
    assert_eq!(record.payload.len(), 1);
    let body = &record.payload[0];
    assert_eq!(body.len(), row.bytes);
    assert_eq!(&body[..16], row.id.as_bytes());
    assert!(body[16..].iter().all(|&byte| byte == row.fill));
}

async fn load_progress<T>(
    brokers: &[Broker],
    stage: &str,
    operation: impl std::future::Future<Output = T>,
) -> T {
    tokio::select! {
        result = operation => result,
        (result, broker, _) = futures::future::select_all(brokers.iter().map(|broker| Box::pin(broker.closed()))) => {
            panic!("broker {broker} exited during {stage}: {result:?}")
        },
        () = tokio::time::sleep(Duration::from_secs(15)) => panic!("no progress during {stage}"),
    }
}
