//! Fast end-to-end inproc gate with real segments on tmpfs.

use super::*;
use ozzy_broker::host_resources;
use ozzy_config::HostResources;
use ozzy_runtime::replicated::TopicReaderStats;
use rustix::thread::{CpuSet, sched_getaffinity, sched_setaffinity};
use std::{path::Path, thread, time::Instant};

const PARTITIONS: usize = 8;
const CASES: [(usize, usize); 3] = [(128, 4096), (1024, 4096), (8192, 1024)];

struct ThreadAffinity(CpuSet);

impl ThreadAffinity {
    fn on(cpu: u32) -> Self {
        let original = sched_getaffinity(None).unwrap();
        let mut selected = CpuSet::new();
        selected.set(cpu as usize);
        sched_setaffinity(None, &selected).unwrap();
        assert_eq!(sched_getaffinity(None).unwrap(), selected);
        Self(original)
    }
}

impl Drop for ThreadAffinity {
    fn drop(&mut self) {
        sched_setaffinity(None, &self.0).unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inproc_tmpfs_writer_broker_reader_throughput_gate() {
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .unwrap();
}

async fn run() {
    let resources = host_resources().unwrap();
    let cpus = resources.cpus.keys().copied().take(6).collect::<Vec<_>>();
    if cpus.len() < 6 {
        eprintln!("inproc performance gate needs six allowed CPUs; skipping");
        return;
    }
    let root = tempfile::Builder::new()
        .prefix("ozzy-inproc-perf-")
        .tempdir_in("/tmp")
        .unwrap();
    let runtime = {
        let _client_cpu = ThreadAffinity::on(cpus[4]);
        WriterRuntime::new().unwrap()
    };
    let deployment = pinned_deployment(root.path(), &resources, &cpus);
    let wire = DataLimits {
        envelope: EnvelopeLimits {
            max_metadata_bytes: 16 * 1024,
            max_payload_bytes: 64 * 1024,
        },
        max_records: 128,
        max_parts: 128,
        max_record_bytes: 8192,
    };
    let sdk = role_links_with_limits(
        &runtime,
        &deployment[0].0,
        handshake::PRODUCER | handshake::CONSUMER,
        wire,
    )
    .await;
    let brokers = start_brokers(&runtime, deployment).await;
    let _writer_cpu = ThreadAffinity::on(cpus[4]);
    let mut writer = live_many(
        &brokers,
        "open performance writer",
        SharedTopicWriter::open(
            &sdk,
            "orders",
            SharedTopicWriterConfig::new(wire),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    let keys = (0..PARTITIONS as u32)
        .map(|partition| {
            (0_u32..1000)
                .map(u32::to_be_bytes)
                .find(|key| writer.metadata().keyed_partition(key).number == partition)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let (ready_rx, mut done_rx, reader_thread) = start_reader(sdk.clone(), cpus[5]);
    ready_rx.await.unwrap();
    let mut sent = 0;
    for (size, count) in CASES {
        let body = bytes::Bytes::from(vec![0x5a; size]);
        let start = Instant::now();
        let mut pending = Vec::with_capacity(count);
        for record in 0..count {
            let sequence = (sent + record + 1) as u128;
            let id = MessageId::from_bytes(sequence.to_be_bytes());
            pending.push(
                writer
                    .send(
                        RecordInput::single(id, body.clone()),
                        Some(&keys[record % PARTITIONS]),
                    )
                    .await
                    .unwrap(),
            );
        }
        for pending in pending {
            pending.confirmed().await.unwrap();
        }
        sent += count;
        let (received_size, stats) = done_rx.recv().await.unwrap();
        assert_eq!(received_size, size);
        let elapsed = start.elapsed();
        let per_second = count as f64 / elapsed.as_secs_f64();
        println!(
            "inproc tmpfs {size} B: {per_second:.0} verified records/s on CPUs {cpus:?}; reader {stats:?}"
        );
        let minimum = minimum_rate(size);
        assert!(
            per_second >= minimum,
            "{size} B inproc gate: {per_second:.0} records/s below {minimum:.0}"
        );
    }
    writer.close().await.unwrap();
    reader_thread.join().unwrap();
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

fn pinned_deployment(
    root: &Path,
    resources: &HostResources,
    cpus: &[u32],
) -> Vec<(CheckedConfig, BrokerIdentity)> {
    deployment_with_resources(
        root,
        DeploymentMode::Single,
        Confirmation::LocalDurable,
        PARTITIONS as u32,
        resources,
        |config| {
            let broker = config.brokers.get_mut("broker-0").unwrap();
            broker.topology.dispatcher.cpu = Some(cpus[1]);
            broker.topology.shards[0].affinity.cpu = Some(cpus[0]);
            let workers = &mut broker.devices.get_mut("ssd").unwrap().workers;
            workers.cpus = vec![cpus[2]];
            workers.progress_cpu = Some(cpus[3]);
        },
    )
}

fn start_reader(
    sdk: BrokerLinks,
    cpu: u32,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::mpsc::UnboundedReceiver<(usize, TopicReaderStats)>,
    thread::JoinHandle<()>,
) {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::mpsc::unbounded_channel();
    let thread = thread::Builder::new()
        .name("ozzy-perf-reader".into())
        .spawn(move || {
            let _affinity = ThreadAffinity::on(cpu);
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let mut reader = TopicReader::open(sdk, "orders", TopicReaderConfig::default())
                        .await
                        .unwrap();
                    let mut opening = Box::pin(reader.next());
                    assert!(futures::poll!(opening.as_mut()).is_pending());
                    drop(opening);
                    ready_tx.send(()).unwrap();
                    for (size, count) in CASES {
                        let expected = vec![0x5a; size];
                        for _ in 0..count {
                            let record = reader.next().await.unwrap();
                            let partition = record.partition as usize;
                            let sequence = record.offset.get() as usize * PARTITIONS + partition;
                            let id = MessageId::from_bytes(((sequence + 1) as u128).to_be_bytes());
                            assert_eq!(record.message_id, id);
                            assert_eq!(record.payload.len(), 1);
                            assert_eq!(record.payload[0].as_ref(), expected);
                        }
                        done_tx.send((size, reader.stats())).unwrap();
                    }
                    reader.close().await.unwrap();
                });
        })
        .unwrap();
    (ready_rx, done_rx, thread)
}

fn minimum_rate(size: usize) -> f64 {
    if cfg!(debug_assertions) {
        match size {
            128 => 2500.0,
            1024 => 2000.0,
            8192 => 75.0,
            _ => unreachable!(),
        }
    } else {
        match size {
            128 => 5000.0,
            1024 => 4000.0,
            8192 => 150.0,
            _ => unreachable!(),
        }
    }
}
