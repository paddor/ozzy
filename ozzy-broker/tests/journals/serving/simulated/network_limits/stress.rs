//! TCP evidence for aggregate limits. Storage stays in the shared memory harness.

use super::*;
use omq_tokio::{Context, ContextConfig};
use std::{net::TcpListener, time::Instant};

const RATE: u64 = 32 * 1024;
const BURST: u64 = 8 * 1024;

#[tokio::test(flavor = "current_thread")]
async fn broker_ingress_byte_limit_is_shared_by_two_producers_and_io_threads() {
    tokio::time::timeout(Duration::from_secs(20), transfer(true))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn broker_egress_byte_limit_is_shared_by_two_readers_and_io_threads() {
    tokio::time::timeout(Duration::from_secs(20), transfer(false))
        .await
        .unwrap();
}

fn deployment(ingress: bool) -> (CheckedConfig, BrokerIdentity) {
    let root = std::path::PathBuf::from(format!("/ozzy-network-tcp-{}", Uuid::now_v7()));
    let mut listeners = Vec::new();
    let mut deployment = deployment_with(&root, DeploymentMode::Single, 1, |deployment| {
        let broker = deployment.brokers.values_mut().next().unwrap();
        for endpoint in [
            &mut broker.endpoints.peer,
            &mut broker.endpoints.data_peer,
            &mut broker.endpoints.reader_pub,
        ] {
            let socket = TcpListener::bind("127.0.0.1:0").unwrap();
            *endpoint = format!("tcp://{}", socket.local_addr().unwrap());
            listeners.push(socket);
        }
        broker.topology.omq.io_threads = 2;
        let limit = Some(ByteRateLimit {
            bytes_per_second: RATE,
            burst_bytes: BURST,
        });
        if ingress {
            broker.topology.omq.ingress = limit;
        } else {
            broker.topology.omq.egress = limit;
        }
    });
    deployment.pop().unwrap()
}

async fn transfer(ingress: bool) {
    let (checked, local) = deployment(ingress);
    let runtime = WriterRuntime::new().unwrap();
    let first = links(&runtime, &checked).await;
    let second = links(&runtime, &checked).await;
    let context = Context::with_config_and_name(ContextConfig { io_threads: 2 }, "ozzy/omq");
    let image = provision(&checked, &local, true).await;
    let (broker, _, image) =
        start_image(context.clone(), checked, local, Memory::new(image, true)).await;
    let brokers = [broker];
    let mut options = SharedTopicWriterConfig::new(limits());
    // Count opaque application bytes in the lower-bound oracle. Compression
    // would allow those bytes to fit into fewer transport bytes legitimately.
    options.compress_payloads = false;
    let mut a = live_many(
        &brokers,
        "open first paced producer",
        SharedTopicWriter::open(&first, "orders", options.clone(), RetryPolicy::default()),
    )
    .await
    .unwrap();
    let mut b = live_many(
        &brokers,
        "open second paced producer",
        SharedTopicWriter::open(&second, "orders", options, RetryPolicy::default()),
    )
    .await
    .unwrap();
    let count = if ingress { 32 } else { 16 };
    let start = Instant::now();
    futures::join!(
        write(&brokers, &mut a, 1, count),
        write(&brokers, &mut b, count + 1, count)
    );
    if ingress {
        assert_paced(start.elapsed());
    }
    let start = Instant::now();
    let mut a_reader = live_many(
        &brokers,
        "open first paced reader",
        TopicReader::open(first.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    let mut b_reader = live_many(
        &brokers,
        "open second paced reader",
        TopicReader::open(second.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    futures::join!(
        read(&brokers, &mut a_reader, 2 * count),
        read(&brokers, &mut b_reader, 2 * count)
    );
    if !ingress {
        assert_paced(start.elapsed());
    }
    live_many(&brokers, "close first paced reader", a_reader.close())
        .await
        .unwrap();
    live_many(&brokers, "close second paced reader", b_reader.close())
        .await
        .unwrap();
    live_many(&brokers, "close first paced producer", a.close())
        .await
        .unwrap();
    live_many(&brokers, "close second paced producer", b.close())
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    image.await.unwrap();
}

fn assert_paced(elapsed: Duration) {
    // Each case moves at least 64 KiB in the limited direction. Headers and
    // earlier traffic can only add pacing. Leave ample clock/scheduling slack.
    let minimum = Duration::from_secs_f64((64 * 1024 - BURST) as f64 / RATE as f64);
    assert!(
        elapsed >= minimum.mul_f64(0.8),
        "network cap did not pace 64 KiB: {elapsed:?}"
    );
}

async fn write(brokers: &[Broker], writer: &mut SharedTopicWriter, first: u8, count: u8) {
    live_many(brokers, "admit paced records", async {
        for id in first..first + count {
            writer
                .send_to_partition(
                    RecordInput::copy_from_slice(MessageId::from_bytes([id; 16]), &[id; 1024]),
                    0,
                )
                .await
                .unwrap();
        }
        writer.flush().await.unwrap();
    })
    .await;
}

async fn read(brokers: &[Broker], reader: &mut TopicReader, count: u8) {
    let mut seen = std::collections::BTreeSet::new();
    for offset in 0..count {
        let record = live_many(brokers, "read paced record", reader.next())
            .await
            .unwrap();
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset.get(), u64::from(offset));
        let id = record.message_id.as_bytes()[0];
        assert!((1..=count).contains(&id));
        assert!(seen.insert(id));
        assert_eq!(record.message_id, MessageId::from_bytes([id; 16]));
        assert_eq!(record.payload.len(), 1);
        assert_eq!(record.payload[0].as_ref(), [id; 1024]);
    }
    assert_eq!(seen.len(), usize::from(count));
}
