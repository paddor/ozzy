//! Actual shared PUB/SUB delivery, cancellation, and quiet-tail repair.

use super::*;
use omq_tokio::{Message, Options, Socket, SocketType};
use ozzy_proto::{data::Authority, reader};
use ozzy_runtime::replicated::{TopicCheckpoint, TopicRecord};

pub(super) type History = Vec<(MessageId, bytes::Bytes)>;

pub(super) async fn check(
    brokers: &[Broker],
    sdk: &BrokerLinks,
    writer: &mut SharedTopicWriter,
    expected: &mut [History],
) {
    let sockets = sdk.socket_count();
    let metadata = writer.metadata().clone();
    let mut positions = expected.iter().map(Vec::len).collect::<Vec<_>>();
    let checkpoint = TopicCheckpoint {
        topic: metadata.id(),
        positions: positions
            .iter()
            .enumerate()
            .map(|(n, &next)| (n as u32, ozzy_proto::Offset::new(next as u64)))
            .collect(),
    };
    let mut reader = live_many(
        brokers,
        "open live reader",
        TopicReader::open(
            sdk.clone(),
            "orders",
            TopicReaderConfig {
                start: ozzy_runtime::replicated::ReaderStart::Checkpoint(checkpoint.clone()),
                ..TopicReaderConfig::default()
            },
        ),
    )
    .await
    .unwrap();
    for wave in 0..4 {
        let records = inputs(&metadata, expected, wave, 1);
        let writing = write(brokers, writer, records);
        let reading = read(brokers, &mut reader, expected, &mut positions);
        futures::join!(writing, reading);
    }
    assert!(
        reader.stats().live_records > 0,
        "reader must use actual broker PUB / SDK SUB"
    );
    let stable = reader.checkpoint();
    assert!(
        live_many(
            brokers,
            "quiet live reader",
            tokio::time::timeout(Duration::from_millis(250), reader.next())
        )
        .await
        .is_err()
    );
    assert_eq!(
        reader.checkpoint(),
        stable,
        "canceled quiet probe cannot advance progress"
    );

    // The reader is idle while new publications accumulate or are lost. PEER
    // repairs the exact remaining offsets, including a missing final frame.
    let records = inputs(&metadata, expected, 4, 12);
    write(brokers, writer, records).await;
    read(brokers, &mut reader, expected, &mut positions).await;
    assert_eq!(positions, expected.iter().map(Vec::len).collect::<Vec<_>>());
    let stats = reader.stats();
    assert_eq!(
        (stats.live_records + stats.replayed_records) as usize,
        expected.iter().map(Vec::len).sum::<usize>()
            - checkpoint
                .positions
                .iter()
                .map(|&(_, n)| n.get() as usize)
                .sum::<usize>()
    );
    live_many(brokers, "close live reader", reader.close())
        .await
        .unwrap();
    assert_eq!(reader.stats(), stats);
    assert_eq!(
        sdk.socket_count(),
        sockets,
        "topic partitions reuse shared sockets"
    );
}

pub(super) fn inputs(
    metadata: &ozzy_runtime::topic_metadata::TopicMetadata,
    expected: &mut [History],
    wave: usize,
    rounds: usize,
) -> Vec<(MessageId, bytes::Bytes, [u8; 4])> {
    let mut inputs = Vec::new();
    for (partition, history) in expected.iter_mut().enumerate() {
        let key = (0_u32..1000)
            .map(u32::to_be_bytes)
            .find(|key| metadata.keyed_partition(key).number == partition as u32)
            .unwrap();
        for round in 0..rounds {
            let id = MessageId::from_bytes(*Uuid::now_v7().as_bytes());
            let mut body = format!("live-{wave}-{partition}-{round}").into_bytes();
            if wave == 4 {
                body.resize(768, 7);
            }
            let body = bytes::Bytes::from(body);
            history.push((id, body.clone()));
            inputs.push((id, body, key));
        }
    }
    inputs
}

pub(super) async fn write(
    brokers: &[Broker],
    writer: &mut SharedTopicWriter,
    inputs: Vec<(MessageId, bytes::Bytes, [u8; 4])>,
) {
    let mut pending = Vec::new();
    for (id, body, key) in inputs {
        pending.push(
            writer
                .send(RecordInput::copy_from_slice(id, &body), Some(&key))
                .await
                .unwrap(),
        );
    }
    for pending in pending {
        live_many(brokers, "confirm live record", pending.confirmed())
            .await
            .unwrap();
    }
}

pub(super) async fn read(
    brokers: &[Broker],
    reader: &mut TopicReader,
    expected: &[History],
    positions: &mut [usize],
) {
    let count = expected
        .iter()
        .zip(positions.iter())
        .map(|(history, &next)| history.len() - next)
        .sum::<usize>();
    for index in 0..count {
        let stage = format!(
            "receive live or repaired record {index}/{count}, positions {positions:?}, checkpoint {:?}, stats {:?}",
            reader.checkpoint(),
            reader.stats(),
        );
        let record = live_many(brokers, &stage, reader.next())
            .await
            .unwrap_or_else(|error| panic!("{stage}: {error:?}"));
        check_record(&record, expected, positions);
    }
}

fn check_record(record: &TopicRecord, expected: &[History], positions: &mut [usize]) {
    let partition = record.partition as usize;
    let next = positions[partition];
    assert_eq!(record.offset.get(), next as u64);
    assert_eq!(record.message_id, expected[partition][next].0);
    assert_eq!(
        record.payload.as_slice(),
        std::slice::from_ref(&expected[partition][next].1)
    );
    positions[partition] += 1;
}

#[tokio::test(flavor = "current_thread")]
async fn plain_thread_writer_compresses_groups_and_reader_restores_original_parts() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            plain_thread(policy).await;
        }
    })
    .await
    .unwrap();
}

async fn plain_thread(policy: Confirmation) {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let mode = if policy == Confirmation::LocalDurable {
        DeploymentMode::Single
    } else {
        DeploymentMode::Three
    };
    let deployment = deployment(root.path(), mode, policy, 1);
    let wire = DataLimits {
        envelope: EnvelopeLimits {
            max_payload_bytes: 12 * 1024,
            ..limits().envelope
        },
        max_parts: 8,
        max_record_bytes: 4096,
        ..limits()
    };
    let roles = handshake::PRODUCER | handshake::CONSUMER;
    let sdk = role_links_with_limits(&runtime, &deployment[0].0, roles, wire).await;
    let brokers = start_brokers(&runtime, deployment).await;
    let threaded = sdk.clone();
    let (done, finished) = tokio::sync::oneshot::channel();
    let thread = std::thread::spawn(move || {
        assert!(tokio::runtime::Handle::try_current().is_err());
        futures::executor::block_on(async {
            let mut writer = SharedTopicWriter::open(
                &threaded,
                "orders",
                SharedTopicWriterConfig::new(wire),
                RetryPolicy::default(),
            )
            .await
            .unwrap();
            let boundary = writer.metadata().policy();
            let mut pending = Vec::new();
            for index in 0..3 {
                pending.push(
                    writer
                        .send(
                            RecordInput::multipart(
                                MessageId::from_bytes([index + 1; 16]),
                                compressed_parts(index),
                            ),
                            None,
                        )
                        .await
                        .unwrap(),
                );
            }
            for (offset, pending) in pending.iter().enumerate() {
                let receipt = pending.confirmed().await.unwrap();
                assert_eq!(receipt.partition, 0);
                assert_eq!(receipt.record.offset, offset as u64);
                assert_eq!(receipt.record.policy, boundary);
                assert_eq!(
                    receipt.record.message_id,
                    MessageId::from_bytes([offset as u8 + 1; 16])
                );
            }
            // These are actual SDK socket-admission sizes, including retries.
            // Both 4 KiB records must be packed rather than sent as raw bytes.
            let stats = writer.partition_stats(0).unwrap();
            assert!(stats.requests > 0);
            assert!(stats.max_payload_bytes < 4096, "{stats:?}");
            writer.close().await.unwrap();
        });
        done.send(()).unwrap();
    });
    live_many(&brokers, "ordinary-thread writer", finished)
        .await
        .unwrap();
    thread.join().unwrap();
    let mut reader = live_many(
        &brokers,
        "open multipart reader",
        TopicReader::open(sdk.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    for index in 0..3 {
        let record = live_many(&brokers, "read multipart record", reader.next())
            .await
            .unwrap();
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset.get(), u64::from(index));
        assert_eq!(record.message_id, MessageId::from_bytes([index + 1; 16]));
        assert_eq!(record.payload.as_slice(), compressed_parts(index));
        assert_eq!(
            reader.checkpoint().positions,
            [(0, ozzy_proto::Offset::new(u64::from(index) + 1))],
        );
    }
    live_many(&brokers, "close multipart reader", reader.close())
        .await
        .unwrap();
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

fn compressed_parts(index: u8) -> Vec<bytes::Bytes> {
    match index {
        0 => vec![bytes::Bytes::from_static(b"tiny")],
        1 => vec![bytes::Bytes::from(vec![b'a'; 4096])],
        _ => vec![
            bytes::Bytes::new(),
            bytes::Bytes::from(vec![b'b'; 4093]),
            bytes::Bytes::new(),
            bytes::Bytes::from_static(b"end"),
        ],
    }
}

#[tokio::test(flavor = "current_thread")]
async fn leader_publishes_confirmed_records_once_and_a_stalled_reader_delays_nobody() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
            publication_check(policy).await;
        }
    })
    .await
    .unwrap();
}

async fn publication_check(policy: Confirmation) {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = deployment(root.path(), DeploymentMode::Three, policy, 1);
    let sdk = links(&runtime, &deployment[0].0).await;
    let brokers = start_brokers(&runtime, deployment).await;
    let mut config = SharedTopicWriterConfig::new(limits());
    config.compress_payloads = false;
    let mut writer = live_many(
        &brokers,
        "open publication writer",
        SharedTopicWriter::open(&sdk, "orders", config, RetryPolicy::default()),
    )
    .await
    .unwrap();
    let metadata = writer.metadata().clone();
    // Confirmation does not drain the broker's separate publication queue.
    let before = publish_record(&brokers, &mut writer, 0).await;
    let route = sdk
        .routes(metadata.clone())
        .unwrap()
        .route(0)
        .unwrap()
        .unwrap();
    let leader = route.leader.unwrap();
    let source = reader::Source::Group {
        authority: Authority {
            group_id: route.group,
            config_epoch: route.config_epoch,
            view: route.view,
        },
        partition: route.partition,
        owner_epoch: before.record.owner_epoch,
    };
    let endpoint = &metadata.broker(leader).unwrap().reader_pub;
    let witness = subscriber(&runtime, endpoint, source, 64).await;
    let history_end =
        settle_publications(&brokers, &mut writer, &witness, leader, source, 0, 1).await;
    witness.close().await.unwrap();
    let active = subscriber(&runtime, endpoint, source, 64).await;
    let mut followers = Vec::new();
    for broker in metadata
        .brokers()
        .iter()
        .filter(|broker| broker.node != leader)
    {
        followers.push(subscriber(&runtime, &broker.reader_pub, source, 64).await);
    }
    let stalled = subscriber(&runtime, endpoint, source, 1).await;
    let next = settle_publications(
        &brokers,
        &mut writer,
        &active,
        leader,
        source,
        history_end,
        history_end,
    )
    .await;

    // Slow SUBs can retain the bounded publication backing. Writers and the
    // SDK reader must still progress, repairing any missed PUB frames over PEER.
    let end = next + 40;
    publication_stream(
        &brokers,
        &sdk,
        &mut writer,
        &active,
        leader,
        source,
        next..end,
    )
    .await;
    let quiet = Duration::from_millis(50);
    let (active_tail, first_follower, second_follower) = tokio::join!(
        tokio::time::timeout(quiet, active.recv()),
        tokio::time::timeout(quiet, followers[0].recv()),
        tokio::time::timeout(quiet, followers[1].recv()),
    );
    assert!(active_tail.is_err(), "leader published a duplicate suffix");
    assert!(first_follower.is_err(), "follower published leader records");
    assert!(
        second_follower.is_err(),
        "follower published leader records"
    );
    live_many(&brokers, "close publication writer", writer.close())
        .await
        .unwrap();
    for subscriber in followers.into_iter().chain([active, stalled]) {
        subscriber.close().await.unwrap();
    }
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

async fn publication_stream(
    brokers: &[Broker],
    sdk: &BrokerLinks,
    writer: &mut SharedTopicWriter,
    active: &Socket,
    leader: NodeId,
    source: reader::Source,
    range: std::ops::Range<u64>,
) {
    let mut reader = live_many(
        brokers,
        "open reader beside stalled subscriber",
        TopicReader::open(
            sdk.clone(),
            "orders",
            TopicReaderConfig {
                start: ozzy_runtime::replicated::ReaderStart::Checkpoint(TopicCheckpoint {
                    topic: writer.metadata().id(),
                    positions: vec![(0, ozzy_proto::Offset::new(range.start))],
                }),
                ..TopicReaderConfig::default()
            },
        ),
    )
    .await
    .unwrap();
    let expected = vec![
        (0..range.end)
            .map(|offset| (publication_id(offset), bytes::Bytes::from_static(b"live")))
            .collect::<History>(),
    ];
    let mut positions = vec![range.start as usize];
    let (finished, mut completion) = tokio::sync::oneshot::channel();
    let streaming = async {
        let appends = async {
            for offset in range.clone() {
                publish_record(brokers, writer, offset).await;
            }
        };
        futures::join!(
            appends,
            read(brokers, &mut reader, &expected, &mut positions)
        );
        finished.send(()).unwrap();
    };
    let publications = async {
        let mut position = range.start;
        let mut stopped = false;
        loop {
            tokio::select! {
                message = active.recv() => {
                    let (first, ids) = published(&message.unwrap(), leader, source);
                    assert!(first >= position, "duplicated or out-of-order publication");
                    position = check_publication(first, first, &ids);
                    assert!(position <= range.end);
                }
                result = &mut completion, if !stopped => {
                    result.unwrap();
                    stopped = true;
                }
                () = tokio::time::sleep(Duration::from_millis(50)), if stopped => break,
            }
        }
    };
    futures::join!(streaming, publications);
    assert_eq!(positions, [range.end as usize]);
    assert_eq!(
        reader.checkpoint().positions,
        [(0, ozzy_proto::Offset::new(range.end))]
    );
    live_many(
        brokers,
        "close reader beside stalled subscriber",
        reader.close(),
    )
    .await
    .unwrap();
}

async fn settle_publications(
    brokers: &[Broker],
    writer: &mut SharedTopicWriter,
    subscriber: &Socket,
    leader: NodeId,
    source: reader::Source,
    minimum: u64,
    mut next: u64,
) -> u64 {
    // A connected SUB may still be installing its filter. Settle each admitted
    // record, and observe actual PUB delivery before testing the full stream.
    live_many(brokers, "publication readiness", async {
        let mut observed = minimum;
        loop {
            publish_record(brokers, writer, next).await;
            next += 1;
            // Startup loss can include this probe. Retry only after settling
            // its confirmation, and stop once its publication is witnessed.
            while let Ok(message) =
                tokio::time::timeout(Duration::from_millis(100), subscriber.recv()).await
            {
                let (first, ids) = published(&message.unwrap(), leader, source);
                assert!(
                    first >= observed,
                    "history or duplicate publication after connecting"
                );
                let position = check_publication(first, first, &ids);
                observed = position;
                assert!(position <= next);
                if position == next {
                    return next;
                }
            }
        }
    })
    .await
}

async fn subscriber(
    runtime: &WriterRuntime,
    endpoint: &str,
    source: reader::Source,
    receive_messages: u32,
) -> Socket {
    let socket = runtime.context().socket(
        SocketType::Sub,
        Options::default().recv_hwm(receive_messages),
    );
    socket
        .subscribe(bytes::Bytes::copy_from_slice(
            &reader::publication_topic(source).unwrap(),
        ))
        .await
        .unwrap();
    socket.connect(endpoint.parse().unwrap()).await.unwrap();
    socket
        .wait_connected(1, Duration::from_secs(5))
        .await
        .unwrap();
    socket
}

async fn publish_record(
    brokers: &[Broker],
    writer: &mut SharedTopicWriter,
    offset: u64,
) -> ozzy_runtime::replicated::SharedTopicReceipt {
    let pending = writer
        .send(
            RecordInput::copy_from_slice(publication_id(offset), b"live"),
            None,
        )
        .await
        .unwrap();
    let receipt = live_many(brokers, "confirm publication record", pending.confirmed())
        .await
        .unwrap();
    assert_eq!(receipt.partition, 0);
    assert_eq!(receipt.record.offset, offset);
    assert_eq!(receipt.record.message_id, publication_id(offset));
    receipt
}

fn published(message: &Message, leader: NodeId, source: reader::Source) -> (u64, Vec<MessageId>) {
    assert_eq!(message.len(), 4);
    assert_eq!(
        message.part_bytes(0).unwrap().as_ref(),
        reader::publication_topic(source).unwrap().as_slice(),
    );
    let frames: [bytes::Bytes; 3] =
        std::array::from_fn(|part| message.part_bytes(part + 1).unwrap());
    let packet =
        ozzy_proto::decode_packet(&frames.each_ref().map(AsRef::as_ref), limits().envelope)
            .unwrap();
    assert_eq!(packet.envelope.sender, leader);
    let publication = reader::decode_publication(packet, limits()).unwrap();
    assert_eq!(publication.header.source, source);
    let ids = publication
        .records
        .iter()
        .map(|record| {
            assert!(record.parts.eq([b"live".as_slice()]));
            record.message_id
        })
        .collect();
    (publication.header.first_offset, ids)
}

fn check_publication(expected: u64, first: u64, ids: &[MessageId]) -> u64 {
    assert_eq!(first, expected, "missing or duplicated publication");
    assert!(!ids.is_empty());
    for (offset, &id) in (first..).zip(ids) {
        assert_eq!(id, publication_id(offset));
    }
    first + ids.len() as u64
}

fn publication_id(offset: u64) -> MessageId {
    let mut bytes = [100; 16];
    bytes[8..].copy_from_slice(&offset.to_be_bytes());
    MessageId::from_bytes(bytes)
}
