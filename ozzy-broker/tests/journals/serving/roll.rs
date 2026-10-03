use super::*;

const RECORD_BYTES: usize = 8 * 1024;
const RECORDS_PER_TOPIC: usize = 160;

#[tokio::test(flavor = "current_thread")]
async fn production_two_topics_roll_and_replay_after_restart() {
    tokio::time::timeout(Duration::from_secs(90), run())
        .await
        .unwrap();
}

async fn run() {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let mut deployment = deployment_with(
        root.path(),
        DeploymentMode::Single,
        Confirmation::LocalDurable,
        1,
        |config| {
            let second = config.topics["orders"].clone();
            config.topics.insert("events".into(), second);
        },
    );
    let (checked, local) = deployment.pop().unwrap();
    for device in checked.deployment.deployment().brokers[&checked.plan.name]
        .devices
        .values()
    {
        std::fs::create_dir(&device.root).unwrap();
    }
    initialize_volumes(&checked, &local).unwrap();
    format_partition_journals(&checked, &local).await.unwrap();
    let wire = DataLimits {
        envelope: EnvelopeLimits {
            max_metadata_bytes: 16 * 1024,
            max_payload_bytes: 32 * 1024,
        },
        max_records: 4,
        max_parts: 4,
        max_record_bytes: RECORD_BYTES,
    };
    let mut expected: [Vec<(MessageId, bytes::Bytes)>; 2] = std::array::from_fn(|_| Vec::new());
    for turn in 0..2 {
        let sdk = role_links_with_limits(
            &runtime,
            &checked,
            handshake::PRODUCER | handshake::CONSUMER,
            wire,
        )
        .await;
        let broker = Broker::start_trusted_with_context(
            CheckedConfig {
                plan: checked
                    .deployment
                    .broker_plan(&checked.plan.name, &host())
                    .unwrap(),
                deployment: checked.deployment.clone(),
                identity: checked.identity.clone(),
            },
            local.clone(),
            runtime.context().clone(),
        )
        .await
        .unwrap();
        for (index, topic) in ["orders", "events"].into_iter().enumerate() {
            if turn == 0 {
                expected[index] = write_topic(&broker, &sdk, topic, index, wire).await;
            } else {
                read_topic_records(&broker, &sdk, topic, &expected[index]).await;
            }
        }
        sdk.shutdown().await.unwrap();
        broker.shutdown().await.unwrap();
        if turn == 0 {
            for partition in &checked.plan.partitions {
                let segments = std::fs::read_dir(partition.directory.join("segments"))
                    .unwrap()
                    .filter_map(Result::ok)
                    .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
                    .count();
                assert!(
                    segments >= 2,
                    "{} did not roll",
                    partition.directory.display()
                );
            }
        }
    }
}

async fn write_topic(
    broker: &Broker,
    sdk: &BrokerLinks,
    topic: &str,
    seed: usize,
    wire: DataLimits,
) -> Vec<(MessageId, bytes::Bytes)> {
    let mut writer = live(
        broker,
        SharedTopicWriter::open(
            sdk,
            topic,
            SharedTopicWriterConfig::new(wire),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    let mut expected = Vec::new();
    for batch in 0..RECORDS_PER_TOPIC / 4 {
        let mut pending = Vec::new();
        for item in 0..4 {
            let offset = batch * 4 + item;
            let id = MessageId::from_bytes(*Uuid::now_v7().as_bytes());
            let body = payload(seed * RECORDS_PER_TOPIC + offset);
            pending.push(
                writer
                    .send(RecordInput::single(id, body.clone()), None)
                    .await
                    .unwrap(),
            );
            expected.push((id, body));
        }
        for (item, pending) in pending.into_iter().enumerate() {
            let receipt = live(broker, pending.confirmed()).await.unwrap();
            assert_eq!(receipt.partition, 0);
            assert_eq!(receipt.record.offset, (batch * 4 + item) as u64);
        }
    }
    live(broker, writer.close()).await.unwrap();
    expected
}

async fn read_topic_records(
    broker: &Broker,
    sdk: &BrokerLinks,
    topic: &str,
    expected: &[(MessageId, bytes::Bytes)],
) {
    let mut reader = live(
        broker,
        TopicReader::open(sdk.clone(), topic, TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    for (offset, (id, body)) in expected.iter().enumerate() {
        let record = live(broker, reader.next())
            .await
            .unwrap_or_else(|error| panic!("{topic} replay at offset {offset}: {error:?}"));
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset.get(), offset as u64);
        assert_eq!(record.message_id, *id);
        assert_eq!(record.payload.as_slice(), std::slice::from_ref(body));
    }
    live(broker, reader.close()).await.unwrap();
}

fn payload(seed: usize) -> bytes::Bytes {
    let mut body = vec![0; RECORD_BYTES];
    let mut state = (seed as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    for chunk in body.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    body.into()
}
