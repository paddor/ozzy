use super::*;

const MIB: usize = 1024 * 1024;

#[tokio::test(flavor = "current_thread")]
async fn production_large_records_survive_single_broker_restart() {
    tokio::time::timeout(Duration::from_secs(120), run())
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
            let topic = config.topics.get_mut("orders").unwrap();
            topic.segment_bytes = 256 * MIB as u64;
            topic.max_append_bytes = 112 * MIB as u64;
            let broker = config.brokers.get_mut("broker-0").unwrap();
            let workers = &mut broker.devices.get_mut("ssd").unwrap().workers;
            workers.queued_bytes = 256 * MIB as u64;
            for shard in &mut broker.topology.shards {
                shard.budget.resident_bytes = 512 * MIB as u64;
            }
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
            max_payload_bytes: 100 * MIB,
        },
        max_records: 1,
        max_parts: 1,
        max_record_bytes: 100 * MIB,
    };
    let mut expected = Vec::new();
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
        if turn == 0 {
            expected = write_records(&broker, &sdk, wire).await;
        } else {
            read_records(&broker, &sdk, &expected).await;
        }
        sdk.shutdown().await.unwrap();
        broker.shutdown().await.unwrap();
    }
}

async fn write_records(
    broker: &Broker,
    sdk: &BrokerLinks,
    wire: DataLimits,
) -> Vec<(MessageId, bytes::Bytes)> {
    let mut writer = within(
        broker,
        "open writer",
        SharedTopicWriter::open(
            sdk,
            "orders",
            SharedTopicWriterConfig::new(wire),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    let mut expected = Vec::new();
    for (offset, size) in [5, 10, 100].into_iter().enumerate() {
        let id = MessageId::from_bytes(*Uuid::now_v7().as_bytes());
        let body = body(size * MIB);
        let pending = writer
            .send(RecordInput::single(id, body.clone()), None)
            .await
            .unwrap();
        let receipt = tokio::select! {
            result = pending.confirmed() => result.unwrap(),
            result = broker.closed() => panic!("broker exited before {size} MiB confirmation: {result:?}"),
            () = tokio::time::sleep(Duration::from_secs(10)) => {
                panic!("{size} MiB confirmation stalled; writer={:?}", writer.partition_stats(0));
            }
        };
        assert_eq!(receipt.partition, 0);
        assert_eq!(receipt.record.offset, offset as u64);
        assert_eq!(
            receipt.record.policy,
            ozzy_proto::append::Policy::LocalDurable
        );
        expected.push((id, body));
    }
    within(broker, "close writer", writer.close())
        .await
        .unwrap();
    expected
}

async fn read_records(broker: &Broker, sdk: &BrokerLinks, expected: &[(MessageId, bytes::Bytes)]) {
    let mut reader = within(
        broker,
        "open reader",
        TopicReader::open(sdk.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    for (offset, (id, body)) in expected.iter().enumerate() {
        let record = within(broker, &format!("read offset {offset}"), reader.next())
            .await
            .unwrap();
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset.get(), offset as u64);
        assert_eq!(record.message_id, *id);
        assert_eq!(record.payload.len(), 1);
        assert_eq!(&record.payload[0], body);
    }
    within(broker, "close reader", reader.close())
        .await
        .unwrap();
}

fn body(size: usize) -> bytes::Bytes {
    let mut result = vec![0; size];
    let mut state = size as u64 | 1;
    for chunk in result.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    result.into()
}

async fn within<T>(
    broker: &Broker,
    stage: &str,
    operation: impl std::future::Future<Output = T>,
) -> T {
    tokio::select! {
        result = operation => result,
        result = broker.closed() => panic!("broker exited during {stage}: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(30)) => panic!("large-record {stage} stalled"),
    }
}
