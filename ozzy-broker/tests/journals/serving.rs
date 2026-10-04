use super::*;
use ozzy_broker::{Broker, format_partition_journals, initialize_volumes};
use ozzy_proto::{EnvelopeLimits, MessageId, NodeId, data::DataLimits, handshake};
use ozzy_runtime::replicated::{
    AppendLinkLimits, BrokerAddress, BrokerLinks, BrokerLinksConfig, ReaderLinkLimits, RecordInput,
    RetryPolicy, SdkClock, SharedTopicWriter, SharedTopicWriterConfig, TopicReader,
    TopicReaderConfig, WriterRuntime,
};
use std::{task::Poll, time::Duration};

mod failover;
mod large;
mod live;
#[cfg(target_os = "linux")]
mod perf;
mod pressure;
mod progress;
mod recovery;
mod roll;
mod sessions;
mod simulated;

fn deployment(
    root: &Path,
    mode: DeploymentMode,
    policy: Confirmation,
    partitions: u32,
) -> Vec<(CheckedConfig, BrokerIdentity)> {
    deployment_with(root, mode, policy, partitions, |_| {})
}

fn deployment_with(
    root: &Path,
    mode: DeploymentMode,
    policy: Confirmation,
    partitions: u32,
    configure: impl FnOnce(&mut Deployment),
) -> Vec<(CheckedConfig, BrokerIdentity)> {
    deployment_with_resources(root, mode, policy, partitions, &host(), configure)
}

use ozzy_sim::broker::deployment_with_resources;

fn limits() -> DataLimits {
    DataLimits {
        envelope: EnvelopeLimits {
            max_metadata_bytes: 16 * 1024,
            max_payload_bytes: 4096,
        },
        max_records: 4,
        max_parts: 4,
        max_record_bytes: 1024,
    }
}

async fn links(runtime: &WriterRuntime, checked: &CheckedConfig) -> BrokerLinks {
    role_links(runtime, checked, handshake::PRODUCER | handshake::CONSUMER).await
}

async fn role_links(runtime: &WriterRuntime, checked: &CheckedConfig, roles: u32) -> BrokerLinks {
    role_links_with_limits(runtime, checked, roles, limits()).await
}

async fn role_links_with_limits(
    runtime: &WriterRuntime,
    checked: &CheckedConfig,
    roles: u32,
    wire: DataLimits,
) -> BrokerLinks {
    role_links_with_config(runtime, checked, roles, wire, |_| {}).await
}

async fn role_links_with_config(
    runtime: &WriterRuntime,
    checked: &CheckedConfig,
    roles: u32,
    wire: DataLimits,
    configure: impl FnOnce(&mut BrokerLinksConfig),
) -> BrokerLinks {
    let mut parameters = if roles & handshake::PRODUCER != 0 {
        handshake::Parameters::streaming(wire, roles).unwrap()
    } else {
        handshake::Parameters::reader(wire, roles).unwrap()
    };
    parameters.capabilities |= handshake::OWNER_ROUTING;
    parameters.capabilities |= handshake::OWNER_READ;
    let mut config = BrokerLinksConfig {
        local: NodeId::from_bytes(*Uuid::now_v7().as_bytes()),
        brokers: checked
            .identity
            .brokers
            .iter()
            .map(|(name, id)| BrokerAddress {
                node: NodeId::from_bytes(*id.as_bytes()),
                endpoint: checked.deployment.deployment().brokers[name]
                    .endpoints
                    .peer
                    .parse()
                    .unwrap(),
                data_endpoint: checked.deployment.deployment().brokers[name]
                    .endpoints
                    .data_peer
                    .parse()
                    .unwrap(),
            })
            .collect(),
        parameters,
        requests: 12,
        control_bytes: (1024 * 1024).max(16 * wire.max_record_bytes),
        routing_bytes: 1024 * 1024,
        append: (roles & handshake::PRODUCER != 0).then_some(AppendLinkLimits {
            writers: 32,
            requests: 32,
            records: 128,
            bytes: (256 * 1024 * 1024).max(32 * wire.max_record_bytes),
        }),
        maximum_partitions: checked.plan.partitions.len(),
        reader: (roles & handshake::CONSUMER != 0).then_some(ReaderLinkLimits {
            subscriptions: if wire.max_record_bytes > 1024 * 1024 {
                1
            } else {
                32
            },
            bytes: (16 * 1024 * 1024)
                .max(64 * wire.max_record_bytes)
                .max(wire.max_records * wire.max_record_bytes * 256),
            queue_messages: 4,
        }),
        request_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(10),
        clock: SdkClock::default(),
    };
    configure(&mut config);
    BrokerLinks::connect(runtime, config).await.unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn production_single_broker_startup_confirms_topic_writes_and_restarts() {
    tokio::time::timeout(Duration::from_secs(20), single())
        .await
        .unwrap();
}

async fn single() {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let mut deployment = deployment(
        root.path(),
        DeploymentMode::Single,
        Confirmation::LocalDurable,
        4,
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
    let deployment = checked.deployment.clone();
    let identity = checked.identity.clone();
    let mut next_offsets = [0; 4];
    let mut expected = vec![Vec::new(); 4];
    for turn in 0..3 {
        let checked = CheckedConfig {
            plan: deployment.broker_plan("broker-0", &host()).unwrap(),
            deployment: deployment.clone(),
            identity: identity.clone(),
        };
        let sdk = links(&runtime, &checked).await;
        let broker =
            Broker::start_trusted_with_context(checked, local.clone(), runtime.context().clone())
                .await
                .unwrap();
        assert_eq!(broker.application_threads(), 1);
        assert_eq!(broker.dispatcher_threads(), 1);
        assert_eq!(broker.io_threads(), 1);
        let mut writer = live(
            &broker,
            SharedTopicWriter::open(
                &sdk,
                "orders",
                SharedTopicWriterConfig::new(limits()),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        let keys = (0..4)
            .map(|number| {
                (0_u32..1000)
                    .map(u32::to_be_bytes)
                    .find(|key| writer.metadata().keyed_partition(key).number == number)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut pending = Vec::new();
        for round in 0..4 {
            for (partition, key) in keys.iter().enumerate() {
                let id = MessageId::from_bytes(*Uuid::now_v7().as_bytes());
                let body = bytes::Bytes::from(format!("partition-{partition}-round-{round}"));
                expected[partition].push((id, body.clone()));
                let record = RecordInput::copy_from_slice(id, &body);
                pending.push(writer.send(record, Some(key)).await.unwrap());
            }
        }
        for pending in pending {
            let receipt = live(&broker, pending.confirmed()).await.unwrap();
            assert_eq!(
                receipt.record.policy,
                ozzy_proto::append::Policy::LocalDurable
            );
            let partition = receipt.partition as usize;
            assert_eq!(receipt.record.offset, next_offsets[partition]);
            next_offsets[partition] += 1;
        }
        read_topic(std::slice::from_ref(&broker), &sdk, &expected).await;
        if turn == 0 {
            live::check(
                std::slice::from_ref(&broker),
                &sdk,
                &mut writer,
                &mut expected,
            )
            .await;
            for (next, records) in next_offsets.iter_mut().zip(&expected) {
                *next = records.len() as u64;
            }
        }
        live(&broker, writer.close()).await.unwrap();
        sdk.shutdown().await.unwrap();
        close_broker(broker, turn).await;
    }
}

async fn close_broker(broker: Broker, turn: usize) {
    if turn == 0 {
        let shutdown = broker.shutdown();
        drop(shutdown);
        broker.closed().await.unwrap();
    } else if turn == 1 {
        let closed = broker.closed();
        drop(broker);
        closed.await.unwrap();
    } else {
        broker.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn production_three_brokers_use_asymmetric_shards_and_confirm_disk_quorum() {
    tokio::time::timeout(
        Duration::from_secs(30),
        replicated(Confirmation::DiskQuorum),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn production_three_brokers_confirm_replicated_persisting() {
    tokio::time::timeout(
        Duration::from_secs(30),
        replicated(Confirmation::ReplicatedPersisting),
    )
    .await
    .unwrap();
}

async fn replicated(policy: Confirmation) {
    replicated_partitions(policy, 6).await;
}

#[tokio::test(flavor = "current_thread")]
async fn production_shard_turns_reach_every_partition_beyond_the_first_sixteen() {
    tokio::time::timeout(
        Duration::from_secs(30),
        replicated_partitions(Confirmation::DiskQuorum, 20),
    )
    .await
    .unwrap();
}

async fn replicated_partitions(policy: Confirmation, partitions: u32) {
    let root = tempfile::tempdir().unwrap();
    let runtime = WriterRuntime::new().unwrap();
    let deployment = deployment(root.path(), DeploymentMode::Three, policy, partitions);
    let restart = (
        CheckedConfig {
            deployment: deployment[0].0.deployment.clone(),
            identity: deployment[0].0.identity.clone(),
            plan: deployment[0].0.plan.clone(),
        },
        deployment[0].1.clone(),
    );
    let sdk = links(&runtime, &deployment[0].0).await;
    let mut brokers = start_brokers(&runtime, deployment).await;
    let mut writer = live_many(
        &brokers,
        "open writer",
        SharedTopicWriter::open(
            &sdk,
            "orders",
            SharedTopicWriterConfig::new(limits()),
            RetryPolicy::default(),
        ),
    )
    .await
    .unwrap();
    if partitions >= 3 {
        let initial_leaders: std::collections::BTreeSet<_> = (0..partitions)
            .map(|number| writer.metadata().partition(number).unwrap().members[0])
            .collect();
        assert_eq!(initial_leaders.len(), 3);
    }
    let mut pending = Vec::new();
    let mut expected = vec![Vec::new(); partitions as usize];
    for partition in 0..partitions {
        let key = (0_u32..1000)
            .map(u32::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number == partition)
            .unwrap();
        for sequence in 0..4 {
            let id = MessageId::from_bytes(*Uuid::now_v7().as_bytes());
            let body = bytes::Bytes::from(format!("partition-{partition}-record-{sequence}"));
            expected[partition as usize].push((id, body.clone()));
            pending.push(
                writer
                    .send(RecordInput::copy_from_slice(id, &body), Some(&key))
                    .await
                    .unwrap(),
            );
        }
    }
    let mut next = vec![0; partitions as usize];
    for (index, pending) in pending.into_iter().enumerate() {
        let receipt = live_many(
            &brokers,
            &format!("confirm record {index}"),
            pending.confirmed(),
        )
        .await
        .unwrap();
        assert_eq!(
            receipt.record.policy,
            if policy == Confirmation::DiskQuorum {
                ozzy_proto::append::Policy::QuorumDurable
            } else {
                ozzy_proto::append::Policy::QuorumReplicatedPersisting
            }
        );
        assert_eq!(receipt.record.offset, next[receipt.partition as usize]);
        next[receipt.partition as usize] += 1;
    }
    if partitions <= 16 {
        // Separate subscriptions share the physical PEER links. Canceling and
        // replacing one reader must not consume or fence the other's records.
        futures::join!(
            read_topic(&brokers, &sdk, &expected),
            read_topic(&brokers, &sdk, &expected),
        );
    } else {
        read_topic(&brokers, &sdk, &expected).await;
    }
    if partitions <= 16 {
        live::check(&brokers, &sdk, &mut writer, &mut expected).await;
        failover::check(
            &runtime,
            &mut brokers,
            restart,
            &sdk,
            &mut writer,
            &mut expected,
        )
        .await;
    }
    live_many(&brokers, "close writer", writer.close())
        .await
        .unwrap();
    sdk.shutdown().await.unwrap();
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
}

async fn start_brokers(
    runtime: &WriterRuntime,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
) -> Vec<Broker> {
    let mut brokers = Vec::new();
    for (checked, local) in deployment {
        for device in checked.deployment.deployment().brokers[&checked.plan.name]
            .devices
            .values()
        {
            std::fs::create_dir(&device.root).unwrap();
        }
        initialize_volumes(&checked, &local).unwrap();
        format_partition_journals(&checked, &local).await.unwrap();
        let expected_shards = checked.plan.shards.len();
        let broker = Broker::start_trusted_with_context(checked, local, runtime.context().clone())
            .await
            .unwrap();
        assert_eq!(broker.application_threads(), expected_shards);
        assert_eq!(broker.dispatcher_threads(), 1);
        brokers.push(broker);
    }
    brokers
}

async fn read_topic(
    brokers: &[Broker],
    sdk: &BrokerLinks,
    expected: &[Vec<(MessageId, bytes::Bytes)>],
) {
    let sockets = sdk.socket_count();
    let mut reader = live_many(
        brokers,
        "open topic reader",
        TopicReader::open(sdk.clone(), "orders", TopicReaderConfig::default()),
    )
    .await
    .unwrap();
    let mut canceled = Box::pin(reader.next());
    let mut first = match futures::poll!(canceled.as_mut()) {
        Poll::Pending => None,
        Poll::Ready(record) => Some(record.unwrap()),
    };
    drop(canceled);
    let first_partition = first.as_ref().map(|record| record.partition);
    assert!(
        reader
            .checkpoint()
            .positions
            .iter()
            .all(|&(partition, next)| next.get() == u64::from(first_partition == Some(partition)))
    );
    let mut offsets = vec![0; expected.len()];
    let total = expected.iter().map(Vec::len).sum::<usize>();
    for index in 0..total {
        if index == total / 2 {
            let checkpoint = reader.checkpoint();
            let canceled = reader.close();
            drop(canceled);
            live_many(brokers, "close checkpointed reader", reader.close())
                .await
                .unwrap();
            reader = live_many(
                brokers,
                "resume topic reader",
                TopicReader::open(
                    sdk.clone(),
                    "orders",
                    TopicReaderConfig {
                        start: ozzy_runtime::replicated::ReaderStart::Checkpoint(checkpoint),
                        ..TopicReaderConfig::default()
                    },
                ),
            )
            .await
            .unwrap();
        }
        let record = if let Some(record) = first.take() {
            record
        } else {
            live_many(
                brokers,
                &format!("read confirmed record {index}, offsets {offsets:?}"),
                reader.next(),
            )
            .await
            .unwrap()
        };
        let partition = record.partition as usize;
        let offset = offsets[partition];
        assert_eq!(record.offset.get(), offset as u64);
        assert_eq!(record.message_id, expected[partition][offset].0);
        assert_eq!(
            record.payload.as_slice(),
            std::slice::from_ref(&expected[partition][offset].1)
        );
        offsets[partition] += 1;
    }
    assert_eq!(offsets, expected.iter().map(Vec::len).collect::<Vec<_>>());
    for (partition, &next) in offsets.iter().enumerate() {
        live_many(
            brokers,
            "acknowledge processed records",
            reader.acknowledge(
                partition as u32,
                next.checked_sub(1)
                    .map(|n| ozzy_proto::Offset::new(n as u64)),
            ),
        )
        .await
        .unwrap();
    }
    assert_eq!(sdk.socket_count(), sockets);
    live_many(brokers, "close topic reader", reader.close())
        .await
        .unwrap();
}

async fn live<T>(broker: &Broker, operation: impl std::future::Future<Output = T>) -> T {
    live_many(
        std::slice::from_ref(broker),
        "single-broker SDK operation",
        operation,
    )
    .await
}

async fn live_many<T>(
    brokers: &[Broker],
    stage: &str,
    operation: impl std::future::Future<Output = T>,
) -> T {
    tokio::select! {
        result = operation => result,
        (result, broker, _) = futures::future::select_all(brokers.iter().map(|broker| Box::pin(broker.closed()))) => {
            panic!("broker {broker} exited during {stage}: {result:?}")
        },
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("no progress during {stage}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn production_failed_frontend_bind_drains_shards_before_retry() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let root = tempfile::tempdir().unwrap();
        let runtime = WriterRuntime::new().unwrap();
        let (checked, local) = deployment(
            root.path(),
            DeploymentMode::Single,
            Confirmation::LocalDurable,
            4,
        )
        .pop()
        .unwrap();
        for device in checked.deployment.deployment().brokers[&checked.plan.name]
            .devices
            .values()
        {
            std::fs::create_dir(&device.root).unwrap();
        }
        initialize_volumes(&checked, &local).unwrap();
        format_partition_journals(&checked, &local).await.unwrap();
        let blocker = runtime
            .context()
            .socket(omq_tokio::SocketType::Pub, omq_tokio::Options::default());
        blocker
            .bind(
                checked.deployment.deployment().brokers[&checked.plan.name]
                    .endpoints
                    .reader_pub
                    .parse()
                    .unwrap(),
            )
            .await
            .unwrap();
        let retry = CheckedConfig {
            deployment: checked.deployment.clone(),
            identity: checked.identity.clone(),
            plan: checked.plan.clone(),
        };
        assert!(
            Broker::start_trusted_with_context(checked, local.clone(), runtime.context().clone())
                .await
                .is_err()
        );
        blocker.close().await.unwrap();
        let broker = Broker::start_trusted_with_context(retry, local, runtime.context().clone())
            .await
            .unwrap();
        broker.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn production_startup_refuses_missing_history_and_drains_before_explicit_format() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let root = tempfile::tempdir().unwrap();
        let runtime = WriterRuntime::new().unwrap();
        let (checked, local) = deployment(
            root.path(),
            DeploymentMode::Single,
            Confirmation::LocalDurable,
            4,
        )
        .pop()
        .unwrap();
        for device in checked.deployment.deployment().brokers[&checked.plan.name]
            .devices
            .values()
        {
            std::fs::create_dir(&device.root).unwrap();
        }
        initialize_volumes(&checked, &local).unwrap();
        let stores = checked
            .plan
            .partitions
            .iter()
            .map(|partition| partition.directory.clone())
            .collect::<Vec<_>>();
        assert!(
            Broker::start_trusted_with_context(
                CheckedConfig {
                    plan: checked
                        .deployment
                        .broker_plan(&checked.plan.name, &host())
                        .unwrap(),
                    deployment: checked.deployment.clone(),
                    identity: checked.identity.clone(),
                },
                local.clone(),
                runtime.context().clone()
            )
            .await
            .is_err()
        );
        for store in stores {
            assert!(!store.exists());
        }
        format_partition_journals(&checked, &local).await.unwrap();
        let broker = Broker::start_trusted_with_context(checked, local, runtime.context().clone())
            .await
            .unwrap();
        broker.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
