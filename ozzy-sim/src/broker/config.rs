//! Shared deployment placement for short integration and sustained simulation.
use ozzy_broker::CheckedConfig;
use ozzy_config::{
    Affinity, BrokerIdentity, Confirmation, Deployment, DeploymentMode, IoBackend, QueueBudget,
    Shard,
};
use std::path::Path;
use uuid::Uuid;

const SINGLE: &str = include_str!("../../../ozzy-config/tests/fixtures/single.toml");

/// Build trusted inproc endpoints and bounded shards without opening journal files.
pub fn deployment_with_resources(
    root: &Path,
    mode: DeploymentMode,
    policy: Confirmation,
    partitions: u32,
    resources: &ozzy_config::HostResources,
    configure: impl FnOnce(&mut Deployment),
) -> Vec<(CheckedConfig, BrokerIdentity)> {
    let mut config = Deployment::parse(SINGLE).unwrap();
    config.cluster.mode = mode;
    let topic = config.topics.get_mut("orders").unwrap();
    topic.partitions = partitions;
    topic.confirmation = policy;
    topic.segment_bytes = 1024 * 1024;
    topic.max_append_bytes = 64 * 1024;
    let template = config.brokers.remove("laptop").unwrap();
    for index in 0..if mode == DeploymentMode::Single { 1 } else { 3 } {
        let mut broker = template.clone();
        let namespace = uuid::Uuid::now_v7();
        broker.endpoints.peer = format!("inproc://{namespace}-peer");
        broker.endpoints.data_peer = format!("inproc://{namespace}-data");
        broker.endpoints.reader_pub = format!("inproc://{namespace}-readers");
        broker.endpoints.follower_pub =
            (mode == DeploymentMode::Three).then(|| format!("inproc://{namespace}-followers"));
        let device = broker.devices.get_mut("ssd").unwrap();
        device.root = root.join(format!("broker-{index}"));
        device.workers.backend = IoBackend::Pool;
        device.workers.write_threads = 1;
        broker.topology.shards = (0..=index)
            .map(|shard| Shard {
                id: shard * 7,
                device: "ssd".into(),
                affinity: Affinity::default(),
                budget: QueueBudget::default(),
            })
            .collect();
        config.brokers.insert(format!("broker-{index}"), broker);
    }
    configure(&mut config);
    let deployment = config.validate().unwrap();
    let identity = deployment
        .initialize({
            let mut value = 0;
            move || {
                value += 1;
                Uuid::from_u128(value)
            }
        })
        .unwrap();
    identity
        .brokers
        .keys()
        .map(|name| {
            let local = deployment
                .initialize_broker_identity(&identity, name, Uuid::now_v7)
                .unwrap();
            (
                CheckedConfig {
                    plan: deployment.broker_plan(name, resources).unwrap(),
                    deployment: deployment.clone(),
                    identity: identity.clone(),
                },
                local,
            )
        })
        .collect()
}
