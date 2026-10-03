use super::{SINGLE, host, ids};
use ozzy_broker::{CheckedConfig, JournalConfig, JournalPlan, PartitionAuthority};
use ozzy_config::{
    Affinity, BrokerIdentity, Confirmation, Deployment, DeploymentMode, IoBackend, QueueBudget,
    Shard,
};
use ozzy_replication::{Digest, JournalGeneration};
use std::{collections::BTreeMap, path::Path};
use uuid::Uuid;

mod actors;
mod format;
mod local;
mod serving;
mod startup;

fn fixture(
    root: &Path,
    mode: DeploymentMode,
    policy: Confirmation,
    partitions: u32,
) -> Vec<(CheckedConfig, BrokerIdentity, JournalPlan)> {
    fixture_backend(root, mode, policy, partitions, IoBackend::Pool)
}

fn fixture_backend(
    root: &Path,
    mode: DeploymentMode,
    policy: Confirmation,
    partitions: u32,
    backend: IoBackend,
) -> Vec<(CheckedConfig, BrokerIdentity, JournalPlan)> {
    let mut config = Deployment::parse(SINGLE).unwrap();
    config.cluster.mode = mode;
    let topic = config.topics.get_mut("orders").unwrap();
    topic.partitions = partitions;
    topic.confirmation = policy;
    topic.segment_bytes = 1024 * 1024;
    topic.max_append_bytes = 64 * 1024;
    let template = config.brokers.remove("laptop").unwrap();
    let count = if mode == DeploymentMode::Single { 1 } else { 3 };
    for index in 0..count {
        let mut broker = template.clone();
        broker.endpoints.peer = format!("tcp://127.0.0.1:{}", 7100 + index * 4);
        broker.endpoints.data_peer = format!("tcp://127.0.0.1:{}", 7103 + index * 4);
        broker.endpoints.reader_pub = format!("tcp://127.0.0.1:{}", 7101 + index * 4);
        let device = broker.devices.get_mut("ssd").unwrap();
        device.root = root.join(format!("broker-{index}"));
        device.workers.backend = backend;
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
    let deployment = config.validate().unwrap();
    let identity = deployment.initialize(ids()).unwrap();
    identity
        .brokers
        .keys()
        .map(|name| {
            let plan = deployment.broker_plan(name, &host()).unwrap();
            let local = deployment
                .initialize_broker_identity(&identity, name, Uuid::now_v7)
                .unwrap();
            let checked = CheckedConfig {
                deployment: deployment.clone(),
                identity: identity.clone(),
                plan,
            };
            let journals = JournalPlan::from_trusted_deployment(&checked, &local).unwrap();
            (checked, local, journals)
        })
        .collect()
}

fn principals(identity: &ozzy_config::DeploymentIdentity) -> BTreeMap<Uuid, Digest> {
    identity
        .brokers
        .iter()
        .map(|(name, id)| (*id, Digest::from_bytes(identity.principals[name])))
        .collect()
}

fn storage(checked: &CheckedConfig, local: &BrokerIdentity) {
    for device in checked.deployment.deployment().brokers[&checked.plan.name]
        .devices
        .values()
    {
        std::fs::create_dir(&device.root).unwrap();
    }
    ozzy_broker::initialize_volumes(checked, local).unwrap();
    ozzy_broker::check_volumes(checked, local).unwrap();
    for placement in &checked.plan.partitions {
        let parent = placement.directory.parent().unwrap();
        if !parent.exists() {
            std::fs::create_dir_all(parent).unwrap();
        }
    }
}

#[test]
fn journal_membership_is_identical_across_different_local_shard_counts() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let directory = tempfile::tempdir().unwrap();
        let brokers = fixture(directory.path(), DeploymentMode::Three, policy, 6);
        for partition in 0..6 {
            let configs: Vec<_> = brokers
                .iter()
                .map(|(_, _, plan)| {
                    let JournalConfig::Replicated(config) = &plan.partitions[partition].config
                    else {
                        panic!("replicated placement downgraded");
                    };
                    config
                })
                .collect();
            assert!(
                configs
                    .windows(2)
                    .all(|pair| pair[0].configuration == pair[1].configuration)
            );
            let members = &brokers[0].0.identity.topics["orders"].partitions[partition].members;
            assert_eq!(
                configs[0]
                    .configuration
                    .voters()
                    .map(|voter| *voter.node_id.as_bytes()),
                std::array::from_fn(|index| *members[index].as_bytes())
            );
            assert_ne!(configs[0].identity.store_id, configs[1].identity.store_id);
            assert_ne!(configs[0].identity.volume_id, configs[1].identity.volume_id);
        }
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

#[test]
fn native_journal_planning_rejects_impossible_budgets_and_unbound_principals() {
    let directory = tempfile::tempdir().unwrap();
    let (mut checked, local, _) = fixture(
        directory.path(),
        DeploymentMode::Single,
        Confirmation::LocalDurable,
        1,
    )
    .remove(0);
    let principals = principals(&checked.identity);
    assert!(JournalPlan::new(&checked, &local, &BTreeMap::new()).is_err());
    checked.plan.controllers[0].workers.queued_bytes = 64 * 1024;
    assert!(JournalPlan::new(&checked, &local, &principals).is_err());
    checked.plan.controllers[0].workers.queued_bytes = 4 * 1024 * 1024;
    checked.plan.controllers[0].workers.progress_bytes = 8192;
    assert!(JournalPlan::new(&checked, &local, &principals).is_err());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn native_actor_transfer_limits_match_across_asymmetric_shard_budgets() {
    let directory = tempfile::tempdir().unwrap();
    let brokers = fixture(
        directory.path(),
        DeploymentMode::Three,
        Confirmation::DiskQuorum,
        6,
    );
    let mut config = brokers[0].0.deployment.deployment().clone();
    config.brokers.get_mut("broker-1").unwrap().topology.shards[0]
        .budget
        .append_slots = 8;
    let deployment = config.validate().unwrap();
    let mut pipelines = std::collections::BTreeSet::new();
    for (mut checked, local, _) in brokers {
        checked.plan = deployment.broker_plan(&checked.plan.name, &host()).unwrap();
        checked.deployment = deployment.clone();
        let plan = JournalPlan::new(&checked, &local, &principals(&checked.identity)).unwrap();
        for partition in plan.partitions {
            let ozzy_broker::ActorSettings::Replicated { actor, .. } = partition.actors else {
                panic!("replicated group downgraded");
            };
            assert_eq!(actor.transfer.max_operations, 8);
            assert_eq!(actor.transfer.max_body_bytes, 64 * 1024);
            assert!(actor.pipeline.max_operations >= actor.transfer.max_operations);
            pipelines.insert(actor.pipeline.max_operations);
        }
    }
    assert_eq!(pipelines, [8, 256].into());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
