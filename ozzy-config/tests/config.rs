use std::collections::{BTreeMap, BTreeSet};

use ozzy_config::{
    Affinity, Confirmation, Deployment, DeploymentIdentity, DeploymentMode, HostResources,
    IoBackend, MemoryPool, PartitionOverride, QueueBudget, Shard,
};
use uuid::Uuid;

const SINGLE: &str = include_str!("fixtures/single.toml");

mod broker_identity;

fn single() -> Deployment {
    Deployment::parse(SINGLE).unwrap()
}

fn three() -> Deployment {
    let mut config = single();
    config.cluster.mode = DeploymentMode::Three;
    config.topics.get_mut("orders").unwrap().confirmation = Confirmation::DiskQuorum;
    let broker = config.brokers.remove("laptop").unwrap();
    for name in ["a", "b", "c"] {
        let mut broker = broker.clone();
        broker.endpoints.peer = format!("inproc://{name}-peer");
        broker.endpoints.data_peer = format!("inproc://{name}-data");
        broker.endpoints.reader_pub = format!("inproc://{name}-pub");
        config.brokers.insert(name.to_owned(), broker);
    }
    config
}

fn ids() -> impl FnMut() -> Uuid {
    let mut value = 0;
    move || {
        value += 1;
        Uuid::from_u128(value)
    }
}

fn host() -> HostResources {
    HostResources {
        cpus: [(2, Some(0)), (6, Some(0)), (10, Some(1)), (14, Some(1))].into(),
        memory_nodes: [0, 1].into(),
        linux_aio: true,
    }
}

fn shard(id: u32) -> Shard {
    Shard {
        id,
        device: "ssd".to_owned(),
        affinity: Affinity::default(),
        budget: QueueBudget::default(),
    }
}

#[test]
fn minimal_single_broker_has_production_defaults() {
    let validated = single().validate().unwrap();
    let plan = validated.broker_plan("laptop", &host()).unwrap();
    assert_eq!(plan.omq_io_threads, 1);
    assert_eq!(plan.shards.len(), 1);
    assert_eq!(plan.partitions.len(), 16);
    assert!(plan.partitions.iter().all(|part| part.shard == 0));
    assert_eq!(
        plan.partitions[3].directory.to_str().unwrap(),
        "/var/lib/ozzy/data/topics/orders/partitions/3"
    );
    assert_eq!(validated.partition_for_key("orders", b"").unwrap(), 2);
}

#[test]
fn retention_limits_are_optional_positive_and_fit_a_segment() {
    let source =
        format!("{SINGLE}\n[topics.orders.retention]\nmax_age_secs = 60\nmax_bytes = 536870912\n");
    let config = Deployment::parse(&source).unwrap();
    let retention = config.topics["orders"].retention;
    assert_eq!(retention.max_age_secs, Some(60));
    assert_eq!(retention.max_bytes, Some(536_870_912));
    config.validate().unwrap();
    for invalid in [
        "max_age_secs = 0",
        "max_bytes = 0",
        "max_bytes = 4096",
        "max_age_secs = 18446744073709552",
    ] {
        let source = format!("{SINGLE}\n[topics.orders.retention]\n{invalid}\n");
        let result = Deployment::parse(&source).and_then(Deployment::validate);
        assert!(result.is_err(), "accepted {invalid}");
    }
}

#[test]
fn shared_controller_plan_uses_one_lane_per_sparse_shard() {
    let mut config = single();
    let broker = config.brokers.get_mut("laptop").unwrap();
    let mut second = broker.devices["ssd"].clone();
    second.root = "/second/data".into();
    broker.devices.insert("second".into(), second);
    let mut other = shard(91);
    other.device = "second".into();
    broker.topology.shards = vec![other, shard(7)];
    let plan = config
        .validate()
        .unwrap()
        .broker_plan("laptop", &host())
        .unwrap();
    assert_eq!(plan.controllers.len(), 1);
    assert_eq!(plan.controllers[0].shards, [7, 91]);
    assert_eq!(plan.controllers[0].workers.queued_jobs, 256);
    assert_eq!(plan.partitions[0].shard, 7);
    assert_eq!(plan.partitions[1].shard, 91);
}

#[test]
fn controller_shares_cannot_multiply_or_starve_a_shards_budget() {
    for shortage in 0..4 {
        let mut config = single();
        let broker = config.brokers.get_mut("laptop").unwrap();
        broker.topology.shards = vec![shard(3), shard(9)];
        let workers = &mut broker.devices.get_mut("ssd").unwrap().workers;
        match shortage {
            0 => workers.progress_jobs = 1,
            1 => workers.open_handles = 1,
            2 => workers.queued_bytes = 8 * 1024 * 1024,
            _ => workers.progress_bytes = 1,
        }
        assert!(
            config
                .validate()
                .unwrap()
                .broker_plan("laptop", &host())
                .is_err()
        );
    }
}

#[test]
fn shard_budget_covers_receive_and_preparation_for_one_maximum_append() {
    let mut config = single();
    let body = config.topics["orders"].max_append_bytes;
    let broker = config.brokers.get_mut("laptop").unwrap();
    broker.topology.shards = vec![shard(0)];
    broker.topology.shards[0].budget.resident_bytes = body * 3;
    assert!(config.clone().validate().is_err());
    config.brokers.get_mut("laptop").unwrap().topology.shards[0]
        .budget
        .resident_bytes = body * 4;
    assert!(config.validate().is_ok());
}

#[test]
fn topic_append_body_cannot_exceed_eight_mib() {
    let mut config = single();
    assert!(config.clone().validate().is_ok());
    config.topics.get_mut("orders").unwrap().max_append_bytes += 1;
    assert!(config.validate().is_err());
}

#[test]
fn dedicated_progress_and_aio_placement_use_distinct_allowed_cpus() {
    let mut config = single();
    let workers = &mut config
        .brokers
        .get_mut("laptop")
        .unwrap()
        .devices
        .get_mut("ssd")
        .unwrap()
        .workers;
    workers.backend = IoBackend::Aio;
    workers.cpus = vec![2, 6];
    workers.progress_cpu = Some(10);
    workers.aio_cpu = Some(14);
    config
        .clone()
        .validate()
        .unwrap()
        .broker_plan("laptop", &host())
        .unwrap();
    for cpu in [2, 10, 15] {
        let mut bad = config.clone();
        bad.brokers
            .get_mut("laptop")
            .unwrap()
            .devices
            .get_mut("ssd")
            .unwrap()
            .workers
            .aio_cpu = Some(cpu);
        assert!(
            bad.validate()
                .unwrap()
                .broker_plan("laptop", &host())
                .is_err()
        );
    }
    config
        .brokers
        .get_mut("laptop")
        .unwrap()
        .devices
        .get_mut("ssd")
        .unwrap()
        .workers
        .backend = IoBackend::Pool;
    assert!(config.validate().is_err());
}

#[test]
fn identity_roundtrip_keeps_numeric_partition_order_and_balanced_initial_leaders() {
    let validated = three().validate().unwrap();
    let identity = validated.initialize(ids()).unwrap();
    assert_eq!(
        DeploymentIdentity::decode(&identity.encode().unwrap()).unwrap(),
        identity
    );
    let members: Vec<_> = identity.brokers.values().copied().collect();
    let partitions = &identity.topics["orders"].partitions;
    for (i, part) in partitions.iter().enumerate() {
        assert_eq!(part.partition as usize, i);
        assert_eq!(part.members[0], members[i % 3]);
        assert_eq!(
            part.members.iter().copied().collect::<BTreeSet<_>>(),
            members.iter().copied().collect()
        );
    }
    let unique: BTreeSet<_> = partitions
        .iter()
        .flat_map(|p| [p.group, p.incarnation])
        .collect();
    assert_eq!(unique.len(), 32);
}

#[test]
fn identity_checksum_covers_every_record_byte() {
    let identity = three().validate().unwrap().initialize(ids()).unwrap();
    let encoded = identity.encode().unwrap();
    for offset in 0..encoded.len() {
        let mut damaged = encoded.as_bytes().to_vec();
        damaged[offset] ^= 1;
        let damaged = String::from_utf8(damaged).unwrap();
        assert!(
            DeploymentIdentity::decode(&damaged).is_err(),
            "byte {offset}"
        );
    }
    assert!(DeploymentIdentity::decode(&format!("{encoded}\n")).is_err());
}

#[test]
fn shard_counts_and_ids_may_differ_without_changing_shared_identity() {
    let original = three().validate().unwrap();
    let identity = original.initialize(ids()).unwrap();
    let mut changed = three();
    changed.brokers.get_mut("b").unwrap().topology.shards = vec![shard(20), shard(4)];
    changed.brokers.get_mut("c").unwrap().topology.shards = vec![shard(9), shard(1), shard(5)];
    let changed = changed.validate().unwrap();
    changed.check_identity(&identity).unwrap();
    assert_eq!(changed.initialize(ids()).unwrap(), identity);
    for (name, expected) in [("a", vec![0]), ("b", vec![4, 20]), ("c", vec![1, 5, 9])] {
        let plan = changed.broker_plan(name, &host()).unwrap();
        for (i, part) in plan.partitions.iter().enumerate() {
            assert_eq!(part.shard, expected[i % expected.len()]);
        }
    }
}

#[test]
fn default_distribution_spans_topics_and_explicit_override_is_local() {
    let mut config = single();
    let mut topic = config.topics["orders"].clone();
    topic.partitions = 1;
    config.topics.insert("audit".to_owned(), topic);
    let topology = &mut config.brokers.get_mut("laptop").unwrap().topology;
    topology.shards = vec![shard(0), shard(1)];
    topology.partitions.push(PartitionOverride {
        topic: "orders".to_owned(),
        partition: 3,
        shard: 1,
    });
    let plan = config
        .validate()
        .unwrap()
        .broker_plan("laptop", &host())
        .unwrap();
    assert_eq!(plan.partitions[0].topic, "audit");
    assert_eq!(plan.partitions[1].shard, 1);
    assert_eq!(plan.partitions[4].shard, 1);
}

#[test]
fn no_incidental_256_partition_limit() {
    let mut config = single();
    config.topics.get_mut("orders").unwrap().partitions = 1024;
    let validated = config.validate().unwrap();
    assert_eq!(
        validated
            .broker_plan("laptop", &host())
            .unwrap()
            .partitions
            .len(),
        1024
    );
    let partition = validated.partition_for_key("orders", b"").unwrap();
    assert_eq!(partition, 0x2d06_8005_38d3_94c2_u64 as u32 % 1024);
}

#[test]
fn explicit_ids_are_constraints_and_generation_must_be_unique_nonzero() {
    let mut config = single();
    config.cluster.id = Some(Uuid::from_u128(1000));
    let validated = config.validate().unwrap();
    assert_eq!(
        validated.initialize(ids()).unwrap().cluster,
        Uuid::from_u128(1000)
    );
    assert!(validated.initialize(Uuid::nil).is_err());
    assert!(validated.initialize(|| Uuid::from_u128(42)).is_err());
}

#[test]
fn restart_rejects_changed_immutable_metadata_but_not_worker_settings() {
    let original = three().validate().unwrap().initialize(ids()).unwrap();
    for change in 0..6 {
        let mut config = three();
        match change {
            0 => config.cluster.id = Some(Uuid::from_u128(9999)),
            1 => config.topics.get_mut("orders").unwrap().partitions = 17,
            2 => config.topics.get_mut("orders").unwrap().partitioner_seed = 99,
            3 => {
                config.topics.get_mut("orders").unwrap().confirmation =
                    Confirmation::ReplicatedPersisting;
            }
            4 => config.brokers.get_mut("b").unwrap().id = Some(Uuid::from_u128(9999)),
            5 => config.topics.get_mut("orders").unwrap().id = Some(Uuid::from_u128(9999)),
            _ => unreachable!(),
        }
        assert!(
            config
                .validate()
                .unwrap()
                .check_identity(&original)
                .is_err()
        );
    }
    let mut config = three();
    config.brokers.get_mut("b").unwrap().topology.omq.io_threads = 2;
    config
        .validate()
        .unwrap()
        .check_identity(&original)
        .unwrap();
}

#[test]
fn persisted_identity_rejects_wrong_order_reused_ids_and_unknown_algorithm() {
    let validated = three().validate().unwrap();
    let original = validated.initialize(ids()).unwrap();
    for change in 0..13 {
        let mut identity = original.clone();
        let topic = identity.topics.get_mut("orders").unwrap();
        match change {
            0 => topic.partitions.swap(0, 1),
            1 => topic.partitions[0].members.swap(0, 1),
            2 => topic.partitions[0].group = topic.id,
            3 => topic.partitions[0].incarnation = Uuid::nil(),
            4 => topic.partitioner = "default-hasher".to_owned(),
            5 => {
                identity.brokers.remove("a");
            }
            6 => {
                topic.partitions.pop();
            }
            7 => topic.partitions[0].config_epoch = 0,
            8 => topic.partitions[0].config_epoch = 2,
            9 => {
                identity.principals.remove("a");
            }
            10 => {
                identity.principals.insert("a".into(), [0; 32]);
            }
            11 => {
                identity
                    .principals
                    .insert("b".into(), identity.principals["a"]);
            }
            12 => {
                identity.principals.insert("unknown".into(), [4; 32]);
            }
            _ => unreachable!(),
        }
        assert!(validated.check_identity(&identity).is_err());
    }
}

#[test]
fn parse_rejects_unknown_fields_including_unsupported_omq_affinity() {
    for suffix in [
        "\n[brokers.laptop.topology.omq]\nio_threads = 2\ncpu = 2\n",
        "\n[brokers.laptop.topology.dispatcher]\ncpus = [2]\n",
        "\n[brokers.laptop.devices.ssd.workers]\nbackend = 'io-uring'\n",
        "\n[limits]\nmax_partitons = 16\n",
    ] {
        assert!(Deployment::parse(&format!("{SINGLE}{suffix}")).is_err());
    }
}

#[test]
fn membership_and_confirmation_policy_are_explicit() {
    let mut config = single();
    config.cluster.mode = DeploymentMode::Three;
    assert!(config.validate().is_err());
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let mut config = single();
        config.topics.get_mut("orders").unwrap().confirmation = policy;
        assert!(config.validate().is_err());
    }
    let mut config = three();
    config.topics.get_mut("orders").unwrap().confirmation = Confirmation::LocalDurable;
    assert!(config.validate().is_err());
}

#[test]
fn aggregate_limits_apply_before_provisioning() {
    for change in 0..5 {
        let mut config = single();
        match change {
            0 => config.limits.max_topics = 0,
            1 => config.limits.max_partitions = 15,
            2 => config.topics.get_mut("orders").unwrap().partitions = 0,
            3 => config.topics.get_mut("orders").unwrap().max_append_bytes = u64::MAX,
            4 => {
                config.topics.clear();
            }
            _ => unreachable!(),
        }
        assert!(config.validate().is_err());
    }
}

#[test]
fn unsafe_topic_names_and_storage_paths_are_rejected() {
    for name in ["..", "a/b", "Orders", "a\\b", "", ".hidden", "a\0b"] {
        let mut config = single();
        let topic = config.topics.remove("orders").unwrap();
        config.topics.insert(name.to_owned(), topic);
        assert!(config.validate().is_err(), "{name:?}");
    }
    for path in [
        "/",
        "relative",
        "/var/../data",
        "/var/./data",
        "/var//data",
        "",
    ] {
        let mut config = single();
        config
            .brokers
            .get_mut("laptop")
            .unwrap()
            .devices
            .get_mut("ssd")
            .unwrap()
            .root = path.into();
        assert!(config.validate().is_err(), "{path:?}");
    }
}

#[test]
fn device_roots_cannot_overlap_and_shared_controllers_agree() {
    for root in [
        "/var/lib/ozzy/data",
        "/var/lib/ozzy",
        "/var/lib/ozzy/data/nested",
    ] {
        let mut config = single();
        let broker = config.brokers.get_mut("laptop").unwrap();
        let mut device = broker.devices["ssd"].clone();
        device.root = root.into();
        broker.devices.insert("other".to_owned(), device);
        broker.topology.shards = vec![shard(0)];
        assert!(config.validate().is_err());
    }
    let mut config = single();
    let broker = config.brokers.get_mut("laptop").unwrap();
    let mut other = broker.devices["ssd"].clone();
    other.root = "/other-device/data".into();
    other.workers.write_threads = 3;
    broker.devices.insert("other".to_owned(), other);
    broker.topology.shards = vec![shard(0)];
    assert!(config.validate().is_err());
}

#[test]
fn unavailable_affinity_is_rejected_only_for_selected_broker() {
    let mut config = three();
    config.brokers.get_mut("b").unwrap().topology.dispatcher.cpu = Some(255);
    let validated = config.validate().unwrap();
    validated.broker_plan("a", &host()).unwrap();
    assert!(validated.broker_plan("b", &host()).is_err());
    assert!(validated.broker_plan("missing", &host()).is_err());
}

#[test]
fn explicit_numa_placement_requires_local_cpu_memory_and_sufficient_pool() {
    let mut config = single();
    let topology = &mut config.brokers.get_mut("laptop").unwrap().topology;
    let mut local = shard(0);
    local.affinity = Affinity {
        cpu: Some(10),
        numa_node: Some(1),
    };
    topology.shards.push(local);
    topology.memory_pools.push(MemoryPool {
        numa_node: 1,
        bytes: 512 * 1024 * 1024,
    });
    let validated = config.clone().validate().unwrap();
    validated.broker_plan("laptop", &host()).unwrap();
    let mut restricted = host();
    restricted.memory_nodes.remove(&1);
    assert!(validated.broker_plan("laptop", &restricted).is_err());
    let mut unknown = host();
    unknown.cpus.insert(10, None);
    assert!(validated.broker_plan("laptop", &unknown).is_err());
    config
        .brokers
        .get_mut("laptop")
        .unwrap()
        .topology
        .memory_pools[0]
        .bytes = 1;
    assert!(config.validate().is_err());
}

#[test]
fn two_threads_cannot_claim_the_same_explicit_cpu() {
    let mut config = single();
    let topology = &mut config.brokers.get_mut("laptop").unwrap().topology;
    topology.dispatcher.cpu = Some(2);
    let mut local = shard(0);
    local.affinity.cpu = Some(2);
    topology.shards.push(local);
    assert!(
        config
            .validate()
            .unwrap()
            .broker_plan("laptop", &host())
            .is_err()
    );
}

#[test]
fn unsupported_aio_does_not_silently_fall_back() {
    let mut config = single();
    config
        .brokers
        .get_mut("laptop")
        .unwrap()
        .devices
        .get_mut("ssd")
        .unwrap()
        .workers
        .backend = IoBackend::Aio;
    let mut non_linux = host();
    non_linux.linux_aio = false;
    assert!(
        config
            .validate()
            .unwrap()
            .broker_plan("laptop", &non_linux)
            .is_err()
    );
}

#[test]
fn endpoint_collisions_and_non_connectable_addresses_are_rejected() {
    for endpoint in [
        "tcp://0.0.0.0:7100",
        "tcp://127.0.0.1:0",
        "tcp://*:7100",
        "inproc://",
        "ipc://relative",
        "tcp://host:99999",
        "tcp://host: 1",
    ] {
        let mut config = single();
        config.brokers.get_mut("laptop").unwrap().endpoints.peer = endpoint.to_owned();
        assert!(config.validate().is_err(), "{endpoint}");
    }
    let mut config = three();
    let peer = config.brokers["a"].endpoints.peer.clone();
    config.brokers.get_mut("b").unwrap().endpoints.peer = peer;
    assert!(config.validate().is_err());
}

#[test]
fn known_identity_constraints_cannot_alias_across_entity_types() {
    let mut config = single();
    config.cluster.id = Some(Uuid::from_u128(42));
    config.topics.get_mut("orders").unwrap().id = config.cluster.id;
    assert!(config.validate().is_err());
}

#[test]
fn placement_overrides_cannot_target_unknown_shards_or_duplicate_partitions() {
    for change in 0..3 {
        let mut config = single();
        let topology = &mut config.brokers.get_mut("laptop").unwrap().topology;
        let mut placement = PartitionOverride {
            topic: "orders".to_owned(),
            partition: 0,
            shard: 0,
        };
        match change {
            0 => placement.partition = 16,
            1 => placement.shard = 20,
            2 => topology.partitions.push(placement.clone()),
            _ => unreachable!(),
        }
        topology.partitions.push(placement);
        assert!(config.validate().is_err());
    }
}

#[test]
fn no_empty_cpu_set_or_zero_admission_budgets() {
    let mut empty = host();
    empty.cpus = BTreeMap::new();
    assert!(
        single()
            .validate()
            .unwrap()
            .broker_plan("laptop", &empty)
            .is_err()
    );
    let mut config = single();
    let mut local = shard(0);
    local.budget.control_slots = 0;
    config
        .brokers
        .get_mut("laptop")
        .unwrap()
        .topology
        .shards
        .push(local);
    assert!(config.validate().is_err());
}
