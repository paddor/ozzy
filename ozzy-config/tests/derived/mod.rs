use super::*;

fn clustered(cluster: u128) -> ozzy_config::ValidatedDeployment {
    let mut config = three();
    config.cluster.id = Some(Uuid::from_u128(cluster));
    config.validate().unwrap()
}

#[test]
fn every_broker_derives_the_same_identity_from_the_cluster_id() {
    let identity = clustered(7).derive_identity().unwrap();
    assert_eq!(identity, clustered(7).derive_identity().unwrap());
    assert_eq!(identity.cluster, Uuid::from_u128(7));
    clustered(7).check_identity(&identity).unwrap();
    assert_eq!(identity.brokers["a"].get_version_num(), 8);

    let other = clustered(8).derive_identity().unwrap();
    assert_ne!(identity.brokers, other.brokers);
    assert_ne!(identity.principals, other.principals);
    assert_ne!(identity.topics, other.topics);
}

#[test]
fn a_derived_id_does_not_depend_on_the_other_names() {
    let identity = clustered(7).derive_identity().unwrap();
    let mut config = three();
    config.cluster.id = Some(Uuid::from_u128(7));
    let orders = config.topics["orders"].clone();
    config.topics.insert("audit".to_owned(), orders);
    let grown = config.validate().unwrap().derive_identity().unwrap();
    assert_eq!(grown.brokers, identity.brokers);
    assert_eq!(grown.topics["orders"].id, identity.topics["orders"].id);
    assert_eq!(
        grown.topics["orders"].partitions[3].group,
        identity.topics["orders"].partitions[3].group
    );
    assert_ne!(grown.topics["audit"].id, identity.topics["orders"].id);
}

#[test]
fn derivation_keeps_configured_ids_and_needs_a_cluster_id() {
    let mut config = three();
    config.cluster.id = Some(Uuid::from_u128(7));
    config.brokers.get_mut("b").unwrap().id = Some(Uuid::from_u128(99));
    let identity = config.validate().unwrap().derive_identity().unwrap();
    assert_eq!(identity.brokers["b"], Uuid::from_u128(99));

    let error = three().validate().unwrap().derive_identity().unwrap_err();
    assert!(error.to_string().contains("cluster.id"), "{error}");
}

#[test]
fn a_topic_built_in_code_matches_an_empty_table() {
    let parsed: ozzy_config::Topic = toml::from_str("").unwrap();
    let built = ozzy_config::Topic::default();
    assert_eq!(parsed.id, built.id);
    assert_eq!(parsed.partitions, built.partitions);
    assert_eq!(parsed.partitioner_seed, built.partitioner_seed);
    assert_eq!(parsed.segment_bytes, built.segment_bytes);
    assert_eq!(parsed.max_append_bytes, built.max_append_bytes);
    assert_eq!(parsed.retention, built.retention);
    assert_eq!(parsed.deduplication, built.deduplication);
    assert_eq!(parsed.delay, built.delay);
    assert_eq!(parsed.producer_idle_secs, built.producer_idle_secs);

    let device = ozzy_config::Device::new("/var/lib/ozzy", "nvme0");
    assert_eq!(device.root, std::path::Path::new("/var/lib/ozzy"));
    assert_eq!(device.controller, "nvme0");
    assert_eq!(device.workers, ozzy_config::StorageWorkers::default());
}
