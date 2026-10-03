use super::{ids, shard, single, three};
use ozzy_config::{BrokerIdentity, DeploymentIdentity};
use uuid::Uuid;

fn local_ids() -> impl FnMut() -> Uuid {
    let mut next = 1000;
    move || {
        next += 1;
        Uuid::from_u128(next)
    }
}

#[test]
fn broker_identity_binds_independent_stores_to_shared_groups() {
    let config = three().validate().unwrap();
    let shared = config.initialize(ids()).unwrap();
    let mut source = local_ids();
    let records: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|broker| {
            config
                .initialize_broker_identity(&shared, broker, &mut source)
                .unwrap()
        })
        .collect();
    for (index, record) in records.iter().enumerate() {
        assert_eq!(
            BrokerIdentity::decode(&record.encode().unwrap()).unwrap(),
            *record
        );
        assert_eq!(record.broker, shared.brokers[["a", "b", "c"][index]]);
        for (local, partition) in record.topics["orders"]
            .iter()
            .zip(&shared.topics["orders"].partitions)
        {
            assert_eq!(local.group, partition.group);
            assert_eq!(local.partition, partition.partition);
            assert_eq!(local.volume, record.volumes["ssd"]);
            assert_eq!(local.generation, 1);
        }
    }
    assert_ne!(
        records[0].topics["orders"][0].store,
        records[1].topics["orders"][0].store
    );
    assert!(
        config
            .check_broker_identity(&shared, "b", &records[0])
            .is_err()
    );
}

#[test]
fn shard_changes_keep_store_identity_but_another_device_requires_relocation() {
    let mut source = single();
    let config = source.clone().validate().unwrap();
    let shared = config.initialize(ids()).unwrap();
    let local = config
        .initialize_broker_identity(&shared, "laptop", local_ids())
        .unwrap();
    source.brokers.get_mut("laptop").unwrap().topology.shards = vec![shard(3), shard(80)];
    source
        .clone()
        .validate()
        .unwrap()
        .check_broker_identity(&shared, "laptop", &local)
        .unwrap();
    // Retaining both established devices but changing partition ownership cannot
    // quietly treat whichever local files exist there as the expected store.
    let broker = source.brokers.get_mut("laptop").unwrap();
    let mut second = broker.devices["ssd"].clone();
    second.root = "/other/data".into();
    broker.devices.insert("other".into(), second);
    let config = source.clone().validate().unwrap();
    let local = config
        .initialize_broker_identity(&shared, "laptop", local_ids())
        .unwrap();
    source.brokers.get_mut("laptop").unwrap().topology.shards[1].device = "other".into();
    assert!(
        source
            .validate()
            .unwrap()
            .check_broker_identity(&shared, "laptop", &local)
            .is_err()
    );
}

#[test]
fn wrong_incomplete_reordered_or_aliased_bindings_are_rejected() {
    let config = single().validate().unwrap();
    let shared = config.initialize(ids()).unwrap();
    let local = config
        .initialize_broker_identity(&shared, "laptop", local_ids())
        .unwrap();
    for change in 0..11 {
        let mut bad = local.clone();
        match change {
            0 => bad.cluster = Uuid::nil(),
            1 => bad.broker = Uuid::nil(),
            2 => {
                bad.volumes.clear();
            }
            3 => {
                bad.topics.clear();
            }
            4 => {
                bad.topics.get_mut("orders").unwrap().pop();
            }
            5 => bad.topics.get_mut("orders").unwrap().swap(0, 1),
            6 => bad.topics.get_mut("orders").unwrap()[0].group = Uuid::from_u128(9000),
            7 => bad.topics.get_mut("orders").unwrap()[0].volume = Uuid::from_u128(9000),
            8 => bad.topics.get_mut("orders").unwrap()[0].generation = 0,
            9 => bad.topics.get_mut("orders").unwrap()[0].store = shared.cluster,
            _ => bad.topics.get_mut("orders").unwrap()[0].store = bad.topics["orders"][1].store,
        }
        assert!(
            config
                .check_broker_identity(&shared, "laptop", &bad)
                .is_err(),
            "change {change}"
        );
    }
    for duplicate in [Uuid::nil(), shared.cluster, Uuid::from_u128(5000)] {
        assert!(
            config
                .initialize_broker_identity(&shared, "laptop", || duplicate)
                .is_err()
        );
    }
}

#[test]
fn broker_record_checksum_and_type_cover_every_byte() {
    let config = single().validate().unwrap();
    let shared = config.initialize(ids()).unwrap();
    let local = config
        .initialize_broker_identity(&shared, "laptop", local_ids())
        .unwrap();
    let encoded = local.encode().unwrap();
    for index in 0..encoded.len() {
        let mut damaged = encoded.as_bytes().to_vec();
        damaged[index] ^= 1;
        assert!(BrokerIdentity::decode(std::str::from_utf8(&damaged).unwrap()).is_err());
    }
    assert!(BrokerIdentity::decode(&shared.encode().unwrap()).is_err());
    assert!(DeploymentIdentity::decode(&encoded).is_err());
}
