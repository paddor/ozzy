#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    reason = "test code"
)]

use ozzy_config::{Deployment, LogClass};
use uuid::Uuid;

const SINGLE: &str = include_str!("fixtures/single.toml");

fn enabled() -> Deployment {
    Deployment::parse(&format!("{SINGLE}\n[logs]\nmax_logs = 8\n")).unwrap()
}

#[test]
fn the_log_class_is_opt_in_with_small_default_sizes() {
    let disabled = Deployment::parse(SINGLE).unwrap().validate().unwrap();
    assert!(disabled.deployment().logs.is_none());
    assert!(
        disabled
            .log_placement("laptop", Uuid::from_u128(1))
            .is_err()
    );
    let logs = enabled().validate().unwrap().deployment().logs.unwrap();
    assert_eq!(
        logs,
        LogClass {
            segment_bytes: 2 * 1024 * 1024,
            max_append_bytes: 1024 * 1024,
            max_logs: 8,
        }
    );
    for fields in ["", "max_logs = 1\nretention = 1"] {
        assert!(Deployment::parse(&format!("{SINGLE}\n[logs]\n{fields}\n")).is_err());
    }
}

#[test]
fn the_log_class_obeys_topic_size_rules_and_the_partition_bound() {
    for case in 0..6 {
        let mut deployment = enabled();
        let partitions: usize = deployment
            .topics
            .values()
            .map(|topic| topic.partitions as usize)
            .sum();
        let logs = deployment.logs.as_mut().unwrap();
        match case {
            0 => logs.max_logs = 0,
            1 => logs.max_logs = deployment.limits.max_partitions - partitions + 1,
            2 => logs.max_append_bytes = 0,
            3 => logs.max_append_bytes = ozzy_config::MAX_APPEND_BYTES + 1,
            4 => logs.segment_bytes = 1024 * 1024 + 1,
            _ => logs.max_append_bytes = logs.segment_bytes / 2 + 1,
        }
        assert!(deployment.validate().is_err(), "accepted case {case}");
    }
    let mut deployment = enabled();
    let logs = deployment.logs.as_mut().unwrap();
    logs.max_append_bytes = 8 * 1024 * 1024;
    logs.segment_bytes = 16 * 1024 * 1024;
    // The largest append of any topic or log bounds the transport.
    assert_eq!(deployment.max_append_bytes(), 8 * 1024 * 1024);
    deployment.validate().unwrap();
}

#[test]
fn a_log_is_placed_by_its_id_alone() {
    let deployment = enabled().validate().unwrap();
    let shards: Vec<u32> = deployment
        .partition_placements("laptop")
        .unwrap()
        .iter()
        .map(|placement| placement.shard)
        .collect();
    let mut used = std::collections::BTreeSet::new();
    for id in 1..64 {
        let id = Uuid::from_u128(id);
        let placement = deployment.log_placement("laptop", id).unwrap();
        assert_eq!(placement, deployment.log_placement("laptop", id).unwrap());
        assert!(shards.contains(&placement.shard));
        assert_eq!(placement.partition, 0);
        assert_eq!(placement.topic, format!("l-{}", id.simple()));
        assert!(
            placement
                .directory
                .ends_with(format!("logs/{}", id.simple()))
        );
        let store =
            std::path::Path::new(ozzy_config::LOG_DIRECTORY).join(ozzy_config::log_label(id));
        assert!(placement.directory.ends_with(store));
        assert!(placement.deduplication.is_none() && placement.delay.is_none());
        assert_eq!(placement.retention, ozzy_config::TopicRetention::default());
        used.insert(placement.shard);
    }
    let all: std::collections::BTreeSet<u32> = shards.into_iter().collect();
    assert_eq!(used, all);
    assert!(
        deployment
            .log_placement("nobody", Uuid::from_u128(1))
            .is_err()
    );
}

#[test]
fn a_log_record_survives_encoding_and_detects_damage() {
    use ozzy_config::{LogRecord, LogState};
    let record = LogRecord {
        cluster: Uuid::from_u128(1),
        broker: Uuid::from_u128(2),
        log: Uuid::from_u128(3),
        incarnation: Uuid::from_u128(4),
        members: vec![Uuid::from_u128(2), Uuid::from_u128(5), Uuid::from_u128(6)],
        volume: Uuid::from_u128(7),
        store: Uuid::from_u128(8),
        generation: 1,
        segment_bytes: 4 * 1024 * 1024,
        max_append_bytes: 1024 * 1024,
        state: LogState::Creating,
    };
    let encoded = record.encode().unwrap();
    assert_eq!(LogRecord::decode(&encoded).unwrap(), record);
    assert!(LogRecord::decode(&encoded.replace("creating", "ready")).is_err());
    assert!(ozzy_config::VolumeIdentity::decode(&encoded).is_err());
    // A later state or another store is still the same log.
    let ready = LogRecord {
        state: LogState::Ready,
        store: Uuid::from_u128(9),
        ..record.clone()
    };
    assert!(record.same_log(&ready));
    assert!(LogState::Creating < LogState::Ready && LogState::Deleting < LogState::Deleted);
    for change in 0..4 {
        let mut other = record.clone();
        match change {
            0 => other.incarnation = Uuid::from_u128(40),
            1 => other.members.reverse(),
            2 => other.segment_bytes *= 2,
            _ => other.log = Uuid::from_u128(30),
        }
        assert!(!record.same_log(&other), "accepted change {change}");
    }
}
