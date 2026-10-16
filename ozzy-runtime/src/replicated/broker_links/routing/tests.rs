use super::*;
use ozzy_proto::{PartitionIncarnation, append::Policy, directory};

fn topic(id: u8, count: u32, seed: u64) -> TopicMetadata {
    grouped(id, id, count, seed)
}

/// A topic whose groups are those of topic `groups`.
fn grouped(id: u8, groups: u8, count: u32, seed: u64) -> TopicMetadata {
    let node = NodeId::from_bytes([1; 16]);
    TopicMetadata::from_pages(
        [directory::TopicPage {
            id: TopicId::from_bytes([id; 16]),
            name: format!("topic{id}"),
            partitioner_seed: seed,
            total: count,
            policy: Policy::LocalDurable,
            brokers: vec![directory::BrokerEndpoint {
                node,
                peer: "inproc://broker".into(),
                reader_pub: "inproc://reader".into(),
                follower_pub: None,
            }],
            first: 0,
            partitions: (0..count)
                .map(|number| directory::TopicPartition {
                    number,
                    group: GroupId::from_bytes([groups * 16 + number as u8; 16]),
                    config_epoch: 1,
                    incarnation: PartitionIncarnation::from_bytes([id * 16 + number as u8; 16]),
                    members: vec![node],
                })
                .collect(),
        }],
        count as usize,
    )
    .unwrap()
}

#[test]
fn cached_idle_topics_share_identity_and_preserve_aggregate_partition_capacity() {
    let mut registry = Registry::new(4, 65536);
    let first = registry.topic(topic(1, 2, 7)).unwrap();
    assert!(Arc::ptr_eq(
        &first,
        &registry.topic(topic(1, 2, 7)).unwrap()
    ));
    assert!(matches!(
        registry.topic(topic(1, 2, 8)),
        Err(BrokerLinkError::Response)
    ));
    registry.topic(topic(2, 2, 7)).unwrap();
    assert!(matches!(
        registry.topic(topic(3, 1, 7)),
        Err(BrokerLinkError::Configuration)
    ));
    assert_eq!(registry.partitions, 4);
    assert_eq!(registry.used, 40960);
    assert!(
        first
            .interests
            .iter()
            .all(|interest| interest.revision() == 0),
        "metadata lookup installed eager interests"
    );
}

#[test]
fn routing_byte_refusal_does_not_consume_another_topics_capacity() {
    let mut registry = Registry::new(4, 20479);
    assert!(matches!(
        registry.topic(topic(1, 2, 7)),
        Err(BrokerLinkError::Configuration)
    ));
    assert_eq!((registry.used, registry.partitions), (0, 0));
    registry.topic(topic(2, 1, 7)).unwrap();
    assert_eq!((registry.used, registry.partitions), (18432, 1));
    assert!(Registry::new(4, 0).topic(topic(1, 1, 7)).is_err());
}

#[test]
fn a_topic_sharing_another_topics_group_is_refused() {
    let mut registry = Registry::new(4, 65536);
    let first = registry.topic(topic(1, 2, 7)).unwrap();
    assert!(matches!(
        registry.topic(grouped(2, 1, 1, 7)),
        Err(BrokerLinkError::Response)
    ));
    assert_eq!((registry.partitions, registry.groups.len()), (2, 2));
    assert!(
        registry
            .groups
            .values()
            .all(|state| Arc::ptr_eq(state, &first))
    );
}
