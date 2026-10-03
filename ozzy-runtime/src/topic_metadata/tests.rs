use super::*;

pub(super) fn page(first: u32, count: u32) -> TopicPage {
    let members = vec![
        NodeId::from_bytes([1; 16]),
        NodeId::from_bytes([2; 16]),
        NodeId::from_bytes([3; 16]),
    ];
    TopicPage {
        id: TopicId::from_bytes([4; 16]),
        name: "orders".to_owned(),
        partitioner_seed: 99,
        total: 4,
        policy: Policy::QuorumDurable,
        brokers: members
            .iter()
            .enumerate()
            .map(|(index, &node)| BrokerEndpoint {
                node,
                peer: format!("inproc://peer-{index}"),
                reader_pub: format!("inproc://readers-{index}"),
                follower_pub: None,
            })
            .collect(),
        first,
        partitions: (first..first + count)
            .map(|number| TopicPartition {
                number,
                group: GroupId::from_bytes([number as u8 + 10; 16]),
                config_epoch: 1,
                incarnation: PartitionIncarnation::from_bytes([number as u8 + 20; 16]),
                members: members.clone(),
            })
            .collect(),
    }
}

#[test]
fn assembles_numeric_order_and_hashes_before_writer_assignment() {
    let topic = TopicMetadata::from_pages([page(0, 2), page(2, 2)], 4).unwrap();
    assert_eq!(topic.partition_count(), 4);
    assert_eq!(topic.id(), TopicId::from_bytes([4; 16]));
    assert_eq!(topic.partition(3).unwrap().number, 3);
    assert!(topic.partition(4).is_none());
    assert_eq!(
        xxhash_rust::xxh3::xxh3_64_with_seed(b"customer-42", 99),
        0xa601_743e_03fb_24ca
    );
    let expected = (xxhash_rust::xxh3::xxh3_64_with_seed(b"customer-42", 99) % 4) as u32;
    assert_eq!(topic.keyed_partition(b"customer-42").number, expected);
}

#[test]
fn refuses_missing_changed_or_duplicate_page_identity() {
    assert_eq!(
        TopicMetadata::from_pages([page(0, 2)], 4),
        Err(TopicMetadataError)
    );
    assert_eq!(
        TopicMetadata::from_pages([page(2, 2)], 4),
        Err(TopicMetadataError)
    );
    let mut changed = page(2, 2);
    changed.partitioner_seed += 1;
    assert_eq!(
        TopicMetadata::from_pages([page(0, 2), changed], 4),
        Err(TopicMetadataError)
    );
    let mut duplicate = page(2, 2);
    duplicate.partitions[0].group = page(0, 2).partitions[0].group;
    assert_eq!(
        TopicMetadata::from_pages([page(0, 2), duplicate], 4),
        Err(TopicMetadataError)
    );
    assert_eq!(
        TopicMetadata::from_pages([page(0, 2), page(2, 2)], 3),
        Err(TopicMetadataError)
    );
}
